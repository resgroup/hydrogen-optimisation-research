import os
import tarfile
import pandas as pd

from batch_submission.blob import upload_file_to_container
from batch_submission.batch_submission import BatchSubmission
from batch_submission.monitor import Monitor

from hoptimiser.component_inputs_reader import read_component_data, populate_combinations

from hoptimiser.config import PROJECT_ROOT_DIR


class HoptimiserBatchRunner:

    POOL_COMMANDS: list = [
        'mkdir -p ./src',
        'tar xzf core.tar.gz -C .',
        'conda env create -f batch_environment.yml',
    ]

    def __init__(self, analysis_name: str, combinations: list):
        self.analysis_name = analysis_name
        self.combinations = combinations

        self._max_tasks_per_job: int = 100
        self.batch_job = BatchSubmission()
        self.task_list: list = []

    def _zip_up_core_scripts(self) -> None:
        with tarfile.open('examples/azure_batch/core.tar.gz', 'w:gz') as core_tar:
            core_tar.add('batch_environment.yml',
                         os.path.basename('batch_environment.yml'))

        with tarfile.open('task.tar.gz', 'w:gz') as task_tar:
            task_tar.add('hoptimiser', os.path.basename('hoptimiser'))

    def _build_task_list(self) -> None:
        self.batch_job.create_containers(
            output_container_name=self.analysis_name.lower(),
        )
        component_file = upload_file_to_container(
            blob_service_client=self.batch_job.blob_service_client,
            container_name='input',
            file_path='inputs/component_inputs.xlsx',
        )
        demand_file = upload_file_to_container(
            blob_service_client=self.batch_job.blob_service_client,
            container_name='input',
            file_path='inputs/demand_profiles.csv',
        )
        price_file = upload_file_to_container(
            blob_service_client=self.batch_job.blob_service_client,
            container_name='input',
            file_path='inputs/price_profiles.csv',
        )

        for c in self.combinations:

            str_c = str(c).replace(" ", "")
            output_dir = f'{str(c)[1:-1].replace(",","_").replace(" ", "")}'
            task = {
                "cmd": [
                    'tar xzf task.tar.gz -C .',
                    f'mkdir -p ./{output_dir}',

                    'conda info --envs',
                    'conda list',

                    'source activate hoptimiser',

                    f'python -m hoptimiser.variable_price_orchestrator {str_c} True &> {output_dir}/log.txt'
                ],
                "output_file_pattern_list": [
                    '*/log.txt',
                    '*/*output_time_series.csv',
                    '*/*output_annual_results.csv',
                    '*/lcoh2_result.json',
                ],
                "output_container_sas_url": self.batch_job.output_container_sas_url,
                "resource_files": [component_file, demand_file, price_file],
            }
            self.task_list += [task]

    def _cleanup(self) -> None:
        self.batch_job.cleanup()
        try:
            os.remove('../examples/azure_batch/core.tar.gz')
            os.remove('task.tar.gz')
        except:
            pass

    def run(self) -> None:
        self._zip_up_core_scripts()
        self.batch_job.create_containers()
        self.batch_job.upload_files(
            setup_files_path='examples/azure_batch/core.tar.gz',
            tasks_file_paths=['task.tar.gz']
        )
        self._build_task_list()

        remaining_tasks = self.batch_job.run(
            pool_commands=self.POOL_COMMANDS,
            task_list=self.task_list,
            wait_for_tasks=False,
            max_tasks_per_job=self._max_tasks_per_job,
        )
        # remaining_tasks = [
        #     {key: val} for key, val in remaining_tasks[0].items() if key not in [
        #         'resource_files',
        #         'cmd',
        #         'output_file_pattern_list',
        #         'output_container_sas_url',
        #     ]
        # ]
        # if remaining_tasks:
        #     with open('remaining_tasks.json', 'w', encoding='utf-8') as fname:
        #         json.dump(remaining_tasks, fname, indent=2)

        #self._cleanup()


if __name__ == '__main__':

    analysis_name = 'Analysis'

    maximum_nodes = 350
    input_file_name_components = 'component_inputs.xlsx'

    tank_df, electrolyser_df, data_years = read_component_data(os.path.join(PROJECT_ROOT_DIR, 'inputs', input_file_name_components))
    combinations = populate_combinations(tank_df, electrolyser_df, os.path.join(PROJECT_ROOT_DIR, 'inputs', input_file_name_components))

    batch_runner = HoptimiserBatchRunner(
        analysis_name=analysis_name,
        combinations=combinations
    )

    monitor = Monitor(
        batch_job=batch_runner.batch_job,
    )
    print('Number of combinations = ', len(combinations))
    monitor._resize_pool(target_low_priority_nodes=int(min(maximum_nodes, len(combinations))), target_dedicated_nodes=int(0))

    batch_runner.run()

    monitor.run(
        print_output=True,
        sleep_time_s=30,
        analysis_name=analysis_name,
        combinations=combinations,
        electrolyser_df=electrolyser_df,
        tank_df=tank_df
    )

    #delete container and jobs:
    batch_runner.batch_job.cleanup()

    results = pd.read_csv('batch_results_temp.csv')

    results['electrolyser_id'] = None
    results['number_of_electrolysers'] = None
    results['electrolyser_manufacturer'] = None
    results['electrolyser_capacity'] = None
    results['tank_id'] = None
    results['number_of_tanks'] = None
    results['tank_manufacturer'] = None
    results['tank_capacity'] = None
    results['stack_replacement_years'] = None

    for i in range(0, len(results)):

        combination = results.loc[i, 'combination'][1:-1].split(sep = ',')

        results.loc[i, 'electrolyser_id'] = electrolyser_df.loc[int(combination[0]), 'id']
        results.loc[i, 'electrolyser_manufacturer'] = electrolyser_df.loc[int(combination[0]), 'Manufacturer']
        results.loc[i, 'tank_id'] = tank_df.loc[int(combination[2]), 'id']
        results.loc[i, 'tank_manufacturer'] = tank_df.loc[int(combination[2]), 'Manufacturer']
        results.loc[i, 'number_of_electrolysers'] = int(combination[1])
        results.loc[i, 'electrolyser_capacity'] = electrolyser_df.loc[int(combination[0]), 'Capacity (MW)']
        results.loc[i, 'number_of_tanks'] = int(combination[3])
        results.loc[i, 'tank_capacity'] = tank_df.loc[int(combination[2]), 'H2 MWh Capacity']
        if len(combination) > 4:
            results.loc[i, 'stack_replacement_years'] = str(combination[4:])

    results.to_csv('batch_results_temp.csv')
    results.to_csv('batch_results.csv')

