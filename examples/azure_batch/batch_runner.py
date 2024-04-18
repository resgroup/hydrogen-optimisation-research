import os
import json
import tarfile
from itertools import product

from batch_submission.blob import upload_file_to_container
from batch_submission.batch_submission import BatchSubmission
from batch_submission.monitor import Monitor

from hoptimiser.component_inputs_reader import read_component_data, populate_combinations

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
        with tarfile.open('core.tar.gz', 'w:gz') as core_tar:
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

                    f'python -m hoptimiser.variable_price_azure {str_c} &> {output_dir}/log.txt'
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
            os.remove('core.tar.gz')
            os.remove('task.tar.gz')
        except:
            pass

    def run(self) -> None:
        self._zip_up_core_scripts()
        self.batch_job.create_containers()
        self.batch_job.upload_files(
            setup_files_path='core.tar.gz',
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

        self._cleanup()


if __name__ == '__main__':

    input_file_name_components = r'C:\Users\tyoung\Documents\GitHub\hydrogen-optimisation-research\inputs\component_inputs.xlsx'
    tank_df, electrolyser_df, data_years = read_component_data(input_file_name_components)
    combinations = populate_combinations(tank_df, electrolyser_df, input_file_name_components)

    batch_runner = HoptimiserBatchRunner(
        analysis_name='tom-simple-test',
        combinations=combinations
    )

    batch_runner.run()



#    monitor = Monitor(
#        batch_job=batch_runner.batch_job,
#    )
#    monitor.run(
#        print_output=True,
#        sleep_time_s=300,
#    )
