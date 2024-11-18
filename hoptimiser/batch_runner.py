import os
import tarfile
import pandas as pd
import time

from msrestazure.azure_operation import succeeded

from batch_submission.blob import upload_file_to_container
from batch_submission.batch_submission import BatchSubmission
from batch_submission.monitor import Monitor
from batch_submission.pool import create_pool
from batch_submission import config
from batch_submission.config import POOL_ID
from batch_submission.utils import chunk

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
        self.node_count = len(combinations)

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
            output_dir = f'{str(c)[1:-1].replace(",", "_").replace(" ", "")}'
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

        # Create a new pool:
        self.batch_job.create_pool(pool_commands=self.POOL_COMMANDS, node_count=self.node_count)
        print('New Pool created')

        # create the names of the jobs so that we can delete any jobs with the same name before starting new ones:
        self._build_task_list()

        chunked = chunk(it=self.task_list, size=100)
        for chunk_idx, task_list_chunk in enumerate(chunked):
            job_id = f'{config.JOB_ID}-{chunk_idx}'
            self.batch_job.job_ids.append(job_id)
        print(self.batch_job.job_ids)

        # delete all jobs with the above job_ids and tasks:
        self.batch_job.cleanup(delete_container=False, delete_pool=False)

        success = False
        count = 0

        while success == False and count < 60:
            try:
                remaining_tasks = self.batch_job.run(
                    pool_commands=self.POOL_COMMANDS,
                    task_list=self.task_list,
                    wait_for_tasks=False,
                    max_tasks_per_job=self._max_tasks_per_job,
                )
                success = True

            except:
                print('Error in running batch jobs. Trying again in 10 seconds, please wait...')
                time.sleep(10)
                count += 1


if __name__ == '__main__':

    analysis_name = 'batch-analysis'

    analysis_name = analysis_name.lower()

    maximum_nodes = 350
    input_file_name_components = 'component_inputs.xlsx'

    tank_df, electrolyser_df, data_years = read_component_data(
        os.path.join(PROJECT_ROOT_DIR, 'inputs', input_file_name_components))
    combinations = populate_combinations(tank_df, electrolyser_df,
                                         os.path.join(PROJECT_ROOT_DIR, 'inputs', input_file_name_components))

    input_file_name_components = os.path.join(
        PROJECT_ROOT_DIR, 'inputs',
        'component_inputs.xlsx',
    )

    technical_inputs = pd.read_excel(input_file_name_components, sheet_name='Technical Inputs')
    technical_inputs.set_index('Parameter', inplace=True)

    n_best_results_download = min(int(technical_inputs['Value']['Number of Batch Setups to Download']),
                                  len(combinations))

    print('Number of combinations = ', len(combinations))
    print('The following number of best results will be downloaded in full:', n_best_results_download)

    batch_runner = HoptimiserBatchRunner(
        analysis_name=analysis_name,
        combinations=combinations
    )

    # check if pool exists:
    try:
        pool = batch_runner.batch_job.batch_service_client.pool.get(POOL_ID)
        pool_exists = True
        if pool.allocation_state == 'steady':
            pool_steady = True
            print('Pool exists and is steady, will be deleted and recreated...')
        else:
            pool_steady = False
    except:
        pool_exists = False

    if pool_exists:
        # wait for pool to become steady or for deletion from previous run to complete:
        steady_count = 0
        while not pool_steady and steady_count < 30:
            print('Pool is still in flux from a previous run, waiting for it to stabilise...')
            time.sleep(20)
            steady_count += 1
            try:
                pool = batch_runner.batch_job.batch_service_client.pool.get(POOL_ID)
                pool_exists = True
                if pool.allocation_state == 'steady':
                    pool_steady = True
            except:
                pool_exists = False
                pool_steady = True

        if pool_steady:
            # delete existing pool:
            print('Pool is steady or does not exist. Deleting pool if it still exists...')
            batch_runner.batch_job.cleanup(delete_container=False, delete_jobs=False)
            pool_still_exists = True
            while pool_still_exists:
                try:
                    pool = batch_runner.batch_job.batch_service_client.pool.get(POOL_ID)
                    print('Waiting for pool to delete...')
                    time.sleep(20)
                except:
                    pool_still_exists = False
                    print('Pool deleted. Starting new pool...')

        else:
            print('Pool did not stabilise after 10 minutes. Exiting...')
            exit()
    else:
        print('No existing pool found, new pool will be created...')

    monitor = Monitor(
        batch_job=batch_runner.batch_job,
    )

    # the run command includes creating the new pool, deleting old jobs and creating new ones:
    batch_runner.run()

    # monitor._resize_pool(target_low_priority_nodes=int(min(maximum_nodes, len(combinations))), target_dedicated_nodes=int(0))

    # once pool has been created, we can start the monitor to check for job completion:
    monitor.run(
        print_output=True,
        sleep_time_s=30,
        analysis_name=analysis_name,
        combinations=combinations,
        n_best_results_download=n_best_results_download,
    )

    # delete container, jobs and pool:
    batch_runner.batch_job.cleanup()

    results = pd.read_csv('batch_results/batch_results_temp.csv')

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

        combination = results.loc[i, 'combination'][1:-1].split(sep=',')

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

    results.to_csv('batch_results/batch_results_temp.csv')
    results.to_csv('batch_results/batch_results.csv')
