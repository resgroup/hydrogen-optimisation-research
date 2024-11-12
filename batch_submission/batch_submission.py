import datetime
from typing import List

from azure.batch.batch_auth import SharedKeyCredentials
from azure.batch.models import BatchErrorException
from azure.batch import BatchServiceClient
from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobServiceClient, ContainerSasPermissions

from batch_submission import config
from batch_submission.pool import VmSize, create_pool
from batch_submission.job import create_job
from batch_submission.tasks import add_task, wait_for_tasks_to_complete
from batch_submission.blob import upload_file_to_container, get_container_sas_url
from batch_submission.utils import query_yes_no, print_batch_exception, chunk

import dotenv

dotenv.load_dotenv()


class BatchSubmission:
    def __init__(
            self,
            pool_cmd: list = None,
            pool_dedicated_node_count: int = config.DEDICATED_POOL_NODE_COUNT,
            pool_low_priority_node_count: int = config.LOW_PRIORITY_POOL_NODE_COUNT,
            pool_vm_size: VmSize = VmSize.STANDARD_DS1_v2,
            standard_out_file_name: str = 'stdout.txt',
    ):
        self.pool_cmd = pool_cmd if pool_cmd else []
        self.dedicated_node_count = pool_dedicated_node_count
        self.pool_low_priority_node_count = pool_low_priority_node_count
        self.pool_vm_size = pool_vm_size
        self.standard_out_file_name = standard_out_file_name

        self.blob_service_client = BlobServiceClient(
            account_url=f"https://{config.STORAGE_ACCOUNT_NAME}.{config.STORAGE_ACCOUNT_DOMAIN}",
            credential=config.STORAGE_ACCOUNT_KEY,
        )
        self.batch_credentials = SharedKeyCredentials(
            account_name=config.BATCH_ACCOUNT_NAME,
            key=config.BATCH_ACCOUNT_KEY,
        )
        self.batch_service_client = BatchServiceClient(
            credentials=self.batch_credentials,
            batch_url=config.BATCH_ACCOUNT_URL,
        )

        self.input_container_name: str = 'input'
        self.output_container_name: str = 'output'
        self.input_container_sas_url: str = ''
        self.output_container_sas_url: str = ''
        self.setup_files: list = []
        self.task_files: list = []
        self.job_ids: list = []

    def create_containers(self, input_container_name: str = 'input', output_container_name: str = 'output'):
        self.input_container_sas_url = self._create_input_container(input_container_name=input_container_name)
        self.output_container_sas_url = self._create_output_container(output_container_name=output_container_name)

    def _create_input_container(self, input_container_name: str = 'input'):
        self.input_container_name = input_container_name
        try:
            self.blob_service_client.create_container(
                name=self.input_container_name,
            )
        except ResourceExistsError:
            print(f"Container: '{self.input_container_name}' already exists.")

        input_container_sas_url = get_container_sas_url(
            blob_service_client=self.blob_service_client,
            container_name=self.input_container_name,
            container_permissions=ContainerSasPermissions(write=True),
        )
        print(f'Container [{input_container_name}] created.')
        return input_container_sas_url

    def _create_output_container(self, output_container_name: str = 'output') -> str:
        self.output_container_name = output_container_name
        try:
            self.blob_service_client.create_container(
                name=self.output_container_name,
            )
        except ResourceExistsError:
            print(f"Container: '{self.output_container_name}' already exists.")
        output_container_sas_url = get_container_sas_url(
            blob_service_client=self.blob_service_client,
            container_name=self.output_container_name,
            container_permissions=ContainerSasPermissions(write=True),
        )
        print(f'Container [{output_container_name}] created.')
        return output_container_sas_url

    def upload_files(self, setup_files_path: str = None, tasks_file_paths: list = None):
        if setup_files_path is not None:
            self.setup_files = [upload_file_to_container(
                blob_service_client=self.blob_service_client,
                container_name=self.input_container_name,
                file_path=setup_files_path,
            )]
        if tasks_file_paths is not None:
            self.task_files = [upload_file_to_container(
                blob_service_client=self.blob_service_client,
                container_name=self.input_container_name,
                file_path=tasks_file_path,
            ) for tasks_file_path in tasks_file_paths]

    def create_pool(self,
                    pool_commands: list,
                    node_count: int):
        create_pool(
            batch_service_client=self.batch_service_client,
            pool_id=config.POOL_ID,
            input_files=self.setup_files,
            commands=pool_commands,
            node_count=node_count
        )

    def run(
            self,
            pool_commands: list,
            task_list: list,
            wait_for_tasks: bool = True,
            max_tasks_per_job: int = 100) -> list:
        try:
            if len(task_list) > max_tasks_per_job:
                remaining_tasks = self._run_many_jobs(
                    task_list=task_list,
                    max_tasks_per_job=max_tasks_per_job,
                )
            else:
                remaining_tasks = self._run_single_job(
                    task_list=task_list,
                    job_id=f'{config.JOB_ID}-0',
                    wait_for_tasks=wait_for_tasks,
                )
            return remaining_tasks

        except BatchErrorException as err:
            print_batch_exception(err)
            raise

    def _run_single_job(self, task_list: list, job_id: str, wait_for_tasks: bool = True) -> list:
        create_job(
            batch_service_client=self.batch_service_client,
            job_id=job_id,
            pool_id=config.POOL_ID,
        )
        self.job_ids.append(job_id)
        print(f'Adding {len(task_list)} tasks to job [{job_id}]...')
        for index, task in enumerate(task_list):
            add_task(
                batch_service_client=self.batch_service_client,
                job_id=job_id,
                task_id=f'{index}-{task.get("task_id")}',
                input_files=self.task_files + task.get('resource_files', []),
                output_container_sas_url=task.get('output_container_sas_url', self.output_container_sas_url),
                commands=task["cmd"],
                max_wall_clock_time=config.MAX_WALL_CLOCK_TIME,
                max_task_retry_count=config.MAX_TASK_RETRY_COUNT,
                output_file_pattern_list=task.get("output_file_pattern_list", ["src/log.txt"])
            )
        if wait_for_tasks:
            wait_for_tasks_to_complete(
                batch_service_client=self.batch_service_client,
                job_id=config.JOB_ID,
                timeout=datetime.timedelta(minutes=30)
            )
        remaining_tasks = [x for x in task_list]
        return remaining_tasks

    def _run_many_jobs(self, task_list: list, max_tasks_per_job: int) -> list:
        chunked = chunk(it=task_list, size=max_tasks_per_job)
        for chunk_idx, task_list_chunk in enumerate(chunked):
            job_id = f'{config.JOB_ID}-{chunk_idx}'
            self._run_single_job(
                task_list=task_list_chunk,
                job_id=job_id,
                wait_for_tasks=False,
            )
            if chunk_idx == max_tasks_per_job - 1:
                break
        remaining_tasks = [item for sublist in [x for x in chunked] for item in sublist]
        return remaining_tasks

    def cleanup(self, delete_container: bool = True, delete_jobs: bool = True, delete_pool: bool = True):
        # Delete input container in storage

        if delete_container:
            try:
                self.blob_service_client.delete_container(
                    container=self.input_container_name,
                )
                print(f'Deleting container [{self.input_container_name}]')
            except:
                print('Failed to delete container!')


        if delete_jobs:
            try:
                for job_id in self.job_ids:
                    self.batch_service_client.job.delete(
                        job_id=job_id
                    )
                    print(f'Deleting Job ID: [{job_id}]')
            except:
                print('Failed to delete jobs!')

        if delete_pool:
            try:
                self.batch_service_client.pool.delete(config.POOL_ID)
                print(f'Deleting Pool ID: [{config.POOL_ID}]')
            except:
                print('Failed to delete pool!')
