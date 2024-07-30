
import sys
import time
import datetime

from azure.batch import BatchServiceClient
import azure.batch.models as batchmodels


def add_task(
        batch_service_client: BatchServiceClient,
        job_id: str,
        task_id: str,
        input_files: list,
        output_container_sas_url: str,
        commands: list,
        max_wall_clock_time: datetime.timedelta = None,
        max_task_retry_count: int = None,
        output_file_pattern_list: list = ["src/log.txt"],
) -> None:
    """
    Adds a task for each input file in the collection to the specified job.

    :param batch_service_client: A Batch service client.
    :type batch_service_client: `azure.batch.BatchServiceClient`
    :param str job_id: The ID of the job to which to add the tasks.
    :param str task_id: The ID of the task
    :param list input_files: A collection of input files. One task will be
     created for each input file.
    :param output_container_sas_url: A SAS url granting write access to
    the specified Azure Blob storage container.
    :param list commands: A collection of command line prompts to execute the given task.
    :param datetime.timedelta max_wall_clock_time: Maximum amount of time the task is allowed to run.
    :param int max_task_retry_count: Maximum number of times a task will retry upon failure.
    :param list output_file_pattern_list: Optional collection of file patterns that will persist to
     blob storage regardless of task success or failure.
    """

    startup_commands = [
        'echo "export PATH=/opt/miniconda/bin:$PATH" >> ~/.bashrc',
        'source ~/.bashrc',
    ]

    cmd = "/bin/bash -c 'set -e; set -o pipefail; {}; wait'".format(';'.join(startup_commands + commands))

    tasks = list()

    output_files = [batchmodels.OutputFile(
        file_pattern=output_file_pattern,
        destination=batchmodels.OutputFileDestination(
            container=batchmodels.OutputFileBlobContainerDestination(
                container_url=output_container_sas_url,
            ),
        ),
        upload_options=batchmodels.OutputFileUploadOptions(
            upload_condition=batchmodels.OutputFileUploadCondition.task_completion,
        ),
    ) for output_file_pattern in output_file_pattern_list]

    tasks.append(
        batchmodels.TaskAddParameter(
            id=f'Task-{job_id}-{task_id}',
            command_line=cmd,
            resource_files=input_files,
            output_files=output_files,
            constraints=batchmodels.TaskConstraints(
                max_wall_clock_time=max_wall_clock_time,
                max_task_retry_count=max_task_retry_count,
            ),
        ),
    )
    batch_service_client.task.add_collection(
        job_id=job_id,
        value=tasks,
    )


def wait_for_tasks_to_complete(
        batch_service_client: BatchServiceClient,
        job_id: str,
        timeout: datetime.timedelta):
    """
    Returns when all tasks in the specified job reach the Completed state.

    :param batch_service_client: A Batch service client.
    :type batch_service_client: `azure.batch.BatchServiceClient`
    :param str job_id: The id of the job whose tasks should be monitored.
    :param timedelta timeout: The duration to wait for task completion. If all
    tasks in the specified job do not reach Completed state within this time
    period, an exception will be raised.
    """
    timeout_expiration = datetime.datetime.now() + timeout

    print(f"Monitoring all tasks for 'Completed' state, timeout in {timeout}", end='')

    while datetime.datetime.now() < timeout_expiration:
        print('.', end='')
        sys.stdout.flush()
        tasks = batch_service_client.task.list(job_id=job_id)

        incomplete_tasks = [task for task in tasks if
                            task.state != batchmodels.TaskState.completed]
        if not incomplete_tasks:
            print()
            return True
        else:
            time.sleep(1)

    print()
    raise RuntimeError(f"ERROR: Tasks did not reach 'Completed' state within timout period of {str(timeout)}.")
