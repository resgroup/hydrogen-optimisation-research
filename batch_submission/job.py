
from azure.batch import BatchServiceClient
from azure.batch.models import JobAddParameter, PoolInformation


def create_job(batch_service_client: BatchServiceClient, job_id: str, pool_id: str) -> None:
    """
    Creates a job with the specified ID, associated with the specified pool.

    :param batch_service_client: A Batch service client.
    :type batch_service_client: `azure.batch.BatchServiceClient`
    :param str job_id: The ID for the job.
    :param str pool_id: The ID for the pool.
    """
    print(f'Creating job [{job_id}]...')

    job = JobAddParameter(
        id=job_id,
        pool_info=PoolInformation(
            pool_id=pool_id,
        ),
    )

    batch_service_client.job.add(job)
