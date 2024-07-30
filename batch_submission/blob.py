import os
import datetime

from azure.storage.blob import (
    BlobServiceClient,
    BlobSasPermissions,
    ContainerSasPermissions,
    generate_blob_sas,
    generate_container_sas,
)
from azure.batch.models import ResourceFile, BatchErrorException

from batch_submission import config
from batch_submission.utils import print_batch_exception


def _create_blob_sas_token(
    blob_service_client: BlobServiceClient,
    container_name: str,
    blob_name: str,
    permission: BlobSasPermissions,
    expiry: datetime.datetime = None,
) -> str:
    """Create a blob sas token

    :param blob_service_client: The storage block blob client to use.
    :param container_name: The name of the container to upload the blob to.
    :param blob_name: The name of the blob to upload the local file to.
    :param permission The permissions of the SAS token
    :param expiry: The SAS expiry time.
    :return: A SAS token
    """
    if expiry is None:
        expiry = datetime.datetime.utcnow() + datetime.timedelta(
            hours=config.STORAGE_ACCOUNT_SAS_TOKEN_TIMEOUT_HOURS)

    if not blob_service_client.account_name:
        raise ValueError("Blob service client must have a valid account name")
    if not blob_service_client.credential:
        raise ValueError("Blob service client must have a valid credential")

    return generate_blob_sas(
        account_name=blob_service_client.account_name,
        account_key=blob_service_client.credential.account_key,
        container_name=container_name,
        blob_name=blob_name,
        permission=permission,
        expiry=expiry,
    )


def build_blob_sas_url(
    blob_service_client: BlobServiceClient,
    container_name: str,
    blob_name: str,
    sas_token: str,
) -> str:
    """Builds a signed URL for a blob

    :param blob_service_client: The blob service client
    :param container_name: The name of the blob container
    :param blob_name: The name of the blob
    :param sas_token: An SAS token
    """
    base_url = str(blob_service_client.url)
    if not base_url.endswith("/"):
        base_url += "/"

    return f"{base_url}{container_name}/{blob_name}?{sas_token}"


def upload_file_to_container(
        blob_service_client: BlobServiceClient,
        container_name: str,
        file_path: str,
        path_basename: bool = True
) -> ResourceFile:
    """
    Uploads a local file to an Azure Blob storage container.

    :param blob_service_client: A blob service client.
    :type blob_service_client: `azure.storage.blob.BlockBlobService`
    :param str container_name: The name of the Azure Blob storage container.
    :param str file_path: The local path to the file.
    :rtype: `azure.batch.models.ResourceFile`
    :return: A ResourceFile initialized with a SAS URL appropriate for Batch
    tasks.
    """
    blob_name = os.path.basename(file_path) if path_basename else file_path
    blob_client = blob_service_client.get_blob_client(
        container=container_name,
        blob=blob_name,
    )
    print(f'Uploading file {file_path} to container [{container_name}]...')

    path_2, ext_2 = os.path.splitext(file_path)
    path_1, ext_1 = os.path.splitext(path_2)

    if ext_2 in ['.json', '.csv', '.xlsx']:
        pass
    elif ext_1 != '.tar' and ext_2 != '.gz':
        raise TypeError(
            f'Core and Task resource files must be of type "*.tar.gz" or "*.json". You entered {ext_1}{ext_2}.'
        )

    with open(file_path, "rb") as data:
        blob_client.upload_blob(
            data=data,
            overwrite=True,
        )

    sas_url = build_blob_sas_url(
        blob_service_client=blob_service_client,
        container_name=container_name,
        blob_name=blob_name,
        sas_token=_create_blob_sas_token(
            blob_service_client=blob_service_client,
            container_name=container_name,
            blob_name=blob_name,
            permission=BlobSasPermissions(read=True),
        ),
    )

    resource_file = ResourceFile(
        http_url=sas_url,
        file_path=blob_name,
    )
    return resource_file


def get_resource_file_from_blob(
        blob_service_client: BlobServiceClient,
        container_name: str,
        blob_name: str,
) -> ResourceFile:
    try:
        sas_url = build_blob_sas_url(
            blob_service_client=blob_service_client,
            container_name=container_name,
            blob_name=blob_name,
            sas_token=_create_blob_sas_token(
                blob_service_client=blob_service_client,
                container_name=container_name,
                blob_name=blob_name,
                permission=BlobSasPermissions(read=True),
                expiry=datetime.datetime.utcnow() + datetime.timedelta(
                    hours=config.STORAGE_ACCOUNT_SAS_TOKEN_TIMEOUT_HOURS),
            ),
        )

        resource_file = ResourceFile(
            file_path=blob_name,
            http_url=sas_url,
        )
        return resource_file

    except BatchErrorException as err:
        print_batch_exception(err)
        raise


def get_container_sas_token(
        blob_service_client: BlobServiceClient,
        container_name: str,
        container_permissions: ContainerSasPermissions = ContainerSasPermissions(
            read=True),
) -> str:
    """
    Obtains a shared access signature granting the specified permissions to the
    container.

    :param blob_service_client: A blob service client.
    :type blob_service_client: `azure.storage.blob.BlockBlobService`
    :param str container_name: The name of the Azure Blob storage container.
    :param ContainerSasPermissions container_permissions:
    :rtype: str
    :return: A SAS token granting the specified permissions to the container.
    """
    # Obtain the SAS token for the container, setting the expiry time and
    # permissions. In this case, no start time is specified, so the shared
    # access signature becomes valid immediately. Default expiration is in 1 week.
    container_sas_token = generate_container_sas(
        account_name=blob_service_client.account_name,
        account_key=blob_service_client.credential.account_key,
        container_name=container_name,
        permission=container_permissions,
        expiry=datetime.datetime.utcnow(
        ) + datetime.timedelta(hours=config.STORAGE_ACCOUNT_SAS_TOKEN_TIMEOUT_HOURS),
    )
    return container_sas_token


def get_container_sas_url(
        blob_service_client: BlobServiceClient,
        container_name: str,
        container_permissions: ContainerSasPermissions = ContainerSasPermissions(
            write=True),
) -> str:
    """
    Obtains a shared access signature URL that provides write access to the
    output container to which the tasks will upload their output.

    :param blob_service_client: A blob service client.
    :type blob_service_client: `azure.storage.blob.BlockBlobService`
    :param str container_name: The name of the Azure Blob storage container.
    :param ContainerSasPermissions container_permissions:
    :rtype: str
    :return: A SAS URL granting the specified permissions to the container.
    """
    # Obtain the SAS token for the container.
    sas_token = get_container_sas_token(
        blob_service_client=blob_service_client,
        container_name=container_name,
        container_permissions=container_permissions,
    )

    # Construct SAS URL for the container
    container_sas_url = f'https://{config.STORAGE_ACCOUNT_NAME}.blob.core.windows.net/{container_name}?{sas_token}'

    return container_sas_url


def cleanup_blob_container(blob_service_client: BlobServiceClient) -> None:
    for container in blob_service_client.list_containers():
        blob_service_client.delete_container(container_name=container.name)
