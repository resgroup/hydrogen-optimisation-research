from batch_submission import config
from batch_submission.utils import select_latest_verified_vm_image_with_node_agent_sku

from azure.batch import BatchServiceClient
import azure.batch.models as batchmodels


class VmSize:
    STANDARD_DS1_v2 = "STANDARD_DS1_V2"


def create_pool(batch_service_client: BatchServiceClient, pool_id: str, input_files: list, commands: list = [],
                node_count: int = 1) -> None:
    """
    Creates a pool of compute nodes with the specified OS settings.

    :param batch_service_client: A Batch service client.
    :type batch_service_client: `azure.batch.BatchServiceClient`
    :param str pool_id: An ID for the new pool.
    :param list input_files: List of Input Files
    :param list commands: List of cmd prompts
    """

    if batch_service_client.pool.exists(pool_id=pool_id):
        print(f'Pool [{pool_id}] already exists.')
    else:
        print(f'Creating pool [{pool_id}]...')

        # Create a new pool of Linux compute nodes using an Azure Virtual Machines
        # Marketplace image. For more information about creating pools of Linux
        # nodes, see:
        # https://azure.microsoft.com/documentation/articles/batch-linux-nodes/

        # The start task installs ffmpeg on each node from an available repository, using
        # an administrator user identity.

        startup_commands = [
            'df -lh',

            'apt-get update --fix-missing',
            'apt-get install -y wget bzip2 ca-certificates curl git acl',

            'rm -f ~/miniconda.sh',

            'wget --quiet https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda.sh',
            'sh ~/miniconda.sh -b -p /opt/miniconda',

            'echo "export PATH=/opt/miniconda/bin:$PATH" >> ~/.bashrc',
            'source ~/.bashrc',

            'rm -f /opt/miniconda/.condarc',
            'echo "envs_dirs:" >> /opt/miniconda/.condarc',
            'echo "  - /opt/miniconda/envs" >> /opt/miniconda/.condarc',
            'echo "pkgs_dirs:" >> /opt/miniconda/.condarc',
            'echo "  - /opt/miniconda/pkgs" >> /opt/miniconda/.condarc',

            'chmod -R g=u /opt/miniconda',
            'chmod -R o=u /opt/miniconda',

            'chmod g+s "/opt/miniconda/envs"',
            'chmod g+s "/opt/miniconda/pkgs"',
        ]

        cmd = "/bin/bash -c 'set -e; set -o pipefail; {}; wait'".format(
            ';'.join(startup_commands + commands))

        # # pick the latest supported 18.04 sku for UbuntuServer
        image_ref_to_use = batchmodels.ImageReference(
            publisher="Canonical",
            offer="UbuntuServer",
            sku="18.04-LTS",
            version="latest",
        )

        virtual_machine_config = batchmodels.VirtualMachineConfiguration(
            image_reference=image_ref_to_use,
            node_agent_sku_id="batch.node.ubuntu 18.04",
        )

        start_task = batchmodels.StartTask(
            command_line=cmd,
            wait_for_success=True,
            resource_files=input_files,
            user_identity=batchmodels.UserIdentity(
                auto_user=batchmodels.AutoUserSpecification(
                    scope=batchmodels.AutoUserScope.pool,
                    elevation_level=batchmodels.ElevationLevel.admin,
                ),
            ),
        )

        new_pool = batchmodels.PoolAddParameter(
            id=pool_id,
            vm_size=config.POOL_VM_SIZE,
            virtual_machine_configuration=virtual_machine_config,
            target_dedicated_nodes=config.DEDICATED_POOL_NODE_COUNT,
            target_low_priority_nodes=node_count,
            start_task=start_task,
        )

        batch_service_client.pool.add(pool=new_pool)
