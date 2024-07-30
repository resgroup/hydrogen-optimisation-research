from itertools import islice
from typing import Tuple

from azure.batch.models import BatchErrorException, ImageReference, AccountListSupportedImagesOptions
from azure.batch import BatchServiceClient


def query_yes_no(question: str, default: str = "yes") -> str:
    """
    Prompts the user for yes/no input, displaying the specified question text.

    :param str question: The text of the prompt for input.
    :param str default: The default if the user hits <ENTER>. Acceptable values
    are 'yes', 'no', and None.
    :rtype: str
    :return: 'yes' or 'no'
    """
    valid = {'y': 'yes', 'n': 'no'}
    if default is None:
        prompt = ' [y/n] '
    elif default == 'yes':
        prompt = ' [Y/n] '
    elif default == 'no':
        prompt = ' [y/N] '
    else:
        raise ValueError("Invalid default answer: '{}'".format(default))

    while 1:
        choice = input(question + prompt).lower()
        if default and not choice:
            return default
        try:
            return valid[choice[0]]
        except (KeyError, IndexError):
            print("Please respond with 'yes' or 'no' (or 'y' or 'n').\n")


def print_batch_exception(batch_exception: BatchErrorException):
    """
    Prints the contents of the specified Batch exception.

    :param batch_exception:
    """
    print('-------------------------------------------')
    print('Exception encountered:')
    if batch_exception.error and \
            batch_exception.error.message and \
            batch_exception.error.message.value:
        print(batch_exception.error.message.value)
        if batch_exception.error.values:
            print()
            for mesg in batch_exception.error.values:
                print('{}:\t{}'.format(mesg.key, mesg.value))
    print('-------------------------------------------')


def chunk(it, size):
    it = iter(it)
    return iter(lambda: tuple(islice(it, size)), ())


def select_latest_verified_vm_image_with_node_agent_sku(
    batch_client: BatchServiceClient,
    publisher: str,
    offer: str,
    sku_starts_with: str,
) -> Tuple[str, ImageReference]:
    """Select the latest verified image that Azure Batch supports given
    a publisher, offer and sku (starts with filter).

    :param batch_client: The batch client to use.
    :param publisher: vm image publisher
    :param offer: vm image offer
    :param sku_starts_with: vm sku starts with filter
    :return: (node agent sku id to use, vm image ref to use)
    """
    # get verified vm image list and node agent sku ids from service
    options = AccountListSupportedImagesOptions(
        filter="verificationType eq 'verified'")
    images = batch_client.account.list_supported_images(
        account_list_supported_images_options=options)

    # pick the latest supported sku
    skus_to_use = [
        (image.node_agent_sku_id, image.image_reference) for image in images
        if image.image_reference.publisher.lower() == publisher.lower() and
        image.image_reference.offer.lower() == offer.lower() and
        image.image_reference.sku.startswith(sku_starts_with)
    ]

    # pick first
    agent_sku_id, image_ref_to_use = skus_to_use[0]
    return agent_sku_id, image_ref_to_use

