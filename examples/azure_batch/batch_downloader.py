import json
import pandas as pd
from azure.storage.blob import ContainerClient

from batch_submission.batch_submission import BatchSubmission


class BatchDownloader:

    BLOB_SERVICE_CLIENT = BatchSubmission().blob_service_client

    def __init__(self, analysis_name: str):

        self.analysis_name = analysis_name
        self.container_client: ContainerClient = self._get_container_client()

    def _get_container_client(self) -> ContainerClient:
        return self.BLOB_SERVICE_CLIENT.get_container_client(
            container=self.analysis_name,
        )

    def _list_blob_names(self, target_filename: str) -> list:
        generator = self.container_client.list_blobs()
        return [item.name for item in generator if target_filename in item.name]

    def download_results(self, combinations: list) -> None:
        df = pd.DataFrame()
        df_all = pd.DataFrame()
        blob_names = self._list_blob_names(target_filename='result.json')

        for combination in combinations:
            config_string = str(combination)[1:-1].replace(",", "_").replace(" ", "")
            blob_name = f'{config_string}/lcoh2_result.json'
            if blob_name not in blob_names:
                blob_content = None
            else:
                blob_client = self.container_client.get_blob_client(blob=blob_name)
                blob_content = blob_client.download_blob()
                blob_content = json.loads(blob_content.readall())
                df = pd.json_normalize(blob_content)
                df_all = pd.concat([df_all, df])

        return df_all

