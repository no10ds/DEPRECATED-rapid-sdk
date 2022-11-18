from datetime import datetime
import json
import time
import requests

from pandas import DataFrame

from rapid.auth import RapidAuth
from rapid.exceptions import (
    DataFrameUploadFailedException,
    JobFailedException,
    SchemaGenerationFailedException,
    UnableToFetchJobStatusException,
)


class Rapid:
    def __init__(self, auth: RapidAuth = None) -> None:
        self.auth = auth if auth else RapidAuth()

    def generate_headers(self):
        return {"Authorization": "Bearer " + self.auth.fetch_token()}

    def list_datasets(self):
        response = requests.post(
            f"{self.auth.url}/datasets", headers=self.generate_headers()
        )
        return json.loads(response.content.decode("utf-8"))

    def fetch_job_progress(self, _id: str):
        url = f"{self.auth.url}/jobs/{_id}"
        response = requests.get(
            url,
            headers=self.generate_headers(),
        )
        data = json.loads(response.content.decode("utf-8"))
        if response.status_code == 200:
            return data
        raise UnableToFetchJobStatusException("Could not check job status", data)

    def wait_for_job_outcome(self, _id: str, interval: int = 1):
        while True:
            progress = self.fetch_job_progress(_id)
            status = progress["status"]
            if status == "SUCCESS":
                return None
            if status == "FAILED":
                raise JobFailedException("Upload failed", progress)
            time.sleep(interval)

    def upload_dataframe(self, domain, dataset, df, wait_to_complete: bool = True):
        url = f"{self.auth.url}/datasets/{domain}/{dataset}"
        response = requests.post(
            url,
            headers=self.generate_headers(),
            files=self.convert_dataframe_for_file_upload(df),
        )
        data = json.loads(response.content.decode("utf-8"))

        if response.status_code == 202:
            if wait_to_complete:
                self.wait_for_job_outcome(data["details"]["job_id"])
                return "Success"
            return data["details"]["job_id"]
        raise DataFrameUploadFailedException(
            "Encountered an unexpected error, could not upload dataframe",
            data["details"],
        )

    def convert_dataframe_for_file_upload(self, df: DataFrame):
        return {
            "file": (
                f"rapid-sdk-{int(datetime.now().timestamp())}.csv",
                df.to_csv(index=False),
            )
        }

    def generate_schema(
        self, df: DataFrame, domain: str, dataset: str, sensitivity: str
    ):
        url = f"{self.auth.url}/schema/{sensitivity}/{domain}/{dataset}/generate"

        response = requests.post(
            url,
            headers=self.generate_headers(),
            files=self.convert_dataframe_for_file_upload(df),
        )
        data = json.loads(response.content.decode("utf-8"))
        if response.status_code == 200:
            return data
        raise SchemaGenerationFailedException("Could not create schema", data)
