from datetime import datetime
import json
import time
import requests

from pandas import DataFrame

from rapid.auth import RapidAuth
from rapid.utils.constants import TIMEOUT_PERIOD
from rapid.exceptions import (
    DataFrameUploadFailedException,
    DataFrameUploadValidationException,
    JobFailedException,
    SchemaGenerationFailedException,
    UnableToFetchJobStatusException,
    DatasetInfoFailedException,
)


class Rapid:
    def __init__(self, auth: RapidAuth = None) -> None:
        """
        The rAPId class is the main SDK class forthe rAPId API. It acts as a wrapper for the various
        API endpoints, providing a simple and intuitive interface for developers to interact
        with the API.

        Attributes:
            auth: An instance of the rAPId auth class, which is used for authentication and authorization with the API.

        Methods:
            __init__: Initializes an instance of the rAPId class, with an instance of the  rAPId auth class.
        """
        self.auth = auth if auth else RapidAuth()

    def generate_headers(self):
        return {"Authorization": "Bearer " + self.auth.fetch_token()}

    def list_datasets(self):
        """
        Makes a POST request to the API to list the current datasets.

        Returns:
            A JSON response of the API's response.

            For more details on the response structure, see the API documentation:
            https://getrapid.link/docs#/Datasets/list_all_datasets_datasets_post
        """
        response = requests.post(
            f"{self.auth.url}/datasets",
            headers=self.generate_headers(),
            timeout=TIMEOUT_PERIOD,
        )
        return json.loads(response.content.decode("utf-8"))

    def fetch_job_progress(self, _id: str):
        """
        Makes a GET request to the API to fetch the progress of a specific job.

        Args:
            _id (str): The ID of the job to fetch the progress for.

        Returns:
            A JSON response of the API's response.

        For more details on the response structure, see the API documentation:
        https://getrapid.link/docs#/Jobs/get_job_jobs__job_id__get
        """
        url = f"{self.auth.url}/jobs/{_id}"
        response = requests.get(
            url, headers=self.generate_headers(), timeout=TIMEOUT_PERIOD
        )
        data = json.loads(response.content.decode("utf-8"))
        if response.status_code == 200:
            return data
        raise UnableToFetchJobStatusException("Could not check job status", data)

    def wait_for_job_outcome(self, _id: str, interval: int = 1):
        """
        Makes periodic requests to the API to wait for the outcome of a specific job.

        Args:
            _id (str): The ID of the job to wait for the outcome of.
            interval (int): The number of seconds to sleep between requests to the API.

        Returns:
            None if the job is successful, otherwise raises a JobFailedException.
        """
        while True:
            progress = self.fetch_job_progress(_id)
            status = progress["status"]
            if status == "SUCCESS":
                return None
            if status == "FAILED":
                raise JobFailedException("Upload failed", progress)
            time.sleep(interval)

    def upload_dataframe(self, domain, dataset, df, wait_to_complete: bool = True):
        """
        Uploads a pandas DataFrame to a specified dataset in the API.

        Args:
            domain (str): The domain of the dataset to upload the DataFrame to.
            dataset (str): The name of the dataset to upload the DataFrame to.
            df (DataFrame): The pandas DataFrame to upload.
            wait_to_complete (bool, optional): Whether to wait for the upload job to
            complete before returning. Defaults to True.

        Returns:
            If wait_to_complete is True, returns "Success" if the upload is successful,
            otherwise raises an appropriate exception.
            If wait_to_complete is False, returns the ID of the upload job if the upload is
            accepted, otherwise raises an appropriate exception.

        Raises:
            :class:`rapid.exceptions.DataFrameUploadValidationException`: If the DataFrame's schema is incorrect.
            :class:`rapid.exceptions.DataFrameUploadFailedException`: If an unexpected error occurs while uploading the DataFrame.
        """
        url = f"{self.auth.url}/datasets/{domain}/{dataset}"
        response = requests.post(
            url,
            headers=self.generate_headers(),
            files=self.convert_dataframe_for_file_upload(df),
            timeout=TIMEOUT_PERIOD,
        )
        data = json.loads(response.content.decode("utf-8"))

        if response.status_code == 202:
            if wait_to_complete:
                self.wait_for_job_outcome(data["details"]["job_id"])
                return "Success"
            return data["details"]["job_id"]
        if response.status_code == 422:
            raise DataFrameUploadValidationException(
                "Could not upload dataframe due to an incorrect schema definition"
            )

        raise DataFrameUploadFailedException(
            "Encountered an unexpected error, could not upload dataframe",
            data["details"],
        )

    def generate_info(self, df: DataFrame, domain: str, dataset: str):
        """
        Generates metadata information for a pandas DataFrame and a specified dataset in the API.

        Args:
            df (DataFrame): The pandas DataFrame to generate metadata for.
            domain (str): The domain of the dataset to generate metadata for.
            dataset (str): The name of the dataset to generate metadata for.

        Returns:
            A dictionary containing the metadata information for the DataFrame and dataset.

        Raises:
            :class:`rapid.exceptions.DatasetInfoFailedException`: If an error occurs while generating the metadata information.
        """
        url = f"{self.auth.url}/datasets/{domain}/{dataset}/info"
        response = requests.post(
            url,
            headers=self.generate_headers(),
            files=self.convert_dataframe_for_file_upload(df),
            timeout=TIMEOUT_PERIOD,
        )
        data = json.loads(response.content.decode("utf-8"))
        if response.status_code == 200:
            return data

        raise DatasetInfoFailedException(
            "Failed to gather the dataset info", data["details"]
        )

    def convert_dataframe_for_file_upload(self, df: DataFrame):
        """
        Converts a pandas DataFrame to a format that can be used for file uploads to the API.

        Args:
            df (DataFrame): The pandas DataFrame to convert.

        Returns:
            A dictionary containing the converted DataFrame in a format suitable for file uploads to the API.
        """
        return {
            "file": (
                f"rapid-sdk-{int(datetime.now().timestamp())}.csv",
                df.to_csv(index=False),
            )
        }

    def generate_schema(
        self, df: DataFrame, domain: str, dataset: str, sensitivity: str
    ):

        """
        Generates a schema for a pandas DataFrame and a specified dataset in the API.

        Args:
            df (DataFrame): The pandas DataFrame to generate a schema for.
            domain (str): The domain of the dataset to generate a schema for.
            dataset (str): The name of the dataset to generate a schema for.
            sensitivity (str): The sensitivity level of the schema to generate.

        Returns:
            A dictionary containing the generated schema for the DataFrame and dataset.

        Raises:
            :class:`rapid.exceptions.SchemaGenerationFailedException`: If an error occurs while generating the schema.
        """
        url = f"{self.auth.url}/schema/{sensitivity}/{domain}/{dataset}/generate"

        response = requests.post(
            url,
            headers=self.generate_headers(),
            files=self.convert_dataframe_for_file_upload(df),
            timeout=TIMEOUT_PERIOD,
        )
        data = json.loads(response.content.decode("utf-8"))
        if response.status_code == 200:
            return data
        raise SchemaGenerationFailedException("Could not create schema", data)
