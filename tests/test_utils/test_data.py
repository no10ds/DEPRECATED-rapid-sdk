from mock import Mock
from pandas import DataFrame
from requests_mock import Mocker

from rapid.items.schema import Owner, SchemaMetadata, SensitivityLevel
from rapid.utils.data import upload_and_create_dataframe
from rapid import Rapid
from tests.conftest import RAPID_URL


class TestUtils:
    def test_upload_and_create_dataframe(self, requests_mock: Mocker, rapid: Rapid):
        df = DataFrame(
            {
                "column_a": ["one", "two", "three"],
                "column_b": ["one", "two", "three"],
                "column_c": ["one", "two", "three"],
            }
        )
        metadata = SchemaMetadata(
            "test",
            "rapid_sdk",
            SensitivityLevel.PUBLIC,
            owners=[Owner("test", "test@email.com")],
        )
        mock_response = {
            "metadata": {
                "domain": "test",
                "dataset": "rapid_sdk",
                "sensitivity": "PUBLIC",
                "key_value_tags": {},
                "key_only_tags": [],
                "owners": [{"name": "change_me", "email": "change_me@email.com"}],
                "update_behaviour": "APPEND",
            },
            "columns": [
                {
                    "name": "column_a",
                    "partition_index": None,
                    "data_type": "object",
                    "allow_null": True,
                    "format": None,
                },
                {
                    "name": "column_b",
                    "partition_index": None,
                    "data_type": "object",
                    "allow_null": True,
                    "format": None,
                },
                {
                    "name": "column_c",
                    "partition_index": None,
                    "data_type": "object",
                    "allow_null": True,
                    "format": None,
                },
            ],
        }
        requests_mock.post(
            f"{RAPID_URL}/schema/{metadata.sensitivity}/{metadata.domain}"
            + f"/{metadata.dataset}/generate",
            json=mock_response,
        )
        requests_mock.post(
            f"{RAPID_URL}/schema",
        )
        rapid.upload_dataframe = Mock()
        upload_and_create_dataframe(rapid, metadata, df)

        rapid.upload_dataframe.assert_called_once_with(
            metadata.domain, metadata.dataset, df
        )
