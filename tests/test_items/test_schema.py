import pytest

from rapid import Rapid
from rapid.items.schema import Schema, SchemaMetadata, Column, Owner, SensitivityLevel
from rapid.exceptions import (
    SchemaAlreadyExistsException,
    SchemaCreateFailedException,
    SchemaInitialisationException,
)

from tests.conftest import RAPID_URL
from requests_mock import Mocker


DUMMY_COLUMNS = [
    Column(
        name="column_a",
        partition_index=None,
        data_type="object",
        allow_null=True,
        format=None,
    ),
    Column(
        name="column_b",
        partition_index=None,
        data_type="object",
        allow_null=True,
        format=None,
    ),
]


DUMMY_METADATA = SchemaMetadata(
    "test", "rapid_sdk", SensitivityLevel.PUBLIC, [Owner("Test", "test@email.com")]
)


@pytest.fixture
def schema():
    return Schema(DUMMY_METADATA, DUMMY_COLUMNS)


class TestSchema:
    def test_format_columns_when_dict(self):
        input = [
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
        ]

        expected = DUMMY_COLUMNS

        schema = Schema(None, input)
        assert schema.columns == expected

    def test_format_columns_when_columns(self):
        schema = Schema(None, DUMMY_COLUMNS)
        assert schema.columns == DUMMY_COLUMNS

    def test_format_columns_when_neither(self):
        input = [
            1234,
            1234,
        ]

        with pytest.raises(SchemaInitialisationException):
            Schema(None, input)

    def test_create_success(self, requests_mock: Mocker, rapid: Rapid, schema: Schema):
        requests_mock.post(f"{RAPID_URL}/schema")
        schema.create(rapid)
        requests_mock.call_count == 1
        assert requests_mock.last_request.json() == schema.to_dict()

    def test_create_schema_already_exists(
        self, requests_mock: Mocker, rapid: Rapid, schema: Schema
    ):
        requests_mock.post(f"{RAPID_URL}/schema", status_code=409)
        with pytest.raises(SchemaAlreadyExistsException):
            schema.create(rapid)
            requests_mock.call_count == 1
            assert requests_mock.last_request.json() == schema.to_dict()

    def test_create_failure(self, requests_mock: Mocker, rapid: Rapid, schema: Schema):
        requests_mock.post(
            f"{RAPID_URL}/schema", status_code=500, json={"error": "create failed"}
        )
        with pytest.raises(SchemaCreateFailedException):
            schema.create(rapid)
            requests_mock.call_count == 1
            assert requests_mock.last_request.json() == schema.to_dict()
