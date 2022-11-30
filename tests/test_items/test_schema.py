import pytest
from requests_mock import Mocker

from rapid import Rapid
from rapid.items.schema import Schema, SchemaMetadata, Column, Owner, SensitivityLevel
from rapid.exceptions import (
    SchemaAlreadyExistsException,
    SchemaCreateFailedException,
    SchemaInitialisationException,
)

from tests.conftest import RAPID_URL


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

DUMMY_COLUMNS_TWO = [
    Column(
        name="column_c",
        partition_index=None,
        data_type="Float64",
        allow_null=True,
        format=None,
    )
]


DUMMY_METADATA = SchemaMetadata(
    "test", "rapid_sdk", SensitivityLevel.PUBLIC, [Owner("Test", "test@email.com")]
)


@pytest.fixture
def schema():
    return Schema(DUMMY_METADATA, DUMMY_COLUMNS)


class TestSchema:
    def test_format_columns_when_dict(self):
        _input = [
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

        schema = Schema(None, _input)
        assert schema.columns == expected

    def test_format_columns_when_columns(self):
        schema = Schema(None, DUMMY_COLUMNS)
        assert schema.columns == DUMMY_COLUMNS

    def test_format_columns_when_neither(self):
        _input = [
            1234,
            1234,
        ]

        with pytest.raises(SchemaInitialisationException):
            Schema(None, _input)

    def test_reset_columns(self):
        schema = Schema(None, DUMMY_COLUMNS)
        schema.set_columns(DUMMY_COLUMNS_TWO)
        assert schema.columns == DUMMY_COLUMNS_TWO

    def test_are_columns_the_same_passes(self):
        schema = Schema(None, DUMMY_COLUMNS)
        same = schema.are_columns_the_same(DUMMY_COLUMNS)
        assert same is True

    def test_are_columns_the_same_fails(self):
        schema = Schema(None, DUMMY_COLUMNS)
        same = schema.are_columns_the_same(DUMMY_COLUMNS_TWO)
        assert same is False

    def test_create_success(self, requests_mock: Mocker, rapid: Rapid, schema: Schema):
        requests_mock.post(f"{RAPID_URL}/schema")
        schema.create(rapid)
        assert requests_mock.call_count == 1
        assert requests_mock.last_request.json() == schema.to_dict()

    def test_create_schema_already_exists(
        self, requests_mock: Mocker, rapid: Rapid, schema: Schema
    ):
        requests_mock.post(f"{RAPID_URL}/schema", status_code=409)
        with pytest.raises(SchemaAlreadyExistsException):
            schema.create(rapid)
            assert requests_mock.call_count == 1
            assert requests_mock.last_request.json() == schema.to_dict()

    def test_create_failure(self, requests_mock: Mocker, rapid: Rapid, schema: Schema):
        requests_mock.post(
            f"{RAPID_URL}/schema", status_code=500, json={"error": "create failed"}
        )
        with pytest.raises(SchemaCreateFailedException):
            schema.create(rapid)
            assert requests_mock.call_count == 1
            assert requests_mock.last_request.json() == schema.to_dict()
