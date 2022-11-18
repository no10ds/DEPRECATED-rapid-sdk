from pandas import DataFrame

from rapid.items.schema import Schema, SchemaMetadata
from rapid import Rapid
from rapid.exceptions import SchemaAlreadyExistsException


def upload_and_create_dataframe(rapid: Rapid, metadata: SchemaMetadata, df: DataFrame):
    schema = rapid.generate_schema(
        df, metadata.domain, metadata.dataset, metadata.sensitivity
    )
    try:
        Schema(metadata, schema["columns"]).create(rapid)
    except SchemaAlreadyExistsException:
        pass

    rapid.upload_dataframe(metadata.domain, metadata.dataset, df)
