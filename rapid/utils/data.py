from typing import Union, List
from pandas import DataFrame
from rapid.items.schema import Schema, SchemaMetadata, Column
from rapid import Rapid


def upload_and_create_dataframe(rapid: Rapid, metadata: SchemaMetadata, df: DataFrame):
    schema = rapid.generate_schema(
        df, metadata.domain, metadata.dataset, metadata.sensitivity
    )
    try:
        Schema(metadata, schema["columns"]).create(rapid)
    except Exception as e:
        raise(e)

    rapid.upload_dataframe(metadata.domain, metadata.dataset, df)


def update_schema_dataframe(
    rapid: Rapid,
    metadata: SchemaMetadata,
    df: DataFrame,
    new_columns: Union[List[Column], List[dict]],
):
    info = rapid.generate_info(df, metadata.domain, metadata.dataset)
    try:
        schema = Schema(metadata, info["columns"])
        schema.compare_columns(columns_b=new_columns)
        schema.set_columns(new_columns)
        schema.update(rapid)
    except Exception as e:
        raise(e)
