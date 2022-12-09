from typing import Union, List
from pandas import DataFrame
from rapid.exceptions import (
    ColumnNotDifferentException,
    DataFrameUploadValidationException,
)
from rapid.items.schema import Schema, SchemaMetadata, Column
from rapid import Rapid


def upload_and_create_dataframe(
    rapid: Rapid, metadata: SchemaMetadata, df: DataFrame, upgrade_schema_on_fail=False
):
    """
    Generates a schema for a pandas DataFrame and a specified dataset in the API,
    creates the schema in the API, and uploads the DataFrame to the dataset.

    Args:
        rapid (Rapid): An instance of the rAPId SDK's main class.
        metadata (SchemaMetadata): The metadata for the schema to be created and the dataset
        to upload the DataFrame to.

        df (DataFrame): The pandas DataFrame to generate a schema for and upload to the dataset.
        upgrade_schema_on_fail (bool, optional): Whether to upgrade the schema if the
        DataFrame's schema is incorrect. Defaults to False.

    Raises:
        DataFrameUploadValidationException: If the DataFrame's schema is incorrect and
        upgrade_schema_on_fail is False.

        Exception: If an error occurs while generating the schema, creating the schema,
        or uploading the DataFrame.
    """
    schema = rapid.generate_schema(
        df, metadata.domain, metadata.dataset, metadata.sensitivity
    )
    try:
        Schema(metadata, schema["columns"]).create(rapid)
        rapid.upload_dataframe(metadata.domain, metadata.dataset, df)
    except DataFrameUploadValidationException as e:
        if upgrade_schema_on_fail:
            update_schema_dataframe(rapid, metadata, df, schema["columns"])
        else:
            raise e
    except Exception as e:
        raise e


def update_schema_dataframe(
    rapid: Rapid,
    metadata: SchemaMetadata,
    df: DataFrame,
    new_columns: Union[List[Column], List[dict]],
):
    """
    Updates a schema for a specified dataset in the API based on a pandas DataFrame.

    Args:
        rapid (Rapid): An instance of the rAPId SDK's main class.
        metadata (SchemaMetadata): The metadata for the schema to be updated
        and the dataset the DataFrame belongs to.
        df (DataFrame): The pandas DataFrame to generate updated schema columns from.
        new_columns (Union[List[Column], List[dict]]): The new schema columns
        to update the schema with.

    Raises:
        ColumnNotDifferentException: If the new schema columns are the same as
        the existing schema columns.

        Exception: If an error occurs while generating the schema information,
        updating the schema, or comparing the schema columns.
    """
    info = rapid.generate_info(df, metadata.domain, metadata.dataset)
    try:
        schema = Schema(metadata, info["columns"])
        if schema.are_columns_the_same(columns_b=new_columns):
            raise ColumnNotDifferentException

        schema.set_columns(new_columns)
        schema.update(rapid)
    except Exception as e:
        raise e
