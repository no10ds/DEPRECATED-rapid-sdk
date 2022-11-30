from dataclasses import dataclass
from enum import Enum
import json
from typing import Dict, List, Optional, Union
from deepdiff import DeepDiff
import requests

from rapid import Rapid
from rapid.utils.constants import TIMEOUT_PERIOD
from rapid.exceptions import (
    SchemaAlreadyExistsException,
    SchemaCreateFailedException,
    SchemaInitialisationException,
)


class SensitivityLevel(Enum):
    PUBLIC = "PUBLIC"
    PRIVATE = "PRIVATE"
    PROTECTED = "PROTECTED"


class UpdateBehaviour(Enum):
    APPEND = "APPEND"
    OVERWRITE = "OVERWRITE"


@dataclass
class Column:
    name: str
    data_type: str
    partition_index: Optional[int] = None
    allow_null: bool = True
    format: Optional[str] = None

    def to_dict(self):
        return {
            "name": self.name,
            "data_type": self.data_type,
            "partition_index": self.partition_index,
            "allow_null": self.allow_null,
            "format": self.format,
        }


@dataclass
class Owner:
    name: str
    email: str

    def to_dict(self):
        return {"name": self.name, "email": self.email}


@dataclass
class SchemaMetadata:
    domain: str
    dataset: str
    _sensitivity: SensitivityLevel
    owners: List[Owner]
    version: Optional[int] = None
    key_value_tags: Optional[Dict[str, str]] = None
    key_only_tags: Optional[List[str]] = None

    @property
    def sensitivity(self) -> str:
        return self._sensitivity.value

    def to_dict(self):
        return {
            key: value
            for key, value in {
                "domain": self.domain,
                "dataset": self.dataset,
                "sensitivity": self.sensitivity,
                "version": self.version,
                "key_value_tags": self.key_value_tags,
                "key_only_tags": self.key_only_tags,
                "owners": [owner.to_dict() for owner in self.owners],
            }.items()
            if value
        }


class Schema:
    metadata: SchemaMetadata
    columns: List[Column]

    def __init__(
        self, metadata: SchemaMetadata, columns: Union[List[Column], List[dict]]
    ):
        self.metadata = metadata
        self.columns = self._format_columns(columns)

    def _format_columns(self, columns: Union[List[Column], List[dict]]) -> List[Column]:
        if all(isinstance(col, Column) for col in columns):
            return columns

        if all(isinstance(col, dict) for col in columns):
            return [
                Column(
                    name=col["name"],
                    partition_index=col["partition_index"],
                    data_type=col["data_type"],
                    allow_null=col["allow_null"],
                    format=col["format"],
                )
                for col in columns
            ]

        raise SchemaInitialisationException("The columns are not of the expected type")

    def set_columns(self, columns: List[Column]):
        self.columns = columns

    def to_dict(self):
        return {
            "metadata": self.metadata.to_dict(),
            "columns": [column.to_dict() for column in self.columns],
        }

    def are_columns_the_same(self, columns_b: Union[List[Column], List[dict]]):
        diff = DeepDiff(
            [x.to_dict() for x in self.columns],
            [x.to_dict() for x in self._format_columns(columns_b)],
            ignore_order=True,
        )
        if not diff:
            return True
        return False

    def create(self, rapid: Rapid):
        schema = self.to_dict()
        response = requests.post(
            f"{rapid.auth.url}/schema",
            data=json.dumps(schema),
            headers=rapid.generate_headers(),
            timeout=TIMEOUT_PERIOD,
        )
        if response.status_code == 200:
            pass
        elif response.status_code == 409:
            raise SchemaAlreadyExistsException("The schema already exists")
        else:
            data = json.loads(response.content.decode("utf-8"))
            raise SchemaCreateFailedException("Could not create the schema", data)

    def update(self, rapid: Rapid):
        schema = self.to_dict()
        requests.put(
            f"{rapid.auth.url}/schema",
            data=json.dumps(schema),
            headers=rapid.generate_headers(),
            timeout=TIMEOUT_PERIOD,
        )
