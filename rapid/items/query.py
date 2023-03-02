from enum import Enum
from typing import Optional, List
from pydantic.main import BaseModel


class SortDirection(Enum):
    ASC = "ASC"
    DESC = "DESC"


class SQLQueryOrderBy(BaseModel):
    column: str
    direction: SortDirection = SortDirection("ASC")


class Query(BaseModel):
    select_columns: Optional[List[str]] = None
    filter: Optional[str] = None
    group_by_columns: Optional[List[str]] = None
    aggregation_conditions: Optional[str] = None
    order_by_columns: Optional[List[SQLQueryOrderBy]] = None
    limit: Optional[str] = None
