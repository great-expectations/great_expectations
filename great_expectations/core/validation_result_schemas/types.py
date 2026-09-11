"""Type definitions for validation result schemas.

Defines the enumeration types, scalar aliases, and TypedDicts used across the
validation_result_schemas package.
"""

from __future__ import annotations

from enum import Enum
from typing import Dict, List, Optional, TypedDict, Union

from great_expectations.compatibility import pydantic

# Numeric fields in a result dict are floats in practice, but an exactly-integral
# value may arrive as an int.  Strict members accept each unchanged instead of
# normalising to one of them: this package reports the values it types and must
# never alter them, and pydantic v1's non-strict ``int``/``float`` would truncate
# 2.7 to 2 or widen 5 to 5.0 without a trace.
StrictNumber = Union[pydantic.StrictInt, pydantic.StrictFloat]


class Status(str, Enum):
    PARSED = "parsed"
    FAILED = "failed"
    UNSUPPORTED = "unsupported"
    """The data source cannot evaluate this expectation at all, by its own declaration.

    Recorded, not failed: the cell was never a candidate for a result dict, and the declaration
    that says so lives on the case with a reason.
    """


class RuntimeTypeName(str, Enum):
    NONE = "none"
    INT = "int"
    FLOAT = "float"
    STR = "str"
    BOOL = "bool"
    LIST = "list"
    DICT = "dict"
    DATAFRAME_PANDAS = "DataFrame"
    DATAFRAME_SPARK = "SparkDataFrame"
    OTHER = "other"


class CellCoordinates(TypedDict):
    expectation_type: str
    result_format: str  # ResultFormat enum value
    engine: str  # 'pandas' | 'spark' | 'sql'
    datasource_test_id: str


class Finding(TypedDict, total=False):
    expectation_type: str
    result_format: str
    engine: str
    datasource_test_id: str
    status: str  # Status enum value
    raw_field_set: List[str]
    raw_field_types: Dict[str, str]  # field name -> RuntimeTypeName value
    matched_variant: Optional[str]
    schema_fields_absent_from_result: List[str]
    schema_extras_rejected: List[str]
    error_summary: Optional[str]
