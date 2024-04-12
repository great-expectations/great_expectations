from typing import List, Literal, Union

from great_expectations._docs_decorators import public_api
from great_expectations.compatibility import pydantic


@public_api
class PartitionerYear(pydantic.BaseModel):
    column_name: str
    sort_ascending: bool = True
    method_name: Literal["partition_on_year"] = "partition_on_year"


@public_api
class PartitionerYearAndMonth(pydantic.BaseModel):
    column_name: str
    sort_ascending: bool = True
    method_name: Literal["partition_on_year_and_month"] = "partition_on_year_and_month"


@public_api
class PartitionerYearAndMonthAndDay(pydantic.BaseModel):
    column_name: str
    sort_ascending: bool = True
    method_name: Literal["partition_on_year_and_month_and_day"] = (
        "partition_on_year_and_month_and_day"
    )


@public_api
class PartitionerDatetimePart(pydantic.BaseModel):
    datetime_parts: List[str]
    column_name: str
    sort_ascending: bool = True
    method_name: Literal["partition_on_date_parts"] = "partition_on_date_parts"


@public_api
class PartitionerDividedInteger(pydantic.BaseModel):
    divisor: int
    column_name: str
    sort_ascending: bool = True
    method_name: Literal["partition_on_divided_integer"] = "partition_on_divided_integer"


@public_api
class PartitionerModInteger(pydantic.BaseModel):
    mod: int
    column_name: str
    sort_ascending: bool = True
    method_name: Literal["partition_on_mod_integer"] = "partition_on_mod_integer"


@public_api
class PartitionerColumnValue(pydantic.BaseModel):
    column_name: str
    sort_ascending: bool = True
    method_name: Literal["partition_on_column_value"] = "partition_on_column_value"


@public_api
class PartitionerMultiColumnValue(pydantic.BaseModel):
    column_names: List[str]
    sort_ascending: bool = True
    method_name: Literal["partition_on_multi_column_values"] = "partition_on_multi_column_values"


@public_api
class PartitionerConvertedDatetime(pydantic.BaseModel):
    column_name: str
    sort_ascending: bool = True
    method_name: Literal["partition_on_converted_datetime"] = "partition_on_converted_datetime"
    date_format_string: str


Partitioner = Union[
    PartitionerColumnValue,
    PartitionerMultiColumnValue,
    PartitionerDividedInteger,
    PartitionerModInteger,
    PartitionerYear,
    PartitionerYearAndMonth,
    PartitionerYearAndMonthAndDay,
    PartitionerDatetimePart,
    PartitionerConvertedDatetime,
]
