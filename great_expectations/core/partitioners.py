from typing import List, Literal, Union

from great_expectations.compatibility import pydantic


class PartitionerYear(pydantic.BaseModel):
    column_name: str
    method_name: Literal["partition_on_year"] = "partition_on_year"


class PartitionerYearAndMonth(pydantic.BaseModel):
    column_name: str
    method_name: Literal["partition_on_year_and_month"] = "partition_on_year_and_month"


class PartitionerYearAndMonthAndDay(pydantic.BaseModel):
    column_name: str
    method_name: Literal[
        "partition_on_year_and_month_and_day"
    ] = "partition_on_year_and_month_and_day"


class PartitionerDatetimePart(pydantic.BaseModel):
    datetime_parts: List[str]
    column_name: str
    method_name: Literal["partition_on_date_parts"] = "partition_on_date_parts"


class PartitionerDividedInteger(pydantic.BaseModel):
    divisor: int
    column_name: str
    method_name: Literal[
        "partition_on_divided_integer"
    ] = "partition_on_divided_integer"


class PartitionerModInteger(pydantic.BaseModel):
    mod: int
    column_name: str
    method_name: Literal["partition_on_mod_integer"] = "partition_on_mod_integer"


class PartitionerColumnValue(pydantic.BaseModel):
    column_name: str
    method_name: Literal["partition_on_column_value"] = "partition_on_column_value"


class PartitionerMultiColumnValue(pydantic.BaseModel):
    column_names: List[str]
    method_name: Literal[
        "partition_on_multi_column_values"
    ] = "partition_on_multi_column_values"


class PartitionerConvertedDatetime(pydantic.BaseModel):
    column_name: str
    method_name: Literal[
        "partition_on_converted_datetime"
    ] = "partition_on_converted_datetime"
    date_format_string: str


class PartitionerHashedColumn(pydantic.BaseModel):
    column_name: str
    method_name: Literal["partition_on_hashed_column"] = "partition_on_hashed_column"
    hash_digits: int


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
    PartitionerHashedColumn,
]
