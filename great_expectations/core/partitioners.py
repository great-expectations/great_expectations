from __future__ import annotations

import re
from typing import Final, List, Literal, Union

from great_expectations._docs_decorators import public_api
from great_expectations.compatibility import pydantic


@public_api
class PartitionerYear(pydantic.BaseModel):
    column_name: str
    sort_ascending: bool = True
    method_name: Literal["partition_on_year"] = "partition_on_year"
    param_names = ["year"]


@public_api
class PartitionerYearAndMonth(pydantic.BaseModel):
    column_name: str
    sort_ascending: bool = True
    method_name: Literal["partition_on_year_and_month"] = "partition_on_year_and_month"
    param_names = ["year", "month"]


@public_api
class PartitionerYearAndMonthAndDay(pydantic.BaseModel):
    column_name: str
    sort_ascending: bool = True
    method_name: Literal["partition_on_year_and_month_and_day"] = (
        "partition_on_year_and_month_and_day"
    )
    param_names = ["year", "month", "day"]


@public_api
class PartitionerDatetimePart(pydantic.BaseModel):
    datetime_parts: List[str]
    column_name: str
    sort_ascending: bool = True
    method_name: Literal["partition_on_date_parts"] = "partition_on_date_parts"
    param_names: List[str] = []


@public_api
class PartitionerDividedInteger(pydantic.BaseModel):
    divisor: int
    column_name: str
    sort_ascending: bool = True
    method_name: Literal["partition_on_divided_integer"] = "partition_on_divided_integer"
    param_names: List[str] = []


@public_api
class PartitionerModInteger(pydantic.BaseModel):
    mod: int
    column_name: str
    sort_ascending: bool = True
    method_name: Literal["partition_on_mod_integer"] = "partition_on_mod_integer"
    param_names: List[str] = []


@public_api
class PartitionerColumnValue(pydantic.BaseModel):
    column_name: str
    sort_ascending: bool = True
    method_name: Literal["partition_on_column_value"] = "partition_on_column_value"
    param_names: List[str] = []


@public_api
class PartitionerMultiColumnValue(pydantic.BaseModel):
    column_names: List[str]
    sort_ascending: bool = True
    method_name: Literal["partition_on_multi_column_values"] = "partition_on_multi_column_values"
    param_names: List[str] = []


@public_api
class PartitionerConvertedDatetime(pydantic.BaseModel):
    column_name: str
    sort_ascending: bool = True
    method_name: Literal["partition_on_converted_datetime"] = "partition_on_converted_datetime"
    date_format_string: str
    param_names: List[str] = []


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


class PartitionerYearly(pydantic.BaseModel):
    regex: re.Pattern
    param_names = ["year"]
    sort_ascending: bool = True


class PartitionerMonthly(pydantic.BaseModel):
    regex: re.Pattern
    param_names = ["year", "month"]
    sort_ascending: bool = True


class PartitionerDaily(pydantic.BaseModel):
    regex: re.Pattern
    param_names = ["year", "month", "day"]
    sort_ascending: bool = True


class PartitionerPath(pydantic.BaseModel):
    regex: re.Pattern
    param_names: Final[List[str]] = []
    sort_ascending: bool = True


RegexPartitioner = Union[PartitionerYearly, PartitionerMonthly, PartitionerDaily, PartitionerPath]
