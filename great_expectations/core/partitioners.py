from __future__ import annotations

import re
from typing import List, Literal, Tuple, Union

from great_expectations.compatibility import pydantic


class ColumnPartitionerYearly(pydantic.BaseModel):
    column_name: str
    sort_ascending: bool = True
    method_name: Literal["partition_on_year"] = "partition_on_year"


class ColumnPartitionerMonthly(pydantic.BaseModel):
    column_name: str
    sort_ascending: bool = True
    method_name: Literal["partition_on_year_and_month"] = "partition_on_year_and_month"


class ColumnPartitionerDaily(pydantic.BaseModel):
    column_name: str
    sort_ascending: bool = True
    method_name: Literal["partition_on_year_and_month_and_day"] = (
        "partition_on_year_and_month_and_day"
    )


class PartitionerDatetimePart(pydantic.BaseModel):
    datetime_parts: List[str]
    column_name: str
    sort_ascending: bool = True
    method_name: Literal["partition_on_date_parts"] = "partition_on_date_parts"


class PartitionerDividedInteger(pydantic.BaseModel):
    divisor: int
    column_name: str
    sort_ascending: bool = True
    method_name: Literal["partition_on_divided_integer"] = "partition_on_divided_integer"


class PartitionerModInteger(pydantic.BaseModel):
    mod: int
    column_name: str
    sort_ascending: bool = True
    method_name: Literal["partition_on_mod_integer"] = "partition_on_mod_integer"


class PartitionerColumnValue(pydantic.BaseModel):
    column_name: str
    sort_ascending: bool = True
    method_name: Literal["partition_on_column_value"] = "partition_on_column_value"


class PartitionerMultiColumnValue(pydantic.BaseModel):
    column_names: List[str]
    sort_ascending: bool = True
    method_name: Literal["partition_on_multi_column_values"] = "partition_on_multi_column_values"


class PartitionerConvertedDatetime(pydantic.BaseModel):
    column_name: str
    sort_ascending: bool = True
    method_name: Literal["partition_on_converted_datetime"] = "partition_on_converted_datetime"
    date_format_string: str


ColumnPartitioner = Union[
    PartitionerColumnValue,
    PartitionerMultiColumnValue,
    PartitionerDividedInteger,
    PartitionerModInteger,
    ColumnPartitionerYearly,
    ColumnPartitionerMonthly,
    ColumnPartitionerDaily,
    PartitionerDatetimePart,
    PartitionerConvertedDatetime,
]


class FileNamePartitionerYearly(pydantic.BaseModel):
    regex: re.Pattern
    param_names: Tuple[Literal["year"]] = ("year",)
    sort_ascending: bool = True


class FileNamePartitionerMonthly(pydantic.BaseModel):
    regex: re.Pattern
    param_names: Tuple[Literal["year"], Literal["month"]] = ("year", "month")
    sort_ascending: bool = True


class FileNamePartitionerDaily(pydantic.BaseModel):
    regex: re.Pattern
    param_names: Tuple[Literal["year"], Literal["month"], Literal["day"]] = ("year", "month", "day")
    sort_ascending: bool = True


class FileNamePartitionerPath(pydantic.BaseModel):
    regex: re.Pattern
    param_names: Tuple[()] = ()
    sort_ascending: bool = True


FileNamePartitioner = Union[
    FileNamePartitionerYearly,
    FileNamePartitionerMonthly,
    FileNamePartitionerDaily,
    FileNamePartitionerPath,
]
