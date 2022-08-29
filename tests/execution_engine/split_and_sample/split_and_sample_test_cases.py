import datetime
from typing import List

import pytest

import great_expectations.exceptions as ge_exceptions
from great_expectations.execution_engine.split_and_sample.data_splitter import DatePart

SINGLE_DATE_PART_BATCH_IDENTIFIERS: List[pytest.param] = [
    pytest.param({"month": 10}, id="month_dict"),
    pytest.param("10-31-2018", id="dateutil parseable date string"),
    pytest.param(
        datetime.datetime(2018, 10, 31, 0, 0, 0),
        id="datetime",
    ),
    pytest.param(
        {"month": 10, "unsupported": 5},
        marks=pytest.mark.xfail(strict=True, raises=ge_exceptions.InvalidConfigError),
        id="month_dict, unsupported batch identifier",
    ),
    pytest.param(
        {"month": 11},
        marks=pytest.mark.xfail(strict=True),
        id="incorrect month_dict should fail",
    ),
    pytest.param(
        "not a real date",
        marks=pytest.mark.xfail(strict=True),
        id="non dateutil parseable date string",
    ),
    pytest.param(
        datetime.datetime(2018, 11, 30, 0, 0, 0),
        marks=pytest.mark.xfail(strict=True),
        id="incorrect datetime should fail",
    ),
]

SINGLE_DATE_PART_DATE_PARTS: List[pytest.param] = [
    pytest.param(
        [DatePart.MONTH],
        id="month_with_DatePart",
    ),
    pytest.param(
        ["month"],
        id="month_with_string_DatePart",
    ),
    pytest.param(
        ["Month"],
        id="month_with_string_mixed_case_DatePart",
    ),
    pytest.param(None, marks=pytest.mark.xfail(strict=True), id="date_parts=None"),
    pytest.param([], marks=pytest.mark.xfail(strict=True), id="date_parts=[]"),
    pytest.param(
        ["invalid"], marks=pytest.mark.xfail(strict=True), id="invalid date_parts"
    ),
    pytest.param(
        "invalid",
        marks=pytest.mark.xfail(strict=True),
        id="invalid date_parts (not a list)",
    ),
    pytest.param(
        "month",
        marks=pytest.mark.xfail(strict=True),
        id="invalid date_parts (not a list but valid str)",
    ),
    pytest.param(
        DatePart.MONTH,
        marks=pytest.mark.xfail(strict=True),
        id="invalid date_parts (not a list but valid DatePart)",
    ),
]


MULTIPLE_DATE_PART_BATCH_IDENTIFIERS: List[pytest.param] = [
    pytest.param({"year": 2018, "month": 10}, id="year_and_month_dict"),
    pytest.param("10-31-2018", id="dateutil parseable date string"),
    pytest.param(
        datetime.datetime(2018, 10, 30, 0, 0, 0),
        id="datetime",
    ),
    pytest.param(
        {"year": 2018, "month": 10, "unsupported": 5},
        marks=pytest.mark.xfail(strict=True, raises=ge_exceptions.InvalidConfigError),
        id="year_and_month_dict, unsupported batch identifier",
    ),
    pytest.param(
        {"year": 2021, "month": 10},
        marks=pytest.mark.xfail(strict=True),
        id="incorrect year in year_and_month_dict should fail",
    ),
    pytest.param(
        {"year": 2018, "month": 11},
        marks=pytest.mark.xfail(strict=True),
        id="incorrect month in year_and_month_dict should fail",
    ),
    pytest.param(
        "not a real date",
        marks=pytest.mark.xfail(strict=True),
        id="non dateutil parseable date string",
    ),
    pytest.param(
        datetime.datetime(2018, 11, 30, 0, 0, 0),
        marks=pytest.mark.xfail(strict=True),
        id="incorrect datetime should fail",
    ),
]

MULTIPLE_DATE_PART_DATE_PARTS: List[pytest.param] = [
    pytest.param(
        [DatePart.YEAR, DatePart.MONTH],
        id="year_month_with_DatePart",
    ),
    pytest.param(
        [DatePart.YEAR, "month"],
        id="year_month_with_mixed_DatePart",
    ),
    pytest.param(
        ["year", "month"],
        id="year_month_with_string_DatePart",
    ),
    pytest.param(
        ["YEAR", "Month"],
        id="year_month_with_string_mixed_case_DatePart",
    ),
    pytest.param(None, marks=pytest.mark.xfail(strict=True), id="date_parts=None"),
    pytest.param([], marks=pytest.mark.xfail(strict=True), id="date_parts=[]"),
    pytest.param(
        ["invalid", "date", "parts"],
        marks=pytest.mark.xfail(strict=True),
        id="invalid date_parts",
    ),
]
