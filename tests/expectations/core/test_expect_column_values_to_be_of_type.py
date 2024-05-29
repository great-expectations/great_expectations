from unittest.mock import Mock

import pandas as pd
import pytest

from great_expectations.compatibility import aws
from great_expectations.compatibility.snowflake import snowflakesqlalchemy as sf
from great_expectations.core.expectation_validation_result import (
    ExpectationValidationResult,
)
from great_expectations.expectations.core import ExpectColumnValuesToBeOfType
from great_expectations.self_check.util import build_sa_validator_with_data
from great_expectations.util import is_library_loadable


@pytest.mark.skipif(
    not (aws.sqlalchemy_athena and is_library_loadable(library_name="pyathena")),
    reason="pyathena is not installed",
)
@pytest.mark.athena
@pytest.mark.external_sqldialect
def test_expect_column_values_to_be_of_type_string_dialect_pyathena(sa):
    df = pd.DataFrame({"col": ["test_val1", "test_val2"]})
    validator = build_sa_validator_with_data(
        df=df,
        sa_engine_name="sqlite",
        table_name="expect_column_values_to_be_of_type_string_dialect_pyathena_1",
    )

    # Monkey-patch dialect for testing purposes.
    validator.execution_engine.dialect_module = aws.sqlalchemy_athena

    result = validator.expect_column_values_to_be_of_type("col", type_="string")

    assert result == ExpectationValidationResult(
        success=True,
        expectation_config={
            "expectation_type": "expect_column_values_to_be_of_type",
            "kwargs": {
                "column": "col",
                "type_": "string",
            },
            "meta": {},
        },
        result={
            "element_count": 2,
            "unexpected_count": 0,
            "unexpected_percent": 0.0,
            "partial_unexpected_list": [],
            "missing_count": 0,
            "missing_percent": 0.0,
            "unexpected_percent_total": 0.0,
            "unexpected_percent_nonmissing": 0.0,
        },
        exception_info={
            "raised_exception": False,
            "exception_traceback": None,
            "exception_message": None,
        },
        meta={},
    )


@pytest.mark.sqlite
@pytest.mark.external_sqldialect
def test_expect_column_values_to_be_of_type_string_dialect_sqlite(sa):
    df = pd.DataFrame({"col": ["test_val1", "test_val2"]})
    validator = build_sa_validator_with_data(
        df=df,
        sa_engine_name="sqlite",
        table_name="expect_column_values_to_be_of_type_string_dialect_sqlite_1",
    )

    result = validator.expect_column_values_to_be_of_type("col", type_="TEXT")

    assert result == ExpectationValidationResult(
        success=True,
        expectation_config={
            "expectation_type": "expect_column_values_to_be_of_type",
            "kwargs": {
                "column": "col",
                "type_": "TEXT",
            },
            "meta": {},
        },
        result={
            "element_count": 2,
            "unexpected_count": 0,
            "unexpected_percent": 0.0,
            "partial_unexpected_list": [],
            "missing_count": 0,
            "missing_percent": 0.0,
            "unexpected_percent_total": 0.0,
            "unexpected_percent_nonmissing": 0.0,
        },
        exception_info={
            "raised_exception": False,
            "exception_traceback": None,
            "exception_message": None,
        },
        meta={},
    )


@pytest.mark.snowflake
@pytest.mark.parametrize(
    "actual_column_type_sf_py_class_name,expected_type,expected_success,expected_observed_value",
    [
        ("NUMBER", "NUMBER", True, "NUMBER"),
        ("BIGINT", "NUMBER", True, "NUMBER"),
        ("BINARY", "BINARY", True, "BINARY"),
        ("BOOLEAN", "BOOLEAN", True, "BOOLEAN"),
        ("CHAR", "TEXT", True, "TEXT"),
        ("DATE", "DATE", True, "DATE"),
        ("DATETIME", "TIMESTAMP_NTZ", True, "TIMESTAMP_NTZ"),
        ("DECIMAL", "NUMBER", True, "NUMBER"),
        ("FLOAT", "FLOAT", True, "FLOAT"),
        ("INT", "NUMBER", True, "NUMBER"),
        ("INTEGER", "NUMBER", True, "NUMBER"),
        ("REAL", "FLOAT", True, "FLOAT"),
        ("SMALLINT", "NUMBER", True, "NUMBER"),
        ("TIME", "TIME", True, "TIME"),
        ("TIMESTAMP", "TIMESTAMP_NTZ", True, "TIMESTAMP_NTZ"),
        ("VARCHAR", "TEXT", True, "TEXT"),
        ("ARRAY", "ARRAY", True, "ARRAY"),
        ("BYTEINT", "NUMBER", True, "NUMBER"),
        ("CHARACTER", "TEXT", True, "TEXT"),
        ("DEC", "NUMBER", True, "NUMBER"),
        ("DOUBLE", "FLOAT", True, "FLOAT"),
        ("FIXED", "NUMBER", True, "NUMBER"),
        ("GEOGRAPHY", "GEOGRAPHY", True, "GEOGRAPHY"),
        ("GEOMETRY", "GEOMETRY", True, "GEOMETRY"),
        ("NUMBER", "NUMBER", True, "NUMBER"),
        ("OBJECT", "OBJECT", True, "OBJECT"),
        ("STRING", "TEXT", True, "TEXT"),
        ("TEXT", "TEXT", True, "TEXT"),
        ("TIMESTAMP_LTZ", "TIMESTAMP_LTZ", True, "TIMESTAMP_LTZ"),
        ("TIMESTAMP_NTZ", "TIMESTAMP_NTZ", True, "TIMESTAMP_NTZ"),
        ("TIMESTAMP_TZ", "TIMESTAMP_TZ", True, "TIMESTAMP_TZ"),
        ("TINYINT", "NUMBER", True, "NUMBER"),
        ("VARBINARY", "BINARY", True, "BINARY"),
        ("VARIANT", "VARIANT", True, "VARIANT"),
        # known name mismatches between snowflake and sqlalchemy
        ("TEXT", "VARCHAR", False, "TEXT"),
        ("INTEGER", "INTEGER", False, "NUMBER"),
        ("DECIMAL", "DECIMAL", False, "NUMBER"),
        # case-insensitive check, ignore precision
        ("BOOLEAN", "BoolEAN", True, "BOOLEAN"),
        ("NUMBER", "number(42)", True, "NUMBER"),
        # unknown type
        ("FOO", "TEXT", False, "FOO"),
    ],
)
def test_expect_column_values_to_be_of_type_snowflake_types(
    actual_column_type_sf_py_class_name,
    expected_type,
    expected_success,
    expected_observed_value,
):
    try:
        actual_column_type = getattr(sf, actual_column_type_sf_py_class_name)()
    except AttributeError:  # handle unknown types
        actual_column_type = actual_column_type_sf_py_class_name

    x = ExpectColumnValuesToBeOfType()

    eng = Mock()
    eng.dialect_module = Mock()
    eng.dialect_module.__name__ = "snowflake.sqlalchemy.snowdialect"

    result = x._validate_sqlalchemy(
        actual_column_type=actual_column_type,
        expected_type=expected_type,
        execution_engine=eng,
    )

    assert result == {
        "success": expected_success,
        "result": {"observed_value": expected_observed_value},
    }
