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


@pytest.mark.parametrize(
    "actual_column_type,expected_type,expected_success,expected_observed_value",
    [
        (sf.NUMBER(), "NUMBER", True, "NUMBER"),
        (sf.BIGINT(), "NUMBER", True, "NUMBER"),
        (sf.BINARY(), "BINARY", True, "BINARY"),
        (sf.BOOLEAN(), "BOOLEAN", True, "BOOLEAN"),
        (sf.CHAR(), "TEXT", True, "TEXT"),
        (sf.DATE(), "DATE", True, "DATE"),
        (sf.DATETIME(), "TIMESTAMP_NTZ", True, "TIMESTAMP_NTZ"),
        (sf.DECIMAL(), "NUMBER", True, "NUMBER"),
        (sf.FLOAT(), "FLOAT", True, "FLOAT"),
        (sf.INT(), "NUMBER", True, "NUMBER"),
        (sf.INTEGER(), "NUMBER", True, "NUMBER"),
        (sf.REAL(), "FLOAT", True, "FLOAT"),
        (sf.SMALLINT(), "NUMBER", True, "NUMBER"),
        (sf.TIME(), "TIME", True, "TIME"),
        (sf.TIMESTAMP(), "TIMESTAMP_NTZ", True, "TIMESTAMP_NTZ"),
        (sf.VARCHAR(), "TEXT", True, "TEXT"),
        (sf.ARRAY(), "ARRAY", True, "ARRAY"),
        (sf.BYTEINT(), "NUMBER", True, "NUMBER"),
        (sf.CHARACTER(), "TEXT", True, "TEXT"),
        (sf.DEC(), "NUMBER", True, "NUMBER"),
        (sf.DOUBLE(), "FLOAT", True, "FLOAT"),
        (sf.FIXED(), "NUMBER", True, "NUMBER"),
        (sf.GEOGRAPHY(), "GEOGRAPHY", True, "GEOGRAPHY"),
        (sf.GEOMETRY(), "GEOMETRY", True, "GEOMETRY"),
        (sf.NUMBER(), "NUMBER", True, "NUMBER"),
        (sf.OBJECT(), "OBJECT", True, "OBJECT"),
        (sf.STRING(), "TEXT", True, "TEXT"),
        (sf.TEXT(), "TEXT", True, "TEXT"),
        (sf.TIMESTAMP_LTZ(), "TIMESTAMP_LTZ", True, "TIMESTAMP_LTZ"),
        (sf.TIMESTAMP_NTZ(), "TIMESTAMP_NTZ", True, "TIMESTAMP_NTZ"),
        (sf.TIMESTAMP_TZ(), "TIMESTAMP_TZ", True, "TIMESTAMP_TZ"),
        (sf.TINYINT(), "NUMBER", True, "NUMBER"),
        (sf.VARBINARY(), "BINARY", True, "BINARY"),
        (sf.VARIANT(), "VARIANT", True, "VARIANT"),
        # known name mismatches between snowflake and sqlalchemy
        (sf.TEXT(), "VARCHAR", False, "TEXT"),
        (sf.INTEGER(), "INTEGER", False, "NUMBER"),
        (sf.DECIMAL(), "DECIMAL", False, "NUMBER"),
        # case-insensitive check, ignore precision
        (sf.BOOLEAN(), "BoolEAN", True, "BOOLEAN"),
        (sf.NUMBER(), "number(42)", True, "NUMBER"),
        # unknown type
        ("FOO", "TEXT", False, "FOO"),
    ],
)
@pytest.mark.unit
def test_expect_column_values_to_be_of_type_snowflake_types(
    actual_column_type, expected_type, expected_success, expected_observed_value
):
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
