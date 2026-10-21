import timeit
import unittest
from unittest.mock import PropertyMock

import pandas as pd
import pytest

from great_expectations.compatibility import aws
from great_expectations.core.expectation_validation_result import (
    ExpectationValidationResult,
)
from great_expectations.execution_engine.sqlalchemy_dialect import GXSqlDialect
from great_expectations.expectations import (
    ExpectColumnValuesToBeInSet,
)
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
            "type": "expect_column_values_to_be_of_type",
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
            "type": "expect_column_values_to_be_of_type",
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


@pytest.mark.sqlite
def test_delegates_to_compare_column_type_success(sa, mocker):
    """Verify the expectation calls compare_column_type and uses its return value."""
    df = pd.DataFrame({"str_col": ["a", "b", "c"]})
    validator = build_sa_validator_with_data(
        df=df, sa_engine_name="sqlite", table_name="of_type_wiring_success"
    )

    mock_compare = mocker.patch(
        "great_expectations.expectations.core.expect_column_values_to_be_of_type.compare_column_type",
        return_value=(True, "SENTINEL_TYPE"),
    )

    result = validator.expect_column_values_to_be_of_type("str_col", type_="TEXT")

    mock_compare.assert_called_once_with(validator.execution_engine, mocker.ANY, "TEXT")
    assert result.success is True
    assert result.result["observed_value"] == "SENTINEL_TYPE"


@pytest.mark.sqlite
def test_delegates_to_compare_column_type_failure(sa, mocker):
    """Verify a False return from compare_column_type propagates as expectation failure."""
    df = pd.DataFrame({"str_col": ["a", "b", "c"]})
    validator = build_sa_validator_with_data(
        df=df, sa_engine_name="sqlite", table_name="of_type_wiring_failure"
    )

    mock_compare = mocker.patch(
        "great_expectations.expectations.core.expect_column_values_to_be_of_type.compare_column_type",
        return_value=(False, "WHATEVER"),
    )

    result = validator.expect_column_values_to_be_of_type("str_col", type_="INTEGER")

    mock_compare.assert_called_once_with(validator.execution_engine, mocker.ANY, "INTEGER")
    assert result.success is False
    assert result.result["observed_value"] == "WHATEVER"


@pytest.mark.unit
def test_expect_column_values_to_be_in_set_render_performance():
    """
    This test prevents a historical bug in which rendering took ~10 seconds to render 400 items.
    """

    large_number = 150

    x = ExpectColumnValuesToBeInSet(
        column="foo_column_name", value_set=["foo" for _ in range(large_number)]
    )

    duration_s = timeit.timeit(x.render, number=1)
    assert duration_s < 1, f"Rendering took {duration_s} seconds"


@pytest.mark.unit
@pytest.mark.parametrize(
    "dialect_name",
    [
        GXSqlDialect.DATABRICKS,
        GXSqlDialect.POSTGRESQL,
        GXSqlDialect.SNOWFLAKE,
    ],
)
def test_expect_column_values_to_be_of_type_case_insensitivity(sa, dialect_name):
    df = pd.DataFrame(
        {
            "datetime_col": pd.Series(
                [pd.Timestamp("2023-01-01"), pd.Timestamp("2023-01-02")], dtype="datetime64[ns]"
            ),
            "int_col": pd.Series([1, 2, None], dtype=pd.Int64Dtype()),
            "string_col": pd.Series(["apple", "banana", "cherry"], dtype=pd.StringDtype()),
            "bool_col": pd.Series([True, False, None], dtype=pd.BooleanDtype()),
        }
    )
    validator = build_sa_validator_with_data(
        df=df,
        sa_engine_name="sqlite",  # Using sqlite as base, will mock dialect
        table_name="type_test_table_parametrized",
    )

    # Mock the dialect_name property to test different SQL dialects
    with unittest.mock.patch.object(
        type(validator.execution_engine),
        "dialect_name",
        new_callable=PropertyMock,
        return_value=dialect_name,
    ):
        column_test_configs = [
            {"col_name": "datetime_col", "canonical_type": "DATETIME", "wrong_type": "INTEGER"},
            {"col_name": "int_col", "canonical_type": "BIGINT", "wrong_type": "STRING"},
            # For SQLite, pandas StringDtype typically maps to TEXT
            {"col_name": "string_col", "canonical_type": "TEXT", "wrong_type": "BOOLEAN"},
            {"col_name": "bool_col", "canonical_type": "BOOLEAN", "wrong_type": "DATE"},
        ]

        capitalization_variations = [
            lambda t: t.lower(),
            lambda t: t.upper(),
            lambda t: t.capitalize(),
        ]

        for config in column_test_configs:
            col_name = config["col_name"]
            canonical_type = config["canonical_type"]
            wrong_type = config["wrong_type"]

            # Test correct types with different capitalizations
            for cap_func in capitalization_variations:
                type_str = cap_func(canonical_type)
                result = validator.expect_column_values_to_be_of_type(col_name, type_=type_str)
                assert result.success is True, (
                    f"Expected success=True for type='{type_str}' on column '{col_name}' "
                    f"with dialect '{dialect_name}', "
                    f"but got success={result.success}. "
                    f"Observed value: {result.result.get('observed_value')}"
                )

            # Test incorrect type
            result = validator.expect_column_values_to_be_of_type(col_name, type_=wrong_type)
            assert result.success is False, (
                f"Expected success=False for wrong_type='{wrong_type}' on column '{col_name}' "
                f"with dialect '{dialect_name}', "
                f"but got success={result.success}. "
                f"Observed value: {result.result.get('observed_value')}"
            )
