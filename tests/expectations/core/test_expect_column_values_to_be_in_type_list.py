import pandas as pd
import pytest

import great_expectations as gx
from great_expectations.compatibility import aws
from great_expectations.core.expectation_validation_result import (
    ExpectationValidationResult,
)
from great_expectations.self_check.util import (
    build_sa_validator_with_data,
    get_test_validator_with_data,
)
from great_expectations.util import is_library_loadable


@pytest.mark.skipif(
    not (aws.sqlalchemy_athena and is_library_loadable(library_name="pyathena")),
    reason="pyathena is not installed",
)
@pytest.mark.athena
@pytest.mark.external_sqldialect
def test_expect_column_values_to_be_in_type_list_dialect_pyathena_string(sa):
    df = pd.DataFrame({"col": ["test_val1", "test_val2"]})
    validator = build_sa_validator_with_data(
        df=df,
        sa_engine_name="sqlite",
        table_name="expect_column_values_to_be_in_type_list_dialect_pyathena_string_1",
    )

    # Monkey-patch dialect for testing purposes.
    validator.execution_engine.dialect_module = aws.sqlalchemy_athena

    result = validator.expect_column_values_to_be_in_type_list(
        "col", type_list=["string", "boolean"]
    )

    assert result == ExpectationValidationResult(
        success=True,
        expectation_config={
            "type": "expect_column_values_to_be_in_type_list",
            "kwargs": {
                "column": "col",
                "type_list": ["string", "boolean"],
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


@pytest.mark.skipif(
    not (aws.sqlalchemy_athena and is_library_loadable(library_name="pyathena")),
    reason="pyathena is not installed",
)
@pytest.mark.athena
@pytest.mark.external_sqldialect
def test_expect_column_values_to_be_in_type_list_dialect_pyathena_boolean(sa):
    df = pd.DataFrame({"col": [True, False]})
    validator = build_sa_validator_with_data(
        df=df,
        sa_engine_name="sqlite",
        table_name="expect_column_values_to_be_in_type_list_dialect_pyathena_boolean_1",
    )

    # Monkey-patch dialect for testing purposes.
    validator.execution_engine.dialect_module = aws.sqlalchemy_athena

    result = validator.expect_column_values_to_be_in_type_list(
        "col", type_list=["string", "boolean"]
    )

    assert result == ExpectationValidationResult(
        success=True,
        expectation_config={
            "type": "expect_column_values_to_be_in_type_list",
            "kwargs": {
                "column": "col",
                "type_list": ["string", "boolean"],
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


@pytest.mark.big
def test_expect_column_values_to_be_in_type_list_nullable_int():
    from packaging.version import parse

    pandas_version = parse(pd.__version__)
    if pandas_version < parse("0.24"):
        # Prior to 0.24, Pandas did not have
        pytest.skip("Prior to 0.24, Pandas did not have `Int32Dtype` or related.")

    df = pd.DataFrame({"col": pd.Series([1, 2, None], dtype=pd.Int32Dtype())})

    context = gx.get_context(mode="ephemeral")

    validator = get_test_validator_with_data(
        execution_engine="pandas",
        data=df,
        context=context,
    )

    result = validator.expect_column_values_to_be_in_type_list("col", type_list=["Int32Dtype"])
    assert result == ExpectationValidationResult(
        success=True,
        expectation_config={
            "type": "expect_column_values_to_be_in_type_list",
            "kwargs": {
                "column": "col",
                "type_list": ["Int32Dtype"],
            },
            "meta": {},
        },
        result={
            "element_count": 3,
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


# ---------------------------------------------------------------------------
# Wiring tests: verify the expectation delegates to compare_column_type_list
# ---------------------------------------------------------------------------


@pytest.mark.sqlite
def test_delegates_to_compare_column_type_list_success(sa, mocker):
    """Verify the expectation calls compare_column_type_list and uses its return value."""
    df = pd.DataFrame({"str_col": ["a", "b", "c"]})
    validator = build_sa_validator_with_data(
        df=df, sa_engine_name="sqlite", table_name="in_type_list_wiring_success"
    )

    mock_compare = mocker.patch(
        "great_expectations.expectations.core.expect_column_values_to_be_in_type_list.compare_column_type_list",
        return_value=(True, "SENTINEL_TYPE"),
    )

    result = validator.expect_column_values_to_be_in_type_list(
        "str_col", type_list=["INTEGER", "TEXT"]
    )

    mock_compare.assert_called_once_with(
        validator.execution_engine, mocker.ANY, ["INTEGER", "TEXT"]
    )
    assert result.success is True
    assert result.result["observed_value"] == "SENTINEL_TYPE"


@pytest.mark.sqlite
def test_delegates_to_compare_column_type_list_failure(sa, mocker):
    """Verify a False return from compare_column_type_list propagates as expectation failure."""
    df = pd.DataFrame({"str_col": ["a", "b", "c"]})
    validator = build_sa_validator_with_data(
        df=df, sa_engine_name="sqlite", table_name="in_type_list_wiring_failure"
    )

    mock_compare = mocker.patch(
        "great_expectations.expectations.core.expect_column_values_to_be_in_type_list.compare_column_type_list",
        return_value=(False, "WHATEVER"),
    )

    result = validator.expect_column_values_to_be_in_type_list(
        "str_col", type_list=["INTEGER", "FLOAT"]
    )

    mock_compare.assert_called_once_with(
        validator.execution_engine, mocker.ANY, ["INTEGER", "FLOAT"]
    )
    assert result.success is False
    assert result.result["observed_value"] == "WHATEVER"
