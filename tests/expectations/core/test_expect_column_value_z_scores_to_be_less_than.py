import pandas as pd
import pytest

from great_expectations.core.expectation_validation_result import (
    ExpectationValidationResult,
)
from great_expectations.self_check.util import get_test_validator_with_data
from great_expectations.util import build_in_memory_runtime_context


@pytest.fixture
def z_score_validation_result():
    return ExpectationValidationResult(
        success=True,
        expectation_config={
            "expectation_type": "expect_column_value_z_scores_to_be_less_than",
            "kwargs": {
                "column": "a",
                "mostly": 0.9,
                "threshold": 4,
                "double_sided": True,
            },
            "meta": {},
        },
        result={
            "element_count": 6,
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


@pytest.mark.unit
def test_pandas_expect_column_value_z_scores_to_be_less_than_impl(
    z_score_validation_result,
):
    df = pd.DataFrame({"a": [1, 5, 22, 3, 5, 10]})

    context = build_in_memory_runtime_context()

    validator = get_test_validator_with_data(
        execution_engine="pandas",
        data=df,
        context=context,
    )

    result = validator.expect_column_value_z_scores_to_be_less_than(
        column="a", mostly=0.9, threshold=4, double_sided=True
    )

    assert result == z_score_validation_result


@pytest.mark.postgresql
def test_sa_expect_column_value_z_scores_to_be_less_than_impl(
    z_score_validation_result, test_backends
):
    if "postgresql" not in test_backends:
        pytest.skip("test_database_store_backend_get_url_for_key requires postgresql")

    df = pd.DataFrame({"a": [1, 5, 22, 3, 5, 10]})

    context = build_in_memory_runtime_context()

    validator = get_test_validator_with_data(
        execution_engine="postgresql",
        table_name="expect_column_value_z_scores_to_be_less_than_impl_1",
        data=df,
        context=context,
    )

    result = validator.expect_column_value_z_scores_to_be_less_than(
        column="a", mostly=0.9, threshold=4, double_sided=True
    )

    assert result == z_score_validation_result


# noinspection PyUnusedLocal
@pytest.mark.spark
def test_spark_expect_column_value_z_scores_to_be_less_than_impl(
    spark_session, basic_spark_df_execution_engine, z_score_validation_result
):
    df = pd.DataFrame({"a": [1, 5, 22, 3, 5, 10]})

    context = build_in_memory_runtime_context()

    validator = get_test_validator_with_data(
        execution_engine="spark",
        data=df,
        context=context,
    )

    result = validator.expect_column_value_z_scores_to_be_less_than(
        column="a", mostly=0.9, threshold=4, double_sided=True
    )

    assert result == z_score_validation_result
