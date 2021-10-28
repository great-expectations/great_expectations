import pandas as pd
import pytest

from great_expectations.core.expectation_validation_result import (
    ExpectationValidationResult,
)
from great_expectations.self_check.util import (
    build_pandas_validator_with_data,
    build_sa_validator_with_data,
    build_spark_validator_with_data,
)


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


def test_pandas_expect_column_value_z_scores_to_be_less_than_impl(
    z_score_validation_result,
):
    df = pd.DataFrame({"a": [1, 5, 22, 3, 5, 10]})

    validator = build_pandas_validator_with_data(df)

    result = validator.expect_column_value_z_scores_to_be_less_than(
        column="a", mostly=0.9, threshold=4, double_sided=True
    )

    assert result == z_score_validation_result


def test_sa_expect_column_value_z_scores_to_be_less_than_impl(
    z_score_validation_result, test_backends
):
    if "postgresql" not in test_backends:
        pytest.skip("test_database_store_backend_get_url_for_key requires postgresql")

    df = pd.DataFrame({"a": [1, 5, 22, 3, 5, 10]})

    validator = build_sa_validator_with_data(df=df, sa_engine_name="postgresql")

    result = validator.expect_column_value_z_scores_to_be_less_than(
        column="a", mostly=0.9, threshold=4, double_sided=True
    )

    assert result == z_score_validation_result


def test_spark_expect_column_value_z_scores_to_be_less_than_impl(
    spark_session, basic_spark_df_execution_engine, z_score_validation_result
):
    df = pd.DataFrame({"a": [1, 5, 22, 3, 5, 10]})

    validator = build_spark_validator_with_data(df, spark_session)

    result = validator.expect_column_value_z_scores_to_be_less_than(
        column="a", mostly=0.9, threshold=4, double_sided=True
    )

    assert result == z_score_validation_result
