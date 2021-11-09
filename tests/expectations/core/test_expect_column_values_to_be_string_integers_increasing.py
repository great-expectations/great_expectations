import pandas as pd
import pytest

from contrib.experimental.great_expectations_experimental.expectations.expect_column_values_to_be_string_integers_increasing import (
    ExpectColumnValuesToBeStringIntegersIncreasing,
)
from contrib.experimental.great_expectations_experimental.metrics.column_values_string_integers_increasing import (
    ColumnValuesStringIntegersIncreasing,
)
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.core.expectation_validation_result import (
    ExpectationValidationResult,
)
from great_expectations.data_context import DataContext
from great_expectations.exceptions.exceptions import MetricResolutionError
from great_expectations.self_check.util import (
    build_sa_validator_with_data,
    build_spark_validator_with_data,
)


def test_pandas_expect_column_values_to_be_string_integers_increasing_success(
    data_context_with_datasource_pandas_engine,
):
    context: DataContext = data_context_with_datasource_pandas_engine

    df = pd.DataFrame({"a": ["0", "1", "2", "3", "3", "9", "11"]})

    batch_request = RuntimeBatchRequest(
        datasource_name="my_datasource",
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name="my_data_asset",
        runtime_parameters={"batch_data": df},
        batch_identifiers={"default_identifier_name": "my_identifier"},
    )
    validator = context.get_validator(
        batch_request=batch_request,
        create_expectation_suite_with_name="test",
    )

    result = validator.expect_column_values_to_be_string_integers_increasing(
        column="a", strictly=False
    )

    assert result == ExpectationValidationResult(
        exception_info={
            "raised_exception": False,
            "exception_traceback": None,
            "exception_message": None,
        },
        expectation_config={
            "expectation_type": "expect_column_values_to_be_string_integers_increasing",
            "kwargs": {
                "column": "a",
                "strictly": False,
                "batch_id": "57175eeb4a8baa7ae63f44c6540eb559",
            },
            "meta": {},
            "ge_cloud_id": None,
        },
        meta={},
        result={"observed_value": [[True], [6]]},
        success=True,
    )


def test_pandas_strictly_expect_column_values_to_be_string_integers_increasing_success(
    data_context_with_datasource_pandas_engine,
):
    context: DataContext = data_context_with_datasource_pandas_engine

    df = pd.DataFrame({"a": ["0", "1", "2", "3", "4", "9", "11"]})

    batch_request = RuntimeBatchRequest(
        datasource_name="my_datasource",
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name="my_data_asset",
        runtime_parameters={"batch_data": df},
        batch_identifiers={"default_identifier_name": "my_identifier"},
    )
    validator = context.get_validator(
        batch_request=batch_request,
        create_expectation_suite_with_name="test",
    )

    result = validator.expect_column_values_to_be_string_integers_increasing(
        column="a", strictly=True
    )

    assert result == ExpectationValidationResult(
        exception_info={
            "raised_exception": False,
            "exception_traceback": None,
            "exception_message": None,
        },
        expectation_config={
            "expectation_type": "expect_column_values_to_be_string_integers_increasing",
            "kwargs": {
                "column": "a",
                "strictly": True,
                "batch_id": "57175eeb4a8baa7ae63f44c6540eb559",
            },
            "meta": {},
            "ge_cloud_id": None,
        },
        meta={},
        result={"observed_value": [[True], [6]]},
        success=True,
    )


def test_pandas_expect_column_values_to_be_string_integers_increasing_type_failure(
    data_context_with_datasource_pandas_engine,
):
    with pytest.raises(MetricResolutionError):
        context: DataContext = data_context_with_datasource_pandas_engine

        df = pd.DataFrame(
            {"a": ["1", "2", "3", "3", "0", "6", "2021-05-01", "test", 8, 9]}
        )

        batch_request = RuntimeBatchRequest(
            datasource_name="my_datasource",
            data_connector_name="default_runtime_data_connector_name",
            data_asset_name="my_data_asset",
            runtime_parameters={"batch_data": df},
            batch_identifiers={"default_identifier_name": "my_identifier"},
        )
        validator = context.get_validator(
            batch_request=batch_request,
            create_expectation_suite_with_name="test",
        )

        result = validator.expect_column_values_to_be_string_integers_increasing(
            column="a", strictly=False
        )


def test_pandas_expect_column_values_to_be_string_integers_increasing_failure(
    data_context_with_datasource_pandas_engine,
):
    context: DataContext = data_context_with_datasource_pandas_engine

    df = pd.DataFrame({"a": ["1", "2", "3", "3", "0", "6", "9"]})

    batch_request = RuntimeBatchRequest(
        datasource_name="my_datasource",
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name="my_data_asset",
        runtime_parameters={"batch_data": df},
        batch_identifiers={"default_identifier_name": "my_identifier"},
    )

    validator = context.get_validator(
        batch_request=batch_request,
        create_expectation_suite_with_name="test",
    )

    result = validator.expect_column_values_to_be_string_integers_increasing(
        column="a", strictly=False
    )

    assert result == ExpectationValidationResult(
        exception_info={
            "raised_exception": False,
            "exception_traceback": None,
            "exception_message": None,
        },
        expectation_config={
            "expectation_type": "expect_column_values_to_be_string_integers_increasing",
            "kwargs": {
                "column": "a",
                "strictly": False,
                "batch_id": "57175eeb4a8baa7ae63f44c6540eb559",
            },
            "meta": {},
            "ge_cloud_id": None,
        },
        meta={},
        result={"observed_value": [[False, True], [1, 5]]},
        success=False,
    )


def test_pandas_strictly_expect_column_values_to_be_string_integers_increasing_failure(
    data_context_with_datasource_pandas_engine,
):
    context: DataContext = data_context_with_datasource_pandas_engine

    df = pd.DataFrame({"a": ["1", "2", "3", "3", "4", "6", "9"]})

    batch_request = RuntimeBatchRequest(
        datasource_name="my_datasource",
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name="my_data_asset",
        runtime_parameters={"batch_data": df},
        batch_identifiers={"default_identifier_name": "my_identifier"},
    )
    validator = context.get_validator(
        batch_request=batch_request,
        create_expectation_suite_with_name="test",
    )

    result = validator.expect_column_values_to_be_string_integers_increasing(
        column="a", strictly=True
    )

    assert result == ExpectationValidationResult(
        exception_info={
            "raised_exception": False,
            "exception_traceback": None,
            "exception_message": None,
        },
        expectation_config={
            "expectation_type": "expect_column_values_to_be_string_integers_increasing",
            "kwargs": {
                "column": "a",
                "strictly": True,
                "batch_id": "57175eeb4a8baa7ae63f44c6540eb559",
            },
            "meta": {},
            "ge_cloud_id": None,
        },
        meta={},
        result={"observed_value": [[False, True], [1, 5]]},
        success=False,
    )


def test_spark_expect_column_values_to_be_string_integers_increasing_success(
    spark_session,
    basic_spark_df_execution_engine,
):
    df = pd.DataFrame({"a": ["0", "1", "2", "3", "3", "9", "11"]})

    validator = build_spark_validator_with_data(df, spark_session)

    result = validator.expect_column_values_to_be_string_integers_increasing(
        column="a", strictly=False
    )

    assert result == ExpectationValidationResult(
        exception_info={
            "raised_exception": False,
            "exception_traceback": None,
            "exception_message": None,
        },
        expectation_config={
            "expectation_type": "expect_column_values_to_be_string_integers_increasing",
            "meta": {},
            "ge_cloud_id": None,
            "kwargs": {
                "column": "a",
                "strictly": False,
                "batch_id": (),
            },
        },
        result={
            "observed_value": [[True], [6]],
        },
        meta={},
        success=True,
    )


def test_spark_strictly_expect_column_values_to_be_string_integers_increasing_success(
    spark_session,
    basic_spark_df_execution_engine,
):
    df = pd.DataFrame({"a": ["0", "1", "2", "3", "4", "9", "11"]})

    validator = build_spark_validator_with_data(df, spark_session)

    result = validator.expect_column_values_to_be_string_integers_increasing(
        column="a", strictly=True
    )

    assert result == ExpectationValidationResult(
        exception_info={
            "raised_exception": False,
            "exception_traceback": None,
            "exception_message": None,
        },
        expectation_config={
            "expectation_type": "expect_column_values_to_be_string_integers_increasing",
            "meta": {},
            "ge_cloud_id": None,
            "kwargs": {
                "column": "a",
                "strictly": True,
                "batch_id": (),
            },
        },
        result={
            "observed_value": [[True], [6]],
        },
        meta={},
        success=True,
    )


def test_spark_expect_column_values_to_be_string_integers_increasing_type_failure(
    spark_session,
    basic_spark_df_execution_engine,
):
    with pytest.raises(MetricResolutionError):

        df = pd.DataFrame(
            {"a": ["1", "2", "3", "3", "0", "6", "2021-05-01", "test", "8", "9"]}
        )

        validator = build_spark_validator_with_data(df, spark_session)

        result = validator.expect_column_values_to_be_string_integers_increasing(
            column="a", strictly=False
        )


def test_spark_expect_column_values_to_be_string_integers_increasing_failure(
    spark_session,
    basic_spark_df_execution_engine,
):
    df = pd.DataFrame({"a": ["0", "1", "2", "3", "2", "9", "11"]})

    validator = build_spark_validator_with_data(df, spark_session)

    result = validator.expect_column_values_to_be_string_integers_increasing(
        column="a", strictly=False
    )

    assert result == ExpectationValidationResult(
        exception_info={
            "raised_exception": False,
            "exception_traceback": None,
            "exception_message": None,
        },
        expectation_config={
            "expectation_type": "expect_column_values_to_be_string_integers_increasing",
            "meta": {},
            "ge_cloud_id": None,
            "kwargs": {
                "column": "a",
                "strictly": False,
                "batch_id": (),
            },
        },
        result={
            "observed_value": [[False, True], [1, 5]],
        },
        meta={},
        success=False,
    )


def test_spark_strictly_expect_column_values_to_be_string_integers_increasing_failure(
    spark_session,
    basic_spark_df_execution_engine,
):
    df = pd.DataFrame({"a": ["0", "1", "2", "3", "3", "9", "11"]})

    validator = build_spark_validator_with_data(df, spark_session)

    result = validator.expect_column_values_to_be_string_integers_increasing(
        column="a", strictly=True
    )

    assert result == ExpectationValidationResult(
        exception_info={
            "raised_exception": False,
            "exception_traceback": None,
            "exception_message": None,
        },
        expectation_config={
            "expectation_type": "expect_column_values_to_be_string_integers_increasing",
            "meta": {},
            "ge_cloud_id": None,
            "kwargs": {
                "column": "a",
                "strictly": True,
                "batch_id": (),
            },
        },
        result={
            "observed_value": [[False, True], [1, 5]],
        },
        meta={},
        success=False,
    )
