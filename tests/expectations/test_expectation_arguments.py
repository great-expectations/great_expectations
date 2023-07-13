import logging
from typing import List
from unittest import mock

import pandas as pd
import pytest

import great_expectations.exceptions as gx_exceptions
from great_expectations import DataContext
from great_expectations.core import (
    ExpectationConfiguration,
    ExpectationSuite,
    ExpectationSuiteValidationResult,
    ExpectationValidationResult,
)
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.core.usage_statistics.usage_statistics import (
    UsageStatisticsHandler,
)
from great_expectations.validator.validator import Validator
from great_expectations.compatibility import pyspark

logger = logging.getLogger(__name__)


@pytest.fixture
def test_pandas_df():
    df: pd.DataFrame = pd.DataFrame(
        data=[["Scott"], ["Jeff"], ["Thomas"], ["Ann"]], columns=["Name"]
    )
    return df


@pytest.fixture
def test_spark_df(test_pandas_df, spark_session):
    df: pyspark.DataFrame = spark_session.createDataFrame(data=test_pandas_df)
    return df


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_catch_exceptions_no_exceptions(
    mock_emit, in_memory_runtime_context, test_spark_df
):
    catch_exceptions: bool = False  # expect exceptions to be raised
    result_format: dict = {
        "result_format": "SUMMARY",
    }
    runtime_environment_arguments = {
        "catch_exceptions": catch_exceptions,
        "result_format": result_format,
    }

    suite: ExpectationSuite = in_memory_runtime_context.add_expectation_suite(
        "test_suite"
    )

    expectation_configuration: ExpectationConfiguration

    expectation_meta: dict = {"Notes": "Some notes"}

    expectation_arguments_without_meta: dict

    expectation_arguments_column: dict = {
        "include_config": True,
        "column": "Name",  # use correct column to avoid error
    }
    expectation_arguments_without_meta = dict(
        **runtime_environment_arguments, **expectation_arguments_column
    )
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs=expectation_arguments_without_meta,
        meta=expectation_meta,
    )
    suite.add_expectation(expectation_configuration=expectation_configuration)

    expectation_arguments_table: dict = {
        "include_config": True,
        "value": 4,
    }
    expectation_arguments_without_meta = dict(
        **runtime_environment_arguments, **expectation_arguments_table
    )
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_table_row_count_to_equal",
        kwargs=expectation_arguments_without_meta,
        meta=expectation_meta,
    )
    suite.add_expectation(expectation_configuration=expectation_configuration)

    runtime_batch_request = RuntimeBatchRequest(
        datasource_name="spark_datasource",
        data_connector_name="runtime_data_connector",
        data_asset_name="insert_your_data_asset_name_here",
        runtime_parameters={"batch_data": test_spark_df},
        batch_identifiers={
            "id_key_0": "id_value_0",
            "id_key_1": "id_value_1",
        },
    )

    validator: Validator = in_memory_runtime_context.get_validator(
        batch_request=runtime_batch_request,
        expectation_suite=suite,
    )

    # Test calling "validator.validate()" explicitly.

    validator_validation: ExpectationSuiteValidationResult = validator.validate(
        **runtime_environment_arguments
    )
    results: List[ExpectationValidationResult] = validator_validation.results
    assert len(results) == 2

    result: ExpectationValidationResult

    for result in results:
        assert result.success
        assert (
            "exception_traceback" not in result.exception_info
        ) or not result.exception_info["exception_traceback"]
        assert (
            "exception_message" not in result.exception_info
        ) or not result.exception_info["exception_message"]

    # Test calling "validator.expect_*" through "validator.validate_expectation()".

    expectation_parameters: dict

    expectation_arguments_without_meta = dict(
        **runtime_environment_arguments, **expectation_arguments_column
    )
    expectation_parameters = dict(
        **expectation_arguments_without_meta, **expectation_meta
    )
    result = validator.expect_column_values_to_not_be_null(**expectation_parameters)
    assert result.success

    expectation_arguments_without_meta = dict(
        **runtime_environment_arguments, **expectation_arguments_table
    )
    expectation_parameters = dict(
        **expectation_arguments_without_meta, **expectation_meta
    )
    result = validator.expect_table_row_count_to_equal(**expectation_parameters)
    assert result.success

    # In-Memory DataContext does not have UsageStatisticsHandler configured
    assert mock_emit.call_count == 0


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_catch_exceptions_exception_occurred_catch_exceptions_false(
    mock_emit, in_memory_runtime_context, test_spark_df
):
    catch_exceptions: bool = False  # expect exceptions to be raised
    result_format: dict = {
        "result_format": "SUMMARY",
    }
    runtime_environment_arguments = {
        "catch_exceptions": catch_exceptions,
        "result_format": result_format,
    }

    suite: ExpectationSuite = in_memory_runtime_context.add_expectation_suite(
        "test_suite"
    )

    expectation_configuration: ExpectationConfiguration

    expectation_meta: dict = {"Notes": "Some notes"}

    expectation_arguments_without_meta: dict

    expectation_arguments_column: dict = {
        "include_config": True,
        "column": "unknown_column",  # use intentionally incorrect column to force error in "MetricProvider" evaluations
    }
    expectation_arguments_without_meta = dict(
        **runtime_environment_arguments, **expectation_arguments_column
    )
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs=expectation_arguments_without_meta,
        meta=expectation_meta,
    )
    suite.add_expectation(expectation_configuration=expectation_configuration)

    expectation_arguments_table: dict = {
        "include_config": True,
        "value": 4,
    }
    expectation_arguments_without_meta = dict(
        **runtime_environment_arguments, **expectation_arguments_table
    )
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_table_row_count_to_equal",
        kwargs=expectation_arguments_without_meta,
        meta=expectation_meta,
    )
    suite.add_expectation(expectation_configuration=expectation_configuration)

    runtime_batch_request = RuntimeBatchRequest(
        datasource_name="spark_datasource",
        data_connector_name="runtime_data_connector",
        data_asset_name="insert_your_data_asset_name_here",
        runtime_parameters={"batch_data": test_spark_df},
        batch_identifiers={
            "id_key_0": "id_value_0",
            "id_key_1": "id_value_1",
        },
    )

    validator: Validator = in_memory_runtime_context.get_validator(
        batch_request=runtime_batch_request,
        expectation_suite=suite,
    )

    expected_exception_message: str = (
        'Error: The column "unknown_column" in BatchData does not exist.'
    )

    # Test calling "validator.validate()" explicitly.

    with pytest.raises(gx_exceptions.MetricResolutionError) as e:
        # noinspection PyUnusedLocal
        validator_validation: ExpectationSuiteValidationResult = validator.validate(
            **runtime_environment_arguments
        )
    assert e.value.message == expected_exception_message

    # Test calling "validator.expect_*" through "validator.validate_expectation()".

    expectation_parameters: dict

    expectation_arguments_without_meta = dict(
        **runtime_environment_arguments, **expectation_arguments_column
    )
    expectation_parameters = dict(
        **expectation_arguments_without_meta, **expectation_meta
    )

    with pytest.raises(gx_exceptions.MetricResolutionError) as e:
        # noinspection PyUnusedLocal
        result: ExpectationValidationResult = (
            validator.expect_column_values_to_not_be_null(**expectation_parameters)
        )
    assert e.value.message == expected_exception_message

    # Confirm that even though exceptions may occur in some expectations, other expectations can be validated properly.

    expectation_arguments_without_meta = dict(
        **runtime_environment_arguments, **expectation_arguments_table
    )
    expectation_parameters = dict(
        **expectation_arguments_without_meta, **expectation_meta
    )
    result: ExpectationValidationResult = validator.expect_table_row_count_to_equal(
        **expectation_parameters
    )
    assert result.success

    # In-Memory DataContext does not have UsageStatisticsHandler configured
    assert mock_emit.call_count == 0


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_catch_exceptions_exception_occurred_catch_exceptions_true(
    mock_emit, in_memory_runtime_context, test_spark_df
):
    catch_exceptions: bool = True  # expect exceptions to be caught
    result_format: dict = {
        "result_format": "SUMMARY",
    }
    runtime_environment_arguments = {
        "catch_exceptions": catch_exceptions,
        "result_format": result_format,
    }

    suite: ExpectationSuite = in_memory_runtime_context.add_expectation_suite(
        "test_suite"
    )

    expectation_configuration: ExpectationConfiguration

    expectation_meta: dict = {"Notes": "Some notes"}

    expectation_arguments_without_meta: dict

    expectation_arguments_column: dict = {
        "include_config": True,
        "column": "unknown_column",  # use intentionally incorrect column to force error in "MetricProvider" evaluations
    }
    expectation_arguments_without_meta = dict(
        **runtime_environment_arguments, **expectation_arguments_column
    )
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs=expectation_arguments_without_meta,
        meta=expectation_meta,
    )
    suite.add_expectation(expectation_configuration=expectation_configuration)

    expectation_arguments_table: dict = {
        "include_config": True,
        "value": 4,
    }
    expectation_arguments_without_meta = dict(
        **runtime_environment_arguments, **expectation_arguments_table
    )
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_table_row_count_to_equal",
        kwargs=expectation_arguments_without_meta,
        meta=expectation_meta,
    )
    suite.add_expectation(expectation_configuration=expectation_configuration)

    runtime_batch_request = RuntimeBatchRequest(
        datasource_name="spark_datasource",
        data_connector_name="runtime_data_connector",
        data_asset_name="insert_your_data_asset_name_here",
        runtime_parameters={"batch_data": test_spark_df},
        batch_identifiers={
            "id_key_0": "id_value_0",
            "id_key_1": "id_value_1",
        },
    )

    validator: Validator = in_memory_runtime_context.get_validator(
        batch_request=runtime_batch_request,
        expectation_suite=suite,
    )

    expected_exception_message: str = (
        'Error: The column "unknown_column" in BatchData does not exist.'
    )

    # Test calling "validator.validate()" explicitly.

    validator_validation: ExpectationSuiteValidationResult = validator.validate(
        **runtime_environment_arguments
    )
    results: List[ExpectationValidationResult] = validator_validation.results
    assert len(results) == 2

    # Confirm that even though an exception occurred in one expectation, the other expectation is validated properly.

    results = sorted(
        results, key=lambda element: element.expectation_config["expectation_type"]
    )

    result: ExpectationValidationResult

    result = results[0]
    assert (
        result.expectation_config["expectation_type"]
        == "expect_column_values_to_not_be_null"
    )
    assert not result.success
    assert "exception_traceback" in result.exception_info
    assert "exception_message" in result.exception_info
    assert result.exception_info["exception_message"] == expected_exception_message

    result = results[1]
    assert (
        result.expectation_config["expectation_type"]
        == "expect_table_row_count_to_equal"
    )
    assert result.success
    assert (
        "exception_traceback" not in result.exception_info
    ) or not result.exception_info["exception_traceback"]
    assert (
        "exception_message" not in result.exception_info
    ) or not result.exception_info["exception_message"]

    # Test calling "validator.expect_*" through "validator.validate_expectation()".

    expectation_parameters: dict

    expectation_arguments_without_meta = dict(
        **runtime_environment_arguments, **expectation_arguments_column
    )
    expectation_parameters = dict(
        **expectation_arguments_without_meta, **expectation_meta
    )
    result = validator.expect_column_values_to_not_be_null(**expectation_parameters)
    assert not result.success
    assert "exception_traceback" in result.exception_info
    assert "exception_message" in result.exception_info
    assert result.exception_info["exception_message"] == expected_exception_message

    # Confirm that even though exceptions may occur in some expectations, other expectations can be validated properly.

    expectation_arguments_without_meta = dict(
        **runtime_environment_arguments, **expectation_arguments_table
    )
    expectation_parameters = dict(
        **expectation_arguments_without_meta, **expectation_meta
    )
    result = validator.expect_table_row_count_to_equal(**expectation_parameters)
    assert result.success
    assert (
        "exception_traceback" not in result.exception_info
    ) or not result.exception_info["exception_traceback"]
    assert (
        "exception_message" not in result.exception_info
    ) or not result.exception_info["exception_message"]

    # In-Memory DataContext does not have UsageStatisticsHandler configured
    assert mock_emit.call_count == 0


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_result_format_configured_no_set_default_override(
    mock_emit, in_memory_runtime_context, test_spark_df
):
    catch_exceptions: bool = False  # expect exceptions to be raised
    result_format: dict

    result_format = {
        "result_format": "SUMMARY",
    }
    runtime_environment_arguments: dict = {
        "catch_exceptions": catch_exceptions,
        "result_format": result_format,
    }

    suite: ExpectationSuite

    suite = in_memory_runtime_context.add_expectation_suite("test_suite")

    expectation_configuration: ExpectationConfiguration

    expectation_meta: dict = {"Notes": "Some notes"}

    expectation_arguments_without_meta: dict

    expectation_arguments_column: dict = {
        "include_config": True,
        "column": "Name",  # use correct column to avoid error
    }
    expectation_arguments_without_meta = dict(
        **runtime_environment_arguments, **expectation_arguments_column
    )
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs=expectation_arguments_without_meta,
        meta=expectation_meta,
    )
    suite.add_expectation(expectation_configuration=expectation_configuration)

    runtime_batch_request = RuntimeBatchRequest(
        datasource_name="spark_datasource",
        data_connector_name="runtime_data_connector",
        data_asset_name="insert_your_data_asset_name_here",
        runtime_parameters={"batch_data": test_spark_df},
        batch_identifiers={
            "id_key_0": "id_value_0",
            "id_key_1": "id_value_1",
        },
    )

    validator: Validator

    validator = in_memory_runtime_context.get_validator(
        batch_request=runtime_batch_request,
        expectation_suite=suite,
    )

    # Test calling "validator.validate()" explicitly.

    validator_validation: ExpectationSuiteValidationResult

    validator_validation = validator.validate(**runtime_environment_arguments)

    results: List[ExpectationValidationResult]

    results = validator_validation.results
    assert len(results) == 1

    result: ExpectationValidationResult

    result = results[0]
    assert result.success
    assert len(result.result.keys()) > 0
    assert result.result == {
        "element_count": 4,
        "unexpected_count": 0,
        "unexpected_percent": 0.0,
        "partial_unexpected_list": [],
        "partial_unexpected_counts": [],
    }

    result_format = {
        "result_format": "BASIC",
    }
    runtime_environment_arguments: dict = {
        "catch_exceptions": catch_exceptions,
        "result_format": result_format,
    }

    suite = in_memory_runtime_context.add_or_update_expectation_suite("test_suite")

    expectation_arguments_without_meta = dict(
        **runtime_environment_arguments, **expectation_arguments_column
    )
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs=expectation_arguments_without_meta,
        meta=expectation_meta,
    )
    suite.add_expectation(expectation_configuration=expectation_configuration)

    validator = in_memory_runtime_context.get_validator(
        batch_request=runtime_batch_request,
        expectation_suite=suite,
    )

    validator_validation = validator.validate(**runtime_environment_arguments)

    results = validator_validation.results
    assert len(results) == 1

    result = results[0]
    assert result.success
    assert len(result.result.keys()) > 0
    assert result.result == {
        "element_count": 4,
        "unexpected_count": 0,
        "unexpected_percent": 0.0,
        "partial_unexpected_list": [],
    }

    result_format = {
        "result_format": "BOOLEAN_ONLY",
    }
    runtime_environment_arguments: dict = {
        "catch_exceptions": catch_exceptions,
        "result_format": result_format,
    }

    suite = in_memory_runtime_context.add_or_update_expectation_suite("test_suite")

    expectation_arguments_without_meta = dict(
        **runtime_environment_arguments, **expectation_arguments_column
    )
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs=expectation_arguments_without_meta,
        meta=expectation_meta,
    )

    suite.add_expectation(expectation_configuration=expectation_configuration)

    validator = in_memory_runtime_context.get_validator(
        batch_request=runtime_batch_request,
        expectation_suite=suite,
    )

    validator_validation = validator.validate(**runtime_environment_arguments)

    results = validator_validation.results
    assert len(results) == 1

    result = results[0]
    assert result.success
    assert result.to_json_dict() == {
        "expectation_config": {
            "kwargs": {
                "catch_exceptions": False,
                "result_format": {"result_format": "BOOLEAN_ONLY"},
                "include_config": True,
                "column": "Name",
                "batch_id": "bd7b9290f981fde37aabd403e8a507ea",
            },
            "expectation_type": "expect_column_values_to_not_be_null",
            "meta": {"Notes": "Some notes"},
        },
        "meta": {},
        "exception_info": {
            "raised_exception": False,
            "exception_traceback": None,
            "exception_message": None,
        },
        "result": {},
        "success": True,
    }
    assert len(result.result.keys()) == 0
    assert result.result == {}

    # Test calling "validator.expect_*" through "validator.validate_expectation()".

    expectation_parameters: dict

    expectation_parameters = dict(
        **expectation_arguments_without_meta, **expectation_meta
    )
    result = validator.expect_column_values_to_not_be_null(**expectation_parameters)
    assert result.success
    assert result.to_json_dict() == {
        "success": True,
        "meta": {},
        "expectation_config": {
            "expectation_type": "expect_column_values_to_not_be_null",
            "meta": {},
            "kwargs": {
                "catch_exceptions": False,
                "result_format": {
                    "result_format": "BOOLEAN_ONLY",
                    "include_unexpected_rows": False,
                    "partial_unexpected_count": 20,
                },
                "include_config": True,
                "column": "Name",
                "Notes": "Some notes",
                "batch_id": "bd7b9290f981fde37aabd403e8a507ea",
            },
        },
        "result": {},
        "exception_info": {
            "raised_exception": False,
            "exception_traceback": None,
            "exception_message": None,
        },
    }
    assert len(result.result.keys()) == 0
    assert result.result == {}

    # In-Memory DataContext does not have UsageStatisticsHandler configured
    assert mock_emit.call_count == 0


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_result_format_configured_with_set_default_override(
    mock_emit, in_memory_runtime_context, test_spark_df
):
    catch_exceptions: bool = False  # expect exceptions to be raised
    result_format: dict

    result_format = {
        "result_format": "SUMMARY",
    }
    runtime_environment_arguments: dict = {
        "catch_exceptions": catch_exceptions,
        "result_format": result_format,
    }

    suite: ExpectationSuite

    suite = in_memory_runtime_context.add_expectation_suite("test_suite")

    expectation_configuration: ExpectationConfiguration

    expectation_meta: dict = {"Notes": "Some notes"}

    expectation_arguments_without_meta: dict

    expectation_arguments_column: dict = {
        "include_config": True,
        "column": "Name",  # use correct column to avoid error
    }
    expectation_arguments_without_meta = dict(
        **runtime_environment_arguments, **expectation_arguments_column
    )
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs=expectation_arguments_without_meta,
        meta=expectation_meta,
    )
    suite.add_expectation(expectation_configuration=expectation_configuration)

    runtime_batch_request = RuntimeBatchRequest(
        datasource_name="spark_datasource",
        data_connector_name="runtime_data_connector",
        data_asset_name="insert_your_data_asset_name_here",
        runtime_parameters={"batch_data": test_spark_df},
        batch_identifiers={
            "id_key_0": "id_value_0",
            "id_key_1": "id_value_1",
        },
    )

    validator: Validator

    validator = in_memory_runtime_context.get_validator(
        batch_request=runtime_batch_request,
        expectation_suite=suite,
    )

    validator.set_default_expectation_argument("result_format", "BOOLEAN_ONLY")

    # Test calling "validator.validate()" explicitly.

    validator_validation: ExpectationSuiteValidationResult

    validator_validation = validator.validate()

    results: List[ExpectationValidationResult]

    results = validator_validation.results
    assert len(results) == 1

    result: ExpectationValidationResult

    result = results[0]
    assert result.success
    assert result.to_json_dict() == {
        "result": {},
        "expectation_config": {
            "kwargs": {
                "catch_exceptions": False,
                "result_format": {"result_format": "SUMMARY"},
                "include_config": True,
                "column": "Name",
                "batch_id": "bd7b9290f981fde37aabd403e8a507ea",
            },
            "meta": {"Notes": "Some notes"},
            "expectation_type": "expect_column_values_to_not_be_null",
        },
        "success": True,
        "meta": {},
        "exception_info": {
            "raised_exception": False,
            "exception_traceback": None,
            "exception_message": None,
        },
    }
    assert len(result.result.keys()) == 0
    assert result.result == {}

    result_format = {
        "result_format": "BASIC",
    }
    runtime_environment_arguments: dict = {
        "catch_exceptions": catch_exceptions,
        "result_format": result_format,
    }

    suite = in_memory_runtime_context.add_or_update_expectation_suite("test_suite")

    expectation_arguments_without_meta = dict(
        **runtime_environment_arguments, **expectation_arguments_column
    )
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs=expectation_arguments_without_meta,
        meta=expectation_meta,
    )
    suite.add_expectation(expectation_configuration=expectation_configuration)

    validator = in_memory_runtime_context.get_validator(
        batch_request=runtime_batch_request,
        expectation_suite=suite,
    )

    validator.set_default_expectation_argument("result_format", "BOOLEAN_ONLY")

    validator_validation = validator.validate()

    results = validator_validation.results
    assert len(results) == 1

    result = results[0]
    assert len(result.result.keys()) == 0

    # Test calling "validator.expect_*" through "validator.validate_expectation()".

    expectation_parameters: dict

    expectation_parameters = dict(**expectation_arguments_column, **expectation_meta)
    result = validator.expect_column_values_to_not_be_null(**expectation_parameters)
    assert result.success
    assert result.to_json_dict() == {
        "result": {},
        "expectation_config": {
            "kwargs": {
                "include_config": True,
                "column": "Name",
                "Notes": "Some notes",
                "batch_id": "bd7b9290f981fde37aabd403e8a507ea",
            },
            "meta": {},
            "expectation_type": "expect_column_values_to_not_be_null",
        },
        "success": True,
        "meta": {},
        "exception_info": {
            "raised_exception": False,
            "exception_traceback": None,
            "exception_message": None,
        },
    }
    assert len(result.result.keys()) == 0
    assert result.result == {}

    # In-Memory DataContext does not have UsageStatisticsHandler configured
    assert mock_emit.call_count == 0


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_in_memory_runtime_context_configured_with_usage_stats_handler(
    mock_emit, in_memory_runtime_context, test_pandas_df
):
    context: DataContext = in_memory_runtime_context

    # manually set usage statistics handler
    handler = UsageStatisticsHandler(
        data_context=context,
        data_context_id=context._data_context_id,
        oss_id=None,
        usage_statistics_url="http://fakeendpoint.com",
    )
    context._usage_statistics_handler = handler

    catch_exceptions: bool = False  # expect exceptions to be raised
    result_format: dict = {
        "result_format": "SUMMARY",
    }
    runtime_environment_arguments = {
        "catch_exceptions": catch_exceptions,
        "result_format": result_format,
    }

    suite: ExpectationSuite = in_memory_runtime_context.add_expectation_suite(
        "test_suite"
    )

    expectation_configuration: ExpectationConfiguration

    expectation_meta: dict = {"Notes": "Some notes"}

    expectation_arguments_without_meta: dict

    expectation_arguments_column: dict = {
        "include_config": True,
        "column": "Name",  # use correct column to avoid error
    }
    expectation_arguments_without_meta = dict(
        **runtime_environment_arguments, **expectation_arguments_column
    )
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs=expectation_arguments_without_meta,
        meta=expectation_meta,
    )
    suite.add_expectation(expectation_configuration=expectation_configuration)

    # emit 1 from add_expectation
    assert mock_emit.call_count == 1
    assert mock_emit.call_args_list == [
        mock.call(
            {
                "event": "expectation_suite.add_expectation",
                "event_payload": {},
                "success": True,
            }
        )
    ]
