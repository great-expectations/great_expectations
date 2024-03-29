import logging
from typing import List, Union

import pandas as pd
import pytest

import great_expectations.exceptions as gx_exceptions
from great_expectations.compatibility import pyspark
from great_expectations.core import (
    ExpectationSuite,
    ExpectationSuiteValidationResult,
    ExpectationValidationResult,
)
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.expectations.expectation_configuration import (
    ExpectationConfiguration,
)
from great_expectations.validator.validator import Validator

logger = logging.getLogger(__name__)


def assert_exception_info(
    result: ExpectationValidationResult, expected_exception_message: Union[str, None]
):
    if result.success:
        if "raised_exception" in result["exception_info"]:
            assert (
                "exception_traceback" not in result.exception_info
            ) or not result.exception_info["exception_traceback"]
            assert ("exception_message" not in result.exception_info) or not result.exception_info[
                "exception_message"
            ]
        else:
            # TODO JT: This accounts for a dictionary of type {"metric_id": ExceptionInfo} path defined in  # noqa: E501
            #  validator._resolve_suite_level_graph_and_process_metric_evaluation_errors
            for k, v in result["exception_info"].items():
                assert ("exception_traceback" not in v) or not v["exception_traceback"]
                assert ("exception_traceback" not in v) or not v["exception_traceback"]
    elif "raised_exception" in result["exception_info"]:
        assert result.exception_info.get("exception_traceback")
        assert result.exception_info.get("exception_message")
        if expected_exception_message:
            assert result["exception_message"] == expected_exception_message
    else:
        # TODO JT: This accounts for a dictionary of type {"metric_id": ExceptionInfo} path defined in  # noqa: E501
        #  validator._resolve_suite_level_graph_and_process_metric_evaluation_errors
        for k, v in result["exception_info"].items():
            assert v.get("exception_traceback")
            assert v.get("exception_message")
            if expected_exception_message:
                assert v["exception_message"] == expected_exception_message


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


@pytest.mark.spark
def test_catch_exceptions_no_exceptions(in_memory_runtime_context, test_spark_df):
    catch_exceptions: bool = False  # expect exceptions to be raised
    result_format: dict = {
        "result_format": "SUMMARY",
    }
    runtime_environment_arguments = {
        "catch_exceptions": catch_exceptions,
        "result_format": result_format,
    }

    suite: ExpectationSuite = in_memory_runtime_context.add_expectation_suite("test_suite")

    expectation_configuration: ExpectationConfiguration

    expectation_meta: dict = {"notes": "Some notes"}

    expectation_arguments_without_meta: dict

    expectation_arguments_column: dict = {
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
    suite.add_expectation_configuration(expectation_configuration=expectation_configuration)

    expectation_arguments_table: dict = {
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
    suite.add_expectation_configuration(expectation_configuration=expectation_configuration)

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
        assert_exception_info(result=result, expected_exception_message=None)

    # Test calling "validator.expect_*" through "validator.validate_expectation()".

    expectation_parameters: dict

    expectation_arguments_without_meta = dict(
        **runtime_environment_arguments, **expectation_arguments_column
    )
    expectation_parameters = dict(**expectation_arguments_without_meta, **expectation_meta)
    result = validator.expect_column_values_to_not_be_null(**expectation_parameters)
    assert result.success

    expectation_arguments_without_meta = dict(
        **runtime_environment_arguments, **expectation_arguments_table
    )
    expectation_parameters = dict(**expectation_arguments_without_meta, **expectation_meta)
    result = validator.expect_table_row_count_to_equal(**expectation_parameters)
    assert result.success


@pytest.mark.spark
def test_catch_exceptions_exception_occurred_catch_exceptions_false(
    in_memory_runtime_context, test_spark_df
):
    catch_exceptions: bool = False  # expect exceptions to be raised
    result_format: dict = {
        "result_format": "SUMMARY",
    }
    runtime_environment_arguments = {
        "catch_exceptions": catch_exceptions,
        "result_format": result_format,
    }

    suite: ExpectationSuite = in_memory_runtime_context.add_expectation_suite("test_suite")

    expectation_configuration: ExpectationConfiguration

    expectation_meta: dict = {"notes": "Some notes"}

    expectation_arguments_without_meta: dict

    expectation_arguments_column: dict = {
        "column": "unknown_column",  # use intentionally incorrect column to force error in "MetricProvider" evaluations  # noqa: E501
    }
    expectation_arguments_without_meta = dict(
        **runtime_environment_arguments, **expectation_arguments_column
    )
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs=expectation_arguments_without_meta,
        meta=expectation_meta,
    )
    suite.add_expectation_configuration(expectation_configuration=expectation_configuration)

    expectation_arguments_table: dict = {
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
    suite.add_expectation_configuration(expectation_configuration=expectation_configuration)

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
        validator_validation = validator.validate(  # noqa: F841
            **runtime_environment_arguments
        )
    assert e.value.message == expected_exception_message

    # Test calling "validator.expect_*" through "validator.validate_expectation()".

    expectation_parameters: dict

    expectation_arguments_without_meta = dict(
        **runtime_environment_arguments, **expectation_arguments_column
    )
    expectation_parameters = dict(**expectation_arguments_without_meta, **expectation_meta)

    with pytest.raises(gx_exceptions.MetricResolutionError) as e:
        result: ExpectationValidationResult = validator.expect_column_values_to_not_be_null(
            **expectation_parameters
        )
    assert e.value.message == expected_exception_message

    # Confirm that even though exceptions may occur in some expectations, other expectations can be validated properly.  # noqa: E501

    expectation_arguments_without_meta = dict(
        **runtime_environment_arguments, **expectation_arguments_table
    )
    expectation_parameters = dict(**expectation_arguments_without_meta, **expectation_meta)
    result: ExpectationValidationResult = validator.expect_table_row_count_to_equal(
        **expectation_parameters
    )
    assert result.success


@pytest.mark.spark
def test_catch_exceptions_exception_occurred_catch_exceptions_true(
    in_memory_runtime_context, test_spark_df
):
    catch_exceptions: bool = True  # expect exceptions to be caught
    result_format: dict = {
        "result_format": "SUMMARY",
    }
    runtime_environment_arguments = {
        "catch_exceptions": catch_exceptions,
        "result_format": result_format,
    }

    suite: ExpectationSuite = in_memory_runtime_context.add_expectation_suite("test_suite")

    expectation_configuration: ExpectationConfiguration

    expectation_meta: dict = {"notes": "Some notes"}

    expectation_arguments_without_meta: dict

    expectation_arguments_column: dict = {
        "column": "unknown_column",  # use intentionally incorrect column to force error in "MetricProvider" evaluations  # noqa: E501
    }
    expectation_arguments_without_meta = dict(
        **runtime_environment_arguments, **expectation_arguments_column
    )
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs=expectation_arguments_without_meta,
        meta=expectation_meta,
    )
    suite.add_expectation_configuration(expectation_configuration=expectation_configuration)

    expectation_arguments_table: dict = {
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
    suite.add_expectation_configuration(expectation_configuration=expectation_configuration)

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

    # Confirm that even though an exception occurred in one expectation, the other expectation is validated properly.  # noqa: E501

    results = sorted(results, key=lambda element: element.expectation_config["expectation_type"])

    result: ExpectationValidationResult

    result = results[0]
    assert result.expectation_config["expectation_type"] == "expect_column_values_to_not_be_null"
    assert not result.success
    assert_exception_info(result=result, expected_exception_message=expected_exception_message)

    result = results[1]
    assert result.expectation_config["expectation_type"] == "expect_table_row_count_to_equal"
    assert result.success
    assert_exception_info(result=result, expected_exception_message=None)

    # Test calling "validator.expect_*" through "validator.validate_expectation()".

    expectation_parameters: dict

    expectation_arguments_without_meta = dict(
        **runtime_environment_arguments, **expectation_arguments_column
    )
    expectation_parameters = dict(**expectation_arguments_without_meta, **expectation_meta)
    result = validator.expect_column_values_to_not_be_null(**expectation_parameters)
    assert not result.success
    assert_exception_info(result=result, expected_exception_message=expected_exception_message)

    # Confirm that even though exceptions may occur in some expectations, other expectations can be validated properly.  # noqa: E501

    expectation_arguments_without_meta = dict(
        **runtime_environment_arguments, **expectation_arguments_table
    )
    expectation_parameters = dict(**expectation_arguments_without_meta, **expectation_meta)
    result = validator.expect_table_row_count_to_equal(**expectation_parameters)
    assert result.success
    assert_exception_info(result=result, expected_exception_message=None)


@pytest.mark.spark
def test_result_format_configured_no_set_default_override(  # noqa: PLR0915
    in_memory_runtime_context, test_spark_df
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

    suite = in_memory_runtime_context.add_expectation_suite("test_suite")

    expectation_arguments_column: dict = {
        "column": "Name",  # use correct column to avoid error
    }
    expectation_arguments_without_meta = dict(
        **runtime_environment_arguments, **expectation_arguments_column
    )
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs=expectation_arguments_without_meta,
    )
    suite.add_expectation_configuration(expectation_configuration=expectation_configuration)

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
    )
    suite.add_expectation_configuration(expectation_configuration=expectation_configuration)

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
    )

    suite.add_expectation_configuration(expectation_configuration=expectation_configuration)

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
                "column": "Name",
                "batch_id": "bd7b9290f981fde37aabd403e8a507ea",
            },
            "expectation_type": "expect_column_values_to_not_be_null",
            "meta": {},
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

    expectation_parameters = dict(**expectation_arguments_without_meta)
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
                "column": "Name",
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


@pytest.mark.spark
def test_result_format_configured_with_set_default_override(
    in_memory_runtime_context, test_spark_df
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

    expectation_arguments_without_meta: dict

    expectation_arguments_column: dict = {
        "column": "Name",  # use correct column to avoid error
    }
    expectation_arguments_without_meta = dict(
        **runtime_environment_arguments, **expectation_arguments_column
    )
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs=expectation_arguments_without_meta,
    )
    suite.add_expectation_configuration(expectation_configuration=expectation_configuration)

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
                "column": "Name",
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
    )
    suite.add_expectation_configuration(expectation_configuration=expectation_configuration)

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

    expectation_parameters = dict(**expectation_arguments_column)
    result = validator.expect_column_values_to_not_be_null(**expectation_parameters)
    assert result.success
    assert result.to_json_dict() == {
        "result": {},
        "expectation_config": {
            "kwargs": {
                "column": "Name",
                "batch_id": "bd7b9290f981fde37aabd403e8a507ea",
            },
            "expectation_type": "expect_column_values_to_not_be_null",
            "meta": {},
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
