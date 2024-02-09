import json

import pytest

import great_expectations.expectations as gxe
from great_expectations.core import (
    ExpectationSuiteValidationResult,
    ExpectationValidationResult,
)
from great_expectations.expectations.expectation_configuration import (
    ExpectationConfiguration,
)


@pytest.mark.unit
def test_expectation_validation_result_describe_returns_expected_description():
    # arrange
    evr = ExpectationValidationResult(
        success=False,
        expectation_config=gxe.ExpectColumnValuesToBeBetween(
            column="passenger_count",
            min_value=0,
            max_value=6,
            notes="Per the TLC data dictionary, this is a driver-submitted value (historically between 0 to 6)",
        ).configuration,
        result={
            "element_count": 100000,
            "unexpected_count": 1,
            "unexpected_percent": 0.001,
            "partial_unexpected_list": [7.0],
            "missing_count": 0,
            "missing_percent": 0.0,
            "unexpected_percent_total": 0.001,
            "unexpected_percent_nonmissing": 0.001,
            "partial_unexpected_counts": [{"value": 7.0, "count": 1}],
            "partial_unexpected_index_list": [48422],
        },
        exception_info={
            "raised_exception": False,
            "exception_traceback": None,
            "exception_message": None,
        },
    )
    # act
    description = evr.describe()
    # assert
    assert description == json.dumps(
        {
            "expectation_type": "expect_column_values_to_be_between",
            "success": False,
            "kwargs": {"column": "passenger_count", "min_value": 0.0, "max_value": 6.0},
            "result": {
                "element_count": 100000,
                "unexpected_count": 1,
                "unexpected_percent": 0.001,
                "partial_unexpected_list": [7.0],
                "missing_count": 0,
                "missing_percent": 0.0,
                "unexpected_percent_total": 0.001,
                "unexpected_percent_nonmissing": 0.001,
                "partial_unexpected_counts": [{"value": 7.0, "count": 1}],
                "partial_unexpected_index_list": [48422],
            },
        },
        indent=4,
    )


@pytest.mark.unit
def test_expectation_validation_result_describe_returns_expected_description_with_null_values():
    # It's unclear if an ExpectationValidationResult can ever be valid without an Expectation
    # or a result, but since it's typed that way we test it
    # arrange
    evr = ExpectationValidationResult(
        success=True,
        exception_info={
            "raised_exception": False,
            "exception_traceback": None,
            "exception_message": None,
        },
    )
    # act
    description = evr.describe()
    # assert
    assert description == json.dumps(
        {
            "expectation_type": None,
            "success": True,
            "kwargs": None,
            "result": {},
        },
        indent=4,
    )


@pytest.mark.unit
def test_expectation_validation_result_describe_returns_expected_description_with_exception():
    # arrange
    evr = ExpectationValidationResult(
        success=False,
        expectation_config=gxe.ExpectColumnValuesToBeBetween(
            column="passenger_count",
            min_value=0,
            max_value=6,
            notes="Per the TLC data dictionary, this is a driver-submitted value (historically between 0 to 6)",
        ).configuration,
        result={
            "element_count": 100000,
            "unexpected_count": 1,
            "unexpected_percent": 0.001,
            "partial_unexpected_list": [7.0],
            "missing_count": 0,
            "missing_percent": 0.0,
            "unexpected_percent_total": 0.001,
            "unexpected_percent_nonmissing": 0.001,
            "partial_unexpected_counts": [{"value": 7.0, "count": 1}],
            "partial_unexpected_index_list": [48422],
        },
        exception_info={
            "raised_exception": True,
            "exception_traceback": "Traceback (most recent call last): something went wrong",
            "exception_message": "Helpful message here",
        },
    )
    # act
    description = evr.describe()
    # assert
    assert description == json.dumps(
        {
            "expectation_type": "expect_column_values_to_be_between",
            "success": False,
            "kwargs": {"column": "passenger_count", "min_value": 0.0, "max_value": 6.0},
            "result": {
                "element_count": 100000,
                "unexpected_count": 1,
                "unexpected_percent": 0.001,
                "partial_unexpected_list": [7.0],
                "missing_count": 0,
                "missing_percent": 0.0,
                "unexpected_percent_total": 0.001,
                "unexpected_percent_nonmissing": 0.001,
                "partial_unexpected_counts": [{"value": 7.0, "count": 1}],
                "partial_unexpected_index_list": [48422],
            },
            "exception_info": {
                "raised_exception": True,
                "exception_traceback": "Traceback (most recent call last): something went wrong",
                "exception_message": "Helpful message here",
            },
        },
        indent=4,
    )


@pytest.mark.unit
def test_expectation_suite_validation_result_returns_expected_shape():
    # arrange
    svr = ExpectationSuiteValidationResult(
        success=True,
        statistics={
            "evaluated_expectations": 2,
            "successful_expectations": 2,
            "unsuccessful_expectations": 0,
            "success_percent": 100.0,
        },
        results=[
            ExpectationValidationResult(
                **{
                    "meta": {},
                    "success": True,
                    "exception_info": {
                        "raised_exception": False,
                        "exception_traceback": None,
                        "exception_message": None,
                    },
                    "result": {
                        "element_count": 100000,
                        "unexpected_count": 1,
                        "unexpected_percent": 0.001,
                        "partial_unexpected_list": [7.0],
                        "missing_count": 0,
                        "missing_percent": 0.0,
                        "unexpected_percent_total": 0.001,
                        "unexpected_percent_nonmissing": 0.001,
                        "partial_unexpected_counts": [{"value": 7.0, "count": 1}],
                        "partial_unexpected_index_list": [48422],
                    },
                    "expectation_config": ExpectationConfiguration(
                        **{
                            "meta": {},
                            "notes": "Per the TLC data dictionary, this is a driver-submitted value (historically between 0 to 6)",
                            "ge_cloud_id": "9f76d0b5-9d99-4ed9-a269-339b35e60490",
                            "kwargs": {
                                "batch_id": "default_pandas_datasource-#ephemeral_pandas_asset",
                                "mostly": 0.95,
                                "column": "passenger_count",
                                "min_value": 0.0,
                                "max_value": 6.0,
                            },
                            "expectation_type": "expect_column_values_to_be_between",
                        }
                    ),
                }
            ),
            ExpectationValidationResult(
                **{
                    "meta": {},
                    "success": True,
                    "exception_info": {
                        "raised_exception": False,
                        "exception_traceback": None,
                        "exception_message": None,
                    },
                    "result": {
                        "element_count": 100000,
                        "unexpected_count": 0,
                        "unexpected_percent": 0.0,
                        "partial_unexpected_list": [],
                        "partial_unexpected_counts": [],
                        "partial_unexpected_index_list": [],
                    },
                    "expectation_config": ExpectationConfiguration(
                        **{
                            "meta": {},
                            "ge_cloud_id": "19c0e80c-d676-4b01-a4a3-2a568552d368",
                            "kwargs": {
                                "batch_id": "default_pandas_datasource-#ephemeral_pandas_asset",
                                "column": "trip_distance",
                            },
                            "expectation_type": "expect_column_values_to_not_be_null",
                        }
                    ),
                }
            ),
        ],
    )
    # act
    description = svr.describe()
    # assert
    assert description == json.dumps(
        {
            "success": True,
            "statistics": {
                "evaluated_expectations": 2,
                "successful_expectations": 2,
                "unsuccessful_expectations": 0,
                "success_percent": 100.0,
            },
            "expectations": [
                {
                    "expectation_type": "expect_column_values_to_be_between",
                    "success": True,
                    "kwargs": {
                        "batch_id": "default_pandas_datasource-#ephemeral_pandas_asset",
                        "mostly": 0.95,
                        "column": "passenger_count",
                        "min_value": 0.0,
                        "max_value": 6.0,
                    },
                    "result": {
                        "element_count": 100000,
                        "unexpected_count": 1,
                        "unexpected_percent": 0.001,
                        "partial_unexpected_list": [7.0],
                        "missing_count": 0,
                        "missing_percent": 0.0,
                        "unexpected_percent_total": 0.001,
                        "unexpected_percent_nonmissing": 0.001,
                        "partial_unexpected_counts": [{"value": 7.0, "count": 1}],
                        "partial_unexpected_index_list": [48422],
                    },
                },
                {
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "success": True,
                    "kwargs": {
                        "batch_id": "default_pandas_datasource-#ephemeral_pandas_asset",
                        "column": "trip_distance",
                    },
                    "result": {
                        "element_count": 100000,
                        "unexpected_count": 0,
                        "unexpected_percent": 0.0,
                        "partial_unexpected_list": [],
                        "partial_unexpected_counts": [],
                        "partial_unexpected_index_list": [],
                    },
                },
            ],
        },
        indent=4,
    )
