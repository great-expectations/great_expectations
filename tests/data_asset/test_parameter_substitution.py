"""
Test the expectation decorator's ability to substitute parameters
at evaluation time, and store parameters in expectation_suite
"""

import json

import numpy as np
import pytest

from great_expectations.data_asset import DataAsset
from great_expectations.exceptions import EvaluationParameterError
from tests.test_utils import expectationSuiteValidationResultSchema


@pytest.fixture
def data_asset():
    return DataAsset()


@pytest.fixture
def single_expectation_custom_data_asset():
    class SlimCustomDataAsset(DataAsset):
        @DataAsset.expectation("expectation_argument")
        def expect_nothing(self, expectation_argument):
            return {
                "success": True,
                "result": {"details": {"expectation_argument": expectation_argument}},
            }

    return SlimCustomDataAsset()


def test_store_evaluation_parameter(data_asset):
    data_asset.set_evaluation_parameter("my_parameter", "value")
    assert data_asset.get_evaluation_parameter("my_parameter") == "value"

    data_asset.set_evaluation_parameter(
        "my_second_parameter", [1, 2, "value", None, np.nan]
    )
    assert data_asset.get_evaluation_parameter("my_second_parameter") == [
        1,
        2,
        "value",
        None,
        np.nan,
    ]

    with pytest.raises(TypeError):
        data_asset.set_evaluation_parameter(
            ["a", "list", "cannot", "be", "a", "parameter"], "value"
        )


def test_parameter_substitution(single_expectation_custom_data_asset):
    # Set our evaluation parameter from upstream
    single_expectation_custom_data_asset.set_evaluation_parameter(
        "upstream_dag_key", "upstream_dag_value"
    )

    # Establish our expectation using that parameter
    result = single_expectation_custom_data_asset.expect_nothing(
        expectation_argument={"$PARAMETER": "upstream_dag_key"}
    )
    suite = single_expectation_custom_data_asset.get_expectation_suite()

    # Ensure our value has been substituted during evaluation, and set properly in the suite
    assert result.result["details"]["expectation_argument"] == "upstream_dag_value"
    assert suite.evaluation_parameters == {"upstream_dag_key": "upstream_dag_value"}
    assert suite.expectations[0].kwargs == {
        "expectation_argument": {"$PARAMETER": "upstream_dag_key"}
    }


def test_exploratory_parameter_substitution(single_expectation_custom_data_asset):
    # Establish our expectation using a parameter provided at runtime

    result = single_expectation_custom_data_asset.expect_nothing(
        expectation_argument={
            "$PARAMETER": "upstream_dag_key",
            "$PARAMETER.upstream_dag_key": "temporary_value",
        }
    )
    suite = single_expectation_custom_data_asset.get_expectation_suite()
    # Ensure our value has been substituted during evaluation, and NOT stored in the suite
    assert result.result["details"]["expectation_argument"] == "temporary_value"
    assert suite.evaluation_parameters == {}
    assert suite.expectations[0].kwargs == {
        "expectation_argument": {"$PARAMETER": "upstream_dag_key"}
    }

    # Evaluating the expectation without the parameter should now fail, because no parameters were set
    with pytest.raises(EvaluationParameterError) as excinfo:
        single_expectation_custom_data_asset.validate(catch_exceptions=False)
    assert str(excinfo.value) == "No value found for $PARAMETER upstream_dag_key"

    # Setting a parameter value should allow it to succeed
    single_expectation_custom_data_asset.set_evaluation_parameter(
        "upstream_dag_key", "upstream_dag_value"
    )
    validation_result = single_expectation_custom_data_asset.validate()
    assert (
        validation_result.results[0].result["details"]["expectation_argument"]
        == "upstream_dag_value"
    )


def test_validation_substitution(single_expectation_custom_data_asset):
    # Set up an expectation using a parameter, providing a default value.
    result = single_expectation_custom_data_asset.expect_nothing(
        expectation_argument={
            "$PARAMETER": "upstream_dag_key",
            "$PARAMETER.upstream_dag_key": "temporary_value",
        }
    )
    assert result.result["details"]["expectation_argument"] == "temporary_value"

    # Provide a run-time evaluation parameter
    validation_result = single_expectation_custom_data_asset.validate(
        evaluation_parameters={"upstream_dag_key": "upstream_dag_value"}
    )
    assert (
        validation_result.results[0].result["details"]["expectation_argument"]
        == "upstream_dag_value"
    )


def test_validation_substitution_with_json_coercion(
    single_expectation_custom_data_asset,
):
    # Set up an expectation using a parameter, providing a default value.

    # Use a value that is a set. Note that there is no problem converting the type for the expectation (set -> list)
    result = single_expectation_custom_data_asset.expect_nothing(
        expectation_argument={
            "$PARAMETER": "upstream_dag_key",
            "$PARAMETER.upstream_dag_key": {"temporary_value"},
        }
    )
    assert result.result["details"]["expectation_argument"] == ["temporary_value"]

    # Provide a run-time evaluation parameter
    validation_result = single_expectation_custom_data_asset.validate(
        evaluation_parameters={"upstream_dag_key": {"upstream_dag_value"}}
    )
    assert validation_result.results[0].result["details"]["expectation_argument"] == [
        "upstream_dag_value"
    ]

    # Verify that the entire result object including evaluation_parameters is serializable
    assert validation_result["evaluation_parameters"]["upstream_dag_key"] == [
        "upstream_dag_value"
    ]
    try:
        json.dumps(expectationSuiteValidationResultSchema.dumps(validation_result))
    except TypeError as err:
        pytest.fail(
            "Error converting validation_result to json. Got TypeError: %s" + str(err)
        )


def test_validation_parameters_returned(single_expectation_custom_data_asset):
    single_expectation_custom_data_asset.expect_nothing(
        expectation_argument={
            "$PARAMETER": "upstream_dag_key",
            "$PARAMETER.upstream_dag_key": "temporary_value",
        }
    )
    validation_result = single_expectation_custom_data_asset.validate(
        evaluation_parameters={"upstream_dag_key": "upstream_dag_value"}
    )
    assert validation_result["evaluation_parameters"] == {
        "upstream_dag_key": "upstream_dag_value"
    }
