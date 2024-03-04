from unittest import mock

import pytest

import great_expectations as gx
import great_expectations.expectations as gxe
from great_expectations.core.batch_config import BatchConfig
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.expectation_validation_result import (
    ExpectationSuiteValidationResult,
    ExpectationValidationResult,
)
from great_expectations.core.validation_config import ValidationConfig
from great_expectations.validator.v1_validator import ResultFormat, Validator


@pytest.fixture
def validation_config() -> ValidationConfig:
    gx.get_context(mode="ephemeral")
    return ValidationConfig(
        name="my_validation",
        data=BatchConfig(name="my_batch_config"),
        suite=ExpectationSuite(name="my_suite"),
    )


@pytest.mark.unit
@mock.patch.object(Validator, "validate_expectation_suite")
def test_validation_config__run__no_args(
    mock_validate_expectation_suite: mock.MagicMock,
    validation_config: ValidationConfig,
):
    results = ExpectationSuiteValidationResult(success=True)
    mock_validate_expectation_suite.return_value = results
    output = validation_config.run()

    assert output == results
    mock_validate_expectation_suite.assert_called_once_with(
        validation_config.suite, None
    )


@pytest.mark.unit
@mock.patch.object(Validator, "validate_expectation_suite")
def test_validation_config__run__passes_evaluation_parameters_to_validator(
    mock_validate_expectation_suite: mock.MagicMock,
    validation_config: ValidationConfig,
):
    results = ExpectationSuiteValidationResult(success=True)
    mock_validate_expectation_suite.return_value = results
    evaluation_parameters = {"foo": "bar"}
    output = validation_config.run(evaluation_parameters=evaluation_parameters)

    assert output == results
    mock_validate_expectation_suite.assert_called_once_with(
        validation_config.suite, evaluation_parameters
    )


@pytest.mark.unit
@mock.patch.object(Validator, "graph_validate")
def test_validation_config__run__passes_simple_data_to_validator(
    mock_graph_validate: mock.MagicMock,
    validation_config: ValidationConfig,
):
    validation_config.suite.add_expectation(
        gxe.ExpectColumnMaxToBeBetween(column="foo", max_value=1)
    )
    mock_graph_validate.return_value = [ExpectationValidationResult(success=True)]
    validation_config.run()

    mock_graph_validate.assert_called_once_with(
        configurations=[
            {
                "expectation_type": "expect_column_max_to_be_between",
                "kwargs": {"column": "foo", "max_value": 1},
            }
        ],
        runtime_configuration={"result_format": "SUMMARY"},
    )


@pytest.mark.unit
@mock.patch.object(Validator, "graph_validate")
def test_validation_config__run__passes_complex_data(
    mock_graph_validate: mock.MagicMock,
    validation_config: ValidationConfig,
):
    validation_config.suite.add_expectation(
        gxe.ExpectColumnMaxToBeBetween(
            column="foo", max_value={"$PARAMETER": "max_value"}
        )
    )
    mock_graph_validate.return_value = [ExpectationValidationResult(success=True)]

    validation_config.run(
        batch_config_options={"year": 2024},
        evaluation_parameters={"min_value": 9000},
        result_format=ResultFormat.COMPLETE,
    )

    mock_graph_validate.assert_called_once_with(
        configurations=[
            {
                "expectation_type": "expect_column_max_to_be_between",
                "kwargs": {"column": "foo", "max_value": 9000},
            }
        ],
        runtime_configuration={"result_format": "COMPLETE"},
    )


@pytest.mark.unit
@mock.patch.object(Validator, "graph_validate")
def test_validation_config__run__returns_expected_data(
    mock_graph_validate: mock.MagicMock,
    validation_config: ValidationConfig,
):
    graph_validate_results = [ExpectationValidationResult(success=True)]
    mock_graph_validate.return_value = graph_validate_results

    output = validation_config.run()

    assert output == ExpectationSuiteValidationResult(
        results=graph_validate_results,
        success=True,
        statistics={
            "evaluated_expectations": 1,
            "successful_expectations": 1,
            "unsuccessful_expectations": 0,
            "success_percent": 1.0,
        },
    )
