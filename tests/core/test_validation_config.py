from unittest import mock

import pytest

import great_expectations as gx
from great_expectations.core.batch_config import BatchConfig
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.expectation_validation_result import (
    ExpectationSuiteValidationResult,
)
from great_expectations.core.validation_config import ValidationConfig
from great_expectations.validator.v1_validator import Validator


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
