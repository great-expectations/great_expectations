from unittest import mock

import pytest

import great_expectations as gx
import great_expectations.expectations as gxe
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.expectation_validation_result import (
    ExpectationSuiteValidationResult,
    ExpectationValidationResult,
)
from great_expectations.core.validation_config import ValidationConfig
from great_expectations.data_context.data_context.context_factory import ProjectManager
from great_expectations.datasource.fluent.pandas_datasource import _PandasDataAsset
from great_expectations.execution_engine.execution_engine import ExecutionEngine
from great_expectations.expectations.expectation_configuration import (
    ExpectationConfiguration,
)
from great_expectations.validator.v1_validator import (
    OldValidator,
    ResultFormat,
)


@pytest.fixture
def validation_config() -> ValidationConfig:
    context = gx.get_context(mode="ephemeral")
    batch_config = (
        context.sources.add_pandas("my_datasource")
        .add_csv_asset("csv_asset", "taxi.csv")  # type: ignore
        .add_batch_config("my_batch_config")
    )
    return ValidationConfig(
        name="my_validation",
        data=batch_config,
        suite=ExpectationSuite(name="my_suite"),
    )


@pytest.fixture
def mock_validator():
    """Set up our ProjectManager to return a mock Validator"""
    with mock.patch.object(ProjectManager, "get_validator") as mock_get_validator:
        with mock.patch.object(OldValidator, "graph_validate"):
            gx.get_context()
            mock_validator = OldValidator(
                execution_engine=mock.MagicMock(spec=ExecutionEngine)
            )
            mock_get_validator.return_value = mock_validator

            yield mock_validator


@pytest.mark.unit
def test_validation_config__run__passes_simple_data_to_validator(
    mock_validator: mock.MagicMock,
    validation_config: ValidationConfig,
):
    validation_config.suite.add_expectation(
        gxe.ExpectColumnMaxToBeBetween(column="foo", max_value=1)
    )
    mock_validator.graph_validate.return_value = [
        ExpectationValidationResult(success=True)
    ]

    validation_config.run()

    mock_validator.graph_validate.assert_called_with(
        configurations=[
            ExpectationConfiguration(
                expectation_type="expect_column_max_to_be_between",
                kwargs={"column": "foo", "max_value": 1.0},
            )
        ],
        runtime_configuration={"result_format": "SUMMARY"},
    )


@mock.patch.object(_PandasDataAsset, "build_batch_request", autospec=True)
@pytest.mark.unit
def test_validation_config__run__passes_complex_data(
    mock_build_batch_request,
    mock_validator: mock.MagicMock,
    validation_config: ValidationConfig,
):
    validation_config.suite.add_expectation(
        gxe.ExpectColumnMaxToBeBetween(
            column="foo", max_value={"$PARAMETER": "max_value"}
        )
    )
    mock_validator.graph_validate.return_value = [
        ExpectationValidationResult(success=True)
    ]

    validation_config.run(
        batch_config_options={"year": 2024},
        evaluation_parameters={"max_value": 9000},
        result_format=ResultFormat.COMPLETE,
    )

    mock_validator.graph_validate.assert_called_with(
        configurations=[
            ExpectationConfiguration(
                expectation_type="expect_column_max_to_be_between",
                kwargs={"column": "foo", "max_value": 9000},
            )
        ],
        runtime_configuration={"result_format": "COMPLETE"},
    )


@pytest.mark.unit
def test_validation_config__run__returns_expected_data(
    mock_validator: mock.MagicMock,
    validation_config: ValidationConfig,
):
    graph_validate_results = [ExpectationValidationResult(success=True)]
    mock_validator.graph_validate.return_value = graph_validate_results

    output = validation_config.run()

    assert output == ExpectationSuiteValidationResult(
        results=graph_validate_results,
        success=True,
        statistics={
            "evaluated_expectations": 1,
            "successful_expectations": 1,
            "unsuccessful_expectations": 0,
            "success_percent": 100.0,
        },
    )
