import pytest
from pytest_mock import MockerFixture

from great_expectations.checkpoint.v1_checkpoint import Checkpoint, CheckpointResult
from great_expectations.core.batch import IDDict, LegacyBatchDefinition
from great_expectations.core.expectation_validation_result import (
    ExpectationSuiteValidationResult,
)
from great_expectations.core.run_identifier import RunIdentifier
from great_expectations.data_context.types.resource_identifiers import ValidationResultIdentifier
from great_expectations.render.renderer import OpsgenieRenderer


@pytest.mark.big
def test_OpsgenieRenderer_validation_results_success():
    validation_result_suite = ExpectationSuiteValidationResult(
        results=[],
        success=True,
        suite_name="default",
        statistics={
            "evaluated_expectations": 0,
            "successful_expectations": 0,
            "unsuccessful_expectations": 0,
            "success_percent": None,
        },
        meta={
            "great_expectations_version": "v0.12.2__develop",
            "batch_kwargs": {"data_asset_name": "x/y/z"},
            "data_asset_name": {
                "datasource": "x",
                "generator": "y",
                "generator_asset": "z",
            },
            "expectation_suite_name": "default",
            "run_id": "2021-01-01T000000.000000Z",
        },
    )

    rendered_output = OpsgenieRenderer().render(validation_result_suite)

    expected_output = "Batch Validation Status: Success 🎉\nExpectation suite name: default\nData asset name: x/y/z\nRun ID: 2021-01-01T000000.000000Z\nBatch ID: data_asset_name=x/y/z\nSummary: 0 of 0 expectations were met"  # noqa: E501

    assert rendered_output == expected_output


@pytest.mark.big
def test_OpsgenieRenderer_checkpoint_validation_results_success():
    batch_definition = LegacyBatchDefinition(
        datasource_name="test_datasource",
        data_connector_name="test_dataconnector",
        data_asset_name="test_data_asset",
        batch_identifiers=IDDict({"id": "my_id"}),
    )
    validation_result_suite = ExpectationSuiteValidationResult(
        results=[],
        success=True,
        suite_name="default",
        statistics={
            "evaluated_expectations": 0,
            "successful_expectations": 0,
            "unsuccessful_expectations": 0,
            "success_percent": None,
        },
        meta={
            "great_expectations_version": "v0.12.2__develop",
            "active_batch_definition": batch_definition,
            "expectation_suite_name": "default",
            "run_id": "2021-01-01T000000.000000Z",
        },
    )

    rendered_output = OpsgenieRenderer().render(validation_result_suite)

    expected_output = "Batch Validation Status: Success 🎉\nExpectation suite name: default\nData asset name: test_data_asset\nRun ID: 2021-01-01T000000.000000Z\nBatch ID: ()\nSummary: 0 of 0 expectations were met"  # noqa: E501

    assert rendered_output == expected_output


@pytest.mark.big
def test_OpsgenieRenderer_validation_results_failure():
    validation_result_suite = ExpectationSuiteValidationResult(
        results=[],
        success=False,
        suite_name="default",
        statistics={
            "evaluated_expectations": 1,
            "successful_expectations": 0,
            "unsuccessful_expectations": 1,
            "success_percent": None,
        },
        meta={
            "great_expectations_version": "v0.12.2__develop",
            "batch_kwargs": {"data_asset_name": "x/y/z"},
            "data_asset_name": {
                "datasource": "x",
                "generator": "y",
                "generator_asset": "z",
            },
            "expectation_suite_name": "default",
            "run_id": "2021-01-01T000000.000000Z",
        },
    )

    rendered_output = OpsgenieRenderer().render(validation_result_suite)

    expected_output = "Batch Validation Status: Failed ❌\nExpectation suite name: default\nData asset name: x/y/z\nRun ID: 2021-01-01T000000.000000Z\nBatch ID: data_asset_name=x/y/z\nSummary: 0 of 1 expectations were met"  # noqa: E501

    assert rendered_output == expected_output


@pytest.mark.unit
def test_OpsgenieRenderer_v1_render(mocker: MockerFixture):
    # Arrange
    result_a = mocker.MagicMock(
        spec=ExpectationSuiteValidationResult,
        suite_name="my_bad_suite",
        meta={},
        statistics={"successful_expectations": 3, "evaluated_expectations": 5},
        batch_id="my_batch",
        success=False,
    )
    result_a.asset_name = "my_first_asset"
    result_b = mocker.MagicMock(
        spec=ExpectationSuiteValidationResult,
        suite_name="my_good_suite",
        meta={"run_id": "my_run_id"},
        statistics={"successful_expectations": 1, "evaluated_expectations": 1},
        batch_id="my_other_batch",
        success=True,
    )
    result_b.asset_name = None

    checkpoint_result = CheckpointResult(
        run_id=RunIdentifier(run_name="my_run_id"),
        run_results={
            mocker.MagicMock(spec=ValidationResultIdentifier): result_a,
            mocker.MagicMock(spec=ValidationResultIdentifier): result_b,
        },
        checkpoint_config=mocker.MagicMock(spec=Checkpoint, name="my_checkpoint"),
        success=False,
    )

    # Act
    renderer = OpsgenieRenderer()
    raw_output = renderer.v1_render(checkpoint_result=checkpoint_result)
    parts = raw_output.split("\n")

    # Assert
    header = parts.pop(0)  # Separately evaluate header due to dynamic content
    assert "Checkpoint:" in header and "Run ID:" in header
    assert parts == [
        "Status: Failed ❌",
        "",
        "Batch Validation Status: Failed ❌",
        "Expectation Suite Name: my_bad_suite",
        "Data Asset Name: my_first_asset",
        "Run ID: __no_run_id__",
        "Batch ID: my_batch",
        "Summary: 3 of 5 expectations were met",
        "",
        "Batch Validation Status: Success 🎉",
        "Expectation Suite Name: my_good_suite",
        "Data Asset Name: __no_data_asset_name__",
        "Run ID: my_run_id",
        "Batch ID: my_other_batch",
        "Summary: 1 of 1 expectations were met",
    ]
