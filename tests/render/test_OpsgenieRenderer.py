from great_expectations.core import ExpectationSuiteValidationResult
from great_expectations.render.renderer import OpsgenieRenderer


def test_OpsgenieRenderer_validation_results_success():

    validation_result_suite = ExpectationSuiteValidationResult(
        results=[],
        success=True,
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

    expected_output = "Batch Validation Status: Success 🎉\nExpectation suite name: default\nData asset name: x/y/z\nRun ID: 2021-01-01T000000.000000Z\nBatch ID: data_asset_name=x/y/z\nSummary: 0 of 0 expectations were met"

    assert rendered_output == expected_output


def test_OpsgenieRenderer_validation_results_failure():

    validation_result_suite = ExpectationSuiteValidationResult(
        results=[],
        success=False,
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

    expected_output = "Batch Validation Status: Failed ❌\nExpectation suite name: default\nData asset name: x/y/z\nRun ID: 2021-01-01T000000.000000Z\nBatch ID: data_asset_name=x/y/z\nSummary: 0 of 1 expectations were met"

    assert rendered_output == expected_output
