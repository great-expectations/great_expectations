import pytest

from great_expectations.core.expectation_validation_result import (
    ExpectationSuiteValidationResult,
    ExpectationSuiteValidationResultMeta,
)
from great_expectations.data_context.types.resource_identifiers import ValidationResultIdentifier
from great_expectations.render.renderer import SlackRenderer


@pytest.mark.unit
def test_SlackRenderer_render(mocker):
    validation_result = ExpectationSuiteValidationResult(
        success=True,
        meta=ExpectationSuiteValidationResultMeta(
            **{
                "active_batch_definition": {
                    "batch_identifiers": {},
                    "data_asset_name": "taxi_data_1.csv",
                    "data_connector_name": "default_inferred_data_connector_name",
                    "datasource_name": "pandas",
                },
                "batch_markers": {
                    "ge_load_time": "20220727T154327.630107Z",
                    "pandas_data_fingerprint": "c4f929e6d4fab001fedc9e075bf4b612",
                },
                "batch_spec": {"path": "../data/taxi_data_1.csv"},
                "checkpoint_name": "single_validation_checkpoint",
                "expectation_suite_name": "taxi_suite_1",
                "great_expectations_version": "0.15.15",
                "run_id": {
                    "run_name": "20220727-114327-my-run-name-template",
                    "run_time": "2022-07-27T11:43:27.625252+00:00",
                },
                "validation_time": "20220727T154327.701100Z",
            }
        ),
        statistics={"successful_expectations": 3, "evaluated_expectations": 3},
        results=[],
        suite_name="my_suite",
    )
    key = mocker.MagicMock(spec=ValidationResultIdentifier)
    data_docs_pages = {key: {"local_site": "file:///localsite/index.html"}}
    notify_with = ["local_site"]

    slack_renderer = SlackRenderer()
    output = slack_renderer.render(
        validation_result=validation_result,
        data_docs_pages=data_docs_pages,
        notify_with=notify_with,
        validation_result_urls=["file:///localsite/index.html"],
    )

    assert output == [
        {
            "text": {
                "text": "*Asset*: taxi_data_1.csv  *Expectation Suite*: my_suite",
                "type": "mrkdwn",
            },
            "type": "section",
        },
        {
            "text": {
                "text": "*DataDocs* can be found here: `file:///localsite/index.html` \n (Please copy and paste link into a browser to view)\n",  # noqa: E501
                "type": "mrkdwn",
            },
            "type": "section",
        },
    ]
