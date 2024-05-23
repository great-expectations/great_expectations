import pytest

from great_expectations.core.expectation_validation_result import (
    ExpectationSuiteValidationResult,
)
from great_expectations.render.renderer import SlackRenderer


@pytest.mark.unit
def test_SlackRenderer_render():
    validation_result = ExpectationSuiteValidationResult(
        success=True,
        statistics={"successful_expectations": 3, "evaluated_expectations": 3},
        results=[],
        suite_name="my_suite",
    )
    data_docs_pages = {"local_site": "file:///localsite/index.html"}
    notify_with = ["local_site"]

    slack_renderer = SlackRenderer()
    output = slack_renderer.render(
        validation_result=validation_result,
        data_docs_pages=data_docs_pages,
        notify_with=notify_with,
    )

    assert output == [
        {
            "text": {
                "text": "*Asset*: __no_data_asset_name__  *Expectation Suite*: my_suite",
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
