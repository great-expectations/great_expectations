from great_expectations.core.expectation_validation_result import (
    ExpectationSuiteValidationResult,
)
from great_expectations.render.renderer import SlackRenderer


def test_SlackRenderer_validation_results_with_datadocs():

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
            "great_expectations_version": "v0.8.0__develop",
            "batch_kwargs": {"data_asset_name": "x/y/z"},
            "data_asset_name": {
                "datasource": "x",
                "generator": "y",
                "generator_asset": "z",
            },
            "expectation_suite_name": "default",
            "run_id": "2019-09-25T060538.829112Z",
        },
    )

    rendered_output = SlackRenderer().render(validation_result_suite)

    expected_output = {
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*Batch Validation Status*: Success :tada:\n*Expectation suite name*: `default`\n*Data asset name*: `x/y/z`\n*Run ID*: `2019-09-25T060538.829112Z`\n*Batch ID*: `data_asset_name=x/y/z`\n*Summary*: *0* of *0* expectations were met",
                },
            },
            {"type": "divider"},
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": "Learn how to review validation results in Data Docs: https://docs.greatexpectations.io/en/latest/guides/tutorials/getting_started/set_up_data_docs.html",
                    }
                ],
            },
        ],
        "text": "default: Success :tada:",
    }
    print(rendered_output)
    print(expected_output)
    assert rendered_output == expected_output

    data_docs_pages = {"local_site": "file:///localsite/index.html"}
    notify_with = ["local_site"]
    rendered_output = SlackRenderer().render(
        validation_result_suite, data_docs_pages, notify_with
    )

    expected_output = {
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*Batch Validation Status*: Success :tada:\n*Expectation suite name*: `default`\n*Data asset name*: `x/y/z`\n*Run ID*: `2019-09-25T060538.829112Z`\n*Batch ID*: `data_asset_name=x/y/z`\n*Summary*: *0* of *0* expectations were met",
                },
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*DataDocs* can be found here: `file:///localsite/index.html` \n (Please copy and paste link into a browser to view)\n",
                },
            },
            {"type": "divider"},
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": "Learn how to review validation results in Data Docs: https://docs.greatexpectations.io/en/latest/guides/tutorials/getting_started/set_up_data_docs.html",
                    }
                ],
            },
        ],
        "text": "default: Success :tada:",
    }
    assert rendered_output == expected_output

    # not configured
    notify_with = ["fake_site"]
    rendered_output = SlackRenderer().render(
        validation_result_suite, data_docs_pages, notify_with
    )

    expected_output = {
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*Batch Validation Status*: Success :tada:\n*Expectation suite name*: `default`\n*Data asset name*: `x/y/z`\n*Run ID*: `2019-09-25T060538.829112Z`\n*Batch ID*: `data_asset_name=x/y/z`\n*Summary*: *0* of *0* expectations were met",
                },
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*ERROR*: Slack is trying to provide a link to the following DataDocs: `fake_site`, but it is not configured under `data_docs_sites` in the `great_expectations.yml`\n",
                },
            },
            {"type": "divider"},
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": "Learn how to review validation results in Data Docs: https://docs.greatexpectations.io/en/latest/guides/tutorials/getting_started/set_up_data_docs.html",
                    }
                ],
            },
        ],
        "text": "default: Success :tada:",
    }

    assert rendered_output == expected_output


def test_SlackRenderer_get_report_element():
    slack_renderer = SlackRenderer()

    # these should all be caught
    assert slack_renderer._get_report_element(docs_link=None) is None
    assert slack_renderer._get_report_element(docs_link=1) is None
    assert slack_renderer._get_report_element(docs_link=slack_renderer) is None

    # this should work
    assert slack_renderer._get_report_element(docs_link="i_should_work") is not None
