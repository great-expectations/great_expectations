from freezegun import freeze_time

from great_expectations.core import ExpectationSuiteValidationResult
from great_expectations.render.renderer import SlackRenderer

from ..test_utils import modify_locale


@modify_locale
@freeze_time("09/24/2019 23:18:36")
def test_SlackRenderer():
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
    print(rendered_output)

    expected_renderer_output = {
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*Batch Validation Status*: Success :tada:\n*Expectation suite name*: `default`\n*Run ID*: `2019-09-25T060538.829112Z`\n*Batch ID*: `None`\n*Summary*: *0* of *0* expectations were met",
                },
            },
            {"type": "divider"},
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": "Learn how to review validation results in Data Docs: https://docs.greatexpectations.io/en/latest/guides/tutorials/getting_started/set_up_data_docs.html#set-up-data-docs",
                    }
                ],
            },
        ],
        "text": "default: Success :tada:",
    }

    # We're okay with system variation in locales (OS X likes 24 hour, but not Travis)
    expected_renderer_output["blocks"][0]["text"]["text"] = expected_renderer_output[
        "blocks"
    ][0]["text"]["text"].replace("09/24/2019 11:18:36 PM", "LOCALEDATE")
    expected_renderer_output["blocks"][0]["text"]["text"] = expected_renderer_output[
        "blocks"
    ][0]["text"]["text"].replace("09/24/2019 23:18:36", "LOCALEDATE")
    rendered_output["blocks"][0]["text"]["text"] = rendered_output["blocks"][0]["text"][
        "text"
    ].replace("09/24/2019 11:18:36 PM UTC", "LOCALEDATE")
    rendered_output["blocks"][0]["text"]["text"] = rendered_output["blocks"][0]["text"][
        "text"
    ].replace("09/24/2019 23:18:36 UTC", "LOCALEDATE")

    assert rendered_output == expected_renderer_output
