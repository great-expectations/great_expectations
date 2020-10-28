import pytest

from great_expectations.core import ExpectationSuiteValidationResult, RunIdentifier
from great_expectations.data_context.types.resource_identifiers import (
    BatchIdentifier,
    ExpectationSuiteIdentifier,
    ValidationResultIdentifier,
)
from great_expectations.render.renderer import MicrosoftTeamsRenderer


def test_MicrosoftTeams_validation_results_with_datadocs():

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
            "expectation_suite_name": "asset.default",
            "run_id": "test_100",
        },
    )

    validation_result_suite_identifier = ValidationResultIdentifier(
        expectation_suite_identifier=ExpectationSuiteIdentifier("asset.default"),
        run_id=RunIdentifier(
            run_name="test_100", run_time="Tue May 08 15:14:45 +0800 2012"
        ),
        batch_identifier=BatchIdentifier(
            batch_identifier="1234", data_asset_name="asset"
        ),
    )

    data_docs_pages = {"local_site": "file:///localsite/index.html"}

    rendered_output = MicrosoftTeamsRenderer().render(
        validation_result_suite, validation_result_suite_identifier, data_docs_pages
    )

    expected_output = {
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "type": "AdaptiveCard",
                    "version": "1.0",
                    "body": [
                        {
                            "type": "Container",
                            "height": "auto",
                            "separator": True,
                            "items": [
                                {
                                    "type": "ColumnSet",
                                    "columns": [
                                        {
                                            "type": "Column",
                                            "width": "stretch",
                                            "items": [
                                                {
                                                    "type": "TextBlock",
                                                    "text": "Validation results",
                                                    "weight": "bolder",
                                                    "size": "large",
                                                    "wrap": True,
                                                },
                                                {
                                                    "type": "TextBlock",
                                                    "spacing": "none",
                                                    "text": "May 08 2012 07:14:45",
                                                    "isSubtle": True,
                                                    "wrap": True,
                                                },
                                            ],
                                        }
                                    ],
                                }
                            ],
                        },
                        {
                            "type": "Container",
                            "height": "auto",
                            "separator": True,
                            "items": [
                                {
                                    "type": "TextBlock",
                                    "text": "**Batch validation status:** Failed :(",
                                    "horizontalAlignment": "left",
                                    "color": "attention",
                                },
                                {
                                    "type": "TextBlock",
                                    "text": "**Data asset name:** asset",
                                    "horizontalAlignment": "left",
                                },
                                {
                                    "type": "TextBlock",
                                    "text": "**Expectation suite name:** asset.default",
                                    "horizontalAlignment": "left",
                                },
                                {
                                    "type": "TextBlock",
                                    "text": "**Run name:** test_100",
                                    "horizontalAlignment": "left",
                                },
                                {
                                    "type": "TextBlock",
                                    "text": "**Batch ID:** 1234",
                                    "horizontalAlignment": "left",
                                },
                                {
                                    "type": "TextBlock",
                                    "text": "**Summary:** *0* of *0* expectations were met",
                                    "horizontalAlignment": "left",
                                },
                            ],
                        },
                    ],
                    "actions": [
                        {
                            "type": "Action.OpenUrl",
                            "title": "Open data docs",
                            "url": "file:///localsite/index.html",
                        }
                    ],
                },
            }
        ],
    }

    print(rendered_output)
    print(expected_output)

    assert rendered_output == expected_output
