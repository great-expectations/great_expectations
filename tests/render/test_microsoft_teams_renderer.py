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
        "attachments": [
            {
                "content": {
                    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "actions": [
                        {
                            "title": "Open data docs",
                            "type": "Action.OpenUrl",
                            "url": "file:///localsite/index.html",
                        }
                    ],
                    "body": [
                        {
                            "height": "auto",
                            "items": [
                                {
                                    "columns": [
                                        {
                                            "items": [
                                                {
                                                    "size": "large",
                                                    "text": "Validation " "results",
                                                    "type": "TextBlock",
                                                    "weight": "bolder",
                                                    "wrap": True,
                                                },
                                                {
                                                    "isSubtle": True,
                                                    "spacing": "none",
                                                    "text": "May "
                                                    "08 "
                                                    "2012 "
                                                    "07:14:45",
                                                    "type": "TextBlock",
                                                    "wrap": True,
                                                },
                                            ],
                                            "type": "Column",
                                            "width": "stretch",
                                        }
                                    ],
                                    "type": "ColumnSet",
                                }
                            ],
                            "separator": True,
                            "type": "Container",
                        },
                        {
                            "height": "auto",
                            "items": [
                                {
                                    "color": "good",
                                    "horizontalAlignment": "left",
                                    "text": "**Batch validation "
                                    "status:** Success "
                                    "!!!",
                                    "type": "TextBlock",
                                },
                                {
                                    "horizontalAlignment": "left",
                                    "text": "**Data asset " "name:** asset",
                                    "type": "TextBlock",
                                },
                                {
                                    "horizontalAlignment": "left",
                                    "text": "**Expectation "
                                    "suite name:** "
                                    "asset.default",
                                    "type": "TextBlock",
                                },
                                {
                                    "horizontalAlignment": "left",
                                    "text": "**Run name:** " "test_100",
                                    "type": "TextBlock",
                                },
                                {
                                    "horizontalAlignment": "left",
                                    "text": "**Batch ID:** 1234",
                                    "type": "TextBlock",
                                },
                                {
                                    "horizontalAlignment": "left",
                                    "text": "**Summary:** *0* "
                                    "of *0* "
                                    "expectations were "
                                    "met",
                                    "type": "TextBlock",
                                },
                            ],
                            "separator": True,
                            "type": "Container",
                        },
                    ],
                    "type": "AdaptiveCard",
                    "version": "1.0",
                },
                "contentType": "application/vnd.microsoft.card.adaptive",
            }
        ],
        "type": "message",
    }

    assert rendered_output == expected_output
