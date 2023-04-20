from unittest.mock import Mock

import pytest

from great_expectations.core import (
    ExpectationSuiteValidationResult,
    RunIdentifier,
)
from great_expectations.core.batch import BatchDefinition
from great_expectations.data_context.types.resource_identifiers import (
    BatchIdentifier,
    ExpectationSuiteIdentifier,
    ValidationResultIdentifier,
)
from great_expectations.render.renderer import MicrosoftTeamsRenderer


@pytest.mark.parametrize(
    "result_meta,result_batch_id,",
    [
        (
            {
                "great_expectations_version": "v0.8.0__develop",
                "expectation_suite_name": "asset.default",
                "run_id": "test_100",
            },
            BatchIdentifier(
                batch_identifier="1234", data_asset_name="expected_asset_name"
            ),
        ),
        (
            {
                "great_expectations_version": "v0.8.0__develop",
                "expectation_suite_name": "asset.default",
                "run_id": "test_100",
                "active_batch_definition": Mock(
                    BatchDefinition, data_asset_name="expected_asset_name"
                ),
            },
            "1234",
        ),
    ],
)
def test_MicrosoftTeams_validation_results_with_datadocs(result_meta, result_batch_id):
    validation_result_suite = ExpectationSuiteValidationResult(
        results=[],
        success=True,
        statistics={
            "evaluated_expectations": 0,
            "successful_expectations": 0,
            "unsuccessful_expectations": 0,
            "success_percent": None,
        },
        meta=result_meta,
    )

    validation_result_suite_identifier = ValidationResultIdentifier(
        expectation_suite_identifier=ExpectationSuiteIdentifier("asset.default"),
        run_id=RunIdentifier(
            run_name="test_100", run_time="Tue May 08 15:14:45 +0800 2012"
        ),
        batch_identifier=result_batch_id,
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
                                                    "wrap": "true",
                                                },
                                                {
                                                    "isSubtle": "true",
                                                    "spacing": "none",
                                                    "text": "May "
                                                    "08 "
                                                    "2012 "
                                                    "15:14:45+0800",
                                                    "type": "TextBlock",
                                                    "wrap": "true",
                                                },
                                            ],
                                            "type": "Column",
                                            "width": "stretch",
                                        }
                                    ],
                                    "type": "ColumnSet",
                                }
                            ],
                            "separator": "true",
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
                                    "text": "**Data asset "
                                    "name:** expected_asset_name",
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
                            "separator": "true",
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
