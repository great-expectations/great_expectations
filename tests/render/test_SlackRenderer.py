from great_expectations.core.batch import BatchDefinition, IDDict
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
                    "text": "*Batch Validation Status*: Success :tada:\n*Expectation suite name*: `default`\n*Data "
                    "asset name*: `x/y/z`\n*Run ID*: `2019-09-25T060538.829112Z`\n*Batch ID*: "
                    "`data_asset_name=x/y/z`\n*Summary*: *0* of *0* expectations were met",
                },
            },
            {"type": "divider"},
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": "Learn how to review validation results in Data Docs: "
                        "https://docs.greatexpectations.io/docs/terms/data_docs",
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
                    "text": "*Batch Validation Status*: Success :tada:\n*Expectation suite name*: `default`\n*Data "
                    "asset name*: `x/y/z`\n*Run ID*: `2019-09-25T060538.829112Z`\n*Batch ID*: "
                    "`data_asset_name=x/y/z`\n*Summary*: *0* of *0* expectations were met",
                },
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*DataDocs* can be found here: `file:///localsite/index.html` \n (Please copy and paste "
                    "link into a browser to view)\n",
                },
            },
            {"type": "divider"},
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": "Learn how to review validation results in Data Docs: "
                        "https://docs.greatexpectations.io/docs/terms/data_docs",
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
                    "text": "*Batch Validation Status*: Success :tada:\n*Expectation suite name*: `default`\n*Data "
                    "asset name*: `x/y/z`\n*Run ID*: `2019-09-25T060538.829112Z`\n*Batch ID*: "
                    "`data_asset_name=x/y/z`\n*Summary*: *0* of *0* expectations were met",
                },
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*ERROR*: Slack is trying to provide a link to the following DataDocs: `fake_site`, "
                    "but it is not configured under `data_docs_sites` in the `great_expectations.yml`\n",
                },
            },
            {"type": "divider"},
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": "Learn how to review validation results in Data Docs: "
                        "https://docs.greatexpectations.io/docs/terms/data_docs",
                    }
                ],
            },
        ],
        "text": "default: Success :tada:",
    }

    assert rendered_output == expected_output


def test_SlackRenderer_checkpoint_validation_results_with_datadocs():
    batch_definition = BatchDefinition(
        datasource_name="test_datasource",
        data_connector_name="test_dataconnector",
        data_asset_name="test_data_asset",
        batch_identifiers=IDDict({"id": "my_id"}),
    )
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
            "active_batch_definition": batch_definition,
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
                    "text": "*Batch Validation Status*: Success :tada:\n*Expectation suite name*: `default`\n*Data "
                    "asset name*: `test_data_asset`\n*Run ID*: `2019-09-25T060538.829112Z`\n*Batch ID*: `("
                    ")`\n*Summary*: *0* of *0* expectations were met",
                },
            },
            {"type": "divider"},
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": "Learn how to review validation results in Data Docs: "
                        "https://docs.greatexpectations.io/docs/terms/data_docs",
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
                    "text": "*Batch Validation Status*: Success :tada:\n*Expectation suite name*: `default`\n*Data "
                    "asset name*: `test_data_asset`\n*Run ID*: `2019-09-25T060538.829112Z`\n*Batch ID*: `("
                    ")`\n*Summary*: *0* of *0* expectations were met",
                },
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*DataDocs* can be found here: `file:///localsite/index.html` \n (Please copy and paste "
                    "link into a browser to view)\n",
                },
            },
            {"type": "divider"},
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": "Learn how to review validation results in Data Docs: "
                        "https://docs.greatexpectations.io/docs/terms/data_docs",
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
                    "text": "*Batch Validation Status*: Success :tada:\n*Expectation suite name*: `default`\n*Data "
                    "asset name*: `test_data_asset`\n*Run ID*: `2019-09-25T060538.829112Z`\n*Batch ID*: `("
                    ")`\n*Summary*: *0* of *0* expectations were met",
                },
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*ERROR*: Slack is trying to provide a link to the following DataDocs: `fake_site`, "
                    "but it is not configured under `data_docs_sites` in the `great_expectations.yml`\n",
                },
            },
            {"type": "divider"},
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": "Learn how to review validation results in Data Docs: "
                        "https://docs.greatexpectations.io/docs/terms/data_docs",
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


def test_SlackRenderer_get_failed_expectation_domain_table():
    slack_renderer = SlackRenderer()
    result = slack_renderer.get_failed_expectation_domain(
        "expect_table_columns_to_be_unique", {}
    )
    assert result == "Table"


def test_SlackRenderer_get_failed_expectation_domain_column_pair():
    slack_renderer = SlackRenderer()
    result = slack_renderer.get_failed_expectation_domain(
        "expect_column_pair_to_be_equal", {"column_A": "first", "column_B": "second"}
    )
    assert result == "first, second"


def test_SlackRenderer_get_failed_expectation_domain_column():
    slack_renderer = SlackRenderer()
    result = slack_renderer.get_failed_expectation_domain(
        "expect_column_pair_to_be_equal", {"column": "first"}
    )
    assert result == "first"


def test_SlackRenderer_get_failed_expectation_domain_column_list():
    slack_renderer = SlackRenderer()
    result = slack_renderer.get_failed_expectation_domain(
        "expect_multicolumn_sum_to_be_equal", {"column_list": ["col_a", "col_b"]}
    )
    assert result == "['col_a', 'col_b']"


def test_SlackRenderer_get_failed_expectation_domain_no_domain():
    slack_renderer = SlackRenderer()
    result = slack_renderer.get_failed_expectation_domain(
        "expect_column_pair_to_be_equal", {"date_column": "kpi_date"}
    )
    assert result is None


def test_create_failed_expectations_text():
    validation_result = [
        {
            "exception_info": {
                "raised_exception": False,
                "exception_traceback": None,
                "exception_message": None,
            },
            "success": False,
            "meta": {},
            "result": {"observed_value": 8565},
            "expectation_config": {
                "meta": {},
                "kwargs": {
                    "max_value": 10000,
                    "min_value": 10000,
                    "batch_id": "b9e06d3884bbfb6e3352ced3836c3bc8",
                },
                "expectation_type": "expect_table_row_count_to_be_between",
            },
        }
    ]
    slack_renderer = SlackRenderer()
    result = slack_renderer.create_failed_expectations_text(validation_result)
    assert (
        result
        == """
*Failed Expectations*:
:x:expect_table_row_count_to_be_between (Table)
"""
    )


def test_SlackRenderer_show_failed_expectations():
    validation_result = ExpectationSuiteValidationResult(
        results=[
            {
                "exception_info": {
                    "raised_exception": False,
                    "exception_traceback": None,
                    "exception_message": None,
                },
                "success": False,
                "meta": {},
                "result": {"observed_value": 8565},
                "expectation_config": {
                    "meta": {},
                    "kwargs": {
                        "column": "my_column",
                        "max_value": 10000,
                        "min_value": 10000,
                        "batch_id": "b9e06d3884bbfb6e3352ced3836c3bc8",
                    },
                    "expectation_type": "expect_column_values_to_be_between",
                },
            }
        ],
        success=False,
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
    slack_renderer = SlackRenderer()
    rendered_msg = slack_renderer.render(
        validation_result, show_failed_expectations=True
    )

    assert (
        """*Failed Expectations*:
:x:expect_column_values_to_be_between (my_column)"""
        in rendered_msg["blocks"][0]["text"]["text"]
    )
