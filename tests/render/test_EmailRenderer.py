import pytest

from great_expectations.core.batch import IDDict, LegacyBatchDefinition
from great_expectations.core.expectation_validation_result import (
    ExpectationSuiteValidationResult,
)
from great_expectations.render.renderer import EmailRenderer


@pytest.mark.big
def test_EmailRenderer_validation_results_with_datadocs():
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

    rendered_output = EmailRenderer().render(validation_result_suite)

    expected_output = (
        "default: Success 🎉",
        '<p><strong>Batch Validation Status</strong>: Success 🎉</p>\n<p><strong>Expectation suite name</strong>: default</p>\n<p><strong>Data asset name</strong>: x/y/z</p>\n<p><strong>Run ID</strong>: 2019-09-25T060538.829112Z</p>\n<p><strong>Batch ID</strong>: data_asset_name=x/y/z</p>\n<p><strong>Summary</strong>: <strong>0</strong> of <strong>0</strong> expectations were met</p><p>Learn <a href="https://docs.greatexpectations.io/docs/terms/data_docs">here</a> how to review validation results in Data Docs</p>',  # noqa: E501
    )
    assert rendered_output == expected_output

    data_docs_pages = {"local_site": "file:///localsite/index.html"}
    notify_with = ["local_site"]
    rendered_output = EmailRenderer().render(validation_result_suite, data_docs_pages, notify_with)

    expected_output = (
        "default: Success 🎉",
        '<p><strong>Batch Validation Status</strong>: Success 🎉</p>\n<p><strong>Expectation suite name</strong>: default</p>\n<p><strong>Data asset name</strong>: x/y/z</p>\n<p><strong>Run ID</strong>: 2019-09-25T060538.829112Z</p>\n<p><strong>Batch ID</strong>: data_asset_name=x/y/z</p>\n<p><strong>Summary</strong>: <strong>0</strong> of <strong>0</strong> expectations were met</p><p><strong>DataDocs</strong> can be found here: <a href="file:///localsite/index.html">file:///localsite/index.html</a>.</br>(Please copy and paste link into a browser to view)</p><p>Learn <a href="https://docs.greatexpectations.io/docs/terms/data_docs">here</a> how to review validation results in Data Docs</p>',  # noqa: E501
    )
    assert rendered_output == expected_output

    # not configured
    notify_with = ["fake_site"]
    rendered_output = EmailRenderer().render(validation_result_suite, data_docs_pages, notify_with)

    expected_output = (
        "default: Success 🎉",
        '<p><strong>Batch Validation Status</strong>: Success 🎉</p>\n<p><strong>Expectation suite name</strong>: default</p>\n<p><strong>Data asset name</strong>: x/y/z</p>\n<p><strong>Run ID</strong>: 2019-09-25T060538.829112Z</p>\n<p><strong>Batch ID</strong>: data_asset_name=x/y/z</p>\n<p><strong>Summary</strong>: <strong>0</strong> of <strong>0</strong> expectations were met</p><strong>ERROR</strong>: The email is trying to provide a link to the following DataDocs: `fake_site`, but it is not configured under data_docs_sites in the great_expectations.yml</br><p>Learn <a href="https://docs.greatexpectations.io/docs/terms/data_docs">here</a> how to review validation results in Data Docs</p>',  # noqa: E501
    )

    assert rendered_output == expected_output


@pytest.mark.big
def test_EmailRenderer_checkpoint_validation_results_with_datadocs():
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
            "great_expectations_version": "v0.8.0__develop",
            "active_batch_definition": batch_definition,
            "expectation_suite_name": "default",
            "run_id": "2019-09-25T060538.829112Z",
        },
    )

    rendered_output = EmailRenderer().render(validation_result_suite)

    expected_output = (
        "default: Success 🎉",
        '<p><strong>Batch Validation Status</strong>: Success 🎉</p>\n<p><strong>Expectation suite name</strong>: default</p>\n<p><strong>Data asset name</strong>: test_data_asset</p>\n<p><strong>Run ID</strong>: 2019-09-25T060538.829112Z</p>\n<p><strong>Batch ID</strong>: ()</p>\n<p><strong>Summary</strong>: <strong>0</strong> of <strong>0</strong> expectations were met</p><p>Learn <a href="https://docs.greatexpectations.io/docs/terms/data_docs">here</a> how to review validation results in Data Docs</p>',  # noqa: E501
    )
    assert rendered_output == expected_output

    data_docs_pages = {"local_site": "file:///localsite/index.html"}
    notify_with = ["local_site"]
    rendered_output = EmailRenderer().render(validation_result_suite, data_docs_pages, notify_with)

    expected_output = (
        "default: Success 🎉",
        '<p><strong>Batch Validation Status</strong>: Success 🎉</p>\n<p><strong>Expectation suite name</strong>: default</p>\n<p><strong>Data asset name</strong>: test_data_asset</p>\n<p><strong>Run ID</strong>: 2019-09-25T060538.829112Z</p>\n<p><strong>Batch ID</strong>: ()</p>\n<p><strong>Summary</strong>: <strong>0</strong> of <strong>0</strong> expectations were met</p><p><strong>DataDocs</strong> can be found here: <a href="file:///localsite/index.html">file:///localsite/index.html</a>.</br>(Please copy and paste link into a browser to view)</p><p>Learn <a href="https://docs.greatexpectations.io/docs/terms/data_docs">here</a> how to review validation results in Data Docs</p>',  # noqa: E501
    )
    assert rendered_output == expected_output

    # not configured
    notify_with = ["fake_site"]
    rendered_output = EmailRenderer().render(validation_result_suite, data_docs_pages, notify_with)

    expected_output = (
        "default: Success 🎉",
        '<p><strong>Batch Validation Status</strong>: Success 🎉</p>\n<p><strong>Expectation suite name</strong>: default</p>\n<p><strong>Data asset name</strong>: test_data_asset</p>\n<p><strong>Run ID</strong>: 2019-09-25T060538.829112Z</p>\n<p><strong>Batch ID</strong>: ()</p>\n<p><strong>Summary</strong>: <strong>0</strong> of <strong>0</strong> expectations were met</p><strong>ERROR</strong>: The email is trying to provide a link to the following DataDocs: `fake_site`, but it is not configured under data_docs_sites in the great_expectations.yml</br><p>Learn <a href="https://docs.greatexpectations.io/docs/terms/data_docs">here</a> how to review validation results in Data Docs</p>',  # noqa: E501
    )

    assert rendered_output == expected_output


@pytest.mark.big
def test_EmailRenderer_get_report_element():
    email_renderer = EmailRenderer()

    # these should all be caught
    assert email_renderer._get_report_element(docs_link=None) is None
    assert email_renderer._get_report_element(docs_link=1) is None
    assert email_renderer._get_report_element(docs_link=email_renderer) is None

    # this should work
    assert email_renderer._get_report_element(docs_link="i_should_work") is not None


@pytest.mark.unit
def test_EmailRenderer_v1_render(v1_checkpoint_result):
    email_renderer = EmailRenderer()
    _, raw_html = email_renderer.v1_render(checkpoint_result=v1_checkpoint_result)
    html_blocks = raw_html.split("\n")

    assert html_blocks == [
        "<p><strong><h3><u>my_bad_suite</u></h3></strong></p>",
        "<p><strong>Batch Validation Status</strong>: Failed ❌</p>",
        "<p><strong>Expectation Suite Name</strong>: my_bad_suite</p>",
        "<p><strong>Data Asset Name</strong>: my_first_asset</p>",
        "<p><strong>Run ID</strong>: __no_run_id__</p>",
        "<p><strong>Batch ID</strong>: my_batch</p>",
        "<p><strong>Summary</strong>: <strong>3</strong> of <strong>5</strong> expectations were met</p><br><p><strong><h3><u>my_good_suite</u></h3></strong></p>",  # noqa: E501
        "<p><strong>Batch Validation Status</strong>: Success 🎉</p>",
        "<p><strong>Expectation Suite Name</strong>: my_good_suite</p>",
        "<p><strong>Data Asset Name</strong>: __no_asset_name__</p>",
        "<p><strong>Run ID</strong>: my_run_id</p>",
        "<p><strong>Batch ID</strong>: my_other_batch</p>",
        "<p><strong>Summary</strong>: <strong>1</strong> of <strong>1</strong> expectations were met</p>",  # noqa: E501
    ]
