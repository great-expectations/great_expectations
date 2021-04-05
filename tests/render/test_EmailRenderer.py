from great_expectations.core.expectation_validation_result import (
    ExpectationSuiteValidationResult,
)
from great_expectations.render.renderer import EmailRenderer


def test_EmailRenderer_validation_results_with_datadocs():

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

    rendered_output = EmailRenderer().render(validation_result_suite)

    expected_output = (
        "default: Success 🎉",
        '<p><strong>Batch Validation Status</strong>: Success 🎉</p>\n<p><strong>Expectation suite name</strong>: default</p>\n<p><strong>Data asset name</strong>: x/y/z</p>\n<p><strong>Run ID</strong>: 2019-09-25T060538.829112Z</p>\n<p><strong>Batch ID</strong>: data_asset_name=x/y/z</p>\n<p><strong>Summary</strong>: <strong>0</strong> of <strong>0</strong> expectations were met</p><p>Learn <a href="https://docs.greatexpectations.io/en/latest/guides/tutorials/getting_started/set_up_data_docs.html">here</a> how to review validation results in Data Docs</p>',
    )
    assert rendered_output == expected_output

    data_docs_pages = {"local_site": "file:///localsite/index.html"}
    notify_with = ["local_site"]
    rendered_output = EmailRenderer().render(
        validation_result_suite, data_docs_pages, notify_with
    )

    expected_output = (
        "default: Success 🎉",
        '<p><strong>Batch Validation Status</strong>: Success 🎉</p>\n<p><strong>Expectation suite name</strong>: default</p>\n<p><strong>Data asset name</strong>: x/y/z</p>\n<p><strong>Run ID</strong>: 2019-09-25T060538.829112Z</p>\n<p><strong>Batch ID</strong>: data_asset_name=x/y/z</p>\n<p><strong>Summary</strong>: <strong>0</strong> of <strong>0</strong> expectations were met</p><p><strong>DataDocs</strong> can be found here: <a href="file:///localsite/index.html">file:///localsite/index.html</a>.</br>(Please copy and paste link into a browser to view)</p><p>Learn <a href="https://docs.greatexpectations.io/en/latest/guides/tutorials/getting_started/set_up_data_docs.html">here</a> how to review validation results in Data Docs</p>',
    )
    assert rendered_output == expected_output

    # not configured
    notify_with = ["fake_site"]
    rendered_output = EmailRenderer().render(
        validation_result_suite, data_docs_pages, notify_with
    )

    expected_output = (
        "default: Success 🎉",
        '<p><strong>Batch Validation Status</strong>: Success 🎉</p>\n<p><strong>Expectation suite name</strong>: default</p>\n<p><strong>Data asset name</strong>: x/y/z</p>\n<p><strong>Run ID</strong>: 2019-09-25T060538.829112Z</p>\n<p><strong>Batch ID</strong>: data_asset_name=x/y/z</p>\n<p><strong>Summary</strong>: <strong>0</strong> of <strong>0</strong> expectations were met</p><strong>ERROR</strong>: The email is trying to provide a link to the following DataDocs: `fake_site`, but it is not configured under data_docs_sites in the great_expectations.yml</br><p>Learn <a href="https://docs.greatexpectations.io/en/latest/guides/tutorials/getting_started/set_up_data_docs.html">here</a> how to review validation results in Data Docs</p>',
    )

    assert rendered_output == expected_output


def test_EmailRenderer_get_report_element():
    email_renderer = EmailRenderer()

    # these should all be caught
    assert email_renderer._get_report_element(docs_link=None) is None
    assert email_renderer._get_report_element(docs_link=1) is None
    assert email_renderer._get_report_element(docs_link=email_renderer) is None

    # this should work
    assert email_renderer._get_report_element(docs_link="i_should_work") is not None
