import textwrap

import pytest

from great_expectations.core.expectation_validation_result import (
    ExpectationSuiteValidationResult,
    ExpectationSuiteValidationResultMeta,
)
from great_expectations.render.renderer import EmailRenderer


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
def test_EmailRenderer_render(v1_checkpoint_result):
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
    data_docs_pages = {"local_site": "file:///localsite/index.html"}

    email_renderer = EmailRenderer()

    html_blocks = email_renderer.render(
        validation_result=validation_result,
        data_docs_pages=data_docs_pages,
        validation_result_urls=["file:///localsite/index.html"],
    )

    overview_block = html_blocks[0]
    data_docs_block = html_blocks[1]

    run_id = {
        "run_name": "20220727-114327-my-run-name-template",
        "run_time": "2022-07-27T11:43:27.625252+00:00",
    }

    summary = "<strong>3</strong> of <strong>3</strong> expectations were met"

    assert overview_block == textwrap.dedent(
        f"""\
        <p><strong>Batch Validation Status</strong>: Success ✅</p>
        <p><strong>Expectation Suite Name</strong>: my_suite</p>
        <p><strong>Data Asset Name</strong>: taxi_data_1.csv</p>
        <p><strong>Run ID</strong>: {run_id}</p>
        <p><strong>Batch ID</strong>: None</p>
        <p><strong>Summary</strong>: {summary}</p>"""
    )
    assert data_docs_block == (
        """<p><strong>DataDocs</strong> can be found here: <a href="file:/localsite/index.html">"""
        + "file:/localsite/index.html</a>.</br>"
        + "(Please copy and paste link into a browser to view)</p>"
    )
