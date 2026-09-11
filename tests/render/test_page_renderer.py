from __future__ import annotations

import json
import re
from pprint import pformat as pf
from typing import TYPE_CHECKING

import mistune
import pytest

from great_expectations.checkpoint import UpdateDataDocsAction
from great_expectations.compatibility.pydantic import AnyUrl, parse_obj_as
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.expectation_validation_result import (
    ExpectationSuiteValidationResult,
)
from great_expectations.core.run_identifier import RunIdentifier
from great_expectations.core.validation_definition import ValidationDefinition
from great_expectations.data_context.util import file_relative_path
from great_expectations.expectations.expectation_configuration import (
    ExpectationConfiguration,
)
from great_expectations.render import RenderedContent, RenderedDocumentContent
from great_expectations.render.renderer import (
    ExpectationSuitePageRenderer,
    ProfilingResultsPageRenderer,
    ValidationResultsPageRenderer,
)

if TYPE_CHECKING:
    from pytest_mock import MockerFixture

    from great_expectations.core.expectation_validation_result import (
        ExpectationValidationResult,
    )

# module level markers
pytestmark = pytest.mark.filesystem


def test_ExpectationSuitePageRenderer_render_expectation_suite_notes():
    result = ExpectationSuitePageRenderer._render_expectation_suite_notes(
        ExpectationSuite(name="test", notes="*hi*")
    )
    # print(RenderedContent.rendered_content_list_to_json(result.text))
    assert RenderedContent.rendered_content_list_to_json(result.text) == [
        "This Expectation suite currently contains 0 total Expectations across 0 columns.",
        {
            "content_block_type": "markdown",
            "markdown": "*hi*",
            "styling": {"parent": {}},
        },
    ]

    result = ExpectationSuitePageRenderer._render_expectation_suite_notes(
        ExpectationSuite(
            name="test",
            notes=["*alpha*", "_bravo_", "charlie"],
        )
    )
    # print(RenderedContent.rendered_content_list_to_json(result.text))
    assert RenderedContent.rendered_content_list_to_json(result.text) == [
        "This Expectation suite currently contains 0 total Expectations across 0 columns.",
        {
            "content_block_type": "markdown",
            "markdown": "*alpha*",
            "styling": {"parent": {}},
        },
        {
            "content_block_type": "markdown",
            "markdown": "_bravo_",
            "styling": {"parent": {}},
        },
        {
            "content_block_type": "markdown",
            "markdown": "charlie",
            "styling": {"parent": {}},
        },
    ]

    result = ExpectationSuitePageRenderer._render_expectation_suite_notes(
        ExpectationSuite(
            name="test",
            notes="*alpha*",
        )
    )
    # print(RenderedContent.rendered_content_list_to_json(result.text))

    try:
        mistune.markdown("*test*")
        assert RenderedContent.rendered_content_list_to_json(result.text) == [
            "This Expectation suite currently contains 0 total Expectations across 0 columns.",
            {
                "content_block_type": "markdown",
                "styling": {"parent": {}},
                "markdown": "*alpha*",
            },
        ]
    except OSError:
        assert RenderedContent.rendered_content_list_to_json(result.text) == [
            "This Expectation suite currently contains 0 total Expectations across 0 columns.",
            "*alpha*",
        ]

    result = ExpectationSuitePageRenderer._render_expectation_suite_notes(
        ExpectationSuite(
            name="test",
            notes=["*alpha*", "_bravo_", "charlie"],
        )
    )
    # print(RenderedContent.rendered_content_list_to_json(result.text))

    try:
        mistune.markdown("*test*")
        assert RenderedContent.rendered_content_list_to_json(result.text) == [
            "This Expectation suite currently contains 0 total Expectations across 0 columns.",
            {
                "content_block_type": "markdown",
                "styling": {"parent": {}},
                "markdown": "*alpha*",
            },
            {
                "content_block_type": "markdown",
                "styling": {"parent": {}},
                "markdown": "_bravo_",
            },
            {
                "content_block_type": "markdown",
                "styling": {"parent": {}},
                "markdown": "charlie",
            },
        ]
    except OSError:
        assert RenderedContent.rendered_content_list_to_json(result.text) == [
            "This Expectation suite currently contains 0 total Expectations across 0 columns.",
            "*alpha*",
            "_bravo_",
            "charlie",
        ]


def test_expectation_summary_in_ExpectationSuitePageRenderer_render_expectation_suite_notes():
    result = ExpectationSuitePageRenderer._render_expectation_suite_notes(
        ExpectationSuite(
            name="test",
            meta={},
            expectations=None,
        )
    )
    # print(RenderedContent.rendered_content_list_to_json(result.text))
    assert RenderedContent.rendered_content_list_to_json(result.text) == [
        "This Expectation suite currently contains 0 total Expectations across 0 columns."
    ]

    result = ExpectationSuitePageRenderer._render_expectation_suite_notes(
        ExpectationSuite(
            name="test",
            notes=["hi"],
        )
    )
    # print(RenderedContent.rendered_content_list_to_json(result.text))

    try:
        mistune.markdown("*test*")
        assert RenderedContent.rendered_content_list_to_json(result.text) == [
            "This Expectation suite currently contains 0 total Expectations across 0 columns.",
            {
                "content_block_type": "markdown",
                "styling": {"parent": {}},
                "markdown": "hi",
            },
        ]
    except OSError:
        assert RenderedContent.rendered_content_list_to_json(result.text) == [
            "This Expectation suite currently contains 0 total Expectations across 0 columns.",
            "hi",
        ]

    result = ExpectationSuitePageRenderer._render_expectation_suite_notes(
        ExpectationSuite(
            name="test",
            meta={},
            expectations=[
                ExpectationConfiguration(
                    type="expect_table_row_count_to_be_between",
                    kwargs={"min_value": 0, "max_value": None},
                ),
                ExpectationConfiguration(type="expect_column_to_exist", kwargs={"column": "x"}),
                ExpectationConfiguration(type="expect_column_to_exist", kwargs={"column": "y"}),
            ],
        )
    )
    # print(RenderedContent.rendered_content_list_to_json(result.text)[0])
    assert (
        RenderedContent.rendered_content_list_to_json(result.text)[0]
        == "This Expectation suite currently contains 3 total Expectations across 2 columns."
    )


def test_ProfilingResultsPageRenderer(
    titanic_profiled_evrs_1: ExpectationValidationResult,
):
    document = ProfilingResultsPageRenderer().render(titanic_profiled_evrs_1)
    assert isinstance(document, RenderedDocumentContent)
    assert len(document.sections) == 8


def test_ValidationResultsPageRenderer_render_validation_header(
    titanic_profiled_evrs_1: ExpectationValidationResult,
):
    validation_header = ValidationResultsPageRenderer._render_validation_header(
        titanic_profiled_evrs_1
    ).to_json_dict()

    expected_validation_header = {
        "content_block_type": "header",
        "styling": {
            "classes": ["col-12", "p-0"],
            "header": {"classes": ["alert", "alert-secondary"]},
        },
        "header": {
            "content_block_type": "string_template",
            "string_template": {
                "template": "Overview",
                "tag": "h5",
                "styling": {"classes": ["m-0"]},
            },
        },
        "subheader": {
            "content_block_type": "string_template",
            "string_template": {
                "template": "${suite_title} ${expectation_suite_name}\n ${data_asset} ${data_asset_name}\n ${status_title} ${html_success_icon} ${success}",  # noqa: E501 # FIXME CoP
                "params": {
                    "suite_title": "Expectation Suite:",
                    "data_asset": "Data asset:",
                    "data_asset_name": None,
                    "status_title": "Status:",
                    "expectation_suite_name": "default",
                    "success": "Failed",
                    "html_success_icon": '<i class="fas fa-times text-danger" aria-hidden="true"></i>',  # noqa: E501 # FIXME CoP
                },
                "styling": {
                    "params": {
                        "suite_title": {"classes": ["h6"]},
                        "status_title": {"classes": ["h6"]},
                        "expectation_suite_name": {
                            "tag": "a",
                            "attributes": {"href": "../../../../expectations/default.html"},
                        },
                    },
                    "classes": ["mb-0", "mt-1"],
                },
            },
        },
    }
    import pprint

    pprint.pprint(validation_header)

    assert validation_header == expected_validation_header


def test_ValidationResultsPageRenderer_render_validation_info(
    titanic_profiled_evrs_1: ExpectationValidationResult,
):
    validation_info = ValidationResultsPageRenderer._render_validation_info(
        titanic_profiled_evrs_1
    ).to_json_dict()
    print(validation_info)

    expected_validation_info = {
        "content_block_type": "table",
        "styling": {
            "classes": ["col-12", "table-responsive", "mt-1"],
            "body": {
                "classes": ["table", "table-sm"],
                "styles": {
                    "margin-bottom": "0.5rem !important",
                    "margin-top": "0.5rem !important",
                },
            },
        },
        "header": {
            "content_block_type": "string_template",
            "string_template": {
                "template": "Info",
                "tag": "h6",
                "styling": {"classes": ["m-0"]},
            },
        },
        "table": [
            ["Great Expectations Version", "0.9.7+17.g02805059.dirty"],
            ["Run Name", "20200322T170247.671855Z"],
            ["Run Time", "2020-03-22T17:02:47Z"],
        ],
    }
    assert validation_info == expected_validation_info


def test_ValidationResultsPageRenderer_render_validation_statistics(
    titanic_profiled_evrs_1,
):
    validation_statistics = ValidationResultsPageRenderer._render_validation_statistics(
        titanic_profiled_evrs_1
    ).to_json_dict()
    print(validation_statistics)
    expected_validation_statistics = {
        "content_block_type": "table",
        "styling": {
            "classes": ["col-6", "table-responsive", "mt-1", "p-1"],
            "body": {
                "classes": ["table", "table-sm"],
                "styles": {
                    "margin-bottom": "0.5rem !important",
                    "margin-top": "0.5rem !important",
                },
            },
        },
        "header": {
            "content_block_type": "string_template",
            "string_template": {
                "template": "Statistics",
                "tag": "h6",
                "styling": {"classes": ["m-0"]},
            },
        },
        "table": [
            ["Evaluated Expectations", 51],
            ["Successful Expectations", 43],
            ["Unsuccessful Expectations", 8],
            ["Success Percent", "≈84.31%"],
        ],
    }

    assert validation_statistics == expected_validation_statistics


@pytest.mark.filesystem
def test_ValidationResultsPageRenderer_render_nested_table_from_dict():
    batch_kwargs = {
        "path": "project_dir/project_path/data/titanic/Titanic.csv",
        "datasource": "Titanic",
        "reader_options": {"sep": None, "engine": "python"},
    }
    batch_kwargs_table = ValidationResultsPageRenderer._render_nested_table_from_dict(
        batch_kwargs, header="Batch Kwargs"
    ).to_json_dict()
    print(batch_kwargs_table)

    expected_batch_kwarg_table = {
        "content_block_type": "table",
        "styling": {
            "body": {
                "classes": ["table", "table-sm"],
                "styles": {
                    "margin-bottom": "0.5rem !important",
                    "margin-top": "0.5rem !important",
                },
            }
        },
        "header": {
            "content_block_type": "string_template",
            "string_template": {
                "template": "Batch Kwargs",
                "tag": "h6",
                "styling": {"classes": ["m-0"]},
            },
        },
        "table": [
            [
                {
                    "content_block_type": "string_template",
                    "styling": {"parent": {"classes": ["pr-3"]}},
                    "string_template": {
                        "template": "$value",
                        "params": {"value": "datasource"},
                        "styling": {"default": {"styles": {"word-break": "break-all"}}},
                    },
                },
                {
                    "content_block_type": "string_template",
                    "styling": {"parent": {"classes": []}},
                    "string_template": {
                        "template": "$value",
                        "params": {"value": "Titanic"},
                        "styling": {"default": {"styles": {"word-break": "break-all"}}},
                    },
                },
            ],
            [
                {
                    "content_block_type": "string_template",
                    "styling": {"parent": {"classes": ["pr-3"]}},
                    "string_template": {
                        "template": "$value",
                        "params": {"value": "path"},
                        "styling": {"default": {"styles": {"word-break": "break-all"}}},
                    },
                },
                {
                    "content_block_type": "string_template",
                    "styling": {"parent": {"classes": []}},
                    "string_template": {
                        "template": "$value",
                        "params": {"value": "project_dir/project_path/data/titanic/Titanic.csv"},
                        "styling": {"default": {"styles": {"word-break": "break-all"}}},
                    },
                },
            ],
            [
                {
                    "content_block_type": "string_template",
                    "styling": {"parent": {"classes": ["pr-3"]}},
                    "string_template": {
                        "template": "$value",
                        "params": {"value": "reader_options"},
                        "styling": {"default": {"styles": {"word-break": "break-all"}}},
                    },
                },
                {
                    "content_block_type": "table",
                    "styling": {
                        "classes": ["col-6", "table-responsive"],
                        "body": {"classes": ["table", "table-sm", "m-0"]},
                        "parent": {"classes": ["pt-0", "pl-0", "border-top-0"]},
                    },
                    "table": [
                        [
                            {
                                "content_block_type": "string_template",
                                "styling": {"parent": {"classes": ["pr-3"]}},
                                "string_template": {
                                    "template": "$value",
                                    "params": {"value": "engine"},
                                    "styling": {"default": {"styles": {"word-break": "break-all"}}},
                                },
                            },
                            {
                                "content_block_type": "string_template",
                                "styling": {"parent": {"classes": []}},
                                "string_template": {
                                    "template": "$value",
                                    "params": {"value": "python"},
                                    "styling": {"default": {"styles": {"word-break": "break-all"}}},
                                },
                            },
                        ],
                        [
                            {
                                "content_block_type": "string_template",
                                "styling": {"parent": {"classes": ["pr-3"]}},
                                "string_template": {
                                    "template": "$value",
                                    "params": {"value": "sep"},
                                    "styling": {"default": {"styles": {"word-break": "break-all"}}},
                                },
                            },
                            {
                                "content_block_type": "string_template",
                                "styling": {"parent": {"classes": []}},
                                "string_template": {
                                    "template": "$value",
                                    "params": {"value": "None"},
                                    "styling": {"default": {"styles": {"word-break": "break-all"}}},
                                },
                            },
                        ],
                    ],
                },
            ],
        ],
    }

    assert batch_kwargs_table == expected_batch_kwarg_table


@pytest.fixture()
def ValidationResultsPageRenderer_render_with_run_info_at_end():
    """
    Rendered validation results with run info at the end
    Returns:
        json string of rendered validation results
    """
    fixture_filename = file_relative_path(
        __file__,
        "./fixtures/ValidationResultsPageRenderer_render_with_run_info_at_end.json",
    )
    with open(fixture_filename) as infile:
        rendered_validation_results = json.load(infile)
        return rendered_validation_results


@pytest.fixture()
def ValidationResultsPageRenderer_render_with_run_info_at_start():
    """
    Rendered validation results with run info at the start
    Returns:
        json string of rendered validation results
    """
    fixture_filename = file_relative_path(
        __file__,
        "./fixtures/ValidationResultsPageRenderer_render_with_run_info_at_start.json",
    )
    with open(fixture_filename) as infile:
        rendered_validation_results = json.load(infile)
        return rendered_validation_results


def _normalize_schema(graph: dict) -> None:
    """Normalize $schema version to v4.8.1."""
    if "$schema" in graph:
        graph["$schema"] = re.sub(r"v\d*\.\d*\.\d*", "v4.8.1", graph["$schema"])


def _normalize_mark(graph: dict) -> None:
    """Normalize mark property: convert string to dict form if needed."""
    if "mark" in graph and isinstance(graph["mark"], str):
        graph["mark"] = {"type": graph["mark"]}


def _normalize_continuous_width(graph: dict) -> None:
    """Normalize continuousWidth to 300."""
    config = graph.get("config")
    if isinstance(config, dict):
        view = config.get("view")
        if isinstance(view, dict) and "continuousWidth" in view:
            view["continuousWidth"] = 300


def _normalize_dataset_names(graph: dict) -> None:
    """Normalize dataset names to stable 'data-inline'."""
    datasets = graph.get("datasets")
    data = graph.get("data")
    if isinstance(datasets, dict) and isinstance(data, dict):
        old_name = data.get("name")
        if old_name and old_name in datasets:
            datasets["data-inline"] = datasets.pop(old_name)
            data["name"] = "data-inline"


def _normalize_single_graph(graph: dict) -> None:
    """Normalize a single graph block."""
    _normalize_schema(graph)
    _normalize_mark(graph)
    _normalize_continuous_width(graph)
    _normalize_dataset_names(graph)


def _normalize_graph_blocks(obj: dict | list) -> None:
    """
    Recursively normalize graph blocks in rendered output to make them
    compatible across Altair versions.

    Normalizes:
    - $schema: strips version to v4.8.1
    - mark: converts to dict form {"type": "bar"} if string
    - config.view.continuousWidth: sets to 300
    - dataset names: replaces with stable "data-inline"
    """
    if isinstance(obj, dict):
        if "graph" in obj and isinstance(obj["graph"], dict):
            _normalize_single_graph(obj["graph"])
        for value in obj.values():
            _normalize_graph_blocks(value)
    elif isinstance(obj, list):
        for item in obj:
            _normalize_graph_blocks(item)


def test_snapshot_ValidationResultsPageRenderer_render_with_run_info_at_end(
    titanic_profiled_evrs_1: ExpectationSuiteValidationResult,
    ValidationResultsPageRenderer_render_with_run_info_at_end,
):
    validation_results_page_renderer = ValidationResultsPageRenderer(run_info_at_end=True)
    rendered_validation_results = validation_results_page_renderer.render(
        titanic_profiled_evrs_1
    ).to_json_dict()

    # Normalize graph blocks for Altair version compatibility
    _normalize_graph_blocks(rendered_validation_results)
    # Also normalize the fixture for consistency
    fixture_copy = json.loads(json.dumps(ValidationResultsPageRenderer_render_with_run_info_at_end))
    _normalize_graph_blocks(fixture_copy)

    assert rendered_validation_results == fixture_copy


def test_snapshot_ValidationResultsPageRenderer_render_with_run_info_at_start(
    titanic_profiled_evrs_1: ExpectationSuiteValidationResult,
    ValidationResultsPageRenderer_render_with_run_info_at_start,
):
    validation_results_page_renderer = ValidationResultsPageRenderer(run_info_at_end=False)
    rendered_validation_results = validation_results_page_renderer.render(
        titanic_profiled_evrs_1
    ).to_json_dict()

    # Normalize graph blocks for Altair version compatibility
    _normalize_graph_blocks(rendered_validation_results)
    # Also normalize the fixture for consistency
    fixture_copy = json.loads(
        json.dumps(ValidationResultsPageRenderer_render_with_run_info_at_start)
    )
    _normalize_graph_blocks(fixture_copy)

    assert rendered_validation_results == fixture_copy


def test_asset_name_is_part_of_resource_info_index(mocker: MockerFixture):
    """
    DefaultSiteIndexBuilder.add_resource_info_to_index_links_dict is what supplies the
    the resource meta-data to the index page.
    This test checks that the asset_name is being provided when checkpoints are run.
    """
    import great_expectations as gx
    from great_expectations.render.renderer.site_builder import DefaultSiteIndexBuilder

    context = gx.get_context(mode="ephemeral")

    add_resource_info_spy = mocker.spy(
        DefaultSiteIndexBuilder,
        "add_resource_info_to_index_links_dict",
    )

    data_asset = context.data_sources.pandas_default.add_csv_asset(
        "my_asset",
        parse_obj_as(
            AnyUrl,
            "https://raw.githubusercontent.com/great-expectations/gx_tutorials/main/data/yellow_tripdata_sample_2019-01.csv",
        ),
    )

    batch_definition = data_asset.add_batch_definition_whole_dataframe("my_batch")

    # Create Expectation Suite containing two Expectations.
    suite = context.suites.add(ExpectationSuite(name="expectations"))
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="passenger_count", min_value=1, max_value=6
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(column="fare_amount", min_value=0)
    )

    # Create Validation Definition.
    validation_definition = context.validation_definitions.add(
        ValidationDefinition(
            name="validation definition",
            data=batch_definition,
            suite=suite,
        )
    )

    # Create Checkpoint, run Checkpoint, and capture result.
    checkpoint = context.checkpoints.add(
        gx.checkpoint.checkpoint.Checkpoint(
            name="checkpoint",
            validation_definitions=[validation_definition],
            actions=[UpdateDataDocsAction(name="update_data_docs")],
        )
    )

    checkpoint.run()

    # check `DefaultSiteIndexBuilder.add_resource_info_to_index_links_dict` has the asset name
    add_resource_info_spy.assert_called()
    last_call = add_resource_info_spy.call_args_list[-1]
    print(f"Last call kwargs:\n{pf(last_call.kwargs, depth=1)}")
    assert last_call.kwargs["asset_name"] == data_asset.name


def _validation_result(meta: dict | None = None) -> ExpectationSuiteValidationResult:
    """A result whose meta has no "run_id" key unless one is passed in."""
    return ExpectationSuiteValidationResult(
        success=True,
        results=[],
        suite_name="my_suite",
        statistics={
            "evaluated_expectations": 0,
            "successful_expectations": 0,
            "unsuccessful_expectations": 0,
            "success_percent": 100.0,
        },
        meta={
            "great_expectations_version": "1.21.0",
            "expectation_suite_name": "my_suite",
            **(meta or {}),
        },
    )


def test_ValidationResultsPageRenderer_render_without_run_id():
    document = ValidationResultsPageRenderer().render(_validation_result())

    assert isinstance(document, RenderedDocumentContent)
    assert document.page_title == "Validations / my_suite / __none__"


def test_ValidationResultsPageRenderer_parse_run_values_without_run_id():
    run_values = ValidationResultsPageRenderer()._parse_run_values(_validation_result())

    assert run_values == ("__none__", "__none__")


def test_ValidationResultsPageRenderer_render_validation_info_without_run_id():
    validation_info = ValidationResultsPageRenderer._render_validation_info(
        _validation_result()
    ).to_json_dict()

    assert validation_info["table"] == [
        ["Great Expectations Version", "1.21.0"],
        ["Run Name", "__none__"],
        ["Run Time", "__none__"],
    ]


@pytest.mark.filterwarnings(
    "ignore:Cannot get %*::great_expectations.render.renderer.profiling_results_overview_section_renderer"  # noqa: E501 # FIXME CoP
)
def test_ProfilingResultsPageRenderer_render_without_run_id():
    document = ProfilingResultsPageRenderer().render(_validation_result())

    assert isinstance(document, RenderedDocumentContent)
    assert document.page_title == "Profiling Results / my_suite / __none__"


@pytest.mark.parametrize(
    ("run_id", "expected_run_values"),
    [
        pytest.param(None, ("__none__", "__none__"), id="none"),
        pytest.param(
            "20200322T170247.671855Z",
            ("20200322T170247.671855Z", "2020-03-22T17:02:47.671855Z"),
            id="str",
        ),
        pytest.param("not_a_time", ("not_a_time", "__none__"), id="str_unparseable"),
        pytest.param(
            {"run_name": "my_run", "run_time": "2020-03-22T17:02:47Z"},
            ("my_run", "2020-03-22T17:02:47Z"),
            id="dict",
        ),
        pytest.param({}, ("__none__", "__none__"), id="dict_empty"),
        pytest.param(
            RunIdentifier(run_name="my_run", run_time="2020-03-22T17:02:47Z"),
            ("my_run", "2020-03-22T17:02:47.000000Z"),
            id="run_identifier",
        ),
    ],
)
def test_ValidationResultsPageRenderer_parse_run_values(run_id, expected_run_values):
    run_values = ValidationResultsPageRenderer()._parse_run_values(
        _validation_result({"run_id": run_id})
    )

    assert run_values == expected_run_values
