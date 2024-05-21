from __future__ import annotations

import json
import re
from pprint import pformat as pf
from typing import TYPE_CHECKING

import mistune
import pytest

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.data_context.util import file_relative_path
from great_expectations.render import RenderedContent, RenderedDocumentContent
from great_expectations.render.renderer import (
    ExpectationSuitePageRenderer,
    ProfilingResultsPageRenderer,
    ValidationResultsPageRenderer,
)
from great_expectations.render.renderer_configuration import MetaNotesFormat

if TYPE_CHECKING:
    from pytest_mock import MockerFixture

    from great_expectations.core.expectation_validation_result import (
        ExpectationValidationResult,
    )
    from great_expectations.data_context import AbstractDataContext

# module level markers
pytestmark = pytest.mark.filesystem


def test_ExpectationSuitePageRenderer_render_expectation_suite_notes(
    empty_data_context,
):
    context: AbstractDataContext = empty_data_context
    result = ExpectationSuitePageRenderer._render_expectation_suite_notes(
        ExpectationSuite(
            expectation_suite_name="test", meta={"notes": "*hi*"}, data_context=context
        )
    )
    # print(RenderedContent.rendered_content_list_to_json(result.text))
    assert RenderedContent.rendered_content_list_to_json(result.text) == [
        "This Expectation suite currently contains 0 total Expectations across 0 columns.",
        "*hi*",
    ]

    result = ExpectationSuitePageRenderer._render_expectation_suite_notes(
        ExpectationSuite(
            expectation_suite_name="test",
            meta={"notes": ["*alpha*", "_bravo_", "charlie"]},
            data_context=context,
        )
    )
    # print(RenderedContent.rendered_content_list_to_json(result.text))
    assert RenderedContent.rendered_content_list_to_json(result.text) == [
        "This Expectation suite currently contains 0 total Expectations across 0 columns.",
        "*alpha*",
        "_bravo_",
        "charlie",
    ]

    result = ExpectationSuitePageRenderer._render_expectation_suite_notes(
        ExpectationSuite(
            expectation_suite_name="test",
            meta={
                "notes": {
                    "format": MetaNotesFormat.STRING,
                    "content": ["*alpha*", "_bravo_", "charlie"],
                }
            },
            data_context=context,
        )
    )
    # print(RenderedContent.rendered_content_list_to_json(result.text))
    assert RenderedContent.rendered_content_list_to_json(result.text) == [
        "This Expectation suite currently contains 0 total Expectations across 0 columns.",
        "*alpha*",
        "_bravo_",
        "charlie",
    ]

    result = ExpectationSuitePageRenderer._render_expectation_suite_notes(
        ExpectationSuite(
            expectation_suite_name="test",
            meta={"notes": {"format": MetaNotesFormat.MARKDOWN, "content": "*alpha*"}},
            data_context=context,
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
            expectation_suite_name="test",
            meta={
                "notes": {
                    "format": MetaNotesFormat.MARKDOWN,
                    "content": ["*alpha*", "_bravo_", "charlie"],
                }
            },
            data_context=context,
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


def test_expectation_summary_in_ExpectationSuitePageRenderer_render_expectation_suite_notes(
    empty_data_context: AbstractDataContext,
):
    context: AbstractDataContext = empty_data_context
    result = ExpectationSuitePageRenderer._render_expectation_suite_notes(
        ExpectationSuite(
            expectation_suite_name="test",
            meta={},
            expectations=None,
            data_context=context,
        )
    )
    # print(RenderedContent.rendered_content_list_to_json(result.text))
    assert RenderedContent.rendered_content_list_to_json(result.text) == [
        "This Expectation suite currently contains 0 total Expectations across 0 columns."
    ]

    result = ExpectationSuitePageRenderer._render_expectation_suite_notes(
        ExpectationSuite(
            expectation_suite_name="test",
            meta={"notes": {"format": MetaNotesFormat.MARKDOWN, "content": ["hi"]}},
            data_context=context,
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
            expectation_suite_name="test",
            meta={},
            expectations=[
                ExpectationConfiguration(
                    expectation_type="expect_table_row_count_to_be_between",
                    kwargs={"min_value": 0, "max_value": None},
                ),
                ExpectationConfiguration(
                    expectation_type="expect_column_to_exist", kwargs={"column": "x"}
                ),
                ExpectationConfiguration(
                    expectation_type="expect_column_to_exist", kwargs={"column": "y"}
                ),
            ],
            data_context=context,
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
                "template": "${suite_title} ${expectation_suite_name}\n ${data_asset} ${data_asset_name}\n ${status_title} ${html_success_icon} ${success}",
                "params": {
                    "suite_title": "Expectation Suite:",
                    "data_asset": "Data asset:",
                    "data_asset_name": None,
                    "status_title": "Status:",
                    "expectation_suite_name": "default",
                    "success": "Failed",
                    "html_success_icon": '<i class="fas fa-times text-danger" aria-hidden="true"></i>',
                },
                "styling": {
                    "params": {
                        "suite_title": {"classes": ["h6"]},
                        "status_title": {"classes": ["h6"]},
                        "expectation_suite_name": {
                            "tag": "a",
                            "attributes": {
                                "href": "../../../../expectations/default.html"
                            },
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
                        "params": {
                            "value": "project_dir/project_path/data/titanic/Titanic.csv"
                        },
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
                                    "styling": {
                                        "default": {
                                            "styles": {"word-break": "break-all"}
                                        }
                                    },
                                },
                            },
                            {
                                "content_block_type": "string_template",
                                "styling": {"parent": {"classes": []}},
                                "string_template": {
                                    "template": "$value",
                                    "params": {"value": "python"},
                                    "styling": {
                                        "default": {
                                            "styles": {"word-break": "break-all"}
                                        }
                                    },
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
                                    "styling": {
                                        "default": {
                                            "styles": {"word-break": "break-all"}
                                        }
                                    },
                                },
                            },
                            {
                                "content_block_type": "string_template",
                                "styling": {"parent": {"classes": []}},
                                "string_template": {
                                    "template": "$value",
                                    "params": {"value": "None"},
                                    "styling": {
                                        "default": {
                                            "styles": {"word-break": "break-all"}
                                        }
                                    },
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


def test_snapshot_ValidationResultsPageRenderer_render_with_run_info_at_end(
    titanic_profiled_evrs_1: ExpectationValidationResult,
    ValidationResultsPageRenderer_render_with_run_info_at_end,
):
    validation_results_page_renderer = ValidationResultsPageRenderer(
        run_info_at_end=True
    )
    rendered_validation_results = validation_results_page_renderer.render(
        titanic_profiled_evrs_1
    ).to_json_dict()

    # replace version of vega-lite in res to match snapshot test
    content_block = rendered_validation_results["sections"][5]["content_blocks"][1][
        "table"
    ][10][2]["content_blocks"][1]
    content_block["graph"]["$schema"] = re.sub(
        r"v\d*\.\d*\.\d*", "v4.8.1", content_block["graph"]["$schema"]
    )
    assert (
        rendered_validation_results
        == ValidationResultsPageRenderer_render_with_run_info_at_end
    )


def test_snapshot_ValidationResultsPageRenderer_render_with_run_info_at_start(
    titanic_profiled_evrs_1: ExpectationValidationResult,
    ValidationResultsPageRenderer_render_with_run_info_at_start,
):
    validation_results_page_renderer = ValidationResultsPageRenderer(
        run_info_at_end=False
    )
    rendered_validation_results = validation_results_page_renderer.render(
        titanic_profiled_evrs_1
    ).to_json_dict()

    # replace version of vega-lite in res to match snapshot test
    content_block = rendered_validation_results["sections"][5]["content_blocks"][1][
        "table"
    ][10][2]["content_blocks"][1]
    content_block["graph"]["$schema"] = re.sub(
        r"v\d*\.\d*\.\d*", "v4.8.1", content_block["graph"]["$schema"]
    )
    assert (
        rendered_validation_results
        == ValidationResultsPageRenderer_render_with_run_info_at_start
    )


def test_asset_name_is_part_of_resource_info_index(mocker: MockerFixture):
    """
    DefaultSiteIndexBuilder.add_resource_info_to_index_links_dict is what supplies the
    the resource meta-data to the index page.
    This test checks that the asset_name is being provided when checkpoints with fluent datasources are run.
    """
    import great_expectations as gx
    from great_expectations.render.renderer.site_builder import DefaultSiteIndexBuilder

    context = gx.get_context(mode="ephemeral")

    add_resource_info_spy = mocker.spy(
        DefaultSiteIndexBuilder,
        "add_resource_info_to_index_links_dict",
    )

    asset_name = "my_asset"
    validator = context.sources.pandas_default.read_csv(
        "https://raw.githubusercontent.com/great-expectations/gx_tutorials/main/data/yellow_tripdata_sample_2019-01.csv",
        asset_name=asset_name,
    )

    validator.expect_column_values_to_not_be_null("pickup_datetime")
    validator.expect_column_values_to_be_between(
        "passenger_count", min_value=1, max_value=6
    )
    validator.save_expectation_suite(discard_failed_expectations=False)

    checkpoint = context.add_or_update_checkpoint(
        name="my_quickstart_checkpoint",
        validator=validator,
    )

    checkpoint.run()

    last_call = add_resource_info_spy.call_args_list[-1]
    print(f"Last call kwargs:\n{pf(last_call.kwargs, depth=1)}")
    assert last_call.kwargs["asset_name"] == asset_name
