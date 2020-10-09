import json
from collections import OrderedDict

import pytest

from great_expectations.core import (
    ExpectationConfiguration,
    ExpectationSuiteValidationResult,
    ExpectationValidationResult,
    expectationSuiteSchema,
)
from great_expectations.data_context.util import file_relative_path
from great_expectations.render.renderer import (
    ExpectationSuiteColumnSectionRenderer,
    ProfilingResultsColumnSectionRenderer,
    ProfilingResultsOverviewSectionRenderer,
    ValidationResultsColumnSectionRenderer,
)
from great_expectations.render.renderer.content_block import (
    ValidationResultsTableContentBlockRenderer,
)


@pytest.fixture(scope="module")
def titanic_expectations():
    with open(
        file_relative_path(__file__, "../test_sets/titanic_expectations.json")
    ) as infile:
        return expectationSuiteSchema.load(
            json.load(infile, object_pairs_hook=OrderedDict)
        )


@pytest.mark.smoketest
@pytest.mark.rendered_output
def test_render_profiling_results_column_section_renderer(titanic_validation_results):
    # Group EVRs by column
    evrs = {}
    for evr in titanic_validation_results.results:
        try:
            column = evr.expectation_config.kwargs["column"]
            if column not in evrs:
                evrs[column] = []
            evrs[column].append(evr)
        except KeyError:
            pass

    for column in evrs.keys():
        with open(
            file_relative_path(
                __file__,
                "./output/test_render_profiling_results_column_section_renderer__"
                + column
                + ".json",
            ),
            "w",
        ) as outfile:
            json.dump(
                ProfilingResultsColumnSectionRenderer()
                .render(evrs[column])
                .to_json_dict(),
                outfile,
                indent=2,
            )


@pytest.mark.smoketest
@pytest.mark.rendered_output
def test_render_expectation_suite_column_section_renderer(titanic_expectations):
    # Group expectations by column
    exp_groups = {}
    # print(json.dumps(titanic_expectations, indent=2))
    for exp in titanic_expectations.expectations:
        try:
            column = exp.kwargs["column"]
            if column not in exp_groups:
                exp_groups[column] = []
            exp_groups[column].append(exp)
        except KeyError:
            pass

    for column in exp_groups.keys():
        with open(
            file_relative_path(
                __file__,
                "./output/test_render_expectation_suite_column_section_renderer"
                + column
                + ".json",
            ),
            "w",
        ) as outfile:
            json.dump(
                ExpectationSuiteColumnSectionRenderer()
                .render(exp_groups[column])
                .to_json_dict(),
                outfile,
                indent=2,
            )


@pytest.mark.smoketest
def test_ProfilingResultsColumnSectionRenderer_render(
    titanic_profiled_evrs_1, titanic_profiled_name_column_evrs
):
    # Smoke test for titanic names
    document = ProfilingResultsColumnSectionRenderer().render(
        titanic_profiled_name_column_evrs
    )
    print(document)
    assert document != {}

    # Smoke test for titanic Ages

    # This is a janky way to fetch expectations matching a specific name from an EVR suite.
    # TODO: It will no longer be necessary once we implement ValidationResultSuite._group_evrs_by_column
    from great_expectations.render.renderer.renderer import Renderer

    evrs_by_column = Renderer()._group_evrs_by_column(titanic_profiled_evrs_1)
    print(evrs_by_column.keys())

    age_column_evrs = evrs_by_column["Age"]
    for evr in age_column_evrs:
        print(evr)

    document = ProfilingResultsColumnSectionRenderer().render(age_column_evrs)
    print(document)


def test_ProfilingResultsColumnSectionRenderer_render_header(
    titanic_profiled_name_column_evrs,
):
    content_block = (
        ProfilingResultsColumnSectionRenderer()
        ._render_header(evrs=titanic_profiled_name_column_evrs, column_type=None)
        .to_json_dict()
    )

    assert content_block["content_block_type"] == "header"
    print(content_block["header"])
    assert content_block["header"] == {
        "content_block_type": "string_template",
        "string_template": {
            "template": "Name",
            "tooltip": {"content": "expect_column_to_exist", "placement": "top"},
            "tag": "h5",
            "styling": {"classes": ["m-0", "p-0"]},
        },
    }
    print(content_block["subheader"])
    assert content_block["subheader"] == {
        "content_block_type": "string_template",
        "string_template": {
            "template": "Type: None",
            "tooltip": {
                "content": "expect_column_values_to_be_of_type <br>expect_column_values_to_be_in_type_list"
            },
            "tag": "h6",
            "styling": {"classes": ["mt-1", "mb-0"]},
        },
    }


def test_ProfilingResultsColumnSectionRenderer_render_header_with_unescaped_dollar_sign(
    titanic_profiled_name_column_evrs,
):
    evr_with_unescaped_dollar_sign = ExpectationValidationResult(
        success=True,
        result={"observed_value": "float64"},
        exception_info={
            "raised_exception": False,
            "exception_message": None,
            "exception_traceback": None,
        },
        expectation_config=ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_type_list",
            kwargs={
                "column": "Car Insurance Premiums ($)",
                "type_list": [
                    "DOUBLE_PRECISION",
                    "DoubleType",
                    "FLOAT",
                    "FLOAT4",
                    "FLOAT8",
                    "FloatType",
                    "NUMERIC",
                    "float",
                ],
                "result_format": "SUMMARY",
            },
            meta={"BasicDatasetProfiler": {"confidence": "very low"}},
        ),
    )

    content_block = ProfilingResultsColumnSectionRenderer._render_header(
        [evr_with_unescaped_dollar_sign], column_type=[],
    ).to_json_dict()
    print(content_block)
    assert content_block == {
        "content_block_type": "header",
        "styling": {
            "classes": ["col-12", "p-0"],
            "header": {"classes": ["alert", "alert-secondary"]},
        },
        "header": {
            "content_block_type": "string_template",
            "string_template": {
                "template": "Car Insurance Premiums ($$)",
                "tooltip": {"content": "expect_column_to_exist", "placement": "top"},
                "tag": "h5",
                "styling": {"classes": ["m-0", "p-0"]},
            },
        },
        "subheader": {
            "content_block_type": "string_template",
            "string_template": {
                "template": "Type: []",
                "tooltip": {
                    "content": "expect_column_values_to_be_of_type <br>expect_column_values_to_be_in_type_list"
                },
                "tag": "h6",
                "styling": {"classes": ["mt-1", "mb-0"]},
            },
        },
    }


# def test_ProfilingResultsColumnSectionRenderer_render_overview_table():
#     evrs = {}
#     ProfilingResultsColumnSectionRenderer()._render_overview_table(evrs, content_blocks)

# def test_ProfilingResultsColumnSectionRenderer_render_quantile_table():
#     evrs = {}
#     ProfilingResultsColumnSectionRenderer()._render_quantile_table(evrs, content_blocks)

# def test_ProfilingResultsColumnSectionRenderer_render_stats_table():
#     evrs = {}
#     ProfilingResultsColumnSectionRenderer()._render_stats_table(evrs, content_blocks)

# def test_ProfilingResultsColumnSectionRenderer_render_histogram(titanic_profiled_evrs_1):
#     evrs = {}
#     ProfilingResultsColumnSectionRenderer()._render_histogram(evrs, content_blocks)

# def test_ProfilingResultsColumnSectionRenderer_render_values_set():
#     evrs = {}
#     ProfilingResultsColumnSectionRenderer()._render_values_set(evrs, content_blocks)


def test_ProfilingResultsColumnSectionRenderer_render_bar_chart_table(
    titanic_profiled_evrs_1,
):
    print(titanic_profiled_evrs_1.results[0])
    distinct_values_evrs = [
        evr
        for evr in titanic_profiled_evrs_1.results
        if evr.expectation_config.expectation_type
        == "expect_column_distinct_values_to_be_in_set"
    ]

    assert len(distinct_values_evrs) == 4

    content_blocks = []
    for evr in distinct_values_evrs:
        content_blocks.append(
            ProfilingResultsColumnSectionRenderer()
            ._render_bar_chart_table(distinct_values_evrs)
            .to_json_dict()
        )

    assert len(content_blocks) == 4

    for content_block in content_blocks:
        assert content_block["content_block_type"] == "graph"
        assert set(content_block.keys()) == {
            "header",
            "content_block_type",
            "graph",
            "styling",
        }
        assert json.loads(content_block["graph"])


def test_ExpectationSuiteColumnSectionRenderer_render_header(
    titanic_profiled_name_column_expectations,
):
    (
        remaining_expectations,
        content_blocks,
    ) = ExpectationSuiteColumnSectionRenderer._render_header(
        titanic_profiled_name_column_expectations,
    )

    expected = {
        "content_block_type": "header",
        "styling": {
            "classes": ["col-12"],
            "header": {"classes": ["alert", "alert-secondary"]},
        },
        "header": {
            "content_block_type": "string_template",
            "string_template": {
                "template": "Name",
                "tag": "h5",
                "styling": {"classes": ["m-0"]},
            },
        },
    }

    print(content_blocks.to_json_dict())

    assert content_blocks.to_json_dict() == expected

    expectation_with_unescaped_dollar_sign = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_type_list",
        kwargs={
            "column": "Car Insurance Premiums ($)",
            "type_list": [
                "DOUBLE_PRECISION",
                "DoubleType",
                "FLOAT",
                "FLOAT4",
                "FLOAT8",
                "FloatType",
                "NUMERIC",
                "float",
            ],
            "result_format": "SUMMARY",
        },
        meta={"BasicDatasetProfiler": {"confidence": "very low"}},
    )
    (
        remaining_expectations,
        content_blocks,
    ) = ExpectationSuiteColumnSectionRenderer._render_header(
        [expectation_with_unescaped_dollar_sign],
    )

    print(content_blocks.to_json_dict())
    expected = {
        "content_block_type": "header",
        "styling": {
            "classes": ["col-12"],
            "header": {"classes": ["alert", "alert-secondary"]},
        },
        "header": {
            "content_block_type": "string_template",
            "string_template": {
                "template": "Car Insurance Premiums ($$)",
                "tag": "h5",
                "styling": {"classes": ["m-0"]},
            },
        },
    }
    assert content_blocks.to_json_dict() == expected


def test_ExpectationSuiteColumnSectionRenderer_expectation_with_markdown_meta_notes():
    expectation_with_markdown_meta_notes = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_type_list",
        kwargs={
            "column": "Car Insurance Premiums ($)",
            "type_list": [
                "DOUBLE_PRECISION",
                "DoubleType",
                "FLOAT",
                "FLOAT4",
                "FLOAT8",
                "FloatType",
                "NUMERIC",
                "float",
            ],
            "result_format": "SUMMARY",
        },
        meta={
            "BasicDatasetProfiler": {"confidence": "very low"},
            "notes": {
                "format": "markdown",
                "content": [
                    "#### These are expectation notes \n - you can use markdown \n - or just strings"
                ],
            },
        },
    )
    expectations = [expectation_with_markdown_meta_notes]
    expected_result_json = {
        "content_blocks": [
            {
                "content_block_type": "header",
                "styling": {
                    "classes": ["col-12"],
                    "header": {"classes": ["alert", "alert-secondary"]},
                },
                "header": {
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": "Car Insurance Premiums ($$)",
                        "tag": "h5",
                        "styling": {"classes": ["m-0"]},
                    },
                },
            },
            {
                "content_block_type": "bullet_list",
                "styling": {"classes": ["col-12"]},
                "bullet_list": [
                    [
                        {
                            "content_block_type": "string_template",
                            "string_template": {
                                "template": "value types must belong to this set: $v__0"
                                " $v__1 $v__2 $v__3 $v__4 $v__5 $v__6 $v__7.",
                                "params": {
                                    "column": "Car Insurance Premiums ($)",
                                    "type_list": [
                                        "DOUBLE_PRECISION",
                                        "DoubleType",
                                        "FLOAT",
                                        "FLOAT4",
                                        "FLOAT8",
                                        "FloatType",
                                        "NUMERIC",
                                        "float",
                                    ],
                                    "result_format": "SUMMARY",
                                    "mostly": None,
                                    "row_condition": None,
                                    "condition_parser": None,
                                    "v__0": "DOUBLE_PRECISION",
                                    "v__1": "DoubleType",
                                    "v__2": "FLOAT",
                                    "v__3": "FLOAT4",
                                    "v__4": "FLOAT8",
                                    "v__5": "FloatType",
                                    "v__6": "NUMERIC",
                                    "v__7": "float",
                                },
                                "styling": {
                                    "default": {
                                        "classes": ["badge", "badge-secondary"]
                                    },
                                    "params": {
                                        "column": {
                                            "classes": ["badge", "badge-primary"]
                                        }
                                    },
                                },
                            },
                        },
                        {
                            "content_block_type": "collapse",
                            "styling": {
                                "body": {"classes": ["card", "card-body", "p-1"]},
                                "parent": {"styles": {"list-style-type": "none"}},
                            },
                            "collapse_toggle_link": {
                                "content_block_type": "string_template",
                                "string_template": {
                                    "template": "$icon",
                                    "params": {"icon": ""},
                                    "styling": {
                                        "params": {
                                            "icon": {
                                                "classes": [
                                                    "fas",
                                                    "fa-comment",
                                                    "text-info",
                                                ],
                                                "tag": "i",
                                            }
                                        }
                                    },
                                },
                            },
                            "collapse": [
                                {
                                    "content_block_type": "text",
                                    "styling": {
                                        "classes": ["col-12", "mt-2", "mb-2"],
                                        "parent": {
                                            "styles": {"list-style-type": "none"}
                                        },
                                    },
                                    "subheader": "Notes:",
                                    "text": [
                                        {
                                            "content_block_type": "markdown",
                                            "styling": {"parent": {}},
                                            "markdown": "#### These are expectation notes \n - "
                                            "you can use markdown \n - or just strings",
                                        }
                                    ],
                                }
                            ],
                            "inline_link": True,
                        },
                    ],
                    {
                        "content_block_type": "string_template",
                        "styling": {"parent": {"styles": {"list-style-type": "none"}}},
                        "string_template": {
                            "template": "",
                            "tag": "hr",
                            "styling": {"classes": ["mt-1", "mb-1"]},
                        },
                    },
                ],
            },
        ],
        "section_name": "Car Insurance Premiums ($)",
    }

    result_json = (
        ExpectationSuiteColumnSectionRenderer().render(expectations).to_json_dict()
    )
    print(result_json)
    assert result_json == expected_result_json


def test_ExpectationSuiteColumnSectionRenderer_expectation_with_string_list_meta_notes_in_dict():
    expectation_with_string_notes_list_in_dict = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_type_list",
        kwargs={
            "column": "Car Insurance Premiums ($)",
            "type_list": [
                "DOUBLE_PRECISION",
                "DoubleType",
                "FLOAT",
                "FLOAT4",
                "FLOAT8",
                "FloatType",
                "NUMERIC",
                "float",
            ],
            "result_format": "SUMMARY",
        },
        meta={
            "BasicDatasetProfiler": {"confidence": "very low"},
            "notes": {
                "format": "string",
                "content": [
                    "This is a",
                    "string list,",
                    "assigned to the 'content' key of a notes dict.",
                    "Cool",
                    "huh?",
                ],
            },
        },
    )
    expectations = [expectation_with_string_notes_list_in_dict]
    expected_result_json = {
        "content_blocks": [
            {
                "content_block_type": "header",
                "styling": {
                    "classes": ["col-12"],
                    "header": {"classes": ["alert", "alert-secondary"]},
                },
                "header": {
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": "Car Insurance Premiums ($$)",
                        "tag": "h5",
                        "styling": {"classes": ["m-0"]},
                    },
                },
            },
            {
                "content_block_type": "bullet_list",
                "styling": {"classes": ["col-12"]},
                "bullet_list": [
                    [
                        {
                            "content_block_type": "string_template",
                            "string_template": {
                                "template": "value types must belong to this set: $v__0 $v__1 "
                                "$v__2 $v__3 $v__4 $v__5 $v__6 $v__7.",
                                "params": {
                                    "column": "Car Insurance Premiums ($)",
                                    "type_list": [
                                        "DOUBLE_PRECISION",
                                        "DoubleType",
                                        "FLOAT",
                                        "FLOAT4",
                                        "FLOAT8",
                                        "FloatType",
                                        "NUMERIC",
                                        "float",
                                    ],
                                    "result_format": "SUMMARY",
                                    "mostly": None,
                                    "row_condition": None,
                                    "condition_parser": None,
                                    "v__0": "DOUBLE_PRECISION",
                                    "v__1": "DoubleType",
                                    "v__2": "FLOAT",
                                    "v__3": "FLOAT4",
                                    "v__4": "FLOAT8",
                                    "v__5": "FloatType",
                                    "v__6": "NUMERIC",
                                    "v__7": "float",
                                },
                                "styling": {
                                    "default": {
                                        "classes": ["badge", "badge-secondary"]
                                    },
                                    "params": {
                                        "column": {
                                            "classes": ["badge", "badge-primary"]
                                        }
                                    },
                                },
                            },
                        },
                        {
                            "content_block_type": "collapse",
                            "styling": {
                                "body": {"classes": ["card", "card-body", "p-1"]},
                                "parent": {"styles": {"list-style-type": "none"}},
                            },
                            "collapse_toggle_link": {
                                "content_block_type": "string_template",
                                "string_template": {
                                    "template": "$icon",
                                    "params": {"icon": ""},
                                    "styling": {
                                        "params": {
                                            "icon": {
                                                "classes": [
                                                    "fas",
                                                    "fa-comment",
                                                    "text-info",
                                                ],
                                                "tag": "i",
                                            }
                                        }
                                    },
                                },
                            },
                            "collapse": [
                                {
                                    "content_block_type": "text",
                                    "styling": {
                                        "classes": ["col-12", "mt-2", "mb-2"],
                                        "parent": {
                                            "styles": {"list-style-type": "none"}
                                        },
                                    },
                                    "subheader": "Notes:",
                                    "text": [
                                        "This is a",
                                        "string list,",
                                        "assigned to the 'content' key of a notes dict.",
                                        "Cool",
                                        "huh?",
                                    ],
                                }
                            ],
                            "inline_link": True,
                        },
                    ],
                    {
                        "content_block_type": "string_template",
                        "styling": {"parent": {"styles": {"list-style-type": "none"}}},
                        "string_template": {
                            "template": "",
                            "tag": "hr",
                            "styling": {"classes": ["mt-1", "mb-1"]},
                        },
                    },
                ],
            },
        ],
        "section_name": "Car Insurance Premiums ($)",
    }

    result_json = (
        ExpectationSuiteColumnSectionRenderer().render(expectations).to_json_dict()
    )
    print(result_json)
    assert result_json == expected_result_json


def test_ExpectationSuiteColumnSectionRenderer_expectation_with_single_string_meta_note_in_dict():
    expectation_with_single_string_note_in_dict = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_type_list",
        kwargs={
            "column": "Car Insurance Premiums ($)",
            "type_list": [
                "DOUBLE_PRECISION",
                "DoubleType",
                "FLOAT",
                "FLOAT4",
                "FLOAT8",
                "FloatType",
                "NUMERIC",
                "float",
            ],
            "result_format": "SUMMARY",
        },
        meta={
            "BasicDatasetProfiler": {"confidence": "very low"},
            "notes": {
                "format": "string",
                "content": "This is just a single string, assigned to the 'content' key of a notes dict.",
            },
        },
    )
    expectations = [expectation_with_single_string_note_in_dict]
    expected_result_json = {
        "content_blocks": [
            {
                "content_block_type": "header",
                "styling": {
                    "classes": ["col-12"],
                    "header": {"classes": ["alert", "alert-secondary"]},
                },
                "header": {
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": "Car Insurance Premiums ($$)",
                        "tag": "h5",
                        "styling": {"classes": ["m-0"]},
                    },
                },
            },
            {
                "content_block_type": "bullet_list",
                "styling": {"classes": ["col-12"]},
                "bullet_list": [
                    [
                        {
                            "content_block_type": "string_template",
                            "string_template": {
                                "template": "value types must belong to this set: $v__0 $v__1 "
                                "$v__2 $v__3 $v__4 $v__5 $v__6 $v__7.",
                                "params": {
                                    "column": "Car Insurance Premiums ($)",
                                    "type_list": [
                                        "DOUBLE_PRECISION",
                                        "DoubleType",
                                        "FLOAT",
                                        "FLOAT4",
                                        "FLOAT8",
                                        "FloatType",
                                        "NUMERIC",
                                        "float",
                                    ],
                                    "result_format": "SUMMARY",
                                    "mostly": None,
                                    "row_condition": None,
                                    "condition_parser": None,
                                    "v__0": "DOUBLE_PRECISION",
                                    "v__1": "DoubleType",
                                    "v__2": "FLOAT",
                                    "v__3": "FLOAT4",
                                    "v__4": "FLOAT8",
                                    "v__5": "FloatType",
                                    "v__6": "NUMERIC",
                                    "v__7": "float",
                                },
                                "styling": {
                                    "default": {
                                        "classes": ["badge", "badge-secondary"]
                                    },
                                    "params": {
                                        "column": {
                                            "classes": ["badge", "badge-primary"]
                                        }
                                    },
                                },
                            },
                        },
                        {
                            "content_block_type": "collapse",
                            "styling": {
                                "body": {"classes": ["card", "card-body", "p-1"]},
                                "parent": {"styles": {"list-style-type": "none"}},
                            },
                            "collapse_toggle_link": {
                                "content_block_type": "string_template",
                                "string_template": {
                                    "template": "$icon",
                                    "params": {"icon": ""},
                                    "styling": {
                                        "params": {
                                            "icon": {
                                                "classes": [
                                                    "fas",
                                                    "fa-comment",
                                                    "text-info",
                                                ],
                                                "tag": "i",
                                            }
                                        }
                                    },
                                },
                            },
                            "collapse": [
                                {
                                    "content_block_type": "text",
                                    "styling": {
                                        "classes": ["col-12", "mt-2", "mb-2"],
                                        "parent": {
                                            "styles": {"list-style-type": "none"}
                                        },
                                    },
                                    "subheader": "Notes:",
                                    "text": [
                                        "This is just a single string, assigned to the 'content' key of a notes dict."
                                    ],
                                }
                            ],
                            "inline_link": True,
                        },
                    ],
                    {
                        "content_block_type": "string_template",
                        "styling": {"parent": {"styles": {"list-style-type": "none"}}},
                        "string_template": {
                            "template": "",
                            "tag": "hr",
                            "styling": {"classes": ["mt-1", "mb-1"]},
                        },
                    },
                ],
            },
        ],
        "section_name": "Car Insurance Premiums ($)",
    }

    result_json = (
        ExpectationSuiteColumnSectionRenderer().render(expectations).to_json_dict()
    )
    print(result_json)
    assert result_json == expected_result_json


def test_ExpectationSuiteColumnSectionRenderer_expectation_with_string_list_meta_notes():
    expectation_with_string_list_note = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_type_list",
        kwargs={
            "column": "Car Insurance Premiums ($)",
            "type_list": [
                "DOUBLE_PRECISION",
                "DoubleType",
                "FLOAT",
                "FLOAT4",
                "FLOAT8",
                "FloatType",
                "NUMERIC",
                "float",
            ],
            "result_format": "SUMMARY",
        },
        meta={
            "BasicDatasetProfiler": {"confidence": "very low"},
            "notes": ["This is a list", "of strings", "assigned to the notes", "key."],
        },
    )
    expectations = [expectation_with_string_list_note]
    expected_result_json = {
        "content_blocks": [
            {
                "content_block_type": "header",
                "styling": {
                    "classes": ["col-12"],
                    "header": {"classes": ["alert", "alert-secondary"]},
                },
                "header": {
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": "Car Insurance Premiums ($$)",
                        "tag": "h5",
                        "styling": {"classes": ["m-0"]},
                    },
                },
            },
            {
                "content_block_type": "bullet_list",
                "styling": {"classes": ["col-12"]},
                "bullet_list": [
                    [
                        {
                            "content_block_type": "string_template",
                            "string_template": {
                                "template": "value types must belong to this set: $v__0 $v__1 "
                                "$v__2 $v__3 $v__4 $v__5 $v__6 $v__7.",
                                "params": {
                                    "column": "Car Insurance Premiums ($)",
                                    "type_list": [
                                        "DOUBLE_PRECISION",
                                        "DoubleType",
                                        "FLOAT",
                                        "FLOAT4",
                                        "FLOAT8",
                                        "FloatType",
                                        "NUMERIC",
                                        "float",
                                    ],
                                    "result_format": "SUMMARY",
                                    "mostly": None,
                                    "row_condition": None,
                                    "condition_parser": None,
                                    "v__0": "DOUBLE_PRECISION",
                                    "v__1": "DoubleType",
                                    "v__2": "FLOAT",
                                    "v__3": "FLOAT4",
                                    "v__4": "FLOAT8",
                                    "v__5": "FloatType",
                                    "v__6": "NUMERIC",
                                    "v__7": "float",
                                },
                                "styling": {
                                    "default": {
                                        "classes": ["badge", "badge-secondary"]
                                    },
                                    "params": {
                                        "column": {
                                            "classes": ["badge", "badge-primary"]
                                        }
                                    },
                                },
                            },
                        },
                        {
                            "content_block_type": "collapse",
                            "styling": {
                                "body": {"classes": ["card", "card-body", "p-1"]},
                                "parent": {"styles": {"list-style-type": "none"}},
                            },
                            "collapse_toggle_link": {
                                "content_block_type": "string_template",
                                "string_template": {
                                    "template": "$icon",
                                    "params": {"icon": ""},
                                    "styling": {
                                        "params": {
                                            "icon": {
                                                "classes": [
                                                    "fas",
                                                    "fa-comment",
                                                    "text-info",
                                                ],
                                                "tag": "i",
                                            }
                                        }
                                    },
                                },
                            },
                            "collapse": [
                                {
                                    "content_block_type": "text",
                                    "styling": {
                                        "classes": ["col-12", "mt-2", "mb-2"],
                                        "parent": {
                                            "styles": {"list-style-type": "none"}
                                        },
                                    },
                                    "subheader": "Notes:",
                                    "text": [
                                        "This is a list",
                                        "of strings",
                                        "assigned to the notes",
                                        "key.",
                                    ],
                                }
                            ],
                            "inline_link": True,
                        },
                    ],
                    {
                        "content_block_type": "string_template",
                        "styling": {"parent": {"styles": {"list-style-type": "none"}}},
                        "string_template": {
                            "template": "",
                            "tag": "hr",
                            "styling": {"classes": ["mt-1", "mb-1"]},
                        },
                    },
                ],
            },
        ],
        "section_name": "Car Insurance Premiums ($)",
    }

    result_json = (
        ExpectationSuiteColumnSectionRenderer().render(expectations).to_json_dict()
    )
    print(result_json)
    assert result_json == expected_result_json


def test_ExpectationSuiteColumnSectionRenderer_expectation_with_single_string_meta_note():
    expectation_with_single_string_note = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_type_list",
        kwargs={
            "column": "Car Insurance Premiums ($)",
            "type_list": [
                "DOUBLE_PRECISION",
                "DoubleType",
                "FLOAT",
                "FLOAT4",
                "FLOAT8",
                "FloatType",
                "NUMERIC",
                "float",
            ],
            "result_format": "SUMMARY",
        },
        meta={
            "BasicDatasetProfiler": {"confidence": "very low"},
            "notes": "This is a single string assigned to the 'notes' key.",
        },
    )
    expectations = [expectation_with_single_string_note]
    expected_result_json = {
        "content_blocks": [
            {
                "content_block_type": "header",
                "styling": {
                    "classes": ["col-12"],
                    "header": {"classes": ["alert", "alert-secondary"]},
                },
                "header": {
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": "Car Insurance Premiums ($$)",
                        "tag": "h5",
                        "styling": {"classes": ["m-0"]},
                    },
                },
            },
            {
                "content_block_type": "bullet_list",
                "styling": {"classes": ["col-12"]},
                "bullet_list": [
                    [
                        {
                            "content_block_type": "string_template",
                            "string_template": {
                                "template": "value types must belong to this set: $v__0"
                                " $v__1 $v__2 $v__3 $v__4 $v__5 $v__6 $v__7.",
                                "params": {
                                    "column": "Car Insurance Premiums ($)",
                                    "type_list": [
                                        "DOUBLE_PRECISION",
                                        "DoubleType",
                                        "FLOAT",
                                        "FLOAT4",
                                        "FLOAT8",
                                        "FloatType",
                                        "NUMERIC",
                                        "float",
                                    ],
                                    "result_format": "SUMMARY",
                                    "mostly": None,
                                    "row_condition": None,
                                    "condition_parser": None,
                                    "v__0": "DOUBLE_PRECISION",
                                    "v__1": "DoubleType",
                                    "v__2": "FLOAT",
                                    "v__3": "FLOAT4",
                                    "v__4": "FLOAT8",
                                    "v__5": "FloatType",
                                    "v__6": "NUMERIC",
                                    "v__7": "float",
                                },
                                "styling": {
                                    "default": {
                                        "classes": ["badge", "badge-secondary"]
                                    },
                                    "params": {
                                        "column": {
                                            "classes": ["badge", "badge-primary"]
                                        }
                                    },
                                },
                            },
                        },
                        {
                            "content_block_type": "collapse",
                            "styling": {
                                "body": {"classes": ["card", "card-body", "p-1"]},
                                "parent": {"styles": {"list-style-type": "none"}},
                            },
                            "collapse_toggle_link": {
                                "content_block_type": "string_template",
                                "string_template": {
                                    "template": "$icon",
                                    "params": {"icon": ""},
                                    "styling": {
                                        "params": {
                                            "icon": {
                                                "classes": [
                                                    "fas",
                                                    "fa-comment",
                                                    "text-info",
                                                ],
                                                "tag": "i",
                                            }
                                        }
                                    },
                                },
                            },
                            "collapse": [
                                {
                                    "content_block_type": "text",
                                    "styling": {
                                        "classes": ["col-12", "mt-2", "mb-2"],
                                        "parent": {
                                            "styles": {"list-style-type": "none"}
                                        },
                                    },
                                    "subheader": "Notes:",
                                    "text": [
                                        "This is a single string assigned to the 'notes' key."
                                    ],
                                }
                            ],
                            "inline_link": True,
                        },
                    ],
                    {
                        "content_block_type": "string_template",
                        "styling": {"parent": {"styles": {"list-style-type": "none"}}},
                        "string_template": {
                            "template": "",
                            "tag": "hr",
                            "styling": {"classes": ["mt-1", "mb-1"]},
                        },
                    },
                ],
            },
        ],
        "section_name": "Car Insurance Premiums ($)",
    }

    result_json = (
        ExpectationSuiteColumnSectionRenderer().render(expectations).to_json_dict()
    )
    print(result_json)
    assert result_json == expected_result_json


def test_ExpectationSuiteColumnSectionRenderer_render_bullet_list(
    titanic_profiled_name_column_expectations,
):
    (
        remaining_expectations,
        content_block,
    ) = ExpectationSuiteColumnSectionRenderer()._render_bullet_list(
        titanic_profiled_name_column_expectations,
    )

    stringified_dump = json.dumps(content_block.to_json_dict())

    assert content_block.content_block_type == "bullet_list"
    assert len(content_block.bullet_list) == 8
    assert "value types must belong to this set" in stringified_dump
    assert "may have any number of unique values" in stringified_dump
    assert "may have any fraction of unique values" in stringified_dump
    assert (
        "values must not be null, at least $mostly_pct % of the time."
        in stringified_dump
    )


def test_ValidationResultsColumnSectionRenderer_render_header(
    titanic_profiled_name_column_evrs,
):
    (
        remaining_evrs,
        content_block,
    ) = ValidationResultsColumnSectionRenderer._render_header(
        validation_results=titanic_profiled_name_column_evrs,
    )
    print(content_block.to_json_dict())
    assert content_block.to_json_dict() == {
        "content_block_type": "header",
        "styling": {
            "classes": ["col-12", "p-0"],
            "header": {"classes": ["alert", "alert-secondary"]},
        },
        "header": {
            "content_block_type": "string_template",
            "string_template": {
                "template": "Name",
                "tag": "h5",
                "styling": {"classes": ["m-0"]},
            },
        },
    }


def test_ValidationResultsColumnSectionRenderer_render_header_evr_with_unescaped_dollar_sign(
    titanic_profiled_name_column_evrs,
):
    evr_with_unescaped_dollar_sign = ExpectationValidationResult(
        success=True,
        result={
            "element_count": 1313,
            "missing_count": 0,
            "missing_percent": 0.0,
            "unexpected_count": 0,
            "unexpected_percent": 0.0,
            "unexpected_percent_nonmissing": 0.0,
            "partial_unexpected_list": [],
            "partial_unexpected_index_list": [],
            "partial_unexpected_counts": [],
        },
        exception_info={
            "raised_exception": False,
            "exception_message": None,
            "exception_traceback": None,
        },
        expectation_config=ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_type_list",
            kwargs={
                "column": "Name ($)",
                "type_list": ["CHAR", "StringType", "TEXT", "VARCHAR", "str", "string"],
                "result_format": "SUMMARY",
            },
        ),
    )

    (
        remaining_evrs,
        content_block,
    ) = ValidationResultsColumnSectionRenderer._render_header(
        validation_results=[evr_with_unescaped_dollar_sign],
    )

    print(content_block.to_json_dict())

    assert content_block.to_json_dict() == {
        "content_block_type": "header",
        "styling": {
            "classes": ["col-12", "p-0"],
            "header": {"classes": ["alert", "alert-secondary"]},
        },
        "header": {
            "content_block_type": "string_template",
            "string_template": {
                "template": "Name ($$)",
                "tag": "h5",
                "styling": {"classes": ["m-0"]},
            },
        },
    }


# noinspection PyPep8Naming
def test_ValidationResultsColumnSectionRenderer_render_table(
    titanic_profiled_name_column_evrs,
):
    (
        remaining_evrs,
        content_block,
    ) = ValidationResultsColumnSectionRenderer()._render_table(
        validation_results=titanic_profiled_name_column_evrs,
    )

    content_block_stringified = json.dumps(content_block.to_json_dict())

    assert content_block.content_block_type == "table"
    assert len(content_block.table) == 6
    assert content_block_stringified.count("$icon") == 6
    assert (
        "value types must belong to this set: $v__0 $v__1 $v__2 $v__3 $v__4 $v__5 $v__6"
        in content_block_stringified
    )
    assert "may have any number of unique values." in content_block_stringified
    assert "may have any fraction of unique values." in content_block_stringified
    assert (
        "values must not be null, at least $mostly_pct % of the time."
        in content_block_stringified
    )
    assert "values must belong to this set: [ ]." in content_block_stringified
    assert (
        "\\n\\n$unexpected_count unexpected values found. $unexpected_percent of $element_count total rows."
        in content_block_stringified
    )
    assert (
        "values must not match this regular expression: $regex."
        in content_block_stringified
    )
    assert (
        "\\n\\n$unexpected_count unexpected values found. $unexpected_percent of $element_count total rows."
        in content_block_stringified
    )


# noinspection PyPep8Naming
def test_ValidationResultsTableContentBlockRenderer_generate_expectation_row_happy_path():
    evr = ExpectationValidationResult(
        success=True,
        result={
            "observed_value": True,
            "element_count": 162,
            "missing_count": 153,
            "missing_percent": 94.44444444444444,
        },
        exception_info={
            "raised_exception": False,
            "exception_message": None,
            "exception_traceback": None,
        },
        expectation_config=ExpectationConfiguration(
            expectation_type="expect_column_min_to_be_between",
            kwargs={
                "column": "live",
                "min_value": None,
                "max_value": None,
                "result_format": "SUMMARY",
            },
            meta={"BasicDatasetProfiler": {"confidence": "very low"}},
        ),
    )
    result = ValidationResultsTableContentBlockRenderer.render([evr]).to_json_dict()
    print(result)

    # Note: A better approach to testing would separate out styling into a separate test.
    assert result == {
        "content_block_type": "table",
        "styling": {
            "body": {"classes": ["table"]},
            "classes": [
                "ml-2",
                "mr-2",
                "mt-0",
                "mb-0",
                "table-responsive",
                "hide-succeeded-validations-column-section-target-child",
            ],
        },
        "table": [
            [
                {
                    "content_block_type": "string_template",
                    "styling": {
                        "parent": {
                            "classes": ["hide-succeeded-validation-target-child"]
                        }
                    },
                    "string_template": {
                        "template": "$icon",
                        "params": {"icon": "", "markdown_status_icon": "✅"},
                        "styling": {
                            "params": {
                                "icon": {
                                    "classes": [
                                        "fas",
                                        "fa-check-circle",
                                        "text-success",
                                    ],
                                    "tag": "i",
                                }
                            }
                        },
                    },
                },
                {
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": "$column minimum value may have any numerical value.",
                        "params": {
                            "column": "live",
                            "min_value": None,
                            "max_value": None,
                            "result_format": "SUMMARY",
                            "parse_strings_as_datetimes": None,
                            "row_condition": None,
                            "condition_parser": None,
                            "strict_max": None,
                            "strict_min": None,
                        },
                        "styling": {
                            "default": {"classes": ["badge", "badge-secondary"]},
                            "params": {
                                "column": {"classes": ["badge", "badge-primary"]}
                            },
                        },
                    },
                },
                "True",
            ]
        ],
        "header_row": ["Status", "Expectation", "Observed Value"],
        "header_row_options": {"Status": {"sortable": True}},
        "table_options": {"search": True, "icon-size": "sm"},
    }


# noinspection PyPep8Naming
def test_ProfilingResultsOverviewSectionRenderer_empty_type_list():
    # This rather specific test is a reaction to the error documented in #679
    validation = ExpectationSuiteValidationResult(
        results=[
            ExpectationValidationResult(
                success=True,
                result={
                    "observed_value": "VARIANT",  # Note this is NOT a recognized type by many backends
                },
                exception_info={
                    "raised_exception": False,
                    "exception_message": None,
                    "exception_traceback": None,
                },
                expectation_config=ExpectationConfiguration(
                    expectation_type="expect_column_values_to_be_in_type_list",
                    kwargs={
                        "column": "live",
                        "type_list": None,
                        "result_format": "SUMMARY",
                    },
                    meta={"BasicDatasetProfiler": {"confidence": "very low"}},
                ),
            )
        ]
    )

    result = ProfilingResultsOverviewSectionRenderer().render(validation)

    # Find the variable types content block:
    types_table = [
        block.table
        for block in result.content_blocks
        if block.content_block_type == "table"
        and block.header.string_template["template"] == "Variable types"
    ][0]
    assert ["unknown", "1"] in types_table
