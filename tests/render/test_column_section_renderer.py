import json
from collections import OrderedDict

import pytest

import great_expectations.expectations as gxe
from great_expectations.core import (
    ExpectationSuite,
)
from great_expectations.core.expectation_validation_result import (
    ExpectationSuiteValidationResult,
    ExpectationValidationResult,
)
from great_expectations.data_context.util import file_relative_path
from great_expectations.expectations.expectation import (
    Expectation,
)
from great_expectations.expectations.expectation_configuration import (
    ExpectationConfiguration,
)
from great_expectations.render.renderer import (
    ExpectationSuiteColumnSectionRenderer,
    ProfilingResultsColumnSectionRenderer,
    ProfilingResultsOverviewSectionRenderer,
    ValidationResultsColumnSectionRenderer,
)
from great_expectations.render.renderer.content_block import (
    ProfilingColumnPropertiesTableContentBlockRenderer,
    ValidationResultsTableContentBlockRenderer,
)
from great_expectations.self_check.util import (
    expectationSuiteSchema,
    expectationSuiteValidationResultSchema,
)


@pytest.fixture(scope="module")
def titanic_expectations():
    with open(file_relative_path(__file__, "../test_sets/titanic_expectations.json")) as infile:
        titanic_expectation_suite_dict: dict = expectationSuiteSchema.load(
            json.load(infile, object_pairs_hook=OrderedDict)
        )
        return ExpectationSuite(**titanic_expectation_suite_dict)


@pytest.fixture
def titanic_profiled_name_column_expectations(empty_data_context_stats_enabled):
    context = empty_data_context_stats_enabled
    with open(
        file_relative_path(__file__, "./fixtures/BasicDatasetProfiler_expectations.json"),
    ) as infile:
        titanic_profiled_expectations_dict: dict = expectationSuiteSchema.load(json.load(infile))
        titanic_profiled_expectations = ExpectationSuite(
            **titanic_profiled_expectations_dict, data_context=context
        )

    (
        columns,
        _ordered_columns,
    ) = titanic_profiled_expectations.get_grouped_and_ordered_expectations_by_column()
    name_column_expectations = columns["Name"]

    return name_column_expectations


@pytest.fixture
def titanic_validation_results():
    with open(
        file_relative_path(__file__, "../test_sets/expected_cli_results_default.json"),
    ) as infile:
        return expectationSuiteValidationResultSchema.load(json.load(infile))


@pytest.fixture
def fake_expectation_with_description() -> Expectation:
    class ExpectColumnAgesToBeLegalAdult(gxe.ExpectColumnValuesToBeBetween):
        column: str = "ages"
        min_value: int = 18
        description: str = "column values must be a legal adult age (**18** or older)"

    return ExpectColumnAgesToBeLegalAdult()


@pytest.mark.smoketest
@pytest.mark.rendered_output
@pytest.mark.unit
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

    for column in evrs:
        with open(
            file_relative_path(
                __file__,
                "./output/test_render_profiling_results_column_section_renderer__"
                + column
                + ".json",
                strict=False,
            ),
            "w",
        ) as outfile:
            json.dump(
                ProfilingResultsColumnSectionRenderer().render(evrs[column]).to_json_dict(),
                outfile,
                indent=2,
            )


@pytest.mark.smoketest
@pytest.mark.rendered_output
@pytest.mark.unit
def test_render_expectation_suite_column_section_renderer(titanic_expectations):
    # Group expectations by column
    exp_groups = {}
    # print(json.dumps(titanic_expectations, indent=2))
    for exp in titanic_expectations.expectation_configurations:
        try:
            column = exp.kwargs["column"]
            if column not in exp_groups:
                exp_groups[column] = []
            exp_groups[column].append(exp)
        except KeyError:
            pass

    for column in exp_groups:
        with open(
            file_relative_path(
                __file__,
                "./output/test_render_expectation_suite_column_section_renderer" + column + ".json",
                strict=False,
            ),
            "w",
        ) as outfile:
            json.dump(
                ExpectationSuiteColumnSectionRenderer().render(exp_groups[column]).to_json_dict(),
                outfile,
                indent=2,
            )


@pytest.mark.unit
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
                "content": "expect_column_values_to_be_of_type <br>expect_column_values_to_be_in_type_list"  # noqa: E501
            },
            "tag": "h6",
            "styling": {"classes": ["mt-1", "mb-0"]},
        },
    }


@pytest.mark.unit
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
            type="expect_column_values_to_be_in_type_list",
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
        [evr_with_unescaped_dollar_sign],
        column_type=[],
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
                    "content": "expect_column_values_to_be_of_type <br>expect_column_values_to_be_in_type_list"  # noqa: E501
                },
                "tag": "h6",
                "styling": {"classes": ["mt-1", "mb-0"]},
            },
        },
    }


@pytest.mark.unit
def test_ProfilingResultsColumnSectionRenderer_render_bar_chart_table(
    titanic_profiled_evrs_1,
):
    print(titanic_profiled_evrs_1.results[0])
    distinct_values_evrs = [
        evr
        for evr in titanic_profiled_evrs_1.results
        if evr.expectation_config.type == "expect_column_distinct_values_to_be_in_set"
    ]

    assert len(distinct_values_evrs) == 4

    content_blocks = []
    for evr in distinct_values_evrs:
        content_blocks.append(
            ProfilingResultsColumnSectionRenderer()
            ._render_value_counts_bar_chart(distinct_values_evrs)
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


@pytest.mark.xfail(reason="legacy test failing with 1.0 refactor")
@pytest.mark.unit
def test_ExpectationSuiteColumnSectionRenderer_render_header(
    titanic_profiled_name_column_expectations,
):
    (
        _remaining_expectations,
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
        type="expect_column_values_to_be_in_type_list",
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
        _remaining_expectations,
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


@pytest.mark.unit
def test_ExpectationSuiteColumnSectionRenderer_expectation_with_markdown_meta_notes():
    expectation_with_markdown_meta_notes = ExpectationConfiguration(
        type="expect_column_values_to_be_in_type_list",
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
        },
        notes=["#### These are expectation notes \n - you can use markdown \n - or just strings"],
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
                                    "default": {"classes": ["badge", "badge-secondary"]},
                                    "params": {"column": {"classes": ["badge", "badge-primary"]}},
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
                                        "parent": {"styles": {"list-style-type": "none"}},
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

    result_json = ExpectationSuiteColumnSectionRenderer().render(expectations).to_json_dict()
    print(result_json)
    assert result_json == expected_result_json


@pytest.mark.unit
def test_ExpectationSuiteColumnSectionRenderer_expectation_with_string_list_meta_notes_in_dict():
    expectation_with_string_notes_list_in_dict = ExpectationConfiguration(
        type="expect_column_values_to_be_in_type_list",
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
        },
        notes=[
            "This is a",
            "string list,",
            "Cool",
            "huh?",
        ],
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
                                    "default": {"classes": ["badge", "badge-secondary"]},
                                    "params": {"column": {"classes": ["badge", "badge-primary"]}},
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
                                        "parent": {"styles": {"list-style-type": "none"}},
                                    },
                                    "subheader": "Notes:",
                                    "text": [
                                        {
                                            "content_block_type": "markdown",
                                            "markdown": "This is a",
                                            "styling": {"parent": {}},
                                        },
                                        {
                                            "content_block_type": "markdown",
                                            "markdown": "string list,",
                                            "styling": {"parent": {}},
                                        },
                                        {
                                            "content_block_type": "markdown",
                                            "markdown": "Cool",
                                            "styling": {"parent": {}},
                                        },
                                        {
                                            "content_block_type": "markdown",
                                            "markdown": "huh?",
                                            "styling": {"parent": {}},
                                        },
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

    result_json = ExpectationSuiteColumnSectionRenderer().render(expectations).to_json_dict()
    print(result_json)
    assert result_json == expected_result_json


@pytest.mark.unit
def test_ExpectationSuiteColumnSectionRenderer_expectation_with_single_string_meta_note_in_dict():
    expectation_with_single_string_note_in_dict = ExpectationConfiguration(
        type="expect_column_values_to_be_in_type_list",
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
        },
        notes="This is just a single string",
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
                                    "default": {"classes": ["badge", "badge-secondary"]},
                                    "params": {"column": {"classes": ["badge", "badge-primary"]}},
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
                                        "parent": {"styles": {"list-style-type": "none"}},
                                    },
                                    "subheader": "Notes:",
                                    "text": [
                                        {
                                            "content_block_type": "markdown",
                                            "markdown": "This is just a single string",
                                            "styling": {
                                                "parent": {"styles": {"color": "red"}},
                                            },
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

    result_json = ExpectationSuiteColumnSectionRenderer().render(expectations).to_json_dict()
    print(result_json)
    assert result_json == expected_result_json


@pytest.mark.unit
def test_ExpectationSuiteColumnSectionRenderer_expectation_with_string_list_meta_notes():
    expectation_with_string_list_note = ExpectationConfiguration(
        type="expect_column_values_to_be_in_type_list",
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
        },
        notes=["This is a list", "of strings"],
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
                                    "default": {"classes": ["badge", "badge-secondary"]},
                                    "params": {"column": {"classes": ["badge", "badge-primary"]}},
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
                                        "parent": {"styles": {"list-style-type": "none"}},
                                    },
                                    "subheader": "Notes:",
                                    "text": [
                                        {
                                            "content_block_type": "markdown",
                                            "markdown": "This is a list",
                                            "styling": {"parent": {}},
                                        },
                                        {
                                            "content_block_type": "markdown",
                                            "markdown": "of strings",
                                            "styling": {"parent": {}},
                                        },
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

    result_json = ExpectationSuiteColumnSectionRenderer().render(expectations).to_json_dict()
    print(result_json)
    assert result_json == expected_result_json


@pytest.mark.unit
def test_ExpectationSuiteColumnSectionRenderer_expectation_with_single_string_meta_note():
    expectation_with_single_string_note = ExpectationConfiguration(
        type="expect_column_values_to_be_in_type_list",
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
        },
        notes="This is a string",
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
                                    "default": {"classes": ["badge", "badge-secondary"]},
                                    "params": {"column": {"classes": ["badge", "badge-primary"]}},
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
                                        "parent": {"styles": {"list-style-type": "none"}},
                                    },
                                    "subheader": "Notes:",
                                    "text": [
                                        {
                                            "content_block_type": "markdown",
                                            "markdown": "This is a string",
                                            "styling": {
                                                "parent": {"styles": {"color": "red"}},
                                            },
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

    result_json = ExpectationSuiteColumnSectionRenderer().render(expectations).to_json_dict()
    print(result_json)
    assert result_json == expected_result_json


@pytest.mark.unit
def test_ExpectationSuiteColumnSectionRenderer_render_expectation_with_description(
    fake_expectation_with_description: Expectation,
):
    expectation = fake_expectation_with_description
    result = ExpectationSuiteColumnSectionRenderer().render([expectation.configuration])

    content_block = result.content_blocks[1]
    content = content_block.bullet_list[0]
    markdown = content.markdown

    assert markdown == expectation.description


@pytest.mark.unit
def test_ValidationResultsColumnSectionRenderer_render_header(
    titanic_profiled_name_column_evrs,
):
    (
        _remaining_evrs,
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


@pytest.mark.unit
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
            type="expect_column_values_to_be_in_type_list",
            kwargs={
                "column": "Name ($)",
                "type_list": ["CHAR", "StringType", "TEXT", "VARCHAR", "str", "string"],
                "result_format": "SUMMARY",
            },
        ),
    )

    (
        _remaining_evrs,
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
@pytest.mark.unit
def test_ValidationResultsColumnSectionRenderer_render_table(
    titanic_profiled_name_column_evrs,
):
    (
        _remaining_evrs,
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
        "values must not be null, at least $mostly_pct % of the time." in content_block_stringified
    )
    assert "values must belong to this set: [ ]." in content_block_stringified
    assert (
        "\\n\\n$unexpected_count unexpected values found. $unexpected_percent of $element_count total rows."  # noqa: E501
        in content_block_stringified
    )
    assert "values must not match this regular expression: $regex." in content_block_stringified
    assert (
        "\\n\\n$unexpected_count unexpected values found. $unexpected_percent of $element_count total rows."  # noqa: E501
        in content_block_stringified
    )


# noinspection PyPep8Naming
@pytest.mark.unit
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
            type="expect_column_min_to_be_between",
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
                    "styling": {"parent": {"classes": ["hide-succeeded-validation-target-child"]}},
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
                            "row_condition": None,
                            "condition_parser": None,
                            "strict_max": None,
                            "strict_min": None,
                        },
                        "styling": {
                            "default": {"classes": ["badge", "badge-secondary"]},
                            "params": {"column": {"classes": ["badge", "badge-primary"]}},
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
@pytest.mark.unit
def test_ValidationResultsTableContentBlockRenderer_generate_expectation_row_happy_path_with_eval_parameter():  # noqa: E501
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
            type="expect_column_min_to_be_between",
            kwargs={
                "column": "live",
                "min_value": {"$PARAMETER": "MIN_VAL_PARAM * 2"},
                "max_value": {"$PARAMETER": "MAX_VAL_PARAM"},
                "result_format": "SUMMARY",
            },
            meta={"BasicDatasetProfiler": {"confidence": "very low"}},
        ),
    )

    # suite_parameters are usually stored at the ExpectationSuiteValidationResult
    # and passed along as a kwarg to the ValidationResultsTableContentBlockRenderer
    evaluation_parameter = {"MIN_VAL_PARAM": 10, "MAX_VAL_PARAM": 40}
    result = ValidationResultsTableContentBlockRenderer.render(
        [evr], suite_parameters=evaluation_parameter
    ).to_json_dict()

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
                    "styling": {"parent": {"classes": ["hide-succeeded-validation-target-child"]}},
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
                [
                    {
                        "content_block_type": "string_template",
                        "string_template": {
                            "template": "$column minimum value must be greater than or equal to $min_value and less than or equal to $max_value.",  # noqa: E501
                            "params": {
                                "column": "live",
                                "min_value": {"$PARAMETER": "MIN_VAL_PARAM * 2"},
                                "max_value": {"$PARAMETER": "MAX_VAL_PARAM"},
                                "result_format": "SUMMARY",
                                "row_condition": None,
                                "condition_parser": None,
                                "strict_min": None,
                                "strict_max": None,
                            },
                            "styling": {
                                "default": {"classes": ["badge", "badge-secondary"]},
                                "params": {"column": {"classes": ["badge", "badge-primary"]}},
                            },
                        },
                    },
                    {
                        "content_block_type": "string_template",
                        "string_template": {
                            "template": "\n - $eval_param = $eval_param_value (at time of validation).",  # noqa: E501
                            "params": {
                                "eval_param": "MIN_VAL_PARAM",
                                "eval_param_value": 10,
                            },
                            "styling": {
                                "default": {"classes": ["badge", "badge-secondary"]},
                                "params": {"column": {"classes": ["badge", "badge-primary"]}},
                            },
                        },
                    },
                    {
                        "content_block_type": "string_template",
                        "string_template": {
                            "template": "\n - $eval_param = $eval_param_value (at time of validation).",  # noqa: E501
                            "params": {
                                "eval_param": "MAX_VAL_PARAM",
                                "eval_param_value": 40,
                            },
                            "styling": {
                                "default": {"classes": ["badge", "badge-secondary"]},
                                "params": {"column": {"classes": ["badge", "badge-primary"]}},
                            },
                        },
                    },
                ],
                "True",
            ]
        ],
        "header_row": ["Status", "Expectation", "Observed Value"],
        "header_row_options": {"Status": {"sortable": True}},
        "table_options": {"search": True, "icon-size": "sm"},
    }

    # also test case where suite_parameters aren't required at runtime such as using now()
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
            type="expect_column_min_to_be_between",
            kwargs={
                "column": "start_date",
                "min_value": {"$PARAMETER": "now() - timedelta(weeks=208)"},
                "max_value": {"$PARAMETER": "now() - timedelta(weeks=1)"},
                "result_format": "SUMMARY",
            },
        ),
    )

    result = ValidationResultsTableContentBlockRenderer.render(
        [evr],
    ).to_json_dict()

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
                    "styling": {"parent": {"classes": ["hide-succeeded-validation-target-child"]}},
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
                        "params": {
                            "column": "start_date",
                            "condition_parser": None,
                            "max_value": {"$PARAMETER": "now() " "- " "timedelta(weeks=1)"},
                            "min_value": {"$PARAMETER": "now() " "- " "timedelta(weeks=208)"},
                            "result_format": "SUMMARY",
                            "row_condition": None,
                            "strict_max": None,
                            "strict_min": None,
                        },
                        "styling": {
                            "default": {"classes": ["badge", "badge-secondary"]},
                            "params": {"column": {"classes": ["badge", "badge-primary"]}},
                        },
                        "template": "$column minimum value must be "
                        "greater than or equal to "
                        "$min_value and less than or "
                        "equal to $max_value.",
                    },
                },
                "True",
            ]
        ],
        "header_row": ["Status", "Expectation", "Observed Value"],
        "header_row_options": {"Status": {"sortable": True}},
        "table_options": {"search": True, "icon-size": "sm"},
    }


@pytest.mark.unit
def test_ValidationResultsTableContentBlockRenderer_render_evr_with_description(
    fake_expectation_with_description: Expectation,
):
    expectation = fake_expectation_with_description
    evr = ExpectationValidationResult(
        success=True,
        expectation_config=expectation.configuration,
    )
    result = ValidationResultsColumnSectionRenderer().render([evr])

    content_block = result.content_blocks[1]
    content = content_block.table[0]
    markdown = content.markdown

    assert markdown == expectation.description


# noinspection PyPep8Naming
@pytest.mark.filterwarnings(
    "ignore:Cannot get %*::great_expectations.render.renderer.profiling_results_overview_section_renderer"  # noqa: E501
)
@pytest.mark.unit
def test_ProfilingResultsOverviewSectionRenderer_empty_type_list():
    # This rather specific test is a reaction to the error documented in #679
    validation = ExpectationSuiteValidationResult(
        success=True,
        suite_name="default",
        results=[
            ExpectationValidationResult(
                success=True,
                result={
                    "observed_value": "VARIANT",  # Note this is NOT a recognized type by many backends  # noqa: E501
                },
                exception_info={
                    "raised_exception": False,
                    "exception_message": None,
                    "exception_traceback": None,
                },
                expectation_config=ExpectationConfiguration(
                    type="expect_column_values_to_be_in_type_list",
                    kwargs={
                        "column": "live",
                        "type_list": None,
                        "result_format": "SUMMARY",
                    },
                    meta={"BasicDatasetProfiler": {"confidence": "very low"}},
                ),
            )
        ],
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


# noinspection PyPep8Naming
@pytest.mark.unit
def test_ProfilingColumnPropertiesTableContentBlockRenderer():
    ge_object = [
        ExpectationValidationResult(
            **{
                "exception_info": {
                    "raised_exception": False,
                    "exception_message": None,
                    "exception_traceback": None,
                },
                "result": {
                    "element_count": 101766,
                    "missing_count": 0,
                    "missing_percent": 0.0,
                    "unexpected_count": 0,
                    "unexpected_percent": 0.0,
                    "unexpected_percent_total": 0.0,
                    "unexpected_percent_nonmissing": 0.0,
                    "partial_unexpected_list": [],
                    "partial_unexpected_index_list": [],
                    "partial_unexpected_counts": [],
                },
                "success": True,
                "expectation_config": ExpectationConfiguration(
                    **{
                        "type": "expect_column_values_to_not_match_regex",
                        "kwargs": {
                            "column": "race",
                            "regex": "^\\s+|\\s+$",
                            "result_format": "SUMMARY",
                        },
                        "meta": {"BasicDatasetProfiler": {"confidence": "very low"}},
                    }
                ),
                "meta": {},
            },
        ),
        ExpectationValidationResult(
            **{
                "exception_info": {
                    "raised_exception": False,
                    "exception_message": None,
                    "exception_traceback": None,
                },
                "result": {
                    "observed_value": 3,
                    "element_count": 101766,
                    "missing_count": None,
                    "missing_percent": None,
                },
                "success": True,
                "expectation_config": ExpectationConfiguration(
                    **{
                        "type": "expect_column_unique_value_count_to_be_between",
                        "kwargs": {
                            "column": "gender",
                            "min_value": None,
                            "max_value": None,
                            "result_format": "SUMMARY",
                        },
                        "meta": {"BasicDatasetProfiler": {"confidence": "very low"}},
                    }
                ),
                "meta": {},
            },
        ),
        ExpectationValidationResult(
            **{
                "exception_info": {
                    "raised_exception": False,
                    "exception_message": None,
                    "exception_traceback": None,
                },
                "result": {
                    "observed_value": 2.947939390366134e-02,
                    "element_count": 101766,
                    "missing_count": None,
                    "missing_percent": None,
                },
                "success": True,
                "expectation_config": ExpectationConfiguration(
                    **{
                        "type": "expect_column_proportion_of_unique_values_to_be_between",
                        "kwargs": {
                            "column": "gender",
                            "min_value": None,
                            "max_value": None,
                            "result_format": "SUMMARY",
                        },
                        "meta": {"BasicDatasetProfiler": {"confidence": "very low"}},
                    }
                ),
                "meta": {},
            },
        ),
        ExpectationValidationResult(
            **{
                "exception_info": {
                    "raised_exception": False,
                    "exception_message": None,
                    "exception_traceback": None,
                },
                "result": {
                    "element_count": 101766,
                    "unexpected_count": 29008,
                    "unexpected_percent": 23,
                    "unexpected_percent_total": 0.0,
                    "partial_unexpected_list": [],
                },
                "success": True,
                "expectation_config": ExpectationConfiguration(
                    **{
                        "type": "expect_column_values_to_not_be_null",
                        "kwargs": {
                            "column": "gender",
                            "mostly": 0.5,
                            "result_format": "SUMMARY",
                        },
                        "meta": {"BasicDatasetProfiler": {"confidence": "very low"}},
                    }
                ),
                "meta": {},
            },
        ),
    ]

    expected_result_json_dict = {
        "content_block_type": "table",
        "table": [
            ["Leading or trailing whitespace (n)", 0],
            [
                {
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": "Distinct (n)",
                        "tooltip": {"content": "expect_column_unique_value_count_to_be_between"},
                    },
                },
                3,
            ],
            [
                {
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": "Distinct (%)",
                        "tooltip": {
                            "content": "expect_column_proportion_of_unique_values_to_be_between"
                        },
                    },
                },
                "2.9%",
            ],
            [
                {
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": "Missing (n)",
                        "tooltip": {"content": "expect_column_values_to_not_be_null"},
                    },
                },
                29008,
            ],
            [
                {
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": "Missing (%)",
                        "tooltip": {"content": "expect_column_values_to_not_be_null"},
                    },
                },
                "23.0%",
            ],
        ],
        "header_row": [],
    }
    result_json_dict = (
        ProfilingColumnPropertiesTableContentBlockRenderer()
        .render(ge_object=ge_object)
        .to_json_dict()
    )

    assert result_json_dict == expected_result_json_dict
