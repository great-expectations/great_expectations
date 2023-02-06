import json

import pytest

from great_expectations.core import ExpectationConfiguration
from great_expectations.core.expectation_validation_result import (
    ExpectationValidationResult,
)
from great_expectations.expectations.registry import get_renderer_impl
from great_expectations.render import (
    LegacyDiagnosticRendererType,
    RenderedComponentContent,
    RenderedStringTemplateContent,
)
from great_expectations.render.renderer.content_block import (
    ValidationResultsTableContentBlockRenderer,
)


@pytest.fixture
def evr_failed_with_exception():
    return ExpectationValidationResult(
        success=False,
        exception_info={
            "raised_exception": True,
            "exception_message": "Invalid partition object.",
            "exception_traceback": 'Traceback (most recent call last):\n  File "/great_expectations/great_expectations/data_asset/data_asset.py", line 216, in wrapper\n    return_obj = func(self, **evaluation_args)\n  File "/great_expectations/great_expectations/dataset/dataset.py", line 106, in inner_wrapper\n    evaluation_result = func(self, column, *args, **kwargs)\n  File "/great_expectations/great_expectations/dataset/dataset.py", line 3381, in expect_column_kl_divergence_to_be_less_than\n    raise ValueError("Invalid partition object.")\nValueError: Invalid partition object.\n',
        },
        expectation_config=ExpectationConfiguration(
            expectation_type="expect_column_kl_divergence_to_be_less_than",
            kwargs={
                "column": "live",
                "partition_object": None,
                "threshold": None,
                "result_format": "SUMMARY",
            },
            meta={"BasicDatasetProfiler": {"confidence": "very low"}},
        ),
    )


@pytest.fixture()
def evr_id_pk_basic_sql() -> ExpectationValidationResult:
    return ExpectationValidationResult(
        success=False,
        expectation_config=ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={
                "batch_id": "cb8e223838fcdb055f6cccad2af592ae",
                "column": "animals",
                "value_set": ["cat", "fish", "dog"],
            },
            meta={},
        ),
        exception_info={
            "exception_message": None,
            "exception_traceback": None,
            "raised_exception": False,
        },
        result={
            "element_count": 6,
            "missing_count": 0,
            "missing_percent": 0.0,
            "partial_unexpected_counts": [
                {"count": 1, "value": "giraffe"},
                {"count": 1, "value": "lion"},
                {"count": 1, "value": "zebra"},
            ],
            "partial_unexpected_index_list": [
                {"animals": "giraffe", "pk_1": 3, "pk_2": "three"},
                {"animals": "lion", "pk_1": 4, "pk_2": "four"},
                {"animals": "zebra", "pk_1": 5, "pk_2": "five"},
            ],
            "partial_unexpected_list": ["giraffe", "lion", "zebra"],
            "unexpected_count": 3,
            "unexpected_index_column_names": ["pk_1", "pk_2"],
            "unexpected_index_list": [
                {"animals": "giraffe", "pk_1": 3, "pk_2": "three"},
                {"animals": "lion", "pk_1": 4, "pk_2": "four"},
                {"animals": "zebra", "pk_1": 5, "pk_2": "five"},
            ],
            "unexpected_index_query": "SELECT animals, pk_1, pk_2 \nFROM animal_names \nWHERE animals IS NOT NULL AND (animals NOT IN ('cat', 'fish', 'dog'))",
            "unexpected_list": ["giraffe", "lion", "zebra"],
            "unexpected_percent": 50.0,
            "unexpected_percent_nonmissing": 50.0,
            "unexpected_percent_total": 50.0,
        },
    )


@pytest.fixture()
def evr_id_pk_basic_pandas() -> ExpectationValidationResult:
    return ExpectationValidationResult(
        success=False,
        expectation_config=ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={
                "batch_id": "cb8e223838fcdb055f6cccad2af592ae",
                "column": "animals",
                "value_set": ["cat", "fish", "dog"],
            },
            meta={},
        ),
        exception_info={
            "exception_message": None,
            "exception_traceback": None,
            "raised_exception": False,
        },
        result={
            "element_count": 6,
            "missing_count": 0,
            "missing_percent": 0.0,
            "partial_unexpected_counts": [
                {"count": 1, "value": "giraffe"},
                {"count": 1, "value": "lion"},
                {"count": 1, "value": "zebra"},
            ],
            "partial_unexpected_index_list": [3, 4, 5],
            "partial_unexpected_list": ["giraffe", "lion", "zebra"],
            "unexpected_count": 3,
            "unexpected_index_list": [3, 4, 5],
            "unexpected_index_query": [3, 4, 5],
            "unexpected_list": ["giraffe", "lion", "zebra"],
            "unexpected_percent": 50.0,
            "unexpected_percent_nonmissing": 50.0,
            "unexpected_percent_total": 50.0,
        },
    )


def test_ValidationResultsTableContentBlockRenderer_generate_expectation_row_with_errored_expectation(
    evr_failed_with_exception,
):
    result = ValidationResultsTableContentBlockRenderer.render(
        [evr_failed_with_exception]
    ).to_json_dict()
    expected_result = {
        "content_block_type": "table",
        "styling": {
            "body": {"classes": ["table"]},
            "classes": ["ml-2", "mr-2", "mt-0", "mb-0", "table-responsive"],
        },
        "table": [
            [
                {
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": "$icon",
                        "params": {"icon": "", "markdown_status_icon": "❗"},
                        "styling": {
                            "params": {
                                "icon": {
                                    "classes": [
                                        "fas",
                                        "fa-exclamation-triangle",
                                        "text-warning",
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
                            "template": "$column can match any distribution.",
                            "params": {
                                "column": "live",
                                "partition_object": None,
                                "threshold": None,
                                "result_format": "SUMMARY",
                                "row_condition": None,
                                "condition_parser": None,
                            },
                        },
                    },
                    {
                        "content_block_type": "string_template",
                        "string_template": {
                            "template": "\n\n$expectation_type raised an exception:\n$exception_message",
                            "params": {
                                "expectation_type": "expect_column_kl_divergence_to_be_less_than",
                                "exception_message": "Invalid partition object.",
                            },
                            "tag": "strong",
                            "styling": {
                                "classes": ["text-danger"],
                                "params": {
                                    "exception_message": {"tag": "code"},
                                    "expectation_type": {
                                        "classes": ["badge", "badge-danger", "mb-2"]
                                    },
                                },
                            },
                        },
                    },
                    {
                        "content_block_type": "collapse",
                        "collapse_toggle_link": "Show exception traceback...",
                        "collapse": [
                            {
                                "content_block_type": "string_template",
                                "string_template": {
                                    "template": 'Traceback (most recent call last):\n  File "/great_expectations/great_expectations/data_asset/data_asset.py", line 216, in wrapper\n    return_obj = func(self, **evaluation_args)\n  File "/great_expectations/great_expectations/dataset/dataset.py", line 106, in inner_wrapper\n    evaluation_result = func(self, column, *args, **kwargs)\n  File "/great_expectations/great_expectations/dataset/dataset.py", line 3381, in expect_column_kl_divergence_to_be_less_than\n    raise ValueError("Invalid partition object.")\nValueError: Invalid partition object.\n',
                                    "tag": "code",
                                },
                            }
                        ],
                        "inline_link": False,
                    },
                ],
                "--",
            ]
        ],
        "header_row": ["Status", "Expectation", "Observed Value"],
        "header_row_options": {"Status": {"sortable": True}},
        "table_options": {"search": True, "icon-size": "sm"},
    }
    assert result == expected_result


def test_ValidationResultsTableContentBlockRenderer_render(
    titanic_profiled_name_column_evrs,
):
    validation_results_table = ValidationResultsTableContentBlockRenderer.render(
        titanic_profiled_name_column_evrs
    )

    assert isinstance(validation_results_table, RenderedComponentContent)
    assert validation_results_table.content_block_type == "table"
    assert len(validation_results_table.table) == 6
    assert validation_results_table.header_row == [
        "Status",
        "Expectation",
        "Observed Value",
    ]
    assert validation_results_table.styling == {
        "body": {"classes": ["table"]},
        "classes": ["ml-2", "mr-2", "mt-0", "mb-0", "table-responsive"],
    }
    assert json.dumps(validation_results_table.to_json_dict()).count("$icon") == 6


def test_ValidationResultsTableContentBlockRenderer_get_custom_columns(evr_success):
    assert (
        ValidationResultsTableContentBlockRenderer._get_custom_columns([evr_success])
        == []
    )

    evr_success.expectation_config.kwargs["meta_properties_to_render"] = {}
    assert (
        ValidationResultsTableContentBlockRenderer._get_custom_columns([evr_success])
        == []
    )

    evr_success.expectation_config.kwargs["meta_properties_to_render"] = {
        "doesntmatterone": "doesntmatter",
        "doesntmattertwo": "doesntmatter",
    }
    assert ValidationResultsTableContentBlockRenderer._get_custom_columns(
        [evr_success]
    ) == ["doesntmatterone", "doesntmattertwo"]


def test_ValidationResultsTableContentBlockRenderer_get_content_block_fn(evr_success):
    content_block_fn = ValidationResultsTableContentBlockRenderer._get_content_block_fn(
        "expect_table_row_count_to_be_between"
    )
    content_block_fn_output = content_block_fn(result=evr_success)

    content_block_fn_expected_output = [
        [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
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
                    "styling": {
                        "parent": {
                            "classes": ["hide-succeeded-validation-target-child"]
                        }
                    },
                }
            ),
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": "Must have greater than or equal to $min_value rows.",
                        "params": {
                            "min_value": 0,
                            "max_value": None,
                            "result_format": "SUMMARY",
                            "strict_max": None,
                            "strict_min": None,
                        },
                        "styling": None,
                    },
                }
            ),
            "1,313",
        ]
    ]
    assert content_block_fn_output == content_block_fn_expected_output

    content_block_fn_expected_output = [
        [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
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
                    "styling": {
                        "parent": {
                            "classes": ["hide-succeeded-validation-target-child"]
                        }
                    },
                }
            ),
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": "Must have greater than or equal to $min_value rows.",
                        "params": {
                            "meta_properties_to_render": {},
                            "min_value": 0,
                            "max_value": None,
                            "result_format": "SUMMARY",
                            "strict_max": None,
                            "strict_min": None,
                        },
                        "styling": None,
                    },
                }
            ),
            "1,313",
        ]
    ]

    evr_success.expectation_config.kwargs["meta_properties_to_render"] = {}
    content_block_fn_output = content_block_fn(result=evr_success)
    assert (
        content_block_fn_output[0][0].to_json_dict()
        == content_block_fn_expected_output[0][0].to_json_dict()
    )
    assert (
        content_block_fn_output[0][1].to_json_dict()
        == content_block_fn_expected_output[0][1].to_json_dict()
    )

    assert content_block_fn_output == content_block_fn_expected_output

    evr_success.expectation_config.kwargs["meta_properties_to_render"] = {
        "property_that_doesnt_exist": "property"
    }
    content_block_fn_expected_output = [
        [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
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
                    "styling": {
                        "parent": {
                            "classes": ["hide-succeeded-validation-target-child"]
                        }
                    },
                }
            ),
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": "Must have greater than or equal to $min_value rows.",
                        "params": {
                            "meta_properties_to_render": {
                                "property_that_doesnt_exist": "property"
                            },
                            "min_value": 0,
                            "max_value": None,
                            "result_format": "SUMMARY",
                            "strict_max": None,
                            "strict_min": None,
                        },
                        "styling": None,
                    },
                }
            ),
            "1,313",
            ["N/A"],
        ]
    ]
    content_block_fn_output = content_block_fn(result=evr_success)
    assert content_block_fn_output == content_block_fn_expected_output

    evr_success.expectation_config.meta = {
        "attributes": {"property": 5, "nested": {"property": "this is nested"}}
    }
    evr_success.expectation_config.kwargs["meta_properties_to_render"] = {
        "property_that_exists": "property",
        "other existing prop": "nested.property",
    }

    content_block_fn_expected_output = [
        [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
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
                    "styling": {
                        "parent": {
                            "classes": ["hide-succeeded-validation-target-child"]
                        }
                    },
                }
            ),
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": "Must have greater than or equal to $min_value rows.",
                        "params": {
                            "meta_properties_to_render": {
                                "property_that_exists": "property",
                                "other existing prop": "nested.property",
                            },
                            "min_value": 0,
                            "max_value": None,
                            "result_format": "SUMMARY",
                            "strict_max": None,
                            "strict_min": None,
                        },
                        "styling": None,
                    },
                }
            ),
            "1,313",
            ["this is nested"],
            [5],
        ]
    ]
    content_block_fn_output = content_block_fn(result=evr_success)
    assert content_block_fn_output == content_block_fn_expected_output


@pytest.mark.filterwarnings("ignore:V2 API style custom rendering*:DeprecationWarning")
def test_ValidationResultsTableContentBlockRenderer_get_content_block_fn_with_v2_api_style_custom_rendering():
    """Test backwards support for custom expectation rendering with the V2 API as described at
    https://docs.greatexpectations.io/en/latest/reference/spare_parts/data_docs_reference.html#customizing-data-docs.
    """

    custom_expectation_template = "custom_expectation_template"
    custom_expectation_observed_value = "custom_expectation_observed_value"

    class ValidationResultsTableContentBlockRendererWithV2ApiStyleCustomExpectations(
        ValidationResultsTableContentBlockRenderer
    ):
        @classmethod
        def expect_custom_expectation_written_in_v2_api_style(
            cls, expectation, styling=None, include_column_name: bool = True
        ):
            return [
                RenderedStringTemplateContent(
                    content_block_type="string_template",
                    string_template={
                        "template": custom_expectation_template,
                        "params": expectation.kwargs,
                        "styling": styling,
                    },
                )
            ]

    evr = ExpectationValidationResult(
        success=True,
        result={
            "observed_value": custom_expectation_observed_value,
        },
        exception_info={
            "raised_exception": False,
            "exception_message": None,
            "exception_traceback": None,
        },
        expectation_config=ExpectationConfiguration(
            expectation_type="expect_custom_expectation",
            kwargs={"column": "a_column_name", "result_format": "SUMMARY"},
        ),
    )

    content_block_fn = ValidationResultsTableContentBlockRendererWithV2ApiStyleCustomExpectations._get_content_block_fn(
        "expect_custom_expectation_written_in_v2_api_style"
    )
    content_block_fn_output = content_block_fn(result=evr)

    content_block_fn_expected_output = [
        [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
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
                    "styling": {
                        "parent": {
                            "classes": ["hide-succeeded-validation-target-child"]
                        }
                    },
                }
            ),
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": custom_expectation_template,
                        "params": {
                            "column": "a_column_name",
                            "result_format": "SUMMARY",
                        },
                        "styling": None,
                    },
                }
            ),
            custom_expectation_observed_value,
        ]
    ]
    assert content_block_fn_output == content_block_fn_expected_output


def test_ValidationResultsTableContentBlockRenderer_get_observed_value(evr_success):
    evr_no_result_key = ExpectationValidationResult(
        success=True,
        exception_info={
            "raised_exception": False,
            "exception_message": None,
            "exception_traceback": None,
        },
        expectation_config=ExpectationConfiguration(
            expectation_type="expect_table_row_count_to_be_between",
            kwargs={"min_value": 0, "max_value": None, "result_format": "SUMMARY"},
        ),
    )

    evr_expect_column_values_to_not_be_null = ExpectationValidationResult(
        success=True,
        result={
            "element_count": 1313,
            "unexpected_count": 1050,
            "unexpected_percent": 79.96953541508,
            "partial_unexpected_list": [],
        },
        exception_info={
            "raised_exception": False,
            "exception_message": None,
            "exception_traceback": None,
        },
        expectation_config=ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": "Unnamed: 0", "mostly": 0.5, "result_format": "SUMMARY"},
        ),
    )

    evr_expect_column_values_to_be_null = ExpectationValidationResult(
        success=True,
        result={
            "element_count": 1313,
            "unexpected_count": 0,
            "unexpected_percent": 0.0,
            "partial_unexpected_list": [],
        },
        exception_info={
            "raised_exception": False,
            "exception_message": None,
            "exception_traceback": None,
        },
        expectation_config=ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_null",
            kwargs={"column": "Unnamed: 0", "mostly": 0.5, "result_format": "SUMMARY"},
        ),
    )

    evr_success_zero = ExpectationValidationResult(
        success=True,
        result={"observed_value": 0},
        exception_info={
            "raised_exception": False,
            "exception_message": None,
            "exception_traceback": None,
        },
        expectation_config=ExpectationConfiguration(
            expectation_type="expect_table_row_count_to_be_between",
            kwargs={"min_value": 0, "max_value": None, "result_format": "SUMMARY"},
        ),
    )

    # test _get_observed_value when evr.result["observed_value"] exists
    output_1 = get_renderer_impl(
        object_name=evr_success.expectation_config.expectation_type,
        renderer_type=LegacyDiagnosticRendererType.OBSERVED_VALUE,
    )[1](result=evr_success)
    assert output_1 == "1,313"
    # test _get_observed_value when evr.result does not exist
    output_2 = get_renderer_impl(
        object_name=evr_no_result_key.expectation_config.expectation_type,
        renderer_type=LegacyDiagnosticRendererType.OBSERVED_VALUE,
    )[1](result=evr_no_result_key)
    assert output_2 == "--"
    # test _get_observed_value for expect_column_values_to_not_be_null expectation type
    output_3 = get_renderer_impl(
        object_name=evr_expect_column_values_to_not_be_null.expectation_config.expectation_type,
        renderer_type=LegacyDiagnosticRendererType.OBSERVED_VALUE,
    )[1](result=evr_expect_column_values_to_not_be_null)
    assert output_3 == "≈20.03% not null"
    # test _get_observed_value for expect_column_values_to_be_null expectation type
    output_4 = get_renderer_impl(
        object_name=evr_expect_column_values_to_be_null.expectation_config.expectation_type,
        renderer_type=LegacyDiagnosticRendererType.OBSERVED_VALUE,
    )[1](result=evr_expect_column_values_to_be_null)
    assert output_4 == "100% null"
    # test _get_observed_value to be 0
    output_5 = get_renderer_impl(
        object_name=evr_success_zero.expectation_config.expectation_type,
        renderer_type=LegacyDiagnosticRendererType.OBSERVED_VALUE,
    )[1](result=evr_success_zero)
    assert output_5 == "0"


def test_ValidationResultsTableContentBlockRenderer_get_unexpected_statement(
    evr_success, evr_failed
):
    evr_no_result = ExpectationValidationResult(
        success=True,
        exception_info={
            "raised_exception": False,
            "exception_message": None,
            "exception_traceback": None,
        },
        expectation_config=ExpectationConfiguration(
            expectation_type="expect_table_row_count_to_be_between",
            kwargs={"min_value": 0, "max_value": None, "result_format": "SUMMARY"},
        ),
    )
    evr_failed_no_unexpected_count = ExpectationValidationResult(
        success=False,
        result={
            "element_count": 1313,
            "missing_count": 0,
            "missing_percent": 0.0,
            "unexpected_percent": 0.2284843869002285,
            "unexpected_percent_nonmissing": 0.2284843869002285,
            "partial_unexpected_list": [
                "Daly, Mr Peter Denis ",
                "Barber, Ms ",
                "Geiger, Miss Emily ",
            ],
            "partial_unexpected_index_list": [77, 289, 303],
            "partial_unexpected_counts": [
                {"value": "Barber, Ms ", "count": 1},
                {"value": "Daly, Mr Peter Denis ", "count": 1},
                {"value": "Geiger, Miss Emily ", "count": 1},
            ],
        },
        exception_info={
            "raised_exception": False,
            "exception_message": None,
            "exception_traceback": None,
        },
        expectation_config=ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_match_regex",
            kwargs={
                "column": "Name",
                "regex": "^\\s+|\\s+$",
                "result_format": "SUMMARY",
            },
        ),
    )

    # test for succeeded evr
    output_1 = get_renderer_impl(
        object_name=evr_success.expectation_config.expectation_type,
        renderer_type=LegacyDiagnosticRendererType.UNEXPECTED_STATEMENT,
    )[1](result=evr_success)
    assert output_1 == []

    # test for failed evr
    output_2 = get_renderer_impl(
        object_name=evr_failed.expectation_config.expectation_type,
        renderer_type=LegacyDiagnosticRendererType.UNEXPECTED_STATEMENT,
    )[1](result=evr_failed)
    assert output_2 == [
        RenderedStringTemplateContent(
            **{
                "content_block_type": "string_template",
                "string_template": {
                    "template": "\n\n$unexpected_count unexpected values found. $unexpected_percent of $element_count total rows.",
                    "params": {
                        "unexpected_count": "3",
                        "unexpected_percent": "≈0.2285%",
                        "element_count": "1,313",
                    },
                    "tag": "strong",
                    "styling": {"classes": ["text-danger"]},
                },
            }
        )
    ]

    # test for evr with no "result" key
    output_3 = get_renderer_impl(
        object_name=evr_no_result.expectation_config.expectation_type,
        renderer_type=LegacyDiagnosticRendererType.UNEXPECTED_STATEMENT,
    )[1](result=evr_no_result)
    print(json.dumps(output_3, indent=2))
    assert output_3 == []

    # test for evr with no unexpected count
    output_4 = get_renderer_impl(
        object_name=evr_failed_no_unexpected_count.expectation_config.expectation_type,
        renderer_type=LegacyDiagnosticRendererType.UNEXPECTED_STATEMENT,
    )[1](result=evr_failed_no_unexpected_count)
    print(output_4)
    assert output_4 == []

    # test for evr with exception
    evr_failed_exception = ExpectationValidationResult(
        success=False,
        exception_info={
            "raised_exception": True,
            "exception_message": "Unrecognized column: not_a_real_column",
            "exception_traceback": "Traceback (most recent call last):\n...more_traceback...",
        },
        expectation_config=ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_match_regex",
            kwargs={
                "column": "Name",
                "regex": "^\\s+|\\s+$",
                "result_format": "SUMMARY",
            },
        ),
    )

    output_5 = get_renderer_impl(
        object_name=evr_failed_exception.expectation_config.expectation_type,
        renderer_type=LegacyDiagnosticRendererType.UNEXPECTED_STATEMENT,
    )[1](result=evr_failed_exception)
    output_5 = [content.to_json_dict() for content in output_5]
    expected_output_5 = [
        {
            "content_block_type": "string_template",
            "string_template": {
                "template": "\n\n$expectation_type raised an exception:\n$exception_message",
                "params": {
                    "expectation_type": "expect_column_values_to_not_match_regex",
                    "exception_message": "Unrecognized column: not_a_real_column",
                },
                "tag": "strong",
                "styling": {
                    "classes": ["text-danger"],
                    "params": {
                        "exception_message": {"tag": "code"},
                        "expectation_type": {
                            "classes": ["badge", "badge-danger", "mb-2"]
                        },
                    },
                },
            },
        },
        {
            "content_block_type": "collapse",
            "collapse_toggle_link": "Show exception traceback...",
            "collapse": [
                {
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": "Traceback (most recent call last):\n...more_traceback...",
                        "tag": "code",
                    },
                }
            ],
            "inline_link": False,
        },
    ]
    assert output_5 == expected_output_5


def test_ValidationResultsTableContentBlockRenderer_get_unexpected_table(evr_success):
    evr_failed_no_result = ExpectationValidationResult(
        success=False,
        exception_info={
            "raised_exception": False,
            "exception_message": None,
            "exception_traceback": None,
        },
        expectation_config=ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={
                "column": "Unnamed: 0",
                "value_set": [],
                "result_format": "SUMMARY",
            },
        ),
    )

    evr_failed_no_unexpected_list_or_counts = ExpectationValidationResult(
        success=False,
        result={
            "element_count": 1313,
            "missing_count": 0,
            "missing_percent": 0.0,
            "unexpected_count": 1313,
            "unexpected_percent": 100.0,
            "unexpected_percent_nonmissing": 100.0,
        },
        exception_info={
            "raised_exception": False,
            "exception_message": None,
            "exception_traceback": None,
        },
        expectation_config=ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={
                "column": "Unnamed: 0",
                "value_set": [],
                "result_format": "SUMMARY",
            },
        ),
    )

    evr_failed_partial_unexpected_list = ExpectationValidationResult(
        success=False,
        result={
            "element_count": 1313,
            "missing_count": 0,
            "missing_percent": 0.0,
            "unexpected_count": 1313,
            "unexpected_percent": 100.0,
            "unexpected_percent_nonmissing": 100.0,
            "partial_unexpected_list": [
                1,
                2,
                3,
                4,
                5,
                6,
                7,
                8,
                9,
                10,
                11,
                12,
                13,
                14,
                15,
                16,
                17,
                18,
                19,
                20,
            ],
        },
        exception_info={
            "raised_exception": False,
            "exception_message": None,
            "exception_traceback": None,
        },
        expectation_config=ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={
                "column": "Unnamed: 0",
                "value_set": [],
                "result_format": "SUMMARY",
            },
        ),
    )

    evr_failed_partial_unexpected_counts = ExpectationValidationResult(
        success=False,
        result={
            "element_count": 1313,
            "missing_count": 0,
            "missing_percent": 0.0,
            "unexpected_count": 1313,
            "unexpected_percent": 100.0,
            "unexpected_percent_nonmissing": 100.0,
            "partial_unexpected_list": [
                1,
                2,
                3,
                4,
                5,
                6,
                7,
                8,
                9,
                10,
                11,
                12,
                13,
                14,
                15,
                16,
                17,
                18,
                19,
                20,
            ],
            "partial_unexpected_index_list": [
                0,
                1,
                2,
                3,
                4,
                5,
                6,
                7,
                8,
                9,
                10,
                11,
                12,
                13,
                14,
                15,
                16,
                17,
                18,
                19,
            ],
            "partial_unexpected_counts": [
                {"value": 1, "count": 1},
                {"value": 2, "count": 1},
                {"value": 3, "count": 1},
                {"value": 4, "count": 1},
                {"value": 5, "count": 1},
                {"value": 6, "count": 1},
                {"value": 7, "count": 1},
                {"value": 8, "count": 1},
                {"value": 9, "count": 1},
                {"value": 10, "count": 1},
                {"value": 11, "count": 1},
                {"value": 12, "count": 1},
                {"value": 13, "count": 1},
                {"value": 14, "count": 1},
                {"value": 15, "count": 1},
                {"value": 16, "count": 1},
                {"value": 17, "count": 1},
                {"value": 18, "count": 1},
                {"value": 19, "count": 1},
                {"value": 20, "count": 1},
            ],
        },
        exception_info={
            "raised_exception": False,
            "exception_message": None,
            "exception_traceback": None,
        },
        expectation_config=ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={
                "column": "Unnamed: 0",
                "value_set": [],
                "result_format": "SUMMARY",
            },
        ),
    )

    # test for succeeded evr
    output_1 = get_renderer_impl(
        object_name=evr_success.expectation_config.expectation_type,
        renderer_type=LegacyDiagnosticRendererType.UNEXPECTED_TABLE,
    )[1](result=evr_success)
    assert output_1 is None

    # test for failed evr with no "result" key
    output_2 = get_renderer_impl(
        object_name=evr_failed_no_result.expectation_config.expectation_type,
        renderer_type=LegacyDiagnosticRendererType.UNEXPECTED_TABLE,
    )[1](result=evr_failed_no_result)
    assert output_2 is None

    # test for failed evr with no unexpected list or unexpected counts
    output_3 = get_renderer_impl(
        object_name=evr_failed_no_unexpected_list_or_counts.expectation_config.expectation_type,
        renderer_type=LegacyDiagnosticRendererType.UNEXPECTED_TABLE,
    )[1](result=evr_failed_no_unexpected_list_or_counts)
    assert output_3 is None

    # test for failed evr with partial unexpected list
    output_4 = get_renderer_impl(
        object_name=evr_failed_partial_unexpected_list.expectation_config.expectation_type,
        renderer_type=LegacyDiagnosticRendererType.UNEXPECTED_TABLE,
    )[1](result=evr_failed_partial_unexpected_list)
    assert output_4[0].to_json_dict() == {
        "content_block_type": "table",
        "table": [
            [1],
            [2],
            [3],
            [4],
            [5],
            [6],
            [7],
            [8],
            [9],
            [10],
            [11],
            [12],
            [13],
            [14],
            [15],
            [16],
            [17],
            [18],
            [19],
            [20],
        ],
        "header_row": ["Sampled Unexpected Values"],
        "styling": {"body": {"classes": ["table-bordered", "table-sm", "mt-3"]}},
    }

    # test for failed evr with partial unexpected counts
    output_5 = get_renderer_impl(
        object_name=evr_failed_partial_unexpected_counts.expectation_config.expectation_type,
        renderer_type=LegacyDiagnosticRendererType.UNEXPECTED_TABLE,
    )[1](result=evr_failed_partial_unexpected_counts)
    assert output_5[0].to_json_dict() == {
        "content_block_type": "table",
        "header_row": ["Sampled Unexpected Values", "Count"],
        "styling": {"body": {"classes": ["table-bordered", "table-sm", "mt-3"]}},
        "table": [
            [1, 1],
            [2, 1],
            [3, 1],
            [4, 1],
            [5, 1],
            [6, 1],
            [7, 1],
            [8, 1],
            [9, 1],
            [10, 1],
            [11, 1],
            [12, 1],
            [13, 1],
            [14, 1],
            [15, 1],
            [16, 1],
            [17, 1],
            [18, 1],
            [19, 1],
            [20, 1],
        ],
    }


def test_ValidationResultsTableContentBlockRenderer_get_status_cell(
    evr_failed_with_exception, evr_success, evr_failed
):
    # test for failed evr with exception
    output_1 = get_renderer_impl(
        object_name=evr_failed_with_exception.expectation_config.expectation_type,
        renderer_type=LegacyDiagnosticRendererType.STATUS_ICON,
    )[1](result=evr_failed_with_exception)
    assert output_1.to_json_dict() == {
        "content_block_type": "string_template",
        "string_template": {
            "template": "$icon",
            "params": {"icon": "", "markdown_status_icon": "❗"},
            "styling": {
                "params": {
                    "icon": {
                        "classes": ["fas", "fa-exclamation-triangle", "text-warning"],
                        "tag": "i",
                    }
                }
            },
        },
    }

    # test for succeeded evr
    output_2 = get_renderer_impl(
        object_name=evr_success.expectation_config.expectation_type,
        renderer_type=LegacyDiagnosticRendererType.STATUS_ICON,
    )[1](result=evr_success)
    assert output_2.to_json_dict() == {
        "content_block_type": "string_template",
        "string_template": {
            "template": "$icon",
            "params": {"icon": "", "markdown_status_icon": "✅"},
            "styling": {
                "params": {
                    "icon": {
                        "classes": ["fas", "fa-check-circle", "text-success"],
                        "tag": "i",
                    }
                }
            },
        },
        "styling": {"parent": {"classes": ["hide-succeeded-validation-target-child"]}},
    }

    # test for failed evr
    output_3 = get_renderer_impl(
        object_name=evr_failed.expectation_config.expectation_type,
        renderer_type=LegacyDiagnosticRendererType.STATUS_ICON,
    )[1](result=evr_failed)
    assert output_3.to_json_dict() == {
        "content_block_type": "string_template",
        "string_template": {
            "template": "$icon",
            "params": {"icon": "", "markdown_status_icon": "❌"},
            "styling": {
                "params": {
                    "icon": {"tag": "i", "classes": ["fas", "fa-times", "text-danger"]}
                }
            },
        },
    }


def test_ValidationResultsTableContentBlockRenderer_get_unexpected_table_no_id_pk_pandas():
    evr_no_id_pk_pandas = ExpectationValidationResult(
        success=False,
        expectation_config=ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={
                "batch_id": "cb8e223838fcdb055f6cccad2af592ae",
                "column": "animals",
                "value_set": ["cat", "fish", "dog"],
            },
            meta={},
        ),
        result={
            "element_count": 6,
            "missing_count": 0,
            "missing_percent": 0.0,
            "partial_unexpected_counts": [
                {"count": 1, "value": "giraffe"},
                {"count": 1, "value": "lion"},
                {"count": 1, "value": "zebra"},
            ],
            "partial_unexpected_index_list": [3, 4, 5],
            "partial_unexpected_list": ["giraffe", "lion", "zebra"],
            "unexpected_count": 3,
            "unexpected_index_list": [3, 4, 5],
            "unexpected_list": ["giraffe", "lion", "zebra"],
            "unexpected_percent": 50.0,
            "unexpected_percent_nonmissing": 50.0,
            "unexpected_percent_total": 50.0,
        },
        exception_info={
            "exception_message": None,
            "exception_traceback": None,
            "raised_exception": False,
        },
    )
    rendered_value = get_renderer_impl(
        object_name=evr_no_id_pk_pandas.expectation_config.expectation_type,
        renderer_type=LegacyDiagnosticRendererType.UNEXPECTED_TABLE,
    )[1](result=evr_no_id_pk_pandas)
    assert rendered_value[0].to_json_dict() == {
        "content_block_type": "table",
        "styling": {"body": {"classes": ["table-bordered", "table-sm", "mt-3"]}},
        "table": [["giraffe", 1, "3"], ["lion", 1, "4"], ["zebra", 1, "5"]],
        "header_row": ["Unexpected Value", "Count", "Index"],
    }


def test_ValidationResultsTableContentBlockRenderer_get_unexpected_table_with_id_pk_pandas_and_query(
    evr_id_pk_basic_pandas,
):
    rendered_value = get_renderer_impl(
        object_name=evr_id_pk_basic_pandas.expectation_config.expectation_type,
        renderer_type=LegacyDiagnosticRendererType.UNEXPECTED_TABLE,
    )[1](result=evr_id_pk_basic_pandas)
    assert rendered_value[0].to_json_dict() == {
        "content_block_type": "table",
        "header_row": ["Unexpected Value", "Count", "Index"],
        "styling": {"body": {"classes": ["table-bordered", "table-sm", "mt-3"]}},
        "table": [["giraffe", 1, "3"], ["lion", 1, "4"], ["zebra", 1, "5"]],
    }
    assert rendered_value[1].to_json_dict() == {
        "collapse": [
            {
                "content_block_type": "string_template",
                "string_template": {"tag": "code", "template": "[3, 4, 5]"},
            }
        ],
        "collapse_toggle_link": "To retrieve all unexpected values...",
        "content_block_type": "collapse",
        "inline_link": False,
    }


def test_ValidationResultsTableContentBlockRenderer_get_unexpected_table_with_id_pk_pandas_with_sampled_table(
    evr_id_pk_basic_pandas,
):
    evr_id_pk_pandas = evr_id_pk_basic_pandas
    new_index = [3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]
    new_unexpected_list = [
        "giraffe",
        "lion",
        "zebra",
        "zebra",
        "zebra",
        "zebra",
        "zebra",
        "zebra",
        "zebra",
        "zebra",
        "zebra",
        "zebra",
        "zebra",
    ]
    evr_id_pk_pandas.result["partial_unexpected_index_list"] = new_index
    evr_id_pk_pandas.result["partial_unexpected_list"] = new_unexpected_list
    evr_id_pk_pandas.result["unexpected_count"] = 100
    evr_id_pk_pandas.result["unexpected_index_list"] = new_index
    evr_id_pk_pandas.result["unexpected_list"] = new_unexpected_list

    rendered_value = get_renderer_impl(
        object_name=evr_id_pk_pandas.expectation_config.expectation_type,
        renderer_type=LegacyDiagnosticRendererType.UNEXPECTED_TABLE,
    )[1](result=evr_id_pk_pandas)
    assert rendered_value[0].to_json_dict() == {
        "content_block_type": "table",
        "header_row": ["Sampled Unexpected Values", "Count", "Index"],
        "styling": {"body": {"classes": ["table-bordered", "table-sm", "mt-3"]}},
        "table": [
            ["giraffe", 1, "3"],
            ["lion", 1, "4"],
            ["zebra", 11, "5, 6, 7, 8, 9, 10, 11, 12, 13, 14, ..."],
        ],
    }


def test_ValidationResultsTableContentBlockRenderer_get_unexpected_table_with_id_pk_sql_with_query(
    evr_id_pk_basic_sql,
):
    rendered_value = get_renderer_impl(
        object_name=evr_id_pk_basic_sql.expectation_config.expectation_type,
        renderer_type=LegacyDiagnosticRendererType.UNEXPECTED_TABLE,
    )[1](result=evr_id_pk_basic_sql)
    assert rendered_value[0].to_json_dict() == {
        "content_block_type": "table",
        "header_row": ["Unexpected Value", "Count", "pk_1", "pk_2"],
        "styling": {"body": {"classes": ["table-bordered", "table-sm", "mt-3"]}},
        "table": [
            ["giraffe", 1, "3", "three"],
            ["lion", 1, "4", "four"],
            ["zebra", 1, "5", "five"],
        ],
    }
    assert rendered_value[1].to_json_dict() == {
        "collapse": [
            {
                "content_block_type": "string_template",
                "string_template": {
                    "tag": "code",
                    "template": "SELECT animals, pk_1, pk_2 \n"
                    "FROM animal_names \n"
                    "WHERE animals IS NOT NULL AND "
                    "(animals NOT IN ('cat', "
                    "'fish', 'dog'))",
                },
            }
        ],
        "collapse_toggle_link": "To retrieve all unexpected values...",
        "content_block_type": "collapse",
        "inline_link": False,
    }


def test_ValidationResultsTableContentBlockRenderer_get_unexpected_table_with_id_pk_sql_with_query_with_sampled_table(
    evr_id_pk_basic_sql,
):
    new_index = [
        {"animals": "giraffe", "pk_1": 3},
        {"animals": "lion", "pk_1": 4},
        {"animals": "zebra", "pk_1": 5},
        {"animals": "zebra", "pk_1": 6},
        {"animals": "zebra", "pk_1": 7},
        {"animals": "zebra", "pk_1": 8},
        {"animals": "zebra", "pk_1": 9},
        {"animals": "zebra", "pk_1": 10},
        {"animals": "zebra", "pk_1": 11},
        {"animals": "zebra", "pk_1": 12},
        {"animals": "zebra", "pk_1": 13},
        {"animals": "zebra", "pk_1": 14},
        {"animals": "zebra", "pk_1": 15},
    ]
    new_unexpected_list = [
        "giraffe",
        "lion",
        "zebra",
        "zebra",
        "zebra",
        "zebra",
        "zebra",
        "zebra",
        "zebra",
        "zebra",
        "zebra",
        "zebra",
        "zebra",
    ]
    evr_id_pk_basic_sql.result["unexpected_index_column_names"] = ["pk_1"]
    evr_id_pk_basic_sql.result["partial_unexpected_index_list"] = new_index
    evr_id_pk_basic_sql.result["partial_unexpected_list"] = new_unexpected_list
    evr_id_pk_basic_sql.result["unexpected_count"] = 100
    evr_id_pk_basic_sql.result["unexpected_index_list"] = new_index
    evr_id_pk_basic_sql.result["unexpected_list"] = new_unexpected_list

    rendered_value = get_renderer_impl(
        object_name=evr_id_pk_basic_sql.expectation_config.expectation_type,
        renderer_type=LegacyDiagnosticRendererType.UNEXPECTED_TABLE,
    )[1](result=evr_id_pk_basic_sql)
    assert rendered_value[0].to_json_dict() == {
        "content_block_type": "table",
        "header_row": ["Sampled Unexpected Values", "Count", "pk_1"],
        "styling": {"body": {"classes": ["table-bordered", "table-sm", "mt-3"]}},
        "table": [
            ["giraffe", 1, "3"],
            ["lion", 1, "4"],
            ["zebra", 11, "5, 6, 7, 8, 9, 10, 11, 12, 13, 14, ..."],
        ],
    }
