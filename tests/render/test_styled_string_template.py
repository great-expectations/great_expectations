import pytest

import json

import great_expectations as ge
import great_expectations.render as render
from great_expectations.render.renderer import (
    DescriptivePageRenderer,
    DescriptiveColumnSectionRenderer,
    PrescriptiveColumnSectionRenderer,
    PrescriptivePageRenderer,
)
from great_expectations.render.view import DefaultJinjaPageView


def test_render_template():
    assert render.view.view.render_string_template({
        "template": "It was the $first_adj of times; it was the $second_adj of times.",
        "params": {
            "first_adj": "best",
            "second_adj": "worst",
        }
    }) == "It was the best of times; it was the worst of times."

    assert render.view.view.render_string_template({
        "template": "It was the $first_adj of times; it was the $second_adj of times.",
        "params": {
            "first_adj": "best",
            "second_adj": "worst",
        },
        "styling": {
            "default": {
                "classes": ["badge", "badge-warning"],
            }
        }
    }) == 'It was the <span class="badge badge-warning" >best</span> of times; it was the <span class="badge badge-warning" >worst</span> of times.'

    assert render.view.view.render_string_template({
        "template": "It was the $first_adj of times; it was the $second_adj of times.",
        "params": {
            "first_adj": "best",
            "second_adj": "worst",
        },
        "styling": {
            "default": {
                "classes": ["badge", "badge-warning"],
            },
            "params": {
                "first_adj": {
                    "classes": ["badge-error"],
                }
            }
        }
    }) == 'It was the <span class="badge-error" >best</span> of times; it was the <span class="badge badge-warning" >worst</span> of times.'

    assert render.view.view.render_string_template({
        "template": "It was the $first_adj of times; it was the $second_adj of times.",
        "params": {
            "first_adj": "best",
            "second_adj": "worst",
        },
        "styling": {
            "params": {
                "first_adj": {
                    "classes": ["badge", "badge-warning"],
                }
            }
        }
    }) == 'It was the <span class="badge badge-warning" >best</span> of times; it was the worst of times.'

    assert render.view.view.render_string_template({
        "template": "It was the $first_adj of times; it was the $second_adj of times.",
        "params": {
            "first_adj": "best",
            "second_adj": "worst",
        },
        "styling": {
            "params": {
                "first_adj": {
                    "classes": ["badge", "badge-warning"],
                    "attributes": {
                        "role": "alert"
                    },
                    "styles": {
                        "padding": "5px"
                    }
                }
            }
        }
    }) == 'It was the <span class="badge badge-warning" role="alert" style="padding:5px;" >best</span> of times; it was the worst of times.'


def test_evr_decriptive_failure_message():
    from great_expectations.render.renderer.content_block.evr_descriptive_failure_message import EvrDescriptiveFailureStylableStringTemplates

    evr = {
        "success": False,
        "result": {
            "element_count": 3484,
            "missing_count": 536,
            "missing_percent": 0.15384615384615385,
            "unexpected_count": 0,
            "unexpected_percent": 0.0,
            "unexpected_percent_nonmissing": 0.0,
            "partial_unexpected_list": [
                "float",
                "DOUBLE_PRECISION"
            ],
            "partial_unexpected_index_list": [],
            "partial_unexpected_counts": []
        },
        "exception_info": {
            "raised_exception": False,
            "exception_message": None,
            "exception_traceback": None,
        },
        "expectation_config": {
            "expectation_type": "expect_column_values_to_be_in_type_list",
            "kwargs": {
                "column": "my_column",
                "type_list": [
                    "string",
                    "VARCHAR",
                    "TEXT"
                ],
                "result_format": "SUMMARY"
            },
            "meta": {
                "BasicDatasetProfiler": {
                    "confidence": "very low"
                }
            }
        }
    }
    styled_string_templates = EvrDescriptiveFailureStylableStringTemplates.render(
        evr)
    print(styled_string_templates)

    assert styled_string_templates == [{
        "template": "$column contains unexpected types: $v__0 $v__1.",
        "params": {
            "column": "my_column",
            "v__0": "float",
            "v__1": "DOUBLE_PRECISION",
            "type_list": ["string", "VARCHAR", "TEXT"],
            "mostly": None,
            "result_format": "SUMMARY",
        },
        "styling": None
    }]
