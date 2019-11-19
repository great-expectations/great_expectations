# -*- coding: utf-8 -*-

from __future__ import unicode_literals
import json

from great_expectations.core import ExpectationConfiguration, ExpectationValidationResult
from great_expectations.render.renderer.content_block import (
    ValidationResultsTableContentBlockRenderer,
)

from great_expectations.render.types import RenderedComponentContent, RenderedStringTemplateContent


def test_ValidationResultsTableContentBlockRenderer_generate_expectation_row_with_errored_expectation(evr_failed_with_exception):
    result = ValidationResultsTableContentBlockRenderer.render([evr_failed_with_exception]).to_json_dict()
    assert result == {
      "content_block_type": "table",
      "table": [
        [
          {
            "content_block_type": "string_template",
            "string_template": {
              "template": "$icon",
              "params": {
                "icon": ""
              },
              "styling": {
                "params": {
                  "icon": {
                    "classes": [
                      "fas",
                      "fa-exclamation-triangle",
                      "text-warning"
                    ],
                    "tag": "i"
                  }
                }
              }
            }
          },
          [
            {
              "content_block_type": "string_template",
              "string_template": {
                "template": "$column Column can match any distribution.",
                "params": {
                  "column": "live",
                  "partition_object": None,
                  "threshold": None,
                  "result_format": "SUMMARY"
                }
              }
            },
            {
              "content_block_type": "string_template",
              "string_template": {
                "template": "\n\n$expectation_type raised an exception:\n$exception_message",
                "params": {
                  "expectation_type": "expect_column_kl_divergence_to_be_less_than",
                  "exception_message": "Invalid partition object."
                },
                "tag": "strong",
                "styling": {
                  "classes": [
                    "text-danger"
                  ],
                  "params": {
                    "exception_message": {
                      "tag": "code"
                    },
                    "expectation_type": {
                      "classes": [
                        "badge",
                        "badge-danger",
                        "mb-2"
                      ]
                    }
                  }
                }
              }
            },
            None
          ],
          "--"
        ]
      ],
      "styling": {
        "body": {
          "classes": [
            "table"
          ]
        },
        "classes": [
          "m-3",
          "table-responsive"
        ]
      },
      "header_row": [
        "Status",
        "Expectation",
        "Observed Value"
      ]
    }
    
    
def test_ValidationResultsTableContentBlockRenderer_render(titanic_profiled_name_column_evrs):
    validation_results_table = ValidationResultsTableContentBlockRenderer.render(titanic_profiled_name_column_evrs)

    assert isinstance(validation_results_table, RenderedComponentContent)
    assert validation_results_table.content_block_type == "table"
    assert len(validation_results_table.table) == 6
    assert validation_results_table.header_row == ["Status", "Expectation", "Observed Value"]
    assert validation_results_table.styling == {
        "body": {
          "classes": [
            "table"
          ]
        },
        "classes": [
          "m-3",
          "table-responsive"
        ]
    }
    assert json.dumps(validation_results_table.to_json_dict()).count("$icon") == 6
    
    
def test_ValidationResultsTableContentBlockRenderer_get_content_block_fn(evr_success):
    content_block_fn = ValidationResultsTableContentBlockRenderer._get_content_block_fn("expect_table_row_count_to_be_between")
    content_block_fn_output = content_block_fn(evr_success)

    content_block_fn_expected_output = [
      [
        RenderedStringTemplateContent(**{
          "content_block_type": "string_template",
          "string_template": {
            "template": "$icon",
            "params": {
              "icon": ""
            },
            "styling": {
              "params": {
                "icon": {
                  "classes": [
                    "fas",
                    "fa-check-circle",
                    "text-success"
                  ],
                  "tag": "i"
                }
              }
            }
          },
          "styling": {
            "parent": {
              "classes": [
                 "hide-succeeded-validation-target-child"
              ]
            }
          }
        }),
        RenderedStringTemplateContent(**{
          "content_block_type": "string_template",
          "string_template": {
            "template": "Must have more than $min_value rows.",
            "params": {
              "min_value": 0,
              "max_value": None,
              "result_format": "SUMMARY"
            },
            "styling": None
          }
        }),
        "1,313"
      ]
    ]
    assert content_block_fn_output == content_block_fn_expected_output
    

def test_ValidationResultsTableContentBlockRenderer_get_observed_value(evr_success):
    evr_no_result_key = ExpectationValidationResult(
      success=True,
      exception_info={
        "raised_exception": False,
        "exception_message": None,
        "exception_traceback": None
      },
      expectation_config=ExpectationConfiguration(
        expectation_type="expect_table_row_count_to_be_between",
        kwargs={
          "min_value": 0,
          "max_value": None,
          "result_format": "SUMMARY"
        }
      )
    )
    
    evr_expect_column_values_to_not_be_null = ExpectationValidationResult(
      success=True,
      result={
        "element_count": 1313,
        "unexpected_count": 1050,
        "unexpected_percent": 79.96953541508,
        "partial_unexpected_list": []
      },
      exception_info={
        "raised_exception": False,
        "exception_message": None,
        "exception_traceback": None
      },
      expectation_config=ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={
          "column": "Unnamed: 0",
          "mostly": 0.5,
          "result_format": "SUMMARY"
        }
        )
    )

    evr_expect_column_values_to_be_null = ExpectationValidationResult(
        success=True,
        result={
            "element_count": 1313,
            "unexpected_count": 0,
            "unexpected_percent": 0.0,
            "partial_unexpected_list": []
        },
        exception_info={
            "raised_exception": False,
            "exception_message": None,
            "exception_traceback": None
        },
        expectation_config=ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_null",
            kwargs={
                "column": "Unnamed: 0",
                "mostly": 0.5,
                "result_format": "SUMMARY"
            }
        )
    )
    
    # test _get_observed_value when evr.result["observed_value"] exists
    output_1 = ValidationResultsTableContentBlockRenderer._get_observed_value(evr_success)
    assert output_1 == "1,313"
    # test _get_observed_value when evr.result does not exist
    output_2 = ValidationResultsTableContentBlockRenderer._get_observed_value(evr_no_result_key)
    assert output_2 == "--"
    # test _get_observed_value for expect_column_values_to_not_be_null expectation type
    output_3 = ValidationResultsTableContentBlockRenderer._get_observed_value(evr_expect_column_values_to_not_be_null)
    assert output_3 == "≈20.03% not null"
    # test _get_observed_value for expect_column_values_to_be_null expectation type
    output_4 = ValidationResultsTableContentBlockRenderer._get_observed_value(evr_expect_column_values_to_be_null)
    assert output_4 == "100% null"

    
def test_ValidationResultsTableContentBlockRenderer_get_unexpected_statement(evr_success, evr_failed):
    evr_no_result = ExpectationValidationResult(
      success=True,
      exception_info={
        "raised_exception": False,
        "exception_message": None,
        "exception_traceback": None
      },
      expectation_config=ExpectationConfiguration(
        expectation_type="expect_table_row_count_to_be_between",
        kwargs={
          "min_value": 0,
          "max_value": None,
          "result_format": "SUMMARY"
        }
      )
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
                "Geiger, Miss Emily "
            ],
            "partial_unexpected_index_list": [
                77,
                289,
                303
            ],
            "partial_unexpected_counts": [
                {
                    "value": "Barber, Ms ",
                    "count": 1
                },
                {
                    "value": "Daly, Mr Peter Denis ",
                    "count": 1
                },
                {
                    "value": "Geiger, Miss Emily ",
                    "count": 1
                }
            ]
        },
        exception_info={
            "raised_exception": False,
            "exception_message": None,
            "exception_traceback": None
        },
        expectation_config=ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_match_regex",
            kwargs={
                "column": "Name",
                "regex": "^\\s+|\\s+$",
                "result_format": "SUMMARY"
            }
    )
    )
    
    # test for succeeded evr
    output_1 = ValidationResultsTableContentBlockRenderer._get_unexpected_statement(evr_success)
    assert output_1 is None
    
    # test for failed evr
    output_2 = ValidationResultsTableContentBlockRenderer._get_unexpected_statement(evr_failed)
    assert output_2 == RenderedStringTemplateContent(**{
      "content_block_type": "string_template",
      "string_template": {
        "template": "\n\n$unexpected_count unexpected values found. $unexpected_percent of $element_count total rows.",
        "params": {
          "unexpected_count": '3',
          "unexpected_percent": "≈0.2285%",
          "element_count": '1,313'
        },
        "tag": "strong",
        "styling": {
          "classes": [
            "text-danger"
          ]
        }
      }
    })
    
    # test for evr with no "result" key
    output_3 = ValidationResultsTableContentBlockRenderer._get_unexpected_statement(evr_no_result)
    print(json.dumps(output_3, indent=2))
    assert output_3 is None
    
    # test for evr with no unexpected count
    output_4 = ValidationResultsTableContentBlockRenderer._get_unexpected_statement(evr_failed_no_unexpected_count)
    print(output_4)
    assert output_4 is None

    # test for evr with exception
    evr_failed_exception = ExpectationValidationResult(
        success=False,
        exception_info={
            "raised_exception": True,
            "exception_message": "Unrecognized column: not_a_real_column",
            "exception_traceback": "Traceback (most recent call last):\n...more_traceback..."
        },
        expectation_config=ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_match_regex",
            kwargs={
                "column": "Name",
                "regex": "^\\s+|\\s+$",
                "result_format": "SUMMARY"
            }
        )
)

    output_5 = ValidationResultsTableContentBlockRenderer._get_unexpected_statement(evr_failed_exception)
    assert output_5 == RenderedStringTemplateContent(**{
        'content_block_type': 'string_template',
        'string_template': {
            'template': '\n\n$expectation_type raised an exception:\n$exception_message',
            'params': {
                'expectation_type': 'expect_column_values_to_not_match_regex',
                'exception_message': 'Unrecognized column: not_a_real_column'},
            'tag': 'strong',
            'styling': {
                'classes': ['text-danger'],
                'params': {
                    'exception_message': {'tag': 'code'},
                    'expectation_type': {
                        'classes': ['badge', 'badge-danger', 'mb-2']
                    }
                }
            }
        }
    })


def test_ValidationResultsTableContentBlockRenderer_get_unexpected_table(evr_success):
    evr_failed_no_result = ExpectationValidationResult(
        success=False,
        exception_info={
            "raised_exception": False,
            "exception_message": None,
            "exception_traceback": None
        },
        expectation_config=ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={
                "column": "Unnamed: 0",
                "value_set": [],
                "result_format": "SUMMARY"
            }
    )
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
            "exception_traceback": None
        },
        expectation_config=ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={
                "column": "Unnamed: 0",
                "value_set": [],
                "result_format": "SUMMARY"
            }
    )
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
                20
            ],
        },
        exception_info={
            "raised_exception": False,
            "exception_message": None,
            "exception_traceback": None
        },
        expectation_config=ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={
                "column": "Unnamed: 0",
                "value_set": [],
                "result_format": "SUMMARY"
            }
)
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
                20
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
                19
            ],
            "partial_unexpected_counts": [
                {
                    "value": 1,
                    "count": 1
                },
                {
                    "value": 2,
                    "count": 1
                },
                {
                    "value": 3,
                    "count": 1
                },
                {
                    "value": 4,
                    "count": 1
                },
                {
                    "value": 5,
                    "count": 1
                },
                {
                    "value": 6,
                    "count": 1
                },
                {
                    "value": 7,
                    "count": 1
                },
                {
                    "value": 8,
                    "count": 1
                },
                {
                    "value": 9,
                    "count": 1
                },
                {
                    "value": 10,
                    "count": 1
                },
                {
                    "value": 11,
                    "count": 1
                },
                {
                    "value": 12,
                    "count": 1
                },
                {
                    "value": 13,
                    "count": 1
                },
                {
                    "value": 14,
                    "count": 1
                },
                {
                    "value": 15,
                    "count": 1
                },
                {
                    "value": 16,
                    "count": 1
                },
                {
                    "value": 17,
                    "count": 1
                },
                {
                    "value": 18,
                    "count": 1
                },
                {
                    "value": 19,
                    "count": 1
                },
                {
                    "value": 20,
                    "count": 1
                }
            ]
        },
        exception_info={
            "raised_exception": False,
            "exception_message": None,
            "exception_traceback": None
        },
        expectation_config=ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={
                "column": "Unnamed: 0",
                "value_set": [],
                "result_format": "SUMMARY"
            }
)
)
    
    # test for succeeded evr
    output_1 = ValidationResultsTableContentBlockRenderer._get_unexpected_table(evr_success)
    assert output_1 is None
    
    # test for failed evr with no "result" key
    output_2 = ValidationResultsTableContentBlockRenderer._get_unexpected_table(evr_failed_no_result)
    assert output_2 is None
    
    # test for failed evr with no unexpected list or unexpected counts
    output_3 = ValidationResultsTableContentBlockRenderer._get_unexpected_table(evr_failed_no_unexpected_list_or_counts)
    assert output_3 is None
    
    # test for failed evr with partial unexpected list
    output_4 = ValidationResultsTableContentBlockRenderer._get_unexpected_table(evr_failed_partial_unexpected_list)
    assert output_4.to_json_dict() == {
      "content_block_type": "table",
      "table": [
        [
          1
        ],
        [
          2
        ],
        [
          3
        ],
        [
          4
        ],
        [
          5
        ],
        [
          6
        ],
        [
          7
        ],
        [
          8
        ],
        [
          9
        ],
        [
          10
        ],
        [
          11
        ],
        [
          12
        ],
        [
          13
        ],
        [
          14
        ],
        [
          15
        ],
        [
          16
        ],
        [
          17
        ],
        [
          18
        ],
        [
          19
        ],
        [
          20
        ]
      ],
      "header_row": [
        "Unexpected Value"
      ],
      "styling": {
        "body": {
          "classes": [
            "table-bordered",
            "table-sm",
            "mt-3"
          ]
        }
      }
    }
    
    # test for failed evr with partial unexpected counts
    output_5 = ValidationResultsTableContentBlockRenderer._get_unexpected_table(evr_failed_partial_unexpected_counts)
    assert output_5.to_json_dict() == {
      "content_block_type": "table",
      "table": [
        [
          1,
          1
        ],
        [
          2,
          1
        ],
        [
          3,
          1
        ],
        [
          4,
          1
        ],
        [
          5,
          1
        ],
        [
          6,
          1
        ],
        [
          7,
          1
        ],
        [
          8,
          1
        ],
        [
          9,
          1
        ],
        [
          10,
          1
        ],
        [
          11,
          1
        ],
        [
          12,
          1
        ],
        [
          13,
          1
        ],
        [
          14,
          1
        ],
        [
          15,
          1
        ],
        [
          16,
          1
        ],
        [
          17,
          1
        ],
        [
          18,
          1
        ],
        [
          19,
          1
        ],
        [
          20,
          1
        ]
      ],
      "header_row": [
        "Unexpected Value",
        "Count"
      ],
      "styling": {
        "body": {
          "classes": [
            "table-bordered",
            "table-sm",
            "mt-3"
          ]
        }
      }
    }


def test_ValidationResultsTableContentBlockRenderer_get_status_cell(evr_failed_with_exception, evr_success, evr_failed):
    # test for failed evr with exception
    output_1 = ValidationResultsTableContentBlockRenderer._get_status_icon(evr_failed_with_exception)
    assert output_1.to_json_dict() == {
      "content_block_type": "string_template",
      "string_template": {
        "template": "$icon",
        "params": {
          "icon": ""
        },
        "styling": {
          "params": {
            "icon": {
              "classes": [
                "fas",
                "fa-exclamation-triangle",
                "text-warning"
              ],
              "tag": "i"
            }
          }
        }
      }
    }

    # test for succeeded evr
    output_2 = ValidationResultsTableContentBlockRenderer._get_status_icon(evr_success)
    assert output_2.to_json_dict() == {
      "content_block_type": "string_template",
      "string_template": {
        "template": "$icon",
        "params": {
          "icon": ""
        },
        "styling": {
          "params": {
            "icon": {
              "classes": [
                "fas",
                "fa-check-circle",
                "text-success"
              ],
              "tag": "i"
            }
          }
        }
      },
      "styling": {
        "parent": {
          "classes": [
            "hide-succeeded-validation-target-child"
          ]
        }
      }
    }
    
    # test for failed evr
    output_3 = ValidationResultsTableContentBlockRenderer._get_status_icon(evr_failed)
    assert output_3.to_json_dict() == {
      "content_block_type": "string_template",
      "string_template": {
        "template": "$icon",
        "params": {
          "icon": ""
        },
        "styling": {
          "params": {
            "icon": {
              "tag": "i",
              "classes": [
                "fas",
                "fa-times",
                "text-danger"
              ]
            }
          }
        }
      }
    }
