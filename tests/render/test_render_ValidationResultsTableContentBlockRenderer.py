
import json

from great_expectations.render.renderer.content_block import (
    ValidationResultsTableContentBlockRenderer,
)

from great_expectations.render.types import RenderedComponentContent


def test_ValidationResultsTableContentBlockRenderer_generate_expectation_row_with_errored_expectation():
    evr = {
        'success': False,
        'exception_info': {
            'raised_exception': True,
            'exception_message': 'Invalid partition object.',
            'exception_traceback': 'Traceback (most recent call last):\n  File "/Users/abe/Documents/superconductive/tools/great_expectations/great_expectations/data_asset/data_asset.py", line 216, in wrapper\n    return_obj = func(self, **evaluation_args)\n  File "/Users/abe/Documents/superconductive/tools/great_expectations/great_expectations/dataset/dataset.py", line 106, in inner_wrapper\n    evaluation_result = func(self, column, *args, **kwargs)\n  File "/Users/abe/Documents/superconductive/tools/great_expectations/great_expectations/dataset/dataset.py", line 3381, in expect_column_kl_divergence_to_be_less_than\n    raise ValueError("Invalid partition object.")\nValueError: Invalid partition object.\n'
        },
        'expectation_config': {
            'expectation_type': 'expect_column_kl_divergence_to_be_less_than',
            'kwargs': {
                'column': 'live',
                'partition_object': None,
                'threshold': None,
                'result_format': 'SUMMARY'
            },
            'meta': {
                'BasicDatasetProfiler': {'confidence': 'very low'}
            }
        }
    }
    result = ValidationResultsTableContentBlockRenderer.render([evr])
    print(json.dumps(result, indent=2))
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
                    "template": "$column Kullback-Leibler (KL) divergence with respect to a given distribution must be lower than a provided threshold but no distribution was specified.",
                    "params": {
                    "column": "live",
                    "partition_object": None,
                    "threshold": None,
                    "result_format": "SUMMARY"
                    },
                    "styling": {
                    "default": {
                        "classes": [
                        "badge",
                        "badge-secondary"
                        ]
                    },
                    "params": {
                        "sparklines_histogram": {
                        "styles": {
                            "font-family": "serif !important"
                        }
                        }
                    }
                    }
                }
                },
                {
                "content_block_type": "string_template",
                "string_template": {
                    "template": "Expectation failed to execute.",
                    "params": {},
                    "tag": "strong",
                    "styling": {
                    "classes": [
                        "text-warning"
                    ]
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
    print(json.dumps(validation_results_table, indent=2))

    assert type(validation_results_table) is RenderedComponentContent
    assert validation_results_table["content_block_type"] == "table"
    assert len(validation_results_table["table"]) == 6
    assert validation_results_table["header_row"] == ["Status", "Expectation", "Observed Value"]
    assert validation_results_table["styling"] == {
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
    assert json.dumps(validation_results_table).count("$icon") == 6
    
    
def test_ValidationResultsTableContentBlockRenderer_get_content_block_fn():
    content_block_fn = ValidationResultsTableContentBlockRenderer._get_content_block_fn("expect_table_row_count_to_be_between")
    evr = {
      "success": True,
      "result": {
        "observed_value": 1313
      },
      "exception_info": {
        "raised_exception": False,
        "exception_message": None,
        "exception_traceback": None
      },
      "expectation_config": {
        "expectation_type": "expect_table_row_count_to_be_between",
        "kwargs": {
          "min_value": 0,
          "max_value": None,
          "result_format": "SUMMARY"
        }
      }
    }
    content_block_fn_output = content_block_fn(evr)
    print(json.dumps(content_block_fn_output, indent=2))
    
    content_block_fn_expected_output = [
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
                    "fa-check-circle",
                    "text-success"
                  ],
                  "tag": "i"
                }
              }
            }
          }
        },
        {
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
        },
        "1313"
      ]
    ]
    assert content_block_fn_output == content_block_fn_expected_output
    

def test_ValidationResultsTableContentBlockRenderer_get_observed_value():
    evr = {
      "success": True,
      "result": {
        "observed_value": 1313
      },
      "exception_info": {
        "raised_exception": False,
        "exception_message": None,
        "exception_traceback": None
      },
      "expectation_config": {
        "expectation_type": "expect_table_row_count_to_be_between",
        "kwargs": {
          "min_value": 0,
          "max_value": None,
          "result_format": "SUMMARY"
        }
      }
    }
    
    evr_no_result_key = {
      "success": True,
      "exception_info": {
        "raised_exception": False,
        "exception_message": None,
        "exception_traceback": None
      },
      "expectation_config": {
        "expectation_type": "expect_table_row_count_to_be_between",
        "kwargs": {
          "min_value": 0,
          "max_value": None,
          "result_format": "SUMMARY"
        }
      }
    }
    
    evr_expect_column_values_to_not_be_null = {
      "success": True,
      "result": {
        "element_count": 1313,
        "missing_count": 0,
        "missing_percent": 0.0,
        "unexpected_count": 0,
        "unexpected_percent": 0.0,
        "partial_unexpected_list": []
      },
      "exception_info": {
        "raised_exception": False,
        "exception_message": None,
        "exception_traceback": None
      },
      "expectation_config": {
        "expectation_type": "expect_column_values_to_not_be_null",
        "kwargs": {
          "column": "Unnamed: 0",
          "mostly": 0.5,
          "result_format": "SUMMARY"
        }
      }
    }

    evr_expect_column_values_to_be_null = {
        "success": True,
        "result": {
            "element_count": 1313,
            "missing_count": 0,
            "missing_percent": 0.0,
            "unexpected_count": 0,
            "unexpected_percent": 0.0,
            "partial_unexpected_list": []
        },
        "exception_info": {
            "raised_exception": False,
            "exception_message": None,
            "exception_traceback": None
        },
        "expectation_config": {
            "expectation_type": "expect_column_values_to_be_null",
            "kwargs": {
                "column": "Unnamed: 0",
                "mostly": 0.5,
                "result_format": "SUMMARY"
            }
        }
    }
    
    output_1 = ValidationResultsTableContentBlockRenderer._get_observed_value(evr)
    print(output_1)
    assert output_1 == 1313
    output_2 = ValidationResultsTableContentBlockRenderer._get_observed_value(evr_no_result_key)
    print(output_2)
    assert output_2 == "--"
    output_3 = ValidationResultsTableContentBlockRenderer._get_observed_value(evr_expect_column_values_to_not_be_null)
    print(output_3)
    assert(output_3) == "0 null"
    output_4 = ValidationResultsTableContentBlockRenderer._get_observed_value(evr_expect_column_values_to_be_null)
    print(output_4)
    assert output_4 == "1313 null"
    
    
def test_ValidationResultsTableContentBlockRenderer_get_unexpected_statement():
    evr_success = {
      "success": True,
      "result": {
        "observed_value": 1313
      },
      "exception_info": {
        "raised_exception": False,
        "exception_message": None,
        "exception_traceback": None
      },
      "expectation_config": {
        "expectation_type": "expect_table_row_count_to_be_between",
        "kwargs": {
          "min_value": 0,
          "max_value": None,
          "result_format": "SUMMARY"
        }
      }
    }
    evr_failed = {
      "success": False,
      "result": {
        "element_count": 1313,
        "missing_count": 0,
        "missing_percent": 0.0,
        "unexpected_count": 3,
        "unexpected_percent": 0.002284843869002285,
        "unexpected_percent_nonmissing": 0.002284843869002285,
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
      "exception_info": {
        "raised_exception": False,
        "exception_message": None,
        "exception_traceback": None
      },
      "expectation_config": {
        "expectation_type": "expect_column_values_to_not_match_regex",
        "kwargs": {
          "column": "Name",
          "regex": "^\\s+|\\s+$",
          "result_format": "SUMMARY"
        }
      }
    }
    evr_no_result = {
      "success": True,
      "exception_info": {
        "raised_exception": False,
        "exception_message": None,
        "exception_traceback": None
      },
      "expectation_config": {
        "expectation_type": "expect_table_row_count_to_be_between",
        "kwargs": {
          "min_value": 0,
          "max_value": None,
          "result_format": "SUMMARY"
        }
      }
    }
    evr_failed_no_unexpected_count = {
        "success": False,
        "result": {
            "element_count": 1313,
            "missing_count": 0,
            "missing_percent": 0.0,
            "unexpected_percent": 0.002284843869002285,
            "unexpected_percent_nonmissing": 0.002284843869002285,
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
        "exception_info": {
            "raised_exception": False,
            "exception_message": None,
            "exception_traceback": None
        },
        "expectation_config": {
            "expectation_type": "expect_column_values_to_not_match_regex",
            "kwargs": {
                "column": "Name",
                "regex": "^\\s+|\\s+$",
                "result_format": "SUMMARY"
            }
        }
    }
    
    output_1 = ValidationResultsTableContentBlockRenderer._get_unexpected_statement(evr_success)
    print(output_1)
    assert output_1 is None
    
    output_2 = ValidationResultsTableContentBlockRenderer._get_unexpected_statement(evr_failed)
    print(json.dumps(output_2, indent=2))
    assert output_2 == {
      "content_block_type": "string_template",
      "string_template": {
        "template": "\n\n$unexpected_count unexpected values found. $unexpected_percent of $element_count total rows.",
        "params": {
          "unexpected_count": 3,
          "unexpected_percent": "0.23%",
          "element_count": 1313
        },
        "tag": "strong",
        "styling": {
          "classes": [
            "text-danger"
          ]
        }
      }
    }
    
    output_3 = ValidationResultsTableContentBlockRenderer._get_unexpected_statement(evr_no_result)
    print(json.dumps(output_3, indent=2))
    assert output_3 == {
      "content_block_type": "string_template",
      "string_template": {
        "template": "Expectation failed to execute.",
        "params": {},
        "tag": "strong",
        "styling": {
          "classes": [
            "text-warning"
          ]
        }
      }
    }
    
    output_4 = ValidationResultsTableContentBlockRenderer._get_unexpected_statement(evr_failed_no_unexpected_count)
    print(output_4)
    assert output_4 is None


def test_ValidationResultsTableContentBlockRenderer_get_unexpected_table():
    evr_success = {
      "success": True,
      "result": {
        "observed_value": 1313
      },
      "exception_info": {
        "raised_exception": False,
        "exception_message": None,
        "exception_traceback": None
      },
      "expectation_config": {
        "expectation_type": "expect_table_row_count_to_be_between",
        "kwargs": {
          "min_value": 0,
          "max_value": None,
          "result_format": "SUMMARY"
        }
      }
    }

    evr_failed_no_result = {
        "success": False,
        "exception_info": {
            "raised_exception": False,
            "exception_message": None,
            "exception_traceback": None
        },
        "expectation_config": {
            "expectation_type": "expect_column_values_to_be_in_set",
            "kwargs": {
                "column": "Unnamed: 0",
                "value_set": [],
                "result_format": "SUMMARY"
            }
        }
    }

    evr_failed_no_unexpected_list_or_counts = {
        "success": False,
        "result": {
            "element_count": 1313,
            "missing_count": 0,
            "missing_percent": 0.0,
            "unexpected_count": 1313,
            "unexpected_percent": 1.0,
            "unexpected_percent_nonmissing": 1.0,
        },
        "exception_info": {
            "raised_exception": False,
            "exception_message": None,
            "exception_traceback": None
        },
        "expectation_config": {
            "expectation_type": "expect_column_values_to_be_in_set",
            "kwargs": {
                "column": "Unnamed: 0",
                "value_set": [],
                "result_format": "SUMMARY"
            }
        }
    }

    evr_failed_partial_unexpected_list = {
        "success": False,
        "result": {
            "element_count": 1313,
            "missing_count": 0,
            "missing_percent": 0.0,
            "unexpected_count": 1313,
            "unexpected_percent": 1.0,
            "unexpected_percent_nonmissing": 1.0,
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
        "exception_info": {
            "raised_exception": False,
            "exception_message": None,
            "exception_traceback": None
        },
        "expectation_config": {
            "expectation_type": "expect_column_values_to_be_in_set",
            "kwargs": {
                "column": "Unnamed: 0",
                "value_set": [],
                "result_format": "SUMMARY"
            }
        }
    }

    evr_failed_partial_unexpected_counts = {
        "success": False,
        "result": {
            "element_count": 1313,
            "missing_count": 0,
            "missing_percent": 0.0,
            "unexpected_count": 1313,
            "unexpected_percent": 1.0,
            "unexpected_percent_nonmissing": 1.0,
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
        "exception_info": {
            "raised_exception": False,
            "exception_message": None,
            "exception_traceback": None
        },
        "expectation_config": {
            "expectation_type": "expect_column_values_to_be_in_set",
            "kwargs": {
                "column": "Unnamed: 0",
                "value_set": [],
                "result_format": "SUMMARY"
            }
        }
    }
    
    output_1 = ValidationResultsTableContentBlockRenderer._get_unexpected_table(evr_success)
    print(output_1)
    assert output_1 is None
    
    output_2 = ValidationResultsTableContentBlockRenderer._get_unexpected_table(evr_failed_no_result)
    print(output_2)
    assert output_2 is None
    
    output_3 = ValidationResultsTableContentBlockRenderer._get_unexpected_table(evr_failed_no_unexpected_list_or_counts)
    print(output_3)
    assert output_3 is None
    
    output_4 = ValidationResultsTableContentBlockRenderer._get_unexpected_table(evr_failed_partial_unexpected_list)
    print(json.dumps(output_4, indent=2))
    assert output_4 == {
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

    output_5 = ValidationResultsTableContentBlockRenderer._get_unexpected_table(evr_failed_partial_unexpected_counts)
    print(json.dumps(output_5, indent=2))
    assert output_5 == {
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


def test_ValidationResultsTableContentBlockRenderer_get_status_cell():
    evr_exception = {
        'success': False,
        'exception_info': {
            'raised_exception': True,
            'exception_message': 'Invalid partition object.',
            'exception_traceback': 'Traceback (most recent call last):\n  File "/Users/abe/Documents/superconductive/tools/great_expectations/great_expectations/data_asset/data_asset.py", line 216, in wrapper\n    return_obj = func(self, **evaluation_args)\n  File "/Users/abe/Documents/superconductive/tools/great_expectations/great_expectations/dataset/dataset.py", line 106, in inner_wrapper\n    evaluation_result = func(self, column, *args, **kwargs)\n  File "/Users/abe/Documents/superconductive/tools/great_expectations/great_expectations/dataset/dataset.py", line 3381, in expect_column_kl_divergence_to_be_less_than\n    raise ValueError("Invalid partition object.")\nValueError: Invalid partition object.\n'
        },
        'expectation_config': {
            'expectation_type': 'expect_column_kl_divergence_to_be_less_than',
            'kwargs': {
                'column': 'live',
                'partition_object': None,
                'threshold': None,
                'result_format': 'SUMMARY'
            },
            'meta': {
                'BasicDatasetProfiler': {'confidence': 'very low'}
            }
        }
    }
    
    evr_success = {
        "success": True,
        "result": {
            "observed_value": 1313
        },
        "exception_info": {
            "raised_exception": False,
            "exception_message": None,
            "exception_traceback": None
        },
        "expectation_config": {
            "expectation_type": "expect_table_row_count_to_be_between",
            "kwargs": {
                "min_value": 0,
                "max_value": None,
                "result_format": "SUMMARY"
            }
        }
    }
    
    evr_failed = {
      "success": False,
      "result": {
        "element_count": 1313,
        "missing_count": 0,
        "missing_percent": 0.0,
        "unexpected_count": 3,
        "unexpected_percent": 0.002284843869002285,
        "unexpected_percent_nonmissing": 0.002284843869002285,
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
      "exception_info": {
        "raised_exception": False,
        "exception_message": None,
        "exception_traceback": None
      },
      "expectation_config": {
        "expectation_type": "expect_column_values_to_not_match_regex",
        "kwargs": {
          "column": "Name",
          "regex": "^\\s+|\\s+$",
          "result_format": "SUMMARY"
        }
      }
    }
    
    output_1 = ValidationResultsTableContentBlockRenderer._get_status_icon(evr_exception)
    print(json.dumps(output_1, indent=2))
    assert output_1 == {
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

    output_2 = ValidationResultsTableContentBlockRenderer._get_status_icon(evr_success)
    print(json.dumps(output_2, indent=2))
    assert output_2 == {
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
      }
    }
    
    output_3 = ValidationResultsTableContentBlockRenderer._get_status_icon(evr_failed)
    print(json.dumps(output_3, indent=2))
    assert output_3 == {
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
