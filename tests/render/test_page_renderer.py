# -*- coding: utf-8 -*-

from __future__ import unicode_literals
import json
import pypandoc

from great_expectations.render.renderer import (
    ExpectationSuitePageRenderer,
    ProfilingResultsPageRenderer,
    ValidationResultsPageRenderer
)


def test_ExpectationSuitePageRenderer_render_asset_notes():
    # import pypandoc
    # print(pypandoc.convert_text("*hi*", to='html', format="md"))

    result = ExpectationSuitePageRenderer._render_asset_notes({
        "meta": {
            "notes": "*hi*"
        }
    })
    print(result)
    assert result["content"] == ["*hi*"]

    result = ExpectationSuitePageRenderer._render_asset_notes({
        "meta": {
            "notes": ["*alpha*", "_bravo_", "charlie"]
        }
    })
    print(result)
    assert result["content"] == ["*alpha*", "_bravo_", "charlie"]

    result = ExpectationSuitePageRenderer._render_asset_notes({
        "meta": {
            "notes": {
                "format": "string",
                "content": ["*alpha*", "_bravo_", "charlie"]
            }
        }
    })
    print(result)
    assert result["content"] == ["*alpha*", "_bravo_", "charlie"]

    result = ExpectationSuitePageRenderer._render_asset_notes({
        "meta": {
            "notes": {
                "format": "markdown",
                "content": "*alpha*"
            }
        }
    })
    print(result)
    
    try:
        pypandoc.convert_text("*test*", format='md', to="html")
        assert result["content"] == ["<p><em>alpha</em></p>\n"]
    except OSError:
        assert result["content"] == ["*alpha*"]

    result = ExpectationSuitePageRenderer._render_asset_notes({
        "meta": {
            "notes": {
                "format": "markdown",
                "content": ["*alpha*", "_bravo_", "charlie"]
            }
        }
    })
    print(result)
    
    try:
        pypandoc.convert_text("*test*", format='md', to="html")
        assert result["content"] == ["<p><em>alpha</em></p>\n", "<p><em>bravo</em></p>\n", "<p>charlie</p>\n"]
    except OSError:
        assert result["content"] == ["*alpha*", "_bravo_", "charlie"]


def test_expectation_summary_in_ExpectationSuitePageRenderer_render_asset_notes():
    result = ExpectationSuitePageRenderer._render_asset_notes({
        "meta" : {},
        "expectations" : {}
    })
    print(result)
    assert result["content"] == ['This Expectation suite currently contains 0 total Expectations across 0 columns.']

    result = ExpectationSuitePageRenderer._render_asset_notes({
        "meta" : {
            "notes" : {
                "format": "markdown",
                "content": ["hi"]
            }
        },
        "expectations" : {}
    })
    print(result)
    
    try:
        pypandoc.convert_text("*test*", format='md', to="html")
        assert result["content"] == [
            'This Expectation suite currently contains 0 total Expectations across 0 columns.',
            '<p>hi</p>\n',
        ]
    except OSError:
        assert result["content"] == [
            'This Expectation suite currently contains 0 total Expectations across 0 columns.',
            'hi',
        ]

    result = ExpectationSuitePageRenderer._render_asset_notes({
        "meta" : {},
        "expectations" : [
            {
                "expectation_type": "expect_table_row_count_to_be_between",
                "kwargs": { "min_value": 0, "max_value": None, }
            },
            {
                "expectation_type": "expect_column_to_exist",
                "kwargs": { "column": "x", }
            },
            {
                "expectation_type": "expect_column_to_exist",
                "kwargs": { "column": "y", }
            },
        ]
    })
    print(result)
    assert result["content"][0] == 'This Expectation suite currently contains 3 total Expectations across 2 columns.'


def test_ProfilingResultsPageRenderer(titanic_profiled_evrs_1):
    document = ProfilingResultsPageRenderer().render(titanic_profiled_evrs_1)
    print(document)
    # assert document == 0
    
    
def test_ValidationResultsPageRenderer_render_validation_header():
    validation_header = {
        "content_block_type": "header",
        "header": "Validation Overview",
        "styling": {
            "classes": ["col-12"],
            "header": {
                "classes": ["alert", "alert-secondary"]
            }
        }
    }
    assert ValidationResultsPageRenderer._render_validation_header() == validation_header
    
    
def test_ValidationResultsPageRenderer_render_validation_info(titanic_profiled_evrs_1):
    validation_info = ValidationResultsPageRenderer._render_validation_info(titanic_profiled_evrs_1)
    print(json.dumps(validation_info, indent=2))
    
    expected_validation_info = {
      "content_block_type": "table",
      "header": "Info",
      "table": [
        [
          "Full Data Asset Identifier",
          ""
        ],
        [
          "Expectation Suite Name",
          "default"
        ],
        [
          "Great Expectations Version",
          "__fixture__"
        ],
        [
          "Run ID",
          "__run_id_fixture__"
        ],
        [
          "Validation Succeeded",
          False
        ]
      ],
      "styling": {
        "classes": [
          "col-12",
          "table-responsive"
        ],
        "styles": {
          "margin-top": "20px"
        },
        "body": {
          "classes": [
            "table",
            "table-sm"
          ]
        }
      }
    }

    assert validation_info == expected_validation_info


def test_ValidationResultsPageRenderer_render_validation_statistics(titanic_profiled_evrs_1):
    validation_statistics = ValidationResultsPageRenderer._render_validation_statistics(titanic_profiled_evrs_1)
    print(json.dumps(validation_statistics, indent=2))
    
    expected_validation_statistics = {
      "content_block_type": "table",
      "header": "Statistics",
      "table": [
        [
          "Evaluated Expectations",
          51
        ],
        [
          "Successful Expectations",
          43
        ],
        [
          "Unsuccessful Expectations",
          8
        ],
        [
          "Success Percent",
          "≈84.31%"
        ]
      ],
      "styling": {
        "classes": [
          "col-6",
          "table-responsive"
        ],
        "styles": {
          "margin-top": "20px"
        },
        "body": {
          "classes": [
            "table",
            "table-sm"
          ]
        }
      }
    }

    assert validation_statistics == expected_validation_statistics


def test_ValidationResultsPageRenderer_render_batch_kwargs(titanic_profiled_evrs_1):
    batch_kwargs_table = ValidationResultsPageRenderer._render_batch_kwargs(titanic_profiled_evrs_1)
    print(json.dumps(batch_kwargs_table, indent=2))

    expected_batch_kwarg_table = {
        "content_block_type": "table",
        "header": "Batch Kwargs",
        "table": [
            [
                "engine",
                {
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": "$value",
                        "params": {
                            "value": "python"
                        },
                        "styling": {
                            "default": {
                                "styles": {
                                    "word-break": "break-all"
                                }
                            }
                        }
                    },
                    "styling": {
                        "parent": {
                            "classes": [
                                "pl-3"
                            ]
                        }
                    }
                }
            ],
            [
                "partition_id",
                {
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": "$value",
                        "params": {
                            "value": "Titanic"
                        },
                        "styling": {
                            "default": {
                                "styles": {
                                    "word-break": "break-all"
                                }
                            }
                        }
                    },
                    "styling": {
                        "parent": {
                            "classes": [
                                "pl-3"
                            ]
                        }
                    }
                }
            ],
            [
                "path",
                {
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": "$value",
                        "params": {
                            "value": "project_dir/project_path/data/titanic/Titanic.csv"
                        },
                        "styling": {
                            "default": {
                                "styles": {
                                    "word-break": "break-all"
                                }
                            }
                        }
                    },
                    "styling": {
                        "parent": {
                            "classes": [
                                "pl-3"
                            ]
                        }
                    }
                }
            ],
            [
                "sep",
                {
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": "$value",
                        "params": {
                            "value": "None"
                        },
                        "styling": {
                            "default": {
                                "styles": {
                                    "word-break": "break-all"
                                }
                            }
                        }
                    },
                    "styling": {
                        "parent": {
                            "classes": [
                                "pl-3"
                            ]
                        }
                    }
                }
            ]
        ],
        "styling": {
            "classes": [
                "col-12",
                "table-responsive"
            ],
            "styles": {
                "margin-top": "20px"
            },
            "body": {
                "classes": [
                    "table",
                    "table-sm"
                ]
            }
        }
    }

    assert batch_kwargs_table == expected_batch_kwarg_table
