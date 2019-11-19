# -*- coding: utf-8 -*-

from __future__ import unicode_literals
import json
import pypandoc

from great_expectations.core import ExpectationSuite, ExpectationConfiguration
from great_expectations.render.renderer import (
    ExpectationSuitePageRenderer,
    ProfilingResultsPageRenderer,
    ValidationResultsPageRenderer
)
from great_expectations.render.types import RenderedHeaderContent, RenderedTableContent


def test_ExpectationSuitePageRenderer_render_asset_notes():
    # import pypandoc
    # print(pypandoc.convert_text("*hi*", to='html', format="md"))

    result = ExpectationSuitePageRenderer._render_asset_notes(ExpectationSuite(
        data_asset_name="test", expectation_suite_name="test",
        meta={
            "notes": "*hi*"
        }
    ))
    print(result)
    assert result.text == ['This Expectation suite currently contains 0 total Expectations across 0 columns.', "*hi*"]

    result = ExpectationSuitePageRenderer._render_asset_notes(ExpectationSuite(
        data_asset_name="test", expectation_suite_name="test",
        meta={
            "notes": ["*alpha*", "_bravo_", "charlie"]
        }
    ))
    print(result)
    assert result.text == ['This Expectation suite currently contains 0 total Expectations across 0 columns.',
                              "*alpha*", "_bravo_", "charlie"]

    result = ExpectationSuitePageRenderer._render_asset_notes(ExpectationSuite(
        data_asset_name="test", expectation_suite_name="test",
        meta={
            "notes": {
                "format": "string",
                "content": ["*alpha*", "_bravo_", "charlie"]
            }
        }
    ))
    print(result)
    assert result.text == ['This Expectation suite currently contains 0 total Expectations across 0 columns.',
                           "*alpha*", "_bravo_", "charlie"]

    result = ExpectationSuitePageRenderer._render_asset_notes(ExpectationSuite(
        data_asset_name="test", expectation_suite_name="test",
        meta={
            "notes": {
                "format": "markdown",
                "content": "*alpha*"
            }
        }
    ))
    print(result)
    
    try:
        pypandoc.convert_text("*test*", format='md', to="html")
        assert result.text == ['This Expectation suite currently contains 0 total Expectations across 0 columns.',
                               "<p><em>alpha</em></p>\n"]
    except OSError:
        assert result.text == ['This Expectation suite currently contains 0 total Expectations across 0 columns.',
                               "*alpha*"]

    result = ExpectationSuitePageRenderer._render_asset_notes(ExpectationSuite(
        data_asset_name="test", expectation_suite_name="test",
        meta={
            "notes": {
                "format": "markdown",
                "content": ["*alpha*", "_bravo_", "charlie"]
            }
        }
    ))
    print(result)
    
    try:
        pypandoc.convert_text("*test*", format='md', to="html")
        assert result.text == ['This Expectation suite currently contains 0 total Expectations across 0 columns.',
                               "<p><em>alpha</em></p>\n", "<p><em>bravo</em></p>\n", "<p>charlie</p>\n"]
    except OSError:
        assert result.text == ['This Expectation suite currently contains 0 total Expectations across 0 columns.',
                               "*alpha*", "_bravo_", "charlie"]


def test_expectation_summary_in_ExpectationSuitePageRenderer_render_asset_notes():
    result = ExpectationSuitePageRenderer._render_asset_notes(ExpectationSuite(
        data_asset_name="test", expectation_suite_name="test",
        meta={},
        expectations=None
    ))
    print(result)
    assert result.text == ['This Expectation suite currently contains 0 total Expectations across 0 columns.']

    result = ExpectationSuitePageRenderer._render_asset_notes(ExpectationSuite(
        data_asset_name="test", expectation_suite_name="test",
        meta={
            "notes": {
                "format": "markdown",
                "content": ["hi"]
            }
        }
    ))
    print(result)
    
    try:
        pypandoc.convert_text("*test*", format='md', to="html")
        assert result.text == [
            'This Expectation suite currently contains 0 total Expectations across 0 columns.',
            '<p>hi</p>\n',
        ]
    except OSError:
        assert result.text == [
            'This Expectation suite currently contains 0 total Expectations across 0 columns.',
            'hi',
        ]

    result = ExpectationSuitePageRenderer._render_asset_notes(ExpectationSuite(
        data_asset_name="test", expectation_suite_name="test",
        meta={},
        expectations=[
            ExpectationConfiguration(
                expectation_type="expect_table_row_count_to_be_between",
                kwargs={"min_value": 0, "max_value": None}
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_to_exist",
                kwargs={"column": "x"}
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_to_exist",
                kwargs={"column": "y"}
            )
        ]
    ))
    print(result)
    assert result.text[0] == 'This Expectation suite currently contains 3 total Expectations across 2 columns.'


def test_ProfilingResultsPageRenderer(titanic_profiled_evrs_1):
    document = ProfilingResultsPageRenderer().render(titanic_profiled_evrs_1)
    print(document)
    # assert document == 0
    
    
def test_ValidationResultsPageRenderer_render_validation_header():
    validation_header = RenderedHeaderContent(**{
        "content_block_type": "header",
        "header": "Validation Overview",
        "styling": {
            "classes": ["col-12"],
            "header": {
                "classes": ["alert", "alert-secondary"]
            }
        }
    })
    assert ValidationResultsPageRenderer._render_validation_header() == validation_header
    
    
def test_ValidationResultsPageRenderer_render_validation_info(titanic_profiled_evrs_1):
    validation_info = ValidationResultsPageRenderer._render_validation_info(titanic_profiled_evrs_1)
    validation_info.table[2][1] = "__fixture__"
    validation_info.table[3][1] = "__run_id_fixture__"
    expected_validation_info = RenderedTableContent(**{
      "content_block_type": "table",
      "header": "Info",
      "table": [
        [
          "Full Data Asset Identifier",
          "my_datasource/default/default"
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
    })

    assert validation_info.to_json_dict() == expected_validation_info.to_json_dict()


def test_ValidationResultsPageRenderer_render_validation_statistics(titanic_profiled_evrs_1):
    validation_statistics = ValidationResultsPageRenderer._render_validation_statistics(titanic_profiled_evrs_1)
    expected_validation_statistics = RenderedTableContent(**{
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
    })

    assert validation_statistics == expected_validation_statistics

