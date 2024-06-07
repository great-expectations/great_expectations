import pytest

import great_expectations as gx
from great_expectations.core import (
    ExpectationSuite,
)
from great_expectations.expectations.expectation_configuration import (
    ExpectationConfiguration,
)
from great_expectations.render import (
    RenderedDocumentContent,
    RenderedHeaderContent,
    RenderedSectionContent,
    RenderedTableContent,
)
from great_expectations.render.renderer import (
    ExpectationSuitePageRenderer,
)
from great_expectations.render.view import DefaultMarkdownPageView

# module level markers
pytestmark = pytest.mark.big


@pytest.fixture()
def expectation_suite_to_render_with_notes():
    expectation_suite = ExpectationSuite(
        name="default",
        meta={"great_expectations_version": "0.13.0-test"},
        expectations=[
            ExpectationConfiguration(
                type="expect_column_to_exist",
                kwargs={"column": "infinities"},
            ),
            ExpectationConfiguration(type="expect_column_to_exist", kwargs={"column": "nulls"}),
            ExpectationConfiguration(type="expect_column_to_exist", kwargs={"column": "naturals"}),
            ExpectationConfiguration(
                type="expect_column_distinct_values_to_be_in_set",
                kwargs={"column": "irrationals", "value_set": ["*", "1st", "2nd"]},
            ),
            ExpectationConfiguration(
                type="expect_column_values_to_be_unique",
                kwargs={"column": "testings"},
                notes=[
                    "Example notes about this expectation. **Markdown** `Supported`.",
                    "Second example note **with** *Markdown*",
                ],
            ),
        ],
    )
    return expectation_suite


def test_render_section_page():
    section = RenderedSectionContent(
        **{
            "section_name": None,
            "content_blocks": [
                RenderedHeaderContent(
                    **{
                        "content_block_type": "header",
                        "header": "Overview",
                    }
                ),
                RenderedTableContent(
                    **{
                        "content_block_type": "table",
                        "header": "Dataset info",
                        "table": [
                            ["Number of variables", "12"],
                            ["Number of observations", "891"],
                        ],
                        "styling": {
                            "classes": ["col-6", "table-responsive"],
                            "styles": {"margin-top": "20px"},
                            "body": {"classes": ["table", "table-sm"]},
                        },
                    }
                ),
            ],
        }
    )

    rendered_doc = gx.render.view.view.DefaultMarkdownPageView().render(
        RenderedDocumentContent(sections=[section])
    )

    rendered_doc = rendered_doc.replace(" ", "").replace("\t", "").replace("\n", "")

    assert (
        rendered_doc
        == """
        #ValidationResults
        ##Overview
        ###Datasetinfo
        ||||------------|------------|Numberofvariables|12Numberofobservations|891
        -----------------------------------------------------------
        Poweredby[GreatExpectations](https://greatexpectations.io/)
        """.replace(" ", "")
        .replace("\t", "")
        .replace("\n", "")
    )


def test_render_expectation_suite_for_Markdown(expectation_suite_to_render_with_notes):
    expectation_suite_page_renderer = ExpectationSuitePageRenderer()
    rendered_document_content_list = expectation_suite_page_renderer.render(
        expectation_suite_to_render_with_notes
    )
    md_str = DefaultMarkdownPageView().render(rendered_document_content_list)
    md_str = " ".join(md_str)
    md_str = md_str.replace(" ", "").replace("\t", "").replace("\n", "")
    assert (
        md_str
        == r"""
   # Validation Results
## Overview
### Info
 |  |  |
 | ------------  | ------------ |
Expectation Suite Name  | default
Great Expectations Version  | 0.13.0-test
### Notes
    This Expectation suite currently contains 5 total Expectations across 5 columns.
## infinities
  * is a required field.
  * ***
## irrationals
  * distinct values must belong to this set: \* **1st** **2nd**.
  * ***
## naturals
  * is a required field.
  * ***
## nulls
  * is a required field.
  * ***
## testings
  * values must be unique.
    #### Notes:
      Example notes about this expectation. **Markdown** `Supported`.

      Second example note **with** *Markdown*
  * ***
-----------------------------------------------------------
Powered by [Great Expectations](https://greatexpectations.io/)
    """.replace(" ", "")
        .replace("\t", "")
        .replace("\n", "")
    )
