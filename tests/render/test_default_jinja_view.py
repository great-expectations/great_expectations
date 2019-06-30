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


@pytest.fixture()
def validation_results():
    with open("./tests/test_sets/expected_cli_results_default.json", "r") as infile:
        return json.load(infile)


@pytest.fixture()
def expectations():
    with open("./tests/test_sets/titanic_expectations.json", "r") as infile:
        return json.load(infile)


@pytest.fixture()
def document_snapshot():
    with open("./tests/render/fixtures/document_snapshot.json", "r") as infile:
        return json.load(infile)


@pytest.fixture()
def html_snapshot():
    with open("./tests/render/fixtures/html_snapshot.html", "r") as infile:
        return infile.read()


# def test_render_descriptive_page_view(validation_results, document_snapshot):
#     document = DescriptivePageRenderer.render(validation_results)
#     print(json.dumps(document, indent=2))
#     # print(DefaultJinjaPageView.render(renderer))
#     # TODO: Use above print to set up snapshot test once we like the result
#     # assert document == document_snapshot

def test_render_DefaultJinjaPageView(document_snapshot, html_snapshot):
    html = DefaultJinjaPageView.render(document_snapshot)
    print(html)
    # TODO: Use above print to set up snapshot test once we like the result

    assert "This is a beta feature! Expect changes in API, behavior, and design." in html
    assert html == html_snapshot


# def test_render_section_page():
#     section = {
#         "section_name": None,
#         "component_content": [
#             {
#                 "component_type": "header",
#                 "header": "Overview",
#             },
#             {
#                 "component_type": "table",
#                 "header": "Dataset info",
#                 "table_rows": [
#                     ["Number of variables", "12"],
#                     ["Number of observations", "891"],
#                 ],
#                 "styling": {
#                     "classes": [
#                         "col-6",
#                         "table-responsive"
#                     ],
#                     "styles": {
#                         "margin-top": "20px"
#                     },
#                     "body": {
#                         "classes": [
#                             "table",
#                             "table-sm"
#                         ]
#                     }
#                 }
#             }
#         ]
#     }

#     rendered_doc = ge.render.view.view.DefaultJinjaSectionView.render(section)


def test_render_header_component():
    header_component_content = {
        # "component_type": "header",
        "content_block_type": "header",
        "header": "Overview",
    }
    rendered_doc = ge.render.view.view.DefaultJinjaComponentView.render(
        {"content_block": header_component_content},
        section_loop=1,
        content_block_loop=2,
    )
    print(rendered_doc)
    assert False
