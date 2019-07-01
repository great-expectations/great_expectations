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
    # print(html)
    # TODO: Use above print to set up snapshot test once we like the result

    open("./tests/render/fixtures/html_snapshot.html", "w").write(html)

    assert "This is a beta feature! Expect changes in API, behavior, and design." in html
    assert html == html_snapshot


def test_render_section_page():
    section = {
        "section_name": None,
        "content_blocks": [
            {
                "content_block_type": "header",
                "header": "Overview",
            },
            {
                "content_block_type": "table",
                "header": "Dataset info",
                "table_rows": [
                    ["Number of variables", "12"],
                    ["Number of observations", "891"],
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
        ]
    }

    rendered_doc = ge.render.view.view.DefaultJinjaSectionView.render({
        "section": section,
        "section_loop": {"index": 1},
    })

    print(rendered_doc)
    assert rendered_doc == """\
<div id="section-1" class="ge-section container-fluid">
    <div class="row">
        
<div id="content-block-1" >
    <h3 id="content-block-1-header" >
        Overview
    </h3>
</div>
        
<div id="content-block-2" class="col-6 table-responsive" style="margin-top:20px;" >
    <h4 id="content-block-2-header" >
        Dataset info
    </h4>
    <table id="content-block-2-body" class="table table-sm" >
        <tr>
            <td id="content-block-2-cell-1-1" >Number of variables</td><td id="content-block-2-cell-1-2" >12</td></tr><tr>
            <td id="content-block-2-cell-2-1" >Number of observations</td><td id="content-block-2-cell-2-2" >891</td></tr></table>
</div>
        
    </div>
</div>"""


def test_rendering_components_without_section_loop_index():
    header_component_content = {
        # "component_type": "header",
        "content_block_type": "header",
        "header": "Overview",
    }
    rendered_doc = ge.render.view.view.DefaultJinjaComponentView.render({
        "content_block": header_component_content,
        "content_block_loop": {"index": 2},
    })
    print(rendered_doc)
    assert rendered_doc == \
        """
<div id="content-block-2" >
    <h3 id="content-block-2-header" >
        Overview
    </h3>
</div>"""

    rendered_doc = ge.render.view.view.DefaultJinjaComponentView.render({
        "content_block": header_component_content,
    })
    print(rendered_doc)
    assert rendered_doc == \
        """
<div id="content-block" >
    <h3 id="content-block-header" >
        Overview
    </h3>
</div>"""

    rendered_doc = ge.render.view.view.DefaultJinjaComponentView.render({
        "content_block": header_component_content,
        "section_loop": {"index": 3},
    })
    print(rendered_doc)
    assert rendered_doc == \
        """
<div id="content-block" >
    <h3 id="content-block-header" >
        Overview
    </h3>
</div>"""


def test_rendering_components_with_styling():
    # Medium-complicated example to verify that all the things are correctly piped to all the places

    header_component_content = {
        # "component_type": "table",
        "content_block_type": "table",
        "header": {
            "template": "$var1 $var2 $var3",
            "params": {
                "var1": "AAA",
                "var2": "BBB",
                "var3": "CCC",
            },
            "styling": {
                "default": {
                    "classes": ["x"]
                },
                "params": {
                    "var1": {
                        "classes": ["y"]
                    }
                }
            }
        },
        "subheader": {
            "template": "$var1 $var2 $var3",
            "params": {
                "var1": "aaa",
                "var2": "bbb",
                "var3": "ccc",
            },
            "styling": {
                "default": {
                    "classes": ["xx"]
                },
                "params": {
                    "var1": {
                        "classes": ["yy"]
                    }
                }
            }
        },
        "table_rows": [
            ["Mean", "446"],
            ["Minimum", "1"],
        ],
        "styling": {
            "classes": ["root_foo"],
            "styles": {"root": "bar"},
            "attributes": {"root": "baz"},
            "header": {
                "classes": ["header_foo"],
                "styles": {"header": "bar"},
                "attributes": {"header": "baz"},
            },
            "subheader": {
                "classes": ["subheader_foo"],
                "styles": {"subheader": "bar"},
                "attributes": {"subheader": "baz"},
            },
            "body": {
                "classes": ["body_foo"],
                "styles": {"body": "bar"},
                "attributes": {"body": "baz"},
            }
        }
    }
    rendered_doc = ge.render.view.view.DefaultJinjaComponentView.render({
        "content_block": header_component_content,
        "section_loop": {"index": 1},
        "content_block_loop": {"index": 2},
    })
    print(rendered_doc)
    assert rendered_doc == \
        """
<div id="section-1-content-block-2" class="root_foo" root="baz" style="root:bar;" >
    <h4 id="section-1-content-block-2-header" class="header_foo" header="baz" style="header:bar;" >
        <span class="y" >AAA</span> <span class="x" >BBB</span> <span class="x" >CCC</span>
    </h4>
    <h5 id="section-1-content-block-2-subheader" class="subheader_foo" subheader="baz" style="subheader:bar;" >
        <span class="yy" >aaa</span> <span class="xx" >bbb</span> <span class="xx" >ccc</span>
    </h5>
    <table id="section-1-content-block-2-body" class="body_foo" body="baz" style="body:bar;" >
        <tr>
            <td id="section-1-content-block-2-cell-1-1" >Mean</td><td id="section-1-content-block-2-cell-1-2" >446</td></tr><tr>
            <td id="section-1-content-block-2-cell-2-1" >Minimum</td><td id="section-1-content-block-2-cell-2-2" >1</td></tr></table>
</div>"""


### Test all the component types ###


def test_render_header_component():
    header_component_content = {
        # "component_type": "header",
        "content_block_type": "header",
        "header": "Overview",
    }
    rendered_doc = ge.render.view.view.DefaultJinjaComponentView.render({
        "content_block": header_component_content,
        "section_loop": {"index": 1},
        "content_block_loop": {"index": 2},
    })
    print(rendered_doc)
    assert rendered_doc == \
        """
<div id="section-1-content-block-2" >
    <h3 id="section-1-content-block-2-header" >
        Overview
    </h3>
</div>"""


def test_render_table_component():
    table_component_content = {
        # "component_type": "header",
        "content_block_type": "table",
        "header": "Overview",
        "table_rows": [
            ["Mean", "446"],
            ["Minimum", "1"],
        ],
        "styling": {
            "classes": ["col-4"],
        }
    }
    rendered_doc = ge.render.view.view.DefaultJinjaComponentView.render({
        "content_block": table_component_content,
        "section_loop": {"index": 1},
        "content_block_loop": {"index": 2},
    })
    print(rendered_doc)
    assert rendered_doc == \
        """
<div id="section-1-content-block-2" class="col-4" >
    <h4 id="section-1-content-block-2-header" >
        Overview
    </h4>
    <table id="section-1-content-block-2-body" >
        <tr>
            <td id="section-1-content-block-2-cell-1-1" >Mean</td><td id="section-1-content-block-2-cell-1-2" >446</td></tr><tr>
            <td id="section-1-content-block-2-cell-2-1" >Minimum</td><td id="section-1-content-block-2-cell-2-2" >1</td></tr></table>
</div>"""

# TODO: Add tests for the remaining component types
# * text
# * value_list
# * bullet_list
# * graph
# * example_list
