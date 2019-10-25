import pytest

import json
from collections import OrderedDict

import great_expectations as ge
import great_expectations.render as render
from great_expectations.render.renderer import (
    ProfilingResultsPageRenderer,
)
from great_expectations.render.view import DefaultJinjaPageView
from great_expectations.render.types import (
    RenderedComponentContent,
    RenderedSectionContent,
    RenderedComponentContentWrapper,
    RenderedDocumentContent
)
from ..test_utils import dict_to_ordered_dict

@pytest.fixture()
def validation_results():
    with open("./tests/test_sets/expected_cli_results_default.json", "r") as infile:
        return json.load(infile, object_pairs_hook=OrderedDict)


@pytest.fixture()
def expectations():
    with open("./tests/test_sets/titanic_expectations.json", "r") as infile:
        return json.load(infile, object_pairs_hook=OrderedDict)


# noinspection PyPep8Naming
@pytest.mark.smoketest
def test_render_DefaultJinjaPageView_meta_info():
    validation_results = {
        "results": [],
        "statistics": {
            "evaluated_expectations": 156,
            "successful_expectations": 139,
            "unsuccessful_expectations": 17,
            "success_percent": 89.1025641025641
        },
        "meta": {
            "great_expectations.__version__": "0.7.0-beta",
            "data_asset_name": "tetanusvaricella",
            "expectation_suite_name": "my_suite",
            "run_id": "2019-06-25T14:58:09.960521",
            "batch_kwargs": {
                "path": "/Users/user/project_data/public_healthcare_datasets/tetanusvaricella/tetvardata.csv",
                "timestamp": 1561474688.693565
            }
        }
    }

    document = RenderedDocumentContent(dict_to_ordered_dict(ProfilingResultsPageRenderer().render(validation_results)))
    html = DefaultJinjaPageView().render(document)
    print(html)

    expected_html = """
<!DOCTYPE html>
<html>
  <head>
    <title>Data documentation compiled by Great Expectations</title>
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <meta charset="UTF-8">
    <title></title>

    
    
    <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/4.3.1/css/bootstrap.min.css"/>

    <style>  body {
    position: relative;
  }
  .container {
    padding-top: 50px;
  }
  .sticky {
    position: -webkit-sticky;
    position: sticky;
    max-height: 90vh;
    overflow-y: auto;
    top: 15px;
  }

  .ge-section {
    clear:both;
    margin-bottom: 30px;
    padding-bottom: 20px;
  }

  .triangle {
    border: solid #222;
    border-width: 0 1px 1px 0;
    display: inline;
    cursor: pointer;
    padding: 3px;
    position: absolute;
    right: 0;
    margin-top: 10px;

    transform: rotate(40deg);
    -webkit-transform: rotate(40deg);
    transition: .3s transform ease-in-out;
  }

  .collapsed .triangle{
    transform: rotate(-140deg);
    -webkit-transform: rotate(-140deg);
    transition: .3s transform ease-in-out;
  }

  .popover {
    max-width: 100%;
  }

  .cooltip {
    display:inline-block;
    position:relative;
    text-align:left;
    cursor:pointer;
  }

  .cooltip .top {
    min-width:200px;
    top:-6px;
    left:50%;
    transform:translate(-50%, -100%);
    padding:10px 20px;
    color:#FFFFFF;
    background-color:#222222;
    font-weight:normal;
    font-size:13px;
    border-radius:8px;
    position:absolute;
    z-index:99999999 !important;
    box-sizing:border-box;
    box-shadow:0 1px 8px rgba(0,0,0,0.5);
    display:none;
    }

  .cooltip:hover .top {
      display:block;
      z-index:99999999 !important;
  }

  .cooltip .top i {
      position:absolute;
      top:100%;
      left:50%;
      margin-left:-12px;
      width:24px;
      height:12px;
      overflow:hidden;
  }

  .cooltip .top i::after {
      content:'';
      position:absolute;
      width:12px;
      height:12px;
      left:50%;
      transform:translate(-50%,-50%) rotate(45deg);
      background-color:#222222;
      box-shadow:0 1px 8px rgba(0,0,0,0.5);
  }

  ul {
    padding-inline-start: 20px;
  }

  .table-cell-frame {
      overflow-y: auto;
      max-height: 335px;
  }

  .table-cell-frame ul {
    padding-bottom: 20px
  }

  .table-cell-frame::-webkit-scrollbar {
      -webkit-appearance: none;
  }

  .table-cell-frame::-webkit-scrollbar:vertical {
      width: 11px;
  }

  .table-cell-frame::-webkit-scrollbar:horizontal {
      height: 11px;
  }

  .table-cell-frame::-webkit-scrollbar-thumb {
      border-radius: 8px;
      border: 2px solid white; /* should match background, can't be transparent */
      background-color: rgba(0, 0, 0, .5);
  }</style>
    <style></style>

    <script src="https://cdn.jsdelivr.net/npm/vega@5.3.5/build/vega.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/vega-lite@3.2.1/build/vega-lite.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/vega-embed@4.0.0/build/vega-embed.js"></script>
    <script src="https://kit.fontawesome.com/8217dffd95.js"></script>
  </head>

  <body
        data-spy="scroll"
        data-target="#navigation"
        data-offset="50"
    >
    
<nav aria-label="breadcrumb">
    <ol class="ge-breadcrumbs breadcrumb">
        
          <li class="ge-breadcrumbs-item breadcrumb-item"><a href="../../../../../index.html">Home</a></li>
        
        
            <li class="ge-breadcrumbs-item breadcrumb-item">tetanusvaricella</li>
        
        <li class="ge-breadcrumbs-item breadcrumb-item active" aria-current="page">2019-06-25T14:58:09.960521-my_suite-ProfilingResults</li>
    </ol>
</nav>


    <div class="container-fluid px-3">
      <div class="row">
        <div class="col-2 navbar-collapse">
          <nav id="navigation" class="navbar navbar-light bg-light sticky d-none d-sm-block ge-navigation-sidebar-container">
    <nav class="nav nav-pills flex-column ge-navigation-sidebar-content">
        <a class="navbar-brand ge-navigation-sidebar-title" href="#" style="white-space: normal; word-break: break-all;overflow-wrap: normal; font-size: 1rem; font-weight: 500;">tetanusvaricella</a>
        
            <a class="nav-link ge-navigation-sidebar-link" href="#section-1">Overview</a>
        
    </nav>
</nav>
        </div>
        <div class="col-md-8 col-lg-8 col-xs-12">
        
          <div id="section-1" class="ge-section container-fluid">
    <div class="row">
    

<div id="section-1-content-block-1" class="col-12" >

    <div id="section-1-content-block-1-header" class="alert alert-secondary" >
    <h3>
        Overview
    </h3></div>

</div>


<div id="section-1-content-block-2" class="col-6" style="margin-top:20px;" >

    <div id="section-1-content-block-2-header" >
        
        <h4>
            Dataset info
        </h4>
        </div>


<table id="section-1-content-block-2-body" class="table table-sm" >
    

    <tr>
        <td id="section-1-content-block-2-cell-1-1" ><div class="table-cell-frame"><span>Number of variables</span></div></td><td id="section-1-content-block-2-cell-1-2" ><div class="table-cell-frame">0</div></td></tr><tr>
        <td id="section-1-content-block-2-cell-2-1" ><div class="table-cell-frame">
                <span class="cooltip" >
                    Number of observations
                    <span class=top>
                        expect_table_row_count_to_be_between
                    </span>
                </span>
            </div></td><td id="section-1-content-block-2-cell-2-2" ><div class="table-cell-frame"><span>--</span></div></td></tr><tr>
        <td id="section-1-content-block-2-cell-3-1" ><div class="table-cell-frame"><span>Missing cells</span></div></td><td id="section-1-content-block-2-cell-3-2" ><div class="table-cell-frame"><span>?</span></div></td></tr></table>

</div>


<div id="section-1-content-block-3" class="col-6 table-responsive" style="margin-top:20px;" >

    <div id="section-1-content-block-3-header" >
        
        <h4>
            Variable types
        </h4>
        </div>


<table id="section-1-content-block-3-body" class="table table-sm" >
    

    <tr>
        <td id="section-1-content-block-3-cell-1-1" ><div class="table-cell-frame"><span>int</span></div></td><td id="section-1-content-block-3-cell-1-2" ><div class="table-cell-frame"><span>0</span></div></td></tr><tr>
        <td id="section-1-content-block-3-cell-2-1" ><div class="table-cell-frame"><span>float</span></div></td><td id="section-1-content-block-3-cell-2-2" ><div class="table-cell-frame"><span>0</span></div></td></tr><tr>
        <td id="section-1-content-block-3-cell-3-1" ><div class="table-cell-frame"><span>string</span></div></td><td id="section-1-content-block-3-cell-3-2" ><div class="table-cell-frame"><span>0</span></div></td></tr><tr>
        <td id="section-1-content-block-3-cell-4-1" ><div class="table-cell-frame"><span>unknown</span></div></td><td id="section-1-content-block-3-cell-4-2" ><div class="table-cell-frame"><span>0</span></div></td></tr></table>

</div>"""

    expected_html_2 = """
</div>
</div>

        </div>
        <div class="col-lg-2 col-md-2 col-xs-12 d-none d-md-block">
            <div>
  <div class="col-12">
    <div class="row">
      <img
        src="https://great-expectations.readthedocs.io/en/latest/_images/generic_dickens_protagonist.png"
        style="
          width: 200px;
          margin-left: auto;
          margin-right: auto;
        ">
    </div>
  </div>
  <div class="col-12">
    <p>
      Documentation autogenerated using
      <a href="https://greatexpectations.io">Great Expectations</a>.
    </p>
  </div>
  <div class="alert alert-danger col-12" role="alert">
      This is a beta feature! Expect changes in API, behavior, and design.
  </div>
</div>
        </div>
        <div class="d-block d-sm-none col-12">
            <div>
              <div class="row">
                <img
                  src="https://great-expectations.readthedocs.io/en/latest/_images/generic_dickens_protagonist.png"
                  style="
                    width: 200px;
                    margin-left: auto;
                    margin-right: auto;
                  ">
              </div>
            </div>
            <div class="col-12">
              <p>
                Documentation autogenerated using
                <a href="https://greatexpectations.io">Great Expectations</a>.
              </p>
            </div>
            <div class="alert alert-danger col-12" role="alert">
                This is a beta feature! Expect changes in API, behavior, and design.
            </div>
          </div>

      </div>
    </div>
    <script src="https://code.jquery.com/jquery-3.2.1.slim.min.js"></script>
    <script src="https://maxcdn.bootstrapcdn.com/bootstrap/4.3.1/js/bootstrap.min.js"></script>
    <script>
        $(window).on('activate.bs.scrollspy', function () {
          document.querySelector(".nav-link.active").scrollIntoViewIfNeeded();
        });
    </script>
  </body>
</html>
"""
    
    assert expected_html.replace(" ", "").replace("\t", "").replace("\n", "") in html.replace(" ", "").replace("\t", "").replace("\n", "")
    assert expected_html_2.replace(" ", "").replace("\t", "").replace("\n", "") in html.replace(" ", "").replace("\t", "").replace("\n", "")


def test_render_section_page():
    section = RenderedSectionContent(**{
        "section_name": None,
        "content_blocks": [
            RenderedComponentContent(**{
                "content_block_type": "header",
                "header": "Overview",
            }),
            RenderedComponentContent(**{
                "content_block_type": "table",
                "header": "Dataset info",
                "table": [
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
            })
        ]
    })

    rendered_doc = ge.render.view.view.DefaultJinjaSectionView().render(
        RenderedComponentContentWrapper(**{
            "section": section,
            "section_loop": {"index": 1},
        })
    )#.replace(" ", "").replace("\t", "").replace("\n", "")

    print(rendered_doc)
    
    rendered_doc = rendered_doc.replace(" ", "").replace("\t", "").replace("\n", "")
    assert rendered_doc == """<div id="section-1" class="ge-section container-fluid">
    <div class="row">
        

<div id="content-block-1" >

    <div id="content-block-1-header" >
    <h3>
        Overview
    </h3></div>

</div>


<div id="content-block-2" class="col-6 table-responsive" style="margin-top:20px;" >

    <div id="content-block-2-header" >
        
        <h4>
            Dataset info
        </h4>
        </div>


<table id="content-block-2-body" class="table table-sm" >
    

    <tr>
        <td id="content-block-2-cell-1-1" ><div class="table-cell-frame"><span>Number of variables</span></div></td><td id="content-block-2-cell-1-2" ><div class="table-cell-frame"><span>12</span></div></td></tr><tr>
        <td id="content-block-2-cell-2-1" ><div class="table-cell-frame"><span>Number of observations</span></div></td><td id="content-block-2-cell-2-2" ><div class="table-cell-frame"><span>891</span></div></td></tr></table>

</div>
        
    </div>
</div>""".replace(" ", "").replace("\t", "").replace("\n", "")


def test_rendering_components_without_section_loop_index():
    header_component_content = RenderedComponentContent(**{
        # "component_type": "header",
        "content_block_type": "header",
        "header": "Overview",
    })
    rendered_doc = ge.render.view.view.DefaultJinjaComponentView().render(
        RenderedComponentContentWrapper(**{
            "content_block": header_component_content,
            "content_block_loop": {"index": 2},
        })
    )
    print(rendered_doc)
    rendered_doc = rendered_doc.replace(" ", "").replace("\t", "").replace("\n", "")
    assert rendered_doc == \
        """
<div id="content-block-2" >

    <div id="content-block-2-header" >
    <h3>
        Overview
    </h3></div>

</div>""".replace(" ", "").replace("\t", "").replace("\n", "")

    rendered_doc = ge.render.view.view.DefaultJinjaComponentView().render(
        RenderedComponentContentWrapper(**{
            "content_block": header_component_content,
        })
    )
    print(rendered_doc)
    rendered_doc = rendered_doc.replace(" ", "").replace("\t", "").replace("\n", "")
    assert rendered_doc == \
        """
<div id="content-block" >
    <div id="content-block-header" >
        <h3>
            Overview
        </h3></div>
</div>""".replace(" ", "").replace("\t", "").replace("\n", "")

    rendered_doc = ge.render.view.view.DefaultJinjaComponentView().render(
        RenderedComponentContentWrapper(**{
            "content_block": header_component_content,
            "section_loop": {"index": 3},
        })
    )
    print(rendered_doc)
    rendered_doc = rendered_doc.replace(" ", "").replace("\t", "").replace("\n", "")
    assert rendered_doc == \
        """
<div id="content-block" >
    <div id="content-block-header" >
        <h3>
            Overview
        </h3></div>
</div>""".replace(" ", "").replace("\t", "").replace("\n", "")


def test_rendering_components_with_styling():
    # Medium-complicated example to verify that all the things are correctly piped to all the places

    header_component_content = RenderedComponentContent(**{
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
        "table": [
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
    })
    rendered_doc = ge.render.view.view.DefaultJinjaComponentView().render(
        RenderedComponentContentWrapper(**{
            "content_block": header_component_content,
            "section_loop": {"index": 1},
            "content_block_loop": {"index": 2},
        })
    )
    print(rendered_doc)
    rendered_doc = rendered_doc.replace(" ", "").replace("\t", "").replace("\n", "")
    
    assert rendered_doc == \
        """
<div id="section-1-content-block-2" class="root_foo" root="baz" style="root:bar;" >

    <div id="section-1-content-block-2-header" class="header_foo" header="baz" style="header:bar;" >
        
        <h4>
            
                <span >
                    <span class="y" >AAA</span> <span class="x" >BBB</span> <span class="x" >CCC</span>
                </span>
            
        </h4>
        <h5 id="section-1-content-block-2-subheader" class="subheader_foo" subheader="baz" style="subheader:bar;" >
            
                <span >
                    <span class="yy" >aaa</span> <span class="xx" >bbb</span> <span class="xx" >ccc</span>
                </span>
            
        </h5>
        </div>


<table id="section-1-content-block-2-body" class="body_foo" body="baz" style="body:bar;" >
    

    <tr>
        <td id="section-1-content-block-2-cell-1-1" ><div class="table-cell-frame"><span>Mean</span></div></td><td id="section-1-content-block-2-cell-1-2" ><div class="table-cell-frame"><span>446</span></div></td></tr><tr>
        <td id="section-1-content-block-2-cell-2-1" ><div class="table-cell-frame"><span>Minimum</span></div></td><td id="section-1-content-block-2-cell-2-2" ><div class="table-cell-frame"><span>1</span></div></td></tr></table>

</div>""".replace(" ", "").replace("\t", "").replace("\n", "")


### Test all the component types ###


def test_render_header_component():
    header_component_content = RenderedComponentContent(**{
        # "component_type": "header",
        "content_block_type": "header",
        "header": "Overview",
    })
    rendered_doc = ge.render.view.view.DefaultJinjaComponentView().render(
        RenderedComponentContentWrapper(**{
            "content_block": header_component_content,
            "section_loop": {"index": 1},
            "content_block_loop": {"index": 2},
        })
    )
    print(rendered_doc)
    rendered_doc = rendered_doc.replace(" ", "").replace("\t", "").replace("\n", "")
    assert rendered_doc == \
        """
<div id="section-1-content-block-2" >

    <div id="section-1-content-block-2-header" >
    <h3>
        Overview
    </h3></div>

</div>""".replace(" ", "").replace("\t", "").replace("\n", "")


def test_render_table_component():
    table_component_content = RenderedComponentContent(**{
        # "component_type": "header",
        "content_block_type": "table",
        "header": "Overview",
        "table": [
            ["Mean", "446"],
            ["Minimum", "1"],
        ],
        "styling": {
            "classes": ["col-4"],
        }
    })
    rendered_doc = ge.render.view.view.DefaultJinjaComponentView().render(
        RenderedComponentContentWrapper(**{
            "content_block": table_component_content,
            "section_loop": {"index": 1},
            "content_block_loop": {"index": 2},
        })
    )
    print(rendered_doc)
    rendered_doc = rendered_doc.replace(" ", "").replace("\t", "").replace("\n", "")
    assert rendered_doc == \
        """
<div id="section-1-content-block-2" class="col-4" >

    <div id="section-1-content-block-2-header" >
        
        <h4>
            Overview
        </h4>
        </div>


<table id="section-1-content-block-2-body" >
    

    <tr>
        <td id="section-1-content-block-2-cell-1-1" ><div class="table-cell-frame"><span>Mean</span></div></td><td id="section-1-content-block-2-cell-1-2" ><div class="table-cell-frame"><span>446</span></div></td></tr><tr>
        <td id="section-1-content-block-2-cell-2-1" ><div class="table-cell-frame"><span>Minimum</span></div></td><td id="section-1-content-block-2-cell-2-2" ><div class="table-cell-frame"><span>1</span></div></td></tr></table>

</div>""".replace(" ", "").replace("\t", "").replace("\n", "")


def test_render_value_list():
    value_list_component_content = RenderedComponentContent(**{
        'content_block_type': 'value_list',
        'header': 'Example values',
        'value_list': [{
            'content_block_type': 'string_template',
            'string_template': {
                'template': '$value',
                'params': {'value': '0'},
                'styling': {'default': {'classes': ['badge', 'badge-info']}}
            }
        }, {
            'content_block_type': 'string_template',
            'string_template': {
                'template': '$value',
                'params': {'value': '1'},
                'styling': {'default': {'classes': ['badge', 'badge-info']}}
            }
        }],
        'styling': {
            'classes': ['col-4'],
            'styles': {'margin-top': '20px'}
        }
    })

    rendered_doc = ge.render.view.view.DefaultJinjaComponentView().render(
        RenderedComponentContentWrapper(**{
            "content_block": value_list_component_content,
            "section_loop": {"index": 1},
            "content_block_loop": {"index": 2},
        })
    )
    print(rendered_doc)
    rendered_doc = rendered_doc.replace(" ", "").replace("\t", "").replace("\n", "")
    assert rendered_doc == """
<div id="section-1-content-block-2" class="col-4" style="margin-top:20px;" >

    <div id="section-1-content-block-2-header" >
        <h4>
            Example values
        </h4></div>


<p id="section-1-content-block-2-body" >
    
                <span >
                    <span class="badge badge-info" >0</span>
                </span>
            
    
                <span >
                    <span class="badge badge-info" >1</span>
                </span>
            
    </p>

</div>""".replace(" ", "").replace("\t", "").replace("\n", "")


def test_render_graph():
    graph_component_content = RenderedComponentContent(**{
        "content_block_type": "graph",
        "header": "Histogram",
        "graph": "{\"$schema\": \"https://vega.github.io/schema/vega-lite/v2.6.0.json\", \"autosize\": \"fit\", \"config\": {\"view\": {\"height\": 300, \"width\": 400}}, \"data\": {\"name\": \"data-a681d02fb484e64eadd9721b37015d5b\"}, \"datasets\": {\"data-a681d02fb484e64eadd9721b37015d5b\": [{\"bins\": 3.7, \"weights\": 5.555555555555555}, {\"bins\": 10.8, \"weights\": 3.439153439153439}, {\"bins\": 17.9, \"weights\": 17.857142857142858}, {\"bins\": 25.0, \"weights\": 24.206349206349206}, {\"bins\": 32.0, \"weights\": 16.137566137566136}, {\"bins\": 39.1, \"weights\": 12.3015873015873}, {\"bins\": 46.2, \"weights\": 9.788359788359788}, {\"bins\": 53.3, \"weights\": 5.423280423280423}, {\"bins\": 60.4, \"weights\": 3.439153439153439}, {\"bins\": 67.5, \"weights\": 1.8518518518518516}]}, \"encoding\": {\"x\": {\"field\": \"bins\", \"type\": \"ordinal\"}, \"y\": {\"field\": \"weights\", \"type\": \"quantitative\"}}, \"height\": 200, \"mark\": \"bar\", \"width\": 200}",
        "styling": {
            "classes": ["col-4"]
        }
    })

    rendered_doc = ge.render.view.view.DefaultJinjaComponentView().render(
        RenderedComponentContentWrapper(**{
            "content_block": graph_component_content,
            "section_loop": {"index": 1},
            "content_block_loop": {"index": 2},
        })
    )
    print(rendered_doc)
    rendered_doc = rendered_doc.replace(" ", "").replace("\t", "").replace("\n", "")
    assert rendered_doc == """
<div id="section-1-content-block-2" class="col-4" >

    <div id="section-1-content-block-2-header" >
        
        <h4>
            Histogram
        </h4>
        </div>
<div id="section-1-content-block-2-graph" ></div>
<script>
    // Assign the specification to a local variable vlSpec.
    vlSpec = {"$schema": "https://vega.github.io/schema/vega-lite/v2.6.0.json", "autosize": "fit", "config": {"view": {"height": 300, "width": 400}}, "data": {"name": "data-a681d02fb484e64eadd9721b37015d5b"}, "datasets": {"data-a681d02fb484e64eadd9721b37015d5b": [{"bins": 3.7, "weights": 5.555555555555555}, {"bins": 10.8, "weights": 3.439153439153439}, {"bins": 17.9, "weights": 17.857142857142858}, {"bins": 25.0, "weights": 24.206349206349206}, {"bins": 32.0, "weights": 16.137566137566136}, {"bins": 39.1, "weights": 12.3015873015873}, {"bins": 46.2, "weights": 9.788359788359788}, {"bins": 53.3, "weights": 5.423280423280423}, {"bins": 60.4, "weights": 3.439153439153439}, {"bins": 67.5, "weights": 1.8518518518518516}]}, "encoding": {"x": {"field": "bins", "type": "ordinal"}, "y": {"field": "weights", "type": "quantitative"}}, "height": 200, "mark": "bar", "width": 200};
    // Embed the visualization in the container with id `vis`
    vegaEmbed('#section-1-content-block-2-graph', vlSpec, {
        actions: false
    }).then(result=>console.log(result)).catch(console.warn);
</script>

</div>""".replace(" ", "").replace("\t", "").replace("\n", "")


def test_render_text():
    text_component_content = RenderedComponentContent(**{
        "content_block_type": "text",
        "header": "Histogram",
        "content": ["hello"],
        "styling": {
            "classes": ["col-4"]
        }
    })

    rendered_doc = ge.render.view.view.DefaultJinjaComponentView().render(
        RenderedComponentContentWrapper(**{
            "content_block": text_component_content,
            "section_loop": {"index": 1},
            "content_block_loop": {"index": 2},
        })
    )
    print(rendered_doc)
    rendered_doc = rendered_doc.replace(" ", "").replace("\t", "").replace("\n", "")
    assert rendered_doc == """
<div id="section-1-content-block-2" class="col-4" >

    <div id="section-1-content-block-2-header" >
        <h4>
            Histogram
        </h4></div>


<div id="section-1-content-block-2-body" >
    <p>hello</p>
    </div>

</div>""".replace(" ", "").replace("\t", "").replace("\n", "")

    text_component_content = RenderedComponentContent(**{
        "content_block_type": "text",
        "header": "Histogram",
        "content": ["hello", "goodbye"],
        "styling": {
            "classes": ["col-4"]
        }
    })

    rendered_doc = ge.render.view.view.DefaultJinjaComponentView().render(
        RenderedComponentContentWrapper(**{
            "content_block": text_component_content,
            "section_loop": {"index": 1},
            "content_block_loop": {"index": 2},
        })
    )
    print(rendered_doc)
    rendered_doc = rendered_doc.replace(" ", "").replace("\t", "").replace("\n", "")
    assert rendered_doc == """
<div id="section-1-content-block-2" class="col-4" >

    <div id="section-1-content-block-2-header" >
        <h4>
            Histogram
        </h4></div>


<div id="section-1-content-block-2-body" >
    <p>hello</p>
    <p>goodbye</p>
    </div>

</div>""".replace(" ", "").replace("\t", "").replace("\n", "")


# TODO: Add tests for the remaining component types
# * value_list
# * bullet_list
# * graph
# * example_list
