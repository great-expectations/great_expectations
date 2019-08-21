import logging
import sys

import great_expectations as ge
import great_expectations.jupyter_ux as jux
from great_expectations.profile.basic_dataset_profiler import BasicDatasetProfiler


def test_styling_elements_exist():
    assert "<link" in jux.bootstrap_link_element
    assert "bootstrap" in jux.bootstrap_link_element

    assert jux.cooltip_style_element[:23] == '<style type="text/css">'
    assert ".cooltip" in jux.cooltip_style_element


def test_display_column_expectations_as_section(basic_expectation_suite):
    html_to_display = jux.display_column_expectations_as_section(
        basic_expectation_suite,
        "naturals",
        include_styling=False,
        return_without_displaying=True
    )
    print(html_to_display)
    html_to_display = html_to_display.replace(" ", "").replace("\t", "").replace("\n", "")
    assert html_to_display == """\
<div id="section-1" class="ge-section container-fluid">
    <div class="row">
    
<div id="content-block-1" class="col-12" >
    <div id="content-block-1-header" class="alert alert-secondary" >
        <h3>
            naturals
        </h3></div>
</div>

<div id="content-block-2" class="col-12" >
    <ul id="content-block-2-body" >
            <li >
            <span>
                is a required field.
            </span>
        </li>
            <li >
            <span>
                values must be unique.
            </span>
        </li>
        
        </ul>
</div>
    
    </div>
</div>""".replace(" ", "").replace("\t", "").replace("\n", "")

    html_to_display = jux.display_column_expectations_as_section(
        basic_expectation_suite,
        "naturals",
        return_without_displaying=True
    )
    print(html_to_display)
    html_to_display = html_to_display.replace(" ", "").replace("\t", "").replace("\n", "")
    assert html_to_display == """\
<link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.3.1/css/bootstrap.min.css" integrity="sha384-ggOyR0iXCbMQv3Xipma34MD+dH/1fQ784/j6cY/iJTQUOhcWr7x9JvoRxT2MZw1T" crossorigin="anonymous"><style type="text/css">
.cooltip {
    display:inline-block;
    position:relative;
    text-align:left;
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
    z-index:99999999;
    box-sizing:border-box;
    box-shadow:0 1px 8px rgba(0,0,0,0.5);
    display:none;
}

.cooltip:hover .top {
    display:block;
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
</style>
<div id="section-1" class="ge-section container-fluid">
    <div class="row">
    
<div id="content-block-1" class="col-12" >
    <div id="content-block-1-header" class="alert alert-secondary" >
        <h3>
            naturals
        </h3></div>
</div>

<div id="content-block-2" class="col-12" >
    <ul id="content-block-2-body" >
            <li >
            <span>
                is a required field.
            </span>
        </li>
            <li >
            <span>
                values must be unique.
            </span>
        </li>
        
        </ul>
</div>
    
    </div>
</div>
""".replace(" ", "").replace("\t", "").replace("\n", "")


def test_display_column_evrs_as_section():
    #TODO: We should add a fixture that contains EVRs
    df = ge.read_csv("./tests/test_sets/Titanic.csv")
    df.profile(BasicDatasetProfiler)
    evrs = df.validate(result_format="SUMMARY")  # ["results"]

    html_to_display = jux.display_column_evrs_as_section(
        evrs,
        "Name",
        include_styling=False,
        return_without_displaying=True
    )
    print(html_to_display)

    #FIXME: This isn't a full snapshot test.
    assert '<div id="section-1" class="ge-section container-fluid">' in html_to_display
    assert '<span class="badge badge-info" style="word-break:break-all;" >Carlsson, Mr Frans Olof</span>' in html_to_display
    assert """\
    <span class="cooltip" >
                    Type: None
                    <span class=top>
                        expect_column_values_to_be_of_type <br>expect_column_values_to_be_in_type_list
                    </span>
                </span>""" in html_to_display


def test_configure_logging(caplog):
    # First, ensure we set the root logger to close-to-jupyter settings (only show warnings)
    caplog.set_level(logging.WARNING)
    caplog.set_level(logging.WARNING, logger="great_expectations")

    root = logging.getLogger()  # root logger
    root.info("do_not_show")

    # This df is used only for logging; we don't want to test against different backends
    df = ge.dataset.PandasDataset({"a": [1, 2, 3]})
    df.expect_column_to_exist("a")
    df.get_expectation_suite()

    res = caplog.text
    assert "do_not_show" not in res

    assert "expectation_suite" not in res
    caplog.clear()

    # Now use the logging setup from the notebook
    logger = logging.getLogger("great_expectations")
    jux.setup_notebook_logging(logger)
    df = ge.dataset.PandasDataset({"a": [1, 2, 3]})
    df.expect_column_to_exist("a")
    df.get_expectation_suite()

    root.info("do_not_show")
    res = caplog.text
    assert "do_not_show" not in res

    assert "expectation_suite" in res
