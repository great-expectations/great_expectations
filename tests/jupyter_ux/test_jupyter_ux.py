import logging

import pytest

import great_expectations as ge
import great_expectations.jupyter_ux as jux


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
        return_without_displaying=True,
    )
    print(html_to_display)
    html_to_display = (
        html_to_display.replace(" ", "").replace("\t", "").replace("\n", "")
    )
    assert (
        html_to_display
        == """\
<div id="section-1" class="ge-section container-fluid mb-1 pb-1 pl-sm-3 px-0">
    <div class="row" >
<div id="content-block-1" class="col-12" >
    <div id="content-block-1-header" class="alert alert-secondary" >
        <div>
                <h5 class="m-0" >
                    naturals
                </h5>
        </div>
    </div>
</div>
<div id="content-block-2" class="col-12" >
<ul id="content-block-2-body" >
        <li >
                <span >
                    is a required field.
                </span>
            </li>
        <li style="list-style-type:none;" >
                <hr class="mt-1 mb-1" >

                </hr>
            </li>
        <li >
                <span >
                    values must be unique.
                </span>
            </li>
        <li style="list-style-type:none;" >
                <hr class="mt-1 mb-1" >
                </hr>
            </li>
</ul>
</div>
    </div>
</div>
""".replace(
            " ", ""
        )
        .replace("\t", "")
        .replace("\n", "")
    )

    html_to_display = jux.display_column_expectations_as_section(
        basic_expectation_suite, "naturals", return_without_displaying=True
    )
    print(html_to_display)
    html_to_display = (
        html_to_display.replace(" ", "").replace("\t", "").replace("\n", "")
    )
    assert (
        html_to_display
        == """\
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
<div id="section-1" class="ge-section container-fluid mb-1 pb-1 pl-sm-3 px-0">
    <div class="row" >
<div id="content-block-1" class="col-12" >
    <div id="content-block-1-header" class="alert alert-secondary" >
        <div>
                <h5 class="m-0" >
                    naturals
                </h5>
        </div>
    </div>
</div>
<div id="content-block-2" class="col-12" >
<ul id="content-block-2-body" >
        <li >
                <span >
                    is a required field.
                </span>
            </li>
        <li style="list-style-type:none;" >
                <hr class="mt-1 mb-1" >
                </hr>
            </li>
        <li >
                <span >
                    values must be unique.
                </span>
            </li>
        <li style="list-style-type:none;" >
                <hr class="mt-1 mb-1" >
                </hr>
            </li>
</ul>
</div>
    </div>
</div>
""".replace(
            " ", ""
        )
        .replace("\t", "")
        .replace("\n", "")
    )


@pytest.mark.smoketest
def test_display_profiled_column_evrs_as_section(titanic_profiled_evrs_1):
    section_html = jux.display_profiled_column_evrs_as_section(
        titanic_profiled_evrs_1,
        "SexCode",
        include_styling=False,
        return_without_displaying=True,
    )


@pytest.mark.smoketest
def test_display_column_evrs_as_section(titanic_profiled_evrs_1):
    section_html = jux.display_column_evrs_as_section(
        titanic_profiled_evrs_1,
        "SexCode",
        include_styling=False,
        return_without_displaying=True,
    )


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
