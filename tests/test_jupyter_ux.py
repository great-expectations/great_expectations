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
    assert html_to_display == """\
<div id="section-1" class="ge-section container-fluid">
    <div class="row">
        
<div id="content-block-1" class="col-12" >
    <h3 id="content-block-1-header" class="alert alert-secondary" >
        naturals
    </h3>
</div>
        
<div id="content-block-2" class="col-12" >
    <ul id="content-block-2-body" >
            <li >is a required field.</li>
            <li >values must be unique.</li>
            
        </ul>
</div>
        
    </div>
</div>"""

    html_to_display = jux.display_column_expectations_as_section(
        basic_expectation_suite,
        "naturals",
        return_without_displaying=True
    )
    print(html_to_display)
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
    <h3 id="content-block-1-header" class="alert alert-secondary" >
        naturals
    </h3>
</div>
        
<div id="content-block-2" class="col-12" >
    <ul id="content-block-2-body" >
            <li >is a required field.</li>
            <li >values must be unique.</li>
            
        </ul>
</div>
        
    </div>
</div>"""

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

    #FIXME: This snapshot test is brittle, since it depends on the behavior of the profiler, renderer, and view.
    # At the very least, we can take the profiler out of the loop by creating a new fixture for it.
    # Next time this snapshot test fails, please do that. 
    assert html_to_display == """\
<div id="section-1" class="ge-section container-fluid">
    <div class="row">
        
<div id="content-block-1" class="col-12" >
    <h3 id="content-block-1-header" class="alert alert-secondary" >
        Name
    </h3>
</div>
        
<div id="content-block-2" class="col-4" style="margin-top:20px;" >
    <h4 id="content-block-2-header" >
        Properties
    </h4>
    <table id="content-block-2-body" class="table table-sm table-unbordered" style="width:100%;" >
        <tr>
            <td id="content-block-2-cell-1-1" >Distinct (n)</td><td id="content-block-2-cell-1-2" >1310</td></tr><tr>
            <td id="content-block-2-cell-2-1" >Distinct (%)</td><td id="content-block-2-cell-2-2" >99.8%</td></tr><tr>
            <td id="content-block-2-cell-3-1" >Missing (n)</td><td id="content-block-2-cell-3-2" >0</td></tr><tr>
            <td id="content-block-2-cell-4-1" >Missing (%)</td><td id="content-block-2-cell-4-2" >0.0%</td></tr></table>
</div>
        
<div id="content-block-3" class="col-12" style="margin-top:20px;" >
    <h4 id="content-block-3-header" >
        Example values
    </h4>
    <p id="content-block-3-body" >
        <span class="badge badge-info" >Carlsson, Mr Frans Olof</span>
        <span class="badge badge-info" >Connolly, Miss Kate</span>
        <span class="badge badge-info" >Kelly, Mr James</span>
        <span class="badge badge-info" >Allen, Miss Elisabeth Walton</span>
        <span class="badge badge-info" >Allison, Master Hudson Trevor</span>
        <span class="badge badge-info" >Allison, Miss Helen Loraine</span>
        <span class="badge badge-info" >Allison, Mr Hudson Joshua Creighton</span>
        <span class="badge badge-info" >Allison, Mrs Hudson JC (Bessie Waldo Daniels)</span>
        <span class="badge badge-info" >Anderson, Mr Harry</span>
        <span class="badge badge-info" >Andrews, Miss Kornelia Theodosia</span>
        <span class="badge badge-info" >Andrews, Mr Thomas, jr</span>
        <span class="badge badge-info" >Appleton, Mrs Edward Dale (Charlotte Lamson)</span>
        <span class="badge badge-info" >Artagaveytia, Mr Ramon</span>
        <span class="badge badge-info" >Astor, Colonel John Jacob</span>
        <span class="badge badge-info" >Astor, Mrs John Jacob (Madeleine Talmadge Force)</span>
        <span class="badge badge-info" >Aubert, Mrs Leontine Pauline</span>
        <span class="badge badge-info" >Barkworth, Mr Algernon H</span>
        <span class="badge badge-info" >Baumann, Mr John D</span>
        <span class="badge badge-info" >Baxter, Mr Quigg Edmond</span>
        <span class="badge badge-info" >Baxter, Mrs James (Helene DeLaudeniere Chaput)</span>
        </p>
</div>
        
<div id="content-block-4" class="col-12" style="margin-top:20px;" >
    <h4 id="content-block-4-header" class="collapsed" data-toggle="collapse" href="#content-block-4-body" role="button" aria-expanded="true" aria-controls="collapseExample" style="cursor:pointer;" >
        Expectation types <span class="mr-3 triangle"></span>
    </h4>
    <ul id="content-block-4-body" class="list-group collapse" >
            <li class="list-group-item d-flex justify-content-between align-items-center" >expect_column_values_to_be_in_type_list <span class="badge badge-secondary badge-pill" >True</span></li>
            <li class="list-group-item d-flex justify-content-between align-items-center" >expect_column_unique_value_count_to_be_between <span class="badge badge-secondary badge-pill" >True</span></li>
            <li class="list-group-item d-flex justify-content-between align-items-center" >expect_column_proportion_of_unique_values_to_be_between <span class="badge badge-secondary badge-pill" >True</span></li>
            <li class="list-group-item d-flex justify-content-between align-items-center" >expect_column_values_to_not_be_null <span class="badge badge-secondary badge-pill" >True</span></li>
            <li class="list-group-item d-flex justify-content-between align-items-center" >expect_column_values_to_be_in_set <span class="badge badge-secondary badge-pill" >False</span></li>
            <li class="list-group-item d-flex justify-content-between align-items-center" >expect_column_values_to_not_match_regex <span class="badge badge-secondary badge-pill" >False</span></li>
            
        </ul>
</div>
        
    </div>
</div>"""