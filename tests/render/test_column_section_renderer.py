import pytest
import json

from collections import OrderedDict

from great_expectations.render.renderer import (
    PrescriptiveColumnSectionRenderer,
    DescriptiveColumnSectionRenderer,
)
from great_expectations.render.view import (
    DefaultJinjaPageView,
)
from great_expectations.render.types import (
    RenderedComponentContent,
)


@pytest.fixture(scope="module")
def titanic_expectations():
    with open("./tests/test_sets/titanic_expectations.json", "r") as infile:
        return json.load(infile, object_pairs_hook=OrderedDict)


@pytest.mark.smoketest
def test_render_descriptive_column_section_renderer(titanic_validation_results):
    # Group EVRs by column
    evrs = {}
    for evr in titanic_validation_results["results"]:
        try:
            column = evr["expectation_config"]["kwargs"]["column"]
            if column not in evrs:
                evrs[column] = []
            evrs[column].append(evr)
        except KeyError:
            pass

    for column in evrs.keys():
        with open('./tests/render/output/test_render_descriptive_column_section_renderer__' + column + '.json', 'w') \
                as outfile:
            json.dump(DescriptiveColumnSectionRenderer.render(evrs[column]), outfile, indent=2)


@pytest.mark.smoketest
def test_render_prescriptive_column_section_renderer(titanic_expectations):
    # Group expectations by column
    exp_groups = {}
    # print(json.dumps(titanic_expectations, indent=2))
    for exp in titanic_expectations["expectations"]:
        try:
            column = exp["kwargs"]["column"]
            if column not in exp_groups:
                exp_groups[column] = []
            exp_groups[column].append(exp)
        except KeyError:
            pass

    for column in exp_groups.keys():
        with open('./tests/render/output/test_render_prescriptive_column_section_renderer' + column + '.json', 'w') \
                as outfile:
            json.dump(PrescriptiveColumnSectionRenderer.render(exp_groups[column]), outfile, indent=2)

    # # This can be used for regression testing
    # for column in exp_groups.keys():
    #     with open('./tests/render/output/test_render_prescriptive_column_section_renderer' + column + '.json') as infile:
    #         assert json.dumps(PrescriptiveColumnSectionRenderer.render(exp_groups[column]), indent=2) == infile


def test_DescriptiveColumnSectionRenderer_render(titanic_profiled_evrs_1, titanic_profiled_name_column_evrs):
    #Smoke test for titanic names
    document = DescriptiveColumnSectionRenderer().render(titanic_profiled_name_column_evrs)
    print(document)
    assert document != {}


    #Smoke test for titanic Ages

    #This is a janky way to fetch expectations matching a specific name from an EVR suite.
    #TODO: It will no longer be necessary once we implement ValidationResultSuite._group_evrs_by_column
    from great_expectations.render.renderer.renderer import (
        Renderer,
    )
    evrs_by_column = Renderer()._group_evrs_by_column(titanic_profiled_evrs_1)
    print(evrs_by_column.keys())

    age_column_evrs = evrs_by_column["Age"]
    print(json.dumps(age_column_evrs, indent=2))

    document = DescriptiveColumnSectionRenderer().render(age_column_evrs)
    print(document)

    # Save output to view
    # html = DefaultJinjaPageView.render({"sections":[document]})
    # print(html)
    # open('./tests/render/output/titanic.html', 'w').write(html)


def test_DescriptiveColumnSectionRenderer_render_header(titanic_profiled_name_column_evrs):
    content_blocks = []
    DescriptiveColumnSectionRenderer()._render_header(
        titanic_profiled_name_column_evrs,
        content_blocks,
        column_type = None
    )
    print(json.dumps(content_blocks, indent=2))
    
    assert len(content_blocks) == 1
    content_block = content_blocks[0]
    assert content_block["content_block_type"] == "header"
    assert content_block["header"] == {
        "template": "Name",
        "tooltip": {
            "content": "expect_column_to_exist",
            "placement": "top"
        }
    }
    assert content_block["sub_header"] == {
        "template": "Type: None",
        "tooltip": {
            "content": "expect_column_values_to_be_of_type <br>expect_column_values_to_be_in_type_list"
        }
    }


# def test_DescriptiveColumnSectionRenderer_render_overview_table():
#     evrs = {}
#     DescriptiveColumnSectionRenderer()._render_overview_table(evrs, content_blocks)

# def test_DescriptiveColumnSectionRenderer_render_quantile_table():
#     evrs = {}
#     DescriptiveColumnSectionRenderer()._render_quantile_table(evrs, content_blocks)

# def test_DescriptiveColumnSectionRenderer_render_stats_table():
#     evrs = {}
#     DescriptiveColumnSectionRenderer()._render_stats_table(evrs, content_blocks)

# def test_DescriptiveColumnSectionRenderer_render_histogram(titanic_profiled_evrs_1):
#     evrs = {}
#     DescriptiveColumnSectionRenderer()._render_histogram(evrs, content_blocks)

# def test_DescriptiveColumnSectionRenderer_render_values_set():
#     evrs = {}
#     DescriptiveColumnSectionRenderer()._render_values_set(evrs, content_blocks)

def test_DescriptiveColumnSectionRenderer_render_bar_chart_table(titanic_profiled_evrs_1):

    print(titanic_profiled_evrs_1["results"][0])
    distinct_values_evrs = [evr for evr in titanic_profiled_evrs_1["results"] if evr["expectation_config"]["expectation_type"] == "expect_column_distinct_values_to_be_in_set"]
    
    assert len(distinct_values_evrs) == 4

    content_blocks = []
    for evr in distinct_values_evrs:
        DescriptiveColumnSectionRenderer()._render_bar_chart_table(
            distinct_values_evrs,
            content_blocks,
        )
    print(json.dumps(content_blocks, indent=2))

    assert len(content_blocks) == 4

    for content_block in content_blocks:
        assert content_block["content_block_type"] == "graph"
        assert set(content_block.keys()) == {"header", "content_block_type", "graph", "styling"}
        assert json.loads(content_block["graph"])

    # expect_column_kl_divergence_to_be_less_than

    # #This is a janky way to fetch expectations matching a specific name from an EVR suite.
    # #TODO: It will no longer be necessary once we implement ValidationResultSuite._group_evrs_by_column
    # from great_expectations.render.renderer.renderer import (
    #     Renderer,
    # )
    # evrs_by_column = Renderer()._group_evrs_by_column(titanic_profiled_evrs_1)
    # print(evrs_by_column.keys())

    # age_column_evrs = evrs_by_column["Survived"]
    # # print(json.dumps(age_column_evrs, indent=2))

    # document = DescriptiveColumnSectionRenderer().render(age_column_evrs)
    # # print(document)

    # html = DefaultJinjaPageView.render({"sections":[document]})
    # # print(html)
    # open('./tests/render/output/titanic_age.html', 'w').write(html)


# def test_DescriptiveColumnSectionRenderer_render_expectation_types():
#     evrs = {}
#     DescriptiveColumnSectionRenderer()._render_expectation_types(evrs, content_blocks)

# def test_DescriptiveColumnSectionRenderer_render_failed():
#     evrs = {}
#     DescriptiveColumnSectionRenderer()._render_failed(evrs, content_blocks)


def test_PrescriptiveColumnSectionRenderer_render_header(titanic_profiled_name_column_expectations):
    remaining_expectations, content_blocks = PrescriptiveColumnSectionRenderer._render_header(
        titanic_profiled_name_column_expectations,#["expectations"],
        [],
    )

    print(json.dumps(content_blocks, indent=2))
    assert content_blocks == [
        RenderedComponentContent(**{
            "content_block_type": "header",
            "header": "Name",
            "styling": {
            "classes": [
                "col-12"
            ],
            "header": {
                "classes": [
                "alert",
                "alert-secondary"
                ]
            }
            }
        })
    ]


def test_PrescriptiveColumnSectionRenderer_render_bullet_list(titanic_profiled_name_column_expectations):
    remaining_expectations, content_blocks = PrescriptiveColumnSectionRenderer._render_bullet_list(
        titanic_profiled_name_column_expectations,#["expectations"],
        [],
    )

    print(json.dumps(content_blocks, indent=2))

    assert len(content_blocks) == 1

    content_block = content_blocks[0]
    assert content_block["content_block_type"] == "bullet_list"
    assert len(content_block["bullet_list"]) == 4
    assert "value types must belong to this set" in json.dumps(content_block)
    assert "may have any number of unique values" in json.dumps(content_block)
    assert "may have any percentage of unique values" in json.dumps(content_block)
    assert "values must not be null, at least $mostly_pct % of the time." in json.dumps(content_block)
    
