import pytest
import json

from collections import OrderedDict

from great_expectations.render.renderer import (
    PrescriptiveColumnSectionRenderer,
    DescriptiveColumnSectionRenderer,
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


def test_DescriptiveColumnSectionRenderer_render(titanic_profiled_evrs_1):
    document = DescriptiveColumnSectionRenderer().render(titanic_profiled_evrs_1["results"])
    print(document)
    assert document != {}

def test_DescriptiveColumnSectionRenderer_render_header(titanic_profiled_evrs_1):
    evrs_by_column = DescriptiveColumnSectionRenderer()._group_evrs_by_column(titanic_profiled_evrs_1)
    print(evrs_by_column.keys())

    column_evrs = evrs_by_column["Name"]

    print(json.dumps(column_evrs, indent=2))
    content_blocks = []
    column_type = None
    DescriptiveColumnSectionRenderer()._render_header(column_evrs, content_blocks, column_type)
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

# def test_DescriptiveColumnSectionRenderer_render_histogram():
#     evrs = {}
#     DescriptiveColumnSectionRenderer()._render_histogram(evrs, content_blocks)

# def test_DescriptiveColumnSectionRenderer_render_values_set():
#     evrs = {}
#     DescriptiveColumnSectionRenderer()._render_values_set(evrs, content_blocks)

# def test_DescriptiveColumnSectionRenderer_render_bar_chart_table():
#     evrs = {}
#     DescriptiveColumnSectionRenderer()._render_bar_chart_table(evrs, content_blocks)

# def test_DescriptiveColumnSectionRenderer_render_expectation_types():
#     evrs = {}
#     DescriptiveColumnSectionRenderer()._render_expectation_types(evrs, content_blocks)

# def test_DescriptiveColumnSectionRenderer_render_failed():
#     evrs = {}
#     DescriptiveColumnSectionRenderer()._render_failed(evrs, content_blocks)


# def test_PrescriptiveColumnSectionRenderer_render_header():
#     remaining_expectations, content_blocks = cls._render_header(
#         expectations, [])

# def test_PrescriptiveColumnSectionRenderer_render_bullet_list():
#     remaining_expectations, content_blocks = cls._render_bullet_list(
#         remaining_expectations, content_blocks)
