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


def test_DescriptiveColumnSectionRenderer_render(profiled_evrs_1):
    document = DescriptiveColumnSectionRenderer().render(profiled_evrs_1["results"])
    print(document)
    assert document != {}

def test_DescriptiveColumnSectionRenderer_render_header(profiled_evrs_1):
    content_blocks = []
    column_type = None
    DescriptiveColumnSectionRenderer()._render_header(profiled_evrs_1, content_blocks, column_type)

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
