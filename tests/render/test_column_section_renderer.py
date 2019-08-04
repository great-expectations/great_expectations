import pytest
import json


from great_expectations.render.renderer import (
    PrescriptiveColumnSectionRenderer,
    DescriptiveColumnSectionRenderer,
)

@pytest.fixture(scope="module")
def profiled_evrs_1():
    return json.load(open("./tests/render/fixtures/BasicDatasetProfiler_evrs.json"))

def test_DescriptiveColumnSectionRenderer_render(profiled_evrs_1):
    document = DescriptiveColumnSectionRenderer().render(profiled_evrs_1["results"])

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
