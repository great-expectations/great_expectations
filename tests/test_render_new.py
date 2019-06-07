import pytest

import json

from great_expectations.render.renderer import DescriptivePageRenderer, DescriptiveColumnSectionRenderer, PrescriptiveColumnSectionRenderer
from great_expectations.render.view import DescriptivePageView
from great_expectations.render.renderer.content_block import ValueListContentBlock

@pytest.fixture()
def validation_results():
    with open("./tests/test_sets/expected_cli_results_default.json", "r") as infile:
        return json.load(infile)

@pytest.fixture()
def expectations():
    with open("./tests/test_sets/titanic_expectations.json", "r") as infile:
        return json.load(infile)

def test_render_descriptive_page_renderer(validation_results):
    print(json.dumps(DescriptivePageRenderer.render(validation_results), indent=2))
    # TODO: Use above print to set up snapshot test once we like the result
    assert True

def test_render_descriptive_page_view(validation_results):
    renderer = DescriptivePageRenderer.render(validation_results)
    print(DescriptivePageView.render(renderer))
    # TODO: Use above print to set up snapshot test once we like the result
    assert True

def test_render_descriptive_column_section_renderer(validation_results):
    # Group EVRs by column
    evrs = {}
    for evr in validation_results["results"]:
        try:
            column = evr["expectation_config"]["kwargs"]["column"]
            if column not in evrs:
                evrs[column] = []
            evrs[column].append(evr)
        except KeyError:
            pass

    for column in evrs.keys():
        print(json.dumps(DescriptiveColumnSectionRenderer.render(evrs[column]), indent=2))
    # TODO: Use above print to set up snapshot test once we like the result
    assert True


def test_render_prescriptive_column_section_renderer(expectations):
    # Group expectations by column
    exp_groups = {}
    # print(json.dumps(expectations, indent=2))
    for exp in expectations["expectations"]:
        try:
            column = exp["kwargs"]["column"]
            if column not in exp_groups:
                exp_groups[column] = []
            exp_groups[column].append(exp)
        except KeyError:
            pass

    for column in exp_groups.keys():
        print(column)
        print(json.dumps(PrescriptiveColumnSectionRenderer.render(exp_groups[column]), indent=2))
    # TODO: Use above print to set up snapshot test once we like the result
    assert True


def test_content_block_list_available_expectations(expectations):
    available_expectations = ValueListContentBlock.list_available_expectations()
    assert available_expectations == ['expect_column_values_to_be_in_set']
