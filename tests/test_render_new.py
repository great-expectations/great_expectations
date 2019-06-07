import pytest

import json

from great_expectations.render.renderer import DescriptivePageRenderer, DescriptiveColumnSectionRenderer
from great_expectations.render.view import DescriptivePageView


@pytest.fixture()
def validation_results():
    with open("./tests/test_sets/expected_cli_results_default.json", "r") as infile:
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