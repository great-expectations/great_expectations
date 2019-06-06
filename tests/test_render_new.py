import pytest

import json

from great_expectations.render.model import DescriptivePageModel, DescriptiveColumnSectionModel
from great_expectations.render.view import DescriptivePageView


@pytest.fixture()
def validation_results():
    with open("./tests/test_sets/expected_cli_results_default.json", "r") as infile:
        return json.load(infile)

def test_render_descriptive_page_model(validation_results):
    print(json.dumps(DescriptivePageModel.render(validation_results)), indent=2)
    assert True

def test_render_descriptive_page_view(validation_results):
    model = DescriptivePageModel.render(validation_results)
    print(DescriptivePageView.render(model))
    assert False

def test_render_descriptive_column_section_model(validation_results):
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
        print(json.dumps(DescriptiveColumnSectionModel.render(evrs[column]), indent=2))
    assert True