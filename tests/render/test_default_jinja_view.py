import pytest

import json

import great_expectations as ge
import great_expectations.render as render
from great_expectations.render.renderer import (
    DescriptivePageRenderer,
    DescriptiveColumnSectionRenderer,
    PrescriptiveColumnSectionRenderer,
    PrescriptivePageRenderer,
)
from great_expectations.render.view import DefaultJinjaPageView


@pytest.fixture()
def validation_results():
    with open("./tests/test_sets/expected_cli_results_default.json", "r") as infile:
        return json.load(infile)


@pytest.fixture()
def expectations():
    with open("./tests/test_sets/titanic_expectations.json", "r") as infile:
        return json.load(infile)


@pytest.fixture()
def document_snapshot():
    with open("./tests/render/fixtures/document_snapshot.json", "r") as infile:
        return json.load(infile)


def test_render_descriptive_page_view(validation_results, document_snapshot):
    document = DescriptivePageRenderer.render(validation_results)
    print(json.dumps(document, indent=2))
    # print(DefaultJinjaPageView.render(renderer))
    # TODO: Use above print to set up snapshot test once we like the result
    # assert document == document_snapshot
