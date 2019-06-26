import pytest

import json

import great_expectations as ge
from great_expectations.render.renderer import (
    DescriptivePageRenderer,
    DescriptiveColumnSectionRenderer,
    PrescriptiveColumnSectionRenderer,
    PrescriptivePageRenderer,
)
from great_expectations.render.view import PageView
from great_expectations.render.renderer.content_block import ValueListContentBlockRenderer
from great_expectations.profile.basic_dataset_profiler import BasicDatasetProfiler


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
    print(PageView.render(renderer))
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
        print(json.dumps(DescriptiveColumnSectionRenderer.render(
            evrs[column]), indent=2))
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
        print(json.dumps(PrescriptiveColumnSectionRenderer.render(
            exp_groups[column]), indent=2))
    # TODO: Use above print to set up snapshot test once we like the result
    assert True


def test_content_block_list_available_expectations(expectations):
    available_expectations = ValueListContentBlockRenderer.list_available_expectations()
    assert available_expectations == ['expect_column_values_to_be_in_set']


def test_render_profiled_fixtures():
    expectations = json.load(
        open('tests/render/fixtures/BasicDatasetProfiler_expectations.json')
    )

    rendered_json = PrescriptivePageRenderer.render(expectations)
    # print(json.dumps(rendered_json, indent=2))
    rendered_page = PageView.render(rendered_json)
    assert rendered_page != None

    with open('./test.html', 'w') as f:
        f.write(rendered_page)

    # print(rendered_page)

    assert False


def test_full_oobe_flow():
    df = ge.read_csv("examples/data/Titanic.csv")
    # df = ge.read_csv("examples/data/Meteorite_Landings.csv")
    # df = ge.read_csv("examples/data/adult.data")
    df.profile(BasicDatasetProfiler)
    # df.autoinspect(ge.dataset.autoinspect.columns_exist)
    evrs = df.validate()  # ["results"]
    # print(json.dumps(evrs, indent=2))

    rendered_json = DescriptivePageRenderer.render(evrs)
    # print(json.dumps(rendered_json, indent=2))
    rendered_page = PageView.render(rendered_json)
    assert rendered_page != None

    # assert False
    # with open('./test.html', 'w') as f:
    #     f.write(rendered_page)
