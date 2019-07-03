import pytest

import json
from collections import OrderedDict

import great_expectations as ge
from great_expectations.render.renderer import (
    DescriptivePageRenderer,
    DescriptiveColumnSectionRenderer,
    PrescriptiveColumnSectionRenderer,
    PrescriptivePageRenderer,
)
from great_expectations.render.view import DefaultJinjaPageView
from great_expectations.render.renderer.content_block import ValueListContentBlockRenderer
from great_expectations.profile.basic_dataset_profiler import BasicDatasetProfiler


@pytest.fixture(scope="module")
def titanic_validation_results():
    with open("./tests/test_sets/expected_cli_results_default.json", "r") as infile:
        return json.load(infile)


@pytest.fixture(scope="module")
def titanic_expectations():
    with open("./tests/test_sets/titanic_expectations.json", "r") as infile:
        return json.load(infile, object_pairs_hook=OrderedDict)


@pytest.fixture(scope="module")
def tetanus_varicella_basic_dataset_profiler_evrs():
    with open('tests/render/fixtures/BasicDatasetProfiler_evrs.json', 'r') as infile:
        return json.load(infile, object_pairs_hook=OrderedDict)


@pytest.fixture(scope="module")
def tetanus_varicella_basic_dataset_profiler_expectations():
    with open('tests/render/fixtures/BasicDatasetProfiler_expectations.json', 'r') as infile:
        return json.load(infile, object_pairs_hook=OrderedDict)


def test_render_descriptive_page_renderer(titanic_validation_results):
    print(json.dumps(DescriptivePageRenderer.render(titanic_validation_results), indent=2))
    # TODO: Use above print to set up snapshot test once we like the result
    assert True


def test_render_descriptive_page_view(titanic_validation_results):
    renderer = DescriptivePageRenderer.render(titanic_validation_results)
    print(DefaultJinjaPageView.render(renderer))
    # TODO: Use above print to set up snapshot test once we like the result
    assert True


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
        print(json.dumps(DescriptiveColumnSectionRenderer.render(
            evrs[column]), indent=2))
    # TODO: Use above print to set up snapshot test once we like the result
    assert True


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
        print(column)
        print(json.dumps(PrescriptiveColumnSectionRenderer.render(
            exp_groups[column]), indent=2))
    # TODO: Use above print to set up snapshot test once we like the result
    assert True


def test_content_block_list_available_expectations(titanic_expectations):
    available_expectations = ValueListContentBlockRenderer.list_available_expectations()
    assert available_expectations == ['expect_column_values_to_be_in_set']


def test_render_profiled_fixture_expectations(tetanus_varicella_basic_dataset_profiler_expectations):

    rendered_json = PrescriptivePageRenderer.render(tetanus_varicella_basic_dataset_profiler_expectations)

    # print(json.dumps(rendered_json, indent=2))
    # with open('./test.json', 'w') as f:
    #     f.write(json.dumps(rendered_json, indent=2))

    rendered_page = DefaultJinjaPageView.render(rendered_json)
    assert rendered_page != None

    # print(rendered_page)
    with open('./tests/render/output/test_render_profiled_fixture_expectations.html', 'w') as f:
        f.write(rendered_page)


def test_render_profiled_fixture_evrs(tetanus_varicella_basic_dataset_profiler_evrs):
    rendered_json = DescriptivePageRenderer.render(tetanus_varicella_basic_dataset_profiler_evrs)

    print(json.dumps(rendered_json, indent=2))
    # with open('./test.json', 'w') as f:
    #     f.write(json.dumps(rendered_json, indent=2))

    rendered_page = DefaultJinjaPageView.render(rendered_json)
    assert rendered_page != None

    # print(rendered_page)
    with open('./tests/render/output/test_render_profiled_fixture_evrs.html', 'w') as f:
        f.write(rendered_page)

    # assert False


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
    rendered_page = DefaultJinjaPageView.render(rendered_json)
    assert rendered_page != None
    # print(rendered_page)

    with open('./tests/render/output/test_full_oobe_flow.html', 'w') as f:
        f.write(rendered_page)

    # assert False
