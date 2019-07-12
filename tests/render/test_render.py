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
    with open('./tests/render/fixtures/BasicDatasetProfiler_evrs.json', 'r') as infile:
        return json.load(infile, object_pairs_hook=OrderedDict)


@pytest.fixture(scope="module")
def tetanus_varicella_basic_dataset_profiler_evrs_with_exception():
    with open('./tests/render/fixtures/BasicDatasetProfiler_evrs_with_exception.json', 'r') as infile:
        return json.load(infile)


@pytest.fixture(scope="module")
def tetanus_varicella_basic_dataset_profiler_expectations():
    with open('./tests/render/fixtures/BasicDatasetProfiler_expectations.json', 'r') as infile:
        return json.load(infile, object_pairs_hook=OrderedDict)


@pytest.mark.smoketest
def test_smoke_render_descriptive_page_renderer(titanic_validation_results):
    rendered = DescriptivePageRenderer.render(titanic_validation_results)
    with open('./tests/render/output/test_render_descriptive_page_renderer.json', 'w') as outfile:
        json.dump(rendered, outfile, indent=2)

    assert len(rendered["sections"]) > 5


@pytest.mark.smoketest
def test_render_descriptive_page_view(titanic_validation_results):
    document = DescriptivePageRenderer.render(titanic_validation_results)
    rendered = DefaultJinjaPageView.render(document)
    with open('./tests/render/output/test_render_descriptive_page_view.html', 'w') as outfile:
        outfile.write(rendered)

    assert len(rendered) > 1000


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


def test_content_block_list_available_expectations():
    available_expectations = ValueListContentBlockRenderer.list_available_expectations()
    assert available_expectations == ['expect_column_values_to_be_in_set']


@pytest.mark.smoketest
def test_render_profiled_fixture_expectations(tetanus_varicella_basic_dataset_profiler_expectations):
    rendered_json = PrescriptivePageRenderer.render(tetanus_varicella_basic_dataset_profiler_expectations)
    rendered_page = DefaultJinjaPageView.render(rendered_json)

    with open('./tests/render/output/test_render_profiled_fixture_expectations.html', 'w') as f:
        f.write(rendered_page)

    assert rendered_page[:15] == "<!DOCTYPE html>"
    assert rendered_page[-7:] == "</html>"


@pytest.mark.smoketest
def test_render_profiled_fixture_evrs(tetanus_varicella_basic_dataset_profiler_evrs):
    rendered_json = DescriptivePageRenderer.render(tetanus_varicella_basic_dataset_profiler_evrs)
    rendered_page = DefaultJinjaPageView.render(rendered_json)

    with open('./tests/render/output/test_render_profiled_fixture_evrs.html', 'w') as f:
        f.write(rendered_page)

    assert rendered_page[:15] == "<!DOCTYPE html>"
    assert rendered_page[-7:] == "</html>"


@pytest.mark.smoketest
def test_smoke_render_descriptive_page_renderer_with_exception(
        tetanus_varicella_basic_dataset_profiler_evrs_with_exception):
    rendered_json = DescriptivePageRenderer.render(tetanus_varicella_basic_dataset_profiler_evrs_with_exception)
    rendered_page = DefaultJinjaPageView.render(rendered_json)

    with open('./tests/render/output/test_render_descriptive_column_section_renderer_with_exception.html', 'w') as f:
        f.write(rendered_page)

    assert rendered_page[:15] == "<!DOCTYPE html>"
    assert rendered_page[-7:] == "</html>"
    assert "exception" in rendered_page


@pytest.mark.smoketest
def test_full_oobe_flow():
    df = ge.read_csv("examples/data/Titanic.csv")
    df.profile(BasicDatasetProfiler)
    evrs = df.validate()  # ["results"]

    rendered_json = DescriptivePageRenderer.render(evrs)
    rendered_page = DefaultJinjaPageView.render(rendered_json)

    with open('./tests/render/output/test_full_oobe_flow.html', 'w') as f:
        f.write(rendered_page)

    assert rendered_page[:15] == "<!DOCTYPE html>"
    assert rendered_page[-7:] == "</html>"

