import json
from collections import OrderedDict

import pytest

import great_expectations as ge
from great_expectations.core import expectationSuiteSchema
from great_expectations.data_context.util import file_relative_path
from great_expectations.profile.basic_dataset_profiler import BasicDatasetProfiler
from great_expectations.render.renderer import (
    ExpectationSuiteColumnSectionRenderer,
    ExpectationSuitePageRenderer,
    ProfilingResultsColumnSectionRenderer,
    ProfilingResultsPageRenderer,
    ValidationResultsColumnSectionRenderer,
    ValidationResultsPageRenderer,
)
from great_expectations.render.renderer.content_block import (
    ValidationResultsTableContentBlockRenderer,
)
from great_expectations.render.view import DefaultJinjaPageView
from tests.test_utils import expectationSuiteValidationResultSchema


@pytest.fixture(scope="module")
def titanic_profiler_evrs():
    with open(
        file_relative_path(__file__, "./fixtures/BasicDatasetProfiler_evrs.json")
    ) as infile:
        return expectationSuiteValidationResultSchema.load(
            json.load(infile, object_pairs_hook=OrderedDict)
        )


@pytest.fixture(scope="module")
def titanic_profiler_evrs_with_exception():
    with open(
        file_relative_path(
            __file__, "./fixtures/BasicDatasetProfiler_evrs_with_exception.json"
        ),
    ) as infile:
        return expectationSuiteValidationResultSchema.load(json.load(infile))


@pytest.fixture(scope="module")
def titanic_dataset_profiler_expectations():
    with open(
        file_relative_path(
            __file__, "./fixtures/BasicDatasetProfiler_expectations.json"
        ),
    ) as infile:
        return expectationSuiteSchema.load(
            json.load(infile, object_pairs_hook=OrderedDict)
        )


@pytest.fixture(scope="module")
def titanic_dataset_profiler_expectations_with_distribution():
    with open(
        file_relative_path(
            __file__,
            "./fixtures/BasicDatasetProfiler_expectations_with_distribution.json",
        ),
    ) as infile:
        return expectationSuiteSchema.load(
            json.load(infile, encoding="utf-8", object_pairs_hook=OrderedDict)
        )


@pytest.mark.smoketest
@pytest.mark.rendered_output
def test_smoke_render_profiling_results_page_renderer(titanic_profiled_evrs_1):
    rendered = ProfilingResultsPageRenderer().render(titanic_profiled_evrs_1)
    with open(
        file_relative_path(
            __file__, "./output/test_render_profiling_results_page_renderer.json"
        ),
        "w",
    ) as outfile:
        json.dump(rendered.to_json_dict(), outfile, indent=2)

    assert len(rendered.sections) > 5


@pytest.mark.smoketest
@pytest.mark.rendered_output
def test_render_profiling_results_column_section_renderer(titanic_profiled_evrs_1):
    # Group EVRs by column
    evrs = {}
    for evr in titanic_profiled_evrs_1.results:
        try:
            column = evr.expectation_config.kwargs["column"]
            if column not in evrs:
                evrs[column] = []
            evrs[column].append(evr)
        except KeyError:
            pass

    for column in evrs.keys():
        with open(
            file_relative_path(
                __file__,
                "./output/test_render_profiling_results_column_section_renderer__"
                + column
                + ".json",
            ),
            "w",
        ) as outfile:
            json.dump(
                ProfilingResultsColumnSectionRenderer()
                .render(evrs[column])
                .to_json_dict(),
                outfile,
                indent=2,
            )


@pytest.mark.smoketest
@pytest.mark.rendered_output
def test_smoke_render_validation_results_page_renderer(titanic_profiler_evrs):
    rendered = ValidationResultsPageRenderer().render(titanic_profiler_evrs)
    with open(
        file_relative_path(
            __file__, "./output/test_render_validation_results_page_renderer.json"
        ),
        "w",
    ) as outfile:
        json.dump(rendered.to_json_dict(), outfile, indent=2)
    assert len(rendered.sections) > 5


@pytest.mark.smoketest
@pytest.mark.rendered_output
def test_render_validation_results_column_section_renderer(titanic_profiler_evrs):
    # Group EVRs by column
    evrs = {}
    for evr in titanic_profiler_evrs.results:
        try:
            column = evr.expectation_config.kwargs["column"]
            if column not in evrs:
                evrs[column] = []
            evrs[column].append(evr)
        except KeyError:
            pass

    for column in evrs.keys():
        with open(
            file_relative_path(
                __file__,
                "./output/test_render_validation_results_column_section_renderer__"
                + column
                + ".json",
            ),
            "w",
        ) as outfile:
            json.dump(
                ValidationResultsColumnSectionRenderer()
                .render(evrs[column])
                .to_json_dict(),
                outfile,
                indent=2,
            )


@pytest.mark.smoketest
@pytest.mark.rendered_output
def test_render_expectation_suite_column_section_renderer(
    titanic_profiled_expectations_1,
):
    # Group expectations by column
    exp_groups = {}
    # print(json.dumps(titanic_profiled_expectations_1, indent=2))
    for exp in titanic_profiled_expectations_1.expectations:
        try:
            column = exp.kwargs["column"]
            if column not in exp_groups:
                exp_groups[column] = []
            exp_groups[column].append(exp)
        except KeyError:
            pass

    for column in exp_groups.keys():
        with open(
            file_relative_path(
                __file__,
                "./output/test_render_expectation_suite_column_section_renderer"
                + column
                + ".json",
            ),
            "w",
        ) as outfile:
            json.dump(
                ExpectationSuiteColumnSectionRenderer()
                .render(exp_groups[column])
                .to_json_dict(),
                outfile,
                indent=2,
            )


def test_content_block_list_available_expectations():
    available_expectations = (
        ValidationResultsTableContentBlockRenderer.list_available_expectations()
    )
    known_validation_results_implemented_expectations = {
        "expect_column_distinct_values_to_be_in_set",
        "expect_column_distinct_values_to_contain_set",
        "expect_column_distinct_values_to_equal_set",
        "expect_column_kl_divergence_to_be_less_than",
        "expect_column_max_to_be_between",
        "expect_column_mean_to_be_between",
        "expect_column_median_to_be_between",
        "expect_column_min_to_be_between",
        "expect_column_most_common_value_to_be_in_set",
        "expect_column_pair_values_A_to_be_greater_than_B",
        "expect_column_pair_values_to_be_equal",
        "expect_column_proportion_of_unique_values_to_be_between",
        "expect_column_stdev_to_be_between",
        "expect_column_sum_to_be_between",
        "expect_column_to_exist",
        "expect_column_unique_value_count_to_be_between",
        "expect_column_value_lengths_to_be_between",
        "expect_column_value_lengths_to_equal",
        "expect_column_values_to_be_between",
        "expect_column_values_to_be_dateutil_parseable",
        "expect_column_values_to_be_decreasing",
        "expect_column_values_to_be_in_set",
        "expect_column_values_to_be_in_type_list",
        "expect_column_values_to_be_increasing",
        "expect_column_values_to_be_json_parseable",
        "expect_column_values_to_be_null",
        "expect_column_values_to_be_of_type",
        "expect_column_values_to_be_unique",
        "expect_column_values_to_match_json_schema",
        "expect_column_values_to_match_regex",
        "expect_column_values_to_match_regex_list",
        "expect_column_values_to_match_strftime_format",
        "expect_column_values_to_not_be_in_set",
        "expect_column_values_to_not_be_null",
        "expect_column_values_to_not_match_regex",
        "expect_column_values_to_not_match_regex_list",
        "expect_select_column_values_to_be_unique_within_record",
        "expect_compound_columns_to_be_unique",
        "expect_table_columns_to_match_ordered_list",
        "expect_table_columns_to_match_set",
        "expect_table_row_count_to_be_between",
        "expect_table_row_count_to_equal",
        "expect_column_pair_cramers_phi_value_to_be_less_than",
    }
    assert known_validation_results_implemented_expectations <= set(
        available_expectations
    )
    assert len(available_expectations) >= len(
        known_validation_results_implemented_expectations
    )


@pytest.mark.smoketest
@pytest.mark.rendered_output
def test_render_profiled_fixture_expectation_suite(
    titanic_dataset_profiler_expectations,
):
    rendered_content = ExpectationSuitePageRenderer().render(
        titanic_dataset_profiler_expectations
    )
    rendered_page = DefaultJinjaPageView().render(rendered_content)

    with open(
        file_relative_path(
            __file__, "./output/test_render_profiled_fixture_expectation_suite.html"
        ),
        "wb",
    ) as f:
        f.write(rendered_page.encode("utf-8"))

    assert rendered_page[:15] == "<!DOCTYPE html>"
    assert rendered_page[-7:] == "</html>"


@pytest.mark.smoketest
@pytest.mark.rendered_output
def test_render_profiled_fixture_expectation_suite_with_distribution(
    titanic_dataset_profiler_expectations_with_distribution,
):
    # Tests sparkline
    rendered_content = ExpectationSuitePageRenderer().render(
        titanic_dataset_profiler_expectations_with_distribution
    )
    rendered_page = DefaultJinjaPageView().render(rendered_content)

    with open(
        file_relative_path(
            __file__,
            "./output/titanic_dataset_profiler_expectation_suite_with_distribution.html",
        ),
        "wb",
    ) as f:
        f.write(rendered_page.encode("utf-8"))

    assert rendered_page[:15] == "<!DOCTYPE html>"
    assert rendered_page[-7:] == "</html>"


@pytest.mark.smoketest
@pytest.mark.rendered_output
def test_render_profiling_results(titanic_profiled_evrs_1):
    rendered_content = ProfilingResultsPageRenderer().render(titanic_profiled_evrs_1)
    rendered_page = DefaultJinjaPageView().render(rendered_content)

    with open(
        file_relative_path(__file__, "./output/test_render_profiling_results.html"),
        "wb",
    ) as f:
        f.write(rendered_page.encode("utf-8"))

    assert rendered_page[:15] == "<!DOCTYPE html>"
    assert rendered_page[-7:] == "</html>"


@pytest.mark.smoketest
@pytest.mark.rendered_output
def test_render_validation_results(titanic_profiled_evrs_1):
    rendered_content = ValidationResultsPageRenderer().render(titanic_profiled_evrs_1)
    rendered_page = DefaultJinjaPageView().render(rendered_content)

    with open(
        file_relative_path(__file__, "./output/test_render_validation_results.html"),
        "wb",
    ) as f:
        f.write(rendered_page.encode("utf-8"))

    assert rendered_page[:15] == "<!DOCTYPE html>"
    assert rendered_page[-7:] == "</html>"
    assert "Table-Level Expectations" in rendered_page
    assert (
        'Must have greater than or equal to <span class="badge badge-secondary" >0</span> rows.'
        in rendered_page
    )
    # assert 'This table should have a list of columns in a specific order, but that order is not specified.' \
    #        in rendered_page


@pytest.mark.smoketest
@pytest.mark.rendered_output
def test_smoke_render_profiling_results_page_renderer_with_exception(
    titanic_profiler_evrs_with_exception,
):
    rendered_content = ProfilingResultsPageRenderer().render(
        titanic_profiler_evrs_with_exception
    )
    rendered_page = DefaultJinjaPageView().render(rendered_content)

    with open(
        file_relative_path(
            __file__,
            "./output/test_render_profiling_results_column_section_renderer_with_exception.html",
        ),
        "wb",
    ) as outfile:
        outfile.write(rendered_page.encode("utf-8"))

    assert rendered_page[:15] == "<!DOCTYPE html>"
    assert rendered_page[-7:] == "</html>"
    assert "exception" in rendered_page


@pytest.mark.smoketest
@pytest.mark.rendered_output
def test_full_oobe_flow():
    df = ge.read_csv(file_relative_path(__file__, "../../examples/data/Titanic.csv"))
    df.data_asset_name = "my_datasource/my_generator/my_asset"
    df.profile(BasicDatasetProfiler)
    evrs = df.validate()  # results

    rendered_content = ProfilingResultsPageRenderer().render(evrs)
    rendered_page = DefaultJinjaPageView().render(rendered_content)

    with open(
        file_relative_path(__file__, "./output/test_full_oobe_flow.html"), "wb"
    ) as f:
        f.write(rendered_page.encode("utf-8"))

    assert rendered_page[:15] == "<!DOCTYPE html>"
    assert rendered_page[-7:] == "</html>"


def test_render_string_template():
    template = {
        "template": "$column Kullback-Leibler (KL) divergence with respect to the following distribution must be lower than $threshold: $sparklines_histogram",
        "params": {
            "column": "categorical_fixed",
            "partition_object": {
                "weights": [0.54, 0.32, 0.14],
                "values": ["A", "B", "C"],
            },
            "threshold": 0.1,
            "sparklines_histogram": "\u2588\u2584\u2581",
        },
        "styling": {
            "default": {"classes": ["badge", "badge-secondary"]},
            "params": {"sparklines_histogram": {"styles": {"font-family": "serif"}}},
        },
    }

    res = (
        DefaultJinjaPageView()
        .render_string_template(template)
        .replace(" ", "")
        .replace("\t", "")
        .replace("\n", "")
    )
    expected = (
        """<span>
                <span class="badge badge-secondary" >categorical_fixed</span> Kullback-Leibler (KL) divergence with respect to the following distribution must be lower than <span class="badge badge-secondary" >0.1</span>: <span style="font-family:serif;" >█▄▁</span>
            </span>""".replace(
            " ", ""
        )
        .replace("\t", "")
        .replace("\n", "")
    )
    assert res == expected

    template = {
        "template": "$column Kullback-Leibler (KL) divergence with respect to the following distribution must be lower than $threshold: $sparklines_histogram",
        "params": {
            "column": "categorical_fixed",
            "partition_object": {
                "weights": [0.54, 0.32, 0.14],
                "values": ["A", "B", "C"],
            },
            "threshold": 0.1,
            "sparklines_histogram": "▃▆▁█",
        },
        "styling": {
            "default": {"classes": ["badge", "badge-secondary"]},
            "params": {"sparklines_histogram": {"styles": {"font-family": "serif"}}},
        },
    }

    res = (
        DefaultJinjaPageView()
        .render_string_template(template)
        .replace(" ", "")
        .replace("\t", "")
        .replace("\n", "")
    )
    expected = (
        """<span>
                <span class="badge badge-secondary" >categorical_fixed</span> Kullback-Leibler (KL) divergence with respect to the following distribution must be lower than <span class="badge badge-secondary" >0.1</span>: <span style="font-family:serif;" >▃▆▁█</span>
            </span>""".replace(
            " ", ""
        )
        .replace("\t", "")
        .replace("\n", "")
    )

    assert res == expected
