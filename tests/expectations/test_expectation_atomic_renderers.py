import re
from pprint import pprint
from typing import Callable, Dict, Union

import pytest

from great_expectations.core import ExpectationValidationResult
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.expectations.registry import get_renderer_impl
from great_expectations.render import RenderedAtomicContent


@pytest.fixture
def expectation_configuration_kwargs():
    # These below fields are defaults; specific tests will overwrite as deemed necessary
    return {
        "ge_cloud_id": "abcdefgh-ijkl-mnop-qrst-uvwxyz123456",
        "expectation_type": "",
        "kwargs": {},
        "meta": {},
    }


@pytest.fixture
def get_prescriptive_rendered_content(
    expectation_configuration_kwargs: Dict[str, Union[str, dict]]
) -> Callable:
    def _get_prescriptive_rendered_content(
        update_dict: Dict[str, Union[str, dict]],
    ) -> RenderedAtomicContent:
        # Overwrite any fields passed in from test and instantiate ExpectationConfiguration
        expectation_configuration_kwargs.update(update_dict)
        config = ExpectationConfiguration(**expectation_configuration_kwargs)
        expectation_type = expectation_configuration_kwargs["expectation_type"]

        # Programatically determine the renderer implementations
        renderer_impl = get_renderer_impl(
            object_name=expectation_type,
            renderer_type="atomic.prescriptive.summary",
        )[1]

        # Determine RenderedAtomicContent output
        source_obj = {"configuration": config}
        res = renderer_impl(**source_obj)
        return res

    return _get_prescriptive_rendered_content


@pytest.fixture
def evr_kwargs(expectation_configuration_kwargs):
    # These below fields are defaults; specific tests will overwrite as deemed necessary
    return {
        "expectation_config": ExpectationConfiguration(
            **expectation_configuration_kwargs
        ),
        "result": {},
    }


@pytest.fixture
def get_diagnostic_rendered_content(
    evr_kwargs: Dict[str, Union[dict, ExpectationConfiguration]]
) -> Callable:
    def _get_diagnostic_rendered_content(
        update_dict: Dict[str, Union[dict, ExpectationConfiguration]],
    ) -> RenderedAtomicContent:
        # Overwrite any fields passed in from test and instantiate ExpectationValidationResult
        evr_kwargs.update(update_dict)
        evr = ExpectationValidationResult(**evr_kwargs)
        expectation_config = evr_kwargs["expectation_config"]
        expectation_type = expectation_config["expectation_type"]

        # Programatically determine the renderer implementations
        renderer_impl = get_renderer_impl(
            object_name=expectation_type,
            renderer_type="atomic.diagnostic.observed_value",
        )[1]

        # Determine RenderedAtomicContent output
        source_obj = {"result": evr}
        res = renderer_impl(**source_obj)
        return res

    return _get_diagnostic_rendered_content


# "atomic.prescriptive.summary" tests


def test_atomic_prescriptive_summary_expect_column_bootstrapped_ks_test_p_value_to_be_greater_than(
    snapshot,
    get_prescriptive_rendered_content,
):
    # Expectation is a stub; open to implement test once renderer method is available
    pass


def test_atomic_prescriptive_summary_expect_column_chisquare_test_p_value_to_be_greater_than(
    snapshot,
    get_prescriptive_rendered_content,
):
    # Expectation is a stub; open to implement test once renderer method is available
    pass


def test_atomic_prescriptive_summary_expect_column_distinct_values_to_be_in_set(
    snapshot,
    get_prescriptive_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_column_distinct_values_to_be_in_set",
        "kwargs": {
            "column": "my_column",
            "value_set": [1, 2, 3],
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_prescriptive_summary_expect_column_distinct_values_to_contain_set(
    snapshot,
    get_prescriptive_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_column_distinct_values_to_contain_set",
        "kwargs": {
            "column": "my_column",
            "value_set": ["a", "b", "c"],
            "parse_strings_as_datetimes": True,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_prescriptive_summary_expect_column_distinct_values_to_equal_set(
    snapshot,
    get_prescriptive_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_column_distinct_values_to_equal_set",
        "kwargs": {
            "column": "my_column",
            "value_set": ["a", "b", "c"],
            "parse_strings_as_datetimes": True,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_prescriptive_summary_expect_column_kl_divergence_to_be_less_than(
    snapshot,
    get_prescriptive_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_column_kl_divergence_to_be_less_than",
        "kwargs": {
            "column": "min_event_time",
            "partition_object": {
                "bins": [0, 5, 10, 30, 50],
                "weights": [0.2, 0.3, 0.1, 0.4],
            },
            "threshold": 0.1,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)

    # replace version of vega-lite in res to match snapshot test
    res["value"]["graph"]["$schema"] = re.sub(
        r"v\d*\.\d*\.\d*", "v4.8.1", res["value"]["graph"]["$schema"]
    )

    snapshot.assert_match(res)


def test_atomic_diagnostic_observed_value_expect_column_kl_divergence_to_be_less_than(
    snapshot, get_diagnostic_rendered_content
):
    # Please note that the vast majority of Expectations are calling `Expectation._atomic_diagnostic_observed_value()`
    # As such, the specific expectation_type used here is irrelevant and is simply used to trigger the parent class.
    expectation_config = {
        "expectation_type": "expect_column_kl_divergence_to_be_less_than",
        "kwargs": {
            "column": "min_event_time",
            "partition_object": {
                "bins": [0, 5, 10, 30, 50],
                "weights": [0.2, 0.3, 0.1, 0.4],
            },
            "threshold": 0.1,
        },
        "meta": {},
        "ge_cloud_id": "4b53c4d5-90ba-467a-b7a7-379640bbd729",
    }
    update_dict = {
        "expectation_config": ExpectationConfiguration(**expectation_config),
        "result": {
            "observed_value": 0.0,
            "details": {
                "observed_partition": {
                    "values": [1, 2, 4],
                    "weights": [0.3754, 0.615, 0.0096],
                },
                "expected_partition": {
                    "values": [1, 2, 4],
                    "weights": [0.3754, 0.615, 0.0096],
                },
            },
        },
    }
    rendered_content = get_diagnostic_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)

    # replace version of vega-lite in res to match snapshot test
    res["value"]["graph"]["$schema"] = re.sub(
        r"v\d*\.\d*\.\d*", "v4.8.1", res["value"]["graph"]["$schema"]
    )
    snapshot.assert_match(res)


def test_atomic_prescriptive_summary_expect_column_max_to_be_between(
    snapshot, get_prescriptive_rendered_content
):
    update_dict = {
        "expectation_type": "expect_column_max_to_be_between",
        "kwargs": {
            "column": "my_column",
            "min_value": 1,
            "max_value": 5,
            "parse_strings_as_datetimes": True,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_prescriptive_summary_expect_column_mean_to_be_between(
    snapshot, get_prescriptive_rendered_content
):
    update_dict = {
        "expectation_type": "expect_column_mean_to_be_between",
        "kwargs": {
            "column": "my_column",
            "min_value": 3,
            "max_value": 7,
            "parse_strings_as_datetimes": True,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_prescriptive_summary_expect_column_median_to_be_between(
    snapshot, get_prescriptive_rendered_content
):
    update_dict = {
        "expectation_type": "expect_column_median_to_be_between",
        "kwargs": {
            "column": "my_column",
            "min_value": 5,
            "max_value": 10,
            "parse_strings_as_datetimes": True,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_prescriptive_summary_expect_column_min_to_be_between(
    snapshot, get_prescriptive_rendered_content
):
    update_dict = {
        "expectation_type": "expect_column_min_to_be_between",
        "kwargs": {
            "column": "my_column",
            "min_value": 1,
            "max_value": 5,
            "parse_strings_as_datetimes": True,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_prescriptive_summary_expect_column_most_common_value_to_be_in_set(
    snapshot,
    get_prescriptive_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_column_most_common_value_to_be_in_set",
        "kwargs": {
            "column": "my_column",
            "value_set": [1, 2, 3],
            "ties_okay": True,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_prescriptive_summary_expect_column_pair_cramers_phi_value_to_be_less_than(
    snapshot,
    get_prescriptive_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_column_pair_cramers_phi_value_to_be_less_than",
        "kwargs": {
            "column_A": "foo",
            "column_B": "bar",
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_prescriptive_summary_expect_column_pair_values_a_to_be_greater_than_b(
    snapshot,
    get_prescriptive_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_column_pair_values_a_to_be_greater_than_b",
        "kwargs": {
            "column_A": "foo",
            "column_B": "bar",
            "parse_strings_as_datetimes": True,
            "mostly": 0.8,
            "ignore_row_if": "baz",
            "or_equal": True,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_prescriptive_summary_expect_column_pair_values_to_be_equal(
    snapshot,
    get_prescriptive_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_column_pair_values_to_be_equal",
        "kwargs": {
            "column_A": "foo",
            "column_B": "bar",
            "mostly": 0.8,
            "ignore_row_if": "baz",
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_prescriptive_summary_expect_column_pair_values_to_be_in_set(
    snapshot,
    get_prescriptive_rendered_content,
):
    # Expectation is a stub; open to implement test once renderer method is available
    pass


def test_atomic_prescriptive_summary_expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than(
    snapshot,
    get_prescriptive_rendered_content,
):
    # Expectation is a stub; open to implement test once renderer method is available
    pass


def test_atomic_prescriptive_summary_expect_column_proportion_of_unique_values_to_be_between(
    snapshot,
    get_prescriptive_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_column_proportion_of_unique_values_to_be_between",
        "kwargs": {
            "column": "my_column",
            "min_value": 10,
            "max_value": 20,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_prescriptive_summary_expect_column_quantile_values_to_be_between(
    snapshot,
    get_prescriptive_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_column_quantile_values_to_be_between",
        "kwargs": {
            "column": "Unnamed: 0",
            "quantile_ranges": {
                "quantiles": [0.05, 0.25, 0.5, 0.75, 0.95],
                "value_ranges": [
                    [66, 68],
                    [328, 330],
                    [656, 658],
                    [984, 986],
                    [1246, 1248],
                ],
            },
            "allow_relative_error": False,
        },
        "meta": {},
        "ge_cloud_id": "cd6b4f19-8167-4984-b495-54bffcb070da",
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_diagnostic_observed_value_expect_column_quantile_values_to_be_between(
    snapshot, get_diagnostic_rendered_content
):
    # Please note that the vast majority of Expectations are calling `Expectation._atomic_diagnostic_observed_value()`
    # As such, the specific expectation_type used here is irrelevant and is simply used to trigger the parent class.
    expectation_config = {
        "expectation_type": "expect_column_quantile_values_to_be_between",
        "kwargs": {
            "column": "Unnamed: 0",
            "quantile_ranges": {
                "quantiles": [0.05, 0.25, 0.5, 0.75, 0.95],
                "value_ranges": [
                    [66, 68],
                    [328, 330],
                    [656, 658],
                    [984, 986],
                    [1246, 1248],
                ],
            },
            "allow_relative_error": False,
        },
        "meta": {},
        "ge_cloud_id": "cd6b4f19-8167-4984-b495-54bffcb070da",
    }
    update_dict = {
        "expectation_config": ExpectationConfiguration(**expectation_config),
        "result": {
            "observed_value": {
                "quantiles": [0.05, 0.25, 0.5, 0.75, 0.95],
                "values": [67, 329, 657, 985, 1247],
            },
            "element_count": 1313,
            "missing_count": None,
            "missing_percent": None,
            "details": {"success_details": [True, True, True, True, True]},
        },
    }
    rendered_content = get_diagnostic_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_prescriptive_summary_expect_column_stdev_to_be_between(
    snapshot, get_prescriptive_rendered_content
):
    update_dict = {
        "expectation_type": "expect_column_stdev_to_be_between",
        "kwargs": {
            "column": "my_column",
            "min_value": 10,
            "max_value": 20,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_prescriptive_summary_expect_column_sum_to_be_between(
    snapshot, get_prescriptive_rendered_content
):
    update_dict = {
        "expectation_type": "expect_column_sum_to_be_between",
        "kwargs": {
            "column": "my_column",
            "min_value": 10,
            "max_value": 20,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_prescriptive_summary_expect_column_to_exist(
    snapshot, get_prescriptive_rendered_content
):
    update_dict = {
        "expectation_type": "expect_column_to_exist",
        "kwargs": {
            "column": "my_column",
            "column_index": 5,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_prescriptive_summary_expect_column_unique_value_count_to_be_between(
    snapshot,
    get_prescriptive_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_column_unique_value_count_to_be_between",
        "kwargs": {
            "column": "my_column",
            "mostly": 0.8,
            "min_value": 10,
            "max_value": 20,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_prescriptive_summary_expect_column_value_lengths_to_be_between(
    snapshot,
    get_prescriptive_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_column_value_lengths_to_be_between",
        "kwargs": {
            "column": "my_column",
            "mostly": 0.8,
            "min_value": 10,
            "max_value": 20,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_prescriptive_summary_expect_column_value_lengths_to_equal(
    snapshot, get_prescriptive_rendered_content
):
    update_dict = {
        "expectation_type": "expect_column_value_lengths_to_equal",
        "kwargs": {
            "column": "my_column",
            "value": 100,
            "mostly": 0.8,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_prescriptive_summary_expect_column_value_z_scores_to_be_less_than(
    snapshot,
    get_prescriptive_rendered_content,
):
    # Expectation is a stub; open to implement test once renderer method is available
    pass


def test_atomic_prescriptive_summary_expect_column_values_to_be_between(
    snapshot, get_prescriptive_rendered_content
):
    update_dict = {
        "expectation_type": "expect_column_values_to_be_between",
        "kwargs": {
            "column": "my_column",
            "mostly": 0.8,
            "min_value": 1,
            "max_value": 5,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_prescriptive_summary_expect_column_values_to_be_dateutil_parseable(
    snapshot,
    get_prescriptive_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_column_values_to_be_dateutil_parseable",
        "kwargs": {
            "column": "my_column",
            "mostly": 0.8,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_prescriptive_summary_expect_column_values_to_be_decreasing(
    snapshot,
    get_prescriptive_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_column_values_to_be_decreasing",
        "kwargs": {
            "column": "my_column",
            "mostly": 0.8,
            "parse_strings_as_datetimes": True,
            "strictly": 50,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_prescriptive_summary_expect_column_values_to_be_in_set(
    snapshot, get_prescriptive_rendered_content
):
    update_dict = {
        "expectation_type": "expect_column_values_to_be_in_set",
        "kwargs": {
            "column": "my_column",
            "value_set": [1, 2, 3, 4],
            "mostly": 0.8,
            "parse_strings_as_datetimes": True,
            "strictly": 50,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_prescriptive_summary_expect_column_values_to_be_in_type_list(
    snapshot,
    get_prescriptive_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_column_values_to_be_in_type_list",
        "kwargs": {
            "column": "my_column",
            "type_list": ["type_a", "type_b", "type_c"],
            "value_set": [1, 2, 3, 4],
            "mostly": 0.8,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_prescriptive_summary_expect_column_values_to_be_increasing(
    snapshot,
    get_prescriptive_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_column_values_to_be_increasing",
        "kwargs": {
            "column": "my_column",
            "strictly": 10,
            "mostly": 0.8,
            "parse_strings_as_datetimes": True,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_prescriptive_summary_expect_column_values_to_be_json_parseable(
    snapshot,
    get_prescriptive_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_column_values_to_be_json_parseable",
        "kwargs": {
            "column": "my_column",
            "mostly": 0.8,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_prescriptive_summary_expect_column_values_to_be_null(
    snapshot, get_prescriptive_rendered_content
):
    update_dict = {
        "expectation_type": "expect_column_values_to_be_null",
        "kwargs": {
            "column": "my_column",
            "mostly": 0.8,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_prescriptive_summary_expect_column_values_to_be_null_with_mostly_equals_1(
    snapshot, get_prescriptive_rendered_content
):
    update_dict = {
        "expectation_type": "expect_column_values_to_be_null",
        "kwargs": {
            "column": "my_column",
            "mostly": 1.0,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_prescriptive_summary_expect_column_values_to_be_of_type(
    snapshot, get_prescriptive_rendered_content
):
    update_dict = {
        "expectation_type": "expect_column_values_to_be_of_type",
        "kwargs": {
            "column": "my_column",
            "type_": "my_type",
            "mostly": 0.8,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_prescriptive_summary_expect_column_values_to_be_unique(
    snapshot, get_prescriptive_rendered_content
):
    update_dict = {
        "expectation_type": "expect_column_values_to_be_unique",
        "kwargs": {
            "column": "my_column",
            "mostly": 0.8,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_prescriptive_summary_expect_column_values_to_match_json_schema(
    snapshot,
    get_prescriptive_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_column_values_to_match_json_schema",
        "kwargs": {
            "column": "my_column",
            "mostly": 0.8,
            "json_schema": {"foo": "bar"},
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_prescriptive_summary_expect_column_values_to_match_like_pattern(
    snapshot,
    get_prescriptive_rendered_content,
):
    # Expectation is a stub; open to implement test once renderer method is available
    pass


def test_atomic_prescriptive_summary_expect_column_values_to_match_like_pattern_list(
    snapshot,
    get_prescriptive_rendered_content,
):
    # Expectation is a stub; open to implement test once renderer method is available
    pass


def test_atomic_prescriptive_summary_expect_column_values_to_match_regex(
    snapshot, get_prescriptive_rendered_content
):
    update_dict = {
        "expectation_type": "expect_column_values_to_match_regex",
        "kwargs": {
            "column": "my_column",
            "mostly": 0.8,
            "regex": "^superconductive$",
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_prescriptive_summary_expect_column_values_to_match_regex_list(
    snapshot,
    get_prescriptive_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_column_values_to_match_regex_list",
        "kwargs": {
            "column": "my_column",
            "regex_list": ["^superconductive$", "ge|great_expectations"],
            "mostly": 0.8,
            "match_on": "all",
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_prescriptive_summary_expect_column_values_to_match_strftime_format(
    snapshot,
    get_prescriptive_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_column_values_to_match_strftime_format",
        "kwargs": {
            "column": "my_column",
            "strftime_format": "%Y-%m",
            "mostly": 0.8,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_prescriptive_summary_expect_column_values_to_not_be_in_set(
    snapshot,
    get_prescriptive_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_column_values_to_not_be_in_set",
        "kwargs": {
            "column": "my_column",
            "value_set": [1, 2, 3],
            "mostly": 0.8,
            "parse_string_as_datetimes": True,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_prescriptive_summary_expect_column_values_to_not_be_null(
    snapshot, get_prescriptive_rendered_content
):
    update_dict = {
        "expectation_type": "expect_column_values_to_not_be_null",
        "kwargs": {
            "column": "my_column",
            "mostly": 0.8,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_prescriptive_summary_expect_column_values_to_not_match_like_pattern(
    snapshot,
    get_prescriptive_rendered_content,
):
    # Expectation is a stub; open to implement test once renderer method is available
    pass


def test_atomic_prescriptive_summary_expect_column_values_to_not_match_like_pattern_list(
    snapshot,
    get_prescriptive_rendered_content,
):
    # Expectation is a stub; open to implement test once renderer method is available
    pass


def test_atomic_prescriptive_summary_expect_column_values_to_not_match_regex(
    snapshot,
    get_prescriptive_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_column_values_to_not_match_regex",
        "kwargs": {
            "column": "my_column",
            "regex": "^superconductive$",
            "mostly": 0.8,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_prescriptive_summary_expect_column_values_to_not_match_regex_list(
    snapshot,
    get_prescriptive_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_column_values_to_not_match_regex_list",
        "kwargs": {
            "column": "my_column",
            "regex_list": ["^a", "^b", "^c"],
            "mostly": 0.8,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_prescriptive_summary_expect_compound_columns_to_be_unique(
    snapshot, get_prescriptive_rendered_content
):
    update_dict = {
        "expectation_type": "expect_compound_columns_to_be_unique",
        "kwargs": {
            "column_list": ["my_first_col", "my_second_col", "my_third_col"],
            "mostly": 0.8,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_prescriptive_summary_expect_multicolumn_sum_to_equal(
    snapshot, get_prescriptive_rendered_content
):
    # Expectation is a stub; open to implement test once renderer method is available
    pass


def test_atomic_prescriptive_summary_expect_multicolumn_values_to_be_unique(
    snapshot,
    get_prescriptive_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_multicolumn_values_to_be_unique",
        "kwargs": {
            "column_list": ["A", "B", "C"],
            "ignore_row_if": "foo",
            "mostly": 0.8,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_prescriptive_summary_expect_select_column_values_to_be_unique_within_record(
    snapshot,
    get_prescriptive_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_select_column_values_to_be_unique_within_record",
        "kwargs": {
            "column_list": ["my_first_column", "my_second_column"],
            "mostly": 0.8,
            "ignore_row_if": "foo",
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_prescriptive_summary_expect_table_column_count_to_be_between(
    snapshot,
    get_prescriptive_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_table_column_count_to_be_between",
        "kwargs": {
            "min_value": 5,
            "max_value": None,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_prescriptive_summary_expect_table_column_count_to_equal(
    snapshot, get_prescriptive_rendered_content
):
    update_dict = {
        "expectation_type": "expect_table_column_count_to_equal",
        "kwargs": {
            "value": 10,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_prescriptive_summary_expect_table_columns_to_match_ordered_list(
    snapshot,
    get_prescriptive_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_table_columns_to_match_ordered_list",
        "kwargs": {"column_list": ["a", "b", "c"]},
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_prescriptive_summary_expect_table_columns_to_match_set(
    snapshot, get_prescriptive_rendered_content
):
    update_dict = {
        "expectation_type": "expect_table_columns_to_match_set",
        "kwargs": {
            "column_set": ["a", "b", "c"],
            "exact_match": True,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_prescriptive_summary_expect_table_row_count_to_be_between(
    snapshot, get_prescriptive_rendered_content
):
    update_dict = {
        "expectation_type": "expect_table_row_count_to_be_between",
        "kwargs": {"max_value": None, "min_value": 1},
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_prescriptive_summary_expect_table_row_count_to_equal(
    snapshot, get_prescriptive_rendered_content
):
    update_dict = {
        "expectation_type": "expect_table_row_count_to_equal",
        "kwargs": {"value": 10},
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_prescriptive_summary_expect_table_row_count_to_equal_other_table(
    snapshot,
    get_prescriptive_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_table_row_count_to_equal_other_table",
        "kwargs": {
            "other_table_name": {
                "schema": {"type": "string"},
                "value": "other_table_name",
            }
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


# "atomic.diagnostic.observed_value" tests


def test_atomic_diagnostic_observed_value_without_result(
    snapshot, get_diagnostic_rendered_content
):
    # Please note that the vast majority of Expectations are calling `Expectation._atomic_diagnostic_observed_value()`
    # As such, the specific expectation_type used here is irrelevant and is simply used to trigger the parent class.
    expectation_config = {
        "expectation_type": "expect_table_row_count_to_equal",
        "kwargs": {},
    }
    update_dict = {
        "expectation_config": ExpectationConfiguration(**expectation_config),
    }
    rendered_content = get_diagnostic_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_diagnostic_observed_value_with_numeric_observed_value(
    snapshot, get_diagnostic_rendered_content
):
    # Please note that the vast majority of Expectations are calling `Expectation._atomic_diagnostic_observed_value()`
    # As such, the specific expectation_type used here is irrelevant and is simply used to trigger the parent class.
    expectation_config = {
        "expectation_type": "expect_table_row_count_to_equal",
        "kwargs": {},
    }
    update_dict = {
        "expectation_config": ExpectationConfiguration(**expectation_config),
        "result": {"observed_value": 1776},
    }
    rendered_content = get_diagnostic_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_diagnostic_observed_value_with_str_observed_value(
    snapshot, get_diagnostic_rendered_content
):
    # Please note that the vast majority of Expectations are calling `Expectation._atomic_diagnostic_observed_value()`
    # As such, the specific expectation_type used here is irrelevant and is simply used to trigger the parent class.
    expectation_config = {
        "expectation_type": "expect_table_row_count_to_equal",
        "kwargs": {},
    }
    update_dict = {
        "expectation_config": ExpectationConfiguration(**expectation_config),
        "result": {"observed_value": "foo"},
    }
    rendered_content = get_diagnostic_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_diagnostic_observed_value_with_unexpected_percent(
    snapshot, get_diagnostic_rendered_content
):
    # Please note that the vast majority of Expectations are calling `Expectation._atomic_diagnostic_observed_value()`
    # As such, the specific expectation_type used here is irrelevant and is simply used to trigger the parent class.
    expectation_config = {
        "expectation_type": "expect_table_row_count_to_equal",
        "kwargs": {},
    }
    update_dict = {
        "expectation_config": ExpectationConfiguration(**expectation_config),
        "result": {"unexpected_percent": 10},
    }
    rendered_content = get_diagnostic_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_diagnostic_observed_value_with_empty_result(
    snapshot, get_diagnostic_rendered_content
):
    # Please note that the vast majority of Expectations are calling `Expectation._atomic_diagnostic_observed_value()`
    # As such, the specific expectation_type used here is irrelevant and is simply used to trigger the parent class.
    expectation_config = {
        "expectation_type": "expect_table_row_count_to_equal",
        "kwargs": {},
    }
    update_dict = {
        "expectation_config": ExpectationConfiguration(**expectation_config),
        "result": {},
    }
    rendered_content = get_diagnostic_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)
