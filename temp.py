from pprint import pprint
from typing import Callable, Dict, Union

import pytest
from typing_extensions import Literal

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.expectations.registry import get_renderer_impl
from great_expectations.render.types import RenderedAtomicContent


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
def get_rendered_content(
    expectation_configuration_kwargs: Dict[str, Union[str, dict]]
) -> Callable:
    def _get_rendered_content(
        renderer_type: Union[
            Literal["atomic.prescriptive.summary"],
            Literal["atomic.descriptive.summary"],
            Literal["atomic.diagnostic.summary"],
        ],
        update_dict: Dict[str, Union[str, dict]],
    ) -> RenderedAtomicContent:
        # Overwrite any fields passed in from test and instantiate ExpectationConfiguration
        expectation_configuration_kwargs.update(update_dict)
        config = ExpectationConfiguration(**expectation_configuration_kwargs)

        # Programatically determine the renderer implementations
        renderer_impl = get_renderer_impl(
            object_name=expectation_configuration_kwargs["expectation_type"],
            renderer_type=renderer_type,
        )[1]

        # Determine output list of RenderedAtomicContent
        source_obj = {"configuration": config}
        res = renderer_impl(**source_obj)
        return res

    return _get_rendered_content


def test_atomic_diagnostic_expect_column_bootstrapped_ks_test_p_value_to_be_greater_than(
    snapshot,
    get_rendered_content,
):
    # Expectation is a stub; open to implement test once renderer method is available
    pass


def test_atomic_diagnostic_expect_column_chisquare_test_p_value_to_be_greater_than(
    snapshot,
    get_rendered_content,
):
    # Expectation is a stub; open to implement test once renderer method is available
    pass


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_distinct_values_to_be_in_set(
    snapshot,
    get_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_column_distinct_values_to_be_in_set",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_distinct_values_to_contain_set(
    snapshot,
    get_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_column_distinct_values_to_contain_set",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_distinct_values_to_equal_set(
    snapshot,
    get_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_column_distinct_values_to_equal_set",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_kl_divergence_to_be_less_than(
    snapshot,
    get_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_column_kl_divergence_to_be_less_than",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_max_to_be_between(
    snapshot, get_rendered_content
):
    update_dict = {
        "expectation_type": "expect_column_max_to_be_between",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_mean_to_be_between(
    snapshot, get_rendered_content
):
    update_dict = {
        "expectation_type": "expect_column_mean_to_be_between",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_median_to_be_between(
    snapshot, get_rendered_content
):
    update_dict = {
        "expectation_type": "expect_column_median_to_be_between",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_min_to_be_between(
    snapshot, get_rendered_content
):
    update_dict = {
        "expectation_type": "expect_column_min_to_be_between",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_most_common_value_to_be_in_set(
    snapshot,
    get_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_column_most_common_value_to_be_in_set",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_pair_cramers_phi_value_to_be_less_than(
    snapshot,
    get_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_column_pair_cramers_phi_value_to_be_less_than",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_pair_values_a_to_be_greater_than_b(
    snapshot,
    get_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_column_pair_values_a_to_be_greater_than_b",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_pair_values_to_be_equal(
    snapshot, get_rendered_content
):
    update_dict = {
        "expectation_type": "expect_column_pair_values_to_be_equal",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_diagnostic_expect_column_pair_values_to_be_in_set(
    snapshot, get_rendered_content
):
    # Expectation is a stub; open to implement test once renderer method is available
    pass


def test_atomic_diagnostic_expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than(
    snapshot,
    get_rendered_content,
):
    # Expectation is a stub; open to implement test once renderer method is available
    pass


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_proportion_of_unique_values_to_be_between(
    snapshot,
    get_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_column_proportion_of_unique_values_to_be_between",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_quantile_values_to_be_between(
    snapshot,
    get_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_column_quantile_values_to_be_between",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_stdev_to_be_between(
    snapshot, get_rendered_content
):
    update_dict = {
        "expectation_type": "expect_column_stdev_to_be_between",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_sum_to_be_between(
    snapshot, get_rendered_content
):
    update_dict = {
        "expectation_type": "expect_column_sum_to_be_between",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_to_exist(snapshot, get_rendered_content):
    update_dict = {
        "expectation_type": "expect_column_to_exist",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_unique_value_count_to_be_between(
    snapshot,
    get_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_column_unique_value_count_to_be_between",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_value_lengths_to_be_between(
    snapshot,
    get_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_column_value_lengths_to_be_between",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_value_lengths_to_equal(
    snapshot, get_rendered_content
):
    update_dict = {
        "expectation_type": "expect_column_value_lengths_to_equal",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_diagnostic_expect_column_value_z_scores_to_be_less_than(
    snapshot,
    get_rendered_content,
):
    # Expectation is a stub; open to implement test once renderer method is available
    pass


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_values_to_be_between(
    snapshot, get_rendered_content
):
    update_dict = {
        "expectation_type": "expect_column_values_to_be_between",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_values_to_be_dateutil_parseable(
    snapshot,
    get_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_column_values_to_be_dateutil_parseable",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_values_to_be_decreasing(
    snapshot, get_rendered_content
):
    update_dict = {
        "expectation_type": "expect_column_values_to_be_decreasing",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_values_to_be_in_set(
    snapshot, get_rendered_content
):
    update_dict = {
        "expectation_type": "expect_column_values_to_be_in_set",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_values_to_be_in_type_list(
    snapshot,
    get_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_column_values_to_be_in_type_list",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_values_to_be_increasing(
    snapshot, get_rendered_content
):
    update_dict = {
        "expectation_type": "expect_column_values_to_be_increasing",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_values_to_be_json_parseable(
    snapshot,
    get_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_column_values_to_be_json_parseable",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_values_to_be_null(
    snapshot, get_rendered_content
):
    update_dict = {
        "expectation_type": "expect_column_values_to_be_null",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_values_to_be_of_type(
    snapshot, get_rendered_content
):
    update_dict = {
        "expectation_type": "expect_column_values_to_be_of_type",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_values_to_be_unique(
    snapshot, get_rendered_content
):
    update_dict = {
        "expectation_type": "expect_column_values_to_be_unique",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_values_to_match_json_schema(
    snapshot,
    get_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_column_values_to_match_json_schema",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_diagnostic_expect_column_values_to_match_like_pattern(
    snapshot,
    get_rendered_content,
):
    # Expectation is a stub; open to implement test once renderer method is available
    pass


def test_atomic_diagnostic_expect_column_values_to_match_like_pattern_list(
    snapshot,
    get_rendered_content,
):
    # Expectation is a stub; open to implement test once renderer method is available
    pass


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_values_to_match_regex(
    snapshot, get_rendered_content
):
    update_dict = {
        "expectation_type": "expect_column_values_to_match_regex",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_values_to_match_regex_list(
    snapshot,
    get_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_column_values_to_match_regex_list",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_values_to_match_strftime_format(
    snapshot,
    get_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_column_values_to_match_strftime_format",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_values_to_not_be_in_set(
    snapshot, get_rendered_content
):
    update_dict = {
        "expectation_type": "expect_column_values_to_not_be_in_set",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_values_to_not_be_null(
    snapshot, get_rendered_content
):
    update_dict = {
        "expectation_type": "expect_column_values_to_not_be_null",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_diagnostic_expect_column_values_to_not_match_like_pattern(
    snapshot,
    get_rendered_content,
):
    # Expectation is a stub; open to implement test once renderer method is available
    pass


def test_atomic_diagnostic_expect_column_values_to_not_match_like_pattern_list(
    snapshot,
    get_rendered_content,
):
    # Expectation is a stub; open to implement test once renderer method is available
    pass


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_values_to_not_match_regex(
    snapshot,
    get_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_column_values_to_not_match_regex",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_values_to_not_match_regex_list(
    snapshot,
    get_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_column_values_to_not_match_regex_list",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_compound_columns_to_be_unique(
    snapshot, get_rendered_content
):
    update_dict = {
        "expectation_type": "expect_compound_columns_to_be_unique",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_diagnostic_expect_multicolumn_sum_to_equal(
    snapshot, get_rendered_content
):
    # Expectation is a stub; open to implement test once renderer method is available
    pass


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_multicolumn_values_to_be_unique(
    snapshot, get_rendered_content
):
    update_dict = {
        "expectation_type": "expect_multicolumn_values_to_be_unique",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_select_column_values_to_be_unique_within_record(
    snapshot,
    get_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_select_column_values_to_be_unique_within_record",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_table_column_count_to_be_between(
    snapshot,
    get_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_table_column_count_to_be_between",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_table_column_count_to_equal(
    snapshot, get_rendered_content
):
    update_dict = {
        "expectation_type": "expect_table_column_count_to_equal",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_table_columns_to_match_ordered_list(
    snapshot,
    get_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_table_columns_to_match_ordered_list",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_table_columns_to_match_set(
    snapshot, get_rendered_content
):
    update_dict = {
        "expectation_type": "expect_table_columns_to_match_set",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_table_row_count_to_be_between(
    snapshot, get_rendered_content
):
    update_dict = {
        "expectation_type": "expect_table_row_count_to_be_between",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_table_row_count_to_equal(
    snapshot, get_rendered_content
):
    update_dict = {
        "expectation_type": "expect_table_row_count_to_equal",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_table_row_count_to_equal_other_table(
    snapshot,
    get_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_table_row_count_to_equal_other_table",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_prescriptive_expect_column_bootstrapped_ks_test_p_value_to_be_greater_than(
    snapshot,
    get_rendered_content,
):
    # Expectation is a stub; open to implement test once renderer method is available
    pass


def test_atomic_prescriptive_expect_column_chisquare_test_p_value_to_be_greater_than(
    snapshot,
    get_rendered_content,
):
    # Expectation is a stub; open to implement test once renderer method is available
    pass


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_column_distinct_values_to_be_in_set(
    snapshot,
    get_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_column_distinct_values_to_be_in_set",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_column_distinct_values_to_contain_set(
    snapshot,
    get_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_column_distinct_values_to_contain_set",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_column_distinct_values_to_equal_set(
    snapshot,
    get_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_column_distinct_values_to_equal_set",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_column_kl_divergence_to_be_less_than(
    snapshot,
    get_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_column_kl_divergence_to_be_less_than",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_column_max_to_be_between(
    snapshot, get_rendered_content
):
    update_dict = {
        "expectation_type": "expect_column_max_to_be_between",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_column_mean_to_be_between(
    snapshot, get_rendered_content
):
    update_dict = {
        "expectation_type": "expect_column_mean_to_be_between",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_column_median_to_be_between(
    snapshot, get_rendered_content
):
    update_dict = {
        "expectation_type": "expect_column_median_to_be_between",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_column_min_to_be_between(
    snapshot, get_rendered_content
):
    update_dict = {
        "expectation_type": "expect_column_min_to_be_between",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_column_most_common_value_to_be_in_set(
    snapshot,
    get_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_column_most_common_value_to_be_in_set",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_column_pair_cramers_phi_value_to_be_less_than(
    snapshot,
    get_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_column_pair_cramers_phi_value_to_be_less_than",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_column_pair_values_a_to_be_greater_than_b(
    snapshot,
    get_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_column_pair_values_a_to_be_greater_than_b",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_column_pair_values_to_be_equal(
    snapshot,
    get_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_column_pair_values_to_be_equal",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_prescriptive_expect_column_pair_values_to_be_in_set(
    snapshot,
    get_rendered_content,
):
    # Expectation is a stub; open to implement test once renderer method is available
    pass


def test_atomic_prescriptive_expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than(
    snapshot,
    get_rendered_content,
):
    # Expectation is a stub; open to implement test once renderer method is available
    pass


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_column_proportion_of_unique_values_to_be_between(
    snapshot,
    get_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_column_proportion_of_unique_values_to_be_between",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_column_quantile_values_to_be_between(
    snapshot,
    get_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_column_quantile_values_to_be_between",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_column_stdev_to_be_between(
    snapshot, get_rendered_content
):
    update_dict = {
        "expectation_type": "expect_column_stdev_to_be_between",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_column_sum_to_be_between(
    snapshot, get_rendered_content
):
    update_dict = {
        "expectation_type": "expect_column_sum_to_be_between",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_column_to_exist(snapshot, get_rendered_content):
    update_dict = {
        "expectation_type": "expect_column_to_exist",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_column_unique_value_count_to_be_between(
    snapshot,
    get_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_column_unique_value_count_to_be_between",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_column_value_lengths_to_be_between(
    snapshot,
    get_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_column_value_lengths_to_be_between",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_column_value_lengths_to_equal(
    snapshot, get_rendered_content
):
    update_dict = {
        "expectation_type": "expect_column_value_lengths_to_equal",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_column_value_z_scores_to_be_less_than(
    snapshot,
    get_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_column_value_z_scores_to_be_less_than",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_prescriptive_expect_column_values_to_be_between(
    snapshot, get_rendered_content
):
    # Expectation is a stub; open to implement test once renderer method is available
    pass


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_column_values_to_be_dateutil_parseable(
    snapshot,
    get_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_column_values_to_be_dateutil_parseable",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_prescriptive_expect_column_values_to_be_decreasing(
    snapshot,
    get_rendered_content,
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
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_prescriptive_expect_column_values_to_be_in_set(
    snapshot, get_rendered_content
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
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_prescriptive_expect_column_values_to_be_in_type_list(
    snapshot,
    get_rendered_content,
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
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_prescriptive_expect_column_values_to_be_increasing(
    snapshot,
    get_rendered_content,
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
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_prescriptive_expect_column_values_to_be_json_parseable(
    snapshot,
    get_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_column_values_to_be_json_parseable",
        "kwargs": {
            "column": "my_column",
            "mostly": 0.8,
        },
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_prescriptive_expect_column_values_to_be_null(
    snapshot, get_rendered_content
):
    update_dict = {
        "expectation_type": "expect_column_values_to_be_null",
        "kwargs": {
            "column": "my_column",
            "mostly": 0.8,
        },
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_prescriptive_expect_column_values_to_be_of_type(
    snapshot, get_rendered_content
):
    update_dict = {
        "expectation_type": "expect_column_values_to_be_of_type",
        "kwargs": {
            "column": "my_column",
            "type_": "my_type",
            "mostly": 0.8,
        },
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_prescriptive_expect_column_values_to_be_unique(
    snapshot, get_rendered_content
):
    update_dict = {
        "expectation_type": "expect_column_values_to_be_unique",
        "kwargs": {
            "column": "my_column",
            "mostly": 0.8,
        },
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_prescriptive_expect_column_values_to_match_json_schema(
    snapshot,
    get_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_column_values_to_match_json_schema",
        "kwargs": {
            "column": "my_column",
            "mostly": 0.8,
            "json_schema": {"foo": "bar"},
        },
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_prescriptive_expect_column_values_to_match_like_pattern(
    snapshot,
    get_rendered_content,
):
    # Expectation is a stub; open to implement test once renderer method is available
    pass


def test_atomic_prescriptive_expect_column_values_to_match_like_pattern_list(
    snapshot,
    get_rendered_content,
):
    # Expectation is a stub; open to implement test once renderer method is available
    pass


def test_atomic_prescriptive_expect_column_values_to_match_regex(
    snapshot, get_rendered_content
):
    update_dict = {
        "expectation_type": "expect_column_values_to_match_regex",
        "kwargs": {
            "column": "my_column",
            "mostly": 0.8,
            "regex": "^superconductive$",
        },
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_column_values_to_match_regex_list(
    snapshot,
    get_rendered_content,
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
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_prescriptive_expect_column_values_to_match_strftime_format(
    snapshot,
    get_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_column_values_to_match_strftime_format",
        "kwargs": {
            "column": "my_column",
            "strftime_format": "%Y-%m",
            "mostly": 0.8,
        },
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_prescriptive_expect_column_values_to_not_be_in_set(
    snapshot,
    get_rendered_content,
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
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_prescriptive_expect_column_values_to_not_be_null(
    snapshot, get_rendered_content
):
    update_dict = {
        "expectation_type": "expect_column_values_to_not_be_null",
        "kwargs": {
            "column": "my_column",
            "mostly": 0.8,
        },
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_prescriptive_expect_column_values_to_not_match_like_pattern(
    snapshot,
    get_rendered_content,
):
    # Expectation is a stub; open to implement test once renderer method is available
    pass


def test_atomic_prescriptive_expect_column_values_to_not_match_like_pattern_list(
    snapshot,
    get_rendered_content,
):
    # Expectation is a stub; open to implement test once renderer method is available
    pass


def test_atomic_prescriptive_expect_column_values_to_not_match_regex(
    snapshot,
    get_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_column_values_to_not_match_regex",
        "kwargs": {
            "column": "my_column",
            "regex": "^superconductive$",
            "mostly": 0.8,
        },
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_column_values_to_not_match_regex_list(
    snapshot,
    get_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_column_values_to_not_match_regex_list",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_prescriptive_expect_compound_columns_to_be_unique(
    snapshot, get_rendered_content
):
    update_dict = {
        "expectation_type": "expect_compound_columns_to_be_unique",
        "kwargs": {
            "column_list": ["my_first_col", "my_second_col", "my_third_col"],
            "mostly": 0.8,
        },
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_prescriptive_expect_multicolumn_sum_to_equal(
    snapshot, get_rendered_content
):
    # Expectation is a stub; open to implement test once renderer method is available
    pass


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_multicolumn_values_to_be_unique(
    snapshot,
    get_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_multicolumn_values_to_be_unique",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_select_column_values_to_be_unique_within_record(
    snapshot,
    get_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_select_column_values_to_be_unique_within_record",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_prescriptive_expect_table_column_count_to_be_between(
    snapshot,
    get_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_table_column_count_to_be_between",
        "kwargs": {
            "min_value": 5,
            "max_value": None,
        },
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_table_column_count_to_equal(
    snapshot, get_rendered_content
):
    update_dict = {
        "expectation_type": "expect_table_column_count_to_equal",
        "kwargs": {
            "value": 10,
        },
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_table_columns_to_match_ordered_list(
    snapshot,
    get_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_table_columns_to_match_ordered_list",
        "kwargs": {"column_list": ["a", "b", "c"]},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_prescriptive_expect_table_columns_to_match_set(
    snapshot, get_rendered_content
):
    update_dict = {
        "expectation_type": "expect_table_columns_to_match_set",
        "kwargs": {
            "column_set": ["a", "b", "c"],
            "exact_match": True,
        },
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_prescriptive_expect_table_row_count_to_be_between(
    snapshot, get_rendered_content
):
    update_dict = {
        "expectation_type": "expect_table_row_count_to_be_between",
        "kwargs": {"max_value": None, "min_value": 1},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_prescriptive_expect_table_row_count_to_equal(
    snapshot, get_rendered_content
):
    update_dict = {
        "expectation_type": "expect_table_row_count_to_equal",
        "kwargs": {"value": 10},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)


def test_atomic_prescriptive_expect_table_row_count_to_equal_other_table(
    snapshot,
    get_rendered_content,
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
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    snapshot.assert_match(res)
