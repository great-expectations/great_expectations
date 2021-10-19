from pprint import pprint
from typing import Callable, Dict, List, Union

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
    ) -> List[RenderedAtomicContent]:
        # Overwrite any fields passed in from test and instantiate ExpectationConfiguration
        expectation_configuration_kwargs.update(update_dict)
        config = ExpectationConfiguration(**expectation_configuration_kwargs)

        # Programatically determine the renderer implementations
        renderer_impls = [
            get_renderer_impl(
                object_name=expectation_configuration_kwargs["expectation_type"],
                renderer_type=renderer_type,
            )[1]
        ]

        # Determine output list of RenderedAtomicContent
        rendered_list = []
        for renderer_fn in renderer_impls:
            source_obj = {"configuration": config}
            res = renderer_fn(**source_obj)
            rendered_list.append(res)

        return rendered_list

    return _get_rendered_content


def test_atomic_diagnostic_expect_column_bootstrapped_ks_test_p_value_to_be_greater_than(
    get_rendered_content,
):
    # Expectation is a stub; open to implement test once renderer method is available
    pass


def test_atomic_diagnostic_expect_column_chisquare_test_p_value_to_be_greater_than(
    get_rendered_content,
):
    # Expectation is a stub; open to implement test once renderer method is available
    pass


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_distinct_values_to_be_in_set(
    get_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_column_distinct_values_to_be_in_set",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_distinct_values_to_contain_set(
    get_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_column_distinct_values_to_contain_set",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_distinct_values_to_equal_set(
    get_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_column_distinct_values_to_equal_set",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_kl_divergence_to_be_less_than(
    get_rendered_content,
):
    update_dict = {
        "expectation_type": "expect_column_kl_divergence_to_be_less_than",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_max_to_be_between(get_rendered_content):
    update_dict = {
        "expectation_type": "expect_column_max_to_be_between",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_mean_to_be_between(get_rendered_content):
    update_dict = {
        "expectation_type": "expect_column_mean_to_be_between",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_median_to_be_between(get_rendered_content):
    update_dict = {
        "expectation_type": "expect_column_median_to_be_between",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_min_to_be_between(get_rendered_content):
    pass
    update_dict = {
        "expectation_type": "expect_column_min_to_be_between",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_most_common_value_to_be_in_set(
    get_rendered_content,
):
    pass
    update_dict = {
        "expectation_type": "expect_column_most_common_value_to_be_in_set",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_pair_cramers_phi_value_to_be_less_than(
    get_rendered_content,
):
    pass
    update_dict = {
        "expectation_type": "expect_column_pair_cramers_phi_value_to_be_less_than",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_pair_values_a_to_be_greater_than_b(
    get_rendered_content,
):
    pass
    update_dict = {
        "expectation_type": "expect_column_pair_values_a_to_be_greater_than_b",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_pair_values_to_be_equal(get_rendered_content):
    pass
    update_dict = {
        "expectation_type": "expect_column_pair_values_to_be_equal",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


def test_atomic_diagnostic_expect_column_pair_values_to_be_in_set(get_rendered_content):
    # Expectation is a stub; open to implement test once renderer method is available
    pass


def test_atomic_diagnostic_expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than(
    get_rendered_content,
):
    # Expectation is a stub; open to implement test once renderer method is available
    pass


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_proportion_of_unique_values_to_be_between(
    get_rendered_content,
):
    pass
    update_dict = {
        "expectation_type": "expect_column_proportion_of_unique_values_to_be_between",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_quantile_values_to_be_between(
    get_rendered_content,
):
    pass
    update_dict = {
        "expectation_type": "expect_column_quantile_values_to_be_between",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_stdev_to_be_between(get_rendered_content):
    pass
    update_dict = {
        "expectation_type": "expect_column_stdev_to_be_between",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_sum_to_be_between(get_rendered_content):
    pass
    update_dict = {
        "expectation_type": "expect_column_sum_to_be_between",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_to_exist(get_rendered_content):
    pass
    update_dict = {
        "expectation_type": "expect_column_to_exist",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_unique_value_count_to_be_between(
    get_rendered_content,
):
    pass
    update_dict = {
        "expectation_type": "expect_column_unique_value_count_to_be_between",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_value_lengths_to_be_between(
    get_rendered_content,
):
    pass
    update_dict = {
        "expectation_type": "expect_column_value_lengths_to_be_between",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_value_lengths_to_equal(get_rendered_content):
    pass
    update_dict = {
        "expectation_type": "expect_column_value_lengths_to_equal",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


def test_atomic_diagnostic_expect_column_value_z_scores_to_be_less_than(
    get_rendered_content,
):
    # Expectation is a stub; open to implement test once renderer method is available
    pass


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_values_to_be_between(get_rendered_content):
    pass
    update_dict = {
        "expectation_type": "expect_column_values_to_be_between",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_values_to_be_dateutil_parseable(
    get_rendered_content,
):
    pass
    update_dict = {
        "expectation_type": "expect_column_values_to_be_dateutil_parseable",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_values_to_be_decreasing(get_rendered_content):
    pass
    update_dict = {
        "expectation_type": "expect_column_values_to_be_decreasing",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_values_to_be_in_set(get_rendered_content):
    pass
    update_dict = {
        "expectation_type": "expect_column_values_to_be_in_set",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_values_to_be_in_type_list(
    get_rendered_content,
):
    pass
    update_dict = {
        "expectation_type": "expect_column_values_to_be_in_type_list",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_values_to_be_increasing(get_rendered_content):
    pass
    update_dict = {
        "expectation_type": "expect_column_values_to_be_increasing",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_values_to_be_json_parseable(
    get_rendered_content,
):
    pass
    update_dict = {
        "expectation_type": "expect_column_values_to_be_json_parseable",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_values_to_be_null(get_rendered_content):
    pass
    update_dict = {
        "expectation_type": "expect_column_values_to_be_null",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_values_to_be_of_type(get_rendered_content):
    pass
    update_dict = {
        "expectation_type": "expect_column_values_to_be_of_type",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_values_to_be_unique(get_rendered_content):
    pass
    update_dict = {
        "expectation_type": "expect_column_values_to_be_unique",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_values_to_match_json_schema(
    get_rendered_content,
):
    pass
    update_dict = {
        "expectation_type": "expect_column_values_to_match_json_schema",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


def test_atomic_diagnostic_expect_column_values_to_match_like_pattern(
    get_rendered_content,
):
    # Expectation is a stub; open to implement test once renderer method is available
    pass


def test_atomic_diagnostic_expect_column_values_to_match_like_pattern_list(
    get_rendered_content,
):
    # Expectation is a stub; open to implement test once renderer method is available
    pass


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_values_to_match_regex(get_rendered_content):
    pass
    update_dict = {
        "expectation_type": "expect_column_values_to_match_regex",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_values_to_match_regex_list(
    get_rendered_content,
):
    pass
    update_dict = {
        "expectation_type": "expect_column_values_to_match_regex_list",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_values_to_match_strftime_format(
    get_rendered_content,
):
    pass
    update_dict = {
        "expectation_type": "expect_column_values_to_match_strftime_format",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_values_to_not_be_in_set(get_rendered_content):
    pass
    update_dict = {
        "expectation_type": "expect_column_values_to_not_be_in_set",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_values_to_not_be_null(get_rendered_content):
    pass
    update_dict = {
        "expectation_type": "expect_column_values_to_not_be_null",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


def test_atomic_diagnostic_expect_column_values_to_not_match_like_pattern(
    get_rendered_content,
):
    # Expectation is a stub; open to implement test once renderer method is available
    pass


def test_atomic_diagnostic_expect_column_values_to_not_match_like_pattern_list(
    get_rendered_content,
):
    # Expectation is a stub; open to implement test once renderer method is available
    pass


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_values_to_not_match_regex(
    get_rendered_content,
):
    pass
    update_dict = {
        "expectation_type": "expect_column_values_to_not_match_regex",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_column_values_to_not_match_regex_list(
    get_rendered_content,
):
    pass
    update_dict = {
        "expectation_type": "expect_column_values_to_not_match_regex_list",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_compound_columns_to_be_unique(get_rendered_content):
    pass
    update_dict = {
        "expectation_type": "expect_compound_columns_to_be_unique",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


def test_atomic_diagnostic_expect_multicolumn_sum_to_equal(get_rendered_content):
    # Expectation is a stub; open to implement test once renderer method is available
    pass


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_multicolumn_values_to_be_unique(get_rendered_content):
    pass
    update_dict = {
        "expectation_type": "expect_multicolumn_values_to_be_unique",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_select_column_values_to_be_unique_within_record(
    get_rendered_content,
):
    pass
    update_dict = {
        "expectation_type": "expect_select_column_values_to_be_unique_within_record",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_table_column_count_to_be_between(
    get_rendered_content,
):
    pass
    update_dict = {
        "expectation_type": "expect_table_column_count_to_be_between",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_table_column_count_to_equal(get_rendered_content):
    pass
    update_dict = {
        "expectation_type": "expect_table_column_count_to_equal",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_table_columns_to_match_ordered_list(
    get_rendered_content,
):
    pass
    update_dict = {
        "expectation_type": "expect_table_columns_to_match_ordered_list",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_table_columns_to_match_set(get_rendered_content):
    pass
    update_dict = {
        "expectation_type": "expect_table_columns_to_match_set",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_table_row_count_to_be_between(get_rendered_content):
    pass
    update_dict = {
        "expectation_type": "expect_table_row_count_to_be_between",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_table_row_count_to_equal(get_rendered_content):
    pass
    update_dict = {
        "expectation_type": "expect_table_row_count_to_equal",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_diagnostic_expect_table_row_count_to_equal_other_table(
    get_rendered_content,
):
    pass
    update_dict = {
        "expectation_type": "expect_table_row_count_to_equal_other_table",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.diagnostic.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


def test_atomic_prescriptive_expect_column_bootstrapped_ks_test_p_value_to_be_greater_than(
    get_rendered_content,
):
    # Expectation is a stub; open to implement test once renderer method is available
    pass


def test_atomic_prescriptive_expect_column_chisquare_test_p_value_to_be_greater_than(
    get_rendered_content,
):
    # Expectation is a stub; open to implement test once renderer method is available
    pass


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_column_distinct_values_to_be_in_set(
    get_rendered_content,
):
    pass
    update_dict = {
        "expectation_type": "expect_column_distinct_values_to_be_in_set",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_column_distinct_values_to_contain_set(
    get_rendered_content,
):
    pass
    update_dict = {
        "expectation_type": "expect_column_distinct_values_to_contain_set",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_column_distinct_values_to_equal_set(
    get_rendered_content,
):
    pass
    update_dict = {
        "expectation_type": "expect_column_distinct_values_to_equal_set",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_column_kl_divergence_to_be_less_than(
    get_rendered_content,
):
    pass
    update_dict = {
        "expectation_type": "expect_column_kl_divergence_to_be_less_than",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_column_max_to_be_between(get_rendered_content):
    pass
    update_dict = {
        "expectation_type": "expect_column_max_to_be_between",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_column_mean_to_be_between(get_rendered_content):
    pass
    update_dict = {
        "expectation_type": "expect_column_mean_to_be_between",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_column_median_to_be_between(get_rendered_content):
    pass
    update_dict = {
        "expectation_type": "expect_column_median_to_be_between",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_column_min_to_be_between(get_rendered_content):
    pass
    update_dict = {
        "expectation_type": "expect_column_min_to_be_between",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_column_most_common_value_to_be_in_set(
    get_rendered_content,
):
    pass
    update_dict = {
        "expectation_type": "expect_column_most_common_value_to_be_in_set",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_column_pair_cramers_phi_value_to_be_less_than(
    get_rendered_content,
):
    pass
    update_dict = {
        "expectation_type": "expect_column_pair_cramers_phi_value_to_be_less_than",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_column_pair_values_a_to_be_greater_than_b(
    get_rendered_content,
):
    pass
    update_dict = {
        "expectation_type": "expect_column_pair_values_a_to_be_greater_than_b",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_column_pair_values_to_be_equal(
    get_rendered_content,
):
    pass
    update_dict = {
        "expectation_type": "expect_column_pair_values_to_be_equal",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


def test_atomic_prescriptive_expect_column_pair_values_to_be_in_set(
    get_rendered_content,
):
    # Expectation is a stub; open to implement test once renderer method is available
    pass


def test_atomic_prescriptive_expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than(
    get_rendered_content,
):
    # Expectation is a stub; open to implement test once renderer method is available
    pass


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_column_proportion_of_unique_values_to_be_between(
    get_rendered_content,
):
    pass
    update_dict = {
        "expectation_type": "expect_column_proportion_of_unique_values_to_be_between",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_column_quantile_values_to_be_between(
    get_rendered_content,
):
    pass
    update_dict = {
        "expectation_type": "expect_column_quantile_values_to_be_between",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_column_stdev_to_be_between(get_rendered_content):
    pass
    update_dict = {
        "expectation_type": "expect_column_stdev_to_be_between",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_column_sum_to_be_between(get_rendered_content):
    pass
    update_dict = {
        "expectation_type": "expect_column_sum_to_be_between",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_column_to_exist(get_rendered_content):
    pass
    update_dict = {
        "expectation_type": "expect_column_to_exist",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_column_unique_value_count_to_be_between(
    get_rendered_content,
):
    pass
    update_dict = {
        "expectation_type": "expect_column_unique_value_count_to_be_between",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_column_value_lengths_to_be_between(
    get_rendered_content,
):
    pass
    update_dict = {
        "expectation_type": "expect_column_value_lengths_to_be_between",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_column_value_lengths_to_equal(get_rendered_content):
    pass
    update_dict = {
        "expectation_type": "expect_column_value_lengths_to_equal",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_column_value_z_scores_to_be_less_than(
    get_rendered_content,
):
    pass
    update_dict = {
        "expectation_type": "expect_column_value_z_scores_to_be_less_than",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


def test_atomic_prescriptive_expect_column_values_to_be_between(get_rendered_content):
    # Expectation is a stub; open to implement test once renderer method is available
    pass


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_column_values_to_be_dateutil_parseable(
    get_rendered_content,
):
    pass
    update_dict = {
        "expectation_type": "expect_column_values_to_be_dateutil_parseable",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_column_values_to_be_decreasing(
    get_rendered_content,
):
    pass
    update_dict = {
        "expectation_type": "expect_column_values_to_be_decreasing",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_column_values_to_be_in_set(get_rendered_content):
    pass
    update_dict = {
        "expectation_type": "expect_column_values_to_be_in_set",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_column_values_to_be_in_type_list(
    get_rendered_content,
):
    pass
    update_dict = {
        "expectation_type": "expect_column_values_to_be_in_type_list",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_column_values_to_be_increasing(
    get_rendered_content,
):
    pass
    update_dict = {
        "expectation_type": "expect_column_values_to_be_increasing",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_column_values_to_be_json_parseable(
    get_rendered_content,
):
    pass
    update_dict = {
        "expectation_type": "expect_column_values_to_be_json_parseable",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_column_values_to_be_null(get_rendered_content):
    pass
    update_dict = {
        "expectation_type": "expect_column_values_to_be_null",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_column_values_to_be_of_type(get_rendered_content):
    pass
    update_dict = {
        "expectation_type": "expect_column_values_to_be_of_type",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_column_values_to_be_unique(get_rendered_content):
    pass
    update_dict = {
        "expectation_type": "expect_column_values_to_be_unique",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_column_values_to_match_json_schema(
    get_rendered_content,
):
    pass
    update_dict = {
        "expectation_type": "expect_column_values_to_match_json_schema",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


def test_atomic_prescriptive_expect_column_values_to_match_like_pattern(
    get_rendered_content,
):
    # Expectation is a stub; open to implement test once renderer method is available
    pass


def test_atomic_prescriptive_expect_column_values_to_match_like_pattern_list(
    get_rendered_content,
):
    # Expectation is a stub; open to implement test once renderer method is available
    pass


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_column_values_to_match_regex(get_rendered_content):
    pass
    update_dict = {
        "expectation_type": "expect_column_values_to_match_regex",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_column_values_to_match_regex_list(
    get_rendered_content,
):
    pass
    update_dict = {
        "expectation_type": "expect_column_values_to_match_regex_list",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_column_values_to_match_strftime_format(
    get_rendered_content,
):
    pass
    update_dict = {
        "expectation_type": "expect_column_values_to_match_strftime_format",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_column_values_to_not_be_in_set(
    get_rendered_content,
):
    pass
    update_dict = {
        "expectation_type": "expect_column_values_to_not_be_in_set",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_column_values_to_not_be_null(get_rendered_content):
    pass
    update_dict = {
        "expectation_type": "expect_column_values_to_not_be_null",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


def test_atomic_prescriptive_expect_column_values_to_not_match_like_pattern(
    get_rendered_content,
):
    # Expectation is a stub; open to implement test once renderer method is available
    pass


def test_atomic_prescriptive_expect_column_values_to_not_match_like_pattern_list(
    get_rendered_content,
):
    # Expectation is a stub; open to implement test once renderer method is available
    pass


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_column_values_to_not_match_regex(
    get_rendered_content,
):
    pass
    update_dict = {
        "expectation_type": "expect_column_values_to_not_match_regex",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_column_values_to_not_match_regex_list(
    get_rendered_content,
):
    pass
    update_dict = {
        "expectation_type": "expect_column_values_to_not_match_regex_list",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_compound_columns_to_be_unique(get_rendered_content):
    pass
    update_dict = {
        "expectation_type": "expect_compound_columns_to_be_unique",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


def test_atomic_prescriptive_expect_multicolumn_sum_to_equal(get_rendered_content):
    # Expectation is a stub; open to implement test once renderer method is available
    pass


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_multicolumn_values_to_be_unique(
    get_rendered_content,
):
    pass
    update_dict = {
        "expectation_type": "expect_multicolumn_values_to_be_unique",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_select_column_values_to_be_unique_within_record(
    get_rendered_content,
):
    pass
    update_dict = {
        "expectation_type": "expect_select_column_values_to_be_unique_within_record",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_table_column_count_to_be_between(
    get_rendered_content,
):
    pass
    update_dict = {
        "expectation_type": "expect_table_column_count_to_be_between",
        "kwargs": {},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_table_column_count_to_equal(get_rendered_content):
    pass
    update_dict = {
        "expectation_type": "expect_table_column_count_to_equal",
        "kwargs": {
            "value": 10,
        },
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


# FIXME(cdkini): Implement!
def test_atomic_prescriptive_expect_table_columns_to_match_ordered_list(
    get_rendered_content,
):
    pass
    update_dict = {
        "expectation_type": "expect_table_columns_to_match_ordered_list",
        "kwargs": {"column_list": ["a", "b", "c"]},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {}


def test_atomic_prescriptive_expect_table_columns_to_match_set(get_rendered_content):
    pass
    update_dict = {
        "expectation_type": "expect_table_columns_to_match_set",
        "kwargs": {
            "column_set": ["a", "b", "c"],
            "exact_match": True,
        },
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {
        "name": "atomic.prescriptive.summary",
        "value": {
            "params": {
                "column_list": {"schema": {"type": "array"}, "value": ["a", "b", "c"]},
                "exact_match": {"schema": {"type": "boolean"}, "value": True},
            },
            "schema": {"type": "com.superconductive.rendered.string"},
            "template": "Must have exactly these columns (in any order): "
            "$column_list_0, $column_list_1, $column_list_2",
        },
        "valuetype": "StringValueType",
    }


def test_atomic_prescriptive_expect_table_row_count_to_be_between(get_rendered_content):
    update_dict = {
        "expectation_type": "expect_table_row_count_to_be_between",
        "kwargs": {"max_value": None, "min_value": 1},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {
        "name": "atomic.prescriptive.summary",
        "value": {
            "params": {
                "condition_parser": {"schema": {"type": "string"}, "value": None},
                "max_value": {"schema": {"type": "number"}, "value": None},
                "min_value": {"schema": {"type": "number"}, "value": 1},
                "strict_max": {"schema": {"type": "boolean"}, "value": None},
                "strict_min": {"schema": {"type": "boolean"}, "value": None},
            },
            "schema": {"type": "com.superconductive.rendered.string"},
            "template": "Must have greater than or equal to $min_value rows.",
        },
        "valuetype": "StringValueType",
    }


def test_atomic_prescriptive_expect_table_row_count_to_equal(get_rendered_content):
    update_dict = {
        "expectation_type": "expect_table_row_count_to_equal",
        "kwargs": {"value": 10},
    }
    rendered_content = get_rendered_content("atomic.prescriptive.summary", update_dict)

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {
        "name": "atomic.prescriptive.summary",
        "value": {
            "params": {
                "condition_parser": {"schema": {"type": "string"}, "value": None},
                "row_condition": {"schema": {"type": "string"}, "value": None},
                "value": {"schema": {"type": "number"}, "value": 10},
            },
            "schema": {"type": "com.superconductive.rendered.string"},
            "template": "Must have exactly $value rows.",
        },
        "valuetype": "StringValueType",
    }


def test_atomic_prescriptive_expect_table_row_count_to_equal_other_table(
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

    assert len(rendered_content) == 1
    pprint(rendered_content[0].to_json_dict())
    assert rendered_content[0].to_json_dict() == {
        "name": "atomic.prescriptive.summary",
        "value": {
            "params": {
                "other_table_name": {
                    "schema": {"type": "string"},
                    "value": {
                        "schema": {"type": "string"},
                        "value": "other_table_name",
                    },
                }
            },
            "schema": {"type": "com.superconductive.rendered.string"},
            "template": "Row count must equal the row count of table "
            "$other_table_name.",
        },
        "valuetype": "StringValueType",
    }
