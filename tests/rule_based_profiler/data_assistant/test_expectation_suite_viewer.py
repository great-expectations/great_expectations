import itertools

import pytest

from great_expectations.rule_based_profiler.data_assistant_result.data_assistant_result import (
    ExpectationSuiteViewer,
)


@pytest.mark.unit
def test_get_table_expectations_returns_empty_list_on_empty_suite(empty_suite):
    assert ExpectationSuiteViewer(suite=empty_suite).get_table_expectations() == []


@pytest.mark.unit
def test_get_table_expectations_returns_empty_list_on_suite_without_any(baseline_suite):
    assert ExpectationSuiteViewer(baseline_suite).get_table_expectations() == []


@pytest.mark.unit
def test_get_table_expectations(
    suite_with_table_and_column_expectations, table_exp1, table_exp2, table_exp3
):
    obs = ExpectationSuiteViewer(
        suite_with_table_and_column_expectations
    ).get_table_expectations()
    assert obs == [table_exp1, table_exp2, table_exp3]


@pytest.mark.unit
def test_get_column_expectations_returns_empty_list_on_empty_suite(empty_suite):
    assert ExpectationSuiteViewer(empty_suite).get_column_expectations() == []


@pytest.mark.unit
def test_get_column_expectations(
    suite_with_table_and_column_expectations,
    expect_column_values_to_be_in_set_col_a_with_meta,
    exp2,
    exp3,
    exp4,
):
    obs = ExpectationSuiteViewer(
        suite_with_table_and_column_expectations
    ).get_column_expectations()
    assert obs == [expect_column_values_to_be_in_set_col_a_with_meta, exp2, exp3, exp4]


@pytest.mark.unit
def test_get_expectations_by_expectation_type(
    suite_with_table_and_column_expectations,
    expect_column_values_to_be_in_set_col_a_with_meta,
    exp2,
    exp3,
    exp4,
    column_pair_expectation,
    table_exp1,
    table_exp2,
    table_exp3,
):
    obs = ExpectationSuiteViewer(
        suite_with_table_and_column_expectations
    ).get_grouped_and_ordered_expectations_by_expectation_type()
    assert obs == [
        table_exp1,
        table_exp2,
        table_exp3,
        expect_column_values_to_be_in_set_col_a_with_meta,
        exp2,
        exp3,
        exp4,
        column_pair_expectation,
    ]


@pytest.mark.unit
def test_get_expectations_by_domain_type(
    suite_with_table_and_column_expectations,
    expect_column_values_to_be_in_set_col_a_with_meta,
    exp2,
    exp3,
    exp4,
    column_pair_expectation,
    table_exp1,
    table_exp2,
    table_exp3,
):
    obs = ExpectationSuiteViewer(
        suite_with_table_and_column_expectations
    ).get_grouped_and_ordered_expectations_by_domain_type()
    assert list(itertools.chain.from_iterable(obs.values())) == [
        table_exp1,
        table_exp2,
        table_exp3,
        expect_column_values_to_be_in_set_col_a_with_meta,
        exp2,
        exp3,
        exp4,
        column_pair_expectation,
    ]
