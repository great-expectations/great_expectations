import pandas as pd
import pytest

import great_expectations as ge
from great_expectations.core import ExpectationSuite
from great_expectations.data_context.util import file_relative_path
from great_expectations.profile.user_configurable_profiler import (
    UserConfigurableProfiler,
)


@pytest.fixture()
def cardinality_dataset():
    df = pd.DataFrame(
        {
            "col_none": [None for i in range(0, 1000)],
            "col_one": [0 for i in range(0, 1000)],
            "col_two": [i % 2 for i in range(0, 1000)],
            "col_very_few": [i % 10 for i in range(0, 1000)],
            "col_few": [i % 50 for i in range(0, 1000)],
            "col_many": [i % 100 for i in range(0, 1000)],
            "col_very_many": [i % 500 for i in range(0, 1000)],
            "col_unique": [i for i in range(0, 1000)],
        }
    )
    batch_df = ge.dataset.PandasDataset(df)

    return batch_df


@pytest.fixture()
def titanic_dataset():
    df = ge.read_csv(file_relative_path(__file__, "../test_sets/Titanic.csv"))
    batch_df = ge.dataset.PandasDataset(df)

    return batch_df


@pytest.fixture()
def possible_expectations_set():
    return {
        "expect_table_columns_to_match_ordered_list",
        "expect_table_row_count_to_be_between",
        "expect_column_values_to_be_in_type_list",
        "expect_column_values_to_not_be_null",
        "expect_column_values_to_be_null",
        "expect_column_proportion_of_unique_values_to_be_between",
        "expect_column_min_to_be_between",
        "expect_column_max_to_be_between",
        "expect_column_mean_to_be_between",
        "expect_column_median_to_be_between",
        "expect_column_quantile_values_to_be_between",
        "expect_column_values_to_be_in_set",
        "expect_column_values_to_be_between",
        "expect_column_values_to_be_unique",
    }


@pytest.fixture()
def full_config_cardinality_dataset_no_semantic_types():
    return {
        "primary_or_compound_key": ["col_unique"],
        "ignored_columns": [
            "col_one",
        ],
        "value_set_threshold": "unique",
        "table_expectations_only": False,
        "excluded_expectations": ["expect_column_values_to_not_be_null"],
    }


@pytest.fixture()
def full_config_cardinality_dataset_with_semantic_types():
    return {
        "semantic_types": {
            "numeric": ["col_few", "col_many", "col_very_many"],
            "value_set": ["col_one", "col_two", "col_very_few"],
        },
        "primary_or_compound_key": ["col_unique"],
        "ignored_columns": [
            "col_one",
        ],
        "value_set_threshold": "unique",
        "table_expectations_only": False,
        "excluded_expectations": ["expect_column_values_to_not_be_null"],
    }


def get_set_of_columns_and_expectations_from_suite(suite):
    columns = {
        i.kwargs.get("column") for i in suite.expectations if i.kwargs.get("column")
    }
    expectations = {i.expectation_type for i in suite.expectations}

    return columns, expectations


def test_profiler_init_no_config(
    cardinality_dataset,
):
    profiler = UserConfigurableProfiler(cardinality_dataset)
    assert profiler.primary_or_compound_key == []
    assert profiler.ignored_columns == []
    assert not profiler.value_set_threshold
    assert not profiler.table_expectations_only
    assert profiler.excluded_expectations == []


def test_profiler_init_full_config_no_semantic_types(
    cardinality_dataset, full_config_cardinality_dataset_no_semantic_types
):
    profiler = UserConfigurableProfiler(
        cardinality_dataset, full_config_cardinality_dataset_no_semantic_types
    )
    assert profiler.primary_or_compound_key == ["col_unique"]
    assert profiler.ignored_columns == [
        "col_one",
    ]
    assert profiler.value_set_threshold == "unique"
    assert not profiler.table_expectations_only
    assert profiler.excluded_expectations == ["expect_column_values_to_not_be_null"]

    assert "col_one" not in profiler.column_info


def test_config_with_not_null_only(possible_expectations_set):
    excluded_expectations = [i for i in possible_expectations_set if "null" not in i]

    df = pd.DataFrame(
        {
            "mostly_null": [i if i % 3 == 0 else None for i in range(0, 1000)],
            "mostly_not_null": [None if i % 3 == 0 else i for i in range(0, 1000)],
        }
    )
    batch_df = ge.dataset.PandasDataset(df)

    config_without_not_null_only = {
        "excluded_expectations": excluded_expectations,
        "not_null_only": False,
    }
    profiler = UserConfigurableProfiler(batch_df, config_without_not_null_only)
    suite = profiler.build_suite()
    _, expectations = get_set_of_columns_and_expectations_from_suite(suite)
    assert expectations == {
        "expect_column_values_to_be_null",
        "expect_column_values_to_not_be_null",
    }

    config_with_not_null_only = {
        "excluded_expectations": excluded_expectations,
        "not_null_only": True,
    }
    not_null_only_profiler = UserConfigurableProfiler(
        batch_df, config_with_not_null_only
    )
    not_null_only_suite = not_null_only_profiler.build_suite()
    _, expectations = get_set_of_columns_and_expectations_from_suite(
        not_null_only_suite
    )
    assert expectations == {"expect_column_values_to_not_be_null"}

    no_config_profiler = UserConfigurableProfiler(batch_df)
    no_config_suite = no_config_profiler.build_suite()
    _, expectations = get_set_of_columns_and_expectations_from_suite(no_config_suite)
    assert "expect_column_values_to_be_null" in expectations


def test__initialize_cach_with_metadata_with_semantic_types(
    cardinality_dataset, full_config_cardinality_dataset_with_semantic_types
):
    profiler = UserConfigurableProfiler(
        cardinality_dataset, full_config_cardinality_dataset_with_semantic_types
    )

    assert "col_one" not in profiler.column_info

    assert profiler.column_info.get("col_none") == {
        "cardinality": "none",
        "type": "numeric",
        "semantic_types": [],
    }
    assert profiler.column_info.get("col_two") == {
        "cardinality": "two",
        "type": "int",
        "semantic_types": ["value_set"],
    }
    assert profiler.column_info.get("col_very_few") == {
        "cardinality": "very_few",
        "type": "int",
        "semantic_types": ["value_set"],
    }
    assert profiler.column_info.get("col_few") == {
        "cardinality": "few",
        "type": "int",
        "semantic_types": ["numeric"],
    }
    assert profiler.column_info.get("col_many") == {
        "cardinality": "many",
        "type": "int",
        "semantic_types": ["numeric"],
    }
    assert profiler.column_info.get("col_very_many") == {
        "cardinality": "very_many",
        "type": "int",
        "semantic_types": ["numeric"],
    }
    assert profiler.column_info.get("col_unique") == {
        "cardinality": "unique",
        "type": "int",
        "semantic_types": [],
    }


def test__validate_config(cardinality_dataset):
    bad_keyword_config = {"bad_keyword": 100}
    with pytest.raises(AssertionError) as e:
        UserConfigurableProfiler(cardinality_dataset, bad_keyword_config)
    assert e.value.args[0] == "Parameter bad_keyword from config is not recognized."

    bad_param_type_ignored_columns = {"ignored_columns": "col_name"}
    with pytest.raises(AssertionError) as e:
        UserConfigurableProfiler(cardinality_dataset, bad_param_type_ignored_columns)
    assert (
        e.value.args[0]
        == "Config parameter ignored_columns must be formatted as a <class 'list'> rather than a <class 'str'>."
    )

    bad_param_type_table_expectations_only = {"table_expectations_only": "True"}
    with pytest.raises(AssertionError) as e:
        UserConfigurableProfiler(
            cardinality_dataset, bad_param_type_table_expectations_only
        )
    assert (
        e.value.args[0]
        == "Config parameter table_expectations_only must be formatted as a <class 'bool'> rather than a <class 'str'>."
    )


def test__validate_semantic_types_dict(
    cardinality_dataset, full_config_cardinality_dataset_with_semantic_types
):
    bad_semantic_types_dict_type = {"semantic_types": {"value_set": "few"}}
    with pytest.raises(AssertionError) as e:
        UserConfigurableProfiler(cardinality_dataset, bad_semantic_types_dict_type)
    assert e.value.args[0] == (
        "Entries in semantic type dict must be lists of column names e.g. "
        "{'semantic_types': {'numeric': ['number_of_transactions']}}"
    )


def test_build_suite_no_config(titanic_dataset, possible_expectations_set):
    profiler = UserConfigurableProfiler(titanic_dataset)
    suite = profiler.build_suite()
    expectations_from_suite = {i.expectation_type for i in suite.expectations}

    assert expectations_from_suite.issubset(possible_expectations_set)
    assert len(suite.expectations) == 48


def test_build_suite_with_config(titanic_dataset, possible_expectations_set):
    config = {
        "ignored_columns": ["Survived", "Unnamed: 0"],
        "excluded_expectations": ["expect_column_mean_to_be_between"],
        "primary_or_compound_key": ["Name"],
        "table_expectations_only": False,
        "value_set_threshold": "very_few",
    }
    profiler = UserConfigurableProfiler(titanic_dataset, config=config)
    suite = profiler.build_suite()
    (
        columns_with_expectations,
        expectations_from_suite,
    ) = get_set_of_columns_and_expectations_from_suite(suite)

    columns_expected_in_suite = {"Name", "PClass", "Age", "Sex", "SexCode"}
    assert columns_with_expectations == columns_expected_in_suite
    assert expectations_from_suite.issubset(possible_expectations_set)
    assert "expect_column_mean_to_be_between" not in expectations_from_suite
    assert len(suite.expectations) == 29


def test_build_suite_with_semantic_types_dict(
    cardinality_dataset,
    possible_expectations_set,
    full_config_cardinality_dataset_with_semantic_types,
):
    profiler = UserConfigurableProfiler(
        cardinality_dataset, full_config_cardinality_dataset_with_semantic_types
    )
    suite = profiler.build_suite()
    (
        columns_with_expectations,
        expectations_from_suite,
    ) = get_set_of_columns_and_expectations_from_suite(suite)

    assert "column_one" not in columns_with_expectations
    assert "expect_column_values_to_not_be_null" not in expectations_from_suite
    assert expectations_from_suite.issubset(possible_expectations_set)
    assert len(suite.expectations) == 34

    value_set_expectations = [
        i
        for i in suite.expectations
        if i.expectation_type == "expect_column_values_to_be_in_set"
    ]
    value_set_columns = {i.kwargs.get("column") for i in value_set_expectations}

    assert len(value_set_columns) == 2
    assert value_set_columns == {"col_two", "col_very_few"}


def test_build_suite_when_suite_already_exists(cardinality_dataset):
    config = {
        "table_expectations_only": True,
        "excluded_expectations": ["expect_table_row_count_to_be_between"],
    }

    profiler = UserConfigurableProfiler(cardinality_dataset, config)

    suite = profiler.build_suite()
    _, expectations = get_set_of_columns_and_expectations_from_suite(suite)
    assert len(suite.expectations) == 1
    assert "expect_table_columns_to_match_ordered_list" in expectations

    profiler.excluded_expectations = ["expect_table_columns_to_match_ordered_list"]
    suite = profiler.build_suite()
    _, expectations = get_set_of_columns_and_expectations_from_suite(suite)
    assert len(suite.expectations) == 1
    assert "expect_table_row_count_to_be_between" in expectations
