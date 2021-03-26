import pandas as pd
import pytest

import great_expectations as ge
from great_expectations.data_context.util import file_relative_path
from great_expectations.dataset import PandasDataset
from great_expectations.profile.base import (
    OrderedProfilerCardinality,
    profiler_semantic_types,
)
from great_expectations.profile.user_configurable_profiler import (
    UserConfigurableProfiler,
)
from tests.profile.conftest import get_set_of_columns_and_expectations_from_suite


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
    batch_df = PandasDataset(df)

    return batch_df


@pytest.fixture()
def nulls_dataset():
    df = pd.DataFrame(
        {
            "mostly_null": [i if i % 3 == 0 else None for i in range(0, 1000)],
            "mostly_not_null": [None if i % 3 == 0 else i for i in range(0, 1000)],
        }
    )
    batch_df = PandasDataset(df)

    return batch_df


@pytest.fixture()
def titanic_dataset():
    df = ge.read_csv(file_relative_path(__file__, "../test_sets/Titanic.csv"))
    batch_df = PandasDataset(df)

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
        "expect_compound_columns_to_be_unique",
    }


def test_profiler_init_no_config(
    cardinality_dataset,
):
    """
    What does this test do and why?
    Confirms that profiler can initialize with no config.
    """
    profiler = UserConfigurableProfiler(cardinality_dataset)
    assert profiler.primary_or_compound_key == []
    assert profiler.ignored_columns == []
    assert profiler.value_set_threshold == "MANY"
    assert not profiler.table_expectations_only
    assert profiler.excluded_expectations == []


def test_profiler_init_full_config_no_semantic_types(cardinality_dataset):
    """
    What does this test do and why?
    Confirms that profiler initializes properly with a full config, without a semantic_types dict
    """

    profiler = UserConfigurableProfiler(
        cardinality_dataset,
        primary_or_compound_key=["col_unique"],
        ignored_columns=["col_one"],
        value_set_threshold="UNIQUE",
        table_expectations_only=False,
        excluded_expectations=["expect_column_values_to_not_be_null"],
    )
    assert profiler.primary_or_compound_key == ["col_unique"]
    assert profiler.ignored_columns == [
        "col_one",
    ]
    assert profiler.value_set_threshold == "UNIQUE"
    assert not profiler.table_expectations_only
    assert profiler.excluded_expectations == ["expect_column_values_to_not_be_null"]

    assert "col_one" not in profiler.column_info


def test_init_with_semantic_types(cardinality_dataset):
    """
    What does this test do and why?
    Confirms that profiler initializes properly with a full config and a semantic_types dict
    """

    semantic_types = {
        "numeric": ["col_few", "col_many", "col_very_many"],
        "value_set": ["col_two", "col_very_few"],
    }
    profiler = UserConfigurableProfiler(
        cardinality_dataset,
        semantic_types_dict=semantic_types,
        primary_or_compound_key=["col_unique"],
        ignored_columns=["col_one"],
        value_set_threshold="unique",
        table_expectations_only=False,
        excluded_expectations=["expect_column_values_to_not_be_null"],
    )

    assert "col_one" not in profiler.column_info

    assert profiler.column_info.get("col_none") == {
        "cardinality": "NONE",
        "type": "NUMERIC",
        "semantic_types": [],
    }
    assert profiler.column_info.get("col_two") == {
        "cardinality": "TWO",
        "type": "INT",
        "semantic_types": ["VALUE_SET"],
    }
    assert profiler.column_info.get("col_very_few") == {
        "cardinality": "VERY_FEW",
        "type": "INT",
        "semantic_types": ["VALUE_SET"],
    }
    assert profiler.column_info.get("col_few") == {
        "cardinality": "FEW",
        "type": "INT",
        "semantic_types": ["NUMERIC"],
    }
    assert profiler.column_info.get("col_many") == {
        "cardinality": "MANY",
        "type": "INT",
        "semantic_types": ["NUMERIC"],
    }
    assert profiler.column_info.get("col_very_many") == {
        "cardinality": "VERY_MANY",
        "type": "INT",
        "semantic_types": ["NUMERIC"],
    }
    assert profiler.column_info.get("col_unique") == {
        "cardinality": "UNIQUE",
        "type": "INT",
        "semantic_types": [],
    }


def test__validate_config(cardinality_dataset):
    """
    What does this test do and why?
    Tests the validate config function on the profiler
    """

    with pytest.raises(AssertionError) as e:
        UserConfigurableProfiler(cardinality_dataset, ignored_columns="col_name")
    assert e.typename == "AssertionError"

    with pytest.raises(AssertionError) as e:
        UserConfigurableProfiler(cardinality_dataset, table_expectations_only="True")
    assert e.typename == "AssertionError"


def test_value_set_threshold(cardinality_dataset):
    """
    What does this test do and why?
    Tests the value_set_threshold logic on the profiler works as expected.
    """
    # Test that when value_set_threshold is unset, it will default to "MANY"
    profiler = UserConfigurableProfiler(cardinality_dataset)
    assert profiler.value_set_threshold == "MANY"

    # Test that when value_set_threshold is set to an appropriate enum value, the value_set_threshold will be correct
    profiler = UserConfigurableProfiler(cardinality_dataset, value_set_threshold="FEW")
    assert profiler.value_set_threshold == "FEW"

    # Test that when value_set_threshold is set to a non-string, it will error
    with pytest.raises(AssertionError) as e:
        UserConfigurableProfiler(cardinality_dataset, value_set_threshold=None)
    assert e.typename == "AssertionError"

    # Test that when value_set_threshold is set to a string that is not in the cardinality enum, it will error
    with pytest.raises(AssertionError) as e:
        UserConfigurableProfiler(cardinality_dataset, value_set_threshold="wrong_value")
    assert e.typename == "AssertionError"
    assert (
        e.value.args[0]
        == "value_set_threshold must be one of ['NONE', 'ONE', 'TWO', 'VERY_FEW', 'FEW', 'MANY', 'VERY_MANY', 'UNIQUE']"
    )


def test__validate_semantic_types_dict(cardinality_dataset):
    """
    What does this test do and why?
    Tests that _validate_semantic_types_dict function errors when not formatted correctly
    """

    bad_semantic_types_dict_type = {"value_set": "col_few"}
    with pytest.raises(AssertionError) as e:
        UserConfigurableProfiler(
            cardinality_dataset, semantic_types_dict=bad_semantic_types_dict_type
        )
    assert e.value.args[0] == (
        "Entries in semantic type dict must be lists of column names e.g. "
        "{'semantic_types': {'numeric': ['number_of_transactions']}}"
    )

    bad_semantic_types_incorrect_type = {"incorrect_type": ["col_few"]}
    with pytest.raises(ValueError) as e:
        UserConfigurableProfiler(
            cardinality_dataset, semantic_types_dict=bad_semantic_types_incorrect_type
        )
    assert e.value.args[0] == (
        f"incorrect_type is not a recognized semantic_type. Please only include one of "
        f"{profiler_semantic_types}"
    )

    # Error if column is specified for both semantic_types and ignored
    working_semantic_type = {"numeric": ["col_few"]}
    with pytest.raises(ValueError) as e:
        UserConfigurableProfiler(
            cardinality_dataset,
            semantic_types_dict=working_semantic_type,
            ignored_columns=["col_few"],
        )
    assert e.value.args[0] == (
        f"Column col_few is specified in both the semantic_types_dict and the list of ignored columns. Please remove "
        f"one of these entries to proceed."
    )


def test_build_suite_no_config(titanic_dataset, possible_expectations_set):
    """
    What does this test do and why?
    Tests that the build_suite function works as expected with no config
    """
    profiler = UserConfigurableProfiler(titanic_dataset)
    suite = profiler.build_suite()
    expectations_from_suite = {i.expectation_type for i in suite.expectations}

    assert expectations_from_suite.issubset(possible_expectations_set)
    assert len(suite.expectations) == 48


def test_build_suite_with_config_and_no_semantic_types_dict(
    titanic_dataset, possible_expectations_set
):
    """
    What does this test do and why?
    Tests that the build_suite function works as expected with a config and without a semantic_types dict
    """
    profiler = UserConfigurableProfiler(
        titanic_dataset,
        ignored_columns=["Survived", "Unnamed: 0"],
        excluded_expectations=["expect_column_mean_to_be_between"],
        primary_or_compound_key=["Name"],
        table_expectations_only=False,
        value_set_threshold="very_few",
    )
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
):
    """
    What does this test do and why?
    Tests that the build_suite function works as expected with a semantic_types dict
    """

    semantic_types = {
        "numeric": ["col_few", "col_many", "col_very_many"],
        "value_set": ["col_two", "col_very_few"],
    }

    profiler = UserConfigurableProfiler(
        cardinality_dataset,
        semantic_types_dict=semantic_types,
        primary_or_compound_key=["col_unique"],
        ignored_columns=["col_one"],
        value_set_threshold="unique",
        table_expectations_only=False,
        excluded_expectations=["expect_column_values_to_not_be_null"],
    )
    suite = profiler.build_suite()
    (
        columns_with_expectations,
        expectations_from_suite,
    ) = get_set_of_columns_and_expectations_from_suite(suite)

    assert "column_one" not in columns_with_expectations
    assert "expect_column_values_to_not_be_null" not in expectations_from_suite
    assert expectations_from_suite.issubset(possible_expectations_set)
    assert len(suite.expectations) == 33

    value_set_expectations = [
        i
        for i in suite.expectations
        if i.expectation_type == "expect_column_values_to_be_in_set"
    ]
    value_set_columns = {i.kwargs.get("column") for i in value_set_expectations}

    assert len(value_set_columns) == 2
    assert value_set_columns == {"col_two", "col_very_few"}


def test_build_suite_when_suite_already_exists(cardinality_dataset):
    """
    What does this test do and why?
    Confirms that creating a new suite on an existing profiler wipes the previous suite
    """
    profiler = UserConfigurableProfiler(
        cardinality_dataset,
        table_expectations_only=True,
        excluded_expectations=["expect_table_row_count_to_be_between"],
    )

    suite = profiler.build_suite()
    _, expectations = get_set_of_columns_and_expectations_from_suite(suite)
    assert len(suite.expectations) == 1
    assert "expect_table_columns_to_match_ordered_list" in expectations

    profiler.excluded_expectations = ["expect_table_columns_to_match_ordered_list"]
    suite = profiler.build_suite()
    _, expectations = get_set_of_columns_and_expectations_from_suite(suite)
    assert len(suite.expectations) == 1
    assert "expect_table_row_count_to_be_between" in expectations


def test_primary_or_compound_key_not_found_in_columns(cardinality_dataset):
    """
    What does this test do and why?
    Confirms that an error is raised if a primary_or_compound key is specified with a column not found in the dataset
    """
    # regular case, should pass
    working_profiler = UserConfigurableProfiler(
        cardinality_dataset, primary_or_compound_key=["col_unique"]
    )
    assert working_profiler.primary_or_compound_key == ["col_unique"]

    # key includes a non-existent column, should fail
    with pytest.raises(ValueError) as e:
        bad_key_profiler = UserConfigurableProfiler(
            cardinality_dataset,
            primary_or_compound_key=["col_unique", "col_that_does_not_exist"],
        )
    assert e.value.args[0] == (
        f"Column col_that_does_not_exist not found. Please ensure that this column is in the PandasDataset if "
        f"you would like to use it as a primary_or_compound_key."
    )

    # key includes a column that exists, but is in ignored_columns, should pass
    ignored_column_profiler = UserConfigurableProfiler(
        cardinality_dataset,
        primary_or_compound_key=["col_unique", "col_one"],
        ignored_columns=["col_none", "col_one"],
    )
    assert ignored_column_profiler.primary_or_compound_key == ["col_unique", "col_one"]


def test_config_with_not_null_only(nulls_dataset, possible_expectations_set):
    """
    What does this test do and why?
    Confirms that the not_null_only key in config works as expected.
    """

    excluded_expectations = [i for i in possible_expectations_set if "null" not in i]

    batch_df = nulls_dataset

    profiler_without_not_null_only = UserConfigurableProfiler(
        batch_df, excluded_expectations, not_null_only=False
    )
    suite_without_not_null_only = profiler_without_not_null_only.build_suite()
    _, expectations = get_set_of_columns_and_expectations_from_suite(
        suite_without_not_null_only
    )
    assert expectations == {
        "expect_column_values_to_be_null",
        "expect_column_values_to_not_be_null",
    }

    profiler_with_not_null_only = UserConfigurableProfiler(
        batch_df, excluded_expectations, not_null_only=True
    )
    not_null_only_suite = profiler_with_not_null_only.build_suite()
    _, expectations = get_set_of_columns_and_expectations_from_suite(
        not_null_only_suite
    )
    assert expectations == {"expect_column_values_to_not_be_null"}

    no_config_profiler = UserConfigurableProfiler(batch_df)
    no_config_suite = no_config_profiler.build_suite()
    _, expectations = get_set_of_columns_and_expectations_from_suite(no_config_suite)
    assert "expect_column_values_to_be_null" in expectations


def test_nullity_expectations_mostly_tolerance(
    nulls_dataset, possible_expectations_set
):
    excluded_expectations = [i for i in possible_expectations_set if "null" not in i]

    batch_df = nulls_dataset

    profiler = UserConfigurableProfiler(
        batch_df, excluded_expectations, not_null_only=False
    )
    suite = profiler.build_suite()

    for i in suite.expectations:
        assert i["kwargs"]["mostly"] == 0.66


def test_profiled_dataset_passes_own_validation(
    cardinality_dataset, titanic_data_context
):
    """
    What does this test do and why?
    Confirms that a suite created on a dataset with no config will pass when validated against itself
    """
    context = titanic_data_context
    profiler = UserConfigurableProfiler(
        cardinality_dataset, ignored_columns=["col_none"]
    )
    suite = profiler.build_suite()

    context.save_expectation_suite(suite)
    results = context.run_validation_operator(
        "action_list_operator", assets_to_validate=[cardinality_dataset]
    )

    assert results["success"]


def test_profiler_all_expectation_types(
    titanic_data_context, possible_expectations_set
):
    """
    What does this test do and why?
    Ensures that all available expectation types work as expected
    """
    context = titanic_data_context
    df = ge.read_csv(
        file_relative_path(__file__, "../test_sets/yellow_tripdata_sample_2019-01.csv")
    )
    batch_df = ge.dataset.PandasDataset(df)

    ignored_columns = [
        "pickup_location_id",
        "dropoff_location_id",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "congestion_surcharge",
    ]
    semantic_types = {
        "datetime": ["pickup_datetime", "dropoff_datetime"],
        "numeric": ["total_amount", "passenger_count"],
        "value_set": [
            "payment_type",
            "rate_code_id",
            "store_and_fwd_flag",
            "passenger_count",
        ],
        "boolean": ["store_and_fwd_flag"],
    }

    profiler = UserConfigurableProfiler(
        batch_df,
        semantic_types_dict=semantic_types,
        ignored_columns=ignored_columns,
        primary_or_compound_key=[
            "vendor_id",
            "pickup_datetime",
            "dropoff_datetime",
            "trip_distance",
            "pickup_location_id",
            "dropoff_location_id",
        ],
    )

    assert profiler.column_info.get("rate_code_id")
    suite = profiler.build_suite()
    assert len(suite.expectations) == 46
    (
        columns_with_expectations,
        expectations_from_suite,
    ) = get_set_of_columns_and_expectations_from_suite(suite)

    unexpected_expectations = {
        "expect_column_values_to_be_unique",
        "expect_column_values_to_be_null",
    }
    assert expectations_from_suite == {
        i for i in possible_expectations_set if i not in unexpected_expectations
    }

    ignored_included_columns_overlap = [
        i for i in columns_with_expectations if i in ignored_columns
    ]
    assert len(ignored_included_columns_overlap) == 0

    results = context.run_validation_operator(
        "action_list_operator", assets_to_validate=[batch_df]
    )

    assert results["success"]


def test_column_cardinality_functions(cardinality_dataset):
    profiler = UserConfigurableProfiler(cardinality_dataset)
    assert profiler.column_info.get("col_none").get("cardinality") == "NONE"
    assert profiler.column_info.get("col_one").get("cardinality") == "ONE"
    assert profiler.column_info.get("col_two").get("cardinality") == "TWO"
    assert profiler.column_info.get("col_very_few").get("cardinality") == "VERY_FEW"
    assert profiler.column_info.get("col_few").get("cardinality") == "FEW"
    assert profiler.column_info.get("col_many").get("cardinality") == "MANY"
    assert profiler.column_info.get("col_very_many").get("cardinality") == "VERY_MANY"

    cardinality_with_ten_num_and_no_pct = (
        OrderedProfilerCardinality.get_basic_column_cardinality(num_unique=10)
    )
    assert cardinality_with_ten_num_and_no_pct.name == "VERY_FEW"

    cardinality_with_unique_pct_and_no_num = (
        OrderedProfilerCardinality.get_basic_column_cardinality(pct_unique=1.0)
    )
    assert cardinality_with_unique_pct_and_no_num.name == "UNIQUE"

    cardinality_with_no_pct_and_no_num = (
        OrderedProfilerCardinality.get_basic_column_cardinality()
    )
    assert cardinality_with_no_pct_and_no_num.name == "NONE"

    cardinality_with_large_pct_and_no_num = (
        OrderedProfilerCardinality.get_basic_column_cardinality(pct_unique=0.5)
    )
    assert cardinality_with_large_pct_and_no_num.name == "NONE"
