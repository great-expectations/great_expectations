import pytest

from great_expectations import __version__ as ge_version
from great_expectations.core import ExpectationConfiguration, ExpectationSuite
from great_expectations.data_asset import DataAsset, FileDataAsset
from great_expectations.dataset import Dataset, PandasDataset


def test_data_asset_expectation_suite():
    asset = DataAsset()
    default_suite = ExpectationSuite(
        expectation_suite_name="default",
        data_asset_type="DataAsset",
        meta={"great_expectations_version": ge_version},
        expectations=[],
    )

    # We should have a default-initialized suite stored internally and available for getting
    assert asset._expectation_suite == default_suite
    assert asset.get_expectation_suite() == default_suite


def test_interactive_evaluation(dataset):
    # We should be able to enable and disable interactive evaluation

    # Default is on
    assert dataset.get_config_value("interactive_evaluation") is True
    res = dataset.expect_column_values_to_be_between(
        "naturals", 1, 10, include_config=True
    )
    assert res.success is True

    # Disable
    dataset.set_config_value("interactive_evaluation", False)
    disable_res = dataset.expect_column_values_to_be_between(
        "naturals", 1, 10
    )  # No need to explicitly include_config
    assert disable_res.success is None

    assert res.expectation_config == disable_res.expectation_config


def test_data_asset_name_inheritance(dataset):
    # A data_asset should have a generic type
    data_asset = DataAsset()
    assert data_asset.get_expectation_suite().data_asset_type == "DataAsset"

    # A FileDataAsset should pick up its type
    data_asset = FileDataAsset()
    assert data_asset.get_expectation_suite().data_asset_type == "FileDataAsset"

    # So should a Dataset
    data_asset = Dataset()
    assert data_asset.get_expectation_suite().data_asset_type == "Dataset"

    # Backends should *not* change the implementation
    assert dataset.get_expectation_suite().data_asset_type == "Dataset"

    # But custom classes should choose to
    class MyCustomDataset(Dataset):
        _data_asset_type = "MyCustomDataset"

    data_asset = MyCustomDataset()
    assert data_asset.get_expectation_suite().data_asset_type == "MyCustomDataset"


def test_catch_exceptions_with_bad_expectation_type():
    # We want to catch degenerate cases where an expectation suite is incompatible with
    my_df = PandasDataset({"x": range(10)})
    my_df._expectation_suite.append_expectation(
        ExpectationConfiguration(expectation_type="foobar", kwargs={})
    )
    result = my_df.validate(catch_exceptions=True)

    # Find the foobar result
    idx = 0
    for idx, val_result in enumerate(result.results):
        if val_result.expectation_config.expectation_type == "foobar":
            break

    assert result.results[idx].success is False
    assert result.results[idx].expectation_config.expectation_type == "foobar"
    assert result.results[idx].expectation_config.kwargs == {}
    assert result.results[idx].exception_info["raised_exception"] is True
    assert (
        "AttributeError: 'PandasDataset' object has no attribute 'foobar'"
        in result.results[idx].exception_info["exception_traceback"]
    )

    with pytest.raises(AttributeError):
        result = my_df.validate(catch_exceptions=False)


def test_valid_expectation_types(dataset, pandas_dataset):
    assert pandas_dataset.list_available_expectation_types() == [
        "expect_column_bootstrapped_ks_test_p_value_to_be_greater_than",
        "expect_column_chisquare_test_p_value_to_be_greater_than",
        "expect_column_distinct_values_to_be_in_set",
        "expect_column_distinct_values_to_contain_set",
        "expect_column_distinct_values_to_equal_set",
        "expect_column_kl_divergence_to_be_less_than",
        "expect_column_max_to_be_between",
        "expect_column_mean_to_be_between",
        "expect_column_median_to_be_between",
        "expect_column_min_to_be_between",
        "expect_column_most_common_value_to_be_in_set",
        "expect_column_pair_cramers_phi_value_to_be_less_than",
        "expect_column_pair_values_A_to_be_greater_than_B",
        "expect_column_pair_values_to_be_equal",
        "expect_column_pair_values_to_be_in_set",
        "expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than",
        "expect_column_proportion_of_unique_values_to_be_between",
        "expect_column_quantile_values_to_be_between",
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
        "expect_compound_columns_to_be_unique",
        "expect_multicolumn_sum_to_equal",
        "expect_multicolumn_values_to_be_unique",
        "expect_select_column_values_to_be_unique_within_record",
        "expect_table_column_count_to_be_between",
        "expect_table_column_count_to_equal",
        "expect_table_columns_to_match_ordered_list",
        "expect_table_columns_to_match_set",
        "expect_table_row_count_to_be_between",
        "expect_table_row_count_to_equal",
    ]
