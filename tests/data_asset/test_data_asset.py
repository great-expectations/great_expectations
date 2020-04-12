import pytest

from great_expectations import __version__ as ge_version
from great_expectations.core import ExpectationSuite, ExpectationConfiguration, ExpectationKwargs
from great_expectations.data_asset import DataAsset, FileDataAsset
from great_expectations.dataset import Dataset, PandasDataset


def test_data_asset_expectation_suite():
    asset = DataAsset()
    default_suite = ExpectationSuite(
        expectation_suite_name="default",
        data_asset_type="DataAsset",
        meta={
            "great_expectations.__version__": ge_version
        },
        expectations=[]
    )

    # We should have a default-initialized suite stored internally and available for getting
    assert asset._expectation_suite == default_suite
    assert asset.get_expectation_suite() == default_suite


def test_interactive_evaluation(dataset):
    # We should be able to enable and disable interactive evaluation

    # Default is on
    assert dataset.get_config_value("interactive_evaluation") is True
    res = dataset.expect_column_values_to_be_between("naturals", 1, 10, include_config=True)
    assert res.success is True

    # Disable
    dataset.set_config_value("interactive_evaluation", False)
    disable_res = dataset.expect_column_values_to_be_between("naturals", 1, 10)  # No need to explicitly include_config
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
    my_df.append_or_update_expectation(ExpectationConfiguration(expectation_type='foobar', kwargs={}))
    result = my_df.validate(catch_exceptions=True)

    # Find the foobar result
    idx = 0
    for idx, val_result in enumerate(result.results):
        if val_result.expectation_config.expectation_type == "foobar":
            break

    assert result.results[idx].success is False
    assert result.results[idx].expectation_config.expectation_type == "foobar"
    assert result.results[idx].expectation_config.kwargs == ExpectationKwargs()
    assert result.results[idx].exception_info["raised_exception"] is True
    assert "AttributeError: \'PandasDataset\' object has no attribute \'foobar\'" in \
           result.results[idx].exception_info["exception_traceback"]

    with pytest.raises(AttributeError):
        result = my_df.validate(catch_exceptions=False)

def test_append_or_update_expectation():
    # Note: some asserts within this test rely on strong assumptions about the ordering of expectation_suite._expectations

    asset = DataAsset()
    assert len(asset._expectation_suite.expectations) == 0

    asset.append_or_update_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_to_exist",
            kwargs={
                "column": "a"
            },
        )
    )
    assert len(asset._expectation_suite.expectations) == 1

    #If we try to append_or_update an expectation with the same type and column, it gets overwritten
    asset.append_or_update_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_to_exist",
            kwargs={
                "column": "a"
            },
        )
    )
    assert len(asset._expectation_suite.expectations) == 1

    #An expectation with the same type and different column creates a new expectation
    asset.append_or_update_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_to_exist",
            kwargs={
                "column": "b"
            },
        )
    )
    assert len(asset._expectation_suite.expectations) == 2

    #An expectation with the same column and different type creates a new expectation
    asset.append_or_update_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={
                "column": "b"
            },
        )
    )
    assert len(asset._expectation_suite.expectations) == 3

    #If we try to append_or_update an expectation with the same type and column, parameters get overwritten, too.
    assert "mostly" not in asset._expectation_suite.expectations[2].kwargs
    asset.append_or_update_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={
                "column": "b",
                "mostly": .9
            },
        )
    )
    assert len(asset._expectation_suite.expectations) == 3
    assert "mostly" in asset._expectation_suite.expectations[2].kwargs

    asset.append_or_update_expectation(
        ExpectationConfiguration(
            expectation_type="expect_table_row_count_to_be_between",
            kwargs={
                "min_value": 10,
                "max_value": 90,
            },
        )
    )
    assert len(asset._expectation_suite.expectations) == 4
    assert asset._expectation_suite.expectations[3].kwargs["max_value"] == 90

    #If an Expectation doesn't have a column kwarg, then the expectation_type alone is enough to match and overwrite
    asset.append_or_update_expectation(
        ExpectationConfiguration(
            expectation_type="expect_table_row_count_to_be_between",
            kwargs={
                "min_value": 20,
                "max_value": 80,
            },
        )
    )
    assert len(asset._expectation_suite.expectations) == 4
    assert asset._expectation_suite.expectations[3].kwargs["max_value"] == 80

    #Behavior for append_expectation is a true append. It never overwrites.
    asset.append_expectation(
        ExpectationConfiguration(
            expectation_type="expect_table_row_count_to_be_between",
            kwargs={
                "min_value": 20,
                "max_value": 80,
            },
        )
    )
    asset.append_expectation(
        ExpectationConfiguration(
            expectation_type="expect_table_row_count_to_be_between",
            kwargs={
                "min_value": 20,
                "max_value": 80,
            },
        )
    )
    asset.append_expectation(
        ExpectationConfiguration(
            expectation_type="expect_table_row_count_to_be_between",
            kwargs={
                "min_value": 20,
                "max_value": 80,
            },
        )
    )
    assert len(asset._expectation_suite.expectations) == 7

    # We should have 4 copies of this puppy
    assert len(asset._expectation_suite.find_expectations(
        expectation_type="expect_table_row_count_to_be_between"
    ))==4

    #...and if we now _append a matching one, they should all be deleted except the new one
    asset.append_or_update_expectation(
        ExpectationConfiguration(
            expectation_type="expect_table_row_count_to_be_between",
            kwargs={
                "min_value": 30,
                "max_value": 70,
            },
        )
    )
    assert len(asset._expectation_suite.find_expectations(
        expectation_type="expect_table_row_count_to_be_between"
    ))==1
    assert len(asset._expectation_suite.expectations) == 4
    assert asset._expectation_suite.expectations[3].kwargs["max_value"] == 70

    #Note: This behavior is questionable. You could imagine circumstances where
    # appending a fifth expectation, or updating all four in place would be better.
    # For now, that's how it works.

    # appending (not _appending) a couple more expectations with column names also adds duplicates...
    asset.append_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={
                "column": "b",
                "mostly": .9
            },
        )
    )
    asset.append_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={
                "column": "b",
                "mostly": .9
            },
        )
    )
    assert len(asset._expectation_suite.expectations) == 6

    # We should have 3 copies of this puppy
    assert len(asset._expectation_suite.find_expectations(
        expectation_type="expect_column_values_to_not_be_null"
    ))==3

    asset.append_or_update_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={
                "column": "b",
                "mostly": .8
            },
        )
    )
    assert len(asset._expectation_suite.find_expectations(
        expectation_type="expect_column_values_to_not_be_null"
    ))==1
    assert len(asset._expectation_suite.expectations) == 4
    assert asset._expectation_suite.expectations[3].kwargs["mostly"] == .8

    # It works for column pair expectations
    asset.append_or_update_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_pair_values_to_be_equal",
            kwargs={
                "column_A": "a",
                "column_B": "b",
                "mostly": .8
            },
        )
    )
    assert len(asset._expectation_suite.expectations) == 5

    asset.append_or_update_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_pair_values_to_be_equal",
            kwargs={
                "column_A": "a",
                "column_B": "b",
                "mostly": .8
            },
        )
    )
    assert len(asset._expectation_suite.expectations) == 5

    asset.append_or_update_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_pair_values_to_be_equal",
            kwargs={
                "column_A": "a",
                "column_B": "c",
                "mostly": .8
            },
        )
    )
    assert len(asset._expectation_suite.expectations) == 6

    asset.append_or_update_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_pair_values_to_be_equal",
            kwargs={
                "column_A": "b",
                "column_B": "a",
                "mostly": .8
            },
        )
    )
    assert len(asset._expectation_suite.expectations) == 7

    # It works for multicolumn expectations
    asset.append_or_update_expectation(
        ExpectationConfiguration(
            expectation_type="expect_multicolumn_values_to_be_unique",
            kwargs={
                "column_list": ["a","b"],
                "mostly": .8
            },
        )
    )
    assert len(asset._expectation_suite.expectations) == 8

    asset.append_or_update_expectation(
        ExpectationConfiguration(
            expectation_type="expect_multicolumn_values_to_be_unique",
            kwargs={
                "column_list": ["a","b"],
                "mostly": .8
            },
        )
    )
    assert len(asset._expectation_suite.expectations) == 8

    asset.append_or_update_expectation(
        ExpectationConfiguration(
            expectation_type="expect_multicolumn_values_to_be_unique",
            kwargs={
                "column_list": ["a",],
                "mostly": .8
            },
        )
    )
    assert len(asset._expectation_suite.expectations) == 9

    # Note: this specific behavior is a bit dubious, since uniqueness is commutative,
    # so ["a", "b"] is identical to ["b", "a"]
    asset.append_or_update_expectation(
        ExpectationConfiguration(
            expectation_type="expect_multicolumn_values_to_be_unique",
            kwargs={
                "column_list": ["b","a"],
                "mostly": .8
            },
        )
    )
    assert len(asset._expectation_suite.expectations) == 10
