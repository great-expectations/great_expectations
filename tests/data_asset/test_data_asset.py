import pytest

from great_expectations import __version__ as ge_version
from great_expectations.core import ExpectationSuite, ExpectationConfiguration, ExpectationKwargs
from great_expectations.data_asset import DataAsset, FileDataAsset
from great_expectations.dataset import Dataset, PandasDataset


def test_data_asset_expectation_suite():
    asset = DataAsset()
    default_suite = ExpectationSuite(
        data_asset_name="default",
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
    my_df._append_expectation(ExpectationConfiguration(expectation_type='foobar', kwargs={}))
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
