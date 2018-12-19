"""
Tests for autoinspection framework.
"""

import pytest
from .test_utils import get_dataset

import great_expectations as ge
import great_expectations.dataset.autoinspect as autoinspect


def test_no_autoinspection():
    df = ge.dataset.PandasDataset({"a": [1, 2, 3]}, autoinspect_func=None)
    config = df.get_expectations_config()

    assert len(config["expectations"]) == 0


def test_default_no_autoinspection():
    df = ge.dataset.PandasDataset({"a": [1, 2, 3]})
    config = df.get_expectations_config()

    assert len(config["expectations"]) == 0


@pytest.mark.parametrize("dataset_type", ["PandasDataset", "SqlAlchemyDataset"])
def test_autoinspect_existing_dataset(dataset_type):
    # Get a basic dataset with no expectations
    df = get_dataset(dataset_type, {"a": [1, 2, 3]}, autoinspect_func=None)
    config = df.get_expectations_config()
    assert len(config["expectations"]) == 0

    # Run autoinspect
    df.autoinspect(autoinspect.columns_exist)
    config = df.get_expectations_config()

    # Ensure that autoinspect worked
    assert config["expectations"] == \
        [{'expectation_type': 'expect_column_to_exist', 'kwargs': {'column': 'a'}}]


@pytest.mark.parametrize("dataset_type", ["PandasDataset", "SqlAlchemyDataset"])
def test_autoinspect_columns_exist(dataset_type):
    df = get_dataset(
        dataset_type, {"a": [1, 2, 3]}, autoinspect_func=autoinspect.columns_exist)
    config = df.get_expectations_config()

    assert len(config["expectations"]) == 1
    assert config["expectations"] == \
        [{'expectation_type': 'expect_column_to_exist', 'kwargs': {'column': 'a'}}]


def test_autoinspect_warning():
    with pytest.warns(UserWarning, match="No columns list found in dataset; no autoinspection performed."):
        ge.dataset.Dataset(autoinspect_func=autoinspect.columns_exist)


def test_autoinspect_error():
    df = ge.dataset.Dataset()
    df.columns = [{"title": "nonstandard_columns"}]
    with pytest.raises(autoinspect.AutoInspectError) as autoinspect_error:
        df.autoinspect(autoinspect.columns_exist)
        assert autoinspect_error.message == "Unable to determine column names for this dataset."
