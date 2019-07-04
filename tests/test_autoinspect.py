"""
Tests for autoinspection framework.
"""

import pytest
from .test_utils import get_dataset
from .conftest import CONTEXTS

import great_expectations as ge


def test_no_autoinspection():
    df = ge.dataset.PandasDataset({"a": [1, 2, 3]}, profiler=None)
    suite = df.get_expectation_suite()

    assert len(suite["expectations"]) == 0


def test_default_no_autoinspection():
    df = ge.dataset.PandasDataset({"a": [1, 2, 3]})
    suite = df.get_expectation_suite()

    assert len(suite["expectations"]) == 0


@pytest.mark.parametrize("dataset_type", CONTEXTS)
def test_autoinspect_existing_dataset(dataset_type):
    # Get a basic dataset with no expectations
    df = get_dataset(dataset_type, {"a": [1, 2, 3]}, profiler=None)
    suite = df.get_expectation_suite()
    assert len(suite["expectations"]) == 0

    # Run autoinspect
    df.profile(ge.profile.ColumnsExistProfiler)
    suite = df.get_expectation_suite()

    # Ensure that autoinspect worked
    assert suite["expectations"] == \
        [{'expectation_type': 'expect_column_to_exist', 'kwargs': {'column': 'a'}}]


@pytest.mark.parametrize("dataset_type", CONTEXTS)
def test_autoinspect_columns_exist(dataset_type):
    df = get_dataset(
        dataset_type, {"a": [1, 2, 3]}, profiler=ge.profile.ColumnsExistProfiler)
    suite = df.get_expectation_suite()

    assert len(suite["expectations"]) == 1
    assert suite["expectations"] == \
        [{'expectation_type': 'expect_column_to_exist', 'kwargs': {'column': 'a'}}]


def test_autoinspect_warning():
    with pytest.raises(NotImplementedError):
        ge.dataset.Dataset(profiler=ge.profile.ColumnsExistProfiler)
