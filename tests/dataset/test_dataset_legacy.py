from collections import OrderedDict

import pytest

from great_expectations.dataset import PandasDataset
from tests.test_utils import get_dataset

data = OrderedDict([["a", [2.0, 5.0]], ["b", [5, 5]], ["c", [0, 10]], ["d", [0, None]]])
schemas = {
    "SparkDFDataset": {
        "a": "float",
        "b": "int",
        "c": "int",
        "d": "int",
    },
}


def test_caching(test_backend):
    dataset = get_dataset(
        test_backend, data, schemas=schemas.get(test_backend), caching=True
    )
    dataset.get_column_max("a")
    dataset.get_column_max("a")
    dataset.get_column_max("b")
    assert dataset.get_column_max.cache_info().hits == 1
    assert dataset.get_column_max.cache_info().misses == 2
    assert dataset.get_column_max.cache_info().misses == 2

    dataset = get_dataset(
        test_backend, data, schemas=schemas.get(test_backend), caching=False
    )
    with pytest.raises(AttributeError):
        dataset.get_column_max.cache_info()


def test_head(test_backend):
    dataset = get_dataset(
        test_backend, data, schemas=schemas.get(test_backend), caching=True
    )
    dataset.expect_column_mean_to_be_between("b", 5, 5)
    head = dataset.head(1)
    assert isinstance(head, PandasDataset)
    assert len(head) == 1
    assert list(head.columns) == ["a", "b", "c", "d"]
    assert head["a"][0] == 2.0
    suite = head.get_expectation_suite()
    assert len(suite.expectations) == 5

    # Interestingly, the original implementation failed to work for a single
    # column (it would always name the column "*").
    # This should also work if we only get a single column
    dataset = get_dataset(
        test_backend, {"a": data["a"]}, schemas=schemas.get(test_backend), caching=True
    )
    head = dataset.head(1)
    assert isinstance(head, PandasDataset)
    assert len(head) == 1
    assert list(head.columns) == ["a"]

    # We also needed special handling for empty tables in SqlalchemyDataset
    dataset = get_dataset(
        test_backend, {"a": []}, schemas=schemas.get(test_backend), caching=True
    )
    head = dataset.head(1)
    assert isinstance(head, PandasDataset)
    assert len(head) == 0
    assert list(head.columns) == ["a"]
