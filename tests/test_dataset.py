import pytest

from .conftest import CONTEXTS
from .test_utils import get_dataset
from collections import OrderedDict

from great_expectations.dataset import PandasDataset

data = OrderedDict([
    ["a", [2.0, 5.0]],
    ["b", [5, 5]],
    ["c", [0, 10]],
    ["d", [0, None]]
])
schemas = {
    "SparkDFDataset": {
        "a": "float",
        "b": "int",
        "c": "int",
        "d": "int",
    },
}


@pytest.mark.parametrize('context', CONTEXTS)
def test_caching(context):
    dataset = get_dataset(context, data, schemas=schemas.get(context), caching=True)
    dataset.get_column_max('a')
    dataset.get_column_max('a')
    dataset.get_column_max('b')
    assert dataset.get_column_max.cache_info().hits == 1
    assert dataset.get_column_max.cache_info().misses == 2
    assert dataset.get_column_max.cache_info().misses == 2

    dataset = get_dataset(context, data, schemas=schemas.get(context), caching=False)
    with pytest.raises(AttributeError):
        dataset.get_column_max.cache_info()


@pytest.mark.parametrize('context', CONTEXTS)
def test_head(context):
    dataset = get_dataset(context, data, schemas=schemas.get(context), caching=True)
    dataset.expect_column_mean_to_be_between("b", 5, 5)
    head = dataset.head(1)
    assert isinstance(head, PandasDataset)
    assert len(head) == 1
    assert list(head.columns) == ["a", "b", "c", "d"]
    assert head["a"][0] == 2.0
    suite = head.get_expectation_suite()
    assert len(suite["expectations"]) == 5
