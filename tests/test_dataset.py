import pytest

from .test_utils import CONTEXTS, get_dataset


data = {
    "a": [2.0, 5.0],
    "b": [5, 5],
    "c": [0, 10],
    "d": [0, None],
}
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

    dataset = get_dataset(context, data, schemas=schemas.get(context))
    with pytest.raises(AttributeError):
        dataset.get_column_max.cache_info()
