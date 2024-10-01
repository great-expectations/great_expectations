import pytest

from great_expectations.constants import DATAFRAME_REPLACEMENT_STR
from great_expectations.datasource.fluent.batch_identifier_util import make_batch_identifier


@pytest.mark.unit
def test_make_batch_identifier_with_dataframe():
    identifier_dict = {"a": 1, "b": 2, "dataframe": 10, "c": 3}
    batch_id = make_batch_identifier(identifier_dict)
    assert set(batch_id.keys()) == set(identifier_dict.keys())
    assert batch_id["dataframe"] == DATAFRAME_REPLACEMENT_STR


@pytest.mark.unit
def test_make_batch_identifier_without_dataframe():
    identifier_dict = {"a": 1, "b": 2, "c": 3}
    batch_id = make_batch_identifier(identifier_dict)
    assert set(batch_id.keys()) == set(identifier_dict.keys())
    assert "dataframe" not in batch_id


@pytest.mark.unit
def test_make_batch_identifier_empty_dict():
    batch_id = make_batch_identifier({})
    assert set(batch_id.keys()) == set()
