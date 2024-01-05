import pytest

from great_expectations.data_context.data_context.abstract_data_context import (
    AbstractDataContext,
)
from great_expectations.datasource.fluent.interfaces import Datasource
from great_expectations.exceptions.exceptions import DataContextError


def test_save__success(empty_data_context: AbstractDataContext):
    datasource = empty_data_context.sources.add_pandas("my_datasource")

    datasource.save()
    assert empty_data_context.get_datasource("my_datasource") == datasource


def test_save__missing_data_context():
    datasource = Datasource(name="my_datasource", type="pandas")

    with pytest.raises(DataContextError):
        datasource.save()
