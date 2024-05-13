import pytest

import great_expectations.exceptions as gx_exceptions
from great_expectations.datasource.data_connector.sorter import (
    Sorter,
)

# module level markers
pytestmark = pytest.mark.unit


def test_sorter_instantiation_base():
    # base
    my_sorter = Sorter(name="base", orderby="asc")
    assert isinstance(my_sorter, Sorter)
    assert my_sorter.name == "base"
    # defaults
    assert my_sorter.reverse is False
    # with fake orderby
    with pytest.raises(gx_exceptions.SorterError):
        my_sorter = Sorter(name="base", orderby="fake")
