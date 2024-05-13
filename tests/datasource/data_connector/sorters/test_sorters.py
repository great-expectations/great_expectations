import pytest

import great_expectations.exceptions as gx_exceptions
from great_expectations.datasource.data_connector.sorter import (
    LexicographicSorter,
    NumericSorter,
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


def test_sorter_instantiation_lexicographic():
    # Lexicographic
    my_lex = LexicographicSorter(name="lex", orderby="desc")
    assert isinstance(my_lex, LexicographicSorter)
    assert my_lex.name == "lex"
    assert my_lex.reverse is True


def test_sorter_instantiation_numeric():
    # NumericSorter
    my_num = NumericSorter(name="num", orderby="asc")
    assert isinstance(my_num, NumericSorter)
    assert my_num.name == "num"
    assert my_num.reverse is False
