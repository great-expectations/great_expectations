import logging

try:
    from unittest import mock
except ImportError:
    import mock

import pytest

from great_expectations.exceptions import SorterError

logger = logging.getLogger(__name__)

from great_expectations.execution_environment.data_connector.partitioner.sorter import (
    Sorter,
    LexicographicSorter,
    NumericSorter,
    CustomListSorter,
    DateTimeSorter,
)


def test_sorter_instantiation_base():
    # base
    my_sorter = Sorter(name="base", class_name="Sorter", orderby="asc")
    assert isinstance(my_sorter, Sorter)
    assert my_sorter.name == "base"

    # defaults
    assert my_sorter.reverse is False

    # with fake orderby
    with pytest.raises(SorterError):
        my_sorter = Sorter(name="base", class_name="Sorter", orderby="fake")


def test_sorter_instantiation_lexicographic():
    # Lexicographic
    my_lex = LexicographicSorter(name="lex", orderby="desc")
    assert isinstance(my_lex, LexicographicSorter)
    assert my_lex.name == "lex"
    assert my_lex.reverse is True


def test_sorter_instantiation_datetime():
    sorter_params: dict = {'config_params': {
        'datetime_format': '%Y%m%d',
    }}
    # DateTimeSorter
    my_dt = DateTimeSorter(name="dt", orderby="desc", **sorter_params)
    assert isinstance(my_dt, DateTimeSorter)
    assert my_dt.name == "dt"
    assert my_dt.reverse is True
    assert my_dt.config_params["datetime_format"] == '%Y%m%d'


def test_sorter_instantiation_numeric():
    # NumericSorter
    my_num = NumericSorter(name="num", orderby="asc")
    assert isinstance(my_num, NumericSorter)
    assert my_num.name == "num"
    assert my_num.reverse is False


def test_sorter_instantiation_custom_list():
    # CustomListSorter
    sorter_params: dict = {'config_params': {
        'reference_list': ['a', 'b', 'c'],
    }}

    my_custom = CustomListSorter(name="custom", orderby="asc", **sorter_params)
    assert isinstance(my_custom, CustomListSorter)
    assert my_custom.name == "custom"
    assert my_custom.reverse is False
    assert my_custom.config_params['reference_list'] == ['a', 'b', 'c']
