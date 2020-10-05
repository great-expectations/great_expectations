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

from great_expectations.execution_environment.data_connector.partitioner.partition import Partition


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

    # with incorrectly configured reference list
    sorter_params: dict = {'config_params': {
        'reference_list': [111, 222, 333]  # this shouldn't work. the reference list should only contain strings
    }}

    with pytest.raises(SorterError):
        my_custom = CustomListSorter(name="custom", orderby="asc", **sorter_params)

    sorter_params: dict = {'config_params': {
        'reference_list': None
    }}
    with pytest.raises(SorterError):
        my_custom = CustomListSorter(name="custom", orderby="asc", **sorter_params)

    sorter_params: dict = {'config_params': {
        'reference_list': 1 # not a list
    }}
    with pytest.raises(SorterError):
        my_custom = CustomListSorter(name="custom", orderby="asc", **sorter_params)




def test_sorter_instantiation_custom_list_with_periodic_table(periodic_table_of_elements):
    # CustomListSorter
    sorter_params: dict = {'config_params': {
        'reference_list': periodic_table_of_elements,
    }}

    my_custom = CustomListSorter(name="element", orderby="asc", **sorter_params)
    assert my_custom.reference_list == periodic_table_of_elements

    # This element exists : Hydrogen
    test_partition = Partition(name="test", data_asset_name="fake", definition={"element": "Hydrogen"}, source="nowhere")
    returned_partition_key = my_custom.get_partition_key(test_partition)
    assert returned_partition_key == 0

    # This element does not : Vibranium
    test_partition = Partition(name="test", data_asset_name="fake", definition={"element": "Vibranium"}, source="nowhere")
    with pytest.raises(SorterError):
        my_custom.get_partition_key(test_partition)

