import pytest

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.batch import BatchDefinition
from great_expectations.core.id_dict import IDDict
from great_expectations.datasource.data_connector.sorter import (
    CustomListSorter,
    DateTimeSorter,
    LexicographicSorter,
    NumericSorter,
    Sorter,
)


def test_sorter_instantiation_base():
    # base
    my_sorter = Sorter(name="base", orderby="asc")
    assert isinstance(my_sorter, Sorter)
    assert my_sorter.name == "base"
    # defaults
    assert my_sorter.reverse is False
    # with fake orderby
    with pytest.raises(ge_exceptions.SorterError):
        my_sorter = Sorter(name="base", orderby="fake")


def test_sorter_instantiation_lexicographic():
    # Lexicographic
    my_lex = LexicographicSorter(name="lex", orderby="desc")
    assert isinstance(my_lex, LexicographicSorter)
    assert my_lex.name == "lex"
    assert my_lex.reverse is True


def test_sorter_instantiation_datetime():
    sorter_params: dict = {
        "datetime_format": "%Y%m%d",
    }
    # DateTimeSorter
    my_dt = DateTimeSorter(name="dt", orderby="desc", **sorter_params)
    assert isinstance(my_dt, DateTimeSorter)
    assert my_dt.name == "dt"
    assert my_dt.reverse is True
    assert my_dt._datetime_format == "%Y%m%d"


def test_sorter_instantiation_numeric():
    # NumericSorter
    my_num = NumericSorter(name="num", orderby="asc")
    assert isinstance(my_num, NumericSorter)
    assert my_num.name == "num"
    assert my_num.reverse is False


def test_sorter_instantiation_custom_list():
    # CustomListSorter
    sorter_params: dict = {
        "reference_list": ["a", "b", "c"],
    }
    my_custom = CustomListSorter(name="custom", orderby="asc", **sorter_params)
    assert isinstance(my_custom, CustomListSorter)
    assert my_custom.name == "custom"
    assert my_custom.reverse is False
    # noinspection PyProtectedMember
    assert my_custom._reference_list == ["a", "b", "c"]
    # with incorrectly configured reference list
    sorter_params: dict = {
        "reference_list": [
            111,
            222,
            333,
        ]  # this shouldn't work. the reference list should only contain strings
    }
    with pytest.raises(ge_exceptions.SorterError):
        my_custom = CustomListSorter(name="custom", orderby="asc", **sorter_params)
    sorter_params: dict = {"reference_list": None}
    with pytest.raises(ge_exceptions.SorterError):
        # noinspection PyUnusedLocal
        my_custom = CustomListSorter(name="custom", orderby="asc", **sorter_params)
    sorter_params: dict = {"reference_list": 1}  # not a list
    with pytest.raises(ge_exceptions.SorterError):
        # noinspection PyUnusedLocal
        my_custom = CustomListSorter(name="custom", orderby="asc", **sorter_params)


def test_sorter_instantiation_custom_list_with_periodic_table(
    periodic_table_of_elements,
):
    # CustomListSorter
    sorter_params: dict = {
        "reference_list": periodic_table_of_elements,
    }
    my_custom_sorter = CustomListSorter(name="element", orderby="asc", **sorter_params)
    # noinspection PyProtectedMember
    assert my_custom_sorter._reference_list == periodic_table_of_elements
    # This element exists : Hydrogen
    test_batch_def = BatchDefinition(
        datasource_name="test",
        data_connector_name="fake",
        data_asset_name="nowhere",
        batch_identifiers=IDDict({"element": "Hydrogen"}),
    )
    returned_partition_key = my_custom_sorter.get_batch_key(test_batch_def)
    assert returned_partition_key == 0

    # This element does not : Vibranium
    test_batch_def = BatchDefinition(
        datasource_name="test",
        data_connector_name="fake",
        data_asset_name="nowhere",
        batch_identifiers=IDDict({"element": "Vibranium"}),
    )
    with pytest.raises(ge_exceptions.SorterError):
        my_custom_sorter.get_batch_key(test_batch_def)
