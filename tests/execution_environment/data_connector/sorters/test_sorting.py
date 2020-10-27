import pytest
from typing import Iterator
from great_expectations.core.batch import (
    BatchDefinition,
    PartitionDefinition,
)

from great_expectations.execution_environment.data_connector.sorter import(
Sorter,
LexicographicSorter,
NumericSorter,
DateTimeSorter,
CustomListSorter
)

import great_expectations.exceptions.exceptions as ge_exceptions


def test_create_three_batch_definitions_sort_lexicographically():
    a = BatchDefinition(
        execution_environment_name="A",
        data_connector_name="a",
        data_asset_name="aaa",
        partition_definition=PartitionDefinition({"id": "A"}),
    )
    b = BatchDefinition(
        execution_environment_name="B",
        data_connector_name="b",
        data_asset_name="bbb",
        partition_definition=PartitionDefinition({"id": "B"}),
    )
    c = BatchDefinition(
        execution_environment_name="C",
        data_connector_name="c",
        data_asset_name="ccc",
        partition_definition=PartitionDefinition({"id": "C"}),
    )

    batch_list = [a, b, c]

    # sorting by "id" reverse alphabetically
    my_sorter = LexicographicSorter(name="id", orderby="desc")
    sorted_batch_list = my_sorter.get_sorted_batch_definitions(batch_list,)
    assert sorted_batch_list == [c, b, a]


def test_create_three_batch_definitions_sort_numerically():
    one = BatchDefinition(
        execution_environment_name="A",
        data_connector_name="a",
        data_asset_name="aaa",
        partition_definition=PartitionDefinition({"id": 1}),
    )
    two = BatchDefinition(
        execution_environment_name="B",
        data_connector_name="b",
        data_asset_name="bbb",
        partition_definition=PartitionDefinition({"id": 2}),
    )
    three = BatchDefinition(
        execution_environment_name="C",
        data_connector_name="c",
        data_asset_name="ccc",
        partition_definition=PartitionDefinition({"id": 3}),
    )

    batch_list = [one, two, three]
    my_sorter = NumericSorter(name="id", orderby="desc")
    sorted_batch_list = my_sorter.get_sorted_batch_definitions(batch_list)
    assert sorted_batch_list == [three, two, one]

    # testing a non-numeric, which should throw an error
    i_should_not_work = BatchDefinition(
        execution_environment_name="C",
        data_connector_name="c",
        data_asset_name="ccc",
        partition_definition=PartitionDefinition({"id": "aaa"}),
    )

    batch_list = [one, two, three, i_should_not_work]
    with pytest.raises(ge_exceptions.SorterError):
        sorted_batch_list = my_sorter.get_sorted_batch_definitions(batch_list)


def test_date_time():
    first = BatchDefinition(
        execution_environment_name="A",
        data_connector_name="a",
        data_asset_name="aaa",
        partition_definition=PartitionDefinition({"date": "20210101"}),
    )
    second = BatchDefinition(
        execution_environment_name="B",
        data_connector_name="b",
        data_asset_name="bbb",
        partition_definition=PartitionDefinition({"date": "20210102"}),
    )
    third = BatchDefinition(
        execution_environment_name="C",
        data_connector_name="c",
        data_asset_name="ccc",
        partition_definition=PartitionDefinition({"date": "20210103"}),
    )

    batch_list = [first, second, third]
    my_sorter = DateTimeSorter(name="date", datetime_format="%Y%m%d", orderby="desc")
    sorted_batch_list = my_sorter.get_sorted_batch_definitions(batch_list)
    assert sorted_batch_list == [third, second, first]

    with pytest.raises(ge_exceptions.SorterError):
        # numeric date_time_format
        i_dont_work = DateTimeSorter(name="date", datetime_format=12345, orderby="desc")


    my_date_is_not_a_string = BatchDefinition(
        execution_environment_name="C",
        data_connector_name="c",
        data_asset_name="ccc",
        partition_definition=PartitionDefinition({"date": 20210103}),
    )

    batch_list = [first, second, third, my_date_is_not_a_string]
    my_sorter = DateTimeSorter(name="date", datetime_format="%Y%m%d", orderby="desc")

    with pytest.raises(ge_exceptions.SorterError):
        sorted_batch_list = my_sorter.get_sorted_batch_definitions(batch_list)


def test_custom_list(periodic_table_of_elements):
    Hydrogen = BatchDefinition(
        execution_environment_name="A",
        data_connector_name="a",
        data_asset_name="aaa",
        partition_definition=PartitionDefinition({
            "element": "Hydrogen"
        })
    )
    Helium = BatchDefinition(
        execution_environment_name="B",
        data_connector_name="b",
        data_asset_name="bbb",
        partition_definition=PartitionDefinition({
            "element": "Helium"
        })
    )
    Lithium = BatchDefinition(
        execution_environment_name="C",
        data_connector_name="c",
        data_asset_name="ccc",
        partition_definition=PartitionDefinition({
            "element": "Lithium"
        })
    )

    batch_list = [Hydrogen, Helium, Lithium]
    my_sorter = CustomListSorter(name="element", orderby="desc", reference_list=periodic_table_of_elements)
    sorted_batch_list = my_sorter.get_sorted_batch_definitions(batch_list)
    assert sorted_batch_list == [Lithium, Helium, Hydrogen]


# <WILL> TODO
def test_combination_of_sorters():

    a = BatchDefinition(
        execution_environment_name= "A",
        data_connector_name="a",
        data_asset_name="james_20200810_1003",
        partition_definition=PartitionDefinition({"name": "james", "timestamp": "20200810", "price": "1003"}),
    )
    b = BatchDefinition(
        execution_environment_name="A",
        data_connector_name="a",
        data_asset_name="abe_20200809_1040",
        partition_definition=PartitionDefinition({"name": "abe", "timestamp": "20200809", "price": "1040"}),
    )
    c = BatchDefinition(
        execution_environment_name="A",
        data_connector_name="a",
        data_asset_name="eugene_20200809_1500",
        partition_definition=PartitionDefinition({"name": "eugene", "timestamp": "20200809", "price": "1500"}),
    )
    d = BatchDefinition(
        execution_environment_name="A",
        data_connector_name="a",
        data_asset_name="alex_20200819_1300",
        partition_definition=PartitionDefinition({"name": "alex", "timestamp": "20200819", "price": "1300"}),
    )
    e = BatchDefinition(
        execution_environment_name="A",
        data_connector_name="a",
        data_asset_name="alex_20200809_1000",
        partition_definition=PartitionDefinition({"name": "alex", "timestamp": "20200809", "price": "1000"}),
    )
    f = BatchDefinition(
        execution_environment_name="A",
        data_connector_name="a",
        data_asset_name="will_20200810_1001",
        partition_definition=PartitionDefinition({"name": "will", "timestamp": "20200810", "price": "1001"}),
    )
    g = BatchDefinition(
        execution_environment_name="A",
        data_connector_name="a",
        data_asset_name="eugene_20201129_1900",
        partition_definition=PartitionDefinition({"name": "eugene", "timestamp": "20201129", "price": "1900"}),
    )
    h = BatchDefinition(
        execution_environment_name="A",
        data_connector_name="a",
        data_asset_name="will_20200809_1002",
        partition_definition=PartitionDefinition({"name": "will", "timestamp": "20200809", "price": "1002"}),
    )
    i = BatchDefinition(
        execution_environment_name="A",
        data_connector_name="a",
        data_asset_name="james_20200811_1009",
        partition_definition=PartitionDefinition({"name": "james", "timestamp": "20200811", "price": "1009"}),
    )
    j = BatchDefinition(
        execution_environment_name="A",
        data_connector_name="a",
        data_asset_name="james_20200713_1567",
        partition_definition=PartitionDefinition({"name": "james", "timestamp": "20200713", "price": "1567"}),
    )

    batch_list = [a, b, c, d, e, f, g, h, i, j]

    name_sorter = LexicographicSorter(name="name", orderby="desc")
    timestamp_sorter = DateTimeSorter(name="timestamp", datetime_format="%Y%m%d", orderby="desc")
    price_sorter = NumericSorter(name="price", orderby="desc")

    sorters_list = [name_sorter, timestamp_sorter, price_sorter]

    # WILL :
    sorters: Iterator[Sorter] = reversed(sorters_list)
    for sorter in sorters:
        sorted_batch_list = sorter.get_sorted_batch_definitions(batch_definitions=batch_list)

    for i in sorted_batch_list:
        print(i.data_asset_name)


