from typing import Iterator

import pytest

import great_expectations.exceptions.exceptions as ge_exceptions
from great_expectations.core.batch import BatchDefinition, PartitionDefinition
from great_expectations.datasource.data_connector.sorter import (
    CustomListSorter,
    DateTimeSorter,
    LexicographicSorter,
    NumericSorter,
    Sorter,
)


@pytest.fixture()
def example_batch_def_list():
    a = BatchDefinition(
        datasource_name="A",
        data_connector_name="a",
        data_asset_name="james_20200810_1003",
        partition_definition=PartitionDefinition(
            {"name": "james", "timestamp": "20200810", "price": "1003"}
        ),
    )
    b = BatchDefinition(
        datasource_name="A",
        data_connector_name="b",
        data_asset_name="abe_20200809_1040",
        partition_definition=PartitionDefinition(
            {"name": "abe", "timestamp": "20200809", "price": "1040"}
        ),
    )
    c = BatchDefinition(
        datasource_name="A",
        data_connector_name="c",
        data_asset_name="eugene_20200809_1500",
        partition_definition=PartitionDefinition(
            {"name": "eugene", "timestamp": "20200809", "price": "1500"}
        ),
    )
    d = BatchDefinition(
        datasource_name="A",
        data_connector_name="d",
        data_asset_name="alex_20200819_1300",
        partition_definition=PartitionDefinition(
            {"name": "alex", "timestamp": "20200819", "price": "1300"}
        ),
    )
    e = BatchDefinition(
        datasource_name="A",
        data_connector_name="e",
        data_asset_name="alex_20200809_1000",
        partition_definition=PartitionDefinition(
            {"name": "alex", "timestamp": "20200809", "price": "1000"}
        ),
    )
    f = BatchDefinition(
        datasource_name="A",
        data_connector_name="f",
        data_asset_name="will_20200810_1001",
        partition_definition=PartitionDefinition(
            {"name": "will", "timestamp": "20200810", "price": "1001"}
        ),
    )
    g = BatchDefinition(
        datasource_name="A",
        data_connector_name="g",
        data_asset_name="eugene_20201129_1900",
        partition_definition=PartitionDefinition(
            {"name": "eugene", "timestamp": "20201129", "price": "1900"}
        ),
    )
    h = BatchDefinition(
        datasource_name="A",
        data_connector_name="h",
        data_asset_name="will_20200809_1002",
        partition_definition=PartitionDefinition(
            {"name": "will", "timestamp": "20200809", "price": "1002"}
        ),
    )
    i = BatchDefinition(
        datasource_name="A",
        data_connector_name="i",
        data_asset_name="james_20200811_1009",
        partition_definition=PartitionDefinition(
            {"name": "james", "timestamp": "20200811", "price": "1009"}
        ),
    )
    j = BatchDefinition(
        datasource_name="A",
        data_connector_name="j",
        data_asset_name="james_20200713_1567",
        partition_definition=PartitionDefinition(
            {"name": "james", "timestamp": "20200713", "price": "1567"}
        ),
    )
    return [a, b, c, d, e, f, g, h, i, j]


def test_create_three_batch_definitions_sort_lexicographically():
    a = BatchDefinition(
        datasource_name="A",
        data_connector_name="a",
        data_asset_name="aaa",
        partition_definition=PartitionDefinition({"id": "A"}),
    )
    b = BatchDefinition(
        datasource_name="B",
        data_connector_name="b",
        data_asset_name="bbb",
        partition_definition=PartitionDefinition({"id": "B"}),
    )
    c = BatchDefinition(
        datasource_name="C",
        data_connector_name="c",
        data_asset_name="ccc",
        partition_definition=PartitionDefinition({"id": "C"}),
    )

    batch_list = [a, b, c]

    # sorting by "id" reverse alphabetically (descending)
    my_sorter = LexicographicSorter(name="id", orderby="desc")
    sorted_batch_list = my_sorter.get_sorted_batch_definitions(
        batch_list,
    )
    assert sorted_batch_list == [c, b, a]

    # sorting by "id" reverse alphabetically (ascending)
    my_sorter = LexicographicSorter(name="id", orderby="asc")
    sorted_batch_list = my_sorter.get_sorted_batch_definitions(
        batch_list,
    )
    assert sorted_batch_list == [a, b, c]


def test_create_three_batch_definitions_sort_numerically():
    one = BatchDefinition(
        datasource_name="A",
        data_connector_name="a",
        data_asset_name="aaa",
        partition_definition=PartitionDefinition({"id": 1}),
    )
    two = BatchDefinition(
        datasource_name="B",
        data_connector_name="b",
        data_asset_name="bbb",
        partition_definition=PartitionDefinition({"id": 2}),
    )
    three = BatchDefinition(
        datasource_name="C",
        data_connector_name="c",
        data_asset_name="ccc",
        partition_definition=PartitionDefinition({"id": 3}),
    )

    batch_list = [one, two, three]
    my_sorter = NumericSorter(name="id", orderby="desc")
    sorted_batch_list = my_sorter.get_sorted_batch_definitions(batch_list)
    assert sorted_batch_list == [three, two, one]

    my_sorter = NumericSorter(name="id", orderby="asc")
    sorted_batch_list = my_sorter.get_sorted_batch_definitions(batch_list)
    assert sorted_batch_list == [one, two, three]

    # testing a non-numeric, which should throw an error
    i_should_not_work = BatchDefinition(
        datasource_name="C",
        data_connector_name="c",
        data_asset_name="ccc",
        partition_definition=PartitionDefinition({"id": "aaa"}),
    )

    batch_list = [one, two, three, i_should_not_work]
    with pytest.raises(ge_exceptions.SorterError):
        sorted_batch_list = my_sorter.get_sorted_batch_definitions(batch_list)


def test_date_time():
    first = BatchDefinition(
        datasource_name="A",
        data_connector_name="a",
        data_asset_name="aaa",
        partition_definition=PartitionDefinition({"date": "20210101"}),
    )
    second = BatchDefinition(
        datasource_name="B",
        data_connector_name="b",
        data_asset_name="bbb",
        partition_definition=PartitionDefinition({"date": "20210102"}),
    )
    third = BatchDefinition(
        datasource_name="C",
        data_connector_name="c",
        data_asset_name="ccc",
        partition_definition=PartitionDefinition({"date": "20210103"}),
    )

    batch_list = [first, second, third]
    my_sorter = DateTimeSorter(name="date", datetime_format="%Y%m%d", orderby="desc")
    sorted_batch_list = my_sorter.get_sorted_batch_definitions(batch_list)
    assert sorted_batch_list == [third, second, first]

    my_sorter = DateTimeSorter(name="date", datetime_format="%Y%m%d", orderby="asc")
    sorted_batch_list = my_sorter.get_sorted_batch_definitions(batch_list)
    assert sorted_batch_list == [first, second, third]

    with pytest.raises(ge_exceptions.SorterError):
        # numeric date_time_format
        i_dont_work = DateTimeSorter(name="date", datetime_format=12345, orderby="desc")

    my_date_is_not_a_string = BatchDefinition(
        datasource_name="C",
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
        datasource_name="A",
        data_connector_name="a",
        data_asset_name="aaa",
        partition_definition=PartitionDefinition({"element": "Hydrogen"}),
    )
    Helium = BatchDefinition(
        datasource_name="B",
        data_connector_name="b",
        data_asset_name="bbb",
        partition_definition=PartitionDefinition({"element": "Helium"}),
    )
    Lithium = BatchDefinition(
        datasource_name="C",
        data_connector_name="c",
        data_asset_name="ccc",
        partition_definition=PartitionDefinition({"element": "Lithium"}),
    )

    batch_list = [Hydrogen, Helium, Lithium]
    my_sorter = CustomListSorter(
        name="element", orderby="desc", reference_list=periodic_table_of_elements
    )
    sorted_batch_list = my_sorter.get_sorted_batch_definitions(batch_list)
    assert sorted_batch_list == [Lithium, Helium, Hydrogen]

    my_sorter = CustomListSorter(
        name="element", orderby="asc", reference_list=periodic_table_of_elements
    )
    sorted_batch_list = my_sorter.get_sorted_batch_definitions(batch_list)
    assert sorted_batch_list == [Hydrogen, Helium, Lithium]


def test_example_file_list_sorters(example_batch_def_list):
    [a, b, c, d, e, f, g, h, i, j] = example_batch_def_list
    batch_list = [a, b, c, d, e, f, g, h, i, j]

    name_sorter = LexicographicSorter(name="name", orderby="desc")
    timestamp_sorter = DateTimeSorter(
        name="timestamp", datetime_format="%Y%m%d", orderby="desc"
    )
    price_sorter = NumericSorter(name="price", orderby="desc")

    # 1. sorting just by name
    sorters_list = [name_sorter]
    sorters: Iterator[Sorter] = reversed(sorters_list)
    for sorter in sorters:
        sorted_batch_list = sorter.get_sorted_batch_definitions(
            batch_definitions=batch_list
        )
    assert sorted_batch_list == [f, h, a, i, j, c, g, d, e, b]

    # 2. sorting by timestamp + name
    sorters_list = [timestamp_sorter, name_sorter]
    sorters: Iterator[Sorter] = reversed(sorters_list)
    for sorter in sorters:
        sorted_batch_list = sorter.get_sorted_batch_definitions(
            batch_definitions=batch_list
        )
    assert sorted_batch_list == [g, d, i, a, f, b, c, e, h, j]

    # 3. sorting just by price + timestamp + name
    sorters_list = [price_sorter, timestamp_sorter, name_sorter]
    sorters: Iterator[Sorter] = reversed(sorters_list)
    for sorter in sorters:
        sorted_batch_list = sorter.get_sorted_batch_definitions(
            batch_definitions=batch_list
        )
    assert sorted_batch_list == [g, j, c, d, b, i, a, h, f, e]
