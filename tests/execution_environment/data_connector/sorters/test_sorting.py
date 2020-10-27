import pytest
from great_expectations.core.batch import (
    BatchDefinition,
    PartitionDefinition,
)

from great_expectations.execution_environment.data_connector.sorter import(
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


def test_custom_list(periodic_table_of_elements):
    Hydrogen = BatchDefinition(
        "A",
        "a",
        "aaa",
        PartitionDefinition({
            "element": "Hydrogen"
        })
    )
    Helium = BatchDefinition(
        "B",
        "b",
        "bbb",
        PartitionDefinition({
            "element": "Helium"
        })
    )
    Lithium = BatchDefinition(
        "B",
        "b",
        "bbb",
        PartitionDefinition({
            "element": "Lithium"
        })
    )

    batch_list = [Hydrogen, Helium, Lithium]
    my_sorter = CustomListSorter(name="element", orderby="desc", reference_list=periodic_table_of_elements)
    sorted_batch_list = my_sorter.get_sorted_batch_definitions(batch_list)
    assert sorted_batch_list == [Lithium, Helium, Hydrogen]

