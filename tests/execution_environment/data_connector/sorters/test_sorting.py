import pytest
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

def test_create_two_batch_definitions_sort_lexicographically():
    A = BatchDefinition(
        "A",
        "a",
        "aaa",
        PartitionDefinition({
            "id": "A"
        })
    )
    B = BatchDefinition(
        "B",
        "b",
        "bbb",
        PartitionDefinition({
            "id": "B"
        })
    )
    C = BatchDefinition(
        "C",
        "c",
        "ccc",
        PartitionDefinition({
            "id": "C"
        })
    )
    D = BatchDefinition(
        "D",
        "d",
        "ddd",
        PartitionDefinition({
            "id": "D"
        })
    )

    batch_list = [A, B, C, D]
    #print(batch_list)
    #print("hi will see if we can do this in the next hour")
    for batch in batch_list:
        print(batch.data_asset_name)

    # attach a sorter to this list?
    # how do you compare?
    # how do you take a list
    print("-"*40)
    my_sorter = LexicographicSorter(name="id", orderby="desc")
    sorted_list = my_sorter.get_sorted_batch_definitions(batch_list,)
    for batch in sorted_list:
        print(batch.data_asset_name)

    # list of batch_definitions


# batch definition has a data_reference?
#

def test_create_two_batch_definitions_sort_numeric():
    one = BatchDefinition(
        "A",
        "a",
        "aaa",
        PartitionDefinition({
            "id": 1
        })
    )
    two = BatchDefinition(
        "B",
        "b",
        "bbb",
        PartitionDefinition({
            "id": 2
        })
    )
    three = BatchDefinition(
        "C",
        "c",
        "ccc",
        PartitionDefinition({
            "id": 3
        })
    )
    four = BatchDefinition(
        "D",
        "d",
        "ddd",
        PartitionDefinition({
            "id": 4
        })
    )

    batch_list = [one, two, three, four]
    for batch in batch_list:
        print(batch.data_asset_name)

    # attach a sorter to this list?
    # how do you compare?
    # how do you take a list
    print("-"*40)
    my_sorter = NumericSorter(name="id", orderby="desc")
    sorted_list = my_sorter.get_sorted_batch_definitions(batch_list,)
    for batch in sorted_list:
        print(batch.data_asset_name)

    # this works well

    # test error

    this_should_break = BatchDefinition(
        "D",
        "d",
        "ddd",
        PartitionDefinition({
            "id": 'aaa'
        })
    )

    batch_list = [one, two, three, four, this_should_break]
    with pytest.raises(ge_exceptions.SorterError):
        sorted_list = my_sorter.get_sorted_batch_definitions(batch_list)


def test_date_time():
    one = BatchDefinition(
        "A",
        "a",
        "aaa",
        PartitionDefinition({
            "date": "20201026"
        })
    )
    two = BatchDefinition(
        "B",
        "b",
        "bbb",
        PartitionDefinition({
            "date": "20201027"
        })
    )
    batch_list = [one, two]
    for batch in batch_list:
        print(batch.data_asset_name)
    print("-"*40)
    my_sorter = DateTimeSorter(name="date", datetime_format="%Y%m%d", orderby="desc")
    sorted_list = my_sorter.get_sorted_batch_definitions(batch_list)
    for batch in sorted_list:
        print(batch.data_asset_name)

    # two exceptions:
    # date_time_string having a string type
    # data_time_format_string being a string type

def test_custom_list():
    one = BatchDefinition(
        "A",
        "a",
        "aaa",
        PartitionDefinition({
            "element": "Hydrogen"
        })
    )
    two = BatchDefinition(
        "B",
        "b",
        "bbb",
        PartitionDefinition({
            "element": "Helium"
        })
    )

    batch_list = [one, two]
    for batch in batch_list:
        print(batch.data_asset_name)

    print("-"*40)
    ref = ["Hydrogen", "Helium"]
    my_sorter = CustomListSorter(name="element", orderby="desc", reference_list=ref)
    sorted_list = my_sorter.get_sorted_batch_definitions(batch_list)
    for batch in sorted_list:
        print(batch.data_asset_name)


def test_combination_of_sorters():
