import logging
import pytest


from great_expectations.execution_environment.data_connector.partitioner.regex_partitioner import RegexPartitioner
from great_expectations.execution_environment.data_connector.partitioner.partition import Partition
from great_expectations.execution_environment.data_connector.partitioner.custom_list_sorter import CustomListSorter

try:
    from unittest import mock
except ImportError:
    import mock

logger = logging.getLogger(__name__)

# TODO: <Alex>We might wish to invent more cool paths to test and different column types and sort orders...</Alex>

PeriodicTable = ["Hydrogen", "Helium", "Lithium", "Beryllium", "Boron", "Carbon", "Nitrogen", "Oxygen", "Flourine", "Neon"]

batch_paths = [
  "my_dir/Boron.csv",
  "my_dir/Oxygen.csv",
  "my_dir/Hydrogen.csv",
]

def test_custom_list_sorter():
    my_partitioner = RegexPartitioner(
        name="mine_all_mine",
        sorters=[
            CustomListSorter(name='element', orderby='asc', reference_list=PeriodicTable)
        ]
    )

    regex = r".*/(.*).csv"
    my_partitioner.regex = regex
    batch_paths = [
        "my_dir/Boron.csv",
        "my_dir/Oxygen.csv",
        "my_dir/Hydrogen.csv",
    ]

    returned_partitions = my_partitioner.get_available_partitions(batch_paths)
    assert returned_partitions == [
        Partition(name="Hydrogen", definition={'element': 'Hydrogen'}),
        Partition(name="Boron", definition={'element': 'Boron'}),
        Partition(name="Oxygen", definition={'element': 'Oxygen'}),
        ]

    my_partitioner = RegexPartitioner(
        name="mine_all_mine",
        sorters=[
            CustomListSorter(name='element', orderby='asc', reference_list=PeriodicTable)
        ]
    )

    # now with an extra element
    batch_path_2= [
        "my_dir/Boron.csv",
        "my_dir/Oxygen.csv",
        "my_dir/Hydrogen.csv",
        "my_dir/Vibranium.csv",
    ]

    my_partitioner.regex = regex
    with pytest.raises(ValueError):
        returned_partitions = my_partitioner.get_available_partitions(batch_path_2)
