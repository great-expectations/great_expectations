import logging
import pytest


from great_expectations.execution_environment.data_connector.partitioner.regex_partitioner import RegexPartitioner
from great_expectations.execution_environment.data_connector.partitioner.partition import Partition
from great_expectations.execution_environment.data_connector.partitioner.lexicographic_sorter import LexicographicSorter
from great_expectations.execution_environment.data_connector.partitioner.date_time_sorter import DateTimeSorter
from great_expectations.execution_environment.data_connector.partitioner.numeric_sorter import NumericSorter
from great_expectations.execution_environment.data_connector.partitioner.custom_list_sorter import CustomListSorter

try:
    from unittest import mock
except ImportError:
    import mock

logger = logging.getLogger(__name__)

# TODO: <Alex>We might wish to invent more cool paths to test and different column types and sort orders...</Alex>
# TODO: <WILL> Is the order always going to be from "left-to-right"? in the example below, what happens if you want the date-time to take priority?
# TODO: more tests :  more complex custom lists, and playing with asc desc combinations

def test_regex_partitioner_only_regex_configured():
    batch_paths = [
        "my_dir/alex_20200809_1000.csv",
        "my_dir/eugene_20200809_1500.csv",
        "my_dir/james_20200811_1009.csv",
        "my_dir/abe_20200809_1040.csv",
        "my_dir/will_20200809_1002.csv",
        "my_dir/james_20200713_1567.csv",
        "my_dir/eugene_20201129_1900.csv",
        "my_dir/will_20200810_1001.csv",
        "my_dir/james_20200810_1003.csv",
        "my_dir/alex_20200819_1300.csv",
    ]

    my_partitioner = RegexPartitioner(
        name="mine_all_mine",
    )

    regex = r".*/(.*)_.*_.*.csv"
    my_partitioner.regex = regex
    returned_partitions = my_partitioner.get_available_partitions(batch_paths)
    
    print(returned_partitions)
