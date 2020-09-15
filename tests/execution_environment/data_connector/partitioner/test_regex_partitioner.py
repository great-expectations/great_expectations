import logging
import regex
import pytest



from great_expectations.execution_environment.execution_environment import (
    ExecutionEnvironment as exec,
)

from great_expectations.execution_environment.data_connector import (
    DataConnector,
    FilesDataConnector,
)

from great_expectations.execution_environment.data_connector.partitioner import(
    RegexPartitioner
)

try:
    from unittest import mock
except ImportError:
    import mock

logger = logging.getLogger(__name__)

"""
asset_param = {
    "test_asset": {
        "partition_regex": r"file_(.*)_(.*).csv",
        "partition_param": ["year", "file_num"],
        "partition_delimiter": "-",
        "reader_method": "read_csv",
    }
}

"""
batch_paths = [
  "my_dir/alex_20200809_1000.csv",
  "my_dir/eugene_20200809_1500.csv",
  "my_dir/abe_20200809_1040.csv",
  "my_dir/will_20200809_1002.csv",
  "my_dir/will_20200810_1001.csv",
]

def test_regex_partitioner():
    my_partitioner = RegexPartitioner(name="mine_all_mine")
    regex = r".*/(.*)_(.*)_(.*).csv"

    # test 1: no regex configured. we  raise error
    with pytest.raises(ValueError) as exc:
        partitions = my_partitioner.get_available_partitions(batch_paths)

    # set the regex
    my_partitioner.regex = regex
    returned_partitions = my_partitioner.get_available_partitions(batch_paths)
    print(returned_partitions)
    # compare full partitions
    assert returned_partitions == [{'partition_definition': {'group_0': 'alex', 'group_1': '20200809', 'group_2': '1000'}, 'partition_key': 'alex-20200809-1000'},
                                    {'partition_definition': {'group_0': 'eugene', 'group_1': '20200809', 'group_2': '1500'}, 'partition_key': 'eugene-20200809-1500'},
                                    {'partition_definition': {'group_0': 'abe', 'group_1': '20200809', 'group_2': '1040'}, 'partition_key': 'abe-20200809-1040'},
                                    {'partition_definition': {'group_0': 'will', 'group_1': '20200809', 'group_2': '1002'}, 'partition_key': 'will-20200809-1002'},
                                    {'partition_definition': {'group_0': 'will', 'group_1': '20200810', 'group_2': '1001'}, 'partition_key': 'will-20200810-1001'}]

    # partition keys
    returned_partition_keys = my_partitioner.get_available_partition_keys(batch_paths)
    assert returned_partition_keys == ['alex-20200809-1000', 'eugene-20200809-1500', 'abe-20200809-1040', 'will-20200809-1002', 'will-20200810-1001']
