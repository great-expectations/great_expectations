import logging
import pytest


from great_expectations.execution_environment.data_connector.partitioner.regex_partitioner import (
    Partition,
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
  "my_dir/james_20200811_1009.csv",
  "my_dir/abe_20200809_1040.csv",
  "my_dir/will_20200809_1002.csv",
  "my_dir/james_20200713_1567.csv",
  "my_dir/eugene_20201129_1900.csv",
  "my_dir/will_20200810_1001.csv",
  "my_dir/james_20200810_1003.csv",
  "my_dir/alex_20200819_1300.csv",
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
    assert returned_partitions == [
        Partition(name='alex-20200809-1000', definition={'group_0': 'alex', 'group_1': '20200809', 'group_2': '1000'}),
        Partition(name='eugene-20200809-1500', definition={'group_0': 'eugene', 'group_1': '20200809', 'group_2': '1500'}),
        Partition(name='james-20200811-1009', definition={'group_0': 'james', 'group_1': '20200811', 'group_2': '1009'}),
        Partition(name='abe-20200809-1040', definition={'group_0': 'abe', 'group_1': '20200809', 'group_2': '1040'}),
        Partition(name='will-20200809-1002', definition={'group_0': 'will', 'group_1': '20200809', 'group_2': '1002'}),
        Partition(name='james-20200713-1567', definition={'group_0': 'james', 'group_1': '20200713', 'group_2': '1567'}),
        Partition(name='eugene-20201129-1900', definition={'group_0': 'eugene', 'group_1': '20201129', 'group_2': '1900'}),
        Partition(name='will-20200810-1001', definition={'group_0': 'will', 'group_1': '20200810', 'group_2': '1001'}),
        Partition(name='james-20200810-1003', definition={'group_0': 'james', 'group_1': '20200810', 'group_2': '1003'}),
        Partition(name='alex-20200819-1300', definition={'group_0': 'alex', 'group_1': '20200819', 'group_2': '1300'}),
    ]

    # partition names
    returned_partition_names = my_partitioner.get_available_partition_names(batch_paths)
    assert returned_partition_names == [
        'alex-20200809-1000',
        'eugene-20200809-1500',
        'james-20200811-1009',
        'abe-20200809-1040',
        'will-20200809-1002',
        'james-20200713-1567',
        'eugene-20201129-1900',
        'will-20200810-1001',
        'james-20200810-1003',
        'alex-20200819-1300'
    ]
