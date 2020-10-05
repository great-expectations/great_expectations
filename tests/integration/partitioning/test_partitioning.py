import sys
import pytest
import os
import shutil
from typing import List
from pathlib import Path

from great_expectations.data_context.types.base import DataContextConfig
from great_expectations.data_context import (
    BaseDataContext,
    DataContext,
)
from great_expectations.data_context.util import file_relative_path
from great_expectations.execution_environment import ExecutionEnvironment
from great_expectations.execution_environment.data_connector.data_connector import DataConnector
from great_expectations.execution_environment.data_connector.files_data_connector import FilesDataConnector
from great_expectations.execution_environment.data_connector.partitioner.partition import Partition
from great_expectations.util import gen_directory_tree_str
from tests.test_utils import safe_remove

# try:
#     from unittest import mock
# except ImportError:
#     from unittest import mock


# def test_config(execution_environment_files_data_connector_regex_partitioner_no_groups_no_sorters_data_context):
#     execution_environment_files_data_connector_regex_partitioner_no_groups_no_sorters_data_context.get_config().to_yaml(outfile=sys.stdout)


def test_return_all_available_partitions_unsorted(
    execution_environment_files_data_connector_regex_partitioner_no_groups_no_sorters_data_context
):
    data_context: DataContext = \
        execution_environment_files_data_connector_regex_partitioner_no_groups_no_sorters_data_context
    execution_environment_name: str = "test_execution_environment"
    data_connector_name: str = "test_filesystem_data_connector"
    execution_environment: ExecutionEnvironment = data_context.get_execution_environment(
        execution_environment_name=execution_environment_name
    )
    data_connector: DataConnector = execution_environment.get_data_connector(name=data_connector_name)
    available_partitions: List[Partition] = data_context.get_available_partitions(
        execution_environment_name=execution_environment_name,
        data_connector_name=data_connector_name,
        data_asset_name=None,
        partition_query={
            "custom_filter": None,
            "partition_name": None,
            "partition_definition": None,
            "limit": None,
            "partition_index": None,
        },
        in_memory_dataset=None,
        repartition=False
    )

    assert len(available_partitions) == 10

    # noinspection PyUnresolvedReferences
    base_directory: str = data_connector.base_directory

    expected_returned_partitions: List[Partition] = [
        Partition(
            name="james-20200810-1003",
            data_asset_name="james_20200810_1003",
            definition={"group_0": "james", "group_1": "20200810", "group_2": "1003"},
            source=f"{base_directory}/james_20200810_1003.csv"
        ),
        Partition(
            name="abe-20200809-1040",
            data_asset_name="abe_20200809_1040",
            definition={"group_0": "abe", "group_1": "20200809", "group_2": "1040"},
            source=f"{base_directory}/abe_20200809_1040.csv"
        ),
        Partition(
            name="eugene-20200809-1500",
            data_asset_name="eugene_20200809_1500",
            definition={"group_0": "eugene", "group_1": "20200809", "group_2": "1500"},
            source=f"{base_directory}/eugene_20200809_1500.csv"
        ),
        Partition(
            name="alex-20200819-1300",
            data_asset_name="alex_20200819_1300",
            definition={"group_0": "alex", "group_1": "20200819", "group_2": "1300"},
            source=f"{base_directory}/alex_20200819_1300.csv"
        ),
         Partition(
             name="alex-20200809-1000",
             data_asset_name="alex_20200809_1000",
             definition={"group_0": "alex", "group_1": "20200809", "group_2": "1000"},
             source=f"{base_directory}/alex_20200809_1000.csv"
         ),
        Partition(
            name="will-20200810-1001",
            data_asset_name="will_20200810_1001",
            definition={"group_0": "will", "group_1": "20200810", "group_2": "1001"},
            source=f"{base_directory}/will_20200810_1001.csv"
        ),
        Partition(
            name="eugene-20201129-1900",
            data_asset_name="eugene_20201129_1900",
            definition={"group_0": "eugene", "group_1": "20201129", "group_2": "1900"},
            source=f"{base_directory}/eugene_20201129_1900.csv"
        ),
        Partition(
            name="will-20200809-1002",
            data_asset_name="will_20200809_1002",
            definition={"group_0": "will", "group_1": "20200809", "group_2": "1002"},
            source=f"{base_directory}/will_20200809_1002.csv"
        ),
         Partition(
             name="james-20200811-1009",
             data_asset_name="james_20200811_1009",
             definition={"group_0": "james", "group_1": "20200811", "group_2": "1009"},
             source=f"{base_directory}/james_20200811_1009.csv"
         ),
         Partition(
             name="james-20200713-1567",
             data_asset_name="james_20200713_1567",
             definition={"group_0": "james", "group_1": "20200713", "group_2": "1567"},
             source=f"{base_directory}/james_20200713_1567.csv"
         ),
    ]

    assert available_partitions == expected_returned_partitions


def test_return_all_available_partitions_sorted(
    execution_environment_files_data_connector_regex_partitioner_with_groups_with_sorters_data_context
):
    data_context: DataContext = \
        execution_environment_files_data_connector_regex_partitioner_with_groups_with_sorters_data_context
    execution_environment_name: str = "test_execution_environment"
    data_connector_name: str = "test_filesystem_data_connector"
    execution_environment: ExecutionEnvironment = data_context.get_execution_environment(
        execution_environment_name=execution_environment_name
    )
    data_connector: DataConnector = execution_environment.get_data_connector(name=data_connector_name)
    available_partitions: List[Partition] = data_context.get_available_partitions(
        execution_environment_name=execution_environment_name,
        data_connector_name=data_connector_name,
        data_asset_name=None,
        partition_query={
            "custom_filter": None,
            "partition_name": None,
            "partition_definition": None,
            "limit": None,
            "partition_index": None,
        },
        in_memory_dataset=None,
        repartition=False
    )

    assert len(available_partitions) == 10

    # noinspection PyUnresolvedReferences
    base_directory: str = data_connector.base_directory

    expected_returned_partitions: List[Partition] = [
        Partition(
            name="eugene-20201129-1900",
            data_asset_name="eugene_20201129_1900",
            definition={"name": "eugene", "timestamp": "20201129", "price": "1900"},
            source=f"{base_directory}/eugene_20201129_1900.csv"
        ),
        Partition(
            name="alex-20200819-1300",
            data_asset_name="alex_20200819_1300",
            definition={"name": "alex", "timestamp": "20200819", "price": "1300"},
            source=f"{base_directory}/alex_20200819_1300.csv"
        ),
        Partition(
            name="james-20200811-1009",
            data_asset_name="james_20200811_1009",
            definition={"name": "james", "timestamp": "20200811", "price": "1009"},
            source=f"{base_directory}/james_20200811_1009.csv"
        ),
        Partition(
            name="james-20200810-1003",
            data_asset_name="james_20200810_1003",
            definition={"name": "james", "timestamp": "20200810", "price": "1003"},
            source=f"{base_directory}/james_20200810_1003.csv"
        ),
        Partition(
            name="will-20200810-1001",
            data_asset_name="will_20200810_1001",
            definition={"name": "will", "timestamp": "20200810", "price": "1001"},
            source=f"{base_directory}/will_20200810_1001.csv"
        ),
        Partition(
            name="eugene-20200809-1500",
            data_asset_name="eugene_20200809_1500",
            definition={"name": "eugene", "timestamp": "20200809", "price": "1500"},
            source=f"{base_directory}/eugene_20200809_1500.csv"
        ),
        Partition(
            name="abe-20200809-1040",
            data_asset_name="abe_20200809_1040",
            definition={"name": "abe", "timestamp": "20200809", "price": "1040"},
            source=f"{base_directory}/abe_20200809_1040.csv"
        ),
        Partition(
            name="will-20200809-1002",
            data_asset_name="will_20200809_1002",
            definition={"name": "will", "timestamp": "20200809", "price": "1002"},
            source=f"{base_directory}/will_20200809_1002.csv"
        ),
        Partition(
            name="alex-20200809-1000",
            data_asset_name="alex_20200809_1000",
            definition={"name": "alex", "timestamp": "20200809", "price": "1000"},
            source=f"{base_directory}/alex_20200809_1000.csv"
        ),
        Partition(
            name="james-20200713-1567",
            data_asset_name="james_20200713_1567",
            definition={"name": "james", "timestamp": "20200713", "price": "1567"},
            source=f"{base_directory}/james_20200713_1567.csv"
        ),
    ]

    assert available_partitions == expected_returned_partitions
