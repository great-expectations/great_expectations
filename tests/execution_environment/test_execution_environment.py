import os
import shutil
import pandas as pd

from typing import Union, List

# TODO: <Alex>Commenting out (temporarily).</Alex>
# import yaml

from great_expectations.execution_environment import ExecutionEnvironment
from great_expectations.execution_environment.data_connector.partitioner.partition import Partition
from great_expectations.core.batch import Batch
from great_expectations.data_context.util import file_relative_path
from tests.test_utils import (
    execution_environment_files_data_connector_regex_partitioner_config,
    create_files_for_regex_partitioner,
)


# TODO: <Alex>Commenting out (temporarily).</Alex>
# def test_basic_execution_environment_setup():
#
#     datasource = ExecutionEnvironment(
#         "my_pandas_datasource",
#         **yaml.load("""
# execution_engine:
#     module_name: great_expectations.execution_engine.pandas_execution_engine
#     class_name: PandasExecutionEngine
#     engine_spec_passthrough:
#         reader_method: read_csv
#         reader_options:
#         header: 0
#
# data_connector:
#     subdir:
#         module_name: great_expectations.execution_environment.data_connector.files_data_connector
#         class_name: FilesDataConnector
#         assets:
#         rapid_prototyping:
#             partitioner:
#             regex: /foo/(.*)\.csv
#             partition_id:
#                 - file
#         # engine spec passthrough at per-asset level
#         engine_spec_passthrough:
#             reader_method: read_csv
#             reader_options:
#             header: 0
#         base_path: /usr/data
#         # engine spec passthrough at a per-connector level
#         # closest to invocation overrides
#         engine_spec_passthrough:
#         reader_method: read_csv
#         reader_options:
#             header: 0
#
#     """, Loader=yaml.FullLoader)
#     )


def test_get_batch(tmp_path_factory):
    base_dir_path = str(tmp_path_factory.mktemp("project_dirs"))
    project_dir_path = os.path.join(base_dir_path, "project_path")
    os.mkdir(project_dir_path)

    os.makedirs(os.path.join(project_dir_path, "data"), exist_ok=True)
    os.makedirs(os.path.join(project_dir_path, "data/titanic"), exist_ok=True)

    titanic_csv_source_file_path: str = file_relative_path(__file__, "../test_sets/Titanic.csv")
    titanic_csv_destination_file_path: str = str(os.path.join(project_dir_path, "data/titanic/Titanic.csv"))
    shutil.copy(titanic_csv_source_file_path, titanic_csv_destination_file_path)

    default_base_directory: str = "data/titanic"
    data_asset_base_directory: Union[str, None] = None

    execution_environment_name: str = "test_execution_environment"
    execution_environment: ExecutionEnvironment = ExecutionEnvironment(
        name=execution_environment_name,
        **execution_environment_files_data_connector_regex_partitioner_config(
            use_group_names=False,
            use_sorters=False,
            default_base_directory=default_base_directory,
            data_asset_base_directory=data_asset_base_directory
        )[execution_environment_name],
        in_memory_dataset=None,
        data_context_root_directory=project_dir_path
    )
    data_connector_name: str = "test_filesystem_data_connector"
    data_asset_name: str = "Titanic"

    batch_definition: dict = {
        "execution_environment": execution_environment_name,
        "data_connector": data_connector_name,
        "data_asset_name": data_asset_name,
        "partition_query": None,
        "limit": None,
        "batch_spec_passthrough": {
            "path": titanic_csv_destination_file_path,
            "reader_method": "read_csv",
            "reader_options": None,
            "limit": 2000
        }
    }
    batch: Batch = execution_environment.get_batch(
        batch_definition=batch_definition
    )

    assert batch.batch_spec is not None
    assert batch.batch_spec["data_asset_name"] == data_asset_name
    assert isinstance(batch.data, pd.DataFrame)
    assert batch.data.shape[0] == 1313


def test_get_batch_with_caching():
    pass


def test_get_batch_with_pipeline_style_batch_definition():
    test_df = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    execution_environment_name: str = "test_execution_environment"
    execution_environment: ExecutionEnvironment = ExecutionEnvironment(
        name=execution_environment_name,
        **execution_environment_files_data_connector_regex_partitioner_config(
            use_group_names=False,
            use_sorters=False,
            default_base_directory=None,
            data_asset_base_directory=None
        )[execution_environment_name],
        in_memory_dataset=test_df,
        data_context_root_directory=None
    )
    data_connector_name: str = "test_pipeline_data_connector"
    data_asset_name: str = "test_asset_0"

    batch_definition: dict = {
        "execution_environment": execution_environment_name,
        "data_connector": data_connector_name,
        "data_asset_name": data_asset_name,
        "partition_query": None,
        "limit": None,
    }
    batch: Batch = execution_environment.get_batch(
        batch_definition=batch_definition
    )
    assert batch.batch_spec is not None
    assert batch.batch_spec["data_asset_name"] == data_asset_name
    assert isinstance(batch.data, pd.DataFrame)
    assert batch.data.shape == (2, 2)
    assert batch.data["col2"].values[1] == 4


def test_get_available_data_asset_names(tmp_path_factory):
    base_dir_path = str(tmp_path_factory.mktemp("project_dirs"))
    project_dir_path = os.path.join(base_dir_path, "project_path")
    os.mkdir(project_dir_path)

    os.makedirs(os.path.join(project_dir_path, "data"), exist_ok=True)
    os.makedirs(os.path.join(project_dir_path, "data/test_files"), exist_ok=True)

    default_base_directory: str = "data/test_files"
    data_asset_base_directory: Union[str, None] = None

    base_directory_names: list = [default_base_directory, data_asset_base_directory]
    create_files_for_regex_partitioner(root_directory_path=project_dir_path, directory_paths=base_directory_names)

    execution_environment_name: str = "test_execution_environment"
    execution_environment: ExecutionEnvironment = ExecutionEnvironment(
        name=execution_environment_name,
        **execution_environment_files_data_connector_regex_partitioner_config(
            use_group_names=False,
            use_sorters=False,
            default_base_directory=default_base_directory,
            data_asset_base_directory=data_asset_base_directory
        )[execution_environment_name],
        in_memory_dataset=None,
        data_context_root_directory=project_dir_path
    )
    data_connector_names: Union[List, str, None] = None

    expected_data_asset_names: dict = {
        "test_pipeline_data_connector": ["test_asset_1"],
        "test_filesystem_data_connector": [
            "test_asset_0",
            "abe_20200809_1040", "james_20200811_1009", "eugene_20201129_1900",
            "will_20200809_1002", "eugene_20200809_1500", "james_20200810_1003",
            "alex_20200819_1300", "james_20200713_1567", "will_20200810_1001", "alex_20200809_1000"
        ]
    }

    available_data_asset_names: dict = execution_environment.get_available_data_asset_names(
        data_connector_names=data_connector_names
    )

    assert set(available_data_asset_names.keys()) == set(expected_data_asset_names.keys())
    for connector_name, asset_list in available_data_asset_names.items():
        assert set(asset_list) == set(expected_data_asset_names[connector_name])

    data_connector_names = ["test_filesystem_data_connector", "test_pipeline_data_connector"]

    expected_data_asset_names: dict = {
        "test_pipeline_data_connector": ["test_asset_1"],
        "test_filesystem_data_connector": [
            "test_asset_0",
            "abe_20200809_1040", "james_20200811_1009", "eugene_20201129_1900",
            "will_20200809_1002", "eugene_20200809_1500", "james_20200810_1003",
            "alex_20200819_1300", "james_20200713_1567", "will_20200810_1001", "alex_20200809_1000"
        ]
    }

    available_data_asset_names: dict = execution_environment.get_available_data_asset_names(
        data_connector_names=data_connector_names
    )

    assert set(available_data_asset_names.keys()) == set(expected_data_asset_names.keys())
    for connector_name, asset_list in available_data_asset_names.items():
        assert set(asset_list) == set(expected_data_asset_names[connector_name])

    data_connector_names = ["test_filesystem_data_connector"]

    expected_data_asset_names: dict = {
        'test_filesystem_data_connector': [
            'test_asset_0',
            'abe_20200809_1040', 'james_20200811_1009', 'eugene_20201129_1900',
            'will_20200809_1002', 'eugene_20200809_1500', 'james_20200810_1003',
            'alex_20200819_1300', 'james_20200713_1567', 'will_20200810_1001', 'alex_20200809_1000'
        ]
    }

    available_data_asset_names: dict = execution_environment.get_available_data_asset_names(
        data_connector_names=data_connector_names
    )

    assert set(available_data_asset_names.keys()) == set(expected_data_asset_names.keys())
    for connector_name, asset_list in available_data_asset_names.items():
        assert set(asset_list) == set(expected_data_asset_names[connector_name])

    data_connector_names = "test_filesystem_data_connector"

    expected_data_asset_names: dict = {
        'test_filesystem_data_connector': [
            'test_asset_0',
            'abe_20200809_1040', 'james_20200811_1009', 'eugene_20201129_1900',
            'will_20200809_1002', 'eugene_20200809_1500', 'james_20200810_1003',
            'alex_20200819_1300', 'james_20200713_1567', 'will_20200810_1001', 'alex_20200809_1000'
        ]
    }

    available_data_asset_names: dict = execution_environment.get_available_data_asset_names(
        data_connector_names=data_connector_names
    )

    assert set(available_data_asset_names.keys()) == set(expected_data_asset_names.keys())
    for connector_name, asset_list in available_data_asset_names.items():
        assert set(asset_list) == set(expected_data_asset_names[connector_name])

    data_connector_names = ["test_pipeline_data_connector"]

    expected_data_asset_names: dict = {
        'test_pipeline_data_connector': ['test_asset_1']
    }

    available_data_asset_names: dict = execution_environment.get_available_data_asset_names(
        data_connector_names=data_connector_names
    )

    assert set(available_data_asset_names.keys()) == set(expected_data_asset_names.keys())
    for connector_name, asset_list in available_data_asset_names.items():
        assert set(asset_list) == set(expected_data_asset_names[connector_name])


def test_get_available_data_asset_names_with_caching():
    pass


def test_get_available_partitions(tmp_path_factory):
    base_dir_path = str(tmp_path_factory.mktemp("project_dirs"))
    project_dir_path = os.path.join(base_dir_path, "project_path")
    os.mkdir(project_dir_path)

    os.makedirs(os.path.join(project_dir_path, "data"), exist_ok=True)
    os.makedirs(os.path.join(project_dir_path, "data/test_files"), exist_ok=True)

    default_base_directory: str = "data/test_files"
    data_asset_base_directory: Union[str, None] = None

    base_directory_names: list = [default_base_directory, data_asset_base_directory]
    create_files_for_regex_partitioner(root_directory_path=project_dir_path, directory_paths=base_directory_names)

    execution_environment_name: str = "test_execution_environment"
    execution_environment: ExecutionEnvironment = ExecutionEnvironment(
        name=execution_environment_name,
        **execution_environment_files_data_connector_regex_partitioner_config(
            use_group_names=False,
            use_sorters=False,
            default_base_directory=default_base_directory,
            data_asset_base_directory=data_asset_base_directory
        )[execution_environment_name],
        in_memory_dataset=None,
        data_context_root_directory=project_dir_path
    )

    data_connector_name: str = "test_filesystem_data_connector"

    available_partitions: List[Partition] = execution_environment.get_available_partitions(
        data_connector_name=data_connector_name,
        data_asset_name=None,
        partition_query={
            "custom_filter": None,
            "partition_name": None,
            "partition_definition": None,
            "limit": None,
            "partition_index": None,
        },
        repartition=False
    )

    assert len(available_partitions) == 10


def test_get_available_partitions_with_caching():
    pass
