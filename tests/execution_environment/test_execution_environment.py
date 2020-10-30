import pytest
import os
import shutil
import pandas as pd
import yaml

from typing import Union, List, Optional


from great_expectations.execution_environment import ExecutionEnvironment
from great_expectations.execution_environment.data_connector.files_data_connector import FilesDataConnector
from great_expectations.execution_environment.data_connector.partitioner.partition import Partition
from great_expectations.core.batch import (
    Batch,
    BatchRequest,
    BatchDefinition,
    PartitionDefinition,
)
from great_expectations.data_context.util import (
    file_relative_path,
    instantiate_class_from_config,
)
from tests.test_utils import (
    execution_environment_files_data_connector_regex_partitioner_config,
    create_files_for_regex_partitioner,
    create_files_in_directory,
)


@pytest.fixture
def basic_execution_environment(tmp_path_factory):
    base_directory: str = str(tmp_path_factory.mktemp("basic_execution_environment_filesystem_data_connector"))

    basic_execution_environment: ExecutionEnvironment = instantiate_class_from_config(yaml.load(f"""
class_name: ExecutionEnvironment

execution_engine:
    class_name: PandasExecutionEngine

data_connectors:
    my_filesystem_data_connector:
        class_name: FilesDataConnector
        base_directory: {base_directory}
        # TODO: <Alex>Investigate: this potentially breaks the data_reference centric design.</Alex>
        glob_directive: "*.csv"
        # glob_directive: "*"
        
        assets:
            Titanic: {{}}

        default_regex:
            # TODO: <Alex>Investigate: this potentially breaks the data_reference centric design.</Alex>
            pattern: (.+)_(\\d+)\\.csv
            # pattern: (.+)_(\\d+)\\.[a-z][a-z][a-z]
            group_names:
            - letter
            - number
    """, Loader=yaml.FullLoader), runtime_environment={
            "name": "my_execution_environment"
        },
        config_defaults={
            "module_name": "great_expectations.execution_environment"
        }
    )
    return basic_execution_environment


def test_get_batch_list_from_batch_request(basic_execution_environment):
    execution_environment_name: str = "my_execution_environment"
    data_connector_name: str = "my_filesystem_data_connector"
    data_asset_name: str = "Titanic"
    titanic_csv_source_file_path: str = file_relative_path(__file__, "../test_sets/Titanic.csv")
    base_directory: str = basic_execution_environment.get_data_connector(name=data_connector_name).base_directory
    titanic_csv_destination_file_path: str = str(os.path.join(base_directory, "Titanic_19120414.csv"))
    shutil.copy(titanic_csv_source_file_path, titanic_csv_destination_file_path)

    batch_request: dict = {
        "execution_environment_name": execution_environment_name,
        "data_connector_name": data_connector_name,
        "data_asset_name": data_asset_name,
        "partition_request": {
            "letter": "Titanic",
            "number": "19120414"
        },
        # "limit": None,
        # "batch_spec_passthrough": {
        #     "path": titanic_csv_destination_file_path,
        #     "reader_method": "read_csv",
        #     "reader_options": None,
        #     "limit": 2000
        # }
    }
    batch_request: BatchRequest = BatchRequest(**batch_request)
    batch_list: List[Batch] = basic_execution_environment.get_batch_list_from_batch_request(
        batch_request=batch_request
    )

    assert len(batch_list) == 1

    batch: Batch = batch_list[0]

    assert batch.batch_spec is not None
    assert isinstance(batch.data, pd.DataFrame)
    assert batch.data.shape[0] == 1313


def test_get_batch_with_caching():
    pass


def test_get_batch_with_pipeline_style_batch_request():
    test_df = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    execution_environment_name: str = "test_execution_environment"
    execution_environment_config: dict = execution_environment_files_data_connector_regex_partitioner_config(
        use_group_names=False,
        use_sorters=False,
    )[execution_environment_name]
    execution_environment_config.pop("class_name")

    execution_environment: ExecutionEnvironment = ExecutionEnvironment(
        name=execution_environment_name,
        **execution_environment_config,
        data_context_root_directory=None
    )
    data_connector_name: str = "test_pipeline_data_connector"
    data_asset_name: str = "test_asset_1"

    batch_request: dict = {
        "execution_environment_name": execution_environment_name,
        "data_connector_name": data_connector_name,
        "data_asset_name": data_asset_name,
        "batch_data": test_df,
        "partition_request": {
            "run_id": 1234567890,
        },
        "limit": None,
    }
    batch_request: BatchRequest = BatchRequest(**batch_request)
    batch_list: List[Batch] = execution_environment.get_batch_list_from_batch_request(
        batch_request=batch_request
    )

    assert len(batch_list) == 1

    batch: Batch = batch_list[0]

    assert batch.batch_spec is not None
    assert batch.batch_definition["data_asset_name"] == data_asset_name
    assert isinstance(batch.data, pd.DataFrame)
    assert batch.data.shape == (2, 2)
    assert batch.data["col2"].values[1] == 4


def test_get_batch_with_pipeline_style_batch_request_missing_partition_request_error():
    test_df = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    execution_environment_name: str = "test_execution_environment"
    execution_environment_config: dict = execution_environment_files_data_connector_regex_partitioner_config(
        use_group_names=False,
        use_sorters=False,
    )[execution_environment_name]
    execution_environment_config.pop("class_name")

    execution_environment: ExecutionEnvironment = ExecutionEnvironment(
        name=execution_environment_name,
        **execution_environment_config,
        data_context_root_directory=None
    )
    data_connector_name: str = "test_pipeline_data_connector"
    data_asset_name: str = "test_asset_1"

    batch_request: dict = {
        "execution_environment_name": execution_environment_name,
        "data_connector_name": data_connector_name,
        "data_asset_name": data_asset_name,
        "batch_data": test_df,
        "partition_request": None,
        "limit": None,
    }
    batch_request: BatchRequest = BatchRequest(**batch_request)
    with pytest.raises(AttributeError):
        # noinspection PyUnusedLocal
        batch_list: List[Batch] = execution_environment.get_batch_list_from_batch_request(
            batch_request=batch_request
        )


# TODO: <Alex>This test needs to be turned into several tests by replacing the all-purpose configuration with purpose-fit configuration sections using YAML.</Alex>
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
    execution_environment_config: dict = execution_environment_files_data_connector_regex_partitioner_config(
        use_group_names=False,
        use_sorters=False,
        default_base_directory=default_base_directory,
        data_asset_base_directory=data_asset_base_directory
    )[execution_environment_name]
    execution_environment_config.pop("class_name")
    execution_environment: ExecutionEnvironment = ExecutionEnvironment(
        name=execution_environment_name,
        **execution_environment_config,
        data_context_root_directory=project_dir_path
    )
    data_connector_names: Optional[Union[List, str]] = None

    # TODO: <Alex>Calling "get_batch_list_from_batch_request" (with the unused batch_list reult) is temporary (to fill up the caches), until this test is turned into multiple purpose-fit tests.</Alex>
    data_connector_name: str = "test_pipeline_data_connector"
    data_asset_name: str = "test_asset_1"
    test_df = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})
    batch_request: dict = {
        "execution_environment_name": execution_environment_name,
        "data_connector_name": data_connector_name,
        "data_asset_name": data_asset_name,
        "batch_data": test_df,
        "partition_request": {
            "run_id": 1234567890,
        },
        "limit": None,
    }
    batch_request: BatchRequest = BatchRequest(**batch_request)
    # noinspection PyUnusedLocal
    batch_list: List[Batch] = execution_environment.get_batch_list_from_batch_request(
        batch_request=batch_request
    )

    # TODO: <Alex>Update with richer tests.</Alex>
    # expected_data_asset_names: dict = {
    #     "test_pipeline_data_connector": ["test_asset_1"],
    #     "test_filesystem_data_connector": [
    #         "test_asset_0",
    #         "abe_20200809_1040", "james_20200811_1009", "eugene_20201129_1900",
    #         "will_20200809_1002", "eugene_20200809_1500", "james_20200810_1003",
    #         "alex_20200819_1300", "james_20200713_1567", "will_20200810_1001", "alex_20200809_1000"
    #     ]
    # }
    expected_data_asset_names: dict = {
        "test_pipeline_data_connector": ["test_asset_1"],
        "test_filesystem_data_connector": ["test_asset_0"]
    }

    available_data_asset_names: dict = execution_environment.get_available_data_asset_names(
        data_connector_names=data_connector_names
    )

    assert set(available_data_asset_names.keys()) == set(expected_data_asset_names.keys())
    for connector_name, asset_list in available_data_asset_names.items():
        assert set(asset_list) == set(expected_data_asset_names[connector_name])

    data_connector_names = ["test_filesystem_data_connector", "test_pipeline_data_connector"]

    # TODO: <Alex>Update with richer tests.</Alex>
    # expected_data_asset_names: dict = {
    #     "test_pipeline_data_connector": ["test_asset_1"],
    #     "test_filesystem_data_connector": [
    #         "test_asset_0",
    #         "abe_20200809_1040", "james_20200811_1009", "eugene_20201129_1900",
    #         "will_20200809_1002", "eugene_20200809_1500", "james_20200810_1003",
    #         "alex_20200819_1300", "james_20200713_1567", "will_20200810_1001", "alex_20200809_1000"
    #     ]
    # }
    expected_data_asset_names: dict = {
        "test_pipeline_data_connector": ["test_asset_1"],
        "test_filesystem_data_connector": ["test_asset_0"]
    }

    available_data_asset_names: dict = execution_environment.get_available_data_asset_names(
        data_connector_names=data_connector_names
    )

    assert set(available_data_asset_names.keys()) == set(expected_data_asset_names.keys())
    for connector_name, asset_list in available_data_asset_names.items():
        assert set(asset_list) == set(expected_data_asset_names[connector_name])

    data_connector_names = ["test_filesystem_data_connector"]

    # TODO: <Alex>Update with richer tests.</Alex>
    # expected_data_asset_names: dict = {
    #     "test_filesystem_data_connector": [
    #         "test_asset_0",
    #         "abe_20200809_1040", "james_20200811_1009", "eugene_20201129_1900",
    #         "will_20200809_1002", "eugene_20200809_1500", "james_20200810_1003",
    #         "alex_20200819_1300", "james_20200713_1567", "will_20200810_1001", "alex_20200809_1000"
    #     ]
    # }
    expected_data_asset_names: dict = {
        "test_filesystem_data_connector": ["test_asset_0"]
    }

    available_data_asset_names: dict = execution_environment.get_available_data_asset_names(
        data_connector_names=data_connector_names
    )

    assert set(available_data_asset_names.keys()) == set(expected_data_asset_names.keys())
    for connector_name, asset_list in available_data_asset_names.items():
        assert set(asset_list) == set(expected_data_asset_names[connector_name])

    data_connector_names = "test_filesystem_data_connector"

    # TODO: <Alex>Update with richer tests.</Alex>
    # expected_data_asset_names: dict = {
    #     "test_filesystem_data_connector": [
    #         "test_asset_0",
    #         "abe_20200809_1040", "james_20200811_1009", "eugene_20201129_1900",
    #         "will_20200809_1002", "eugene_20200809_1500", "james_20200810_1003",
    #         "alex_20200819_1300", "james_20200713_1567", "will_20200810_1001", "alex_20200809_1000"
    #     ]
    # }
    expected_data_asset_names: dict = {
        "test_filesystem_data_connector": ["test_asset_0"]
    }

    available_data_asset_names: dict = execution_environment.get_available_data_asset_names(
        data_connector_names=data_connector_names
    )

    assert set(available_data_asset_names.keys()) == set(expected_data_asset_names.keys())
    for connector_name, asset_list in available_data_asset_names.items():
        assert set(asset_list) == set(expected_data_asset_names[connector_name])

    data_connector_names = ["test_pipeline_data_connector"]

    expected_data_asset_names: dict = {
        "test_pipeline_data_connector": ["test_asset_1"]
    }

    available_data_asset_names: dict = execution_environment.get_available_data_asset_names(
        data_connector_names=data_connector_names
    )

    assert set(available_data_asset_names.keys()) == set(expected_data_asset_names.keys())
    for connector_name, asset_list in available_data_asset_names.items():
        assert set(asset_list) == set(expected_data_asset_names[connector_name])


def test_get_available_data_asset_names_with_caching():
    pass


# TODO: <Alex>Partitioner.find_or_create_partitions() has been deprecated.  We must develop a test for an equivalent functionality (e.g., "get_batch_list_from_batch_request()").</Alex>
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
    execution_environment_config: dict = execution_environment_files_data_connector_regex_partitioner_config(
        use_group_names=False,
        use_sorters=False,
        default_base_directory=default_base_directory,
        data_asset_base_directory=data_asset_base_directory
    )[execution_environment_name]
    execution_environment_config.pop("class_name")
    execution_environment: ExecutionEnvironment = ExecutionEnvironment(
        name=execution_environment_name,
        **execution_environment_config,
        data_context_root_directory=project_dir_path
    )

    data_connector_name: str = "test_filesystem_data_connector"

    available_partitions: List[Partition] = execution_environment.get_available_partitions(
        data_connector_name=data_connector_name,
        data_asset_name=None,
        partition_request={
            "custom_filter": None,
            "partition_name": None,
            "partition_definition": None,
            "partition_index": None,
            "limit": None,
        },
        runtime_parameters=None,
        repartition=False
    )

    assert len(available_partitions) == 10


def test_get_available_partitions_with_caching():
    pass


def test_some_very_basic_stuff(basic_execution_environment):
    my_data_connector: FilesDataConnector = basic_execution_environment.get_data_connector(
        name="my_filesystem_data_connector"
    )
    create_files_in_directory(
        my_data_connector.base_directory,
        ["A_1.csv", "A_2.csv", "A_3.csv", "B_1.csv", "B_2.csv", "B_3.csv"],
    )

    assert len(
        basic_execution_environment.get_available_batch_definitions(
            batch_request=BatchRequest(
                execution_environment_name="my_execution_environment",
                data_connector_name="my_filesystem_data_connector",
            )
        )
    ) == 6

    batch: Batch = basic_execution_environment.get_batch_from_batch_definition(
        batch_definition=BatchDefinition(
            execution_environment_name="my_execution_environment",
            data_connector_name="my_filesystem_data_connector",
            data_asset_name="B1",
            partition_definition=PartitionDefinition({
                "letter": "B",
                "number": "1",
            })
        )
    )

    assert batch.batch_request is None
    assert type(batch.data) == pd.DataFrame
    assert batch.batch_definition == BatchDefinition(
        execution_environment_name="my_execution_environment",
        data_connector_name="my_filesystem_data_connector",
        data_asset_name="B1",
        partition_definition=PartitionDefinition({
            "letter": "B",
            "number": "1",
        })
    )

    batch_list: List[Batch] = basic_execution_environment.get_batch_list_from_batch_request(batch_request=BatchRequest(
        execution_environment_name="my_execution_environment",
        data_connector_name="my_filesystem_data_connector",
        data_asset_name="B1",
        partition_request={
            "letter": "B",
            "number": "1",
        }
    ))
    assert len(batch_list) == 0

    batch_list: List[Batch] = basic_execution_environment.get_batch_list_from_batch_request(batch_request=BatchRequest(
        execution_environment_name="my_execution_environment",
        data_connector_name="my_filesystem_data_connector",
        data_asset_name="Titanic",
        partition_request={
            "letter": "B",
            "number": "1",
        }
    ))
    assert len(batch_list) == 1
    assert type(batch_list[0].data) == pd.DataFrame

    my_df: pd.DataFrame = pd.DataFrame({"x": range(10), "y": range(10)})
    batch: Batch = basic_execution_environment.get_batch_from_batch_definition(batch_definition=BatchDefinition(
        "my_execution_environment",
        "_pipeline",
        "_pipeline",
        partition_definition=PartitionDefinition({
            "some_random_id": 1
        })
    ), batch_data=my_df)
    assert batch.batch_request is None
