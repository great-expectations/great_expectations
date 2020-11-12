import os
import shutil

import pandas as pd
import pytest
import yaml

from typing import Union, List, Optional

from great_expectations.execution_environment.data_connector import ConfiguredAssetFilesystemDataConnector
from great_expectations.core.batch import (
    Batch,
    BatchDefinition,
    BatchRequest,
    PartitionDefinition,
)
from great_expectations.data_context.util import (
    file_relative_path,
    instantiate_class_from_config,
)
from great_expectations.execution_environment import ExecutionEnvironment
from tests.test_utils import create_files_in_directory
import great_expectations.exceptions as ge_exceptions


@pytest.fixture
def basic_execution_environment(tmp_path_factory):
    base_directory: str = str(tmp_path_factory.mktemp("basic_execution_environment_filesystem_data_connector"))

    basic_execution_environment: ExecutionEnvironment = instantiate_class_from_config(yaml.load(f"""
class_name: ExecutionEnvironment

execution_engine:
    class_name: PandasExecutionEngine

data_connectors:
    test_runtime_data_connector:
        module_name: great_expectations.execution_environment.data_connector
        class_name: RuntimeDataConnector
        runtime_keys:
            - pipeline_stage_name
            - run_id

    my_filesystem_data_connector:
        class_name: ConfiguredAssetFilesystemDataConnector
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


@pytest.fixture
def sample_execution_environment_with_single_partition_file_data_connector(tmp_path_factory):
    base_directory: str = str(
        tmp_path_factory.mktemp(
            "basic_execution_environment_single_partition_filesystem_data_connector"
        )
    )

    sample_execution_environment: ExecutionEnvironment = instantiate_class_from_config(yaml.load(f"""
class_name: ExecutionEnvironment

execution_engine:
    class_name: PandasExecutionEngine

data_connectors:
    test_runtime_data_connector:
        module_name: great_expectations.execution_environment.data_connector
        class_name: RuntimeDataConnector
        runtime_keys:
            - pipeline_stage_name
            - run_id

    my_filesystem_data_connector:
        class_name: InferredAssetFilesystemDataConnector
        base_directory: {base_directory}
        # TODO: <Alex>Investigate: this potentially breaks the data_reference centric design.</Alex>
        glob_directive: "*.csv"
        # glob_directive: "*"
        
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

    sample_file_names: List[str] = [
        "alex_20200809_1000.csv",
        "eugene_20200809_1500.csv",
        "james_20200811_1009.csv",
        "abe_20200809_1040.csv",
        "will_20200809_1002.csv",
        "james_20200713_1567.csv",
        "eugene_20201129_1900.csv",
        "will_20200810_1001.csv",
        "james_20200810_1003.csv",
        "alex_20200819_1300.csv",
    ]

    create_files_in_directory(
        directory=base_directory,
        file_name_list=sample_file_names
    )

    return sample_execution_environment


def test_some_very_basic_stuff(basic_execution_environment):
    my_data_connector: ConfiguredAssetFilesystemDataConnector = basic_execution_environment.data_connectors["my_filesystem_data_connector"]
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

    # TODO Abe 20201104: Make sure this is what we truly want to do.
    assert batch.batch_request == {}
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
            "partition_identifiers": {
                "letter": "B",
                "number": "1",
            }
        }
    ))
    assert len(batch_list) == 0

    batch_list: List[Batch] = basic_execution_environment.get_batch_list_from_batch_request(batch_request=BatchRequest(
        execution_environment_name="my_execution_environment",
        data_connector_name="my_filesystem_data_connector",
        data_asset_name="Titanic",
        partition_request={
            "partition_identifiers": {
                "letter": "B",
                "number": "1",
            }
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
    # TODO Abe 20201104: Make sure this is what we truly want to do.
    assert batch.batch_request == {}


def test_get_batch_list_from_batch_request(basic_execution_environment):
    execution_environment_name: str = "my_execution_environment"
    data_connector_name: str = "my_filesystem_data_connector"
    data_asset_name: str = "Titanic"
    titanic_csv_source_file_path: str = file_relative_path(__file__, "../test_sets/Titanic.csv")
    base_directory: str = basic_execution_environment.data_connectors[data_connector_name].base_directory
    titanic_csv_destination_file_path: str = str(os.path.join(base_directory, "Titanic_19120414.csv"))
    shutil.copy(titanic_csv_source_file_path, titanic_csv_destination_file_path)

    batch_request: dict = {
        "execution_environment_name": execution_environment_name,
        "data_connector_name": data_connector_name,
        "data_asset_name": data_asset_name,
        "partition_request": {
            "partition_identifiers": {
                "letter": "Titanic",
                "number": "19120414"
            }
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
    assert batch.batch_markers["pandas_data_fingerprint"] == "3aaabc12402f987ff006429a7756f5cf"


def test_get_batch_with_caching():
    pass


def test_get_batch_with_pipeline_style_batch_request(basic_execution_environment):
    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    data_connector_name: str = "test_runtime_data_connector"
    data_asset_name: str = "IN_MEMORY_DATA_ASSET"

    batch_request: dict = {
        "execution_environment_name": basic_execution_environment.name,
        "data_connector_name": data_connector_name,
        "data_asset_name": data_asset_name,
        "batch_data": test_df,
        "partition_request": {
            "partition_identifiers": {
                "run_id": 1234567890,
            }
        },
        "limit": None,
    }
    batch_request: BatchRequest = BatchRequest(**batch_request)
    batch_list: List[Batch] = basic_execution_environment.get_batch_list_from_batch_request(
        batch_request=batch_request
    )

    assert len(batch_list) == 1

    batch: Batch = batch_list[0]

    assert batch.batch_spec is not None
    assert batch.batch_definition["data_asset_name"] == data_asset_name
    assert isinstance(batch.data, pd.DataFrame)
    assert batch.data.shape == (2, 2)
    assert batch.data["col2"].values[1] == 4
    assert batch.batch_markers["pandas_data_fingerprint"] == "1e461a0df5fe0a6db2c3bc4ef88ef1f0"


def test_get_batch_with_pipeline_style_batch_request_missing_partition_request_error(basic_execution_environment):
    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    data_connector_name: str = "test_runtime_data_connector"
    data_asset_name: str = "test_asset_1"

    batch_request: dict = {
        "execution_environment_name": basic_execution_environment.name,
        "data_connector_name": data_connector_name,
        "data_asset_name": data_asset_name,
        "batch_data": test_df,
        "partition_request": None,
        "limit": None,
    }
    batch_request: BatchRequest = BatchRequest(**batch_request)
    with pytest.raises(ge_exceptions.DataConnectorError):
        # noinspection PyUnusedLocal
        batch_list: List[Batch] = basic_execution_environment.get_batch_list_from_batch_request(
            batch_request=batch_request
        )


def test_get_available_data_asset_names_with_configured_asset_filesystem_data_connector(basic_execution_environment):
    data_connector_names: Optional[Union[List, str]] = None

    # Call "get_batch_list_from_batch_request()" to fill up the caches
    data_connector_name: str = "test_runtime_data_connector"
    data_asset_name: str = "IN_MEMORY_DATA_ASSET"
    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})
    batch_request: dict = {
        "execution_environment_name": basic_execution_environment.name,
        "data_connector_name": data_connector_name,
        "data_asset_name": data_asset_name,
        "batch_data": test_df,
        "partition_request": {
            "partition_identifiers": {
                "run_id": 1234567890,
            }
        },
        "limit": None,
    }
    batch_request: BatchRequest = BatchRequest(**batch_request)
    # noinspection PyUnusedLocal
    batch_list: List[Batch] = basic_execution_environment.get_batch_list_from_batch_request(
        batch_request=batch_request
    )

    expected_data_asset_names: dict = {
        "test_runtime_data_connector": [data_asset_name],
        "my_filesystem_data_connector": ["Titanic"]
    }

    available_data_asset_names: dict = basic_execution_environment.get_available_data_asset_names(
        data_connector_names=data_connector_names
    )

    assert set(available_data_asset_names.keys()) == set(
        expected_data_asset_names.keys()
    )
    for connector_name, asset_list in available_data_asset_names.items():
        assert set(asset_list) == set(expected_data_asset_names[connector_name])

    data_connector_names = ["my_filesystem_data_connector", "test_runtime_data_connector"]

    expected_data_asset_names: dict = {
        "test_runtime_data_connector": [data_asset_name],
        "my_filesystem_data_connector": ["Titanic"]
    }

    available_data_asset_names: dict = basic_execution_environment.get_available_data_asset_names(
        data_connector_names=data_connector_names
    )

    assert set(available_data_asset_names.keys()) == set(
        expected_data_asset_names.keys()
    )
    for connector_name, asset_list in available_data_asset_names.items():
        assert set(asset_list) == set(expected_data_asset_names[connector_name])

    data_connector_names = ["my_filesystem_data_connector"]

    expected_data_asset_names: dict = {
        "my_filesystem_data_connector": ["Titanic"]
    }

    available_data_asset_names: dict = basic_execution_environment.get_available_data_asset_names(
        data_connector_names=data_connector_names
    )

    assert set(available_data_asset_names.keys()) == set(
        expected_data_asset_names.keys()
    )
    for connector_name, asset_list in available_data_asset_names.items():
        assert set(asset_list) == set(expected_data_asset_names[connector_name])

    data_connector_names = "my_filesystem_data_connector"

    expected_data_asset_names: dict = {
        "my_filesystem_data_connector": ["Titanic"]
    }

    available_data_asset_names: dict = basic_execution_environment.get_available_data_asset_names(
        data_connector_names=data_connector_names
    )

    assert set(available_data_asset_names.keys()) == set(
        expected_data_asset_names.keys()
    )
    for connector_name, asset_list in available_data_asset_names.items():
        assert set(asset_list) == set(expected_data_asset_names[connector_name])

    data_connector_names = ["test_runtime_data_connector"]

    expected_data_asset_names: dict = {
        "test_runtime_data_connector": [data_asset_name]
    }

    available_data_asset_names: dict = basic_execution_environment.get_available_data_asset_names(
        data_connector_names=data_connector_names
    )

    assert set(available_data_asset_names.keys()) == set(
        expected_data_asset_names.keys()
    )
    for connector_name, asset_list in available_data_asset_names.items():
        assert set(asset_list) == set(expected_data_asset_names[connector_name])


def test_get_available_data_asset_names_with_single_partition_file_data_connector(
    sample_execution_environment_with_single_partition_file_data_connector
):
    execution_environment: ExecutionEnvironment = sample_execution_environment_with_single_partition_file_data_connector
    data_connector_names: Optional[Union[List, str]] = None

    # Call "get_batch_list_from_batch_request()" to fill up the caches
    data_connector_name: str = "test_runtime_data_connector"
    data_asset_name: str = "IN_MEMORY_DATA_ASSET"
    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})
    batch_request: dict = {
        "execution_environment_name": execution_environment.name,
        "data_connector_name": data_connector_name,
        "data_asset_name": data_asset_name,
        "batch_data": test_df,
        "partition_request": {
            "partition_identifiers": {
                "run_id": 1234567890,
            },
            "limit": None,
        }
    }
    batch_request: BatchRequest = BatchRequest(**batch_request)
    # noinspection PyUnusedLocal
    batch_list: List[Batch] = execution_environment.get_batch_list_from_batch_request(
        batch_request=batch_request
    )

    expected_data_asset_names: dict = {
        "test_runtime_data_connector": [data_asset_name],
        "my_filesystem_data_connector": ["DEFAULT_ASSET_NAME"]
    }

    available_data_asset_names: dict = execution_environment.get_available_data_asset_names(
        data_connector_names=data_connector_names
    )

    assert set(available_data_asset_names.keys()) == set(expected_data_asset_names.keys())
    for connector_name, asset_list in available_data_asset_names.items():
        assert set(asset_list) == set(expected_data_asset_names[connector_name])

    data_connector_names = ["my_filesystem_data_connector", "test_runtime_data_connector"]

    expected_data_asset_names: dict = {
        "test_runtime_data_connector": [data_asset_name],
        "my_filesystem_data_connector": ["DEFAULT_ASSET_NAME"]
    }

    available_data_asset_names: dict = execution_environment.get_available_data_asset_names(
        data_connector_names=data_connector_names
    )

    assert set(available_data_asset_names.keys()) == set(expected_data_asset_names.keys())
    for connector_name, asset_list in available_data_asset_names.items():
        assert set(asset_list) == set(expected_data_asset_names[connector_name])

    data_connector_names = ["my_filesystem_data_connector"]

    expected_data_asset_names: dict = {
        "my_filesystem_data_connector": ["DEFAULT_ASSET_NAME"]
    }

    available_data_asset_names: dict = execution_environment.get_available_data_asset_names(
        data_connector_names=data_connector_names
    )

    assert set(available_data_asset_names.keys()) == set(expected_data_asset_names.keys())
    for connector_name, asset_list in available_data_asset_names.items():
        assert set(asset_list) == set(expected_data_asset_names[connector_name])

    data_connector_names = "my_filesystem_data_connector"

    expected_data_asset_names: dict = {
        "my_filesystem_data_connector": ["DEFAULT_ASSET_NAME"]
    }

    available_data_asset_names: dict = execution_environment.get_available_data_asset_names(
        data_connector_names=data_connector_names
    )

    assert set(available_data_asset_names.keys()) == set(expected_data_asset_names.keys())
    for connector_name, asset_list in available_data_asset_names.items():
        assert set(asset_list) == set(expected_data_asset_names[connector_name])

    data_connector_names = ["my_filesystem_data_connector"]

    expected_data_asset_names: dict = {
        "my_filesystem_data_connector": ["DEFAULT_ASSET_NAME"]
    }

    available_data_asset_names: dict = execution_environment.get_available_data_asset_names(
        data_connector_names=data_connector_names
    )

    assert set(available_data_asset_names.keys()) == set(expected_data_asset_names.keys())
    for connector_name, asset_list in available_data_asset_names.items():
        assert set(asset_list) == set(expected_data_asset_names[connector_name])


def test_get_available_data_asset_names_with_caching():
    pass


@pytest.fixture
def sql_execution_environment_for_testing_get_batch(tmp_path_factory):
    db_file = os.path.join(os.getcwd(), "tests", "test_sets", "test_cases_for_sql_data_connector.db")

    config = yaml.load(
        f"""
class_name: StreamlinedSqlExecutionEnvironment
connection_string: sqlite:///{db_file}
"""+"""
introspection:
    whole_table: {}

    daily:
        partitioning_directives:
            splitter_method: _split_on_converted_datetime
            splitter_kwargs:
                column_name: date
                date_format_string: "%Y-%m-%d"

    weekly:
        partitioning_directives:
            splitter_method: _split_on_converted_datetime
            splitter_kwargs:
                column_name: date
                date_format_string: "%Y-%W"

    by_id_dozens:
        partitioning_directives:
            splitter_method: _split_on_divided_integer
            splitter_kwargs:
                column_name: id
                divisor: 12
""",
        yaml.FullLoader,
    )

    my_sql_execution_environment = instantiate_class_from_config(
        config,
        config_defaults={
            "module_name": "great_expectations.execution_environment"
        },
        runtime_environment={
            "name" : "my_sql_execution_environment"
        },
    )

    return my_sql_execution_environment

def test_get_batch(sql_execution_environment_for_testing_get_batch):
    my_data_source = sql_execution_environment_for_testing_get_batch

    # Successful specification using a BatchDefinition
    my_data_source.get_batch(
        execution_environment_name="my_sql_execution_environment",
        data_connector_name="my_sqlite_db",
        data_asset_name="table_partitioned_by_date_column__A",
        date="2020-01-15",
    )

    # Failed specification using a BatchDefinition
    # Successful specification using a BatchRequest
    # Failed specification using a BatchRequest