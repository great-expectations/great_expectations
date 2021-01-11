import json
import os
import shutil
from typing import List, Optional, Union

import pandas as pd
import pytest
from ruamel.yaml import YAML

import great_expectations.exceptions as ge_exceptions
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
from great_expectations.datasource.data_connector import (
    ConfiguredAssetFilesystemDataConnector,
)
from great_expectations.datasource.new_datasource import Datasource
from tests.test_utils import create_files_in_directory

yaml = YAML()


@pytest.fixture
def basic_pandas_datasource(tmp_path_factory):
    base_directory: str = str(
        tmp_path_factory.mktemp("basic_pandas_datasource_filesystem_data_connector")
    )

    basic_datasource: Datasource = instantiate_class_from_config(
        yaml.load(
            f"""
class_name: Datasource

execution_engine:
    class_name: PandasExecutionEngine

data_connectors:
    test_runtime_data_connector:
        module_name: great_expectations.datasource.data_connector
        class_name: RuntimeDataConnector
        runtime_keys:
            - pipeline_stage_name
            - airflow_run_id

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
    """,
        ),
        runtime_environment={"name": "my_datasource"},
        config_defaults={"module_name": "great_expectations.datasource"},
    )
    return basic_datasource


@pytest.fixture
def basic_spark_datasource(tmp_path_factory):
    base_directory: str = str(
        tmp_path_factory.mktemp("basic_spark_datasource_filesystem_data_connector")
    )

    basic_datasource: Datasource = instantiate_class_from_config(
        yaml.load(
            f"""
class_name: Datasource

execution_engine:
    class_name: SparkDFExecutionEngine
    spark_config:
        spark.master: local[*]
        spark.executor.memory: 6g
        spark.driver.memory: 6g
        spark.ui.showConsoleProgress: false
        spark.sql.shuffle.partitions: 2
        spark.default.parallelism: 4
data_connectors:
    test_runtime_data_connector:
        module_name: great_expectations.datasource.data_connector
        class_name: RuntimeDataConnector
        runtime_keys:
            - pipeline_stage_name
            - airflow_run_id
    simple_filesystem_data_connector:
        class_name: InferredAssetFilesystemDataConnector
        base_directory: {base_directory}
        glob_directive: '*'
        default_regex:
            pattern: (.+)\\.csv
            group_names:
            - data_asset_name
    """,
        ),
        runtime_environment={"name": "my_datasource"},
        config_defaults={"module_name": "great_expectations.datasource"},
    )
    return basic_datasource


@pytest.fixture
def sample_datasource_with_single_partition_file_data_connector(
    tmp_path_factory,
):
    base_directory: str = str(
        tmp_path_factory.mktemp(
            "basic_pandas_datasource_single_partition_filesystem_data_connector"
        )
    )

    sample_datasource: Datasource = instantiate_class_from_config(
        yaml.load(
            f"""
class_name: Datasource

execution_engine:
    class_name: PandasExecutionEngine

data_connectors:
    test_runtime_data_connector:
        module_name: great_expectations.datasource.data_connector
        class_name: RuntimeDataConnector
        runtime_keys:
            - pipeline_stage_name
            - airflow_run_id

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
    """,
        ),
        runtime_environment={"name": "my_datasource"},
        config_defaults={"module_name": "great_expectations.datasource"},
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
        directory=base_directory, file_name_list=sample_file_names
    )

    return sample_datasource


def test_basic_pandas_datasource_self_check(basic_pandas_datasource):
    report = basic_pandas_datasource.self_check()
    assert report == {
        "execution_engine": {
            "caching": True,
            "class_name": "PandasExecutionEngine",
            "discard_subset_failing_expectations": False,
            "boto3_options": {},
        },
        "data_connectors": {
            "count": 2,
            "my_filesystem_data_connector": {
                "class_name": "ConfiguredAssetFilesystemDataConnector",
                "data_asset_count": 1,
                "example_data_asset_names": ["Titanic"],
                "data_assets": {
                    "Titanic": {
                        "batch_definition_count": 0,
                        "example_data_references": [],
                    }
                },
                "unmatched_data_reference_count": 0,
                "example_unmatched_data_references": [],
                "example_data_reference": {},
            },
            "test_runtime_data_connector": {
                "class_name": "RuntimeDataConnector",
                "data_asset_count": 1,
                "example_data_asset_names": ["IN_MEMORY_DATA_ASSET"],
                "data_assets": {
                    "IN_MEMORY_DATA_ASSET": {
                        "batch_definition_count": 1,
                        "example_data_references": [""],
                    }
                },
                "unmatched_data_reference_count": 0,
                "example_unmatched_data_references": [],
                "example_data_reference": {},
            },
        },
    }


def test_basic_spark_datasource_self_check(basic_spark_datasource):
    report = basic_spark_datasource.self_check()
    assert report == {
        "execution_engine": {
            "caching": True,
            "class_name": "SparkDFExecutionEngine",
            "persist": True,
            "spark_config": {
                "spark.master": "local[*]",
                "spark.executor.memory": "6g",
                "spark.driver.memory": "6g",
                "spark.ui.showConsoleProgress": False,
                "spark.sql.shuffle.partitions": 2,
                "spark.default.parallelism": 4,
            },
        },
        "data_connectors": {
            "count": 2,
            "simple_filesystem_data_connector": {
                "class_name": "InferredAssetFilesystemDataConnector",
                "data_asset_count": 0,
                "example_data_asset_names": [],
                "data_assets": {},
                "unmatched_data_reference_count": 0,
                "example_unmatched_data_references": [],
            },
            "test_runtime_data_connector": {
                "class_name": "RuntimeDataConnector",
                "data_asset_count": 1,
                "example_data_asset_names": ["IN_MEMORY_DATA_ASSET"],
                "data_assets": {
                    "IN_MEMORY_DATA_ASSET": {
                        "batch_definition_count": 1,
                        "example_data_references": [""],
                    }
                },
                "unmatched_data_reference_count": 0,
                "example_unmatched_data_references": [],
                "example_data_reference": {},
            },
        },
    }


def test_get_batch_definitions_and_get_batch_basics(basic_pandas_datasource):
    my_data_connector: ConfiguredAssetFilesystemDataConnector = (
        basic_pandas_datasource.data_connectors["my_filesystem_data_connector"]
    )
    create_files_in_directory(
        my_data_connector.base_directory,
        ["A_1.csv", "A_2.csv", "A_3.csv", "B_1.csv", "B_2.csv", "B_3.csv"],
    )

    assert (
        len(
            basic_pandas_datasource.get_available_batch_definitions(
                batch_request=BatchRequest(
                    datasource_name="my_datasource",
                    data_connector_name="my_filesystem_data_connector",
                )
            )
        )
        == 6
    )

    batch: Batch = basic_pandas_datasource.get_batch_from_batch_definition(
        batch_definition=BatchDefinition(
            datasource_name="my_datasource",
            data_connector_name="my_filesystem_data_connector",
            data_asset_name="B1",
            partition_definition=PartitionDefinition(
                {
                    "letter": "B",
                    "number": "1",
                }
            ),
        )
    )

    # TODO Abe 20201104: Make sure this is what we truly want to do.
    assert batch.batch_request == {}
    assert isinstance(batch.data, pd.DataFrame)
    assert batch.batch_definition == BatchDefinition(
        datasource_name="my_datasource",
        data_connector_name="my_filesystem_data_connector",
        data_asset_name="B1",
        partition_definition=PartitionDefinition(
            {
                "letter": "B",
                "number": "1",
            }
        ),
    )

    batch_list: List[Batch] = basic_pandas_datasource.get_batch_list_from_batch_request(
        batch_request=BatchRequest(
            datasource_name="my_datasource",
            data_connector_name="my_filesystem_data_connector",
            data_asset_name="B1",
            partition_request={
                "partition_identifiers": {
                    "letter": "B",
                    "number": "1",
                }
            },
        )
    )
    assert len(batch_list) == 0

    batch_list: List[Batch] = basic_pandas_datasource.get_batch_list_from_batch_request(
        batch_request=BatchRequest(
            datasource_name="my_datasource",
            data_connector_name="my_filesystem_data_connector",
            data_asset_name="Titanic",
            partition_request={
                "partition_identifiers": {
                    "letter": "B",
                    "number": "1",
                }
            },
        )
    )
    assert len(batch_list) == 1
    assert isinstance(batch_list[0].data, pd.DataFrame)

    my_df: pd.DataFrame = pd.DataFrame({"x": range(10), "y": range(10)})
    batch: Batch = basic_pandas_datasource.get_batch_from_batch_definition(
        batch_definition=BatchDefinition(
            "my_datasource",
            "_pipeline",
            "_pipeline",
            partition_definition=PartitionDefinition({"some_random_id": 1}),
        ),
        batch_data=my_df,
    )
    # TODO Abe 20201104: Make sure this is what we truly want to do.
    assert batch.batch_request == {}


def test_get_batch_list_from_batch_request(basic_pandas_datasource):
    datasource_name: str = "my_datasource"
    data_connector_name: str = "my_filesystem_data_connector"
    data_asset_name: str = "Titanic"
    titanic_csv_source_file_path: str = file_relative_path(
        __file__, "../test_sets/Titanic.csv"
    )
    base_directory: str = basic_pandas_datasource.data_connectors[
        data_connector_name
    ].base_directory
    titanic_csv_destination_file_path: str = str(
        os.path.join(base_directory, "Titanic_19120414.csv")
    )
    shutil.copy(titanic_csv_source_file_path, titanic_csv_destination_file_path)

    batch_request: dict = {
        "datasource_name": datasource_name,
        "data_connector_name": data_connector_name,
        "data_asset_name": data_asset_name,
        "partition_request": {
            "partition_identifiers": {"letter": "Titanic", "number": "19120414"}
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
    batch_list: List[Batch] = basic_pandas_datasource.get_batch_list_from_batch_request(
        batch_request=batch_request
    )

    assert len(batch_list) == 1

    batch: Batch = batch_list[0]

    assert batch.batch_spec is not None
    assert isinstance(batch.data, pd.DataFrame)
    assert batch.data.shape[0] == 1313
    assert (
        batch.batch_markers["pandas_data_fingerprint"]
        == "3aaabc12402f987ff006429a7756f5cf"
    )


def test_get_batch_with_caching():
    pass


def test_get_batch_with_pipeline_style_batch_request(basic_pandas_datasource):
    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    data_connector_name: str = "test_runtime_data_connector"
    data_asset_name: str = "IN_MEMORY_DATA_ASSET"

    batch_request: dict = {
        "datasource_name": basic_pandas_datasource.name,
        "data_connector_name": data_connector_name,
        "data_asset_name": data_asset_name,
        "batch_data": test_df,
        "partition_request": {
            "partition_identifiers": {
                "airflow_run_id": 1234567890,
            }
        },
        "limit": None,
    }
    batch_request: BatchRequest = BatchRequest(**batch_request)
    batch_list: List[Batch] = basic_pandas_datasource.get_batch_list_from_batch_request(
        batch_request=batch_request
    )

    assert len(batch_list) == 1

    batch: Batch = batch_list[0]

    assert batch.batch_spec is not None
    assert batch.batch_definition["data_asset_name"] == data_asset_name
    assert isinstance(batch.data, pd.DataFrame)
    assert batch.data.shape == (2, 2)
    assert batch.data["col2"].values[1] == 4
    assert (
        batch.batch_markers["pandas_data_fingerprint"]
        == "1e461a0df5fe0a6db2c3bc4ef88ef1f0"
    )


def test_get_batch_with_pipeline_style_batch_request_missing_partition_request_error(
    basic_pandas_datasource,
):
    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    data_connector_name: str = "test_runtime_data_connector"
    data_asset_name: str = "test_asset_1"

    batch_request: dict = {
        "datasource_name": basic_pandas_datasource.name,
        "data_connector_name": data_connector_name,
        "data_asset_name": data_asset_name,
        "batch_data": test_df,
        "partition_request": None,
        "limit": None,
    }
    batch_request: BatchRequest = BatchRequest(**batch_request)
    with pytest.raises(ge_exceptions.DataConnectorError):
        # noinspection PyUnusedLocal
        batch_list: List[
            Batch
        ] = basic_pandas_datasource.get_batch_list_from_batch_request(
            batch_request=batch_request
        )


def test_get_available_data_asset_names_with_configured_asset_filesystem_data_connector(
    basic_pandas_datasource,
):
    data_connector_names: Optional[Union[List, str]] = None

    # Call "get_batch_list_from_batch_request()" to fill up the caches
    data_connector_name: str = "test_runtime_data_connector"
    data_asset_name: str = "IN_MEMORY_DATA_ASSET"
    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})
    batch_request: dict = {
        "datasource_name": basic_pandas_datasource.name,
        "data_connector_name": data_connector_name,
        "data_asset_name": data_asset_name,
        "batch_data": test_df,
        "partition_request": {
            "partition_identifiers": {
                "airflow_run_id": 1234567890,
            }
        },
        "limit": None,
    }
    batch_request: BatchRequest = BatchRequest(**batch_request)
    # noinspection PyUnusedLocal
    batch_list: List[Batch] = basic_pandas_datasource.get_batch_list_from_batch_request(
        batch_request=batch_request
    )

    expected_data_asset_names: dict = {
        "test_runtime_data_connector": [data_asset_name],
        "my_filesystem_data_connector": ["Titanic"],
    }

    available_data_asset_names: dict = (
        basic_pandas_datasource.get_available_data_asset_names(
            data_connector_names=data_connector_names
        )
    )

    assert set(available_data_asset_names.keys()) == set(
        expected_data_asset_names.keys()
    )
    for connector_name, asset_list in available_data_asset_names.items():
        assert set(asset_list) == set(expected_data_asset_names[connector_name])

    data_connector_names = [
        "my_filesystem_data_connector",
        "test_runtime_data_connector",
    ]

    expected_data_asset_names: dict = {
        "test_runtime_data_connector": [data_asset_name],
        "my_filesystem_data_connector": ["Titanic"],
    }

    available_data_asset_names: dict = (
        basic_pandas_datasource.get_available_data_asset_names(
            data_connector_names=data_connector_names
        )
    )

    assert set(available_data_asset_names.keys()) == set(
        expected_data_asset_names.keys()
    )
    for connector_name, asset_list in available_data_asset_names.items():
        assert set(asset_list) == set(expected_data_asset_names[connector_name])

    data_connector_names = ["my_filesystem_data_connector"]

    expected_data_asset_names: dict = {"my_filesystem_data_connector": ["Titanic"]}

    available_data_asset_names: dict = (
        basic_pandas_datasource.get_available_data_asset_names(
            data_connector_names=data_connector_names
        )
    )

    assert set(available_data_asset_names.keys()) == set(
        expected_data_asset_names.keys()
    )
    for connector_name, asset_list in available_data_asset_names.items():
        assert set(asset_list) == set(expected_data_asset_names[connector_name])

    data_connector_names = "my_filesystem_data_connector"

    expected_data_asset_names: dict = {"my_filesystem_data_connector": ["Titanic"]}

    available_data_asset_names: dict = (
        basic_pandas_datasource.get_available_data_asset_names(
            data_connector_names=data_connector_names
        )
    )

    assert set(available_data_asset_names.keys()) == set(
        expected_data_asset_names.keys()
    )
    for connector_name, asset_list in available_data_asset_names.items():
        assert set(asset_list) == set(expected_data_asset_names[connector_name])

    data_connector_names = ["test_runtime_data_connector"]

    expected_data_asset_names: dict = {"test_runtime_data_connector": [data_asset_name]}

    available_data_asset_names: dict = (
        basic_pandas_datasource.get_available_data_asset_names(
            data_connector_names=data_connector_names
        )
    )

    assert set(available_data_asset_names.keys()) == set(
        expected_data_asset_names.keys()
    )
    for connector_name, asset_list in available_data_asset_names.items():
        assert set(asset_list) == set(expected_data_asset_names[connector_name])


def test_get_available_data_asset_names_with_single_partition_file_data_connector(
    sample_datasource_with_single_partition_file_data_connector,
):
    datasource: Datasource = sample_datasource_with_single_partition_file_data_connector
    data_connector_names: Optional[Union[List, str]] = None

    # Call "get_batch_list_from_batch_request()" to fill up the caches
    data_connector_name: str = "test_runtime_data_connector"
    data_asset_name: str = "IN_MEMORY_DATA_ASSET"
    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})
    batch_request: dict = {
        "datasource_name": datasource.name,
        "data_connector_name": data_connector_name,
        "data_asset_name": data_asset_name,
        "batch_data": test_df,
        "partition_request": {
            "partition_identifiers": {
                "airflow_run_id": 1234567890,
            },
            "limit": None,
        },
    }
    batch_request: BatchRequest = BatchRequest(**batch_request)
    # noinspection PyUnusedLocal
    batch_list: List[Batch] = datasource.get_batch_list_from_batch_request(
        batch_request=batch_request
    )

    expected_data_asset_names: dict = {
        "test_runtime_data_connector": [data_asset_name],
        "my_filesystem_data_connector": ["DEFAULT_ASSET_NAME"],
    }

    available_data_asset_names: dict = datasource.get_available_data_asset_names(
        data_connector_names=data_connector_names
    )

    assert set(available_data_asset_names.keys()) == set(
        expected_data_asset_names.keys()
    )
    for connector_name, asset_list in available_data_asset_names.items():
        assert set(asset_list) == set(expected_data_asset_names[connector_name])

    data_connector_names = [
        "my_filesystem_data_connector",
        "test_runtime_data_connector",
    ]

    expected_data_asset_names: dict = {
        "test_runtime_data_connector": [data_asset_name],
        "my_filesystem_data_connector": ["DEFAULT_ASSET_NAME"],
    }

    available_data_asset_names: dict = datasource.get_available_data_asset_names(
        data_connector_names=data_connector_names
    )

    assert set(available_data_asset_names.keys()) == set(
        expected_data_asset_names.keys()
    )
    for connector_name, asset_list in available_data_asset_names.items():
        assert set(asset_list) == set(expected_data_asset_names[connector_name])

    data_connector_names = ["my_filesystem_data_connector"]

    expected_data_asset_names: dict = {
        "my_filesystem_data_connector": ["DEFAULT_ASSET_NAME"]
    }

    available_data_asset_names: dict = datasource.get_available_data_asset_names(
        data_connector_names=data_connector_names
    )

    assert set(available_data_asset_names.keys()) == set(
        expected_data_asset_names.keys()
    )
    for connector_name, asset_list in available_data_asset_names.items():
        assert set(asset_list) == set(expected_data_asset_names[connector_name])

    data_connector_names = "my_filesystem_data_connector"

    expected_data_asset_names: dict = {
        "my_filesystem_data_connector": ["DEFAULT_ASSET_NAME"]
    }

    available_data_asset_names: dict = datasource.get_available_data_asset_names(
        data_connector_names=data_connector_names
    )

    assert set(available_data_asset_names.keys()) == set(
        expected_data_asset_names.keys()
    )
    for connector_name, asset_list in available_data_asset_names.items():
        assert set(asset_list) == set(expected_data_asset_names[connector_name])

    data_connector_names = ["my_filesystem_data_connector"]

    expected_data_asset_names: dict = {
        "my_filesystem_data_connector": ["DEFAULT_ASSET_NAME"]
    }

    available_data_asset_names: dict = datasource.get_available_data_asset_names(
        data_connector_names=data_connector_names
    )

    assert set(available_data_asset_names.keys()) == set(
        expected_data_asset_names.keys()
    )
    for connector_name, asset_list in available_data_asset_names.items():
        assert set(asset_list) == set(expected_data_asset_names[connector_name])


def test_get_available_data_asset_names_with_caching():
    pass


def test__data_source_batch_spec_passthrough(tmp_path_factory):
    base_directory = str(
        tmp_path_factory.mktemp("data_connector_batch_spec_passthrough")
    )
    with open(
        os.path.join(base_directory, "csv_with_extra_header_rows.csv"), "w"
    ) as f_:
        f_.write(
            """--- extra ---
--- extra ---
x,y
0,a
1,b
2,c
3,d
4,e
5,f
6,g
7,h
8,i
9,j
"""
        )

    my_datasource: Datasource = instantiate_class_from_config(
        yaml.load(
            f"""
class_name: Datasource

execution_engine:
    class_name: PandasExecutionEngine

data_connectors:

    my_configured_data_connector:
        module_name: great_expectations.datasource.data_connector
        class_name: ConfiguredAssetFilesystemDataConnector

        base_directory: {base_directory}
        glob_directive: "*.csv"
        default_regex:
            pattern: "(.*)"
            group_names:
                - file_name
        assets:
            stuff:
                batch_spec_passthrough:
                    reader_options:
                        skiprows: 2


    my_inferred_data_connector:
        module_name: great_expectations.datasource.data_connector
        class_name: InferredAssetFilesystemDataConnector
        base_directory: {base_directory}
        batch_spec_passthrough:
            reader_options:
                skiprows: 2

        default_regex:
            pattern: (.*)\.csv
            group_names:
                - data_asset_name
    """,
        ),
        runtime_environment={"name": "my_datasource"},
        config_defaults={"module_name": "great_expectations.datasource"},
    )

    report_obj = my_datasource.self_check()

    print(json.dumps(report_obj, indent=2))

    assert (
        report_obj["data_connectors"]["my_configured_data_connector"][
            "example_data_reference"
        ]["n_rows"]
        == 10
    )

    assert (
        report_obj["data_connectors"]["my_inferred_data_connector"][
            "example_data_reference"
        ]["n_rows"]
        == 10
    )
