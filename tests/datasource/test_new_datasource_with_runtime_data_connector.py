from typing import List

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
from great_expectations.data_context.util import instantiate_class_from_config
from great_expectations.datasource.new_datasource import Datasource

yaml = YAML()


@pytest.fixture
def basic_datasource_with_runtime_data_connector():
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
        """,
        ),
        runtime_environment={"name": "my_datasource"},
        config_defaults={"module_name": "great_expectations.datasource"},
    )
    return basic_datasource


def test_get_batch_with_pipeline_style_batch_request(
    basic_datasource_with_runtime_data_connector,
):
    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    data_connector_name: str = "test_runtime_data_connector"
    data_asset_name: str = "IN_MEMORY_DATA_ASSET"

    batch_request: dict = {
        "datasource_name": basic_datasource_with_runtime_data_connector.name,
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
    batch_list: List[
        Batch
    ] = basic_datasource_with_runtime_data_connector.get_batch_list_from_batch_request(
        batch_request=batch_request
    )

    assert len(batch_list) == 1

    batch: Batch = batch_list[0]

    assert batch.batch_spec is not None
    assert batch.batch_definition["data_asset_name"] == data_asset_name
    assert isinstance(batch.data.dataframe, pd.DataFrame)
    assert batch.data.dataframe.shape == (2, 2)
    assert batch.data.dataframe["col2"].values[1] == 4
    assert (
        batch.batch_markers["pandas_data_fingerprint"]
        == "1e461a0df5fe0a6db2c3bc4ef88ef1f0"
    )


def test_get_batch_with_pipeline_style_batch_request_missing_partition_request_error(
    basic_datasource_with_runtime_data_connector,
):
    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    data_connector_name: str = "test_runtime_data_connector"
    data_asset_name: str = "test_asset_1"

    batch_request: dict = {
        "datasource_name": basic_datasource_with_runtime_data_connector.name,
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
        ] = basic_datasource_with_runtime_data_connector.get_batch_list_from_batch_request(
            batch_request=batch_request
        )


def test_basic_datasource_runtime_data_connector_self_check(
    basic_datasource_with_runtime_data_connector,
):
    report = basic_datasource_with_runtime_data_connector.self_check()

    assert report == {
        "execution_engine": {
            "caching": True,
            "module_name": "great_expectations.execution_engine.pandas_execution_engine",
            "class_name": "PandasExecutionEngine",
            "discard_subset_failing_expectations": False,
            "boto3_options": {},
        },
        "data_connectors": {
            "count": 1,
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


def test_get_batch_definitions_and_get_batch_basics(
    basic_datasource_with_runtime_data_connector,
):
    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    data_connector_name: str = "test_runtime_data_connector"
    data_asset_name: str = "test_asset_1"

    batch_request: dict = {
        "datasource_name": basic_datasource_with_runtime_data_connector.name,
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

    assert (
        len(
            basic_datasource_with_runtime_data_connector.get_available_batch_definitions(
                batch_request=batch_request
            )
        )
        == 1
    )

    my_df: pd.DataFrame = pd.DataFrame({"x": range(10), "y": range(10)})
    batch: Batch = (
        basic_datasource_with_runtime_data_connector.get_batch_from_batch_definition(
            batch_definition=BatchDefinition(
                "my_datasource",
                "_pipeline",
                "_pipeline",
                partition_definition=PartitionDefinition({"some_random_id": 1}),
            ),
            batch_data=my_df,
        )
    )
    assert batch.batch_request == {}
