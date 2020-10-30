import pytest
import pandas as pd
import yaml

from typing import List

from great_expectations.execution_environment.execution_environment import ExecutionEnvironment
from great_expectations.execution_environment.data_connector import PipelineDataConnector
from great_expectations.core.batch import (
    BatchRequest,
    BatchDefinition,
    PartitionDefinition,
)
from great_expectations.execution_environment.types import InMemoryBatchSpec
from great_expectations.data_context.util import instantiate_class_from_config


@pytest.fixture
def basic_execution_environment(tmp_path_factory):
    basic_execution_environment: ExecutionEnvironment = instantiate_class_from_config(yaml.load(f"""
class_name: ExecutionEnvironment

execution_engine:
    class_name: PandasExecutionEngine

    """, Loader=yaml.FullLoader), runtime_environment={
        "name": "my_execution_environment"
    },
        config_defaults={
          "module_name": "great_expectations.execution_environment"
        }
    )
    return basic_execution_environment


def test_instantiation(basic_execution_environment):
    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})
    my_pipeline_data_connector: PipelineDataConnector

    # noinspection PyProtectedMember
    my_pipeline_data_connector = basic_execution_environment._get_pipeline_data_connector(
        name="my_pipeline_data_connector",
        execution_environment_name=basic_execution_environment.name,
        data_asset_name="my_data_asset",
        batch_data=test_df,
        partition_request={
            "run_id": 1234567890,
        },
    )

    assert my_pipeline_data_connector.self_check() == {
        "class_name": "PipelineDataConnector",
        "data_asset_count": 1,
        "example_data_asset_names": ["my_data_asset"],
        "data_assets": {
            "my_data_asset": {
                "batch_definition_count": 1,
                "example_data_references": ["1234567890"]
            }
        },
        "unmatched_data_reference_count": 0,
        "example_unmatched_data_references": []
    }

    # Test for an unknown execution environment
    with pytest.raises(ValueError):
        # noinspection PyUnusedLocal
        batch_definition_list: List[BatchDefinition] = \
            my_pipeline_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(
                execution_environment_name="non_existent_execution_environment",
                data_connector_name="my_pipeline_data_connector",
                data_asset_name="my_data_asset",
            )
        )

    # Test for an unknown data_connector
    with pytest.raises(ValueError):
        # noinspection PyUnusedLocal
        batch_definition_list: List[BatchDefinition] = \
            my_pipeline_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(
                execution_environment_name=basic_execution_environment.name,
                data_connector_name="non_existent_data_connector",
                data_asset_name="my_data_asset",
            )
        )

    # Test for illegal absence of partition_request when batch_data is specified
    with pytest.raises(AttributeError):
        # noinspection PyUnusedLocal
        # noinspection PyProtectedMember
        my_pipeline_data_connector = basic_execution_environment._get_pipeline_data_connector(
            name="my_pipeline_data_connector",
            execution_environment_name=basic_execution_environment.name,
            data_asset_name="my_data_asset",
            batch_data=test_df,
            partition_request=None,
        )
        # noinspection PyUnusedLocal
        # noinspection PyProtectedMember
        data_reference_list: List[str] = my_pipeline_data_connector._get_data_reference_list()


def test_get_available_data_asset_names(basic_execution_environment):
    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})
    my_pipeline_data_connector: PipelineDataConnector

    # noinspection PyProtectedMember
    my_pipeline_data_connector = basic_execution_environment._get_pipeline_data_connector(
        name="my_pipeline_data_connector",
        execution_environment_name=basic_execution_environment.name,
        data_asset_name="my_data_asset",
        batch_data=test_df,
        partition_request={
            "run_id": 1234567890,
        },
    )

    expected_available_data_asset_names: List[str] = ["my_data_asset"]

    available_data_asset_names: List[str] = my_pipeline_data_connector.get_available_data_asset_names()

    assert available_data_asset_names == expected_available_data_asset_names


def test_get_batch_definition_list_from_batch_request(basic_execution_environment):
    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})
    my_pipeline_data_connector: PipelineDataConnector

    # noinspection PyProtectedMember
    my_pipeline_data_connector = basic_execution_environment._get_pipeline_data_connector(
        name="my_pipeline_data_connector",
        execution_environment_name=basic_execution_environment.name,
        data_asset_name="my_data_asset",
        batch_data=test_df,
        partition_request={
            "run_id": 1234567890,
        },
    )

    batch_request: dict = {
        "execution_environment_name": basic_execution_environment.name,
        "data_connector_name": my_pipeline_data_connector.name,
        "data_asset_name": "my_data_asset",
        "batch_data": test_df,
        "partition_request": {
            "run_id": 1234567890,
        },
        "limit": None,
    }
    batch_request: BatchRequest = BatchRequest(**batch_request)

    expected_batch_definition_list: List[BatchDefinition] = [
        BatchDefinition(
            execution_environment_name="my_execution_environment",
            data_connector_name="my_pipeline_data_connector",
            data_asset_name="my_data_asset",
            partition_definition=PartitionDefinition({
                "run_id": 1234567890,
            })
        )
    ]

    batch_definition_list: List[BatchDefinition] = \
        my_pipeline_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=batch_request
        )

    assert batch_definition_list == expected_batch_definition_list


def test__get_data_reference_list(basic_execution_environment):
    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})
    my_pipeline_data_connector: PipelineDataConnector

    # noinspection PyProtectedMember
    my_pipeline_data_connector = basic_execution_environment._get_pipeline_data_connector(
        name="my_pipeline_data_connector",
        execution_environment_name=basic_execution_environment.name,
        data_asset_name="my_data_asset",
        batch_data=test_df,
        partition_request={
            "custom_key_0": "staging",
            "run_id": 1234567890,
        },
    )

    expected_data_reference_list: List[str] = ["staging-1234567890"]

    # noinspection PyProtectedMember
    data_reference_list: List[str] = my_pipeline_data_connector._get_data_reference_list()

    assert data_reference_list == expected_data_reference_list


def test__build_batch_spec_from_batch_definition(basic_execution_environment):
    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})
    my_pipeline_data_connector: PipelineDataConnector

    # noinspection PyProtectedMember
    my_pipeline_data_connector = basic_execution_environment._get_pipeline_data_connector(
        name="my_pipeline_data_connector",
        execution_environment_name=basic_execution_environment.name,
        data_asset_name="my_data_asset",
        batch_data=test_df,
        partition_request={
            "custom_key_0": "staging",
            "run_id": 1234567890,
        },
    )

    expected_batch_spec: InMemoryBatchSpec = InMemoryBatchSpec(
        batch_data=test_df,
    )

    # noinspection PyProtectedMember
    batch_spec: InMemoryBatchSpec = my_pipeline_data_connector._build_batch_spec_from_batch_definition(
        batch_definition=BatchDefinition(
            execution_environment_name="my_execution_environment",
            data_connector_name="my_pipeline_data_connector",
            data_asset_name="my_data_asset",
            partition_definition=PartitionDefinition({
                "custom_key_0": "staging",
                "run_id": 1234567890,
            })
        )
    )

    assert batch_spec == expected_batch_spec
