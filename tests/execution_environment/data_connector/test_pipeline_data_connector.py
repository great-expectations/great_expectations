import pytest
import pandas as pd
import yaml

from typing import List

from great_expectations.execution_environment.execution_environment import ExecutionEnvironment
from great_expectations.execution_environment.data_connector import PipelineDataConnector
from great_expectations.core.id_dict import PartitionDefinition
from great_expectations.core.batch import (
    BatchRequest,
    BatchDefinition,
)
from great_expectations.execution_environment.types import InMemoryBatchSpec
from great_expectations.data_context.util import instantiate_class_from_config
import great_expectations.exceptions as ge_exceptions


@pytest.fixture
def basic_execution_environment(tmp_path_factory):
    base_directory: str = str(tmp_path_factory.mktemp("basic_execution_environment_pipeline_data_connector"))

    basic_execution_environment: ExecutionEnvironment = instantiate_class_from_config(
        config=yaml.load(
            f"""
class_name: ExecutionEnvironment

data_connectors:
    test_pipeline_data_connector:
        module_name: great_expectations.execution_environment.data_connector
        class_name: PipelineDataConnector
        runtime_keys:
        - pipeline_stage_name
        - run_id
        - custom_key_0

execution_engine:
    class_name: PandasExecutionEngine

    """,
            Loader=yaml.FullLoader
        ),
        runtime_environment={
            "name": "my_execution_environment",
            "data_context_root_directory": base_directory
        },
        config_defaults={
          "module_name": "great_expectations.execution_environment",
        }
    )

    return basic_execution_environment


def test_self_check(basic_execution_environment):
    test_pipeline_data_connector: PipelineDataConnector = \
        basic_execution_environment.get_data_connector(name="test_pipeline_data_connector")

    assert test_pipeline_data_connector.self_check() == {
        "class_name": "PipelineDataConnector",
        "data_asset_count": 1,
        "example_data_asset_names": ["IN_MEMORY_DATA_ASSET"],
        "data_assets": {
            "IN_MEMORY_DATA_ASSET": {
                "batch_definition_count": 1,
                "example_data_references": [""]
            }
        },
        "unmatched_data_reference_count": 0,
        "example_unmatched_data_references": []
    }


def test_error_checking(basic_execution_environment):
    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    test_pipeline_data_connector: PipelineDataConnector = \
        basic_execution_environment.get_data_connector(name="test_pipeline_data_connector")

    # Test for an unknown execution environment
    with pytest.raises(ValueError):
        # noinspection PyUnusedLocal
        batch_definition_list: List[BatchDefinition] = \
            test_pipeline_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(
                execution_environment_name="non_existent_execution_environment",
                data_connector_name="test_pipeline_data_connector",
                data_asset_name="my_data_asset",
            )
        )

    # Test for an unknown data_connector
    with pytest.raises(ValueError):
        # noinspection PyUnusedLocal
        batch_definition_list: List[BatchDefinition] = \
            test_pipeline_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(
                execution_environment_name=basic_execution_environment.name,
                data_connector_name="non_existent_data_connector",
                data_asset_name="my_data_asset",
            )
        )

    # Test for illegal absence of partition_request when batch_data is specified
    with pytest.raises(ge_exceptions.DataConnectorError):
        # noinspection PyUnusedLocal
        batch_definition_list: List[BatchDefinition] = \
            test_pipeline_data_connector.get_batch_definition_list_from_batch_request(
                batch_request=BatchRequest(
                    execution_environment_name=basic_execution_environment.name,
                    data_connector_name="test_pipeline_data_connector",
                    data_asset_name="my_data_asset",
                    batch_data=test_df,
                    partition_request=None,
                )
            )

    # Test for illegal nullity of partition_request["partition_query"] when batch_data is specified
    partition_request: dict = {
        "partition_query": None
    }
    with pytest.raises(ge_exceptions.DataConnectorError):
        # noinspection PyUnusedLocal
        batch_definition_list: List[BatchDefinition] = \
            test_pipeline_data_connector.get_batch_definition_list_from_batch_request(
                batch_request=BatchRequest(
                    execution_environment_name=basic_execution_environment.name,
                    data_connector_name="test_pipeline_data_connector",
                    data_asset_name="my_data_asset",
                    batch_data=test_df,
                    partition_request=partition_request,
                )
            )

    # Test for illegal falsiness of partition_request["partition_query"] when batch_data is specified
    partition_request: dict = {
        "partition_query": {}
    }
    with pytest.raises(ge_exceptions.DataConnectorError):
        # noinspection PyUnusedLocal
        batch_definition_list: List[BatchDefinition] = \
            test_pipeline_data_connector.get_batch_definition_list_from_batch_request(
                batch_request=BatchRequest(
                    execution_environment_name=basic_execution_environment.name,
                    data_connector_name="test_pipeline_data_connector",
                    data_asset_name="my_data_asset",
                    batch_data=test_df,
                    partition_request=partition_request,
                )
            )


def test_partition_request_and_runtime_keys_success_all_keys_present(basic_execution_environment):
    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    partition_request: dict

    partition_request = {
        "partition_query": {
            "pipeline_stage_name": "core_processing",
            "run_id": 1234567890,
            "custom_key_0": "custom_value_0"
        }
    }

    test_pipeline_data_connector: PipelineDataConnector = \
        basic_execution_environment.get_data_connector(name="test_pipeline_data_connector")

    # Verify that all keys in partition_request are acceptable as runtime_keys (using batch count).
    batch_request: dict = {
        "execution_environment_name": basic_execution_environment.name,
        "data_connector_name": test_pipeline_data_connector.name,
        "data_asset_name": "IN_MEMORY_DATA_ASSET",
        "batch_data": test_df,
        "partition_request": partition_request,
        "limit": None,
    }
    batch_request: BatchRequest = BatchRequest(**batch_request)

    batch_definition_list: List[BatchDefinition] = \
        test_pipeline_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=batch_request
        )

    assert len(batch_definition_list) == 1


def test_partition_request_and_runtime_keys_error_illegal_keys(basic_execution_environment):
    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    partition_request: dict

    partition_request = {
        "partition_query": {
            "pipeline_stage_name": "core_processing",
            "run_id": 1234567890,
            "custom_key_0": "custom_value_0",
            "custom_key_1": "custom_value_1"
        }
    }

    test_pipeline_data_connector: PipelineDataConnector = \
        basic_execution_environment.get_data_connector(name="test_pipeline_data_connector")

    # Insure that keys in partition_request["partition_query"] that are not among runtime_keys declared in configuration
    # are not accepted.  In this test, all legal keys plus a single illegal key are present.
    batch_request: dict = {
        "execution_environment_name": basic_execution_environment.name,
        "data_connector_name": test_pipeline_data_connector.name,
        "data_asset_name": "IN_MEMORY_DATA_ASSET",
        "batch_data": test_df,
        "partition_request": partition_request,
        "limit": None,
    }
    batch_request: BatchRequest = BatchRequest(**batch_request)

    with pytest.raises(ge_exceptions.DataConnectorError):
        # noinspection PyUnusedLocal
        batch_definition_list: List[BatchDefinition] = \
            test_pipeline_data_connector.get_batch_definition_list_from_batch_request(
                batch_request=batch_request
            )

    partition_request = {
        "partition_query": {
            "unknown_key": "some_value"
        }
    }

    test_pipeline_data_connector: PipelineDataConnector = \
        basic_execution_environment.get_data_connector(name="test_pipeline_data_connector")

    # Insure that keys in partition_request["partition_query"] that are not among runtime_keys declared in configuration
    # are not accepted.  In this test, a single illegal key is present.
    batch_request: dict = {
        "execution_environment_name": basic_execution_environment.name,
        "data_connector_name": test_pipeline_data_connector.name,
        "data_asset_name": "IN_MEMORY_DATA_ASSET",
        "batch_data": test_df,
        "partition_request": partition_request,
        "limit": None,
    }
    batch_request: BatchRequest = BatchRequest(**batch_request)

    with pytest.raises(ge_exceptions.DataConnectorError):
        # noinspection PyUnusedLocal
        batch_definition_list: List[BatchDefinition] = \
            test_pipeline_data_connector.get_batch_definition_list_from_batch_request(
                batch_request=batch_request
            )


def test_get_available_data_asset_names(basic_execution_environment):
    test_pipeline_data_connector: PipelineDataConnector = \
        basic_execution_environment.get_data_connector(name="test_pipeline_data_connector")

    expected_available_data_asset_names: List[str] = ["IN_MEMORY_DATA_ASSET"]

    available_data_asset_names: List[str] = test_pipeline_data_connector.get_available_data_asset_names()

    assert available_data_asset_names == expected_available_data_asset_names


def test_get_batch_definition_list_from_batch_request_length_one(basic_execution_environment):
    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    partition_request: dict = {
        "partition_query": {
            "run_id": 1234567890,
        }
    }

    test_pipeline_data_connector: PipelineDataConnector = \
        basic_execution_environment.get_data_connector(name="test_pipeline_data_connector")

    batch_request: dict = {
        "execution_environment_name": basic_execution_environment.name,
        "data_connector_name": test_pipeline_data_connector.name,
        "data_asset_name": "IN_MEMORY_DATA_ASSET",
        "batch_data": test_df,
        "partition_request": partition_request,
        "limit": None,
    }
    batch_request: BatchRequest = BatchRequest(**batch_request)

    expected_batch_definition_list: List[BatchDefinition] = [
        BatchDefinition(
            execution_environment_name="my_execution_environment",
            data_connector_name="test_pipeline_data_connector",
            data_asset_name="IN_MEMORY_DATA_ASSET",
            partition_definition=PartitionDefinition(partition_request["partition_query"])
        )
    ]

    batch_definition_list: List[BatchDefinition] = \
        test_pipeline_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=batch_request
        )

    assert batch_definition_list == expected_batch_definition_list


def test_get_batch_definition_list_from_batch_request_length_zero(basic_execution_environment):
    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    partition_request: dict = {
        "partition_query": {
            "run_id": 1234567890,
        }
    }

    test_pipeline_data_connector: PipelineDataConnector = \
        basic_execution_environment.get_data_connector(name="test_pipeline_data_connector")

    batch_request: dict = {
        "execution_environment_name": basic_execution_environment.name,
        "data_connector_name": test_pipeline_data_connector.name,
        "data_asset_name": "nonexistent_data_asset",
        "batch_data": test_df,
        "partition_request": partition_request,
        "limit": None,
    }
    batch_request: BatchRequest = BatchRequest(**batch_request)

    batch_definition_list: List[BatchDefinition] = \
        test_pipeline_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=batch_request
        )

    assert len(batch_definition_list) == 0


def test__get_data_reference_list(basic_execution_environment):
    test_pipeline_data_connector: PipelineDataConnector = \
        basic_execution_environment.get_data_connector(name="test_pipeline_data_connector")

    expected_data_reference_list: List[str] = [""]

    # noinspection PyProtectedMember
    data_reference_list: List[str] = test_pipeline_data_connector._get_data_reference_list()

    assert data_reference_list == expected_data_reference_list


def test__generate_batch_spec_parameters_from_batch_definition(basic_execution_environment):
    partition_request: dict = {
        "partition_query": {
            "custom_key_0": "staging",
            "run_id": 1234567890,
        }
    }

    test_pipeline_data_connector: PipelineDataConnector = \
        basic_execution_environment.get_data_connector(name="test_pipeline_data_connector")

    expected_batch_spec_parameters: dict = {}

    # noinspection PyProtectedMember
    batch_spec_parameters: dict = test_pipeline_data_connector._generate_batch_spec_parameters_from_batch_definition(
        batch_definition=BatchDefinition(
            execution_environment_name="my_execution_environment",
            data_connector_name="test_pipeline_data_connector",
            data_asset_name="my_data_asset",
            partition_definition=PartitionDefinition(partition_request["partition_query"])
        )
    )

    assert batch_spec_parameters == expected_batch_spec_parameters


def test__build_batch_spec_from_batch_definition(basic_execution_environment):
    partition_request: dict = {
        "partition_query": {
            "custom_key_0": "staging",
            "run_id": 1234567890,
        }
    }

    test_pipeline_data_connector: PipelineDataConnector = \
        basic_execution_environment.get_data_connector(name="test_pipeline_data_connector")

    expected_batch_spec: InMemoryBatchSpec = InMemoryBatchSpec()

    # noinspection PyProtectedMember
    batch_spec: InMemoryBatchSpec = test_pipeline_data_connector._build_batch_spec_from_batch_definition(
        batch_definition=BatchDefinition(
            execution_environment_name="my_execution_environment",
            data_connector_name="test_pipeline_data_connector",
            data_asset_name="my_data_asset",
            partition_definition=PartitionDefinition(partition_request["partition_query"])
        )
    )
    assert batch_spec == expected_batch_spec
