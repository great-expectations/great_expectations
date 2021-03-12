from typing import List

import pandas as pd
import pytest
from ruamel.yaml import YAML

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.batch import BatchDefinition, BatchRequest, BatchSpec, RuntimeBatchRequest
from great_expectations.core.batch_spec import RuntimeDataBatchSpec, RuntimeQueryBatchSpec, PathBatchSpec, S3BatchSpec
from great_expectations.core.id_dict import PartitionDefinition
from great_expectations.datasource.data_connector import RuntimeDataConnector

yaml = YAML()


def test_self_check(basic_datasource):
    test_runtime_data_connector: RuntimeDataConnector = (
        basic_datasource.data_connectors["test_runtime_data_connector"]
    )

    assert test_runtime_data_connector.self_check() == {
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
    }


def test_error_checking(basic_datasource):
    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    test_runtime_data_connector: RuntimeDataConnector = (
        basic_datasource.data_connectors["test_runtime_data_connector"]
    )

    # Test for an unknown execution environment
    with pytest.raises(ValueError):
        # noinspection PyUnusedLocal
        batch_definition_list: List[
            BatchDefinition
        ] = test_runtime_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=RuntimeBatchRequest(
                datasource_name="non_existent_datasource",
                data_connector_name="test_runtime_data_connector",
                data_asset_name="my_data_asset",
                runtime_parameters={
                    "batch_data": test_df
                }
            )
        )

    # Test for an unknown data_connector
    with pytest.raises(ValueError):
        # noinspection PyUnusedLocal
        batch_definition_list: List[
            BatchDefinition
        ] = test_runtime_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=RuntimeBatchRequest(
                datasource_name=basic_datasource.name,
                data_connector_name="non_existent_data_connector",
                data_asset_name="my_data_asset",
                runtime_parameters={
                    "batch_data": test_df
                }
            )
        )

    # test for missing runtime_parameters arg
    with pytest.raises(TypeError):
        # noinspection PyUnusedLocal
        batch_definition_list: List[
            BatchDefinition
        ] = test_runtime_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=RuntimeBatchRequest(
                datasource_name=basic_datasource.name,
                data_connector_name="non_existent_data_connector",
                data_asset_name="my_data_asset",
            )
        )

    # test for too many runtime_parameters keys
    with pytest.raises(ge_exceptions.InvalidBatchRequestError):
        # noinspection PyUnusedLocal
        batch_definition_list: List[
            BatchDefinition
        ] = test_runtime_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=RuntimeBatchRequest(
                datasource_name=basic_datasource.name,
                data_connector_name="non_existent_data_connector",
                data_asset_name="my_data_asset",
                runtime_parameters={
                    "batch_data": test_df,
                    "path": "my_path"
                }
            )
        )


def test_batch_identifiers_and_runtime_keys_success_all_keys_present(
    basic_datasource,
):
    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    batch_identifiers: dict

    batch_identifiers = {
            "pipeline_stage_name": "core_processing",
            "airflow_run_id": 1234567890,
            "custom_key_0": "custom_value_0",
        }

    test_runtime_data_connector: RuntimeDataConnector = (
        basic_datasource.data_connectors["test_runtime_data_connector"]
    )

    # Verify that all keys in batch_identifiers are acceptable as batch_identifiers (using batch count).
    batch_request: dict = {
        "datasource_name": basic_datasource.name,
        "data_connector_name": test_runtime_data_connector.name,
        "data_asset_name": "my_data_asset_name",
        "runtime_parameters": {"batch_data": test_df},
        "batch_identifiers": batch_identifiers,
    }
    batch_request: BatchRequest = RuntimeBatchRequest(**batch_request)

    batch_definition_list: List[
        BatchDefinition
    ] = test_runtime_data_connector.get_batch_definition_list_from_batch_request(
        batch_request=batch_request
    )

    assert len(batch_definition_list) == 1


def test_batch_identifiers_and_runtime_keys_error_illegal_keys(
    basic_datasource,
):
    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    batch_identifiers: dict

    batch_identifiers = {
        "pipeline_stage_name": "core_processing",
        "airflow_run_id": 1234567890,
        "custom_key_0": "custom_value_0",
        "custom_key_1": "custom_value_1",
    }

    test_runtime_data_connector: RuntimeDataConnector = (
        basic_datasource.data_connectors["test_runtime_data_connector"]
    )

    # Insure that keys in batch_identifiers["batch_identifiers"] that are not among batch_identifiers declared in
    # configuration
    # are not accepted.  In this test, all legal keys plus a single illegal key are present.
    batch_request: dict = {
        "datasource_name": basic_datasource.name,
        "data_connector_name": test_runtime_data_connector.name,
        "data_asset_name": "my_data_asset_name",
        "runtime_parameters": {
            "batch_data": test_df
        },
        "batch_identifiers": batch_identifiers,
    }
    batch_request: BatchRequest = RuntimeBatchRequest(**batch_request)

    with pytest.raises(ge_exceptions.DataConnectorError):
        # noinspection PyUnusedLocal
        batch_definition_list: List[
            BatchDefinition
        ] = test_runtime_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=batch_request
        )

    batch_identifiers = {"batch_identifiers": {"unknown_key": "some_value"}}

    test_runtime_data_connector: RuntimeDataConnector = (
        basic_datasource.data_connectors["test_runtime_data_connector"]
    )

    # Insure that keys in batch_identifiers["batch_identifiers"] that are not among batch_identifiers declared in
    # configuration
    # are not accepted.  In this test, a single illegal key is present.
    batch_request: dict = {
        "datasource_name": basic_datasource.name,
        "data_connector_name": test_runtime_data_connector.name,
        "data_asset_name": "IN_MEMORY_DATA_ASSET",
        "runtime_parameters": {
            "batch_data": test_df
        },
        "batch_identifiers": batch_identifiers,
    }
    batch_request: BatchRequest = RuntimeBatchRequest(**batch_request)

    with pytest.raises(ge_exceptions.DataConnectorError):
        # noinspection PyUnusedLocal
        batch_definition_list: List[
            BatchDefinition
        ] = test_runtime_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=batch_request
        )


def test_get_available_data_asset_names(basic_datasource):
    test_runtime_data_connector: RuntimeDataConnector = (
        basic_datasource.data_connectors["test_runtime_data_connector"]
    )

    expected_available_data_asset_names: List[str] = ["IN_MEMORY_DATA_ASSET"]

    available_data_asset_names: List[
        str
    ] = test_runtime_data_connector.get_available_data_asset_names()

    assert available_data_asset_names == expected_available_data_asset_names


def test_get_batch_definition_list_from_batch_request_length_one(
    basic_datasource,
):
    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    batch_identifiers: dict = {
        "airflow_run_id": 1234567890,
    }

    test_runtime_data_connector: RuntimeDataConnector = (
        basic_datasource.data_connectors["test_runtime_data_connector"]
    )

    batch_request: dict = {
        "datasource_name": basic_datasource.name,
        "data_connector_name": test_runtime_data_connector.name,
        "data_asset_name": "my_data_asset_name",
        "runtime_parameters": {
            "batch_data": test_df
        },
        "batch_identifiers": batch_identifiers,
    }
    batch_request: BatchRequest = RuntimeBatchRequest(**batch_request)

    expected_batch_definition_list: List[BatchDefinition] = [
        BatchDefinition(
            datasource_name="my_datasource",
            data_connector_name="test_runtime_data_connector",
            data_asset_name="my_data_asset_name",
            partition_definition=PartitionDefinition(
                batch_identifiers
            ),
        )
    ]

    batch_definition_list: List[
        BatchDefinition
    ] = test_runtime_data_connector.get_batch_definition_list_from_batch_request(
        batch_request=batch_request
    )

    assert batch_definition_list == expected_batch_definition_list


def test__get_data_reference_list(basic_datasource):
    test_runtime_data_connector: RuntimeDataConnector = (
        basic_datasource.data_connectors["test_runtime_data_connector"]
    )

    expected_data_reference_list: List[str] = [""]

    # noinspection PyProtectedMember
    data_reference_list: List[
        str
    ] = test_runtime_data_connector._get_data_reference_list()

    assert data_reference_list == expected_data_reference_list


def test__generate_batch_spec_parameters_from_batch_definition(
    basic_datasource,
):
    batch_identifiers = {
        "custom_key_0": "staging",
        "airflow_run_id": 1234567890,
    }

    test_runtime_data_connector: RuntimeDataConnector = (
        basic_datasource.data_connectors["test_runtime_data_connector"]
    )

    expected_batch_spec_parameters: dict = {}

    # noinspection PyProtectedMember
    batch_spec_parameters: dict = test_runtime_data_connector._generate_batch_spec_parameters_from_batch_definition(
        batch_definition=BatchDefinition(
            datasource_name="my_datasource",
            data_connector_name="test_runtime_data_connector",
            data_asset_name="my_data_asset",
            partition_definition=PartitionDefinition(
                batch_identifiers
            ),
        )
    )

    assert batch_spec_parameters == expected_batch_spec_parameters


def test__build_batch_spec(basic_datasource):
    batch_identifiers = {
        "custom_key_0": "staging",
        "airflow_run_id": 1234567890,
    }

    test_runtime_data_connector: RuntimeDataConnector = (
        basic_datasource.data_connectors["test_runtime_data_connector"]
    )

    batch_definition = BatchDefinition(
        datasource_name="my_datasource",
        data_connector_name="test_runtime_data_connector",
        data_asset_name="my_data_asset",
        partition_definition=PartitionDefinition(
            batch_identifiers
        ),
    )

    batch_spec: BatchSpec = test_runtime_data_connector.build_batch_spec(
        batch_definition=batch_definition,
        runtime_parameters={
            "batch_data": pd.DataFrame({"x": range(10)}),
        }
    )
    assert type(batch_spec) == RuntimeDataBatchSpec
    assert set(batch_spec.keys()) == {"batch_data"}
    assert batch_spec["batch_data"].shape == (10, 1)

    batch_spec: BatchSpec = test_runtime_data_connector.build_batch_spec(
        batch_definition=batch_definition,
        runtime_parameters={
            "query": "my_query",
        }
    )
    assert type(batch_spec) == RuntimeQueryBatchSpec

    batch_spec: BatchSpec = test_runtime_data_connector.build_batch_spec(
        batch_definition=batch_definition,
        runtime_parameters={
            "path": "my_path"
        }
    )
    assert type(batch_spec) == PathBatchSpec

    batch_spec: BatchSpec = test_runtime_data_connector.build_batch_spec(
        batch_definition=batch_definition,
        runtime_parameters={
            "path": "s3://my.s3.path"
        }
    )
    assert type(batch_spec) == S3BatchSpec

    batch_spec: BatchSpec = test_runtime_data_connector.build_batch_spec(
        batch_definition=batch_definition,
        runtime_parameters={
            "path": "s3a://my.s3.path"
        }
    )
    assert type(batch_spec) == S3BatchSpec
