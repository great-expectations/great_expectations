from typing import List

import pandas as pd
import pytest
from ruamel.yaml import YAML

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.batch import BatchDefinition, BatchRequest, BatchSpec
from great_expectations.core.batch_spec import RuntimeDataBatchSpec
from great_expectations.core.id_dict import PartitionDefinition
from great_expectations.datasource.data_connector import RuntimeDataConnector

yaml = YAML()


def test_self_check(basic_datasource):
    test_runtime_data_connector: RuntimeDataConnector = (
        basic_datasource.data_connectors["test_runtime_data_connector"]
    )
    assert test_runtime_data_connector.self_check() == {
        "class_name": "RuntimeDataConnector",
        "data_asset_count": 0,
        "example_data_asset_names": [],
        "data_assets": {},
        "unmatched_data_reference_count": 0,
        "example_unmatched_data_references": [],
    }


def test_error_checking(basic_datasource):
    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    test_runtime_data_connector: RuntimeDataConnector = (
        basic_datasource.data_connectors["test_runtime_data_connector"]
    )

    # Test for an unknown datasource
    with pytest.raises(ValueError):
        # noinspection PyUnusedLocal
        batch_definition_list: List[
            BatchDefinition
        ] = test_runtime_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(
                datasource_name="non_existent_datasource",
                data_connector_name="test_runtime_data_connector",
                data_asset_name="my_data_asset",
            )
        )

    # Test for an unknown data_connector
    with pytest.raises(ValueError):
        # noinspection PyUnusedLocal
        batch_definition_list: List[
            BatchDefinition
        ] = test_runtime_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(
                datasource_name=basic_datasource.name,
                data_connector_name="non_existent_data_connector",
                data_asset_name="my_data_asset",
            )
        )

    # Test for illegal absence of partition_request when batch_data is specified
    with pytest.raises(ge_exceptions.DataConnectorError):
        # noinspection PyUnusedLocal
        batch_definition_list: List[
            BatchDefinition
        ] = test_runtime_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(
                datasource_name=basic_datasource.name,
                data_connector_name="test_runtime_data_connector",
                data_asset_name="my_data_asset",
                batch_data=test_df,
                partition_request=None,
            )
        )

    # Test for illegal nullity of partition_request["batch_identifiers"] when batch_data is specified
    partition_request: dict = {"batch_identifiers": None}
    with pytest.raises(ge_exceptions.DataConnectorError):
        # noinspection PyUnusedLocal
        batch_definition_list: List[
            BatchDefinition
        ] = test_runtime_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(
                datasource_name=basic_datasource.name,
                data_connector_name="test_runtime_data_connector",
                data_asset_name="my_data_asset",
                batch_data=test_df,
                partition_request=partition_request,
            )
        )

    # Test for illegal falsiness of partition_request["batch_identifiers"] when batch_data is specified
    partition_request: dict = {"batch_identifiers": {}}
    with pytest.raises(ge_exceptions.DataConnectorError):
        # noinspection PyUnusedLocal
        batch_definition_list: List[
            BatchDefinition
        ] = test_runtime_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(
                datasource_name=basic_datasource.name,
                data_connector_name="test_runtime_data_connector",
                data_asset_name="my_data_asset",
                batch_data=test_df,
                partition_request=partition_request,
            )
        )


def test_partition_request_and_runtime_keys_success_all_keys_present(
    basic_datasource,
):
    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    partition_request: dict

    partition_request = {
        "batch_identifiers": {
            "pipeline_stage_name": "core_processing",
            "airflow_run_id": 1234567890,
            "custom_key_0": "custom_value_0",
        }
    }

    test_runtime_data_connector: RuntimeDataConnector = (
        basic_datasource.data_connectors["test_runtime_data_connector"]
    )

    # Verify that all keys in partition_request are acceptable as runtime_keys (using batch count).
    batch_request: dict = {
        "datasource_name": basic_datasource.name,
        "data_connector_name": test_runtime_data_connector.name,
        "data_asset_name": "IN_MEMORY_DATA_ASSET",
        "batch_data": test_df,
        "partition_request": partition_request,
        "limit": None,
    }
    batch_request: BatchRequest = BatchRequest(**batch_request)

    batch_definition_list: List[
        BatchDefinition
    ] = test_runtime_data_connector.get_batch_definition_list_from_batch_request(
        batch_request=batch_request
    )

    assert len(batch_definition_list) == 1


def test_partition_request_and_runtime_keys_error_illegal_keys(
    basic_datasource,
):
    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    partition_request: dict

    partition_request = {
        "batch_identifiers": {
            "pipeline_stage_name": "core_processing",
            "airflow_run_id": 1234567890,
            "custom_key_0": "custom_value_0",
            "custom_key_1": "custom_value_1",
        }
    }

    test_runtime_data_connector: RuntimeDataConnector = (
        basic_datasource.data_connectors["test_runtime_data_connector"]
    )

    # Insure that keys in partition_request["batch_identifiers"] that are not among runtime_keys declared in configuration
    # are not accepted.  In this test, all legal keys plus a single illegal key are present.
    batch_request: dict = {
        "datasource_name": basic_datasource.name,
        "data_connector_name": test_runtime_data_connector.name,
        "data_asset_name": "IN_MEMORY_DATA_ASSET",
        "batch_data": test_df,
        "partition_request": partition_request,
        "limit": None,
    }
    batch_request: BatchRequest = BatchRequest(**batch_request)

    with pytest.raises(ge_exceptions.DataConnectorError):
        # noinspection PyUnusedLocal
        batch_definition_list: List[
            BatchDefinition
        ] = test_runtime_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=batch_request
        )

    partition_request = {"batch_identifiers": {"unknown_key": "some_value"}}

    test_runtime_data_connector: RuntimeDataConnector = (
        basic_datasource.data_connectors["test_runtime_data_connector"]
    )

    # Insure that keys in partition_request["batch_identifiers"] that are not among runtime_keys declared in configuration
    # are not accepted.  In this test, a single illegal key is present.
    batch_request: dict = {
        "datasource_name": basic_datasource.name,
        "data_connector_name": test_runtime_data_connector.name,
        "data_asset_name": "IN_MEMORY_DATA_ASSET",
        "batch_data": test_df,
        "partition_request": partition_request,
        "limit": None,
    }
    batch_request: BatchRequest = BatchRequest(**batch_request)

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
    expected_available_data_asset_names: List[str] = []
    available_data_asset_names: List[
        str
    ] = test_runtime_data_connector.get_available_data_asset_names()
    assert available_data_asset_names == expected_available_data_asset_names


def test_get_available_data_asset_names_updating_after_batch_request(basic_datasource):
    test_runtime_data_connector: RuntimeDataConnector = (
        basic_datasource.data_connectors["test_runtime_data_connector"]
    )
    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    # empty if data_connector has not been used
    assert test_runtime_data_connector.get_available_data_asset_names() == []

    partition_request: dict = {
        "batch_identifiers": {
            "airflow_run_id": 1234567890,
        }
    }
    batch_request: dict = {
        "datasource_name": basic_datasource.name,
        "data_connector_name": test_runtime_data_connector.name,
        "data_asset_name": "my_data_asset_1",
        "batch_data": test_df,
        "partition_request": partition_request,
        "limit": None,
    }
    batch_request: BatchRequest = BatchRequest(**batch_request)

    # run with my_data_asset_1
    test_runtime_data_connector.get_batch_definition_list_from_batch_request(
        batch_request=batch_request
    )

    # updated to my_data_asset_1
    assert test_runtime_data_connector.get_available_data_asset_names() == [
        "my_data_asset_1"
    ]

    partition_request: dict = {
        "batch_identifiers": {
            "airflow_run_id": 1234567890,
        }
    }
    batch_request: dict = {
        "datasource_name": basic_datasource.name,
        "data_connector_name": test_runtime_data_connector.name,
        "data_asset_name": "my_data_asset_2",
        "batch_data": test_df,
        "partition_request": partition_request,
        "limit": None,
    }
    batch_request: BatchRequest = BatchRequest(**batch_request)

    # run with my_data_asset_2
    test_runtime_data_connector.get_batch_definition_list_from_batch_request(
        batch_request=batch_request
    )

    # updated to my_data_asset_1 and my_data_asset_2
    assert test_runtime_data_connector.get_available_data_asset_names() == [
        "my_data_asset_1",
        "my_data_asset_2",
    ]


def test_data_references_cache_updating_after_batch_request(
    basic_datasource,
):
    test_runtime_data_connector: RuntimeDataConnector = (
        basic_datasource.data_connectors["test_runtime_data_connector"]
    )
    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    # empty if data_connector has not been used
    assert test_runtime_data_connector.get_available_data_asset_names() == []

    partition_request: dict = {
        "batch_identifiers": {
            "airflow_run_id": 1234567890,
        }
    }
    batch_request: dict = {
        "datasource_name": basic_datasource.name,
        "data_connector_name": test_runtime_data_connector.name,
        "data_asset_name": "my_data_asset_1",
        "batch_data": test_df,
        "partition_request": partition_request,
        "limit": None,
    }
    batch_request: BatchRequest = BatchRequest(**batch_request)

    # run with my_data_asset_1
    test_runtime_data_connector.get_batch_definition_list_from_batch_request(
        batch_request=batch_request
    )

    assert test_runtime_data_connector._data_references_cache == {
        "my_data_asset_1": {
            "1234567890": [
                BatchDefinition(
                    datasource_name="my_datasource",
                    data_connector_name="test_runtime_data_connector",
                    data_asset_name="my_data_asset_1",
                    partition_definition=PartitionDefinition(
                        {"airflow_run_id": 1234567890}
                    ),
                )
            ],
        }
    }

    # update with
    test_df_new: pd.DataFrame = pd.DataFrame(data={"col1": [5, 6], "col2": [7, 8]})
    partition_request: dict = {
        "batch_identifiers": {
            "airflow_run_id": 987654321,
        }
    }

    batch_request: dict = {
        "datasource_name": basic_datasource.name,
        "data_connector_name": test_runtime_data_connector.name,
        "data_asset_name": "my_data_asset_1",
        "batch_data": test_df_new,
        "partition_request": partition_request,
        "limit": None,
    }
    batch_request: BatchRequest = BatchRequest(**batch_request)

    # run with with new_data_asset but a new batch
    test_runtime_data_connector.get_batch_definition_list_from_batch_request(
        batch_request=batch_request
    )

    assert test_runtime_data_connector._data_references_cache == {
        "my_data_asset_1": {
            "1234567890": [
                BatchDefinition(
                    datasource_name="my_datasource",
                    data_connector_name="test_runtime_data_connector",
                    data_asset_name="my_data_asset_1",
                    partition_definition=PartitionDefinition(
                        {"airflow_run_id": 1234567890}
                    ),
                )
            ],
            "987654321": [
                BatchDefinition(
                    datasource_name="my_datasource",
                    data_connector_name="test_runtime_data_connector",
                    data_asset_name="my_data_asset_1",
                    partition_definition=PartitionDefinition(
                        {"airflow_run_id": 987654321}
                    ),
                )
            ],
        },
    }

    # new data_asset_name
    test_df_new_asset: pd.DataFrame = pd.DataFrame(
        data={"col1": [9, 10], "col2": [11, 12]}
    )
    partition_request: dict = {
        "batch_identifiers": {
            "airflow_run_id": 5555555,
        }
    }

    batch_request: dict = {
        "datasource_name": basic_datasource.name,
        "data_connector_name": test_runtime_data_connector.name,
        "data_asset_name": "my_data_asset_2",
        "batch_data": test_df_new_asset,
        "partition_request": partition_request,
        "limit": None,
    }
    batch_request: BatchRequest = BatchRequest(**batch_request)

    # run with with new_data_asset but a new batch
    test_runtime_data_connector.get_batch_definition_list_from_batch_request(
        batch_request=batch_request
    )

    assert test_runtime_data_connector._data_references_cache == {
        "my_data_asset_1": {
            "1234567890": [
                BatchDefinition(
                    datasource_name="my_datasource",
                    data_connector_name="test_runtime_data_connector",
                    data_asset_name="my_data_asset_1",
                    partition_definition=PartitionDefinition(
                        {"airflow_run_id": 1234567890}
                    ),
                )
            ],
            "987654321": [
                BatchDefinition(
                    datasource_name="my_datasource",
                    data_connector_name="test_runtime_data_connector",
                    data_asset_name="my_data_asset_1",
                    partition_definition=PartitionDefinition(
                        {"airflow_run_id": 987654321}
                    ),
                )
            ],
        },
        "my_data_asset_2": {
            "5555555": [
                BatchDefinition(
                    datasource_name="my_datasource",
                    data_connector_name="test_runtime_data_connector",
                    data_asset_name="my_data_asset_2",
                    partition_definition=PartitionDefinition(
                        {"airflow_run_id": 5555555}
                    ),
                )
            ]
        },
    }

    assert test_runtime_data_connector.get_available_data_asset_names() == [
        "my_data_asset_1",
        "my_data_asset_2",
    ]

    assert test_runtime_data_connector.get_data_reference_list_count() == 3


def test_get_batch_definition_list_from_batch_request_length_one(
    basic_datasource,
):
    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    partition_request: dict = {
        "batch_identifiers": {
            "airflow_run_id": 1234567890,
        }
    }

    test_runtime_data_connector: RuntimeDataConnector = (
        basic_datasource.data_connectors["test_runtime_data_connector"]
    )

    batch_request: dict = {
        "datasource_name": basic_datasource.name,
        "data_connector_name": test_runtime_data_connector.name,
        "data_asset_name": "my_data_asset",
        "batch_data": test_df,
        "partition_request": partition_request,
        "limit": None,
    }
    batch_request: BatchRequest = BatchRequest(**batch_request)

    expected_batch_definition_list: List[BatchDefinition] = [
        BatchDefinition(
            datasource_name="my_datasource",
            data_connector_name="test_runtime_data_connector",
            data_asset_name="my_data_asset",
            partition_definition=PartitionDefinition(
                partition_request["batch_identifiers"]
            ),
        )
    ]

    batch_definition_list: List[
        BatchDefinition
    ] = test_runtime_data_connector.get_batch_definition_list_from_batch_request(
        batch_request=batch_request
    )

    assert batch_definition_list == expected_batch_definition_list


def test_get_batch_definition_list_from_batch_request_with_and_without_data_asset_name(
    basic_datasource,
):
    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    partition_request: dict = {
        "batch_identifiers": {
            "airflow_run_id": 1234567890,
        }
    }

    test_runtime_data_connector: RuntimeDataConnector = (
        basic_datasource.data_connectors["test_runtime_data_connector"]
    )

    # data_asset_name is missing
    batch_request: dict = {
        "datasource_name": basic_datasource.name,
        "data_connector_name": test_runtime_data_connector.name,
        "batch_data": test_df,
        "partition_request": partition_request,
        "limit": None,
    }
    with pytest.raises(TypeError):
        batch_request: BatchRequest = BatchRequest(**batch_request)

    # test that name can be set as "my_data_asset"
    batch_request: dict = {
        "datasource_name": basic_datasource.name,
        "data_connector_name": test_runtime_data_connector.name,
        "data_asset_name": "my_data_asset",
        "batch_data": test_df,
        "partition_request": partition_request,
        "limit": None,
    }
    batch_request: BatchRequest = BatchRequest(**batch_request)
    batch_definition_list: List[
        BatchDefinition
    ] = test_runtime_data_connector.get_batch_definition_list_from_batch_request(
        batch_request=batch_request
    )
    assert len(batch_definition_list) == 1
    # check that default value has been set
    assert batch_definition_list[0]["data_asset_name"] == "my_data_asset"


def test__get_data_reference_list(basic_datasource):
    test_runtime_data_connector: RuntimeDataConnector = (
        basic_datasource.data_connectors["test_runtime_data_connector"]
    )

    expected_data_reference_list: List[str] = []

    # noinspection PyProtectedMember
    data_reference_list: List[
        str
    ] = test_runtime_data_connector._get_data_reference_list()

    assert data_reference_list == expected_data_reference_list


def test_refresh_data_references_cache(basic_datasource):
    test_runtime_data_connector: RuntimeDataConnector = (
        basic_datasource.data_connectors["test_runtime_data_connector"]
    )
    assert len(test_runtime_data_connector._data_references_cache) == 0


def test__generate_batch_spec_parameters_from_batch_definition(
    basic_datasource,
):
    partition_request: dict = {
        "batch_identifiers": {
            "custom_key_0": "staging",
            "airflow_run_id": 1234567890,
        }
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
                partition_request["batch_identifiers"]
            ),
        )
    )

    assert batch_spec_parameters == expected_batch_spec_parameters


def test__build_batch_spec(basic_datasource):
    partition_request: dict = {
        "batch_identifiers": {
            "custom_key_0": "staging",
            "airflow_run_id": 1234567890,
        }
    }

    test_runtime_data_connector: RuntimeDataConnector = (
        basic_datasource.data_connectors["test_runtime_data_connector"]
    )

    # noinspection PyProtectedMember
    batch_spec: BatchSpec = test_runtime_data_connector.build_batch_spec(
        batch_definition=BatchDefinition(
            datasource_name="my_datasource",
            data_connector_name="test_runtime_data_connector",
            data_asset_name="my_data_asset",
            partition_definition=PartitionDefinition(
                partition_request["batch_identifiers"]
            ),
        ),
        batch_data=pd.DataFrame({"x": range(10)}),
    )
    assert type(batch_spec) == RuntimeDataBatchSpec
    assert set(batch_spec.keys()) == {"batch_data"}
    assert batch_spec["batch_data"].shape == (10, 1)


def test__get_data_reference_name(basic_datasource):
    partition_request: dict = {
        "batch_identifiers": {
            "airflow_run_id": 1234567890,
        }
    }
    partition_definition = PartitionDefinition(partition_request["batch_identifiers"])

    test_runtime_data_connector: RuntimeDataConnector = (
        basic_datasource.data_connectors["test_runtime_data_connector"]
    )

    assert (
        test_runtime_data_connector._get_data_reference_name(partition_definition)
        == "1234567890"
    )

    partition_request: dict = {
        "batch_identifiers": {
            "run_id_1": 1234567890,
            "run_id_2": 1111111111,
        }
    }
    partition_definition = PartitionDefinition(partition_request["batch_identifiers"])

    test_runtime_data_connector: RuntimeDataConnector = (
        basic_datasource.data_connectors["test_runtime_data_connector"]
    )

    assert (
        test_runtime_data_connector._get_data_reference_name(partition_definition)
        == "1234567890-1111111111"
    )
