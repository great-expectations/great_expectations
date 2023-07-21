from typing import Any, Dict, List, Union

import pytest

try:
    sqlalchemy = pytest.importorskip("sqlalchemy")
except ImportError:
    sqlalchemy = None

import great_expectations.exceptions as gx_exceptions
from great_expectations.core.batch import Batch, RuntimeBatchRequest
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context.util import instantiate_class_from_config
from great_expectations.datasource.new_datasource import Datasource

yaml = YAMLHandler()


@pytest.fixture
def datasource_with_runtime_data_connector_and_sqlalchemy_execution_engine(db_file, sa):
    basic_datasource: Datasource = instantiate_class_from_config(
        yaml.load(
            f"""
    class_name: Datasource

    execution_engine:
        class_name: SqlAlchemyExecutionEngine
        connection_string: sqlite:///{db_file}

    data_connectors:
        test_runtime_data_connector:
            module_name: great_expectations.datasource.data_connector
            class_name: RuntimeDataConnector
            batch_identifiers:
                - pipeline_stage_name
                - airflow_run_id
                - custom_key_0
            assets:
                asset_a:
                    batch_identifiers:
                        - day
                        - month
                asset_b:
                    batch_identifiers:
                        - day
                        - month
                        - year
        """,
        ),
        runtime_environment={"name": "my_datasource"},
        config_defaults={"module_name": "great_expectations.datasource"},
    )
    return basic_datasource


####################################
# Tests with data passed in as query
####################################


def test_datasource_with_runtime_data_connector_and_sqlalchemy_execution_engine_self_check(
    db_file, datasource_with_runtime_data_connector_and_sqlalchemy_execution_engine, sa
):
    report = (
        datasource_with_runtime_data_connector_and_sqlalchemy_execution_engine.self_check()
    )

    assert report == {
        "execution_engine": {
            "connection_string": f"sqlite:///{db_file}",
            "module_name": "great_expectations.execution_engine.sqlalchemy_execution_engine",
            "class_name": "SqlAlchemyExecutionEngine",
        },
        "data_connectors": {
            "count": 1,
            "test_runtime_data_connector": {
                "class_name": "RuntimeDataConnector",
                "data_asset_count": 2,
                "data_assets": {
                    "asset_a": {
                        "batch_definition_count": 0,
                        "example_data_references": [],
                    },
                    "asset_b": {
                        "batch_definition_count": 0,
                        "example_data_references": [],
                    },
                },
                "example_data_asset_names": ["asset_a", "asset_b"],
                "unmatched_data_reference_count": 0,
                "example_unmatched_data_references": [],
            },
        },
    }


def test_datasource_with_runtime_data_connector_and_sqlalchemy_execution_engine_available_data_asset(
    datasource_with_runtime_data_connector_and_sqlalchemy_execution_engine, sa
):
    expected_available_data_asset_names: Dict[List[str]] = {
        "test_runtime_data_connector": ["asset_a", "asset_b"]
    }
    available_data_asset_names: Dict[
        List[str]
    ] = (
        datasource_with_runtime_data_connector_and_sqlalchemy_execution_engine.get_available_data_asset_names()
    )
    assert available_data_asset_names == expected_available_data_asset_names


def test_batch_identifiers_and_batch_identifiers_success_all_keys_present_with_query(
    datasource_with_runtime_data_connector_and_sqlalchemy_execution_engine, sa
):
    # interacting with the database using query
    test_query: str = "SELECT * FROM table_full__I;"
    batch_identifiers: Dict[str, Union[str, int]] = {
        "pipeline_stage_name": "core_processing",
        "airflow_run_id": 1234567890,
        "custom_key_0": "custom_value_0",
    }

    # Verify that all keys in batch_identifiers are acceptable as batch_identifiers (using batch count).
    batch_request: Dict[str, Any] = {
        "datasource_name": datasource_with_runtime_data_connector_and_sqlalchemy_execution_engine.name,
        "data_connector_name": "test_runtime_data_connector",
        "data_asset_name": "TEMP_QUERY_DATA_ASSET",
        "runtime_parameters": {
            "query": test_query,
        },
        "batch_identifiers": batch_identifiers,
    }
    batch_request: RuntimeBatchRequest = RuntimeBatchRequest(**batch_request)
    batch_list: List[
        Batch
    ] = datasource_with_runtime_data_connector_and_sqlalchemy_execution_engine.get_batch_list_from_batch_request(
        batch_request=batch_request
    )
    assert len(batch_list[0].head().columns) == 11


def test_batch_identifiers_and_batch_identifiers_success_no_temp_table(
    datasource_with_runtime_data_connector_and_sqlalchemy_execution_engine, sa
):
    # interacting with the database using query
    test_query: str = "SELECT * FROM table_full__I"
    batch_identifiers: Dict[str, Union[str, int]] = {
        "pipeline_stage_name": "core_processing",
        "airflow_run_id": 1234567890,
        "custom_key_0": "custom_value_0",
    }

    # Verify that all keys in batch_identifiers are acceptable as batch_identifiers (using batch count).
    batch_request: Dict[str, Any] = {
        "datasource_name": datasource_with_runtime_data_connector_and_sqlalchemy_execution_engine.name,
        "data_connector_name": "test_runtime_data_connector",
        "data_asset_name": "TEMP_QUERY_DATA_ASSET",
        "runtime_parameters": {
            "query": test_query,
        },
        "batch_identifiers": batch_identifiers,
        "batch_spec_passthrough": {"create_temp_table": False},
    }
    batch_request: RuntimeBatchRequest = RuntimeBatchRequest(**batch_request)
    batch_list: List[
        Batch
    ] = datasource_with_runtime_data_connector_and_sqlalchemy_execution_engine.get_batch_list_from_batch_request(
        batch_request=batch_request
    )
    assert len(batch_list[0].head().columns) == 11


def test_batch_identifiers_and_batch_identifiers_error_illegal_key_with_query_mostly_legal_keys(
    datasource_with_runtime_data_connector_and_sqlalchemy_execution_engine, sa
):
    # interacting with the database using query
    test_query: str = "SELECT * FROM table_full__I"
    batch_identifiers: Dict[str, Union[str, int]] = {
        "pipeline_stage_name": "core_processing",
        "airflow_run_id": 1234567890,
        "custom_key_0": "custom_value_0",
        "i_am_illegal_key": "i_am_illegal_value",
    }

    # Ensure that keys in batch_identifiers that are not among batch_identifiers declared in
    # configuration are not accepted.  In this test, all legal keys plus a single illegal key are present.
    batch_request: Dict[str, Any] = {
        "datasource_name": datasource_with_runtime_data_connector_and_sqlalchemy_execution_engine.name,
        "data_connector_name": "test_runtime_data_connector",
        "data_asset_name": "TEMP_QUERY_DATA_ASSET",
        "runtime_parameters": {
            "query": test_query,
        },
        "batch_identifiers": batch_identifiers,
    }
    batch_request: RuntimeBatchRequest = RuntimeBatchRequest(**batch_request)

    with pytest.raises(gx_exceptions.DataConnectorError):
        # noinspection PyUnusedLocal
        batch_list: List[  # noqa: F841
            Batch
        ] = datasource_with_runtime_data_connector_and_sqlalchemy_execution_engine.get_batch_list_from_batch_request(
            batch_request=batch_request
        )


def test_batch_identifiers_and_batch_identifiers_error_illegal_key_with_query_single_illegal_key(
    datasource_with_runtime_data_connector_and_sqlalchemy_execution_engine, sa
):
    # interacting with the database using query
    test_query: str = "SELECT * FROM table_full__I;"
    batch_identifiers: Dict[str, Union[str, int]] = {"unknown_key": "some_value"}
    # Ensure that keys in batch_identifiers that are not among batch_identifiers declared in
    # configuration  are not accepted.  In this test, a single illegal key is present.
    batch_request: Dict[str, Any] = {
        "datasource_name": datasource_with_runtime_data_connector_and_sqlalchemy_execution_engine.name,
        "data_connector_name": "test_runtime_data_connector",
        "data_asset_name": "TEMP_QUERY_DATA_ASSET",
        "runtime_parameters": {
            "query": test_query,
        },
        "batch_identifiers": batch_identifiers,
    }
    batch_request: RuntimeBatchRequest = RuntimeBatchRequest(**batch_request)

    with pytest.raises(gx_exceptions.DataConnectorError):
        # noinspection PyUnusedLocal
        batch_list: List[  # noqa: F841
            Batch
        ] = datasource_with_runtime_data_connector_and_sqlalchemy_execution_engine.get_batch_list_from_batch_request(
            batch_request=batch_request
        )


def test_set_data_asset_name_for_runtime_query_data(
    datasource_with_runtime_data_connector_and_sqlalchemy_execution_engine, sa
):
    test_query: str = "SELECT * FROM table_full__I;"
    batch_identifiers: Dict[str, Union[str, int]] = {
        "pipeline_stage_name": "core_processing",
        "airflow_run_id": 1234567890,
        "custom_key_0": "custom_value_0",
    }

    # set : my_runtime_data_asset
    batch_request: Dict[str, Any] = {
        "datasource_name": datasource_with_runtime_data_connector_and_sqlalchemy_execution_engine.name,
        "data_connector_name": "test_runtime_data_connector",
        "data_asset_name": "my_runtime_data_asset",
        "runtime_parameters": {
            "query": test_query,
        },
        "batch_identifiers": batch_identifiers,
    }
    batch_request: RuntimeBatchRequest = RuntimeBatchRequest(**batch_request)
    batch_list: List[
        Batch
    ] = datasource_with_runtime_data_connector_and_sqlalchemy_execution_engine.get_batch_list_from_batch_request(
        batch_request=batch_request
    )
    assert batch_list[0].batch_definition.data_asset_name == "my_runtime_data_asset"


def test_get_batch_definition_list_from_batch_request_length_one_from_query(
    datasource_with_runtime_data_connector_and_sqlalchemy_execution_engine, sa
):
    # interacting with the database using query
    test_query: str = "SELECT * FROM table_full__I;"

    batch_identifiers: Dict[str, Union[str, int]] = {
        "airflow_run_id": 1234567890,
    }

    batch_request: Dict[str, Any] = {
        "datasource_name": datasource_with_runtime_data_connector_and_sqlalchemy_execution_engine.name,
        "data_connector_name": "test_runtime_data_connector",
        "data_asset_name": "my_data_asset",
        "runtime_parameters": {
            "query": test_query,
        },
        "batch_identifiers": batch_identifiers,
    }
    batch_request: RuntimeBatchRequest = RuntimeBatchRequest(**batch_request)
    batch_list: List[
        Batch
    ] = datasource_with_runtime_data_connector_and_sqlalchemy_execution_engine.get_batch_list_from_batch_request(
        batch_request=batch_request
    )
    # batches are a little bit more difficult to test because of batch_markers
    # they are ones that uniquely identify the data
    assert len(batch_list) == 1
    my_batch_1 = batch_list[0]
    assert my_batch_1.batch_spec is not None
    assert my_batch_1.batch_definition["data_asset_name"] == "my_data_asset"
    assert isinstance(my_batch_1.data.selectable, sqlalchemy.Table)


def test_get_batch_list_from_batch_request_length_one_from_query_named_asset(
    datasource_with_runtime_data_connector_and_sqlalchemy_execution_engine, sa
):
    # interacting with the database using query
    test_query: str = "SELECT * FROM table_full__I;"

    batch_identifiers: Dict[str, Union[str, int]] = {"day": 2, "month": 12}

    batch_request: Dict[str, Any] = {
        "datasource_name": datasource_with_runtime_data_connector_and_sqlalchemy_execution_engine.name,
        "data_connector_name": "test_runtime_data_connector",
        "data_asset_name": "asset_a",
        "runtime_parameters": {
            "query": test_query,
        },
        "batch_identifiers": batch_identifiers,
    }
    batch_request: RuntimeBatchRequest = RuntimeBatchRequest(**batch_request)
    batch_list: List[
        Batch
    ] = datasource_with_runtime_data_connector_and_sqlalchemy_execution_engine.get_batch_list_from_batch_request(
        batch_request=batch_request
    )
    # batches are a little bit more difficult to test because of batch_markers
    # they are ones that uniquely identify the data
    assert len(batch_list) == 1
    my_batch_1 = batch_list[0]
    assert my_batch_1.batch_spec is not None
    assert my_batch_1.batch_definition["data_asset_name"] == "asset_a"
    assert isinstance(my_batch_1.data.selectable, sqlalchemy.Table)


def test_get_batch_list_from_batch_request_length_one_from_query_named_asset_two_batch_requests(
    datasource_with_runtime_data_connector_and_sqlalchemy_execution_engine, sa
):
    # interacting with the database using query
    test_query: str = "SELECT * FROM table_full__I;"

    batch_identifiers: Dict[str, Union[str, int]] = {"day": 1, "month": 12}

    batch_request: Dict[str, Any] = {
        "datasource_name": datasource_with_runtime_data_connector_and_sqlalchemy_execution_engine.name,
        "data_connector_name": "test_runtime_data_connector",
        "data_asset_name": "asset_a",
        "runtime_parameters": {
            "query": test_query,
        },
        "batch_identifiers": batch_identifiers,
    }
    batch_request: RuntimeBatchRequest = RuntimeBatchRequest(**batch_request)
    batch_list: List[
        Batch
    ] = datasource_with_runtime_data_connector_and_sqlalchemy_execution_engine.get_batch_list_from_batch_request(
        batch_request=batch_request
    )
    # batches are a little bit more difficult to test because of batch_markers
    # they are ones that uniquely identify the data
    assert len(batch_list) == 1
    my_batch_1 = batch_list[0]
    assert my_batch_1.batch_spec is not None
    assert my_batch_1.batch_definition["data_asset_name"] == "asset_a"
    assert isinstance(my_batch_1.data.selectable, sqlalchemy.Table)

    # interacting with the database using query
    test_query: str = "SELECT * FROM table_full__I;"

    batch_identifiers: Dict[str, Union[str, int]] = {"day": 2, "month": 12}

    batch_request: Dict[str, Any] = {
        "datasource_name": datasource_with_runtime_data_connector_and_sqlalchemy_execution_engine.name,
        "data_connector_name": "test_runtime_data_connector",
        "data_asset_name": "asset_a",
        "runtime_parameters": {
            "query": test_query,
        },
        "batch_identifiers": batch_identifiers,
    }
    batch_request: RuntimeBatchRequest = RuntimeBatchRequest(**batch_request)
    batch_list: List[
        Batch
    ] = datasource_with_runtime_data_connector_and_sqlalchemy_execution_engine.get_batch_list_from_batch_request(
        batch_request=batch_request
    )
    # batches are a little bit more difficult to test because of batch_markers
    # they are ones that uniquely identify the data
    assert len(batch_list) == 1
    my_batch_2 = batch_list[0]
    assert my_batch_2.batch_spec is not None
    assert my_batch_2.batch_definition["data_asset_name"] == "asset_a"
    assert isinstance(my_batch_2.data.selectable, sqlalchemy.Table)


def test_get_batch_definitions_and_get_batch_basics_from_query(
    datasource_with_runtime_data_connector_and_sqlalchemy_execution_engine, sa
):
    # interacting with the database using query
    test_query: str = "SELECT * FROM table_full__I;"

    data_connector_name: str = "test_runtime_data_connector"
    data_asset_name: str = "test_asset_1"

    batch_request: Dict[str, Any] = {
        "datasource_name": datasource_with_runtime_data_connector_and_sqlalchemy_execution_engine.name,
        "data_connector_name": data_connector_name,
        "data_asset_name": data_asset_name,
        "runtime_parameters": {
            "query": test_query,
        },
        "batch_identifiers": {
            "airflow_run_id": 1234567890,
        },
    }
    batch_request: RuntimeBatchRequest = RuntimeBatchRequest(**batch_request)

    assert (
        len(
            datasource_with_runtime_data_connector_and_sqlalchemy_execution_engine.get_available_batch_definitions(
                batch_request=batch_request
            )
        )
        == 1
    )
