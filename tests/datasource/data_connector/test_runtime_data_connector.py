import datetime
from typing import List

import pandas as pd
import pytest

import great_expectations.exceptions as gx_exceptions
from great_expectations.core.batch import (
    Batch,
    BatchDefinition,
    BatchSpec,
    RuntimeBatchRequest,
)
from great_expectations.core.batch_spec import (
    PathBatchSpec,
    RuntimeDataBatchSpec,
    RuntimeQueryBatchSpec,
    S3BatchSpec,
)
from great_expectations.core.id_dict import IDDict
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context.util import instantiate_class_from_config
from great_expectations.datasource import Datasource
from great_expectations.datasource.data_connector import RuntimeDataConnector

yaml = YAMLHandler()


@pytest.fixture
def basic_datasource_with_assets(tmp_path_factory):
    basic_datasource: Datasource = instantiate_class_from_config(
        config=yaml.load(
            """
class_name: Datasource

data_connectors:
    runtime:
        class_name: RuntimeDataConnector
        batch_identifiers:
            - hour
            - minute
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
execution_engine:
    class_name: PandasExecutionEngine
    """,
        ),
        runtime_environment={
            "name": "my_datasource",
        },
        config_defaults={
            "module_name": "great_expectations.datasource",
        },
    )
    return basic_datasource


def test_self_check(basic_datasource):
    test_runtime_data_connector: RuntimeDataConnector = (
        basic_datasource.data_connectors["test_runtime_data_connector"]
    )
    assert test_runtime_data_connector.self_check() == {
        "class_name": "RuntimeDataConnector",
        "data_asset_count": 0,
        "data_assets": {},
        "example_data_asset_names": [],
        "example_unmatched_data_references": [],
        "note": "RuntimeDataConnector will not have data_asset_names until they are "
        "passed in through RuntimeBatchRequest",
        "unmatched_data_reference_count": 0,
    }


def test_self_check_named_assets(basic_datasource_with_assets):
    test_runtime_data_connector: RuntimeDataConnector = (
        basic_datasource_with_assets.data_connectors["runtime"]
    )
    assert test_runtime_data_connector.self_check() == {
        "class_name": "RuntimeDataConnector",
        "data_asset_count": 2,
        "example_data_asset_names": ["asset_a", "asset_b"],
        "data_assets": {
            "asset_a": {"batch_definition_count": 0, "example_data_references": []},
            "asset_b": {"batch_definition_count": 0, "example_data_references": []},
        },
        "unmatched_data_reference_count": 0,
        "example_unmatched_data_references": [],
    }


def test_new_self_check_after_adding_named_asset_a(
    basic_datasource_with_assets, test_df_pandas
):
    runtime_data_connector: RuntimeDataConnector = (
        basic_datasource_with_assets.data_connectors["runtime"]
    )
    # noinspection PyUnusedLocal
    res: List[  # noqa: F841
        BatchDefinition
    ] = runtime_data_connector.get_batch_definition_list_from_batch_request(
        batch_request=RuntimeBatchRequest(
            datasource_name=basic_datasource_with_assets.name,
            data_connector_name="runtime",
            data_asset_name="asset_a",
            batch_identifiers={"month": 4, "day": 1},
            runtime_parameters={"batch_data": test_df_pandas},
        )
    )
    assert runtime_data_connector.self_check() == {
        "class_name": "RuntimeDataConnector",
        "data_asset_count": 2,
        "example_data_asset_names": ["asset_a", "asset_b"],
        "data_assets": {
            "asset_a": {
                "batch_definition_count": 1,
                "example_data_references": ["4-1"],
            },
            "asset_b": {"batch_definition_count": 0, "example_data_references": []},
        },
        "unmatched_data_reference_count": 0,
        "example_unmatched_data_references": [],
    }


def test_new_self_check_after_adding_new_asset_c(
    basic_datasource_with_assets, test_df_pandas
):
    runtime_data_connector: RuntimeDataConnector = (
        basic_datasource_with_assets.data_connectors["runtime"]
    )
    res: List[  # noqa: F841
        BatchDefinition
    ] = runtime_data_connector.get_batch_definition_list_from_batch_request(
        batch_request=RuntimeBatchRequest(
            datasource_name=basic_datasource_with_assets.name,
            data_connector_name="runtime",
            data_asset_name="asset_c",
            batch_identifiers={"hour": 12, "minute": 15},
            runtime_parameters={"batch_data": test_df_pandas},
        )
    )
    assert runtime_data_connector.self_check() == {
        "class_name": "RuntimeDataConnector",
        "data_asset_count": 3,
        "example_data_asset_names": ["asset_a", "asset_b", "asset_c"],
        "data_assets": {
            "asset_a": {"batch_definition_count": 0, "example_data_references": []},
            "asset_b": {"batch_definition_count": 0, "example_data_references": []},
            "asset_c": {
                "batch_definition_count": 1,
                "example_data_references": ["12-15"],
            },
        },
        "unmatched_data_reference_count": 0,
        "example_unmatched_data_references": [],
    }


def test_add_batch_identifiers_correct(basic_datasource_with_assets):
    test_runtime_data_connector: RuntimeDataConnector = (
        basic_datasource_with_assets.data_connectors["runtime"]
    )
    assert test_runtime_data_connector._batch_identifiers == {
        "runtime": ["hour", "minute"],
        "asset_a": ["day", "month"],
        "asset_b": ["day", "month", "year"],
    }


def test_batch_identifiers_missing_completely():
    # missing from base DataConnector
    with pytest.raises(gx_exceptions.DataConnectorError) as data_connector_error:
        instantiate_class_from_config(
            config=yaml.load(
                """
class_name: Datasource
data_connectors:
    runtime:
        class_name: RuntimeDataConnector
execution_engine:
    class_name: PandasExecutionEngine
    """,
            ),
            runtime_environment={
                "name": "my_datasource",
            },
            config_defaults={
                "module_name": "great_expectations.datasource",
            },
        )
    expected_error_message: str = """
        RuntimeDataConnector "runtime" requires batch_identifiers to be configured, either at the DataConnector or Asset-level.
    """
    assert str(data_connector_error.value).strip() == expected_error_message.strip()


def test_batch_identifiers_missing_from_named_asset():
    with pytest.raises(gx_exceptions.DataConnectorError) as data_connector_error:
        basic_datasource: Datasource = instantiate_class_from_config(  # noqa: F841
            config=yaml.load(
                """
class_name: Datasource
data_connectors:
    runtime:
        class_name: RuntimeDataConnector
        batch_identifiers:
            - hour
            - minute
        assets:
            asset_a:
execution_engine:
    class_name: PandasExecutionEngine
    """,
            ),
            runtime_environment={
                "name": "my_datasource",
            },
            config_defaults={
                "module_name": "great_expectations.datasource",
            },
        )

    expected_error_message: str = """
        RuntimeDataConnector "runtime" requires batch_identifiers to be configured when specifying Assets.
        """
    assert str(data_connector_error.value).strip() == expected_error_message.strip()


def test_error_checking_unknown_datasource(basic_datasource):
    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    test_runtime_data_connector: RuntimeDataConnector = (
        basic_datasource.data_connectors["test_runtime_data_connector"]
    )

    # Test for an unknown datasource
    with pytest.raises(ValueError):
        # noinspection PyUnusedLocal
        batch_definition_list: List[  # noqa: F841
            BatchDefinition
        ] = test_runtime_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=RuntimeBatchRequest(
                datasource_name="non_existent_datasource",
                data_connector_name="test_runtime_data_connector",
                data_asset_name="my_data_asset",
                runtime_parameters={"batch_data": test_df},
                batch_identifiers={"airflow_run_id": "first"},
            )
        )


def test_error_checking_unknown_data_connector(basic_datasource):
    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    test_runtime_data_connector: RuntimeDataConnector = (
        basic_datasource.data_connectors["test_runtime_data_connector"]
    )

    # Test for an unknown data_connector
    with pytest.raises(ValueError):
        # noinspection PyUnusedLocal
        batch_definition_list: List[  # noqa: F841
            BatchDefinition
        ] = test_runtime_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=RuntimeBatchRequest(
                datasource_name=basic_datasource.name,
                data_connector_name="non_existent_data_connector",
                data_asset_name="my_data_asset",
                runtime_parameters={"batch_data": test_df},
                batch_identifiers={"airflow_run_id": "first"},
            )
        )


def test_error_checking_missing_runtime_parameters(basic_datasource):
    test_runtime_data_connector: RuntimeDataConnector = (
        basic_datasource.data_connectors["test_runtime_data_connector"]
    )

    # test for missing runtime_parameters arg
    with pytest.raises(TypeError):
        # noinspection PyUnusedLocal, PyArgumentList
        batch_definition_list: List[  # noqa: F841
            BatchDefinition
        ] = test_runtime_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=RuntimeBatchRequest(
                datasource_name=basic_datasource.name,
                data_connector_name="test_runtime_data_connector",
                data_asset_name="my_data_asset",
                batch_identifiers={"pipeline_stage_name": "munge"},
            )
        )


def test_asset_is_name_batch_identifier_correctly_used(
    basic_datasource_with_assets, test_df_pandas
):
    """
    Using asset_a, which is named in the RuntimeDataConnector configuration, and using batch_identifier that is named.
    """
    runtime_data_connector: RuntimeDataConnector = (
        basic_datasource_with_assets.data_connectors["runtime"]
    )
    res: List[
        BatchDefinition
    ] = runtime_data_connector.get_batch_definition_list_from_batch_request(
        batch_request=RuntimeBatchRequest(
            datasource_name=basic_datasource_with_assets.name,
            data_connector_name="runtime",
            data_asset_name="asset_a",
            batch_identifiers={"month": 4, "day": 1},
            runtime_parameters={"batch_data": test_df_pandas},
        )
    )
    assert len(res) == 1
    assert res[0] == BatchDefinition(
        datasource_name="my_datasource",
        data_connector_name="runtime",
        data_asset_name="asset_a",
        batch_identifiers=IDDict({"month": 4, "day": 1}),
    )


def test_asset_is_named_but_batch_identifier_in_other_asset(
    basic_datasource_with_assets, test_df_pandas
):
    runtime_data_connector: RuntimeDataConnector = (
        basic_datasource_with_assets.data_connectors["runtime"]
    )
    with pytest.raises(gx_exceptions.DataConnectorError) as data_connector_error:
        runtime_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=RuntimeBatchRequest(
                datasource_name=basic_datasource_with_assets.name,
                data_connector_name="runtime",
                data_asset_name="asset_a",
                batch_identifiers={
                    "year": 2022,
                    "month": 4,
                    "day": 1,
                },  # year is only defined for asset_b
                runtime_parameters={"batch_data": test_df_pandas},
            )
        )
    expected_error_message: str = """
                Data Asset asset_a was invoked with one or more batch_identifiers
                that were not configured for the asset.

                The Data Asset was configured with : ['day', 'month']
                It was invoked with : ['year', 'month', 'day']
    """
    assert str(data_connector_error.value).strip() == expected_error_message.strip()


def test_asset_is_named_but_batch_identifier_not_defined_anywhere(
    basic_datasource_with_assets, test_df_pandas
):
    runtime_data_connector: RuntimeDataConnector = (
        basic_datasource_with_assets.data_connectors["runtime"]
    )
    with pytest.raises(gx_exceptions.DataConnectorError) as data_connector_error:
        runtime_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=RuntimeBatchRequest(
                datasource_name=basic_datasource_with_assets.name,
                data_connector_name="runtime",
                data_asset_name="asset_a",
                batch_identifiers={"blorg": 2022},  # blorg is not defined anywhere
                runtime_parameters={"batch_data": test_df_pandas},
            )
        )
    expected_error_message: str = """
                Data Asset asset_a was invoked with one or more batch_identifiers
                that were not configured for the asset.

                The Data Asset was configured with : ['day', 'month']
                It was invoked with : ['blorg']
    """
    assert str(data_connector_error.value).strip() == expected_error_message.strip()


def test_named_asset_is_trying_to_use_batch_identifier_defined_in_data_connector(
    basic_datasource_with_assets, test_df_pandas
):
    runtime_data_connector: RuntimeDataConnector = (
        basic_datasource_with_assets.data_connectors["runtime"]
    )
    with pytest.raises(gx_exceptions.DataConnectorError) as data_connector_error:
        runtime_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=RuntimeBatchRequest(
                datasource_name=basic_datasource_with_assets.name,
                data_connector_name="runtime",
                data_asset_name="asset_a",
                batch_identifiers={
                    "month": 4,
                    "day": 1,
                    "hour": 12,
                },  # hour is a data-connector level batch identifier
                runtime_parameters={"batch_data": test_df_pandas},
            )
        )
    expected_error_message: str = """
                Data Asset asset_a was invoked with one or more batch_identifiers
                that were not configured for the asset.

                The Data Asset was configured with : ['day', 'month']
                It was invoked with : ['month', 'day', 'hour']
    """
    assert str(data_connector_error.value).strip() == expected_error_message.strip()


def test_runtime_batch_request_trying_to_use_batch_identifier_defined_at_asset_level(
    basic_datasource_with_assets, test_df_pandas
):
    runtime_data_connector: RuntimeDataConnector = (
        basic_datasource_with_assets.data_connectors["runtime"]
    )
    with pytest.raises(gx_exceptions.DataConnectorError) as data_connector_error:
        runtime_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=RuntimeBatchRequest(
                datasource_name=basic_datasource_with_assets.name,
                data_connector_name="runtime",
                data_asset_name="new_asset",
                batch_identifiers={
                    "year": 2022,
                    "hour": 12,
                    "minute": 30,
                },  # year is a asset_a level batch identifier
                runtime_parameters={"batch_data": test_df_pandas},
            )
        )
    expected_error_message: str = """
                RuntimeDataConnector runtime was invoked with one or more batch identifiers that do not
        appear among the configured batch identifiers.

                The RuntimeDataConnector was configured with : ['hour', 'minute']
                It was invoked with : ['year', 'hour', 'minute']
    """
    assert str(data_connector_error.value).strip() == expected_error_message.strip()


def test_error_checking_too_many_runtime_parameters(basic_datasource):
    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    test_runtime_data_connector: RuntimeDataConnector = (
        basic_datasource.data_connectors["test_runtime_data_connector"]
    )

    # test for too many runtime_parameters keys
    with pytest.raises(gx_exceptions.InvalidBatchRequestError):
        # noinspection PyUnusedLocal
        batch_definition_list: List[  # noqa: F841
            BatchDefinition
        ] = test_runtime_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=RuntimeBatchRequest(
                datasource_name=basic_datasource.name,
                data_connector_name="test_runtime_data_connector",
                data_asset_name="my_data_asset",
                runtime_parameters={"batch_data": test_df, "path": "my_path"},
                batch_identifiers={"pipeline_stage_name": "munge"},
            )
        )


def test_batch_identifiers_and_batch_identifiers_success_all_keys_present(
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
        "data_asset_name": "IN_MEMORY_DATA_ASSET",
        "runtime_parameters": {"batch_data": test_df},
        "batch_identifiers": batch_identifiers,
    }
    batch_request: RuntimeBatchRequest = RuntimeBatchRequest(**batch_request)

    batch_definition_list: List[
        BatchDefinition
    ] = test_runtime_data_connector.get_batch_definition_list_from_batch_request(
        batch_request=batch_request
    )

    assert len(batch_definition_list) == 1


def test_batch_identifiers_and_batch_identifiers_error_illegal_keys(
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

    # Ensure that keys in batch_identifiers["batch_identifiers"] that are not among batch_identifiers declared in
    # configuration are not accepted.  In this test, all legal keys plus a single illegal key are present.
    batch_request: dict = {
        "datasource_name": basic_datasource.name,
        "data_connector_name": test_runtime_data_connector.name,
        "data_asset_name": "my_data_asset_name",
        "runtime_parameters": {"batch_data": test_df},
        "batch_identifiers": batch_identifiers,
    }
    batch_request: RuntimeBatchRequest = RuntimeBatchRequest(**batch_request)

    with pytest.raises(gx_exceptions.DataConnectorError) as data_connector_error:
        # noinspection PyUnusedLocal
        batch_definition_list: List[
            BatchDefinition
        ] = test_runtime_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=batch_request
        )
    expected_error_message: str = """
        RuntimeDataConnector test_runtime_data_connector was invoked with one or more batch identifiers that do not
        appear among the configured batch identifiers.

                The RuntimeDataConnector was configured with : ['pipeline_stage_name', 'airflow_run_id', 'custom_key_0']
                It was invoked with : ['pipeline_stage_name', 'airflow_run_id', 'custom_key_0', 'custom_key_1']
    """
    assert str(data_connector_error.value).strip() == expected_error_message.strip()

    batch_identifiers = {"batch_identifiers": {"unknown_key": "some_value"}}

    test_runtime_data_connector: RuntimeDataConnector = (
        basic_datasource.data_connectors["test_runtime_data_connector"]
    )

    # Ensure that keys in batch_identifiers["batch_identifiers"] that are not among batch_identifiers declared in configuration
    # are not accepted.  In this test, a single illegal key is present.
    batch_request: dict = {
        "datasource_name": basic_datasource.name,
        "data_connector_name": test_runtime_data_connector.name,
        "data_asset_name": "IN_MEMORY_DATA_ASSET",
        "runtime_parameters": {"batch_data": test_df},
        "batch_identifiers": batch_identifiers,
    }
    batch_request: RuntimeBatchRequest = RuntimeBatchRequest(**batch_request)

    with pytest.raises(gx_exceptions.DataConnectorError) as data_connector_error:
        # noinspection PyUnusedLocal
        batch_definition_list: List[  # noqa: F841
            BatchDefinition
        ] = test_runtime_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=batch_request
        )
    expected_error_message: str = """
        RuntimeDataConnector test_runtime_data_connector was invoked with one or more batch identifiers that do not
        appear among the configured batch identifiers.

                The RuntimeDataConnector was configured with : ['pipeline_stage_name', 'airflow_run_id', 'custom_key_0']
                It was invoked with : ['batch_identifiers']    """
    assert str(data_connector_error.value).strip() == expected_error_message.strip()


def test_get_available_data_asset_names(basic_datasource):
    test_runtime_data_connector: RuntimeDataConnector = (
        basic_datasource.data_connectors["test_runtime_data_connector"]
    )
    expected_available_data_asset_names: List[str] = []
    available_data_asset_names: List[
        str
    ] = test_runtime_data_connector.get_available_data_asset_names()
    assert available_data_asset_names == expected_available_data_asset_names


def test_get_available_data_asset_names_named_assets(basic_datasource_with_assets):
    runtime_data_connector: RuntimeDataConnector = (
        basic_datasource_with_assets.data_connectors["runtime"]
    )
    assert runtime_data_connector.get_available_data_asset_names() == [
        "asset_a",
        "asset_b",
    ]


def test_get_available_data_asset_names_updating_after_batch_request(
    basic_datasource_with_assets,
):
    test_runtime_data_connector: RuntimeDataConnector = (
        basic_datasource_with_assets.data_connectors["runtime"]
    )
    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    # empty if data_connector has not been used
    assert test_runtime_data_connector.get_available_data_asset_names() == [
        "asset_a",
        "asset_b",
    ]

    batch_identifiers = {
        "hour": 12,
        "minute": 15,
    }

    batch_request: dict = {
        "datasource_name": basic_datasource_with_assets.name,
        "data_connector_name": test_runtime_data_connector.name,
        "data_asset_name": "my_new_data_asset_1",
        "runtime_parameters": {
            "batch_data": test_df,
        },
        "batch_identifiers": batch_identifiers,
    }
    batch_request: RuntimeBatchRequest = RuntimeBatchRequest(**batch_request)

    # run with my_data_asset_1
    test_runtime_data_connector.get_batch_definition_list_from_batch_request(
        batch_request=batch_request
    )

    # updated to my_data_asset_1
    assert test_runtime_data_connector.get_available_data_asset_names() == [
        "asset_a",
        "asset_b",
        "my_new_data_asset_1",
    ]

    batch_identifiers = {
        "hour": 12,
        "minute": 30,
    }

    batch_request: dict = {
        "datasource_name": basic_datasource_with_assets.name,
        "data_connector_name": test_runtime_data_connector.name,
        "data_asset_name": "my_new_data_asset_2",
        "runtime_parameters": {
            "batch_data": test_df,
        },
        "batch_identifiers": batch_identifiers,
    }
    batch_request: RuntimeBatchRequest = RuntimeBatchRequest(**batch_request)

    # run with my_data_asset_2
    test_runtime_data_connector.get_batch_definition_list_from_batch_request(
        batch_request=batch_request
    )

    # updated to my_data_asset_1 and my_data_asset_2
    assert test_runtime_data_connector.get_available_data_asset_names() == [
        "asset_a",
        "asset_b",
        "my_new_data_asset_1",
        "my_new_data_asset_2",
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

    batch_identifiers = {
        "airflow_run_id": 1234567890,
    }

    batch_request: dict = {
        "datasource_name": basic_datasource.name,
        "data_connector_name": test_runtime_data_connector.name,
        "data_asset_name": "my_data_asset_1",
        "runtime_parameters": {
            "batch_data": test_df,
        },
        "batch_identifiers": batch_identifiers,
    }
    batch_request: RuntimeBatchRequest = RuntimeBatchRequest(**batch_request)

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
                    batch_identifiers=IDDict({"airflow_run_id": 1234567890}),
                )
            ],
        }
    }

    # update with
    test_df_new: pd.DataFrame = pd.DataFrame(data={"col1": [5, 6], "col2": [7, 8]})
    batch_identifiers = {
        "airflow_run_id": 987654321,
    }

    batch_request: dict = {
        "datasource_name": basic_datasource.name,
        "data_connector_name": test_runtime_data_connector.name,
        "data_asset_name": "my_data_asset_1",
        "runtime_parameters": {
            "batch_data": test_df_new,
        },
        "batch_identifiers": batch_identifiers,
    }
    batch_request: RuntimeBatchRequest = RuntimeBatchRequest(**batch_request)

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
                    batch_identifiers=IDDict({"airflow_run_id": 1234567890}),
                )
            ],
            "987654321": [
                BatchDefinition(
                    datasource_name="my_datasource",
                    data_connector_name="test_runtime_data_connector",
                    data_asset_name="my_data_asset_1",
                    batch_identifiers=IDDict({"airflow_run_id": 987654321}),
                )
            ],
        },
    }

    # new data_asset_name
    test_df_new_asset: pd.DataFrame = pd.DataFrame(
        data={"col1": [9, 10], "col2": [11, 12]}
    )
    batch_identifiers = {
        "airflow_run_id": 5555555,
    }

    batch_request: dict = {
        "datasource_name": basic_datasource.name,
        "data_connector_name": test_runtime_data_connector.name,
        "data_asset_name": "my_data_asset_2",
        "runtime_parameters": {
            "batch_data": test_df_new_asset,
        },
        "batch_identifiers": batch_identifiers,
    }
    batch_request: RuntimeBatchRequest = RuntimeBatchRequest(**batch_request)

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
                    batch_identifiers=IDDict({"airflow_run_id": 1234567890}),
                )
            ],
            "987654321": [
                BatchDefinition(
                    datasource_name="my_datasource",
                    data_connector_name="test_runtime_data_connector",
                    data_asset_name="my_data_asset_1",
                    batch_identifiers=IDDict({"airflow_run_id": 987654321}),
                )
            ],
        },
        "my_data_asset_2": {
            "5555555": [
                BatchDefinition(
                    datasource_name="my_datasource",
                    data_connector_name="test_runtime_data_connector",
                    data_asset_name="my_data_asset_2",
                    batch_identifiers=IDDict({"airflow_run_id": 5555555}),
                )
            ]
        },
    }

    assert test_runtime_data_connector.get_available_data_asset_names() == [
        "my_data_asset_1",
        "my_data_asset_2",
    ]

    assert test_runtime_data_connector.get_data_reference_count() == 3


def test_data_references_cache_updating_after_batch_request_named_assets(
    basic_datasource_with_assets,
):
    runtime_data_connector: RuntimeDataConnector = (
        basic_datasource_with_assets.data_connectors["runtime"]
    )

    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    # if data_connector contains the assets in configuration
    assert runtime_data_connector.get_available_data_asset_names() == [
        "asset_a",
        "asset_b",
    ]

    batch_identifiers: dict = {"day": 1, "month": 1}
    batch_request: dict = {
        "datasource_name": basic_datasource_with_assets.name,
        "data_connector_name": runtime_data_connector.name,
        "data_asset_name": "asset_a",
        "runtime_parameters": {
            "batch_data": test_df,
        },
        "batch_identifiers": batch_identifiers,
    }

    batch_request: RuntimeBatchRequest = RuntimeBatchRequest(**batch_request)

    # run with my_data_asset_1
    batch_definitions: List[
        BatchDefinition
    ] = runtime_data_connector.get_batch_definition_list_from_batch_request(
        batch_request=batch_request
    )
    assert batch_definitions == [
        BatchDefinition(
            datasource_name="my_datasource",
            data_connector_name="runtime",
            data_asset_name="asset_a",
            batch_identifiers=IDDict({"month": 1, "day": 1}),
        )
    ]
    assert runtime_data_connector._data_references_cache == {
        "asset_a": {
            "1-1": [
                BatchDefinition(
                    datasource_name="my_datasource",
                    data_connector_name="runtime",
                    data_asset_name="asset_a",
                    batch_identifiers=IDDict({"day": 1, "month": 1}),
                )
            ],
        }
    }
    batch_identifiers: dict = {"day": 1, "month": 2}
    batch_request: dict = {
        "datasource_name": basic_datasource_with_assets.name,
        "data_connector_name": runtime_data_connector.name,
        "data_asset_name": "asset_a",
        "runtime_parameters": {
            "batch_data": test_df,
        },
        "batch_identifiers": batch_identifiers,
    }
    batch_request: RuntimeBatchRequest = RuntimeBatchRequest(**batch_request)

    # run with another batch under asset_a
    batch_definitions: List[
        BatchDefinition
    ] = runtime_data_connector.get_batch_definition_list_from_batch_request(
        batch_request=batch_request
    )
    assert batch_definitions == [
        BatchDefinition(
            datasource_name="my_datasource",
            data_connector_name="runtime",
            data_asset_name="asset_a",
            batch_identifiers=IDDict({"month": 2, "day": 1}),
        ),
    ]
    assert runtime_data_connector._data_references_cache == {
        "asset_a": {
            "1-1": [
                BatchDefinition(
                    datasource_name="my_datasource",
                    data_connector_name="runtime",
                    data_asset_name="asset_a",
                    batch_identifiers=IDDict({"day": 1, "month": 1}),
                )
            ],
            "1-2": [
                BatchDefinition(
                    datasource_name="my_datasource",
                    data_connector_name="runtime",
                    data_asset_name="asset_a",
                    batch_identifiers=IDDict({"day": 1, "month": 2}),
                )
            ],
        }
    }


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
        "data_asset_name": "my_data_asset",
        "runtime_parameters": {"batch_data": test_df},
        "batch_identifiers": batch_identifiers,
    }
    batch_request: RuntimeBatchRequest = RuntimeBatchRequest(**batch_request)

    expected_batch_definition_list: List[BatchDefinition] = [
        BatchDefinition(
            datasource_name="my_datasource",
            data_connector_name="test_runtime_data_connector",
            data_asset_name="my_data_asset",
            batch_identifiers=IDDict(batch_identifiers),
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

    batch_identifiers = {
        "airflow_run_id": 1234567890,
    }

    test_runtime_data_connector: RuntimeDataConnector = (
        basic_datasource.data_connectors["test_runtime_data_connector"]
    )

    # data_asset_name is missing
    batch_request: dict = {
        "datasource_name": basic_datasource.name,
        "data_connector_name": test_runtime_data_connector.name,
        "runtime_parameters": {
            "batch_data": test_df,
        },
        "batch_identifiers": batch_identifiers,
    }
    with pytest.raises(TypeError):
        # noinspection PyUnusedLocal
        batch_request: RuntimeBatchRequest = RuntimeBatchRequest(**batch_request)

    # test that name can be set as "my_data_asset"
    batch_request: dict = {
        "datasource_name": basic_datasource.name,
        "data_connector_name": test_runtime_data_connector.name,
        "data_asset_name": "my_data_asset",
        "runtime_parameters": {
            "batch_data": test_df,
        },
        "batch_identifiers": batch_identifiers,
    }
    batch_request: RuntimeBatchRequest = RuntimeBatchRequest(**batch_request)
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
    batch_identifiers = {
        "custom_key_0": "staging",
        "airflow_run_id": 1234567890,
    }

    test_runtime_data_connector: RuntimeDataConnector = (
        basic_datasource.data_connectors["test_runtime_data_connector"]
    )

    expected_batch_spec_parameters: dict = {"data_asset_name": "my_data_asset"}

    # noinspection PyProtectedMember
    batch_spec_parameters: dict = test_runtime_data_connector._generate_batch_spec_parameters_from_batch_definition(
        batch_definition=BatchDefinition(
            datasource_name="my_datasource",
            data_connector_name="test_runtime_data_connector",
            data_asset_name="my_data_asset",
            batch_identifiers=IDDict(batch_identifiers),
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
        batch_identifiers=IDDict(batch_identifiers),
    )

    batch_spec: BatchSpec = test_runtime_data_connector.build_batch_spec(
        batch_definition=batch_definition,
        runtime_parameters={
            "batch_data": pd.DataFrame({"x": range(10)}),
        },
    )
    assert type(batch_spec) == RuntimeDataBatchSpec
    assert set(batch_spec.keys()) == {"batch_data", "data_asset_name"}
    assert batch_spec["batch_data"].shape == (10, 1)

    batch_spec: BatchSpec = test_runtime_data_connector.build_batch_spec(
        batch_definition=batch_definition,
        runtime_parameters={
            "query": "my_query",
        },
    )
    assert type(batch_spec) == RuntimeQueryBatchSpec

    batch_spec: BatchSpec = test_runtime_data_connector.build_batch_spec(
        batch_definition=batch_definition, runtime_parameters={"path": "my_path"}
    )
    assert type(batch_spec) == PathBatchSpec

    batch_spec: BatchSpec = test_runtime_data_connector.build_batch_spec(
        batch_definition=batch_definition,
        runtime_parameters={"path": "s3://my.s3.path"},
    )
    assert type(batch_spec) == S3BatchSpec

    batch_spec: BatchSpec = test_runtime_data_connector.build_batch_spec(
        batch_definition=batch_definition,
        runtime_parameters={"path": "s3a://my.s3.path"},
    )
    assert type(batch_spec) == S3BatchSpec


def test__get_data_reference_name(basic_datasource):
    data_connector_query: dict = {
        "batch_filter_parameters": {
            "airflow_run_id": 1234567890,
        }
    }
    batch_identifiers = IDDict(data_connector_query["batch_filter_parameters"])

    test_runtime_data_connector: RuntimeDataConnector = (
        basic_datasource.data_connectors["test_runtime_data_connector"]
    )

    assert (
        test_runtime_data_connector._get_data_reference_name(batch_identifiers)
        == "1234567890"
    )

    data_connector_query: dict = {
        "batch_filter_parameters": {
            "run_id_1": 1234567890,
            "run_id_2": 1111111111,
        }
    }
    batch_identifiers = IDDict(data_connector_query["batch_filter_parameters"])

    test_runtime_data_connector: RuntimeDataConnector = (
        basic_datasource.data_connectors["test_runtime_data_connector"]
    )

    assert (
        test_runtime_data_connector._get_data_reference_name(batch_identifiers)
        == "1234567890-1111111111"
    )


def test_batch_identifiers_datetime(
    basic_datasource,
):
    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    batch_identifiers: dict

    batch_identifiers = {
        "pipeline_stage_name": "core_processing",
        "airflow_run_id": 1234567890,
        "custom_key_0": datetime.datetime.utcnow(),
    }

    test_runtime_data_connector: RuntimeDataConnector = (
        basic_datasource.data_connectors["test_runtime_data_connector"]
    )

    # Verify that all keys in batch_identifiers are acceptable as batch_identifiers
    batch_request: dict = {
        "datasource_name": basic_datasource.name,
        "data_connector_name": test_runtime_data_connector.name,
        "data_asset_name": "IN_MEMORY_DATA_ASSET",
        "runtime_parameters": {"batch_data": test_df},
        "batch_identifiers": batch_identifiers,
    }
    batch_request: RuntimeBatchRequest = RuntimeBatchRequest(**batch_request)

    batch_definition = (
        test_runtime_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=batch_request
        )[0]
    )

    batch = Batch(
        data=test_df, batch_request=batch_request, batch_definition=batch_definition
    )

    try:
        _ = batch.id
    except TypeError:
        pytest.fail()


def test_temp_table_schema_name_included_in_batch_spec(basic_datasource):
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
        batch_identifiers=IDDict(batch_identifiers),
    )

    batch_spec: BatchSpec = test_runtime_data_connector.build_batch_spec(
        batch_definition=batch_definition,
        runtime_parameters={"query": "my_query", "temp_table_schema_name": "my_schema"},
    )
    assert type(batch_spec) == RuntimeQueryBatchSpec
    assert batch_spec["temp_table_schema_name"] == "my_schema"
