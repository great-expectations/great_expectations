import copy
import logging
from decimal import Decimal

import pandas as pd
import pytest

from great_expectations import DataContext
from great_expectations.checkpoint import Checkpoint
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.data_context.types.base import (
    AbstractConfig,
    CheckpointConfig,
    CheckpointValidationConfig,
    DatasourceConfig,
    checkpointConfigSchema,
    datasourceConfigSchema,
)
from great_expectations.marshmallow__shade import Schema
from great_expectations.rule_based_profiler.config import RuleBasedProfilerConfig
from great_expectations.rule_based_profiler.config.base import (
    ruleBasedProfilerConfigSchema,
)
from great_expectations.util import (
    deep_filter_properties_iterable,
    filter_properties_dict,
    requires_lossy_conversion,
)


def test_lossy_serialization_warning(caplog):
    caplog.set_level(logging.WARNING, logger="great_expectations.core")

    d = Decimal("12345.678901234567890123456789")

    convert_to_json_serializable(d)
    assert len(caplog.messages) == 1
    assert caplog.messages[0].startswith(
        "Using lossy conversion for decimal 12345.678901234567890123456789"
    )

    caplog.clear()
    d = Decimal("0.1")
    convert_to_json_serializable(d)
    print(caplog.messages)
    assert len(caplog.messages) == 0


def test_lossy_conversion():
    d = Decimal("12345.678901234567890123456789")
    assert requires_lossy_conversion(d)

    d = Decimal("12345.67890123456")
    assert requires_lossy_conversion(d)

    d = Decimal("12345.6789012345")
    assert not requires_lossy_conversion(d)

    d = Decimal("0.12345678901234567890123456789")
    assert requires_lossy_conversion(d)

    d = Decimal("0.1234567890123456")
    assert requires_lossy_conversion(d)

    d = Decimal("0.123456789012345")
    assert not requires_lossy_conversion(d)

    d = Decimal("0.1")
    assert not requires_lossy_conversion(d)


# TODO add unittests for convert_to_json_serializable() and ensure_json_serializable()
def test_serialization_of_spark_df(spark_session):
    df = pd.DataFrame({"a": [1, 2, 3]})
    sdf = spark_session.createDataFrame(df)
    assert convert_to_json_serializable(sdf) == {"a": [1, 2, 3]}

    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    sdf = spark_session.createDataFrame(df)
    assert convert_to_json_serializable(sdf) == {"a": [1, 2, 3], "b": [4, 5, 6]}


def test_batch_request_deepcopy():
    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})
    batch_request: RuntimeBatchRequest = RuntimeBatchRequest(
        **{
            "datasource_name": "my_datasource",
            "data_connector_name": "my_runtime_data_connector",
            "data_asset_name": "default_data_asset_name",
            "batch_identifiers": {
                "pipeline_stage_name": "core_processing",
                "airflow_run_id": 1234567890,
            },
            "runtime_parameters": {"batch_data": test_df},
        }
    )

    batch_request_copy: RuntimeBatchRequest = copy.deepcopy(batch_request)
    assert deep_filter_properties_iterable(
        properties=batch_request_copy.to_dict(),
        clean_falsy=True,
    ) == deep_filter_properties_iterable(
        properties=batch_request.to_dict(),
        clean_falsy=True,
    )


def test_checkpoint_config_deepcopy(
    titanic_pandas_data_context_with_v013_datasource_stats_enabled_with_checkpoints_v1_with_templates,
    monkeypatch,
):
    monkeypatch.setenv("GE_ENVIRONMENT", "my_ge_environment")
    monkeypatch.setenv("VAR", "test")
    monkeypatch.setenv("MY_PARAM", "1")
    monkeypatch.setenv("OLD_PARAM", "2")

    context: DataContext = titanic_pandas_data_context_with_v013_datasource_stats_enabled_with_checkpoints_v1_with_templates

    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    runtime_batch_request: RuntimeBatchRequest = RuntimeBatchRequest(
        **{
            "datasource_name": "my_datasource",
            "data_connector_name": "my_runtime_data_connector",
            "data_asset_name": "default_data_asset_name",
            "batch_identifiers": {
                "pipeline_stage_name": "core_processing",
                "airflow_run_id": 1234567890,
            },
            "runtime_parameters": {"batch_data": test_df},
        }
    )

    nested_checkpoint_config = CheckpointConfig(
        name="my_nested_checkpoint",
        config_version=1,
        template_name="my_nested_checkpoint_template_2",
        expectation_suite_name="users.delivery",
        validations=[
            {
                "batch_request": {
                    "datasource_name": "my_datasource",
                    "data_connector_name": "my_special_data_connector",
                    "data_asset_name": "users",
                    "data_connector_query": {"partition_index": -1},
                }
            },
            {
                "batch_request": {
                    "datasource_name": "my_datasource",
                    "data_connector_name": "my_other_data_connector",
                    "data_asset_name": "users",
                    "data_connector_query": {"partition_index": -2},
                }
            },
        ],
    )
    nested_checkpoint: Checkpoint = Checkpoint(
        data_context=context,
        **filter_properties_dict(
            properties=nested_checkpoint_config.to_json_dict(),
            delete_fields={"class_name", "module_name"},
            clean_falsy=True,
        ),
    )
    substituted_config_template_and_runtime_kwargs: dict = nested_checkpoint.get_substituted_config(
        runtime_kwargs={
            "batch_request": runtime_batch_request,
            "expectation_suite_name": "runtime_suite_name",
            "template_name": "my_nested_checkpoint_template_3",
            "validations": [
                {
                    "batch_request": {
                        "datasource_name": "my_datasource",
                        "data_connector_name": "my_other_data_connector_2_runtime",
                        "data_asset_name": "users",
                        "data_connector_query": {"partition_index": -3},
                    }
                },
                {
                    "batch_request": {
                        "datasource_name": "my_datasource",
                        "data_connector_name": "my_other_data_connector_3_runtime",
                        "data_asset_name": "users",
                        "data_connector_query": {"partition_index": -4},
                    }
                },
            ],
            "run_name_template": "runtime_run_template",
            "action_list": [
                {
                    "name": "store_validation_result",
                    "action": {
                        "class_name": "StoreValidationResultAction",
                    },
                },
                {
                    "name": "store_evaluation_params",
                    "action": {
                        "class_name": "MyCustomRuntimeStoreEvaluationParametersAction",
                    },
                },
                {
                    "name": "update_data_docs",
                    "action": None,
                },
                {
                    "name": "update_data_docs_deluxe_runtime",
                    "action": {
                        "class_name": "UpdateDataDocsAction",
                    },
                },
            ],
            "evaluation_parameters": {
                "environment": "runtime-$GE_ENVIRONMENT",
                "tolerance": 1.0e-2,
                "aux_param_0": "runtime-$MY_PARAM",
                "aux_param_1": "1 + $MY_PARAM",
                "new_runtime_eval_param": "bloopy!",
            },
            "runtime_configuration": {
                "result_format": "BASIC",
                "partial_unexpected_count": 999,
                "new_runtime_config_key": "bleepy!",
            },
        }
    )

    checkpoint_config_copy: dict = copy.deepcopy(
        substituted_config_template_and_runtime_kwargs
    )
    assert deep_filter_properties_iterable(
        properties=checkpoint_config_copy,
        clean_falsy=True,
    ) == deep_filter_properties_iterable(
        properties=substituted_config_template_and_runtime_kwargs,
        clean_falsy=True,
    )


def test_checkpoint_config_print(
    titanic_pandas_data_context_with_v013_datasource_stats_enabled_with_checkpoints_v1_with_templates,
    monkeypatch,
):
    monkeypatch.setenv("GE_ENVIRONMENT", "my_ge_environment")
    monkeypatch.setenv("VAR", "test")
    monkeypatch.setenv("MY_PARAM", "1")
    monkeypatch.setenv("OLD_PARAM", "2")

    context: DataContext = titanic_pandas_data_context_with_v013_datasource_stats_enabled_with_checkpoints_v1_with_templates

    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    runtime_batch_request: RuntimeBatchRequest = RuntimeBatchRequest(
        **{
            "datasource_name": "my_datasource",
            "data_connector_name": "my_runtime_data_connector",
            "data_asset_name": "default_data_asset_name",
            "batch_identifiers": {
                "pipeline_stage_name": "core_processing",
                "airflow_run_id": 1234567890,
            },
            "runtime_parameters": {"batch_data": test_df},
        }
    )

    nested_checkpoint_config = CheckpointConfig(
        name="my_nested_checkpoint",
        config_version=1,
        template_name="my_nested_checkpoint_template_2",
        expectation_suite_name="users.delivery",
        validations=[
            {
                "batch_request": {
                    "datasource_name": "my_datasource",
                    "data_connector_name": "my_special_data_connector",
                    "data_asset_name": "users",
                    "data_connector_query": {"partition_index": -1},
                }
            },
            {
                "batch_request": {
                    "datasource_name": "my_datasource",
                    "data_connector_name": "my_other_data_connector",
                    "data_asset_name": "users",
                    "data_connector_query": {"partition_index": -2},
                }
            },
        ],
    )
    nested_checkpoint: Checkpoint = Checkpoint(
        data_context=context,
        **filter_properties_dict(
            properties=nested_checkpoint_config.to_json_dict(),
            delete_fields={"class_name", "module_name"},
            clean_falsy=True,
        ),
    )
    substituted_config_template_and_runtime_kwargs: dict = nested_checkpoint.get_substituted_config(
        runtime_kwargs={
            "batch_request": runtime_batch_request,
            "expectation_suite_name": "runtime_suite_name",
            "template_name": "my_nested_checkpoint_template_3",
            "validations": [
                {
                    "batch_request": {
                        "datasource_name": "my_datasource",
                        "data_connector_name": "my_other_data_connector_2_runtime",
                        "data_asset_name": "users",
                        "data_connector_query": {"partition_index": -3},
                    }
                },
                {
                    "batch_request": {
                        "datasource_name": "my_datasource",
                        "data_connector_name": "my_other_data_connector_3_runtime",
                        "data_asset_name": "users",
                        "data_connector_query": {"partition_index": -4},
                    }
                },
            ],
            "run_name_template": "runtime_run_template",
            "action_list": [
                {
                    "name": "store_validation_result",
                    "action": {
                        "class_name": "StoreValidationResultAction",
                    },
                },
                {
                    "name": "store_evaluation_params",
                    "action": {
                        "class_name": "MyCustomRuntimeStoreEvaluationParametersAction",
                    },
                },
                {
                    "name": "update_data_docs",
                    "action": None,
                },
                {
                    "name": "update_data_docs_deluxe_runtime",
                    "action": {
                        "class_name": "UpdateDataDocsAction",
                    },
                },
            ],
            "evaluation_parameters": {
                "environment": "runtime-$GE_ENVIRONMENT",
                "tolerance": 1.0e-2,
                "aux_param_0": "runtime-$MY_PARAM",
                "aux_param_1": "1 + $MY_PARAM",
                "new_runtime_eval_param": "bloopy!",
            },
            "runtime_configuration": {
                "result_format": "BASIC",
                "partial_unexpected_count": 999,
                "new_runtime_config_key": "bleepy!",
            },
        }
    )

    expected_nested_checkpoint_config_template_and_runtime_template_name = (
        CheckpointConfig(
            name="my_nested_checkpoint",
            config_version=1.0,
            class_name="Checkpoint",
            module_name="great_expectations.checkpoint",
            template_name="my_nested_checkpoint_template_3",
            run_name_template="runtime_run_template",
            batch_request=runtime_batch_request.to_dict(),
            expectation_suite_name="runtime_suite_name",
            action_list=[
                {
                    "name": "store_validation_result",
                    "action": {
                        "class_name": "StoreValidationResultAction",
                    },
                },
                {
                    "name": "store_evaluation_params",
                    "action": {
                        "class_name": "MyCustomRuntimeStoreEvaluationParametersAction",
                    },
                },
                {
                    "name": "new_action_from_template_2",
                    "action": {"class_name": "Template2SpecialAction"},
                },
                {
                    "name": "new_action_from_template_3",
                    "action": {"class_name": "Template3SpecialAction"},
                },
                {
                    "name": "update_data_docs_deluxe_runtime",
                    "action": {
                        "class_name": "UpdateDataDocsAction",
                    },
                },
            ],
            evaluation_parameters={
                "environment": "runtime-my_ge_environment",
                "tolerance": 1.0e-2,
                "aux_param_0": "runtime-1",
                "aux_param_1": "1 + 1",
                "template_1_key": 456,
                "template_3_key": 123,
                "new_runtime_eval_param": "bloopy!",
            },
            runtime_configuration={
                "result_format": "BASIC",
                "partial_unexpected_count": 999,
                "template_1_key": 123,
                "template_3_key": "bloopy!",
                "new_runtime_config_key": "bleepy!",
            },
            validations=[
                {
                    "batch_request": {
                        "datasource_name": "my_datasource_template_1",
                        "data_connector_name": "my_special_data_connector_template_1",
                        "data_asset_name": "users_from_template_1",
                        "data_connector_query": {"partition_index": -999},
                    }
                },
                {
                    "batch_request": {
                        "datasource_name": "my_datasource",
                        "data_connector_name": "my_special_data_connector",
                        "data_asset_name": "users",
                        "data_connector_query": {"partition_index": -1},
                    }
                },
                {
                    "batch_request": {
                        "datasource_name": "my_datasource",
                        "data_connector_name": "my_other_data_connector",
                        "data_asset_name": "users",
                        "data_connector_query": {"partition_index": -2},
                    }
                },
                {
                    "batch_request": {
                        "datasource_name": "my_datasource",
                        "data_connector_name": "my_other_data_connector_2_runtime",
                        "data_asset_name": "users",
                        "data_connector_query": {"partition_index": -3},
                    }
                },
                {
                    "batch_request": {
                        "datasource_name": "my_datasource",
                        "data_connector_name": "my_other_data_connector_3_runtime",
                        "data_asset_name": "users",
                        "data_connector_query": {"partition_index": -4},
                    }
                },
            ],
        )
    )

    assert deep_filter_properties_iterable(
        properties=substituted_config_template_and_runtime_kwargs,
        clean_falsy=True,
    ) == deep_filter_properties_iterable(
        properties=expected_nested_checkpoint_config_template_and_runtime_template_name.to_dict(),
        clean_falsy=True,
    )

    substituted_config_template_and_runtime_kwargs_json_dict: dict = (
        convert_to_json_serializable(
            data=substituted_config_template_and_runtime_kwargs
        )
    )
    assert deep_filter_properties_iterable(
        properties=substituted_config_template_and_runtime_kwargs_json_dict,
        clean_falsy=True,
    ) == deep_filter_properties_iterable(
        properties=expected_nested_checkpoint_config_template_and_runtime_template_name.to_json_dict(),
        clean_falsy=True,
    )


@pytest.mark.parametrize(
    "checkpoint_config,expected_serialized_checkpoint_config",
    [
        pytest.param(
            CheckpointConfig(
                name="my_nested_checkpoint",
                config_version=1,
                template_name="my_nested_checkpoint_template",
                expectation_suite_name="users.delivery",
                validations=[
                    CheckpointValidationConfig(
                        batch_request={
                            "datasource_name": "my_datasource",
                            "data_connector_name": "my_data_connector",
                            "data_asset_name": "users",
                            "data_connector_query": {"partition_index": -1},
                        },
                    ),
                ],
            ),
            {
                "action_list": [],
                "batch_request": {},
                "class_name": "Checkpoint",
                "config_version": 1.0,
                "evaluation_parameters": {},
                "expectation_suite_ge_cloud_id": None,
                "expectation_suite_name": "users.delivery",
                "ge_cloud_id": None,
                "module_name": "great_expectations.checkpoint",
                "name": "my_nested_checkpoint",
                "profilers": [],
                "run_name_template": None,
                "runtime_configuration": {},
                "template_name": "my_nested_checkpoint_template",
                "validations": [
                    {
                        "batch_request": {
                            "data_asset_name": "users",
                            "data_connector_name": "my_data_connector",
                            "data_connector_query": {
                                "partition_index": -1,
                            },
                            "datasource_name": "my_datasource",
                        },
                    },
                ],
            },
            id="config_without_any_ids",
        ),
        pytest.param(
            CheckpointConfig(
                name="my_nested_checkpoint",
                config_version=1,
                default_validation_id="93e015ee-6405-4d5e-894c-741dc763f509",
                template_name="my_nested_checkpoint_template",
                expectation_suite_name="users.delivery",
                validations=[
                    CheckpointValidationConfig(
                        batch_request={
                            "datasource_name": "my_datasource",
                            "data_connector_name": "my_data_connector",
                            "data_asset_name": "users",
                            "data_connector_query": {"partition_index": -1},
                        },
                    ),
                ],
            ),
            {
                "action_list": [],
                "batch_request": {},
                "class_name": "Checkpoint",
                "config_version": 1.0,
                "default_validation_id": "93e015ee-6405-4d5e-894c-741dc763f509",
                "evaluation_parameters": {},
                "expectation_suite_ge_cloud_id": None,
                "expectation_suite_name": "users.delivery",
                "ge_cloud_id": None,
                "module_name": "great_expectations.checkpoint",
                "name": "my_nested_checkpoint",
                "profilers": [],
                "run_name_template": None,
                "runtime_configuration": {},
                "template_name": "my_nested_checkpoint_template",
                "validations": [
                    {
                        "batch_request": {
                            "data_asset_name": "users",
                            "data_connector_name": "my_data_connector",
                            "data_connector_query": {
                                "partition_index": -1,
                            },
                            "datasource_name": "my_datasource",
                        },
                    },
                ],
            },
            id="config_with_top_level_validation_id",
        ),
        pytest.param(
            CheckpointConfig(
                name="my_nested_checkpoint",
                config_version=1,
                default_validation_id="e3ff7a3a-3529-4c2a-be22-598493269680",
                template_name="my_nested_checkpoint_template",
                expectation_suite_name="users.delivery",
                validations=[
                    CheckpointValidationConfig(
                        batch_request={
                            "datasource_name": "my_datasource",
                            "data_connector_name": "my_data_connector",
                            "data_asset_name": "users",
                            "data_connector_query": {"partition_index": -1},
                        },
                        id_="06871341-f028-4f1f-b8e8-a559ab9f62e1",
                    ),
                ],
            ),
            {
                "action_list": [],
                "batch_request": {},
                "class_name": "Checkpoint",
                "config_version": 1.0,
                "default_validation_id": "e3ff7a3a-3529-4c2a-be22-598493269680",
                "evaluation_parameters": {},
                "expectation_suite_ge_cloud_id": None,
                "expectation_suite_name": "users.delivery",
                "ge_cloud_id": None,
                "module_name": "great_expectations.checkpoint",
                "name": "my_nested_checkpoint",
                "profilers": [],
                "run_name_template": None,
                "runtime_configuration": {},
                "template_name": "my_nested_checkpoint_template",
                "validations": [
                    {
                        "batch_request": {
                            "data_asset_name": "users",
                            "data_connector_name": "my_data_connector",
                            "data_connector_query": {
                                "partition_index": -1,
                            },
                            "datasource_name": "my_datasource",
                        },
                        "id": "06871341-f028-4f1f-b8e8-a559ab9f62e1",
                    },
                ],
            },
            id="config_with_nested_validation_id",
        ),
        pytest.param(
            CheckpointConfig(
                name="my_nested_checkpoint",
                config_version=1,
                template_name="my_nested_checkpoint_template",
                expectation_suite_name="users.delivery",
                validations=[
                    CheckpointValidationConfig(
                        batch_request={
                            "datasource_name": "my_datasource",
                            "data_connector_name": "my_data_connector",
                            "data_asset_name": "users",
                            "data_connector_query": {"partition_index": -1},
                        },
                        id_="06871341-f028-4f1f-b8e8-a559ab9f62e1",
                    ),
                ],
            ),
            {
                "action_list": [],
                "batch_request": {},
                "class_name": "Checkpoint",
                "config_version": 1.0,
                "evaluation_parameters": {},
                "expectation_suite_ge_cloud_id": None,
                "expectation_suite_name": "users.delivery",
                "ge_cloud_id": None,
                "module_name": "great_expectations.checkpoint",
                "name": "my_nested_checkpoint",
                "profilers": [],
                "run_name_template": None,
                "runtime_configuration": {},
                "template_name": "my_nested_checkpoint_template",
                "validations": [
                    {
                        "batch_request": {
                            "data_asset_name": "users",
                            "data_connector_name": "my_data_connector",
                            "data_connector_query": {
                                "partition_index": -1,
                            },
                            "datasource_name": "my_datasource",
                        },
                        "id": "06871341-f028-4f1f-b8e8-a559ab9f62e1",
                    },
                ],
            },
            id="config_with_top_level_and_nested_validation_ids",
        ),
    ],
)
def test_checkpoint_config_and_nested_objects_are_serialized(
    checkpoint_config: CheckpointConfig, expected_serialized_checkpoint_config: dict
) -> None:
    """CheckpointConfig and nested objects like CheckpointValidationConfig should be serialized appropriately with/without optional params."""
    observed_dump = checkpointConfigSchema.dump(checkpoint_config)
    assert observed_dump == expected_serialized_checkpoint_config

    loaded_data = checkpointConfigSchema.load(observed_dump)
    observed_load = CheckpointConfig(**loaded_data)
    assert checkpointConfigSchema.dump(observed_load) == checkpointConfigSchema.dump(
        checkpoint_config
    )


@pytest.mark.unit
@pytest.mark.parametrize(
    "config,schema,expected_serialized_config",
    [
        pytest.param(
            DatasourceConfig(
                class_name="Datasource",
            ),
            datasourceConfigSchema,
            {
                "class_name": "Datasource",
                "module_name": "great_expectations.datasource",
            },
            id="DatasourceConfig-minimal",
        ),
        pytest.param(
            DatasourceConfig(
                name="my_datasource",
                id_="d3a14abd-d4cb-4343-806e-55b555b15c28",
                class_name="Datasource",
            ),
            datasourceConfigSchema,
            {
                "name": "my_datasource",
                "id": "d3a14abd-d4cb-4343-806e-55b555b15c28",
                "class_name": "Datasource",
                "module_name": "great_expectations.datasource",
            },
            id="DatasourceConfig-minimal_with_name_and_id",
        ),
        pytest.param(
            DatasourceConfig(
                name="my_datasource",
                class_name="Datasource",
                data_connectors={
                    "my_data_connector": DatasourceConfig(
                        class_name="RuntimeDataConnector",
                        batch_identifiers=["default_identifier_name"],
                        id_="dd8fe6df-254b-4e37-9c0e-2c8205d1e988",
                    )
                },
            ),
            datasourceConfigSchema,
            {
                "name": "my_datasource",
                "class_name": "Datasource",
                "module_name": "great_expectations.datasource",
                "data_connectors": {
                    "my_data_connector": {
                        "class_name": "RuntimeDataConnector",
                        "module_name": "great_expectations.datasource",
                        "id": "dd8fe6df-254b-4e37-9c0e-2c8205d1e988",
                        "batch_identifiers": ["default_identifier_name"],
                    },
                },
            },
            id="DatasourceConfig-nested_data_connector_id",
        ),
    ],
)
def test_dict_round_trip_serialization(
    config: AbstractConfig,
    schema: Schema,
    expected_serialized_config: dict,
):
    observed_dump = datasourceConfigSchema.dump(config)

    round_tripped = config._dict_round_trip(schema, observed_dump)

    assert round_tripped == config.to_json_dict()

    assert (
        round_tripped.get("id_")
        == observed_dump.get("id")
        == expected_serialized_config.get("id")
    )
