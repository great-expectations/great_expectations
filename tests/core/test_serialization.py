import copy
import inspect
import logging
from decimal import Decimal
from typing import Union
from unittest import mock

import pandas as pd
import pytest
from _pytest.fixtures import FixtureRequest
from marshmallow import Schema

from great_expectations import DataContext
from great_expectations.checkpoint import Checkpoint
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.data_context.types.base import (
    AbstractConfig,
    AssetConfig,
    CheckpointConfig,
    CheckpointValidationConfig,
    DataConnectorConfig,
    DatasourceConfig,
    ExecutionEngineConfig,
    assetConfigSchema,
    checkpointConfigSchema,
    dataConnectorConfigSchema,
    datasourceConfigSchema,
)
from great_expectations.util import (
    deep_filter_properties_iterable,
    filter_properties_dict,
    requires_lossy_conversion,
)

try:
    from pyspark.sql.types import IntegerType, StructField, StructType
except ImportError:
    IntegerType = None
    StructField = None
    StructType = None


@pytest.fixture
def spark_schema(spark_session) -> "StructType":
    return StructType(
        [
            StructField("a", IntegerType(), True, None),
            StructField("b", IntegerType(), True, None),
        ]
    )


# The following fixtures are used by parameterized tests for serializing
# Spark schemas. They follow the pattern described in:
# https://miguendes.me/how-to-use-fixtures-as-arguments-in-pytestmarkparametrize
@pytest.fixture
def checkpoint_config_spark(spark_session) -> CheckpointConfig:
    return CheckpointConfig(
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
                    "batch_spec_passthrough": {"reader_options": {"header": True}},
                },
                id="06871341-f028-4f1f-b8e8-a559ab9f62e1",
            ),
        ],
    )


@pytest.fixture
def checkpoint_config_with_schema_spark(
    spark_session, spark_schema
) -> CheckpointConfig:
    return CheckpointConfig(
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
                    "batch_spec_passthrough": {
                        "reader_options": {"schema": spark_schema}
                    },
                },
                id="06871341-f028-4f1f-b8e8-a559ab9f62e1",
            ),
        ],
    )


@pytest.fixture
def datasource_config_spark(spark_session) -> DatasourceConfig:
    return DatasourceConfig(
        name="taxi_data",
        class_name="Datasource",
        module_name="great_expectations.datasource",
        execution_engine=ExecutionEngineConfig(
            class_name="SparkDFExecutionEngine",
            module_name="great_expectations.execution_engine.sparkdf_execution_engine",
        ),
        data_connectors={
            "configured_asset_connector": DataConnectorConfig(
                class_name="ConfiguredAssetFilesystemDataConnector",
                module_name="great_expectations.datasource.data_connector.configured_asset_filesystem_data_connector",
                assets={
                    "my_asset": AssetConfig(
                        class_name="Asset",
                        module_name="great_expectations.datasource.data_connector.asset",
                        batch_spec_passthrough={
                            "reader_options": {"header": True},
                        },
                    )
                },
            )
        },
    )


@pytest.fixture
def datasource_config_with_schema_at_asset_level_spark(
    spark_session, spark_schema
) -> DatasourceConfig:
    return DatasourceConfig(
        name="taxi_data",
        class_name="Datasource",
        module_name="great_expectations.datasource",
        execution_engine=ExecutionEngineConfig(
            class_name="SparkDFExecutionEngine",
            module_name="great_expectations.execution_engine.sparkdf_execution_engine",
        ),
        data_connectors={
            "configured_asset_connector": DataConnectorConfig(
                class_name="ConfiguredAssetFilesystemDataConnector",
                module_name="great_expectations.datasource.data_connector.configured_asset_filesystem_data_connector",
                assets={
                    "my_asset": AssetConfig(
                        class_name="Asset",
                        module_name="great_expectations.datasource.data_connector.asset",
                        batch_spec_passthrough={
                            "reader_options": {
                                "header": True,
                                "schema": spark_schema,
                            },
                        },
                    )
                },
            )
        },
    )


@pytest.fixture
def datasource_config_with_schema_at_data_connector_level_spark(
    spark_session, spark_schema
) -> DatasourceConfig:
    return DatasourceConfig(
        name="taxi_data",
        class_name="Datasource",
        module_name="great_expectations.datasource",
        execution_engine=ExecutionEngineConfig(
            class_name="SparkDFExecutionEngine",
            module_name="great_expectations.execution_engine.sparkdf_execution_engine",
        ),
        data_connectors={
            "configured_asset_connector": DataConnectorConfig(
                class_name="ConfiguredAssetFilesystemDataConnector",
                module_name="great_expectations.datasource.data_connector.configured_asset_filesystem_data_connector",
                batch_spec_passthrough={
                    "reader_options": {"header": True, "schema": spark_schema},
                },
                assets={
                    "my_asset": AssetConfig(
                        class_name="Asset",
                        module_name="great_expectations.datasource.data_connector.asset",
                    )
                },
            )
        },
    )


@pytest.fixture
def data_connector_config_spark(spark_session) -> DataConnectorConfig:
    return DataConnectorConfig(
        class_name="ConfiguredAssetFilesystemDataConnector",
        module_name="great_expectations.datasource.data_connector.configured_asset_filesystem_data_connector",
        batch_spec_passthrough={
            "reader_options": {"header": True},
        },
    )


@pytest.fixture
def datas_connector_config_with_schema_spark(
    spark_session, spark_schema
) -> DataConnectorConfig:
    return DataConnectorConfig(
        class_name="ConfiguredAssetFilesystemDataConnector",
        module_name="great_expectations.datasource.data_connector.configured_asset_filesystem_data_connector",
        batch_spec_passthrough={
            "reader_options": {"header": True, "schema": spark_schema},
        },
    )


@pytest.fixture
def asset_config_spark(spark_session) -> AssetConfig:
    return AssetConfig(
        class_name="Asset",
        module_name="great_expectations.datasource.data_connector.asset",
        batch_spec_passthrough={
            "reader_options": {"header": True},
        },
    )


@pytest.fixture
def asset_config_with_schema_spark(spark_session, spark_schema) -> AssetConfig:
    return AssetConfig(
        class_name="Asset",
        module_name="great_expectations.datasource.data_connector.asset",
        batch_spec_passthrough={
            "reader_options": {"header": True, "schema": spark_schema},
        },
    )


@pytest.mark.unit
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


@pytest.mark.unit
def test_lossy_serialization_rule_based_profiler_no_warning(caplog):
    caplog.set_level(logging.WARNING, logger="great_expectations.core")

    with mock.patch.multiple(
        inspect.FrameInfo,
        filename="great_expectations/rule_based_profiler/parameter_builder/parameter_builder.py",
        function="get_metrics",
        frame=inspect.FrameInfo.frame,
        lineno=260,
        code_context=None,
        index=None,
    ):
        d = Decimal("12345.678901234567890123456789")
        convert_to_json_serializable(d)
        assert len(caplog.messages) == 0

    caplog.clear()
    d = Decimal("0.1")
    convert_to_json_serializable(d)
    print(caplog.messages)
    assert len(caplog.messages) == 0


@pytest.mark.unit
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


@pytest.mark.unit
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


@pytest.mark.integration
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


@pytest.mark.integration
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
                        id="06871341-f028-4f1f-b8e8-a559ab9f62e1",
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
                        id="06871341-f028-4f1f-b8e8-a559ab9f62e1",
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
@pytest.mark.unit
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


@pytest.mark.parametrize(
    "checkpoint_config,expected_serialized_checkpoint_config",
    [
        pytest.param(
            "checkpoint_config_spark",
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
                            "batch_spec_passthrough": {
                                "reader_options": {"header": True}
                            },
                            "datasource_name": "my_datasource",
                        },
                        "id": "06871341-f028-4f1f-b8e8-a559ab9f62e1",
                    },
                ],
            },
            id="config_no_schema",
        ),
        pytest.param(
            "checkpoint_config_with_schema_spark",
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
                            "batch_spec_passthrough": {
                                "reader_options": {
                                    "schema": {
                                        "fields": [
                                            {
                                                "metadata": {},
                                                "name": "a",
                                                "nullable": True,
                                                "type": "integer",
                                            },
                                            {
                                                "metadata": {},
                                                "name": "b",
                                                "nullable": True,
                                                "type": "integer",
                                            },
                                        ],
                                        "type": "struct",
                                    }
                                }
                            },
                            "datasource_name": "my_datasource",
                        },
                        "id": "06871341-f028-4f1f-b8e8-a559ab9f62e1",
                    },
                ],
            },
            id="config_with_schema",
        ),
    ],
)
@pytest.mark.integration
def test_checkpoint_config_and_nested_objects_are_serialized_spark(
    checkpoint_config: Union[CheckpointConfig, str],
    expected_serialized_checkpoint_config: dict,
    spark_session: "SparkSession",
    request: FixtureRequest,
):
    # when using a fixture value in a parmeterized test, we need to call
    # request.getfixturevalue()
    if isinstance(checkpoint_config, str):
        checkpoint_config = request.getfixturevalue(checkpoint_config)

    observed_dump = checkpointConfigSchema.dump(checkpoint_config)
    assert observed_dump == expected_serialized_checkpoint_config
    loaded_data = checkpointConfigSchema.load(observed_dump)
    observed_load = CheckpointConfig(**loaded_data)
    assert checkpointConfigSchema.dump(observed_load) == checkpointConfigSchema.dump(
        checkpoint_config
    )


@pytest.mark.parametrize(
    "datasource_config,expected_serialized_datasource_config",
    [
        pytest.param(
            "datasource_config_spark",
            {
                "class_name": "Datasource",
                "data_connectors": {
                    "configured_asset_connector": {
                        "assets": {
                            "my_asset": {
                                "batch_spec_passthrough": {
                                    "reader_options": {
                                        "header": True,
                                    }
                                },
                                "class_name": "Asset",
                                "module_name": "great_expectations.datasource.data_connector.asset",
                            }
                        },
                        "class_name": "ConfiguredAssetFilesystemDataConnector",
                        "module_name": "great_expectations.datasource.data_connector.configured_asset_filesystem_data_connector",
                    }
                },
                "execution_engine": {
                    "class_name": "SparkDFExecutionEngine",
                    "module_name": "great_expectations.execution_engine.sparkdf_execution_engine",
                },
                "module_name": "great_expectations.datasource",
                "name": "taxi_data",
            },
            id="datasource_with_header_no_schema",
        ),
        pytest.param(
            "datasource_config_with_schema_at_asset_level_spark",
            {
                "class_name": "Datasource",
                "data_connectors": {
                    "configured_asset_connector": {
                        "assets": {
                            "my_asset": {
                                "batch_spec_passthrough": {
                                    "reader_options": {
                                        "header": True,
                                        "schema": {
                                            "fields": [
                                                {
                                                    "metadata": {},
                                                    "name": "a",
                                                    "nullable": True,
                                                    "type": "integer",
                                                },
                                                {
                                                    "metadata": {},
                                                    "name": "b",
                                                    "nullable": True,
                                                    "type": "integer",
                                                },
                                            ],
                                            "type": "struct",
                                        },
                                    }
                                },
                                "class_name": "Asset",
                                "module_name": "great_expectations.datasource.data_connector.asset",
                            }
                        },
                        "class_name": "ConfiguredAssetFilesystemDataConnector",
                        "module_name": "great_expectations.datasource.data_connector.configured_asset_filesystem_data_connector",
                    }
                },
                "execution_engine": {
                    "class_name": "SparkDFExecutionEngine",
                    "module_name": "great_expectations.execution_engine.sparkdf_execution_engine",
                },
                "module_name": "great_expectations.datasource",
                "name": "taxi_data",
            },
            id="datasource_with_header_schema_at_asset_level",
        ),
        pytest.param(
            "datasource_config_with_schema_at_data_connector_level_spark",
            {
                "class_name": "Datasource",
                "data_connectors": {
                    "configured_asset_connector": {
                        "assets": {
                            "my_asset": {
                                "class_name": "Asset",
                                "module_name": "great_expectations.datasource.data_connector.asset",
                            }
                        },
                        "batch_spec_passthrough": {
                            "reader_options": {
                                "header": True,
                                "schema": {
                                    "fields": [
                                        {
                                            "metadata": {},
                                            "name": "a",
                                            "nullable": True,
                                            "type": "integer",
                                        },
                                        {
                                            "metadata": {},
                                            "name": "b",
                                            "nullable": True,
                                            "type": "integer",
                                        },
                                    ],
                                    "type": "struct",
                                },
                            }
                        },
                        "class_name": "ConfiguredAssetFilesystemDataConnector",
                        "module_name": "great_expectations.datasource.data_connector.configured_asset_filesystem_data_connector",
                    }
                },
                "execution_engine": {
                    "class_name": "SparkDFExecutionEngine",
                    "module_name": "great_expectations.execution_engine.sparkdf_execution_engine",
                },
                "module_name": "great_expectations.datasource",
                "name": "taxi_data",
            },
            id="datasource_with_header_schema_at_data_connector_level",
        ),
    ],
)
@pytest.mark.integration
def test_datasource_config_and_nested_objects_are_serialized_spark(
    datasource_config: Union[DatasourceConfig, str],
    expected_serialized_datasource_config: dict,
    spark_session: "SparkSession",
    request: FixtureRequest,
):
    # when using a fixture value in a parmeterized test, we need to call
    # request.getfixturevalue()
    if isinstance(datasource_config, str):
        datasource_config = request.getfixturevalue(datasource_config)

    observed_dump = datasourceConfigSchema.dump(obj=datasource_config)
    assert observed_dump == expected_serialized_datasource_config
    loaded_data = datasourceConfigSchema.load(observed_dump)
    observed_load = DatasourceConfig(**loaded_data)
    assert checkpointConfigSchema.dump(observed_load) == checkpointConfigSchema.dump(
        datasource_config
    )


@pytest.mark.parametrize(
    "data_connector_config,expected_serialized_data_connector_config",
    [
        pytest.param(
            "data_connector_config_spark",
            {
                "batch_spec_passthrough": {
                    "reader_options": {
                        "header": True,
                    }
                },
                "class_name": "ConfiguredAssetFilesystemDataConnector",
                "module_name": "great_expectations.datasource.data_connector.configured_asset_filesystem_data_connector",
            },
            id="data_connector_with_schema",
        ),
        pytest.param(
            "datas_connector_config_with_schema_spark",
            {
                "batch_spec_passthrough": {
                    "reader_options": {
                        "header": True,
                        "schema": {
                            "fields": [
                                {
                                    "metadata": {},
                                    "name": "a",
                                    "nullable": True,
                                    "type": "integer",
                                },
                                {
                                    "metadata": {},
                                    "name": "b",
                                    "nullable": True,
                                    "type": "integer",
                                },
                            ],
                            "type": "struct",
                        },
                    }
                },
                "class_name": "ConfiguredAssetFilesystemDataConnector",
                "module_name": "great_expectations.datasource.data_connector.configured_asset_filesystem_data_connector",
            },
            id="data_connector_without_schema",
        ),
    ],
)
@pytest.mark.integration
def test_data_connector_and_nested_objects_are_serialized_spark(
    data_connector_config: DataConnectorConfig,
    expected_serialized_data_connector_config: dict,
    spark_session: "SparkSession",
    request: FixtureRequest,
):
    # when using a fixture value in a parmeterized test, we need to call
    # request.getfixturevalue()
    if isinstance(data_connector_config, str):
        data_connector_config = request.getfixturevalue(data_connector_config)

    observed_dump = dataConnectorConfigSchema.dump(obj=data_connector_config)
    assert observed_dump == expected_serialized_data_connector_config
    observed_load = dataConnectorConfigSchema.load(observed_dump)
    assert dataConnectorConfigSchema.dump(
        observed_load
    ) == dataConnectorConfigSchema.dump(data_connector_config)


@pytest.mark.parametrize(
    "asset_config,expected_serialized_asset_config",
    [
        pytest.param(
            "asset_config_with_schema_spark",
            {
                "batch_spec_passthrough": {
                    "reader_options": {
                        "header": True,
                        "schema": {
                            "fields": [
                                {
                                    "metadata": {},
                                    "name": "a",
                                    "nullable": True,
                                    "type": "integer",
                                },
                                {
                                    "metadata": {},
                                    "name": "b",
                                    "nullable": True,
                                    "type": "integer",
                                },
                            ],
                            "type": "struct",
                        },
                    }
                },
                "class_name": "Asset",
                "module_name": "great_expectations.datasource.data_connector.asset",
            },
            id="asset_serialized_with_schema",
        ),
        pytest.param(
            "asset_config_spark",
            {
                "batch_spec_passthrough": {
                    "reader_options": {"header": True},
                },
                "class_name": "Asset",
                "module_name": "great_expectations.datasource.data_connector.asset",
            },
            id="asset_serialized_without_schema",
        ),
    ],
)
@pytest.mark.integration
def test_asset_and_nested_objects_are_serialized_spark(
    asset_config: AssetConfig,
    expected_serialized_asset_config: dict,
    spark_session: "SparkSession",
    request: FixtureRequest,
):
    # when using a fixture value in a parmeterized test, we need to call
    # request.getfixturevalue()
    if isinstance(asset_config, str):
        asset_config = request.getfixturevalue(asset_config)

    observed_dump = assetConfigSchema.dump(obj=asset_config)
    assert observed_dump == expected_serialized_asset_config
    observed_load = assetConfigSchema.load(observed_dump)
    assert assetConfigSchema.dump(observed_load) == assetConfigSchema.dump(asset_config)


def generic_config_serialization_assertions(
    config: AbstractConfig,
    schema: Schema,
    expected_serialized_config: dict,
    expected_roundtrip_config: dict,
) -> None:
    """Generic assertions for testing configuration serialization.

    Args:
        config: config object to check serialization.
        schema: Marshmallow schema used for serializing / deserializing config object.
        expected_serialized_config: expected when serializing a config via dump
        expected_roundtrip_config: expected config after loading the dump

    Returns:
        None

    Raises:
        AssertionError
    """

    observed_dump = schema.dump(config)
    assert observed_dump == expected_serialized_config

    loaded_data = schema.load(observed_dump)
    assert loaded_data.to_json_dict() == expected_roundtrip_config
