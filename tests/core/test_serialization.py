import copy
import logging
from decimal import Decimal
from typing import Any, List, Optional, Tuple

import pandas as pd

from great_expectations import DataContext
from great_expectations.checkpoint import Checkpoint
from great_expectations.core.batch import (
    BatchRequest,
    RuntimeBatchRequest,
    delete_runtime_parameters_batch_data_references_from_config,
    get_runtime_parameters_batch_data_references_from_config,
    restore_runtime_parameters_batch_data_references_into_config,
)
from great_expectations.core.util import (
    convert_to_json_serializable,
    requires_lossy_conversion,
)
from great_expectations.data_context.types.base import CheckpointConfig
from great_expectations.util import deep_filter_properties_iterable


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
    import pandas as pd

    df = pd.DataFrame({"a": [1, 2, 3]})
    sdf = spark_session.createDataFrame(df)
    assert convert_to_json_serializable(sdf) == {"a": [1, 2, 3]}

    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    sdf = spark_session.createDataFrame(df)
    assert convert_to_json_serializable(sdf) == {"a": [1, 2, 3], "b": [4, 5, 6]}


def test_batch_request_deepcopy():
    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})
    batch_request: BatchRequest = RuntimeBatchRequest(
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

    batch_request_copy: BatchRequest = copy.deepcopy(batch_request)
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

    batch_request: BatchRequest = RuntimeBatchRequest(
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

    nested_checkpoint_config: CheckpointConfig = CheckpointConfig(
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
        data_context=context, **nested_checkpoint_config.to_json_dict()
    )
    substituted_config_template_and_runtime_kwargs: CheckpointConfig = nested_checkpoint.get_substituted_config(
        runtime_kwargs={
            "batch_request": batch_request,
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

    checkpoint_config_copy: CheckpointConfig = copy.deepcopy(
        substituted_config_template_and_runtime_kwargs
    )
    assert deep_filter_properties_iterable(
        properties=checkpoint_config_copy.to_json_dict(),
        clean_falsy=True,
    ) == deep_filter_properties_iterable(
        properties=substituted_config_template_and_runtime_kwargs.to_json_dict(),
        clean_falsy=True,
    )


def test_get_runtime_parameters_batch_data_references_from_config(
    titanic_pandas_data_context_with_v013_datasource_stats_enabled_with_checkpoints_v1_with_templates,
    monkeypatch,
):
    monkeypatch.setenv("GE_ENVIRONMENT", "my_ge_environment")
    monkeypatch.setenv("VAR", "test")
    monkeypatch.setenv("MY_PARAM", "1")
    monkeypatch.setenv("OLD_PARAM", "2")

    context: DataContext = titanic_pandas_data_context_with_v013_datasource_stats_enabled_with_checkpoints_v1_with_templates

    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    batch_request: BatchRequest = RuntimeBatchRequest(
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

    nested_checkpoint_config: CheckpointConfig = CheckpointConfig(
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
        data_context=context, **nested_checkpoint_config.to_json_dict()
    )
    substituted_config_template_and_runtime_kwargs: CheckpointConfig = nested_checkpoint.get_substituted_config(
        runtime_kwargs={
            "batch_request": batch_request,
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

    expected_batch_data_references: Tuple[Optional[Any], Optional[List[Any]]] = (
        test_df,
        [None, None, None, None, None],
    )

    batch_data_references: Tuple[
        Optional[Any], Optional[List[Any]]
    ] = get_runtime_parameters_batch_data_references_from_config(
        config=substituted_config_template_and_runtime_kwargs
    )
    assert batch_data_references == expected_batch_data_references


def test_delete_runtime_parameters_batch_data_references_from_config(
    titanic_pandas_data_context_with_v013_datasource_stats_enabled_with_checkpoints_v1_with_templates,
    monkeypatch,
):
    monkeypatch.setenv("GE_ENVIRONMENT", "my_ge_environment")
    monkeypatch.setenv("VAR", "test")
    monkeypatch.setenv("MY_PARAM", "1")
    monkeypatch.setenv("OLD_PARAM", "2")

    context: DataContext = titanic_pandas_data_context_with_v013_datasource_stats_enabled_with_checkpoints_v1_with_templates

    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    batch_request: BatchRequest = RuntimeBatchRequest(
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

    nested_checkpoint_config: CheckpointConfig = CheckpointConfig(
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
        data_context=context, **nested_checkpoint_config.to_json_dict()
    )
    substituted_config_template_and_runtime_kwargs: CheckpointConfig = nested_checkpoint.get_substituted_config(
        runtime_kwargs={
            "batch_request": batch_request,
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

    expected_nested_checkpoint_config_template_and_runtime_template_name: CheckpointConfig = CheckpointConfig(
        name="my_nested_checkpoint",
        config_version=1,
        template_name="my_nested_checkpoint_template_3",
        run_name_template="runtime_run_template",
        batch_request=batch_request,
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
    expected_nested_checkpoint_config_template_and_runtime_template_name.batch_request[
        "runtime_parameters"
    ].pop("batch_data")

    delete_runtime_parameters_batch_data_references_from_config(
        config=substituted_config_template_and_runtime_kwargs
    )
    assert deep_filter_properties_iterable(
        properties=substituted_config_template_and_runtime_kwargs.to_json_dict(),
        clean_falsy=True,
    ) == deep_filter_properties_iterable(
        properties=expected_nested_checkpoint_config_template_and_runtime_template_name.to_json_dict(),
        clean_falsy=True,
    )
    assert (
        substituted_config_template_and_runtime_kwargs.batch_request[
            "runtime_parameters"
        ].get("batch_data")
        is None
    )


def test_restore_runtime_parameters_batch_data_references_into_config(
    titanic_pandas_data_context_with_v013_datasource_stats_enabled_with_checkpoints_v1_with_templates,
    monkeypatch,
):
    monkeypatch.setenv("GE_ENVIRONMENT", "my_ge_environment")
    monkeypatch.setenv("VAR", "test")
    monkeypatch.setenv("MY_PARAM", "1")
    monkeypatch.setenv("OLD_PARAM", "2")

    context: DataContext = titanic_pandas_data_context_with_v013_datasource_stats_enabled_with_checkpoints_v1_with_templates

    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    batch_request: BatchRequest = RuntimeBatchRequest(
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

    nested_checkpoint_config: CheckpointConfig = CheckpointConfig(
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
        data_context=context, **nested_checkpoint_config.to_json_dict()
    )
    substituted_config_template_and_runtime_kwargs: CheckpointConfig = nested_checkpoint.get_substituted_config(
        runtime_kwargs={
            "batch_request": batch_request,
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

    expected_nested_checkpoint_config_template_and_runtime_template_name: CheckpointConfig = CheckpointConfig(
        name="my_nested_checkpoint",
        config_version=1,
        template_name="my_nested_checkpoint_template_3",
        run_name_template="runtime_run_template",
        batch_request=batch_request,
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

    batch_data_references: Tuple[
        Optional[Any], Optional[List[Any]]
    ] = get_runtime_parameters_batch_data_references_from_config(
        config=substituted_config_template_and_runtime_kwargs
    )
    delete_runtime_parameters_batch_data_references_from_config(
        config=substituted_config_template_and_runtime_kwargs
    )
    restore_runtime_parameters_batch_data_references_into_config(
        config=substituted_config_template_and_runtime_kwargs,
        batch_data_references=batch_data_references,
    )
    assert deep_filter_properties_iterable(
        properties=substituted_config_template_and_runtime_kwargs.to_json_dict(),
        clean_falsy=True,
    ) == deep_filter_properties_iterable(
        properties=expected_nested_checkpoint_config_template_and_runtime_template_name.to_json_dict(),
        clean_falsy=True,
    )
    assert (
        type(
            substituted_config_template_and_runtime_kwargs.batch_request[
                "runtime_parameters"
            ]["batch_data"]
        )
        != str
    )

    restore_runtime_parameters_batch_data_references_into_config(
        config=substituted_config_template_and_runtime_kwargs,
        batch_data_references=batch_data_references,
        replace_value_with_type_string=True,
    )
    assert (
        type(
            substituted_config_template_and_runtime_kwargs.batch_request[
                "runtime_parameters"
            ]["batch_data"]
        )
        == str
    )
