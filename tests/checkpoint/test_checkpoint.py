from __future__ import annotations

import logging
import os
import pathlib
import pickle
import shutil
from typing import TYPE_CHECKING, Dict, Optional, Union, cast

import pandas as pd
import pytest

import great_expectations as gx
import great_expectations.exceptions as gx_exceptions
from great_expectations import set_context
from great_expectations.checkpoint import Checkpoint
from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
from great_expectations.checkpoint.util import get_substituted_batch_request
from great_expectations.core import ExpectationSuite, ExpectationSuiteValidationResult
from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest
from great_expectations.core.config_peer import ConfigOutputModes
from great_expectations.core.expectation_validation_result import (
    ExpectationValidationResult,
)
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context import AbstractDataContext, FileDataContext
from great_expectations.data_context.types.base import (
    CheckpointConfig,
    checkpointConfigSchema,
)
from great_expectations.data_context.types.resource_identifiers import (
    ConfigurationIdentifier,
    ValidationResultIdentifier,
)
from great_expectations.execution_engine import SparkDFExecutionEngine
from great_expectations.render import RenderedAtomicContent
from great_expectations.util import deep_filter_properties_iterable
from great_expectations.validator.validator import Validator
from tests.checkpoint import cloud_config

if TYPE_CHECKING:
    from pytest_mock import MockerFixture

    from great_expectations.core.data_context_key import DataContextKey
    from great_expectations.data_context.data_context.ephemeral_data_context import (
        EphemeralDataContext,
    )


yaml = YAMLHandler()

logger = logging.getLogger(__name__)


@pytest.fixture
def dummy_data_context() -> AbstractDataContext:
    class DummyDataContext:
        def __init__(self) -> None:
            pass

    return cast(AbstractDataContext, DummyDataContext())


@pytest.fixture
def dummy_validator() -> Validator:
    class DummyValidator:
        @property
        def expectation_suite_name(self) -> str:
            return "my_expectation_suite"

    return cast(Validator, DummyValidator())


@pytest.fixture
def batch_request_as_dict() -> Dict[str, str]:
    return {
        "datasource_name": "my_datasource",
        "data_connector_name": "my_basic_data_connector",
        "data_asset_name": "Titanic_1911",
    }


@pytest.mark.filesystem
def test_basic_checkpoint_config_validation(
    empty_data_context_stats_enabled,
    common_action_list,
    caplog,
    capsys,
):
    context: FileDataContext = empty_data_context_stats_enabled
    yaml_config_erroneous: str
    config_erroneous: dict
    checkpoint_config: Union[CheckpointConfig, dict]
    checkpoint: Checkpoint

    yaml_config_erroneous = """
    name: misconfigured_checkpoint
    unexpected_property: UNKNOWN_PROPERTY_VALUE
    """
    config_erroneous = yaml.load(yaml_config_erroneous)
    with pytest.raises(TypeError):
        # noinspection PyUnusedLocal
        checkpoint_config = CheckpointConfig(**config_erroneous)

    yaml_config_erroneous = """
    name: my_erroneous_checkpoint
    """

    assert len(context.list_checkpoints()) == 0
    erroneous_checkpoint = context.add_checkpoint(**yaml.load(yaml_config_erroneous))
    assert len(context.list_checkpoints()) == 1

    yaml_config: str = """
    name: my_checkpoint
    validations: []
    action_list:
      - name: store_validation_result
        action:
          class_name: StoreValidationResultAction
      - name: update_data_docs
        action:
          class_name: UpdateDataDocsAction
    """

    expected_checkpoint_config: dict = {
        "name": "my_checkpoint",
        "action_list": common_action_list,
    }

    config: dict = yaml.load(yaml_config)
    checkpoint_config = CheckpointConfig(**config)
    checkpoint: Checkpoint = Checkpoint(
        data_context=context,
        **deep_filter_properties_iterable(
            properties=checkpoint_config.to_json_dict(),
            delete_fields={"class_name", "module_name"},
            clean_falsy=True,
        ),
    )
    filtered_expected_checkpoint_config: dict = deep_filter_properties_iterable(
        properties=expected_checkpoint_config,
        delete_fields={"class_name", "module_name"},
        clean_falsy=True,
    )
    assert (
        deep_filter_properties_iterable(
            properties=checkpoint.get_config(mode=ConfigOutputModes.DICT),
            delete_fields={"class_name", "module_name"},
            clean_falsy=True,
        )
        == filtered_expected_checkpoint_config
    )

    checkpoint: Checkpoint = context.test_yaml_config(
        yaml_config=yaml_config,
        name="my_checkpoint",
        class_name="Checkpoint",
    )
    assert (
        deep_filter_properties_iterable(
            properties=checkpoint.get_config(mode=ConfigOutputModes.DICT),
            delete_fields={"class_name", "module_name"},
            clean_falsy=True,
        )
        == filtered_expected_checkpoint_config
    )

    assert len(context.list_checkpoints()) == 1
    context.add_checkpoint(**yaml.load(yaml_config))
    assert len(context.list_checkpoints()) == 2

    context.suites.add(ExpectationSuite(name="my_expectation_suite"))
    checkpoint = context.get_legacy_checkpoint("my_checkpoint")
    with pytest.raises(
        gx_exceptions.DataContextError,
        match=r'Checkpoint "my_checkpoint" must be called with a validator or contain either a batch_request or validations.',  # noqa: E501
    ):
        checkpoint.run()

    context.delete_legacy_checkpoint(erroneous_checkpoint.name)
    context.delete_legacy_checkpoint(checkpoint.name)
    assert len(context.list_checkpoints()) == 0


@pytest.mark.filesystem
@pytest.mark.slow  # 1.25s
def test_newstyle_checkpoint_instantiates_and_produces_a_validation_result_when_run(
    titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled,
    common_action_list,
):
    context: FileDataContext = titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled  # noqa: E501
    # add checkpoint config
    checkpoint_config = CheckpointConfig(
        name="my_checkpoint",
        expectation_suite_name="my_expectation_suite",
        action_list=common_action_list,
        validations=[
            {
                "batch_request": {
                    "datasource_name": "my_datasource",
                    "data_connector_name": "my_basic_data_connector",
                    "data_asset_name": "Titanic_1911",
                }
            }
        ],
    )
    checkpoint_config_key = ConfigurationIdentifier(configuration_key=checkpoint_config.name)
    context.checkpoint_store.set(key=checkpoint_config_key, value=checkpoint_config)
    checkpoint: Checkpoint = context.get_legacy_checkpoint(checkpoint_config.name)

    with pytest.raises(gx_exceptions.DataContextError, match=r"expectation_suite .* not found"):
        checkpoint.run()

    assert len(context.validations_store.list_keys()) == 0

    context.suites.add(ExpectationSuite("my_expectation_suite"))
    result = checkpoint.run()

    assert len(context.validations_store.list_keys()) == 1
    assert result["success"]


@pytest.mark.filesystem
def test_newstyle_checkpoint_instantiates_and_produces_a_validation_result_with_checkpoint_name_in_meta_when_run(  # noqa: E501
    titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled,
    store_validation_result_action,
):
    context: FileDataContext = titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled  # noqa: E501
    checkpoint_name: str = "test_checkpoint_name"
    # add checkpoint config
    checkpoint_config = CheckpointConfig(
        name=checkpoint_name,
        expectation_suite_name="my_expectation_suite",
        action_list=[
            store_validation_result_action,
        ],
        validations=[
            {
                "batch_request": {
                    "datasource_name": "my_datasource",
                    "data_connector_name": "my_basic_data_connector",
                    "data_asset_name": "Titanic_1911",
                }
            }
        ],
    )
    checkpoint_config_key = ConfigurationIdentifier(configuration_key=checkpoint_config.name)
    context.checkpoint_store.set(key=checkpoint_config_key, value=checkpoint_config)
    checkpoint: Checkpoint = context.get_legacy_checkpoint(checkpoint_config.name)

    assert len(context.validations_store.list_keys()) == 0

    context.suites.add(ExpectationSuite("my_expectation_suite"))
    result: CheckpointResult = checkpoint.run()

    assert len(context.validations_store.list_keys()) == 1
    assert result["success"]

    validation_result_identifier: DataContextKey = context.validations_store.list_keys()[0]
    validation_result: ExpectationSuiteValidationResult = context.validations_store.get(
        validation_result_identifier
    )

    assert "checkpoint_name" in validation_result.meta
    assert validation_result.meta["checkpoint_name"] == checkpoint_name


@pytest.mark.unit
def test_newstyle_checkpoint_raises_error_if_batch_request_and_validator_are_specified_in_constructor(  # noqa: E501
    dummy_data_context,
    common_action_list,
    dummy_validator,
    batch_request_as_dict,
):
    context = dummy_data_context
    validator = dummy_validator

    batch_request: BatchRequest = BatchRequest(**batch_request_as_dict)
    with pytest.raises(
        gx_exceptions.CheckpointError,
        match=r'Checkpoint "my_checkpoint" cannot be called with a validator and contain a batch_request and/or a batch_request in validations.',  # noqa: E501
    ):
        _ = Checkpoint(
            name="my_checkpoint",
            data_context=context,
            expectation_suite_name="my_expectation_suite",
            batch_request=batch_request,
            validator=validator,
            action_list=common_action_list,
        )


@pytest.mark.unit
def test_newstyle_checkpoint_raises_error_if_batch_request_in_validations_and_validator_are_specified_in_constructor(  # noqa: E501
    dummy_data_context,
    common_action_list,
    dummy_validator,
    batch_request_as_dict,
):
    context = dummy_data_context
    validator = dummy_validator

    batch_request: BatchRequest = BatchRequest(**batch_request_as_dict)
    with pytest.raises(
        gx_exceptions.CheckpointError,
        match=r'Checkpoint "my_checkpoint" cannot be called with a validator and contain a batch_request and/or a batch_request in validations.',  # noqa: E501
    ):
        _ = Checkpoint(
            name="my_checkpoint",
            data_context=context,
            expectation_suite_name="my_expectation_suite",
            validator=validator,
            action_list=common_action_list,
            validations=[{"batch_request": batch_request}],
        )


@pytest.mark.unit
def test_newstyle_checkpoint_raises_error_if_expectation_suite_name_and_validator_are_specified_in_constructor(  # noqa: E501
    dummy_data_context,
    common_action_list,
    dummy_validator,
    batch_request_as_dict,
):
    context = dummy_data_context
    validator = dummy_validator

    with pytest.raises(
        gx_exceptions.CheckpointError,
        match=r'Checkpoint "my_checkpoint" cannot be called with a validator and contain an expectation_suite_name and/or an expectation_suite_name in validations.',  # noqa: E501
    ):
        _ = Checkpoint(
            name="my_checkpoint",
            data_context=context,
            expectation_suite_name="my_expectation_suite",
            validator=validator,
            action_list=common_action_list,
        )


@pytest.mark.unit
def test_newstyle_checkpoint_raises_error_if_expectation_suite_name_in_validations_and_validator_are_specified_in_constructor(  # noqa: E501
    dummy_data_context,
    common_action_list,
    dummy_validator,
):
    context = dummy_data_context
    validator = dummy_validator

    with pytest.raises(
        gx_exceptions.CheckpointError,
        match=r'Checkpoint "my_checkpoint" cannot be called with a validator and contain an expectation_suite_name and/or an expectation_suite_name in validations.',  # noqa: E501
    ):
        _ = Checkpoint(
            name="my_checkpoint",
            data_context=context,
            validator=validator,
            action_list=common_action_list,
            validations=[{"expectation_suite_name": "my_expectation_suite"}],
        )


@pytest.mark.filesystem
@pytest.mark.slow  # 1.15s
def test_newstyle_checkpoint_instantiates_and_produces_a_validation_result_when_run_with_validator_specified_in_constructor(  # noqa: E501
    titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled,
    common_action_list,
    batch_request_as_dict,
):
    context: FileDataContext = titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled  # noqa: E501
    batch_request: BatchRequest = BatchRequest(**batch_request_as_dict)
    context.suites.add(ExpectationSuite("my_expectation_suite"))

    validator: Validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name="my_expectation_suite",
    )
    checkpoint: Checkpoint = Checkpoint(
        name="my_checkpoint",
        data_context=context,
        validator=validator,
        action_list=common_action_list,
    )

    result = checkpoint.run()

    assert len(context.validations_store.list_keys()) == 1
    assert result["success"]


@pytest.mark.unit
def test_newstyle_checkpoint_raises_error_if_validator_specified_in_constructor_and_validator_is_specified_in_run(  # noqa: E501
    dummy_data_context,
    common_action_list,
    dummy_validator,
):
    context = dummy_data_context
    validator = dummy_validator

    with pytest.raises(gx_exceptions.CheckpointError) as e:
        _ = Checkpoint(
            name="my_checkpoint",
            data_context=context,
            validator=validator,
            action_list=common_action_list,
        ).run(
            validator=validator,
        )

    assert (
        str(e.value)
        == 'Checkpoint "my_checkpoint" has already been created with a validator and overriding it through run() is not allowed.'  # noqa: E501
    )


@pytest.mark.unit
def test_newstyle_checkpoint_raises_error_if_validator_specified_in_constructor_and_validator_is_specified_in_run(  # noqa: E501, F811
    dummy_data_context,
    common_action_list,
    dummy_validator,
):
    context = dummy_data_context
    validator = dummy_validator

    with pytest.raises(gx_exceptions.CheckpointError) as e:
        _ = Checkpoint(
            name="my_checkpoint",
            data_context=context,
            validator=validator,
            action_list=common_action_list,
        ).run(
            validator=validator,
        )

    assert (
        str(e.value)
        == 'Checkpoint "my_checkpoint" has already been created with a validator and overriding it through run() is not allowed.'  # noqa: E501
    )


@pytest.mark.unit
def test_newstyle_checkpoint_raises_error_if_batch_request_is_specified_in_validations_and_batch_request_and_validator_are_specified_in_run(  # noqa: E501
    dummy_data_context,
    common_action_list,
    dummy_validator,
    batch_request_as_dict,
):
    context = dummy_data_context
    validator = dummy_validator

    batch_request: BatchRequest = BatchRequest(**batch_request_as_dict)
    with pytest.raises(gx_exceptions.CheckpointError) as e:
        _ = Checkpoint(
            name="my_checkpoint",
            data_context=context,
            expectation_suite_name="my_expectation_suite",
            action_list=common_action_list,
            validations=[{"batch_request": batch_request}],
        ).run(
            batch_request=batch_request_as_dict,
            validator=validator,
        )

    assert (
        str(e.value)
        == 'Checkpoint "my_checkpoint" has already been created with a validator and overriding it by supplying a batch_request and/or validations with a batch_request to run() is not allowed.'  # noqa: E501
    )


@pytest.mark.unit
def test_newstyle_checkpoint_raises_error_if_validator_specified_in_constructor_and_expectation_suite_name_is_specified_in_run(  # noqa: E501
    dummy_data_context,
    common_action_list,
    dummy_validator,
):
    context = dummy_data_context
    validator = dummy_validator

    with pytest.raises(gx_exceptions.CheckpointError) as e:
        _ = Checkpoint(
            name="my_checkpoint",
            data_context=context,
            validator=validator,
            action_list=common_action_list,
        ).run(
            expectation_suite_name="my_expectation_suite",
        )

    assert (
        str(e.value)
        == 'Checkpoint "my_checkpoint" has already been created with a validator and overriding its expectation_suite_name by supplying an expectation_suite_name and/or validations with an expectation_suite_name to run() is not allowed.'  # noqa: E501
    )


@pytest.mark.unit
def test_newstyle_checkpoint_raises_error_if_expectation_suite_name_is_specified_in_validations_and_validator_is_specified_in_run(  # noqa: E501
    dummy_data_context,
    common_action_list,
    dummy_validator,
    batch_request_as_dict,
):
    context = dummy_data_context
    validator = dummy_validator

    batch_request: BatchRequest = BatchRequest(**batch_request_as_dict)
    with pytest.raises(gx_exceptions.CheckpointError) as e:
        _ = Checkpoint(
            name="my_checkpoint",
            data_context=context,
            expectation_suite_name="my_expectation_suite",
            action_list=common_action_list,
            validations=[{"batch_request": batch_request}],
        ).run(
            batch_request=batch_request_as_dict,
            validator=validator,
        )

    assert (
        str(e.value)
        == 'Checkpoint "my_checkpoint" has already been created with a validator and overriding it by supplying a batch_request and/or validations with a batch_request to run() is not allowed.'  # noqa: E501
    )


@pytest.mark.filesystem
@pytest.mark.slow  # 1.15s
def test_newstyle_checkpoint_instantiates_and_produces_a_validation_result_when_run_with_validator_specified_in_run(  # noqa: E501
    titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled,
    common_action_list,
    batch_request_as_dict,
):
    context: FileDataContext = titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled  # noqa: E501
    batch_request: BatchRequest = BatchRequest(**batch_request_as_dict)
    context.suites.add(ExpectationSuite("my_expectation_suite"))
    validator: Validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name="my_expectation_suite",
    )
    checkpoint: Checkpoint = Checkpoint(
        name="my_checkpoint",
        data_context=context,
        expectation_suite_name="my_expectation_suite",
        action_list=common_action_list,
    )

    result = checkpoint.run(
        validator=validator,
    )

    assert len(context.validations_store.list_keys()) == 1
    assert result["success"]


@pytest.mark.filesystem
@pytest.mark.slow  # 1.15s
def test_newstyle_checkpoint_instantiates_and_produces_a_validation_result_when_run_batch_request_object(  # noqa: E501
    titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled,
    common_action_list,
    batch_request_as_dict,
):
    context: FileDataContext = titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled  # noqa: E501
    # add checkpoint config
    batch_request: dict = {
        "datasource_name": "my_datasource",
        "data_connector_name": "my_basic_data_connector",
        "data_asset_name": "Titanic_1911",
    }
    checkpoint: Checkpoint = Checkpoint(
        name="my_checkpoint",
        data_context=context,
        expectation_suite_name="my_expectation_suite",
        action_list=common_action_list,
        validations=[{"batch_request": batch_request}],
    )
    with pytest.raises(gx_exceptions.DataContextError, match=r"expectation_suite .* not found"):
        checkpoint.run()

    assert len(context.validations_store.list_keys()) == 0

    context.suites.add(ExpectationSuite("my_expectation_suite"))
    result = checkpoint.run()

    assert len(context.validations_store.list_keys()) == 1
    assert result["success"]


@pytest.mark.filesystem
def test_newstyle_checkpoint_instantiates_and_produces_a_validation_result_when_run_runtime_batch_request_object_pandasdf(  # noqa: E501
    data_context_with_datasource_pandas_engine,
    common_action_list,
):
    context: FileDataContext = data_context_with_datasource_pandas_engine
    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    # RuntimeBatchRequest with a DataFrame
    runtime_batch_request: RuntimeBatchRequest = RuntimeBatchRequest(
        **{
            "datasource_name": "my_datasource",
            "data_connector_name": "default_runtime_data_connector_name",
            "data_asset_name": "test_df",
            "batch_identifiers": {"default_identifier_name": "test_identifier"},
            "runtime_parameters": {"batch_data": test_df},
        }
    )
    checkpoint: Checkpoint = Checkpoint(
        name="my_checkpoint",
        data_context=context,
        expectation_suite_name="my_expectation_suite",
        action_list=common_action_list,
    )
    with pytest.raises(gx_exceptions.DataContextError, match=r"expectation_suite .* not found"):
        checkpoint.run(validations=[{"batch_request": runtime_batch_request}])

    assert len(context.validations_store.list_keys()) == 0

    context.suites.add(ExpectationSuite("my_expectation_suite"))
    result = checkpoint.run(validations=[{"batch_request": runtime_batch_request}])

    assert len(context.validations_store.list_keys()) == 1
    assert result["success"]


@pytest.mark.spark
def test_newstyle_checkpoint_instantiates_and_produces_a_validation_result_when_run_runtime_batch_request_object_sparkdf(  # noqa: E501
    data_context_with_datasource_spark_engine,
    common_action_list,
    spark_session,
):
    context: FileDataContext = data_context_with_datasource_spark_engine
    pandas_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})
    test_df = spark_session.createDataFrame(pandas_df)

    # RuntimeBatchRequest with a DataFrame
    runtime_batch_request: RuntimeBatchRequest = RuntimeBatchRequest(
        **{
            "datasource_name": "my_datasource",
            "data_connector_name": "default_runtime_data_connector_name",
            "data_asset_name": "test_df",
            "batch_identifiers": {"default_identifier_name": "test_identifier"},
            "runtime_parameters": {"batch_data": test_df},
        }
    )
    checkpoint: Checkpoint = Checkpoint(
        name="my_checkpoint",
        data_context=context,
        expectation_suite_name="my_expectation_suite",
        action_list=common_action_list,
    )
    with pytest.raises(gx_exceptions.DataContextError, match=r"expectation_suite .* not found"):
        # noinspection PyUnusedLocal
        result = checkpoint.run(validations=[{"batch_request": runtime_batch_request}])

    assert len(context.validations_store.list_keys()) == 0

    context.suites.add(ExpectationSuite("my_expectation_suite"))
    result = checkpoint.run(validations=[{"batch_request": runtime_batch_request}])

    assert len(context.validations_store.list_keys()) == 1
    assert result["success"]


@pytest.mark.filesystem
@pytest.mark.slow  # 1.31s
def test_newstyle_checkpoint_instantiates_and_produces_a_validation_result_when_run_batch_request_object_multi_validation_pandasdf(  # noqa: E501
    titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled,
    common_action_list,
):
    context: FileDataContext = titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled  # noqa: E501
    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    batch_request: dict = {
        "datasource_name": "my_datasource",
        "data_connector_name": "my_basic_data_connector",
        "data_asset_name": "Titanic_1911",
    }

    # RuntimeBatchRequest with a DataFrame
    runtime_batch_request: RuntimeBatchRequest = RuntimeBatchRequest(
        **{
            "datasource_name": "my_datasource",
            "data_connector_name": "my_runtime_data_connector",
            "data_asset_name": "test_df",
            "batch_identifiers": {
                "pipeline_stage_name": "core_processing",
                "airflow_run_id": 1234567890,
            },
            "runtime_parameters": {"batch_data": test_df},
        }
    )

    checkpoint: Checkpoint = Checkpoint(
        name="my_checkpoint",
        data_context=context,
        expectation_suite_name="my_expectation_suite",
        action_list=common_action_list,
    )
    with pytest.raises(gx_exceptions.DataContextError, match=r"expectation_suite .* not found"):
        # noinspection PyUnusedLocal
        result = checkpoint.run(
            validations=[
                {"batch_request": runtime_batch_request},
                {"batch_request": batch_request},
            ]
        )

    assert len(context.validations_store.list_keys()) == 0

    context.suites.add(ExpectationSuite("my_expectation_suite"))
    # noinspection PyUnusedLocal
    result = checkpoint.run(
        validations=[
            {"batch_request": runtime_batch_request},
            {"batch_request": batch_request},
        ]
    )

    assert len(context.validations_store.list_keys()) == 2
    assert result["success"]


@pytest.mark.spark
def test_newstyle_checkpoint_instantiates_and_produces_a_validation_result_when_run_batch_request_object_multi_validation_sparkdf(  # noqa: E501
    data_context_with_datasource_spark_engine,
    common_action_list,
    spark_session,
):
    context: FileDataContext = data_context_with_datasource_spark_engine
    pandas_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})
    test_df_1 = spark_session.createDataFrame(pandas_df)
    pandas_df: pd.DataFrame = pd.DataFrame(data={"col1": [5, 6], "col2": [7, 8]})
    test_df_2 = spark_session.createDataFrame(pandas_df)

    # RuntimeBatchRequest with a DataFrame
    runtime_batch_request_1: RuntimeBatchRequest = RuntimeBatchRequest(
        **{
            "datasource_name": "my_datasource",
            "data_connector_name": "default_runtime_data_connector_name",
            "data_asset_name": "test_df_1",
            "batch_identifiers": {"default_identifier_name": "test_identifier"},
            "runtime_parameters": {"batch_data": test_df_1},
        }
    )

    # RuntimeBatchRequest with a DataFrame
    runtime_batch_request_2: RuntimeBatchRequest = RuntimeBatchRequest(
        **{
            "datasource_name": "my_datasource",
            "data_connector_name": "default_runtime_data_connector_name",
            "data_asset_name": "test_df_2",
            "batch_identifiers": {"default_identifier_name": "test_identifier"},
            "runtime_parameters": {"batch_data": test_df_2},
        }
    )

    checkpoint: Checkpoint = Checkpoint(
        name="my_checkpoint",
        data_context=context,
        expectation_suite_name="my_expectation_suite",
        action_list=common_action_list,
    )
    with pytest.raises(gx_exceptions.DataContextError, match=r"expectation_suite .* not found"):
        # noinspection PyUnusedLocal
        result = checkpoint.run(
            validations=[
                {"batch_request": runtime_batch_request_1},
                {"batch_request": runtime_batch_request_2},
            ]
        )

    assert len(context.validations_store.list_keys()) == 0

    context.suites.add(ExpectationSuite("my_expectation_suite"))
    # noinspection PyUnusedLocal
    result = checkpoint.run(
        validations=[
            {"batch_request": runtime_batch_request_1},
            {"batch_request": runtime_batch_request_2},
        ]
    )

    assert len(context.validations_store.list_keys()) == 2
    assert result["success"]


@pytest.mark.filesystem
@pytest.mark.slow  # 1.08s
def test_newstyle_checkpoint_instantiates_and_produces_a_validation_result_when_run_single_runtime_batch_request_query_in_validations(  # noqa: E501
    data_context_with_datasource_sqlalchemy_engine,
    common_action_list,
    sa,
):
    context: FileDataContext = data_context_with_datasource_sqlalchemy_engine

    # create expectation suite
    context.suites.add(ExpectationSuite("my_expectation_suite"))

    # RuntimeBatchRequest with a query
    runtime_batch_request: RuntimeBatchRequest = RuntimeBatchRequest(
        **{
            "datasource_name": "my_datasource",
            "data_connector_name": "default_runtime_data_connector_name",
            "data_asset_name": "default_data_asset_name",
            "batch_identifiers": {"default_identifier_name": "test_identifier"},
            "runtime_parameters": {
                "query": "SELECT * from table_partitioned_by_date_column__A LIMIT 10"
            },
        }
    )

    checkpoint: Checkpoint = Checkpoint(
        name="my_checkpoint",
        data_context=context,
        expectation_suite_name="my_expectation_suite",
        action_list=common_action_list,
        validations=[{"batch_request": runtime_batch_request}],
    )

    result = checkpoint.run()

    assert len(context.validations_store.list_keys()) == 1
    assert result["success"]


@pytest.mark.filesystem
def test_newstyle_checkpoint_instantiates_and_produces_a_validation_result_when_run_multiple_runtime_batch_request_query_in_validations(  # noqa: E501
    data_context_with_datasource_sqlalchemy_engine,
    common_action_list,
    sa,
):
    context: FileDataContext = data_context_with_datasource_sqlalchemy_engine

    # create expectation suite
    context.suites.add(ExpectationSuite("my_expectation_suite"))

    checkpoint: Checkpoint = Checkpoint(
        name="my_checkpoint",
        data_context=context,
        expectation_suite_name="my_expectation_suite",
        action_list=common_action_list,
    )

    with pytest.raises(
        gx_exceptions.CheckpointError,
        match='Checkpoint "my_checkpoint" must be called with a validator or contain either a batch_request or validations.',  # noqa: E501
    ):
        checkpoint.run()


@pytest.mark.filesystem
def test_newstyle_checkpoint_instantiates_and_produces_a_validation_result_when_run_runtime_batch_request_query_in_top_level_batch_request(  # noqa: E501
    data_context_with_datasource_sqlalchemy_engine,
    common_action_list,
    sa,
):
    context: FileDataContext = data_context_with_datasource_sqlalchemy_engine

    # create expectation suite
    context.suites.add(ExpectationSuite("my_expectation_suite"))

    # RuntimeBatchRequest with a query
    runtime_batch_request: RuntimeBatchRequest = RuntimeBatchRequest(
        **{
            "datasource_name": "my_datasource",
            "data_connector_name": "default_runtime_data_connector_name",
            "data_asset_name": "default_data_asset_name",
            "batch_identifiers": {"default_identifier_name": "test_identifier"},
            "runtime_parameters": {
                "query": "SELECT * from table_partitioned_by_date_column__A LIMIT 10"
            },
        }
    )

    checkpoint: Checkpoint = Checkpoint(
        name="my_checkpoint",
        data_context=context,
        expectation_suite_name="my_expectation_suite",
        action_list=common_action_list,
        batch_request=runtime_batch_request,
    )

    result = checkpoint.run()

    assert len(context.validations_store.list_keys()) == 1
    assert result["success"]


@pytest.mark.filesystem
def test_newstyle_checkpoint_instantiates_and_produces_a_validation_result_when_run_runtime_batch_request_batch_data_in_top_level_batch_request_pandasdf(  # noqa: E501
    data_context_with_datasource_pandas_engine,
    common_action_list,
):
    context: FileDataContext = data_context_with_datasource_pandas_engine
    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    # create expectation suite
    context.suites.add(ExpectationSuite("my_expectation_suite"))

    # RuntimeBatchRequest with a DataFrame
    runtime_batch_request: RuntimeBatchRequest = RuntimeBatchRequest(
        **{
            "datasource_name": "my_datasource",
            "data_connector_name": "default_runtime_data_connector_name",
            "data_asset_name": "default_data_asset_name",
            "batch_identifiers": {"default_identifier_name": "test_identifier"},
            "runtime_parameters": {"batch_data": test_df},
        }
    )

    checkpoint: Checkpoint = Checkpoint(
        name="my_checkpoint",
        data_context=context,
        expectation_suite_name="my_expectation_suite",
        action_list=common_action_list,
    )

    result = checkpoint.run(batch_request=runtime_batch_request)

    assert len(context.validations_store.list_keys()) == 1
    assert result["success"]


@pytest.mark.spark
def test_newstyle_checkpoint_instantiates_and_produces_a_validation_result_when_run_runtime_batch_request_batch_data_in_top_level_batch_request_sparkdf(  # noqa: E501
    data_context_with_datasource_spark_engine,
    common_action_list,
    spark_session,
):
    context: FileDataContext = data_context_with_datasource_spark_engine
    pandas_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})
    test_df = spark_session.createDataFrame(pandas_df)

    # create expectation suite
    context.suites.add(ExpectationSuite("my_expectation_suite"))

    # RuntimeBatchRequest with a DataFrame
    runtime_batch_request: RuntimeBatchRequest = RuntimeBatchRequest(
        **{
            "datasource_name": "my_datasource",
            "data_connector_name": "default_runtime_data_connector_name",
            "data_asset_name": "default_data_asset_name",
            "batch_identifiers": {"default_identifier_name": "test_identifier"},
            "runtime_parameters": {"batch_data": test_df},
        }
    )

    checkpoint: Checkpoint = Checkpoint(
        name="my_checkpoint",
        data_context=context,
        expectation_suite_name="my_expectation_suite",
        action_list=common_action_list,
    )

    result = checkpoint.run(batch_request=runtime_batch_request)

    assert len(context.validations_store.list_keys()) == 1
    assert result["success"]


@pytest.mark.slow  # 1.09s
@pytest.mark.filesystem
def test_newstyle_checkpoint_instantiates_and_produces_a_validation_result_when_run_runtime_batch_request_path_in_top_level_batch_request_pandas(  # noqa: E501
    titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled,
    common_action_list,
):
    context: FileDataContext = titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled  # noqa: E501
    data_path: str = os.path.join(  # noqa: PTH118
        context.datasources["my_datasource"]
        .data_connectors["my_basic_data_connector"]
        .base_directory,
        "Titanic_19120414_1313.csv",
    )

    # create expectation suite
    context.suites.add(ExpectationSuite("my_expectation_suite"))

    # RuntimeBatchRequest with a query
    runtime_batch_request: RuntimeBatchRequest = RuntimeBatchRequest(
        **{
            "datasource_name": "my_datasource",
            "data_connector_name": "my_runtime_data_connector",
            "data_asset_name": "Titanic_19120414_1313.csv",
            "batch_identifiers": {
                "pipeline_stage_name": "core_processing",
                "airflow_run_id": 1234567890,
            },
            "runtime_parameters": {"path": data_path},
        }
    )

    checkpoint: Checkpoint = Checkpoint(
        name="my_checkpoint",
        data_context=context,
        expectation_suite_name="my_expectation_suite",
        action_list=common_action_list,
        batch_request=runtime_batch_request,
    )

    result = checkpoint.run()

    assert len(context.validations_store.list_keys()) == 1
    assert result["success"]


@pytest.mark.filesystem
def test_newstyle_checkpoint_instantiates_and_produces_a_validation_result_when_run_runtime_batch_request_query_in_checkpoint_run(  # noqa: E501
    data_context_with_datasource_sqlalchemy_engine,
    common_action_list,
    sa,
):
    context: FileDataContext = data_context_with_datasource_sqlalchemy_engine

    # create expectation suite
    context.suites.add(ExpectationSuite("my_expectation_suite"))

    # RuntimeBatchRequest with a query
    runtime_batch_request: RuntimeBatchRequest = RuntimeBatchRequest(
        **{
            "datasource_name": "my_datasource",
            "data_connector_name": "default_runtime_data_connector_name",
            "data_asset_name": "default_data_asset_name",
            "batch_identifiers": {"default_identifier_name": "test_identifier"},
            "runtime_parameters": {
                "query": "SELECT * from table_partitioned_by_date_column__A LIMIT 10"
            },
        }
    )

    checkpoint: Checkpoint = Checkpoint(
        name="my_checkpoint",
        data_context=context,
        expectation_suite_name="my_expectation_suite",
        action_list=common_action_list,
    )

    result = checkpoint.run(batch_request=runtime_batch_request)

    assert len(context.validations_store.list_keys()) == 1
    assert result["success"]


@pytest.mark.filesystem
def test_newstyle_checkpoint_instantiates_and_produces_a_validation_result_when_run_runtime_validations_query_in_checkpoint_run(  # noqa: E501
    data_context_with_datasource_sqlalchemy_engine,
    common_action_list,
    sa,
):
    context: FileDataContext = data_context_with_datasource_sqlalchemy_engine

    # create expectation suite
    context.suites.add(ExpectationSuite("my_expectation_suite"))

    # RuntimeBatchRequest with a query
    runtime_batch_request: RuntimeBatchRequest = RuntimeBatchRequest(
        **{
            "datasource_name": "my_datasource",
            "data_connector_name": "default_runtime_data_connector_name",
            "data_asset_name": "default_data_asset_name",
            "batch_identifiers": {"default_identifier_name": "test_identifier"},
            "runtime_parameters": {
                "query": "SELECT * from table_partitioned_by_date_column__A LIMIT 10"
            },
        }
    )

    checkpoint: Checkpoint = Checkpoint(
        name="my_checkpoint",
        data_context=context,
        expectation_suite_name="my_expectation_suite",
        action_list=common_action_list,
    )

    result = checkpoint.run(validations=[{"batch_request": runtime_batch_request}])

    assert len(context.validations_store.list_keys()) == 1
    assert result["success"]


@pytest.mark.slow  # 1.11s
@pytest.mark.filesystem
def test_newstyle_checkpoint_instantiates_and_produces_a_validation_result_when_run_runtime_batch_request_path_in_checkpoint_run_pandas(  # noqa: E501
    titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled,
    common_action_list,
):
    context: FileDataContext = titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled  # noqa: E501
    data_path: str = os.path.join(  # noqa: PTH118
        context.datasources["my_datasource"]
        .data_connectors["my_basic_data_connector"]
        .base_directory,
        "Titanic_19120414_1313.csv",
    )

    # create expectation suite
    context.suites.add(ExpectationSuite("my_expectation_suite"))

    # RuntimeBatchRequest with a query
    runtime_batch_request: RuntimeBatchRequest = RuntimeBatchRequest(
        **{
            "datasource_name": "my_datasource",
            "data_connector_name": "my_runtime_data_connector",
            "data_asset_name": "Titanic_19120414_1313.csv",
            "batch_identifiers": {
                "pipeline_stage_name": "core_processing",
                "airflow_run_id": 1234567890,
            },
            "runtime_parameters": {"path": data_path},
        }
    )

    checkpoint: Checkpoint = Checkpoint(
        name="my_checkpoint",
        data_context=context,
        expectation_suite_name="my_expectation_suite",
        action_list=common_action_list,
    )

    result = checkpoint.run(batch_request=runtime_batch_request)

    assert len(context.validations_store.list_keys()) == 1
    assert result["success"]


@pytest.mark.filesystem
def test_newstyle_checkpoint_instantiates_and_produces_a_validation_result_when_run_runtime_batch_request_path_in_checkpoint_run_pandas(  # noqa: E501, F811
    titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled,
    common_action_list,
):
    context: FileDataContext = titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled  # noqa: E501
    data_path: str = os.path.join(  # noqa: PTH118
        context.datasources["my_datasource"]
        .data_connectors["my_basic_data_connector"]
        .base_directory,
        "Titanic_19120414_1313.csv",
    )

    # create expectation suite
    context.suites.add(ExpectationSuite("my_expectation_suite"))

    # RuntimeBatchRequest with a query
    runtime_batch_request: RuntimeBatchRequest = RuntimeBatchRequest(
        **{
            "datasource_name": "my_datasource",
            "data_connector_name": "my_runtime_data_connector",
            "data_asset_name": "Titanic_19120414_1313.csv",
            "batch_identifiers": {
                "pipeline_stage_name": "core_processing",
                "airflow_run_id": 1234567890,
            },
            "runtime_parameters": {"path": data_path},
        }
    )

    checkpoint: Checkpoint = Checkpoint(
        name="my_checkpoint",
        data_context=context,
        expectation_suite_name="my_expectation_suite",
        action_list=common_action_list,
    )

    result = checkpoint.run(validations=[{"batch_request": runtime_batch_request}])

    assert len(context.validations_store.list_keys()) == 1
    assert result["success"]


@pytest.mark.filesystem
def test_newstyle_checkpoint_instantiates_and_produces_a_validation_result_when_run_runtime_batch_request_query_in_context_run_checkpoint(  # noqa: E501
    data_context_with_datasource_sqlalchemy_engine,
    common_action_list,
    sa,
):
    context: FileDataContext = data_context_with_datasource_sqlalchemy_engine

    # create expectation suite
    context.suites.add(ExpectationSuite("my_expectation_suite"))

    # RuntimeBatchRequest with a query
    runtime_batch_request: RuntimeBatchRequest = RuntimeBatchRequest(
        **{
            "datasource_name": "my_datasource",
            "data_connector_name": "default_runtime_data_connector_name",
            "data_asset_name": "default_data_asset_name",
            "batch_identifiers": {"default_identifier_name": "test_identifier"},
            "runtime_parameters": {
                "query": "SELECT * from table_partitioned_by_date_column__A LIMIT 10"
            },
        }
    )

    # add checkpoint config
    checkpoint_config: dict = {
        "name": "my_checkpoint",
        "expectation_suite_name": "my_expectation_suite",
        "action_list": common_action_list,
    }

    context.add_checkpoint(**checkpoint_config)

    checkpoint = context.get_legacy_checkpoint("my_checkpoint")
    result = checkpoint.run(batch_request=runtime_batch_request)

    assert len(context.validations_store.list_keys()) == 1
    assert result["success"]


@pytest.mark.filesystem
def test_newstyle_checkpoint_instantiates_and_produces_a_validation_result_when_run_runtime_batch_request_batch_data_in_context_run_checkpoint_pandasdf(  # noqa: E501
    data_context_with_datasource_pandas_engine,
    common_action_list,
):
    context: FileDataContext = data_context_with_datasource_pandas_engine
    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    # create expectation suite
    context.suites.add(ExpectationSuite("my_expectation_suite"))

    # RuntimeBatchRequest with a DataFrame
    runtime_batch_request: RuntimeBatchRequest = RuntimeBatchRequest(
        **{
            "datasource_name": "my_datasource",
            "data_connector_name": "default_runtime_data_connector_name",
            "data_asset_name": "default_data_asset_name",
            "batch_identifiers": {"default_identifier_name": "test_identifier"},
            "runtime_parameters": {"batch_data": test_df},
        }
    )

    # add checkpoint config
    checkpoint_config: dict = {
        "name": "my_checkpoint",
        "expectation_suite_name": "my_expectation_suite",
        "action_list": common_action_list,
    }

    context.add_checkpoint(**checkpoint_config)

    checkpoint = context.get_legacy_checkpoint("my_checkpoint")
    result = checkpoint.run(batch_request=runtime_batch_request)

    assert len(context.validations_store.list_keys()) == 1
    assert result["success"]


@pytest.mark.spark
def test_newstyle_checkpoint_instantiates_and_produces_a_validation_result_when_run_runtime_batch_request_batch_data_in_context_run_checkpoint_sparkdf(  # noqa: E501
    data_context_with_datasource_spark_engine,
    common_action_list,
):
    context: FileDataContext = data_context_with_datasource_spark_engine
    pandas_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})
    test_df = SparkDFExecutionEngine.get_or_create_spark_session().createDataFrame(pandas_df)

    # create expectation suite
    context.suites.add(ExpectationSuite("my_expectation_suite"))

    # RuntimeBatchRequest with a DataFrame
    runtime_batch_request: RuntimeBatchRequest = RuntimeBatchRequest(
        **{
            "datasource_name": "my_datasource",
            "data_connector_name": "default_runtime_data_connector_name",
            "data_asset_name": "default_data_asset_name",
            "batch_identifiers": {"default_identifier_name": "test_identifier"},
            "runtime_parameters": {"batch_data": test_df},
        }
    )

    # add checkpoint config
    checkpoint_config: dict = {
        "name": "my_checkpoint",
        "expectation_suite_name": "my_expectation_suite",
        "action_list": common_action_list,
    }

    context.add_checkpoint(**checkpoint_config)

    checkpoint = context.get_legacy_checkpoint("my_checkpoint")
    result = checkpoint.run(batch_request=runtime_batch_request)

    assert len(context.validations_store.list_keys()) == 1
    assert result["success"]


@pytest.mark.filesystem
def test_newstyle_checkpoint_instantiates_and_produces_a_validation_result_when_run_runtime_validations_query_in_context_run_checkpoint(  # noqa: E501
    data_context_with_datasource_sqlalchemy_engine,
    common_action_list,
    sa,
):
    context: FileDataContext = data_context_with_datasource_sqlalchemy_engine

    # create expectation suite
    context.suites.add(ExpectationSuite("my_expectation_suite"))

    # RuntimeBatchRequest with a query
    runtime_batch_request: RuntimeBatchRequest = RuntimeBatchRequest(
        **{
            "datasource_name": "my_datasource",
            "data_connector_name": "default_runtime_data_connector_name",
            "data_asset_name": "default_data_asset_name",
            "batch_identifiers": {"default_identifier_name": "test_identifier"},
            "runtime_parameters": {
                "query": "SELECT * from table_partitioned_by_date_column__A LIMIT 10"
            },
        }
    )

    # add checkpoint config
    checkpoint_config: dict = {
        "name": "my_checkpoint",
        "expectation_suite_name": "my_expectation_suite",
        "action_list": common_action_list,
    }

    context.add_checkpoint(**checkpoint_config)

    checkpoint = context.get_legacy_checkpoint("my_checkpoint")
    result = checkpoint.run(
        validations=[{"batch_request": runtime_batch_request}],
    )

    assert len(context.validations_store.list_keys()) == 1
    assert result["success"]


@pytest.mark.filesystem
def test_newstyle_checkpoint_instantiates_and_produces_a_validation_result_when_run_runtime_validations_batch_data_in_context_run_checkpoint_pandasdf(  # noqa: E501
    data_context_with_datasource_pandas_engine,
    common_action_list,
):
    context: FileDataContext = data_context_with_datasource_pandas_engine
    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    # create expectation suite
    context.suites.add(ExpectationSuite("my_expectation_suite"))

    # RuntimeBatchRequest with a DataFrame
    runtime_batch_request: RuntimeBatchRequest = RuntimeBatchRequest(
        **{
            "datasource_name": "my_datasource",
            "data_connector_name": "default_runtime_data_connector_name",
            "data_asset_name": "default_data_asset_name",
            "batch_identifiers": {"default_identifier_name": "test_identifier"},
            "runtime_parameters": {"batch_data": test_df},
        }
    )

    # add checkpoint config
    checkpoint_config: dict = {
        "name": "my_checkpoint",
        "expectation_suite_name": "my_expectation_suite",
        "action_list": common_action_list,
    }

    context.add_checkpoint(**checkpoint_config)

    checkpoint = context.get_legacy_checkpoint("my_checkpoint")
    result = checkpoint.run(
        validations=[{"batch_request": runtime_batch_request}],
    )

    assert len(context.validations_store.list_keys()) == 1
    assert result["success"]


@pytest.mark.spark
def test_newstyle_checkpoint_instantiates_and_produces_a_validation_result_when_run_runtime_validations_batch_data_in_context_run_checkpoint_sparkdf(  # noqa: E501
    data_context_with_datasource_spark_engine,
    common_action_list,
):
    context: FileDataContext = data_context_with_datasource_spark_engine
    pandas_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})
    test_df = SparkDFExecutionEngine.get_or_create_spark_session().createDataFrame(pandas_df)

    # create expectation suite
    context.suites.add(ExpectationSuite("my_expectation_suite"))

    # RuntimeBatchRequest with a DataFrame
    runtime_batch_request: RuntimeBatchRequest = RuntimeBatchRequest(
        **{
            "datasource_name": "my_datasource",
            "data_connector_name": "default_runtime_data_connector_name",
            "data_asset_name": "default_data_asset_name",
            "batch_identifiers": {"default_identifier_name": "test_identifier"},
            "runtime_parameters": {"batch_data": test_df},
        }
    )

    # add checkpoint config
    checkpoint_config: dict = {
        "name": "my_checkpoint",
        "expectation_suite_name": "my_expectation_suite",
        "action_list": common_action_list,
    }

    context.add_checkpoint(**checkpoint_config)

    checkpoint = context.get_legacy_checkpoint("my_checkpoint")
    result = checkpoint.run(
        validations=[{"batch_request": runtime_batch_request}],
    )

    assert len(context.validations_store.list_keys()) == 1
    assert result["success"]


@pytest.mark.slow  # 1.18s
@pytest.mark.filesystem
def test_newstyle_checkpoint_instantiates_and_produces_a_validation_result_when_run_runtime_batch_request_path_in_context_run_checkpoint_pandas(  # noqa: E501
    titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled,
    common_action_list,
):
    context: FileDataContext = titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled  # noqa: E501
    data_path: str = os.path.join(  # noqa: PTH118
        context.datasources["my_datasource"]
        .data_connectors["my_basic_data_connector"]
        .base_directory,
        "Titanic_19120414_1313.csv",
    )

    # create expectation suite
    context.suites.add(ExpectationSuite("my_expectation_suite"))

    # RuntimeBatchRequest with a query
    runtime_batch_request: RuntimeBatchRequest = RuntimeBatchRequest(
        **{
            "datasource_name": "my_datasource",
            "data_connector_name": "my_runtime_data_connector",
            "data_asset_name": "Titanic_19120414_1313.csv",
            "batch_identifiers": {
                "pipeline_stage_name": "core_processing",
                "airflow_run_id": 1234567890,
            },
            "runtime_parameters": {"path": data_path},
        }
    )

    # add checkpoint config
    checkpoint_config: dict = {
        "name": "my_checkpoint",
        "expectation_suite_name": "my_expectation_suite",
        "action_list": common_action_list,
    }

    context.add_checkpoint(**checkpoint_config)

    checkpoint = context.get_legacy_checkpoint("my_checkpoint")
    result = checkpoint.run(batch_request=runtime_batch_request)

    assert len(context.validations_store.list_keys()) == 1
    assert result["success"]


@pytest.mark.filesystem
def test_newstyle_checkpoint_instantiates_and_produces_a_validation_result_when_run_runtime_batch_request_path_in_context_run_checkpoint_pandas(  # noqa: E501, F811
    titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled,
    common_action_list,
):
    context: FileDataContext = titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled  # noqa: E501
    data_path: str = os.path.join(  # noqa: PTH118
        context.datasources["my_datasource"]
        .data_connectors["my_basic_data_connector"]
        .base_directory,
        "Titanic_19120414_1313.csv",
    )

    # create expectation suite
    context.suites.add(ExpectationSuite("my_expectation_suite"))

    # RuntimeBatchRequest with a query
    runtime_batch_request: RuntimeBatchRequest = RuntimeBatchRequest(
        **{
            "datasource_name": "my_datasource",
            "data_connector_name": "my_runtime_data_connector",
            "data_asset_name": "Titanic_19120414_1313.csv",
            "batch_identifiers": {
                "pipeline_stage_name": "core_processing",
                "airflow_run_id": 1234567890,
            },
            "runtime_parameters": {"path": data_path},
        }
    )

    # add checkpoint config
    checkpoint_config: dict = {
        "name": "my_checkpoint",
        "expectation_suite_name": "my_expectation_suite",
        "action_list": common_action_list,
    }

    context.add_checkpoint(**checkpoint_config)

    checkpoint = context.get_legacy_checkpoint("my_checkpoint")
    result = checkpoint.run(
        validations=[{"batch_request": runtime_batch_request}],
    )

    assert len(context.validations_store.list_keys()) == 1
    assert result["success"]


@pytest.mark.filesystem
def test_newstyle_checkpoint_instantiates_and_produces_a_printable_validation_result_with_batch_data(  # noqa: E501
    data_context_with_datasource_pandas_engine,
    common_action_list,
):
    context: FileDataContext = data_context_with_datasource_pandas_engine
    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    # create expectation suite
    context.suites.add(ExpectationSuite("my_expectation_suite"))

    # RuntimeBatchRequest with a DataFrame
    runtime_batch_request: RuntimeBatchRequest = RuntimeBatchRequest(
        **{
            "datasource_name": "my_datasource",
            "data_connector_name": "default_runtime_data_connector_name",
            "data_asset_name": "default_data_asset_name",
            "batch_identifiers": {"default_identifier_name": "test_identifier"},
            "runtime_parameters": {"batch_data": test_df},
        }
    )

    # add checkpoint config
    checkpoint: Checkpoint = Checkpoint(
        name="my_checkpoint",
        data_context=context,
        expectation_suite_name="my_expectation_suite",
        action_list=common_action_list,
    )

    result = checkpoint.run(batch_request=runtime_batch_request)

    assert type(repr(result)) == str  # noqa: E721


@pytest.mark.filesystem
def test_newstyle_checkpoint_instantiates_and_produces_a_runtime_parameters_error_contradictory_batch_request_in_checkpoint_yml_and_checkpoint_run(  # noqa: E501
    titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled,
    common_action_list,
):
    context: FileDataContext = titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled  # noqa: E501
    data_path: str = os.path.join(  # noqa: PTH118
        context.datasources["my_datasource"]
        .data_connectors["my_basic_data_connector"]
        .base_directory,
        "Titanic_19120414_1313.csv",
    )

    # create expectation suite
    context.suites.add(ExpectationSuite("my_expectation_suite"))

    # RuntimeBatchRequest with a path
    # Using typed object instead of dictionary, expected by "add_checkpoint()", on purpose to insure that checks work.  # noqa: E501
    batch_request: RuntimeBatchRequest = RuntimeBatchRequest(
        **{
            "datasource_name": "my_datasource",
            "data_connector_name": "my_runtime_data_connector",
            "data_asset_name": "Titanic_19120414_1313.csv",
            "batch_identifiers": {
                "pipeline_stage_name": "core_processing",
                "airflow_run_id": 1234567890,
            },
            "runtime_parameters": {"path": data_path},
        }
    )

    # add checkpoint config
    checkpoint_config: dict = {
        "name": "my_checkpoint",
        "expectation_suite_name": "my_expectation_suite",
        "action_list": common_action_list,
        "batch_request": batch_request,
    }

    context.add_checkpoint(**checkpoint_config)
    checkpoint: Checkpoint = context.get_legacy_checkpoint(name="my_checkpoint")

    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    # RuntimeBatchRequest with a DataFrame
    runtime_batch_request: RuntimeBatchRequest = RuntimeBatchRequest(
        **{
            "datasource_name": "my_datasource",
            "data_connector_name": "my_runtime_data_connector",
            "data_asset_name": "Titanic_19120414_1313.csv",
            "batch_identifiers": {
                "pipeline_stage_name": "core_processing",
                "airflow_run_id": 1234567890,
            },
            "runtime_parameters": {"batch_data": test_df},
        }
    )

    with pytest.raises(
        gx_exceptions.exceptions.InvalidBatchRequestError,
        match=r"The runtime_parameters dict must have one \(and only one\) of the following keys: 'batch_data', 'query', 'path'.",  # noqa: E501
    ):
        checkpoint.run(batch_request=runtime_batch_request)


@pytest.mark.slow  # 1.75s
@pytest.mark.filesystem
def test_newstyle_checkpoint_instantiates_and_produces_a_correct_validation_result_batch_request_in_checkpoint_yml_and_checkpoint_run(  # noqa: E501
    titanic_pandas_data_context_stats_enabled_and_expectation_suite_with_one_expectation,
    common_action_list,
):
    context: FileDataContext = (
        titanic_pandas_data_context_stats_enabled_and_expectation_suite_with_one_expectation
    )
    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    batch_request: dict = {
        "datasource_name": "my_datasource",
        "data_connector_name": "my_basic_data_connector",
        "data_asset_name": "Titanic_1911",
    }

    # RuntimeBatchRequest with a DataFrame
    runtime_batch_request: RuntimeBatchRequest = RuntimeBatchRequest(
        **{
            "datasource_name": "my_datasource",
            "data_connector_name": "my_runtime_data_connector",
            "data_asset_name": "test_df",
            "batch_identifiers": {
                "pipeline_stage_name": "core_processing",
                "airflow_run_id": 1234567890,
            },
            "runtime_parameters": {"batch_data": test_df},
        }
    )

    # add checkpoint config
    checkpoint_config: dict = {
        "name": "my_checkpoint",
        "expectation_suite_name": "my_expectation_suite",
        "action_list": common_action_list,
        "batch_request": batch_request,
    }

    context.add_checkpoint(**checkpoint_config)
    checkpoint: Checkpoint = context.get_legacy_checkpoint(name="my_checkpoint")

    result = checkpoint.run()
    assert not result["success"]
    assert (
        list(result.run_results.values())[0]["validation_result"]["statistics"][
            "evaluated_expectations"
        ]
        == 1
    )
    assert (
        list(result.run_results.values())[0]["validation_result"]["statistics"][
            "successful_expectations"
        ]
        == 0
    )

    result = checkpoint.run(batch_request=runtime_batch_request)
    assert result["success"]
    assert (
        list(result.run_results.values())[0]["validation_result"]["statistics"][
            "evaluated_expectations"
        ]
        == 1
    )
    assert (
        list(result.run_results.values())[0]["validation_result"]["statistics"][
            "successful_expectations"
        ]
        == 1
    )


@pytest.mark.slow  # 2.35s
@pytest.mark.filesystem
def test_newstyle_checkpoint_instantiates_and_produces_a_correct_validation_result_validations_in_checkpoint_yml_and_checkpoint_run(  # noqa: E501
    titanic_pandas_data_context_stats_enabled_and_expectation_suite_with_one_expectation,
    common_action_list,
):
    context: FileDataContext = (
        titanic_pandas_data_context_stats_enabled_and_expectation_suite_with_one_expectation
    )
    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    batch_request: dict = {
        "datasource_name": "my_datasource",
        "data_connector_name": "my_basic_data_connector",
        "data_asset_name": "Titanic_1911",
    }

    # RuntimeBatchRequest with a DataFrame
    runtime_batch_request: RuntimeBatchRequest = RuntimeBatchRequest(
        **{
            "datasource_name": "my_datasource",
            "data_connector_name": "my_runtime_data_connector",
            "data_asset_name": "test_df",
            "batch_identifiers": {
                "pipeline_stage_name": "core_processing",
                "airflow_run_id": 1234567890,
            },
            "runtime_parameters": {"batch_data": test_df},
        }
    )

    # add checkpoint config
    checkpoint_config: dict = {
        "name": "my_checkpoint",
        "expectation_suite_name": "my_expectation_suite",
        "action_list": common_action_list,
        "validations": [{"batch_request": batch_request}],
    }

    context.add_checkpoint(**checkpoint_config)
    checkpoint: Checkpoint = context.get_legacy_checkpoint(name="my_checkpoint")

    result = checkpoint.run()
    assert result["success"] is False
    assert len(result.run_results.values()) == 1
    assert (
        list(result.run_results.values())[0]["validation_result"]["statistics"][
            "evaluated_expectations"
        ]
        == 1
    )
    assert (
        list(result.run_results.values())[0]["validation_result"]["statistics"][
            "successful_expectations"
        ]
        == 0
    )

    result = checkpoint.run(validations=[{"batch_request": runtime_batch_request}])
    assert result["success"] is False
    assert len(result.run_results.values()) == 2
    assert (
        list(result.run_results.values())[0]["validation_result"]["statistics"][
            "evaluated_expectations"
        ]
        == 1
    )
    assert (
        list(result.run_results.values())[0]["validation_result"]["statistics"][
            "successful_expectations"
        ]
        == 0
    )
    assert (
        list(result.run_results.values())[1]["validation_result"]["statistics"][
            "evaluated_expectations"
        ]
        == 1
    )
    assert (
        list(result.run_results.values())[1]["validation_result"]["statistics"][
            "successful_expectations"
        ]
        == 1
    )


@pytest.mark.slow  # 1.91s
@pytest.mark.filesystem
def test_newstyle_checkpoint_instantiates_and_produces_a_correct_validation_result_batch_request_in_checkpoint_yml_and_context_run_checkpoint(  # noqa: E501
    titanic_pandas_data_context_stats_enabled_and_expectation_suite_with_one_expectation,
    common_action_list,
):
    context: FileDataContext = (
        titanic_pandas_data_context_stats_enabled_and_expectation_suite_with_one_expectation
    )
    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    batch_request: dict = {
        "datasource_name": "my_datasource",
        "data_connector_name": "my_basic_data_connector",
        "data_asset_name": "Titanic_1911",
    }

    # RuntimeBatchRequest with a DataFrame
    runtime_batch_request: RuntimeBatchRequest = RuntimeBatchRequest(
        **{
            "datasource_name": "my_datasource",
            "data_connector_name": "my_runtime_data_connector",
            "data_asset_name": "test_df",
            "batch_identifiers": {
                "pipeline_stage_name": "core_processing",
                "airflow_run_id": 1234567890,
            },
            "runtime_parameters": {"batch_data": test_df},
        }
    )

    # add checkpoint config
    checkpoint_config: dict = {
        "name": "my_checkpoint",
        "expectation_suite_name": "my_expectation_suite",
        "action_list": common_action_list,
        "batch_request": batch_request,
    }

    context.add_checkpoint(**checkpoint_config)

    checkpoint = context.get_legacy_checkpoint("my_checkpoint")
    result = checkpoint.run()
    assert result["success"] is False
    assert (
        list(result.run_results.values())[0]["validation_result"]["statistics"][
            "evaluated_expectations"
        ]
        == 1
    )
    assert (
        list(result.run_results.values())[0]["validation_result"]["statistics"][
            "successful_expectations"
        ]
        == 0
    )

    checkpoint = context.get_legacy_checkpoint("my_checkpoint")
    result = checkpoint.run(batch_request=runtime_batch_request)

    assert result["success"]
    assert (
        list(result.run_results.values())[0]["validation_result"]["statistics"][
            "evaluated_expectations"
        ]
        == 1
    )
    assert (
        list(result.run_results.values())[0]["validation_result"]["statistics"][
            "successful_expectations"
        ]
        == 1
    )


@pytest.mark.slow  # 2.46s
@pytest.mark.filesystem
def test_newstyle_checkpoint_instantiates_and_produces_a_correct_validation_result_validations_in_checkpoint_yml_and_context_run_checkpoint(  # noqa: E501
    titanic_pandas_data_context_stats_enabled_and_expectation_suite_with_one_expectation,
    common_action_list,
):
    context: FileDataContext = (
        titanic_pandas_data_context_stats_enabled_and_expectation_suite_with_one_expectation
    )
    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    batch_request: dict = {
        "datasource_name": "my_datasource",
        "data_connector_name": "my_basic_data_connector",
        "data_asset_name": "Titanic_1911",
    }

    # RuntimeBatchRequest with a DataFrame
    runtime_batch_request: RuntimeBatchRequest = RuntimeBatchRequest(
        **{
            "datasource_name": "my_datasource",
            "data_connector_name": "my_runtime_data_connector",
            "data_asset_name": "test_df",
            "batch_identifiers": {
                "pipeline_stage_name": "core_processing",
                "airflow_run_id": 1234567890,
            },
            "runtime_parameters": {"batch_data": test_df},
        }
    )

    # add checkpoint config
    checkpoint_config: dict = {
        "name": "my_checkpoint",
        "expectation_suite_name": "my_expectation_suite",
        "action_list": common_action_list,
        "validations": [{"batch_request": batch_request}],
    }

    context.add_checkpoint(**checkpoint_config)

    checkpoint = context.get_legacy_checkpoint("my_checkpoint")
    result = checkpoint.run()
    assert result["success"] is False
    assert len(result.run_results.values()) == 1
    assert (
        list(result.run_results.values())[0]["validation_result"]["statistics"][
            "evaluated_expectations"
        ]
        == 1
    )
    assert (
        list(result.run_results.values())[0]["validation_result"]["statistics"][
            "successful_expectations"
        ]
        == 0
    )

    checkpoint = context.get_legacy_checkpoint("my_checkpoint")
    result = checkpoint.run(
        validations=[{"batch_request": runtime_batch_request}],
    )
    assert result["success"] is False
    assert len(result.run_results.values()) == 2
    assert (
        list(result.run_results.values())[0]["validation_result"]["statistics"][
            "evaluated_expectations"
        ]
        == 1
    )
    assert (
        list(result.run_results.values())[0]["validation_result"]["statistics"][
            "successful_expectations"
        ]
        == 0
    )
    assert (
        list(result.run_results.values())[1]["validation_result"]["statistics"][
            "evaluated_expectations"
        ]
        == 1
    )
    assert (
        list(result.run_results.values())[1]["validation_result"]["statistics"][
            "successful_expectations"
        ]
        == 1
    )


@pytest.mark.filesystem
def test_newstyle_checkpoint_does_not_pass_dataframes_via_batch_request_into_checkpoint_store(
    data_context_with_datasource_pandas_engine,
    common_action_list,
):
    context: FileDataContext = data_context_with_datasource_pandas_engine
    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    # create expectation suite
    context.suites.add(ExpectationSuite("my_expectation_suite"))

    # RuntimeBatchRequest with a DataFrame
    batch_request: RuntimeBatchRequest = RuntimeBatchRequest(
        **{
            "datasource_name": "my_datasource",
            "data_connector_name": "default_runtime_data_connector_name",
            "data_asset_name": "default_data_asset_name",
            "batch_identifiers": {"default_identifier_name": "test_identifier"},
            "runtime_parameters": {"batch_data": test_df},
        }
    )

    # add checkpoint config
    checkpoint_config: dict = {
        "name": "my_checkpoint",
        "expectation_suite_name": "my_expectation_suite",
        "action_list": common_action_list,
        "batch_request": batch_request,
    }

    with pytest.raises(
        gx_exceptions.InvalidConfigError,
        match='batch_data found in batch_request cannot be saved to CheckpointStore "checkpoint_store"',  # noqa: E501
    ):
        context.add_checkpoint(**checkpoint_config)


@pytest.mark.filesystem
def test_newstyle_checkpoint_does_not_pass_dataframes_via_validations_into_checkpoint_store(
    data_context_with_datasource_pandas_engine,
    common_action_list,
):
    context: FileDataContext = data_context_with_datasource_pandas_engine
    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    # create expectation suite
    context.suites.add(ExpectationSuite("my_expectation_suite"))

    # RuntimeBatchRequest with a DataFrame
    runtime_batch_request: RuntimeBatchRequest = RuntimeBatchRequest(
        **{
            "datasource_name": "my_datasource",
            "data_connector_name": "default_runtime_data_connector_name",
            "data_asset_name": "default_data_asset_name",
            "batch_identifiers": {"default_identifier_name": "test_identifier"},
            "runtime_parameters": {"batch_data": test_df},
        }
    )

    # add checkpoint config
    checkpoint_config: dict = {
        "name": "my_checkpoint",
        "expectation_suite_name": "my_expectation_suite",
        "action_list": common_action_list,
        "validations": [{"batch_request": runtime_batch_request}],
    }

    with pytest.raises(
        gx_exceptions.InvalidConfigError,
        match='batch_data found in validations cannot be saved to CheckpointStore "checkpoint_store"',  # noqa: E501
    ):
        context.add_checkpoint(**checkpoint_config)


@pytest.mark.slow  # 1.19s
@pytest.mark.filesystem
def test_newstyle_checkpoint_result_can_be_pickled(
    titanic_pandas_data_context_stats_enabled_and_expectation_suite_with_one_expectation,
    common_action_list,
):
    context: FileDataContext = (
        titanic_pandas_data_context_stats_enabled_and_expectation_suite_with_one_expectation
    )

    batch_request: dict = {
        "datasource_name": "my_datasource",
        "data_connector_name": "my_basic_data_connector",
        "data_asset_name": "Titanic_1911",
    }

    # add checkpoint config
    checkpoint_config: dict = {
        "name": "my_checkpoint",
        "expectation_suite_name": "my_expectation_suite",
        "action_list": common_action_list,
        "batch_request": batch_request,
    }

    context.add_checkpoint(**checkpoint_config)
    checkpoint: Checkpoint = context.get_legacy_checkpoint(name="my_checkpoint")

    result: CheckpointResult = checkpoint.run()
    assert isinstance(pickle.dumps(result), bytes)


@pytest.mark.slow  # 1.19s
@pytest.mark.filesystem
def test_newstyle_checkpoint_result_validations_include_rendered_content(
    titanic_pandas_data_context_stats_enabled_and_expectation_suite_with_one_expectation,
    common_action_list,
):
    context: FileDataContext = (
        titanic_pandas_data_context_stats_enabled_and_expectation_suite_with_one_expectation
    )

    batch_request: dict = {
        "datasource_name": "my_datasource",
        "data_connector_name": "my_basic_data_connector",
        "data_asset_name": "Titanic_1911",
    }

    include_rendered_content: bool = True

    # add checkpoint config
    checkpoint_config: dict = {
        "name": "my_checkpoint",
        "expectation_suite_name": "my_expectation_suite",
        "action_list": common_action_list,
        "validations": [
            {
                "batch_request": batch_request,
                "include_rendered_content": include_rendered_content,
            }
        ],
    }

    context.add_checkpoint(**checkpoint_config)
    checkpoint: Checkpoint = context.get_legacy_checkpoint(name="my_checkpoint")

    result: CheckpointResult = checkpoint.run()
    validation_result_identifier: ValidationResultIdentifier = (
        result.list_validation_result_identifiers()[0]
    )
    expectation_validation_result: ExpectationValidationResult | dict = result.run_results[
        validation_result_identifier
    ]["validation_result"]
    for result in expectation_validation_result.results:
        for rendered_content in result.rendered_content:
            assert isinstance(rendered_content, RenderedAtomicContent)


@pytest.mark.filesystem
@pytest.mark.slow  # 1.22s
def test_newstyle_checkpoint_result_validations_include_rendered_content_data_context_variable(
    titanic_pandas_data_context_stats_enabled_and_expectation_suite_with_one_expectation,
    common_action_list,
):
    context: FileDataContext = (
        titanic_pandas_data_context_stats_enabled_and_expectation_suite_with_one_expectation
    )

    batch_request: dict = {
        "datasource_name": "my_datasource",
        "data_connector_name": "my_basic_data_connector",
        "data_asset_name": "Titanic_1911",
    }

    context.include_rendered_content.globally = True

    # add checkpoint config
    checkpoint_config: dict = {
        "name": "my_checkpoint",
        "expectation_suite_name": "my_expectation_suite",
        "action_list": common_action_list,
        "validations": [
            {
                "batch_request": batch_request,
            }
        ],
    }

    context.add_checkpoint(**checkpoint_config)
    checkpoint: Checkpoint = context.get_legacy_checkpoint(name="my_checkpoint")

    result: CheckpointResult = checkpoint.run()
    validation_result_identifier: ValidationResultIdentifier = (
        result.list_validation_result_identifiers()[0]
    )
    expectation_validation_result: ExpectationValidationResult | dict = result.run_results[
        validation_result_identifier
    ]["validation_result"]
    for result in expectation_validation_result.results:
        for rendered_content in result.rendered_content:
            assert isinstance(rendered_content, RenderedAtomicContent)


@pytest.mark.filesystem
@pytest.mark.parametrize(
    "checkpoint_config,expected_validation_id",
    [
        pytest.param(
            CheckpointConfig(
                name="my_checkpoint",
                expectation_suite_name="my_expectation_suite",
                action_list=[
                    {
                        "name": "store_validation_result",
                        "action": {
                            "class_name": "StoreValidationResultAction",
                        },
                    },
                ],
                validations=[
                    {
                        "batch_request": {
                            "datasource_name": "my_datasource",
                            "data_connector_name": "my_basic_data_connector",
                            "data_asset_name": "Titanic_1911",
                        },
                    },
                ],
            ),
            None,
            id="no ids",
        ),
        pytest.param(
            CheckpointConfig(
                name="my_checkpoint",
                default_validation_id="7e2bb5c9-cdbe-4c7a-9b2b-97192c55c95b",
                expectation_suite_name="my_expectation_suite",
                batch_request={
                    "datasource_name": "my_datasource",
                    "data_connector_name": "my_basic_data_connector",
                    "data_asset_name": "Titanic_1911",
                },
                action_list=[
                    {
                        "name": "store_validation_result",
                        "action": {
                            "class_name": "StoreValidationResultAction",
                        },
                    },
                ],
                validations=[],
            ),
            "7e2bb5c9-cdbe-4c7a-9b2b-97192c55c95b",
            id="default validation id",
        ),
        pytest.param(
            CheckpointConfig(
                name="my_checkpoint",
                expectation_suite_name="my_expectation_suite",
                action_list=[
                    {
                        "name": "store_validation_result",
                        "action": {
                            "class_name": "StoreValidationResultAction",
                        },
                    },
                ],
                validations=[
                    {
                        "id": "f22601d9-00b7-4d54-beb6-605d87a74e40",
                        "batch_request": {
                            "datasource_name": "my_datasource",
                            "data_connector_name": "my_basic_data_connector",
                            "data_asset_name": "Titanic_1911",
                        },
                    },
                ],
            ),
            "f22601d9-00b7-4d54-beb6-605d87a74e40",
            id="nested validation id",
        ),
        pytest.param(
            CheckpointConfig(
                name="my_checkpoint",
                default_validation_id="7e2bb5c9-cdbe-4c7a-9b2b-97192c55c95b",
                expectation_suite_name="my_expectation_suite",
                action_list=[
                    {
                        "name": "store_validation_result",
                        "action": {
                            "class_name": "StoreValidationResultAction",
                        },
                    },
                ],
                validations=[
                    {
                        "id": "f22601d9-00b7-4d54-beb6-605d87a74e40",
                        "batch_request": {
                            "datasource_name": "my_datasource",
                            "data_connector_name": "my_basic_data_connector",
                            "data_asset_name": "Titanic_1911",
                        },
                    },
                ],
            ),
            "f22601d9-00b7-4d54-beb6-605d87a74e40",
            id="both default and nested validation id",
        ),
    ],
)
def test_checkpoint_run_adds_validation_ids_to_expectation_suite_validation_result_meta(
    titanic_pandas_data_context_stats_enabled_and_expectation_suite_with_one_expectation: FileDataContext,  # noqa: E501
    common_action_list,
    checkpoint_config: CheckpointConfig,
    expected_validation_id: str,
) -> None:
    context: FileDataContext = (
        titanic_pandas_data_context_stats_enabled_and_expectation_suite_with_one_expectation
    )

    checkpoint_config_dict: dict = checkpointConfigSchema.dump(checkpoint_config)
    context.add_checkpoint(**checkpoint_config_dict)
    checkpoint: Checkpoint = context.get_legacy_checkpoint(name="my_checkpoint")

    result: CheckpointResult = checkpoint.run()

    # Always have a single validation result based on the test's parametrization
    validation_result: ExpectationValidationResult | dict = tuple(result.run_results.values())[0][
        "validation_result"
    ]

    actual_validation_id: Optional[str] = validation_result.meta["validation_id"]
    assert expected_validation_id == actual_validation_id


@pytest.fixture()
def _fake_cloud_context_setup(tmp_path, monkeypatch):
    data_dir = tmp_path
    # When setting up a checkpoint, we validate that there is data in the data directory
    # so we create a file.
    data_file = "yellow_tripdata_sample_2019-01.csv"
    data_file_path = (
        pathlib.Path(__file__)
        / ".."
        / ".."
        / "test_sets"
        / "taxi_yellow_tripdata_samples"
        / data_file
    ).resolve()
    shutil.copy(str(data_file_path), data_dir)

    monkeypatch.setenv("GX_CLOUD_BASE_URL", "https://my_cloud_backend.com")
    monkeypatch.setenv("GX_CLOUD_ORGANIZATION_ID", "11111111-1111-1111-1111-123456789012")
    monkeypatch.setenv("GX_CLOUD_ACCESS_TOKEN", "token")

    monkeypatch.setattr(
        gx.data_context.CloudDataContext,
        "retrieve_data_context_config_from_cloud",
        cloud_config.make_retrieve_data_context_config_from_cloud(data_dir),
    )
    monkeypatch.setattr(
        gx.data_context.store.gx_cloud_store_backend.GXCloudStoreBackend,
        "_set",
        cloud_config.store_set,
    )
    monkeypatch.setattr(
        gx.data_context.store.gx_cloud_store_backend.GXCloudStoreBackend,
        "list_keys",
        cloud_config.list_keys,
    )
    yield data_dir, data_file


@pytest.fixture()
def fake_cloud_context_basic(_fake_cloud_context_setup, monkeypatch):
    data_dir, data_file = _fake_cloud_context_setup
    monkeypatch.setattr(
        gx.data_context.store.gx_cloud_store_backend.GXCloudStoreBackend,
        "_get",
        cloud_config.make_store_get(data_file_name=data_file, data_dir=data_dir, with_slack=False),
    )
    context = gx.data_context.CloudDataContext()
    set_context(context)
    yield context


@pytest.fixture()
def fake_cloud_context_with_slack(_fake_cloud_context_setup, monkeypatch):
    data_dir, data_file = _fake_cloud_context_setup
    monkeypatch.setattr(
        gx.data_context.store.gx_cloud_store_backend.GXCloudStoreBackend,
        "_get",
        cloud_config.make_store_get(data_file_name=data_file, data_dir=data_dir, with_slack=True),
    )
    slack_counter = cloud_config.CallCounter()
    monkeypatch.setattr(
        gx.checkpoint.actions,
        "send_slack_notification",
        cloud_config.make_send_slack_notifications(slack_counter),
    )
    context = gx.data_context.CloudDataContext()
    set_context(context)
    yield context, slack_counter


@pytest.mark.cloud
def test_use_validation_url_from_cloud(fake_cloud_context_basic):
    context = fake_cloud_context_basic
    checkpoint_name = "my_checkpoint"
    checkpoint = context.get_legacy_checkpoint(checkpoint_name)
    checkpoint_result = checkpoint.run()
    org_id = os.environ["GX_CLOUD_ORGANIZATION_ID"]
    assert (
        checkpoint_result.validation_result_url
        == f"https://my_cloud_backend.com/{org_id}/?validationResultId=2e13ecc3-eaaa-444b-b30d-2f616f80ae35"
    )


@pytest.mark.cloud
def test_use_validation_url_from_cloud_with_slack(fake_cloud_context_with_slack):
    context, slack_counter = fake_cloud_context_with_slack
    checkpoint_name = "my_checkpoint"
    checkpoint = context.get_legacy_checkpoint(checkpoint_name)
    checkpoint.run()
    assert slack_counter.count == 5


### SparkDF Tests
@pytest.mark.spark
def test_running_spark_checkpoint(
    context_with_single_csv_spark_and_suite,
    common_action_list,
    spark_df_taxi_data_schema,
):
    context = context_with_single_csv_spark_and_suite
    single_batch_batch_request: BatchRequest = BatchRequest(
        datasource_name="my_datasource",
        data_connector_name="configured_data_connector_multi_batch_asset",
        data_asset_name="yellow_tripdata_2020",
        batch_spec_passthrough={
            "reader_options": {
                "header": True,
            }
        },
    )
    checkpoint_config: dict = {
        "name": "my_checkpoint",
        "expectation_suite_name": "my_expectation_suite",
        "action_list": common_action_list,
        "validations": [
            {
                "batch_request": single_batch_batch_request,
            }
        ],
    }
    checkpoint = context.add_checkpoint(**checkpoint_config)
    results = checkpoint.run()
    assert results.success is True


@pytest.mark.spark
def test_run_spark_checkpoint_with_schema(
    context_with_single_csv_spark_and_suite,
    common_action_list,
    spark_df_taxi_data_schema,
):
    context = context_with_single_csv_spark_and_suite
    single_batch_batch_request: BatchRequest = BatchRequest(
        datasource_name="my_datasource",
        data_connector_name="configured_data_connector_multi_batch_asset",
        data_asset_name="yellow_tripdata_2020",
        batch_spec_passthrough={
            "reader_options": {
                "header": True,
                "schema": spark_df_taxi_data_schema,
            }
        },
    )
    checkpoint_config: dict = {
        "name": "my_checkpoint",
        "expectation_suite_name": "my_expectation_suite",
        "action_list": common_action_list,
        "validations": [
            {
                "batch_request": single_batch_batch_request,
            }
        ],
    }
    checkpoint = context.add_checkpoint(**checkpoint_config)
    results = checkpoint.run()

    assert results.success is True


@pytest.mark.unit
def test_checkpoint_conflicting_validator_and_validation_args_raises_error(
    validator_with_mock_execution_engine,
    mocker: MockerFixture,
):
    context = mocker.MagicMock()
    validator = validator_with_mock_execution_engine
    validations = [
        {
            "batch_request": {
                "datasource_name": "my_datasource",
                "data_asset_name": "my_asset",
            }
        }
    ]

    with pytest.raises(gx_exceptions.CheckpointError) as e:
        _ = Checkpoint(
            name="my_checkpoint",
            data_context=context,
            validator=validator,
            validations=validations,
        )

    assert "cannot be called with a validator and contain a batch_request" in str(e.value)


# Marking this as "big" instead of "unit" since it fails intermittently due to the timeout. We've seen it take over 7s.  # noqa: E501
@pytest.mark.big
def test_context_checkpoint_crud_conflicting_validator_and_validation_args_raises_error(
    ephemeral_context_with_defaults,
    validator_with_mock_execution_engine,
):
    context = ephemeral_context_with_defaults
    validator = validator_with_mock_execution_engine
    validations = [
        {
            "batch_request": {
                "datasource_name": "my_datasource",
                "data_asset_name": "my_asset",
            }
        }
    ]

    with pytest.raises(ValueError) as e:
        _ = context.add_checkpoint(
            name="my_checkpoint",
            validator=validator,
            validations=validations,
        )

    assert "either a validator or validations list" in str(e.value)


@pytest.mark.unit
def test_get_substituted_batch_request_with_no_substituted_config():
    runtime_batch_request = {
        "datasource_name": "my_datasource",
        "data_connector_name": "my_basic_data_connector",
        "data_asset_name": "my_asset_name",
        "runtime_parameters": {},
        "batch_identifiers": {},
    }

    batch_request = get_substituted_batch_request({"batch_request": runtime_batch_request}, None)

    assert batch_request == RuntimeBatchRequest(**runtime_batch_request)


@pytest.mark.unit
def test_get_substituted_batch_request_with_substituted_config():
    validation_batch_request = {
        "datasource_name": "my_datasource",
        "data_connector_name": "my_basic_data_connector",
        "data_asset_name": "my_asset_name",
    }
    runtime_batch_request = {
        "data_connector_name": "my_basic_data_connector",
        "data_asset_name": "my_asset_name",
        "runtime_parameters": {"query": "SELECT * FROM whatever"},
        "batch_identifiers": {"default_identifier_name": "my_identifier"},
    }

    batch_request = get_substituted_batch_request(
        {"batch_request": runtime_batch_request}, validation_batch_request
    )

    assert batch_request == RuntimeBatchRequest(
        **{
            "datasource_name": "my_datasource",
            "data_connector_name": "my_basic_data_connector",
            "data_asset_name": "my_asset_name",
            "runtime_parameters": {"query": "SELECT * FROM whatever"},
            "batch_identifiers": {"default_identifier_name": "my_identifier"},
        }
    )


@pytest.mark.unit
def test_get_substituted_batch_request_with_clashing_values():
    validation_batch_request = {
        "datasource_name": "my_datasource",
        "data_connector_name": "my_basic_data_connector",
        "data_asset_name": "my_asset_name",
    }
    runtime_batch_request = {
        "datasource_name": "your_datasource",
        "data_connector_name": "my_basic_data_connector",
        "data_asset_name": "my_asset_name",
    }

    with pytest.raises(gx_exceptions.CheckpointError):
        get_substituted_batch_request(
            {"batch_request": runtime_batch_request}, validation_batch_request
        )


@pytest.mark.big
def test_checkpoint_run_with_runtime_overrides(
    ephemeral_context_with_defaults: EphemeralDataContext,
):
    # This test is regarding incident #51-08-28-2023
    # Unpacking dictionaries with overlapping keys raises when using `dict`: https://treyhunner.com/2016/02/how-to-merge-dictionaries-in-python/
    # The function in question: get_substituted_batch_request

    context = ephemeral_context_with_defaults

    ds = context.sources.add_or_update_pandas("incident_test")
    my_asset = ds.add_dataframe_asset("inmemory_df")
    df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})
    batch_request = my_asset.build_batch_request(dataframe=df)
    context.suites.add(
        ExpectationSuite(
            name="my_expectation_suite",
            expectations=[
                {
                    "expectation_type": "expect_column_min_to_be_between",
                    "kwargs": {
                        "column": "col1",
                        "min_value": 0.1,
                        "max_value": 10.0,
                    },
                }
            ],
        )
    )
    checkpoint = context.add_or_update_checkpoint(
        name="my_checkpoint",
        validations=[
            {
                "expectation_suite_name": "my_expectation_suite",
                "batch_request": {
                    "datasource_name": ds.name,
                    "data_asset_name": my_asset.name,
                },
            }
        ],
    )

    result = checkpoint.run(batch_request=batch_request)
    assert result.success


@pytest.mark.unit
def test_checkpoint_run_raises_error_if_not_associated_with_context():
    checkpoint = Checkpoint(name="my_checkpoint")
    with pytest.raises(ValueError):
        checkpoint.run()
