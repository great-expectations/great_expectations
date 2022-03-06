import datetime
import uuid
from typing import List, Optional, Union

import great_expectations.exceptions as ge_exceptions
from great_expectations.checkpoint import Checkpoint, LegacyCheckpoint, SimpleCheckpoint
from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
from great_expectations.checkpoint.util import (
    batch_request_in_validations_contains_batch_data,
    get_validations_with_batch_request_as_dict,
)
from great_expectations.core.batch import (
    BatchRequest,
    RuntimeBatchRequest,
    batch_request_contains_batch_data,
    get_batch_request_as_dict,
)
from great_expectations.data_context.store import CheckpointStore
from great_expectations.data_context.types.base import CheckpointConfig
from great_expectations.data_context.types.refs import GeCloudIdAwareRef
from great_expectations.data_context.types.resource_identifiers import (
    ConfigurationIdentifier,
    GeCloudIdentifier,
)
from great_expectations.data_context.util import instantiate_class_from_config
from great_expectations.util import (
    deep_filter_properties_iterable,
    filter_properties_dict,
)


def add_checkpoint(
    data_context: "DataContext",  # noqa: F821
    checkpoint_store: CheckpointStore,
    checkpoint_store_name: str,
    ge_cloud_mode: bool,
    name: str,
    config_version: Optional[Union[int, float]] = None,
    template_name: Optional[str] = None,
    module_name: Optional[str] = None,
    class_name: Optional[str] = None,
    run_name_template: Optional[str] = None,
    expectation_suite_name: Optional[str] = None,
    batch_request: Optional[dict] = None,
    action_list: Optional[List[dict]] = None,
    evaluation_parameters: Optional[dict] = None,
    runtime_configuration: Optional[dict] = None,
    validations: Optional[List[dict]] = None,
    profilers: Optional[List[dict]] = None,
    # Next two fields are for LegacyCheckpoint configuration
    validation_operator_name: Optional[str] = None,
    batches: Optional[List[dict]] = None,
    # the following four arguments are used by SimpleCheckpoint
    site_names: Optional[Union[str, List[str]]] = None,
    slack_webhook: Optional[str] = None,
    notify_on: Optional[str] = None,
    notify_with: Optional[Union[str, List[str]]] = None,
    ge_cloud_id: Optional[str] = None,
    expectation_suite_ge_cloud_id: Optional[str] = None,
) -> Union[Checkpoint, LegacyCheckpoint]:
    checkpoint_config: Union[CheckpointConfig, dict]

    # These checks protect against typed objects (BatchRequest and/or RuntimeBatchRequest) encountered in arguments.
    batch_request = get_batch_request_as_dict(batch_request=batch_request)
    validations = get_validations_with_batch_request_as_dict(validations=validations)

    # DataFrames shouldn't be saved to CheckpointStore
    if batch_request_contains_batch_data(batch_request=batch_request):
        raise ge_exceptions.InvalidConfigError(
            f'batch_data found in batch_request cannot be saved to CheckpointStore "{checkpoint_store_name}"'
        )

    if batch_request_in_validations_contains_batch_data(validations=validations):
        raise ge_exceptions.InvalidConfigError(
            f'batch_data found in validations cannot be saved to CheckpointStore "{checkpoint_store_name}"'
        )

    checkpoint_config = {
        "name": name,
        "config_version": config_version,
        "template_name": template_name,
        "module_name": module_name,
        "class_name": class_name,
        "run_name_template": run_name_template,
        "expectation_suite_name": expectation_suite_name,
        "batch_request": batch_request,
        "action_list": action_list,
        "evaluation_parameters": evaluation_parameters,
        "runtime_configuration": runtime_configuration,
        "validations": validations,
        "profilers": profilers,
        # Next two fields are for LegacyCheckpoint configuration
        "validation_operator_name": validation_operator_name,
        "batches": batches,
        # the following four keys are used by SimpleCheckpoint
        "site_names": site_names,
        "slack_webhook": slack_webhook,
        "notify_on": notify_on,
        "notify_with": notify_with,
        "ge_cloud_id": ge_cloud_id,
        "expectation_suite_ge_cloud_id": expectation_suite_ge_cloud_id,
    }

    checkpoint_config = deep_filter_properties_iterable(
        properties=checkpoint_config,
        clean_falsy=True,
    )

    new_checkpoint: Checkpoint = instantiate_class_from_config(
        config=checkpoint_config,
        runtime_environment={
            "data_context": data_context,
        },
        config_defaults={
            "module_name": "great_expectations.checkpoint",
        },
    )

    data_context.checkpoint_store.add_checkpoint(new_checkpoint, name, ge_cloud_id)

    return new_checkpoint


def run_checkpoint(
    data_context: "DataContext",  # noqa: F821
    checkpoint_store: CheckpointStore,
    checkpoint_name: Optional[str] = None,
    template_name: Optional[str] = None,
    run_name_template: Optional[str] = None,
    expectation_suite_name: Optional[str] = None,
    batch_request: Optional[Union[BatchRequest, RuntimeBatchRequest, dict]] = None,
    action_list: Optional[List[dict]] = None,
    evaluation_parameters: Optional[dict] = None,
    runtime_configuration: Optional[dict] = None,
    validations: Optional[List[dict]] = None,
    profilers: Optional[List[dict]] = None,
    run_id: Optional[Union[str, int, float]] = None,
    run_name: Optional[str] = None,
    run_time: Optional[datetime.datetime] = None,
    result_format: Optional[str] = None,
    ge_cloud_id: Optional[str] = None,
    expectation_suite_ge_cloud_id: Optional[str] = None,
    **kwargs,
) -> CheckpointResult:
    """
    Validate against a pre-defined Checkpoint. (Experimental)
    Args:
        data_context: DataContext for Checkpoint class instantiation purposes
        checkpoint_store: CheckpointStore for managing Checkpoint configurations
        checkpoint_name: The name of a Checkpoint defined via the CLI or by manually creating a yml file
        template_name: The name of a Checkpoint template to retrieve from the CheckpointStore
        run_name_template: The template to use for run_name
        expectation_suite_name: Expectation suite to be used by Checkpoint run
        batch_request: Batch request to be used by Checkpoint run
        action_list: List of actions to be performed by the Checkpoint
        evaluation_parameters: $parameter_name syntax references to be evaluated at runtime
        runtime_configuration: Runtime configuration override parameters
        validations: Validations to be performed by the Checkpoint run
        profilers: Profilers to be used by the Checkpoint run
        run_id: The run_id for the validation; if None, a default value will be used
        run_name: The run_name for the validation; if None, a default value will be used
        run_time: The date/time of the run
        result_format: One of several supported formatting directives for expectation validation results
        ge_cloud_id: Great Expectations Cloud id for the checkpoint
        expectation_suite_ge_cloud_id: Great Expectations Cloud id for the expectation suite
        **kwargs: Additional kwargs to pass to the validation operator

    Returns:
        CheckpointResult
    """
    checkpoint: Checkpoint = data_context.get_checkpoint(
        name=checkpoint_name,
        ge_cloud_id=ge_cloud_id,
    )
    checkpoint_config_from_store: CheckpointConfig = checkpoint.get_config()

    if (
        "runtime_configuration" in checkpoint_config_from_store
        and checkpoint_config_from_store.runtime_configuration
        and "result_format" in checkpoint_config_from_store.runtime_configuration
    ):
        result_format = (
            result_format
            or checkpoint_config_from_store.runtime_configuration.get("result_format")
        )

    if result_format is None:
        result_format = {"result_format": "SUMMARY"}

    batch_request = get_batch_request_as_dict(batch_request=batch_request)
    validations = get_validations_with_batch_request_as_dict(validations=validations)

    checkpoint_config_from_call_args: dict = {
        "template_name": template_name,
        "run_name_template": run_name_template,
        "expectation_suite_name": expectation_suite_name,
        "batch_request": batch_request,
        "action_list": action_list,
        "evaluation_parameters": evaluation_parameters,
        "runtime_configuration": runtime_configuration,
        "validations": validations,
        "profilers": profilers,
        "run_id": run_id,
        "run_name": run_name,
        "run_time": run_time,
        "result_format": result_format,
        "expectation_suite_ge_cloud_id": expectation_suite_ge_cloud_id,
    }

    checkpoint_config: dict = {
        key: value
        for key, value in checkpoint_config_from_store.items()
        if key in checkpoint_config_from_call_args
    }
    checkpoint_config.update(checkpoint_config_from_call_args)

    checkpoint_run_arguments: dict = dict(**checkpoint_config, **kwargs)
    filter_properties_dict(
        properties=checkpoint_run_arguments,
        clean_falsy=True,
        inplace=True,
    )

    return checkpoint.run(**checkpoint_run_arguments)
