from __future__ import annotations

import copy
import logging
from typing import List, Optional, Union

import great_expectations.exceptions as gx_exceptions
from great_expectations.core.batch import (
    BatchRequest,
    BatchRequestBase,
    RuntimeBatchRequest,
    get_batch_request_as_dict,
    materialize_batch_request,
)
from great_expectations.core.util import nested_update
from great_expectations.data_context.types.base import CheckpointValidationDefinition
from great_expectations.types import DictDot

logger = logging.getLogger(__name__)


def get_substituted_validation_dict(
    substituted_runtime_config: dict, validation_dict: CheckpointValidationDefinition
) -> dict:
    substituted_validation_dict = {
        "batch_request": get_substituted_batch_request(
            substituted_runtime_config=substituted_runtime_config,
            validation_batch_request=validation_dict.get("batch_request"),
        ),
        "expectation_suite_name": validation_dict.get("expectation_suite_name")
        or substituted_runtime_config.get("expectation_suite_name"),
        "expectation_suite_id": validation_dict.get("expectation_suite_id")
        or substituted_runtime_config.get("expectation_suite_id"),
        "action_list": get_updated_action_list(
            base_action_list=substituted_runtime_config.get("action_list", []),
            other_action_list=validation_dict.get("action_list", {}),
        ),
        "evaluation_parameters": nested_update(
            substituted_runtime_config.get("evaluation_parameters") or {},
            validation_dict.get("evaluation_parameters", {}),
            dedup=True,
        ),
        "runtime_configuration": nested_update(
            substituted_runtime_config.get("runtime_configuration") or {},
            validation_dict.get("runtime_configuration", {}),
            dedup=True,
        ),
        "include_rendered_content": validation_dict.get("include_rendered_content")
        or substituted_runtime_config.get("include_rendered_content")
        or None,
    }

    for attr in ("name", "id"):
        if validation_dict.get(attr) is not None:
            substituted_validation_dict[attr] = validation_dict[attr]

    return substituted_validation_dict


# TODO: <Alex>A common utility function should be factored out from DataContext.get_batch_list() for any purpose.</Alex>  # noqa: E501
def get_substituted_batch_request(
    substituted_runtime_config: dict,
    validation_batch_request: Optional[Union[BatchRequestBase, dict]] = None,
) -> Optional[Union[BatchRequest, RuntimeBatchRequest]]:
    substituted_runtime_batch_request = substituted_runtime_config.get("batch_request")

    if substituted_runtime_batch_request is None and validation_batch_request is None:
        return None

    if substituted_runtime_batch_request is None:
        substituted_runtime_batch_request = {}

    if validation_batch_request is None:
        validation_batch_request = {}

    validation_batch_request = get_batch_request_as_dict(batch_request=validation_batch_request)
    substituted_runtime_batch_request = get_batch_request_as_dict(
        batch_request=substituted_runtime_batch_request
    )

    for key, value in validation_batch_request.items():
        substituted_value = substituted_runtime_batch_request.get(key)
        if value is not None and substituted_value is not None and value != substituted_value:
            raise gx_exceptions.CheckpointError(  # noqa: TRY003
                f'BatchRequest attribute "{key}" was provided with different values'
            )

    effective_batch_request: dict = {
        **validation_batch_request,
        **substituted_runtime_batch_request,
    }

    return materialize_batch_request(batch_request=effective_batch_request)  # type: ignore[return-value] # see materialize_batch_request


def substitute_runtime_config(  # noqa: C901 - 11
    source_config: dict, runtime_kwargs: dict
) -> dict:
    if not (runtime_kwargs and any(runtime_kwargs.values())):
        return source_config

    dest_config: dict = copy.deepcopy(source_config)

    # replace
    if runtime_kwargs.get("expectation_suite_name") is not None:
        dest_config["expectation_suite_name"] = runtime_kwargs["expectation_suite_name"]
    if runtime_kwargs.get("expectation_suite_id") is not None:
        dest_config["expectation_suite_id"] = runtime_kwargs["expectation_suite_id"]
    # update
    if runtime_kwargs.get("batch_request") is not None:
        batch_request = dest_config.get("batch_request") or {}
        batch_request_from_runtime_kwargs = runtime_kwargs["batch_request"]
        batch_request_from_runtime_kwargs = get_batch_request_as_dict(
            batch_request=batch_request_from_runtime_kwargs
        )

        # If "batch_request" has Fluent Datasource form, "options" must be overwritten for DataAsset type compatibility.  # noqa: E501
        updated_batch_request = copy.deepcopy(batch_request)
        if batch_request_from_runtime_kwargs and "options" in updated_batch_request:
            updated_batch_request["options"] = {}

        updated_batch_request = nested_update(
            updated_batch_request,
            batch_request_from_runtime_kwargs,
            dedup=True,
        )
        dest_config["batch_request"] = updated_batch_request
    if runtime_kwargs.get("action_list") is not None:
        action_list = dest_config.get("action_list") or []
        dest_config["action_list"] = get_updated_action_list(
            base_action_list=action_list,
            other_action_list=runtime_kwargs["action_list"],
        )
    if runtime_kwargs.get("evaluation_parameters") is not None:
        evaluation_parameters = dest_config.get("evaluation_parameters") or {}
        updated_evaluation_parameters = nested_update(
            evaluation_parameters,
            runtime_kwargs["evaluation_parameters"],
            dedup=True,
        )
        dest_config["evaluation_parameters"] = updated_evaluation_parameters
    if runtime_kwargs.get("runtime_configuration") is not None:
        runtime_configuration = dest_config.get("runtime_configuration") or {}
        updated_runtime_configuration = nested_update(
            runtime_configuration,
            runtime_kwargs["runtime_configuration"],
            dedup=True,
        )
        dest_config["runtime_configuration"] = updated_runtime_configuration
    if runtime_kwargs.get("validations") is not None:
        validations = dest_config.get("validations") or []
        existing_validations = source_config.get("validations") or []
        validations.extend(
            filter(
                lambda v: v not in existing_validations,
                runtime_kwargs["validations"],
            )
        )
        dest_config["validations"] = validations
    if runtime_kwargs.get("profilers") is not None:
        profilers = dest_config.get("profilers") or []
        existing_profilers = source_config.get("profilers") or []
        profilers.extend(filter(lambda v: v not in existing_profilers, runtime_kwargs["profilers"]))
        dest_config["profilers"] = profilers

    return dest_config


def get_updated_action_list(base_action_list: list, other_action_list: list) -> List[dict]:
    if base_action_list is None:
        base_action_list = []

    base_action_list_dict = {action["name"]: action for action in base_action_list}

    for other_action in other_action_list:
        other_action_name = other_action["name"]
        if other_action_name in base_action_list_dict:
            if other_action["action"]:
                nested_update(
                    base_action_list_dict[other_action_name],
                    other_action,
                    dedup=True,
                )
            else:
                base_action_list_dict.pop(other_action_name)
        else:
            base_action_list_dict[other_action_name] = other_action

    for other_action in other_action_list:
        other_action_name = other_action["name"]
        if other_action_name in base_action_list_dict:
            if not other_action["action"]:
                base_action_list_dict.pop(other_action_name)

    return list(base_action_list_dict.values())


def does_batch_request_in_validations_contain_batch_data(
    validations: List[CheckpointValidationDefinition],
) -> bool:
    for val in validations:
        if (
            "batch_request" in val
            and val["batch_request"] is not None
            and isinstance(val["batch_request"], (dict, DictDot))
            and val["batch_request"].get("runtime_parameters") is not None
            and val["batch_request"]["runtime_parameters"].get("batch_data") is not None
        ):
            return True

    return False


def get_validations_with_batch_request_as_dict(
    validations: list[dict] | list[CheckpointValidationDefinition] | None = None,
) -> list[CheckpointValidationDefinition]:
    if not validations:
        return []

    for value in validations:
        if "batch_request" in value:
            value["batch_request"] = get_batch_request_as_dict(batch_request=value["batch_request"])

    return convert_validations_list_to_checkpoint_validation_definitions(validations)


def convert_validations_list_to_checkpoint_validation_definitions(
    validations: list[dict] | list[CheckpointValidationDefinition] | None,
) -> list[CheckpointValidationDefinition]:
    # We accept both dicts and rich config types but all internal usage should use the latter
    if not validations:
        return []
    return [
        CheckpointValidationDefinition(**validation) if isinstance(validation, dict) else validation
        for validation in validations
    ]


def validate_validation_dict(validation_dict: dict, batch_request_required: bool = True) -> None:
    if batch_request_required and validation_dict.get("batch_request") is None:
        raise gx_exceptions.CheckpointError("validation batch_request cannot be None")  # noqa: TRY003

    if not validation_dict.get("expectation_suite_name"):
        raise gx_exceptions.CheckpointError("validation expectation_suite_name must be specified")  # noqa: TRY003
