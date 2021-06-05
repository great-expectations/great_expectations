from typing import Any, Dict, List, Optional, Tuple, Union

import great_expectations.exceptions as ge_exceptions
from great_expectations import DataContext
from great_expectations.core.batch import BatchDefinition, BatchRequest
from great_expectations.rule_based_profiler.domain_builder.domain import Domain
from great_expectations.rule_based_profiler.parameter_builder.parameter_container import (
    ParameterContainer,
    get_parameter_value,
)
from great_expectations.util import filter_properties_dict
from great_expectations.validator.validator import Validator


def get_batch_ids_from_validator(validator: Validator) -> List[str]:
    return validator.loaded_batch_ids


def get_batch_ids_from_batch_request(
    data_context: DataContext,
    batch_request: BatchRequest,
) -> List[str]:
    datasource_name: str = batch_request.datasource_name
    batch_definitions: List[BatchDefinition] = data_context.get_datasource(
        datasource_name=datasource_name
    ).get_batch_definition_list_from_batch_request(batch_request=batch_request)
    return [batch_definition.id for batch_definition in batch_definitions]


def get_parameter_argument_and_validate_return_type(
    domain: Domain,
    *,
    argument: Optional[Union[Any, str]] = None,
    expected_type: Optional[Union[type, tuple]] = None,
    variables: Optional[ParameterContainer] = None,
    parameters: Optional[Dict[str, ParameterContainer]] = None,
) -> Optional[Any]:
    argument = get_parameter_argument(
        domain=domain,
        argument=argument,
        variables=variables,
        parameters=parameters,
    )
    if expected_type is not None:
        if not isinstance(argument, expected_type):
            raise ge_exceptions.ProfilerExecutionError(
                message=f"""Argument "{argument}" in must be {str(expected_type)}-valued \
(value of type "{str(type(argument))}" was encountered).
"""
            )

    return argument


def get_parameter_argument(
    domain: Domain,
    *,
    argument: Optional[Union[Any, str]] = None,
    variables: Optional[ParameterContainer] = None,
    parameters: Optional[Dict[str, ParameterContainer]] = None,
) -> Optional[Any]:
    if isinstance(argument, dict):
        for key, value in argument.items():
            argument[key] = get_parameter_argument(
                domain=domain,
                argument=value,
                variables=variables,
                parameters=parameters,
            )
    elif isinstance(argument, str) and argument.startswith("$"):
        argument = get_parameter_value(
            fully_qualified_parameter_name=argument,
            domain=domain,
            variables=variables,
            parameters=parameters,
        )
        if isinstance(argument, dict):
            for key, value in argument.items():
                argument[key] = get_parameter_argument(
                    domain=domain,
                    argument=value,
                    variables=variables,
                    parameters=parameters,
                )
    if isinstance(argument, dict):
        return filter_properties_dict(properties=argument)
    return argument
