import copy
from typing import Any, Dict, List, Optional, Union

import great_expectations.exceptions as ge_exceptions
from great_expectations import DataContext
from great_expectations.core import IDDict
from great_expectations.core.batch import BatchDefinition, BatchRequest
from great_expectations.profiler.domain_builder.domain import Domain
from great_expectations.profiler.parameter_builder.parameter_container import (
    DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
    DOMAIN_KWARGS_PARAMETER_NAME,
    VARIABLES_KEY,
    ParameterContainer,
    ParameterNode,
    validate_fully_qualified_parameter_name,
)


def get_parameter_value(
    fully_qualified_parameter_name: str,
    domain: Union[Domain, List[Domain]],
    variables: Optional[ParameterContainer] = None,
    parameters: Optional[Dict[str, ParameterContainer]] = None,
) -> Optional[Any]:
    """
    Get the parameter value from the current rule state using the fully-qualified parameter name.
    A fully-qualified parameter name must be a dot-delimited string, or the name of a parameter (without the dots).
    Args
        :param fully_qualified_parameter_name: str -- A dot-separated string key starting with $ for fetching parameters
        :param domain: Union[Domain, List[Domain]] -- current Domain (or List[Domain]) of interest
        :param variables
        :param parameters
    :return: value
    """
    validate_fully_qualified_parameter_name(
        fully_qualified_parameter_name=fully_qualified_parameter_name
    )

    # Using "__getitem__" (bracket) notation instead of "__getattr__" (dot) notation in order to insure the
    # compatibility of field names (e.g., "domain_kwargs") with user-facing syntax (as governed by the value of the
    # DOMAIN_KWARGS_PARAMETER_NAME constant, which may change, requiring the same change to the field name).
    if fully_qualified_parameter_name == DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME:
        if domain:
            # Supports the "$domain.domain_kwargs" style syntax.
            return domain[DOMAIN_KWARGS_PARAMETER_NAME]
        return None

    if fully_qualified_parameter_name.startswith(
        DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME
    ):
        if domain and domain[DOMAIN_KWARGS_PARAMETER_NAME]:
            # Supports the "$domain.domain_kwargs.column" style syntax.
            return domain[DOMAIN_KWARGS_PARAMETER_NAME].get(
                fully_qualified_parameter_name[
                    (len(DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME) + 1) :
                ]
            )
        return None

    actual_parameters: Optional[Dict[str, ParameterContainer]] = copy.deepcopy(
        parameters
    )
    domain_ids: Optional[List[str]]
    domain_cursor: Domain
    if fully_qualified_parameter_name.startswith(VARIABLES_KEY):
        domain_id: str = IDDict().to_id()
        actual_parameters[domain_id] = copy.deepcopy(variables)
        domain_ids = [domain_id]
        fully_qualified_parameter_name = fully_qualified_parameter_name[
            len(VARIABLES_KEY) :
        ]
    else:
        if isinstance(domain, Domain):
            domain = [domain]
        elif not (
            isinstance(domain, list)
            and len(domain) > 0
            and all([isinstance(domain_cursor, Domain) for domain_cursor in domain])
        ):
            raise ValueError(
                "Either a single Domain object or a non-empty list of Domain objects is required."
            )
        domain_ids = [domain_cursor.id for domain_cursor in domain]
        fully_qualified_parameter_name = fully_qualified_parameter_name[1:]

    fully_qualified_parameter_name_as_list: List[
        str
    ] = fully_qualified_parameter_name.split(".")

    if len(fully_qualified_parameter_name_as_list) == 0:
        return None

    return _get_parameter_value_multiple_domain_scope(
        fully_qualified_parameter_name=fully_qualified_parameter_name,
        fully_qualified_parameter_name_as_list=fully_qualified_parameter_name_as_list,
        parameters=actual_parameters,
        domain_ids=domain_ids,
    )


def _get_parameter_value_multiple_domain_scope(
    fully_qualified_parameter_name: str,
    fully_qualified_parameter_name_as_list: List[str],
    parameters: Dict[str, ParameterContainer],
    domain_ids: List[str],
) -> Optional[Any]:
    domain_id: str
    parameter_container: ParameterContainer
    exception_messages: List[str] = []
    parameter_value: Optional[Any] = None
    for domain_id in domain_ids:
        parameter_container = parameters[domain_id]
        try:
            parameter_value = _get_parameter_value_one_domain_scope(
                fully_qualified_parameter_name=fully_qualified_parameter_name,
                fully_qualified_parameter_name_as_list=fully_qualified_parameter_name_as_list,
                parameters=parameter_container,
            )
            # In the present implementation, the first non-NULL parameter value found is returned.  In the future,
            # parameter name across domains will need to be disambiguated (e.g., bu using a domain-specific scoping).
            if parameter_value is not None:
                return parameter_value
        except KeyError as e:
            exception_messages.append(
                f"""Unable to find value for parameter name "{fully_qualified_parameter_name}" for domain_id \
"{domain_id}": "{e}" occurred.
"""
            )
    if len(exception_messages) > 0:
        raise ge_exceptions.ProfilerExecutionError(message=";".join(exception_messages))
    return parameter_value


def _get_parameter_value_one_domain_scope(
    fully_qualified_parameter_name: str,
    fully_qualified_parameter_name_as_list: List[str],
    parameters: ParameterContainer,
) -> Optional[Any]:
    parameter_node: Optional[ParameterNode] = parameters.get_parameter_node(
        parameter_name_root=fully_qualified_parameter_name_as_list[0]
    )
    if parameter_node is None:
        return None

    parameter_name_part: Optional[str] = None
    try:
        for parameter_name_part in fully_qualified_parameter_name_as_list:
            if (
                parameter_node.attributes
                and parameter_name_part in parameter_node.attributes
            ):
                return parameter_node.attributes[parameter_name_part]

            parameter_node = parameter_node.descendants[parameter_name_part]
    except KeyError:
        raise KeyError(
            f"""Unable to find value for parameter name "{fully_qualified_parameter_name}": Part \
"{parameter_name_part}" does not exist in fully-qualified parameter name.
"""
        )


def get_batch_ids(data_context: DataContext, batch_request: BatchRequest) -> List[str]:
    datasource_name: str = batch_request.datasource_name
    batch_definitions: List[BatchDefinition] = data_context.get_datasource(
        datasource_name=datasource_name
    ).get_batch_definition_list_from_batch_request(batch_request=batch_request)
    return [batch_definition.id for batch_definition in batch_definitions]
