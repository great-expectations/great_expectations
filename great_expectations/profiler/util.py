from typing import Any, Dict, List, Optional

import great_expectations.exceptions as ge_exceptions
from great_expectations import DataContext
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
    domain: Domain,
    variables: Optional[ParameterContainer] = None,
    parameters: Optional[Dict[str, ParameterContainer]] = None,
) -> Optional[Any]:
    """
    Get the parameter value from the current rule state using the fully-qualified parameter name.
    A fully-qualified parameter name must be a dot-delimited string, or the name of a parameter (without the dots).
    Args
        :param fully_qualified_parameter_name: str -- A dot-separated string key starting with $ for fetching parameters
        :param domain: Domain -- current Domain of interest
        :param variables
        :param parameters
    :return: value
    """
    validate_fully_qualified_parameter_name(
        fully_qualified_parameter_name=fully_qualified_parameter_name
    )

    if fully_qualified_parameter_name == DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME:
        # Using "__getitem__" (bracket) notation instead of "__getattr__" (dot) notation in order to insure the
        # compatibility of field names (e.g., "domain_kwargs") with user-facing syntax (as governed by the value of
        # the DOMAIN_KWARGS_PARAMETER_NAME constant, which may change, requiring the same change to the field name).
        return domain[DOMAIN_KWARGS_PARAMETER_NAME]

    fully_qualified_parameter_name_references_variable: bool = False
    if fully_qualified_parameter_name.startswith(VARIABLES_KEY):
        fully_qualified_parameter_name = fully_qualified_parameter_name[
            len(VARIABLES_KEY) :
        ]
        fully_qualified_parameter_name_references_variable = True
    else:
        fully_qualified_parameter_name = fully_qualified_parameter_name[1:]

    fully_qualified_parameter_as_list: List[str] = fully_qualified_parameter_name.split(
        "."
    )

    if len(fully_qualified_parameter_as_list) == 0:
        return None

    parameter_container: ParameterContainer
    if fully_qualified_parameter_name_references_variable:
        parameter_container = variables
    else:
        parameter_container = parameters[domain.id]

    parameter_node: Optional[ParameterNode] = parameter_container.get_parameter_node(
        parameter_name_root=fully_qualified_parameter_as_list[0]
    )

    parameter_name_part: Optional[str] = None
    try:
        for parameter_name_part in fully_qualified_parameter_as_list:
            if (
                parameter_node.attributes
                and parameter_name_part in parameter_node.attributes
            ):
                return parameter_node.attributes[parameter_name_part]

            parameter_node = parameter_node.descendants[parameter_name_part]
    except KeyError:
        raise ge_exceptions.ProfilerExecutionError(
            message=f'Unable to find value for parameter name "{fully_qualified_parameter_name}": key "{parameter_name_part}" was missing.'
        )


def get_batch_ids(data_context: DataContext, batch_request: BatchRequest) -> List[str]:
    datasource_name: str = batch_request.datasource_name
    batch_definitions: List[BatchDefinition] = data_context.get_datasource(
        datasource_name=datasource_name
    ).get_batch_definition_list_from_batch_request(batch_request=batch_request)
    return [batch_definition.id for batch_definition in batch_definitions]
