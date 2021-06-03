from typing import Dict, List, Optional, Union

from great_expectations import DataContext
from great_expectations.core.batch import BatchDefinition, BatchRequest
from great_expectations.rule_based_profiler.domain_builder.domain import Domain
from great_expectations.rule_based_profiler.parameter_builder.parameter_container import (
    ParameterContainer,
    get_parameter_value,
)


def get_batch_ids_from_batch_request(
    data_context: DataContext,
    batch_request: BatchRequest,
) -> List[str]:
    datasource_name: str = batch_request.datasource_name
    batch_definitions: List[BatchDefinition] = data_context.get_datasource(
        datasource_name=datasource_name
    ).get_batch_definition_list_from_batch_request(batch_request=batch_request)
    return [batch_definition.id for batch_definition in batch_definitions]


def get_metric_kwargs(
    domain: Domain,
    *,
    metric_kwargs: Optional[Union[str, dict]] = None,
    variables: Optional[ParameterContainer] = None,
    parameters: Optional[Dict[str, ParameterContainer]] = None,
) -> Optional[Union[str, dict]]:
    if (
        metric_kwargs is not None
        and isinstance(metric_kwargs, str)
        and metric_kwargs.startswith("$")
    ):
        metric_kwargs = get_parameter_value(
            fully_qualified_parameter_name=metric_kwargs,
            domain=domain,
            variables=variables,
            parameters=parameters,
        )

    return metric_kwargs
