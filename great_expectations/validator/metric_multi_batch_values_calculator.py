from typing import Dict, List, Optional

from great_expectations.core import Domain
from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.rule_based_profiler.helpers.util import sanitize_parameter_name
from great_expectations.rule_based_profiler.parameter_builder import (
    MetricMultiBatchParameterBuilder,
    ParameterBuilder,
)
from great_expectations.rule_based_profiler.parameter_container import (
    ParameterContainer,
    ParameterNode,
    build_parameter_container_for_variables,
    get_parameter_values_for_fully_qualified_parameter_names,
)
from great_expectations.validator.metric_configuration import MetricConfiguration


class MetricMultiBatchValuesCalculator:
    """
    Calculates values for multi-Batch metrics.
    """

    def __init__(
        self,
        validator: "Validator",  # noqa: F821
        metric_configurations: List[MetricConfiguration],
    ) -> None:
        self._metric_configurations = metric_configurations
        self._validator = validator

    def run(
        self,
        runtime_configuration: Optional[dict] = None,
    ) -> Dict[Domain, Dict[str, ParameterNode]]:
        domain_metric_multi_batch_parameter_builders_map: Dict[
            Domain, List[ParameterBuilder]
        ] = self._build_domain_metric_multi_batch_parameter_builders_map()

        metrics_by_domain: Dict[Domain, Dict[str, ParameterNode]] = {}

        domain: Domain
        parameter_builders: List[ParameterBuilder]
        variables: Optional[ParameterContainer]
        parameter_builder: ParameterBuilder
        for (
            domain,
            parameter_builders,
        ) in domain_metric_multi_batch_parameter_builders_map.items():
            parameter_container = ParameterContainer(parameter_nodes=None)
            parameters: Dict[str, ParameterContainer] = {
                domain.id: parameter_container,
            }
            if domain.domain_type == MetricDomainTypes.TABLE:
                variables = build_parameter_container_for_variables(
                    variables_configs={
                        "table": domain.domain_kwargs.get("table"),
                    }
                )
            else:
                variables = None

            for parameter_builder in parameter_builders:
                parameter_builder.build_parameters(
                    domain=domain,
                    variables=variables,
                    parameters=parameters,
                    parameter_computation_impl=None,
                    batch_list=list(self._validator.batches.values()),
                    batch_request=None,
                    recompute_existing_parameter_values=False,
                    runtime_configuration=runtime_configuration,
                )

            parameter_values_for_fully_qualified_parameter_names: Dict[
                str, ParameterNode
            ] = get_parameter_values_for_fully_qualified_parameter_names(
                domain=domain,
                variables=variables,
                parameters=parameters,
            )
            metrics_by_domain[
                domain
            ] = parameter_values_for_fully_qualified_parameter_names

        return metrics_by_domain

    def _build_domain_metric_multi_batch_parameter_builders_map(
        self,
    ) -> Dict[Domain, List[ParameterBuilder]]:
        domain_metric_configurations_map: Dict[
            Domain, List[MetricConfiguration]
        ] = self._build_domain_metric_configurations_map()

        domain: Domain
        metric_configurations: List[MetricConfiguration]
        metric_configuration: MetricConfiguration
        return {
            domain: [
                MetricMultiBatchParameterBuilder(
                    name=sanitize_parameter_name(
                        name=metric_configuration.metric_name,
                        suffix=metric_configuration.metric_value_kwargs_id,
                    ),
                    metric_name=metric_configuration.metric_name,
                    metric_domain_kwargs=metric_configuration.metric_domain_kwargs,
                    metric_value_kwargs=metric_configuration.metric_value_kwargs,
                    single_batch_mode=False,
                    enforce_numeric_metric=False,
                    replace_nan_with_zero=False,
                    reduce_scalar_metric=True,
                    evaluation_parameter_builder_configs=None,
                    data_context=self._validator.data_context,
                )
                for metric_configuration in metric_configurations
            ]
            for domain, metric_configurations in domain_metric_configurations_map.items()
        }

    def _build_domain_metric_configurations_map(
        self,
    ) -> Dict[Domain, List[MetricConfiguration]]:
        domain_metric_configurations_map: Dict[Domain, List[MetricConfiguration]] = {}
        metric_configurations_for_domain: List[MetricConfiguration]
        metric_configuration: MetricConfiguration
        domain: Domain
        for metric_configuration in self._metric_configurations:
            domain = metric_configuration.get_domain()
            metric_configurations_for_domain = domain_metric_configurations_map.get(
                domain
            )
            if metric_configurations_for_domain is None:
                metric_configurations_for_domain = []
                domain_metric_configurations_map[
                    domain
                ] = metric_configurations_for_domain

            metric_configurations_for_domain.append(metric_configuration)

        return domain_metric_configurations_map
