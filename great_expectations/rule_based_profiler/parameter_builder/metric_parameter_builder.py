from typing import Any, Dict, List, Optional, Union

from great_expectations import DataContext
from great_expectations.rule_based_profiler.domain_builder.domain import Domain
from great_expectations.rule_based_profiler.parameter_builder.parameter_builder import (
    ParameterBuilder,
)
from great_expectations.rule_based_profiler.parameter_builder.parameter_container import (
    ParameterContainer,
    build_parameter_container,
    get_parameter_value,
)
from great_expectations.validator.validation_graph import MetricConfiguration
from great_expectations.validator.validator import Validator


class MetricParameterBuilder(ParameterBuilder):
    """Class utilized for obtaining a resolved (evaluated) metric (which is labeled a 'parameter') using domain kwargs, value
    kwargs, and a metric name"""

    def __init__(
        self,
        parameter_name: str,
        metric_name: str,
        metric_domain_kwargs: Optional[Union[str, dict]] = "$domain.domain_kwargs",
        metric_value_kwargs: Optional[Union[str, dict]] = None,
        data_context: Optional[DataContext] = None,
    ):
        """
        Args:
            parameter_name: the name of this parameter -- this is user-specified parameter name (from configuration);
            it is not the fully-qualified parameter name; a fully-qualified parameter name must start with "$parameter."
            and may contain one or more subsequent parts (e.g., "$parameter.<my_param_from_config>.<metric_name>").
            data_context: DataContext
        """
        super().__init__(
            parameter_name=parameter_name,
            data_context=data_context,
        )

        self._metric_name = metric_name
        self._metric_domain_kwargs = metric_domain_kwargs
        self._metric_value_kwargs = metric_value_kwargs

    def _build_parameters(
        self,
        parameter_container: ParameterContainer,
        domain: Domain,
        validator: Validator,
        *,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
        batch_ids: Optional[List[str]] = None,
    ):
        """
        Builds ParameterContainer object that holds ParameterNode objects with attribute name-value pairs and optional details.
            Args:
        :return: a ParameterContainer object that holds ParameterNode objects with attribute name-value pairs and optional details
        """
        # Obtaining any necessary domain kwargs from rule state, otherwise using instance var
        if isinstance(
            self._metric_domain_kwargs, str
        ) and self._metric_domain_kwargs.startswith("$"):
            metric_domain_kwargs = get_parameter_value(
                fully_qualified_parameter_name=self._metric_domain_kwargs,
                domain=domain,
                variables=variables,
                parameters=parameters,
            )
        else:
            metric_domain_kwargs = self._metric_domain_kwargs

        # Obtaining any necessary value kwargs from rule state, otherwise using instance var
        if (
            self._metric_value_kwargs is not None
            and isinstance(self._metric_value_kwargs, str)
            and self._metric_value_kwargs.startswith("$")
        ):
            metric_value_kwargs = get_parameter_value(
                fully_qualified_parameter_name=self._metric_value_kwargs,
                domain=domain,
                variables=variables,
                parameters=parameters,
            )
        else:
            metric_value_kwargs = self._metric_value_kwargs

        metric_configuration_arguments: Dict[str, Any] = {
            "metric_name": self._metric_name,
            "metric_domain_kwargs": metric_domain_kwargs,
            "metric_value_kwargs": metric_value_kwargs,
            "metric_dependencies": None,
        }
        parameter_values: Dict[str, Any] = {
            self.fully_qualified_parameter_name: {
                "value": validator.get_metric(
                    metric=MetricConfiguration(**metric_configuration_arguments)
                ),
                "details": {
                    "metric_configuration": metric_configuration_arguments,
                },
            },
        }
        build_parameter_container(
            parameter_container=parameter_container, parameter_values=parameter_values
        )

    @property
    def fully_qualified_parameter_name(self) -> str:
        return f"$parameter.{self.parameter_name}"
