from typing import Any, Dict, List, Optional, Union

from great_expectations import DataContext
from great_expectations.profiler.domain_builder.domain import Domain
from great_expectations.profiler.parameter_builder.parameter_builder import (
    ParameterBuilder,
)
from great_expectations.profiler.parameter_builder.parameter_container import (
    ParameterContainer,
    build_parameter_container,
    get_parameter_value,
)
from great_expectations.validator.validation_graph import MetricConfiguration
from great_expectations.validator.validator import Validator


# TODO: <Alex>ALEX -- this class is not used anywhere in the codebase.</Alex>
class MetricParameterBuilder(ParameterBuilder):
    """Class utilized for obtaining a resolved (evaluated) metric (which is labeled a 'parameter') using domain kwargs, value
    kwargs, and a metric name"""

    def __init__(
        self,
        name: str,
        metric_name: str,
        metric_domain_kwargs: Optional[Union[str, dict]] = "$domain.domain_kwargs",
        metric_value_kwargs: Optional[Union[str, dict]] = None,
        data_context: Optional[DataContext] = None,
    ):
        super().__init__(
            name=name,
            data_context=data_context,
        )

        self._metric_name = metric_name
        self._metric_domain_kwargs = metric_domain_kwargs
        self._metric_value_kwargs = metric_value_kwargs

    def _build_parameters(
        self,
        domain: Domain,
        validator: Validator,
        *,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
        batch_ids: Optional[List[str]] = None,
    ) -> ParameterContainer:
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

        parameter_values: Dict[str, Dict[str, Any]] = {
            f"${self.name}.parameters.{self._metric_name}": {
                "value": validator.get_metric(
                    metric=MetricConfiguration(
                        metric_name=self._metric_name,
                        metric_domain_kwargs=metric_domain_kwargs,
                        metric_value_kwargs=metric_value_kwargs,
                        metric_dependencies=None,
                    )
                ),
                "details": None,
            },
        }
        return build_parameter_container(parameter_values=parameter_values)
