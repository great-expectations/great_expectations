from typing import Any, Dict, Optional, Union

from great_expectations import DataContext
from great_expectations.rule_based_profiler.domain_builder.domain import Domain
from great_expectations.rule_based_profiler.parameter_builder.parameter_builder import (
    ParameterBuilder,
)
from great_expectations.rule_based_profiler.parameter_builder.parameter_container import (
    ParameterContainer,
    build_parameter_container,
)
from great_expectations.rule_based_profiler.util import (
    get_parameter_value_and_validate_return_type,
)
from great_expectations.validator.validation_graph import MetricConfiguration
from great_expectations.validator.validator import Validator


class MetricParameterBuilder(ParameterBuilder):
    """
    A Single-Batch implementation for obtaining a resolved (evaluated) metric, using domain_kwargs, value_kwargs, and
    metric_name as arguments.
    """

    def __init__(
        self,
        parameter_name: str,
        metric_name: str,
        metric_domain_kwargs: Optional[Union[str, dict]] = "$domain.domain_kwargs",
        metric_value_kwargs: Optional[Union[str, dict]] = None,
        data_context: Optional[DataContext] = None,
        batch_request: Optional[Union[dict, str]] = None,
    ):
        """
        Args:
            parameter_name: the name of this parameter -- this is user-specified parameter name (from configuration);
            it is not the fully-qualified parameter name; a fully-qualified parameter name must start with "$parameter."
            and may contain one or more subsequent parts (e.g., "$parameter.<my_param_from_config>.<metric_name>").
            metric_name: the name of a metric used in MetricConfiguration (must be a supported and registered metric)
            metric_domain_kwargs: used in MetricConfiguration
            metric_value_kwargs: used in MetricConfiguration
            data_context: DataContext
            batch_request: specified in ParameterBuilder configuration to get Batch objects for parameter computation.
        """
        super().__init__(
            parameter_name=parameter_name,
            data_context=data_context,
            batch_request=batch_request,
        )

        self._metric_name = metric_name
        self._metric_domain_kwargs = metric_domain_kwargs
        self._metric_value_kwargs = metric_value_kwargs

    def _build_parameters(
        self,
        parameter_container: ParameterContainer,
        domain: Domain,
        *,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
    ):
        """
        Builds ParameterContainer object that holds ParameterNode objects with attribute name-value pairs and optional details.
            Args:
        :return: a ParameterContainer object that holds ParameterNode objects with attribute name-value pairs and optional details
        """
        validator: Validator = self.get_validator(
            domain=domain,
            variables=variables,
            parameters=parameters,
        )

        # Obtain domain kwargs from rule state (i.e., variables and parameters); from instance variable otherwise.
        metric_domain_kwargs: Optional[
            dict
        ] = get_parameter_value_and_validate_return_type(
            domain=domain,
            parameter_reference=self._metric_domain_kwargs,
            expected_return_type=None,
            variables=variables,
            parameters=parameters,
        )
        # Obtain value kwargs from rule state (i.e., variables and parameters); from instance variable otherwise.
        metric_value_kwargs: Optional[
            dict
        ] = get_parameter_value_and_validate_return_type(
            domain=domain,
            parameter_reference=self._metric_value_kwargs,
            expected_return_type=None,
            variables=variables,
            parameters=parameters,
        )

        metric_configuration_arguments: Dict[str, Any] = {
            "metric_name": self._metric_name,
            "metric_domain_kwargs": metric_domain_kwargs,
            "metric_value_kwargs": metric_value_kwargs,
            "metric_dependencies": None,
        }
        parameter_values: Dict[str, Any] = {
            f"$parameter.{self.parameter_name}": {
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
