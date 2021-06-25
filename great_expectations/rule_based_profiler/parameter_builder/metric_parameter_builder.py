from typing import Any, Dict, Optional, Union

import numpy as np

from great_expectations import DataContext
from great_expectations.rule_based_profiler.domain_builder.domain import Domain
from great_expectations.rule_based_profiler.parameter_builder.parameter_builder import (
    ParameterBuilder,
)
from great_expectations.rule_based_profiler.parameter_builder.parameter_container import (
    ParameterContainer,
    build_parameter_container,
)
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
        metric_domain_kwargs: Optional[Union[str, dict]] = None,
        metric_value_kwargs: Optional[Union[str, dict]] = None,
        enforce_numeric_metric: Optional[Union[str, bool]] = False,
        fill_nan_with_zero: Optional[Union[str, bool]] = False,
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
            enforce_numeric_metric: used in MetricConfiguration to insure that metric computations return numeric values
            fill_nan_with_zero: if set to True, then convert every NaN encountered to 0.0 (raise an exception otherwise)
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

        self._enforce_numeric_metric = enforce_numeric_metric
        self._fill_nan_with_zero = fill_nan_with_zero

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

        batch_id: str = self.get_batch_id(variables=variables)

        metric_computation_result: Dict[
            str,
            Union[
                Union[int, np.int32, np.int64, float, np.float32, np.float64],
                Dict[str, Any],
            ],
        ] = self.get_numeric_metric(
            batch_id=batch_id,
            validator=validator,
            metric_name=self._metric_name,
            metric_domain_kwargs=self._metric_domain_kwargs,
            metric_value_kwargs=self._metric_value_kwargs,
            enforce_numeric_metric=self._enforce_numeric_metric,
            fill_nan_with_zero=self._fill_nan_with_zero,
            domain=domain,
            variables=variables,
            parameters=parameters,
        )

        parameter_values: Dict[str, Any] = {
            f"$parameter.{self.parameter_name}": metric_computation_result,
        }

        build_parameter_container(
            parameter_container=parameter_container, parameter_values=parameter_values
        )
