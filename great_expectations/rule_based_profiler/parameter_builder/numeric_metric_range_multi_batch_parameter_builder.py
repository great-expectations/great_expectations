import copy
from typing import Any, Dict, List, Optional, Union

import numpy as np
from scipy import special

import great_expectations.exceptions as ge_exceptions
from great_expectations import DataContext
from great_expectations.core.batch import BatchRequest
from great_expectations.rule_based_profiler.domain_builder.domain import Domain
from great_expectations.rule_based_profiler.parameter_builder import (
    MultiBatchParameterBuilder,
)
from great_expectations.rule_based_profiler.parameter_builder.parameter_container import (
    ParameterContainer,
    build_parameter_container,
    get_parameter_value,
)
from great_expectations.util import is_numeric
from great_expectations.validator.validation_graph import MetricConfiguration
from great_expectations.validator.validator import Validator

NP_EPSILON: float = np.finfo(float).eps
NP_SQRT_2: float = np.sqrt(2.0)


class NumericMetricRangeMultiBatchParameterBuilder(MultiBatchParameterBuilder):
    def __init__(
        self,
        parameter_name: str,
        metric_name: str,
        metric_domain_kwargs: Optional[Union[str, dict]] = "$domain.domain_kwargs",
        metric_value_kwargs: Optional[Union[str, dict]] = None,
        false_positive_rate: Optional[float] = 0.0,
        data_context: Optional[DataContext] = None,
        batch_request: Optional[Union[BatchRequest, dict]] = None,
    ):
        super().__init__(
            parameter_name=parameter_name,
            data_context=data_context,
            batch_request=batch_request,
        )

        self._metric_name = metric_name
        self._metric_domain_kwargs = metric_domain_kwargs
        self._metric_value_kwargs = metric_value_kwargs

        if not (0.0 <= false_positive_rate <= 1.0):
            raise ge_exceptions.ProfilerExecutionError(
                message=f"False-Positive Rate for {self.__class__.__name__} is outside of [0.0, 1.0] closed interval."
            )

        if np.isclose(false_positive_rate, 0.0):
            false_positive_rate = false_positive_rate + NP_EPSILON

        self._false_positive_rate = false_positive_rate

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

        batch_ids: List[str] = self.get_batch_ids(batch_ids=batch_ids)
        if not batch_ids:
            raise ge_exceptions.ProfilerExecutionError(
                message=f"Utilizing a {self.__class__.__name__} requires a non-empty list of batch identifiers."
            )

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

        expectation_suite_name: str = f"tmp_suite_domain_{domain.id}"
        validator_for_metrics_calculations: Validator = self.data_context.get_validator(
            batch_request=self.batch_request,
            create_expectation_suite_with_name=expectation_suite_name,
        )

        metric_values: List[Union[float, np.float32, np.float64]] = []
        metric_domain_kwargs_with_specific_batch_id: Optional[
            Dict[str, Any]
        ] = copy.deepcopy(metric_domain_kwargs)
        batch_id: str
        for batch_id in batch_ids:
            metric_domain_kwargs_with_specific_batch_id["batch_id"] = batch_id
            metric_configuration_arguments: Dict[str, Any] = {
                "metric_name": self._metric_name,
                "metric_domain_kwargs": metric_domain_kwargs_with_specific_batch_id,
                "metric_value_kwargs": metric_value_kwargs,
                "metric_dependencies": None,
            }
            metric_value: Union[
                Any, float
            ] = validator_for_metrics_calculations.get_metric(
                metric=MetricConfiguration(**metric_configuration_arguments)
            )

            if not is_numeric(value=metric_value):
                raise ge_exceptions.ProfilerExecutionError(
                    message=f"""Applicability of {self.__class__.__name__} is restricted to numeric-valued metrics \
(value of type "{str(type(metric_value))}" was computed).
"""
                )

            metric_values.append(float(metric_value))

        metric_values = np.array(metric_values, dtype=np.float64)
        mean: float = np.mean(metric_values)
        std: float = np.std(metric_values)

        num_stds: Union[float, int] = NP_SQRT_2 * special.erfinv(
            1.0 - self._false_positive_rate
        )
        num_stds = float(np.rint(num_stds))

        min_value: float = mean - num_stds * std
        max_value: float = mean + num_stds * std

        parameter_values: Dict[str, Any] = {
            self.fully_qualified_parameter_name: {
                "value": {
                    "min_value": min_value,
                    "max_value": max_value,
                },
                "details": {
                    "metric_configuration": {
                        "metric_name": self._metric_name,
                        "metric_domain_kwargs": metric_domain_kwargs,
                        "metric_value_kwargs": metric_value_kwargs,
                        "metric_dependencies": None,
                    },
                },
            },
        }

        build_parameter_container(
            parameter_container=parameter_container, parameter_values=parameter_values
        )

    @property
    def fully_qualified_parameter_name(self) -> str:
        return f"$parameter.{self.parameter_name}.{self._metric_name}"
