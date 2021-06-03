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
    """
    A Multi-Batch implementation for obtaining the range estimation bounds for a resolved (evaluated) numeric metric,
    using domain_kwargs, value_kwargs, metric_name, and false_positive_rate (tolerance) as arguments.

    This Multi-Batch ParameterBuilder is general in the sense that any metric that computes numbers can be accommodated.
    On the other hand, it is specific in the sense that the parameter names will always have the semantics of numeric
    ranges, which will incorporate the requirements, imposed by the configured false_positive_rate tolerances.
    """

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
        """
        Args:
            parameter_name: the name of this parameter -- this is user-specified parameter name (from configuration);
            it is not the fully-qualified parameter name; a fully-qualified parameter name must start with "$parameter."
            and may contain one or more subsequent parts (e.g., "$parameter.<my_param_from_config>.<metric_name>").
            metric_name: the name of a metric used in MetricConfiguration (must be a supported and registered metric)
            metric_domain_kwargs: used in MetricConfiguration
            metric_value_kwargs: used in MetricConfiguration
            data_context: DataContext
        """
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

        The algorithm operates according to the following steps:
        1. Obtain batch IDs of interest using DataContext and BatchRequest (unless passed explicitly as argument).  Note
        that this particular BatchRequest was specified as part of configuration for the present ParameterBuilder class.
        (This is in contrast to the BatchRequest specified in Checkpoint configuration, or in pipeline, notebook, etc.)
        2. Set up metric_domain_kwargs and metric_value_kwargs (using configuration and/or variables and parameters).
        3. Instantiate the Validator object corresponding to BatchRequest (with a temporary expectation_suite_name) in
           order to have access to all Batch objects, on each of which the specified metric_name will be computed.
        4. While looping through the available batch_ids:
           4.1: Update the metric_domain_kwargs with the specific batch_id (the iteration variable of the loop).
           4.2: Create the metric_configuration_arguments using the metric_domain_kwargs from the previous step.
           4.3: Compute metric_value using the local Validator object (which has access to the required Batch objects).
           4.4: Insure that the metric_value is numeric (ranges can be computed for numeric-valued metrics only).
           4.5: Append the value of the computed metric to the list (one for each batch_id -- loop iteration variable).
        5. Convert the list of floating point metric computation results to a numpy array (for further computations).
        6. Compute the mean and the standard deviation of the metric (aggregated over all the gathered Batch objects).
        7. Compute the number of standard deviations (as a floating point number rounded to the nearest highest integer)
           needed to create the "band" around the mean so as to achieve the specified false_positive_rate (note that the
           false_positive_rate of 0.0 would result in an infinite number of standard deviations, hence it is "nudged" by
           a small quantity, "epsilon", above 0.0 if false_positive_rate of 0.0 is provided as argument in constructor).
           (Please refer to "https://en.wikipedia.org/wiki/Normal_distribution" and references therein for background.)
        8. Compute the "band" around the mean as the min_value and max_value (to be used in ExpectationConfiguration).
        9. Set up the arguments for and call build_parameter_container() to store the parameter as part of "rule state".
        """

        batch_ids: List[str] = self.get_batch_ids(batch_ids=batch_ids)
        if not batch_ids:
            raise ge_exceptions.ProfilerExecutionError(
                message=f"Utilizing a {self.__class__.__name__} requires a non-empty list of batch identifiers."
            )

        # Obtaining domain kwargs from rule state (i.e., variables and parameters); from instance variable otherwise.
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

        # Obtaining value kwargs from rule state (i.e., variables and parameters); from instance variable otherwise.
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

        stds_multiplier: Union[float, int] = NP_SQRT_2 * special.erfinv(
            1.0 - self._false_positive_rate
        )

        min_value: float = mean - stds_multiplier * std
        max_value: float = mean + stds_multiplier * std

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
