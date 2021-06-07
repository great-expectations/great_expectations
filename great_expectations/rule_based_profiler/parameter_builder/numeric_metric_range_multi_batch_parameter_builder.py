import copy
import uuid
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
)
from great_expectations.rule_based_profiler.util import (
    get_parameter_value_and_validate_return_type,
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
        false_positive_rate: Optional[Union[float, str]] = 0.0,
        round_to_nearest_integer: Optional[Union[bool, str]] = False,
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
            false_positive_rate: user-configured fraction between 0 and 1 -- "FP/(FP + TN)" -- where:
            FP stands for "false positives" and TN stands for "true negatives"; this rate specifies allowed "fall-out"
            (in addition, a helpful identity used in this method is: false_positive_rate = 1 - true_negative_rate).
            round_to_nearest_integer: user-configured boolean directive for whether or not to round the computed
            parameter values (i.e., min_value, max_value) to the nearest integer prior to packaging them on output.
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

        self._false_positive_rate = false_positive_rate
        self._round_to_nearest_integer = round_to_nearest_integer

    def _build_parameters(
        self,
        parameter_container: ParameterContainer,
        domain: Domain,
        validator: Validator,
        *,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
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

        batch_ids: List[str] = self.get_batch_ids(
            validator=validator,
            domain=domain,
            variables=variables,
            parameters=parameters,
        )
        if not batch_ids:
            raise ge_exceptions.ProfilerExecutionError(
                message=f"Utilizing a {self.__class__.__name__} requires a non-empty list of batch identifiers."
            )

        # Obtain domain kwargs from rule state (i.e., variables and parameters); from instance variable otherwise.
        metric_domain_kwargs: Optional[
            Union[str, dict]
        ] = get_parameter_value_and_validate_return_type(
            domain=domain,
            argument=self._metric_domain_kwargs,
            expected_return_type=None,
            variables=variables,
            parameters=parameters,
        )
        # Obtain value kwargs from rule state (i.e., variables and parameters); from instance variable otherwise.
        metric_value_kwargs: Optional[
            Union[str, dict]
        ] = get_parameter_value_and_validate_return_type(
            domain=domain,
            argument=self._metric_value_kwargs,
            expected_return_type=None,
            variables=variables,
            parameters=parameters,
        )

        expectation_suite_name: str = (
            f"tmp_suite_domain_{domain.id}_{str(uuid.uuid4())[:8]}"
        )
        # Obtain BatchRequest from rule state (i.e., variables and parameters); from instance variable otherwise.
        batch_request: Optional[
            Union[BatchRequest, dict, str]
        ] = get_parameter_value_and_validate_return_type(
            domain=domain,
            argument=self._batch_request,
            expected_return_type=dict,
            variables=variables,
            parameters=parameters,
        )
        batch_request = BatchRequest(**batch_request)
        validator_for_metrics_calculations: Validator = self.data_context.get_validator(
            batch_request=batch_request,
            create_expectation_suite_with_name=expectation_suite_name,
        )

        metric_values: List[Union[int, float, np.float32, np.float64]] = []
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
                Any, int, float
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

        # Obtain false_positive_rate from rule state (i.e., variables and parameters); from instance variable otherwise.
        false_positive_rate: Union[
            Any, str
        ] = get_parameter_value_and_validate_return_type(
            domain=domain,
            argument=self._false_positive_rate,
            expected_return_type=(int, float),
            variables=variables,
            parameters=parameters,
        )
        if not (0.0 <= false_positive_rate <= 1.0):
            raise ge_exceptions.ProfilerExecutionError(
                message=f"False-Positive Rate for {self.__class__.__name__} is outside of [0.0, 1.0] closed interval."
            )

        if np.isclose(false_positive_rate, 0.0):
            false_positive_rate = false_positive_rate + NP_EPSILON

        true_negative_rate: float = 1.0 - false_positive_rate
        stds_multiplier: float = NP_SQRT_2 * special.erfinv(true_negative_rate)

        min_value: float = mean - stds_multiplier * std
        max_value: float = mean + stds_multiplier * std

        # Obtain round_to_nearest_integer boolean directive from rule state (i.e., variables and parameters); from instance variable otherwise.
        round_to_nearest_integer: Union[
            Any, str
        ] = get_parameter_value_and_validate_return_type(
            domain=domain,
            argument=self._round_to_nearest_integer,
            expected_return_type=bool,
            variables=variables,
            parameters=parameters,
        )

        if round_to_nearest_integer:
            min_value = round(min_value)
            max_value = round(max_value)

        parameter_values: Dict[str, Any] = {
            f"$parameter.{self.parameter_name}": {
                "value": {
                    "min_value": min_value,
                    "max_value": max_value,
                },
                "details": {
                    # Note: the "metric_domain_kwargs" value, used in "details", corresponds to the active Batch.
                    # While any information can be placed into the "details" dictionary, this judicious choice will
                    # allow for the relevant "details" to be used as "meta" in ExpectationConfiguration and render well,
                    # without overwhelming the user (e.g., if instead all "batch_id" values were captured in "details").
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
