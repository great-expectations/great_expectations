import copy
from typing import Any, Dict, List, Optional, Union

import numpy as np
from scipy import special

import great_expectations.exceptions as ge_exceptions
from great_expectations import DataContext
from great_expectations.rule_based_profiler.domain_builder.domain import Domain
from great_expectations.rule_based_profiler.parameter_builder import (
    MultiBatchParameterBuilder,
)
from great_expectations.rule_based_profiler.parameter_builder.parameter_container import (
    ParameterContainer,
    build_parameter_container,
)
from great_expectations.rule_based_profiler.util import (
    BootstrappedStandardErrorOptimizationBasedEstimator,
    SingleNumericStatisticCalculator,
    get_parameter_value_and_validate_return_type,
)
from great_expectations.util import is_int, is_numeric
from great_expectations.validator.validation_graph import MetricConfiguration
from great_expectations.validator.validator import Validator

NP_EPSILON: np.float64 = np.finfo(float).eps
NP_SQRT_2: np.float64 = np.sqrt(2.0)

MAX_DECIMALS: int = 9


class NumericMetricRangeMultiBatchStatisticCalculator(SingleNumericStatisticCalculator):
    def __init__(
        self,
        batch_ids: List[str],
        validator: Validator,
        metric_name: str,
        metric_domain_kwargs: Optional[dict],
        metric_value_kwargs: Optional[dict],
    ):
        self._batch_ids = batch_ids
        self._validator = validator
        self._metric_name = metric_name
        self._metric_domain_kwargs = metric_domain_kwargs
        self._metric_value_kwargs = metric_value_kwargs

    # This property is a required interface method.
    @property
    def sample_identifiers(
        self,
    ) -> List[Union[bytes, str, int, float, complex, tuple, frozenset]]:
        """
        # TODO: <Alex>ALEX -- Improve this DocString.</Alex>
        :return: List of Hashable objects
        """
        return self._batch_ids

    # This is a required interface method.
    def generate_distribution_sample(
        self,
        batch_ids: List[str],
    ) -> Union[
        np.ndarray, List[Union[int, np.int32, np.int64, float, np.float32, np.float64]]
    ]:
        """
        # TODO: <Alex>ALEX -- Improve this DocString.</Alex>
        Computes numeric statistic from unique identifiers of data samples (a unique identifier must be hashable).
        :parameter: randomized_sample_identifiers -- List of Hashable objects
        :return: np.float64
        """
        metric_domain_kwargs_with_specific_batch_id: Optional[
            Dict[str, Any]
        ] = copy.deepcopy(self._metric_domain_kwargs)
        metric_values: List[
            Union[int, np.int32, np.int64, float, np.float32, np.float64]
        ] = []
        metric_value: Union[int, np.int32, np.int64, float, np.float32, np.float64]
        batch_id: str
        for batch_id in batch_ids:
            metric_domain_kwargs_with_specific_batch_id["batch_id"] = batch_id
            metric_configuration_arguments: Dict[str, Any] = {
                "metric_name": self._metric_name,
                "metric_domain_kwargs": metric_domain_kwargs_with_specific_batch_id,
                "metric_value_kwargs": self._metric_value_kwargs,
                "metric_dependencies": None,
            }
            metric_value = self._validator.get_metric(
                metric=MetricConfiguration(**metric_configuration_arguments)
            )
            if not is_numeric(value=metric_value):
                raise ge_exceptions.ProfilerExecutionError(
                    message=f"""Applicability of {self.__class__.__name__} is restricted to numeric-valued metrics \
(value of type "{str(type(metric_value))}" was computed).
"""
                )

            metric_values.append(metric_value)

        return metric_values

    # This is a required interface method.
    def compute_numeric_statistic(
        self,
        randomized_sample_identifiers: List[
            Union[
                bytes,
                str,
                int,
                float,
                complex,
                tuple,
                frozenset,
            ]
        ],
    ) -> np.float64:
        """
        # TODO: <Alex>ALEX -- Improve this DocString.</Alex>
        Computes numeric statistic from unique identifiers of data samples (a unique identifier must be hashable).
        :parameter: randomized_sample_identifiers -- List of Hashable objects
        :return: np.float64
        """
        metric_values: Union[
            np.ndarray,
            List[Union[int, np.int32, np.int64, float, np.float32, np.float64]],
        ] = self.generate_distribution_sample(batch_ids=randomized_sample_identifiers)
        metric_values = np.array(metric_values, dtype=np.float64)
        mean: np.float64 = np.mean(metric_values)
        return mean


class NumericMetricRangeMultiBatchParameterBuilder(MultiBatchParameterBuilder):
    """
    A Multi-Batch implementation for obtaining the range estimation bounds for a resolved (evaluated) numeric metric,
    using domain_kwargs, value_kwargs, metric_name, and false_positive_rate (tolerance) as arguments.

    This Multi-Batch ParameterBuilder is general in the sense that any metric that computes numbers can be accommodated.
    On the other hand, it is specific in the sense that the parameter names will always have the semantics of numeric
    ranges, which will incorporate the requirements, imposed by the configured false_positive_rate tolerances.
    """

    RECOGNIZED_SAMPLING_METHOD_NAMES: set = {
        "single",
        "bootstrap",
    }

    RECOGNIZED_TRUNCATE_DISTRIBUTION_KEYS: set = {
        "lower_bound",
        "upper_bound",
    }

    def __init__(
        self,
        parameter_name: str,
        metric_name: str,
        metric_domain_kwargs: Optional[Union[str, dict]] = "$domain.domain_kwargs",
        metric_value_kwargs: Optional[Union[str, dict]] = None,
        sampling_method: Optional[str] = "bootstrap",
        false_positive_rate: Optional[Union[float, str]] = 0.0,
        truncate_distribution: Optional[
            Union[Dict[str, Union[Optional[int], Optional[float]]], str]
        ] = None,
        round_decimals: Optional[Union[int, str]] = False,
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
            # TODO: <Alex>ALEX -- Here, the DocString needs to elaborate regarding single-pass or bootstrap methods.</Alex>
            sampling_method: choice of the sampling algorithm: "single" (one observation) or "bootstrap" (default)
            false_positive_rate: user-configured fraction between 0 and 1 -- "FP/(FP + TN)" -- where:
            FP stands for "false positives" and TN stands for "true negatives"; this rate specifies allowed "fall-out"
            (in addition, a helpful identity used in this method is: false_positive_rate = 1 - true_negative_rate).
            round_decimals: user-configured non-negative integer indicating the number of decimals of the
            truncate_distribution: user-configured directive for whether or not to allow the computed parameter values
            (i.e., lower_bound, upper_bound) to take on values outside the specified bounds when packaged on output.
            rounding precision of the computed parameter values (i.e., min_value, max_value) prior to packaging them on
            output.  If omitted, then no rounding is performed, unless the computed value is already an integer.
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

        self._sampling_method = sampling_method

        self._false_positive_rate = false_positive_rate

        if not truncate_distribution:
            truncate_distribution = {
                "lower_bound": None,
                "upper_bound": None,
            }
        truncate_distribution_keys: set = set(truncate_distribution.keys())
        if (
            not truncate_distribution_keys
            <= NumericMetricRangeMultiBatchParameterBuilder.RECOGNIZED_TRUNCATE_DISTRIBUTION_KEYS
        ):
            raise ge_exceptions.ProfilerExecutionError(
                message=f"""Unrecognized truncate_distribution_keys key(s) in {self.__class__.__name__}:
"{str(truncate_distribution_keys - NumericMetricRangeMultiBatchParameterBuilder.RECOGNIZED_TRUNCATE_DISTRIBUTION_KEYS)}" detected.
"""
            )
        self._truncate_distribution = truncate_distribution

        self._round_decimals = round_decimals

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
        # TODO: <Alex>ALEX -- Here, the DocString needs to elaborate regarding single-pass or bootstrap methods.</Alex>
        7. Compute the number of standard deviations (as a floating point number rounded to the nearest highest integer)
           needed to create the "band" around the mean so as to achieve the specified false_positive_rate (note that the
           false_positive_rate of 0.0 would result in an infinite number of standard deviations, hence it is "nudged" by
           a small quantity, "epsilon", above 0.0 if false_positive_rate of 0.0 is provided as argument in constructor).
           (Please refer to "https://en.wikipedia.org/wiki/Normal_distribution" and references therein for background.)
        8. Compute the "band" around the mean as the min_value and max_value (to be used in ExpectationConfiguration).
        9. Set up the arguments for and call build_parameter_container() to store the parameter as part of "rule state".
        """

        batch_ids_for_metrics_calculations: Optional[
            List[str]
        ] = self.get_batch_ids_for_metrics_calculations(
            domain=domain,
            variables=variables,
            parameters=parameters,
        )
        if not batch_ids_for_metrics_calculations:
            raise ge_exceptions.ProfilerExecutionError(
                message=f"Utilizing a {self.__class__.__name__} requires a non-empty list of batch identifiers."
            )

        validator_for_metrics_calculations: Validator = (
            self.get_validator_for_metrics_calculations(
                validator=validator,
                domain=domain,
                variables=variables,
                parameters=parameters,
            )
        )

        # Obtain domain kwargs from rule state (i.e., variables and parameters); from instance variable otherwise.
        metric_domain_kwargs: Optional[
            dict
        ] = get_parameter_value_and_validate_return_type(
            domain=domain,
            parameter_reference=self._metric_domain_kwargs,
            expected_return_type=dict,
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

        # Obtain sampling_method directive from rule state (i.e., variables and parameters); from instance variable otherwise.
        sampling_method: str = get_parameter_value_and_validate_return_type(
            domain=domain,
            parameter_reference=self._sampling_method,
            expected_return_type=str,
            variables=variables,
            parameters=parameters,
        )
        if not (
            sampling_method
            in NumericMetricRangeMultiBatchParameterBuilder.RECOGNIZED_SAMPLING_METHOD_NAMES
        ):
            raise ge_exceptions.ProfilerExecutionError(
                message=f"""The directive "sampling_method" for {self.__class__.__name__} can be only one of 
{NumericMetricRangeMultiBatchParameterBuilder.RECOGNIZED_SAMPLING_METHOD_NAMES} ("{sampling_method}" was detected).
"""
            )

        statistic_calculator: NumericMetricRangeMultiBatchStatisticCalculator = (
            NumericMetricRangeMultiBatchStatisticCalculator(
                batch_ids=batch_ids_for_metrics_calculations,
                validator=validator_for_metrics_calculations,
                metric_name=self._metric_name,
                metric_domain_kwargs=metric_domain_kwargs,
                metric_value_kwargs=metric_value_kwargs,
            )
        )

        metric_values: Union[
            np.ndarray,
            List[Union[int, np.int32, np.int64, float, np.float32, np.float64]],
        ] = statistic_calculator.generate_distribution_sample(
            batch_ids=batch_ids_for_metrics_calculations
        )
        metric_value: Union[int, np.int32, np.int64, float, np.float32, np.float64]

        truncate_distribution: Dict[
            str, Union[Optional[int], Optional[float]]
        ] = self._get_truncate_distribution_using_heuristics(
            metric_values=metric_values,
            domain=domain,
            variables=variables,
            parameters=parameters,
        )
        lower_bound: Optional[Union[int, float]] = truncate_distribution.get(
            "lower_bound"
        )
        upper_bound: Optional[Union[int, float]] = truncate_distribution.get(
            "upper_bound"
        )

        round_decimals: int = self._get_round_decimals_using_heuristics(
            metric_values=metric_values,
            domain=domain,
            variables=variables,
            parameters=parameters,
        )

        if sampling_method == "bootstrap":
            bootstrapped_estimator: BootstrappedStandardErrorOptimizationBasedEstimator = BootstrappedStandardErrorOptimizationBasedEstimator(
                statistic_calculator=statistic_calculator,
                sample_size=len(batch_ids_for_metrics_calculations),
                bootstrapped_statistic_deviation_bound=1.0e-1,
                prob_bootstrapped_statistic_deviation_outside_bound=5.0e-2,
            )
            metric_values = (
                bootstrapped_estimator.compute_bootstrapped_statistic_samples()
            )
        else:
            metric_values = np.array(metric_values, dtype=np.float64)

        mean: Union[np.ndarray, np.float64] = np.mean(metric_values)
        std: Union[np.ndarray, np.float64] = self._standard_error(samples=metric_values)

        # Obtain false_positive_rate from rule state (i.e., variables and parameters); from instance variable otherwise.
        false_positive_rate: Union[
            Any, str
        ] = get_parameter_value_and_validate_return_type(
            domain=domain,
            parameter_reference=self._false_positive_rate,
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
        stds_multiplier: np.float64 = NP_SQRT_2 * special.erfinv(true_negative_rate)

        min_value: float = mean - stds_multiplier * std
        max_value: float = mean + stds_multiplier * std

        if round_decimals == 0:
            min_value = round(min_value)
            max_value = round(max_value)
        else:
            min_value = round(min_value, round_decimals)
            max_value = round(max_value, round_decimals)

        if lower_bound is not None:
            min_value = max(min_value, lower_bound)
        if upper_bound is not None:
            max_value = min(max_value, upper_bound)

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

    def _standard_error(
        self,
        samples: np.ndarray,
    ) -> np.float64:
        return np.sqrt(self._standard_variance(samples=samples))

    def _standard_variance(
        self,
        samples: np.ndarray,
    ) -> np.float64:
        num_samples: int = samples.size
        if num_samples < 2:
            raise ValueError(
                f"""Number of samples in {self.__class__.__name__} must be an integer greater than 1 \
(the value {num_samples} was encountered).
"""
            )

        sample_variance: np.float64 = np.var(samples)
        sample_standard_variance: np.float64 = np.float64(
            num_samples * sample_variance / (num_samples - 1)
        )
        return sample_standard_variance

    def _get_truncate_distribution_using_heuristics(
        self,
        metric_values: Union[
            np.ndarray,
            List[Union[int, np.int32, np.int64, float, np.float32, np.float64]],
        ],
        domain: Domain,
        *,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
    ) -> Dict[str, Union[Optional[int], Optional[float]]]:
        # Obtain truncate_distribution directive from rule state (i.e., variables and parameters); from instance variable otherwise.
        truncate_distribution: Dict[
            str, Union[Optional[int], Optional[float]]
        ] = get_parameter_value_and_validate_return_type(
            domain=domain,
            parameter_reference=self._truncate_distribution,
            expected_return_type=dict,
            variables=variables,
            parameters=parameters,
        )
        distribution_boundary: Optional[Union[int, float]]
        if not all(
            [
                (
                    distribution_boundary is None
                    or is_numeric(value=distribution_boundary)
                )
                for distribution_boundary in truncate_distribution.values()
            ]
        ):
            raise ge_exceptions.ProfilerExecutionError(
                message=f"""The directive "truncate_distribution" for {self.__class__.__name__} must specify the
[lower_bound, upper_bound] closed interval, where either boundary is a numeric value (or None).
"""
            )

        lower_bound: Optional[Union[int, float]] = truncate_distribution.get(
            "lower_bound"
        )
        upper_bound: Optional[Union[int, float]] = truncate_distribution.get(
            "upper_bound"
        )
        metric_value: Union[int, np.int32, np.int64, float, np.float32, np.float64]
        if lower_bound is None and all(
            [metric_value > NP_EPSILON for metric_value in metric_values]
        ):
            lower_bound = 0.0
        if upper_bound is None and all(
            [metric_value < (-NP_EPSILON) for metric_value in metric_values]
        ):
            upper_bound = 0.0

        return {
            "lower_bound": lower_bound,
            "upper_bound": upper_bound,
        }

    def _get_round_decimals_using_heuristics(
        self,
        metric_values: Union[
            np.ndarray,
            List[Union[int, np.int32, np.int64, float, np.float32, np.float64]],
        ],
        domain: Domain,
        *,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
    ) -> int:
        # Obtain round_decimals directive from rule state (i.e., variables and parameters); from instance variable otherwise.
        round_decimals: Optional[
            Union[Any]
        ] = get_parameter_value_and_validate_return_type(
            domain=domain,
            parameter_reference=self._round_decimals,
            expected_return_type=None,
            variables=variables,
            parameters=parameters,
        )
        if round_decimals is None:
            round_decimals = MAX_DECIMALS
        elif not isinstance(round_decimals, int) or (round_decimals < 0):
            raise ge_exceptions.ProfilerExecutionError(
                message=f"""The directive "round_decimals" for {self.__class__.__name__} can be 0 or a
positive integer, or must be omitted (or set to None).
"""
            )
        metric_value: Union[int, np.int32, np.int64, float, np.float32, np.float64]
        if all(
            [
                np.issubdtype(type(metric_value), np.integer)
                for metric_value in metric_values
            ]
        ):
            round_decimals = 0

        return round_decimals
