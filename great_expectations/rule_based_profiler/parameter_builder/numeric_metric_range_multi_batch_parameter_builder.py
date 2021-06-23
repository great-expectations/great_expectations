import copy
from dataclasses import make_dataclass
from typing import Any, Dict, List, Optional, Union

import numpy as np
from scipy import special
from scipy.stats import bootstrap

import great_expectations.exceptions as ge_exceptions
from great_expectations import DataContext
from great_expectations.rule_based_profiler.domain_builder import Domain
from great_expectations.rule_based_profiler.parameter_builder import (
    ParameterBuilder,
    ParameterContainer,
    build_parameter_container,
)
from great_expectations.rule_based_profiler.util import (
    NP_EPSILON,
    get_parameter_value_and_validate_return_type,
)
from great_expectations.util import is_numeric
from great_expectations.validator.validation_graph import MetricConfiguration
from great_expectations.validator.validator import Validator

NP_SQRT_2: np.float64 = np.sqrt(2.0)

MAX_DECIMALS: int = 9

ConfidenceInterval = make_dataclass("ConfidenceInterval", ["low", "high"])


class NumericMetricRangeMultiBatchStatisticGetter:
    """
    This class provides methods for combining metric computations for all batches into an array as well as the method
    that computes the statistic, the "mean" value, of this array, which is desired by the given parameter_builder logic.
    Note: Every metric computation is carried out exactly once (hence, no caching of metric values is necessary).
    """

    def __init__(
        self,
        batch_ids: List[str],
        validator: Validator,
        metric_name: str,
        metric_domain_kwargs: Optional[dict],
        metric_value_kwargs: Optional[dict],
        fill_nan_with_zero: Optional[bool],
    ):
        self._batch_ids = batch_ids
        self._validator = validator
        self._metric_name = metric_name
        self._metric_domain_kwargs = metric_domain_kwargs
        self._metric_value_kwargs = metric_value_kwargs
        self._fill_nan_with_zero = fill_nan_with_zero

    def get_metrics(
        self,
        batch_ids: Optional[List[str]],
    ) -> Union[
        np.ndarray, List[Union[int, np.int32, np.int64, float, np.float32, np.float64]]
    ]:
        """
        This method computes the given metric for each Batch (indicated by its respective batch_id) and returns an array
        comprised of these numeric values.  Error checking and NaN conversion to 0.0 (if configured) is also handled.

        :parameter: batch_ids
        :return: an array-like (or list-like) collection of floating point numbers, representing the distribution sample
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
            if np.isnan(metric_value):
                if not self._fill_nan_with_zero:
                    raise ValueError(
                        f"""Computation of metric "{self._metric_name}" resulted in NaN ("not a number") value.
"""
                    )
                metric_value = 0.0

            metric_values.append(metric_value)

        return metric_values

    @staticmethod
    def statistic(
        metric_values: Union[
            np.ndarray,
            List[Union[int, np.int32, np.int64, float, np.float32, np.float64]],
        ],
    ) -> np.float64:
        """
        This method computes a statistic for the distribution sample, passed to it in the form of a numeric array.
        Any single-valued numeric statistic that is a function of all the data points (array elements) is acceptable.

        :parameter: metric_values -- an array-like (or list-like) collection of floating point numbers, representing the
        distribution sample (e.g., this quantity could be obtained by executing such method as "get_metrics()" above).
        :return: np.float64 -- scalar measure of the distribution sample (here, the sample mean of data point metrics)
        """
        metric_values = np.array(metric_values, dtype=np.float64)
        mean: np.float64 = np.mean(metric_values)
        return mean


class NumericMetricRangeMultiBatchParameterBuilder(ParameterBuilder):
    """
    A Multi-Batch implementation for obtaining the range estimation bounds for a resolved (evaluated) numeric metric,
    using domain_kwargs, value_kwargs, metric_name, and false_positive_rate (tolerance) as arguments.

    This Multi-Batch ParameterBuilder is general in the sense that any metric that computes numbers can be accommodated.
    On the other hand, it is specific in the sense that the parameter names will always have the semantics of numeric
    ranges, which will incorporate the requirements, imposed by the configured false_positive_rate tolerances.

    The implementation supports two methods of estimating parameter values from data:
    * bootstrapped (default) -- a statistical technique (see "https://en.wikipedia.org/wiki/Bootstrapping_(statistics)")
    * one-shot -- assumes that metric values, computed on batch data, are normally distributed and computes the mean
      and the standard error using the queried batches as the single sample of the distribution (fast, but inaccurate).
    """

    RECOGNIZED_SAMPLING_METHOD_NAMES: set = {
        "oneshot",
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
        fill_nan_with_zero: Optional[Union[str, bool]] = True,
        sampling_method: Optional[str] = "bootstrap",
        num_bootstrap_samples: Optional[Union[int, str]] = None,
        false_positive_rate: Optional[Union[float, str]] = 0.0,
        truncate_distribution: Optional[
            Union[Dict[str, Union[Optional[int], Optional[float]]], str]
        ] = None,
        round_decimals: Optional[Union[int, str]] = None,
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
            fill_nan_with_zero: if False, then if the computed metric gives NaN, then exception is raised; otherwise,
            if True (default), then if the computed metric gives NaN, then it is converted to the 0.0 (float) value.
            sampling_method: choice of the sampling algorithm: "oneshot" (one observation) or "bootstrap" (default)
            num_bootstrap_samples: Applicable only for the "bootstrap" sampling method -- if omitted (default), then
            9999 is used (default in "https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.bootstrap.html").
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

        self._fill_nan_with_zero = fill_nan_with_zero

        self._sampling_method = sampling_method
        self._num_bootstrap_samples = num_bootstrap_samples

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
        *,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
    ):
        """
         Builds ParameterContainer object that holds ParameterNode objects with attribute name-value pairs and optional details.

         :return: ParameterContainer object that holds ParameterNode objects with attribute name-value pairs and optional details

         The algorithm operates according to the following steps:
         1. Obtain batch IDs of interest using DataContext and BatchRequest (unless passed explicitly as argument). Note
         that this specific BatchRequest was specified as part of configuration for the present ParameterBuilder class.
         (This is in contrast to the BatchRequest used to instantiate the passed in Validator argument, and/or specified
         in a Checkpoint configuration, and/or in a pipeline, a Jupyter notebook, etc.)
         2. Set up metric_domain_kwargs and metric_value_kwargs (using configuration and/or variables and parameters).
         3. Instantiate the Validator object corresponding to BatchRequest (with a temporary expectation_suite_name) in
            order to have access to all Batch objects, on each of which the specified metric_name will be computed.
         4. Instantiate the NumericMetricRangeMultiBatchStatisticGetter class, which implements metric computations.
            4.0 While looping through the available batch_ids:
            4.1: Update the metric_domain_kwargs with the specific batch_id (the iteration variable of the loop).
            4.2: Create the metric_configuration_arguments using the metric_domain_kwargs from the previous step.
            4.3: Compute metric_value using the local Validator object (which has access to the required Batch objects).
            4.4: Insure that the metric_value is numeric (ranges can be computed for numeric-valued metrics only).
            4.5: Append the value of the computed metric to the list (one for each batch_id -- loop iteration variable).
         5. Using the configured directives and heuristics, determine whether or not the ranges should be clipped.
         6. Using the configured directives and heuristics, determine if return values should be rounded to an integer.
         7. Convert the list of floating point metric computation results to a numpy array (for further computations).
         Steps 8 -- 10 are for the "oneshot" sampling method only (the "bootstrap" method achieves same automatically):
         8. Compute the mean and the standard deviation of the metric (aggregated over all the gathered Batch objects).
         9. Compute the number of standard deviations (as floating point number rounded to the nearest highest integer)
            needed to create the "band" around the mean for achieving the specified false_positive_rate (note that the
            false_positive_rate of 0.0 would result in infinite number of standard deviations, hence it is "nudged" by
            a small quantity ("epsilon") above 0.0 if false_positive_rate of 0.0 is given as argument in constructor).
            (Please refer to "https://en.wikipedia.org/wiki/Normal_distribution" and references therein for background.)
        10. Compute the "band" around the mean as the min_value and max_value (to be used in ExpectationConfiguration).
        11. Return ConfidenceInterval for the desired metric as estimated by the specified sampling method.
        12. Set up the arguments and call build_parameter_container() to store the parameter as part of "rule state".
        """

        batch_ids: Optional[List[str]] = self.get_batch_ids(
            domain=domain,
            variables=variables,
            parameters=parameters,
        )
        if not batch_ids:
            raise ge_exceptions.ProfilerExecutionError(
                message=f"Utilizing a {self.__class__.__name__} requires a non-empty list of batch identifiers."
            )

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

        # Obtain fill_nan_with_zero from rule state (i.e., variables and parameters); from instance variable otherwise.
        fill_nan_with_zero: Optional[
            bool
        ] = get_parameter_value_and_validate_return_type(
            domain=domain,
            parameter_reference=self._fill_nan_with_zero,
            expected_return_type=bool,
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

        true_negative_rate: np.float64 = np.float64(1.0 - false_positive_rate)
        stds_multiplier: np.float64

        statistic_calculator: NumericMetricRangeMultiBatchStatisticGetter = (
            NumericMetricRangeMultiBatchStatisticGetter(
                batch_ids=batch_ids,
                validator=validator,
                metric_name=self._metric_name,
                metric_domain_kwargs=metric_domain_kwargs,
                metric_value_kwargs=metric_value_kwargs,
                fill_nan_with_zero=fill_nan_with_zero,
            )
        )

        metric_values: Union[
            np.ndarray,
            List[Union[int, np.int32, np.int64, float, np.float32, np.float64]],
        ] = statistic_calculator.get_metrics(batch_ids=batch_ids)
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

        metric_values = np.array(metric_values, dtype=np.float64)

        confidence_interval: ConfidenceInterval
        if sampling_method == "bootstrap":
            # Use default (bootstrap confidence interval already achieves optimal variance); keep for experimentation.
            stds_multiplier = np.float64(0.0)
            confidence_interval = self._get_bootstrap_confidence_interval(
                metric_values=metric_values,
                false_positive_rate=false_positive_rate,
                domain=domain,
                variables=variables,
                parameters=parameters,
                stds_multiplier=stds_multiplier,
            )
        else:
            stds_multiplier = NP_SQRT_2 * special.erfinv(true_negative_rate)
            confidence_interval = self._get_oneshot_confidence_interval(
                metric_values=metric_values,
                stds_multiplier=stds_multiplier,
            )

        if round_decimals == 0:
            min_value = round(confidence_interval.low)
            max_value = round(confidence_interval.high)
        else:
            min_value = round(confidence_interval.low, round_decimals)
            max_value = round(confidence_interval.high, round_decimals)

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

    def _get_bootstrap_confidence_interval(
        self,
        metric_values: Union[
            np.ndarray,
            List[Union[int, np.int32, np.int64, float, np.float32, np.float64]],
        ],
        false_positive_rate: np.float64,
        domain: Domain,
        *,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
        stds_multiplier: Optional[np.float64] = 0.0,
    ):
        # Obtain num_bootstrap_samples override from rule state (i.e., variables and parameters); from instance variable otherwise.
        num_bootstrap_samples: Optional[
            int
        ] = get_parameter_value_and_validate_return_type(
            domain=domain,
            parameter_reference=self._num_bootstrap_samples,
            expected_return_type=None,
            variables=variables,
            parameters=parameters,
        )

        rng: np.random.Generator = np.random.default_rng()
        bootstrap_samples: tuple = (
            metric_values,
        )  # bootstrap samples must be in a sequence

        true_negative_rate: np.float64 = np.float64(1.0 - false_positive_rate)
        confidence_level: np.float64 = np.float64(
            true_negative_rate + 5.0e-1 * false_positive_rate
        )

        # noinspection PyPep8Naming
        BootstrapResult = make_dataclass(
            "BootstrapResult", ["confidence_interval", "standard_error"]
        )

        bootstrap_result: BootstrapResult
        if num_bootstrap_samples is None:
            bootstrap_result = bootstrap(
                bootstrap_samples,
                NumericMetricRangeMultiBatchStatisticGetter.statistic,
                vectorized=False,
                confidence_level=confidence_level,
                random_state=rng,
            )
        else:
            bootstrap_result = bootstrap(
                bootstrap_samples,
                NumericMetricRangeMultiBatchStatisticGetter.statistic,
                vectorized=False,
                confidence_level=confidence_level,
                n_resamples=num_bootstrap_samples,
                random_state=rng,
            )

        confidence_interval: ConfidenceInterval = bootstrap_result.confidence_interval
        confidence_interval_low: np.float64 = confidence_interval.low
        confidence_interval_high: np.float64 = confidence_interval.high

        std: Union[np.ndarray, np.float64] = bootstrap_result.standard_error

        margin_of_error: np.float64 = stds_multiplier * std

        confidence_interval_low -= margin_of_error
        confidence_interval_high += margin_of_error

        return ConfidenceInterval(
            low=confidence_interval_low, high=confidence_interval_high
        )

    def _get_oneshot_confidence_interval(
        self,
        metric_values: Union[
            np.ndarray,
            List[Union[int, np.int32, np.int64, float, np.float32, np.float64]],
        ],
        stds_multiplier: np.float64,
    ):
        mean: Union[np.ndarray, np.float64] = np.mean(metric_values)
        std: Union[np.ndarray, np.float64] = self._standard_error(samples=metric_values)

        margin_of_error: np.float64 = stds_multiplier * std

        confidence_interval_low: np.float64 = mean - margin_of_error
        confidence_interval_high: np.float64 = mean + margin_of_error

        return ConfidenceInterval(
            low=confidence_interval_low, high=confidence_interval_high
        )

    def _standard_error(
        self,
        samples: np.ndarray,
    ) -> np.float64:
        standard_variance: np.float64 = self._standard_variance_unbiased(
            samples=samples
        )

        if np.isclose(standard_variance, 0.0):
            standard_variance = np.float64(0.0)

        return np.sqrt(standard_variance)

    def _standard_variance_unbiased(
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

        return num_samples * (np.var(samples) + NP_EPSILON) / (num_samples - 1)

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
