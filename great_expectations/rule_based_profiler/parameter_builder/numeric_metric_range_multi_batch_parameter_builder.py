from dataclasses import make_dataclass
from numbers import Number
from typing import Any, Callable, Dict, List, Optional, Union

import numpy as np
from scipy import special

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
    import_scipy_stats_bootstrap_function,
)
from great_expectations.util import import_library_module, is_numeric
from great_expectations.validator.validator import Validator

bootstrap: Optional[Callable] = import_scipy_stats_bootstrap_function()

NP_SQRT_2: np.float64 = np.sqrt(2.0)

MAX_DECIMALS: int = 9

ConfidenceInterval = make_dataclass("ConfidenceInterval", ["low", "high"])


class NumericMetricRangeMultiBatchParameterBuilder(ParameterBuilder):
    """
    A Multi-Batch implementation for obtaining the range estimation bounds for a resolved (evaluated) numeric metric,
    using domain_kwargs, value_kwargs, metric_name, and confidence_level (tolerance) as arguments.

    This Multi-Batch ParameterBuilder is general in the sense that any metric that computes numbers can be accommodated.
    On the other hand, it is specific in the sense that the parameter names will always have the semantics of numeric
    ranges, which will incorporate the requirements, imposed by the configured confidence_level tolerances.

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
        metric_domain_kwargs: Optional[Union[str, dict]] = None,
        metric_value_kwargs: Optional[Union[str, dict]] = None,
        sampling_method: Optional[str] = "bootstrap",
        enforce_numeric_metric: Optional[Union[str, bool]] = True,
        replace_nan_with_zero: Optional[Union[str, bool]] = True,
        confidence_level: Optional[Union[float, str]] = 9.5e-1,
        num_bootstrap_samples: Optional[Union[int, str]] = None,
        round_decimals: Optional[Union[int, str]] = None,
        truncate_values: Optional[
            Union[Dict[str, Union[Optional[int], Optional[float]]], str]
        ] = None,
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
            sampling_method: choice of the sampling algorithm: "oneshot" (one observation) or "bootstrap" (default)
            enforce_numeric_metric: used in MetricConfiguration to insure that metric computations return numeric values
            replace_nan_with_zero: if False, then if the computed metric gives NaN, then exception is raised; otherwise,
            if True (default), then if the computed metric gives NaN, then it is converted to the 0.0 (float) value.
            confidence_level: user-configured fraction between 0 and 1
            num_bootstrap_samples: Applicable only for the "bootstrap" sampling method -- if omitted (default), then
            9999 is used (default in "https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.bootstrap.html").
            round_decimals: user-configured non-negative integer indicating the number of decimals of the
            rounding precision of the computed parameter values (i.e., min_value, max_value) prior to packaging them on
            output.  If omitted, then no rounding is performed, unless the computed value is already an integer.
            truncate_values: user-configured directive for whether or not to allow the computed parameter values
            (i.e., lower_bound, upper_bound) to take on values outside the specified bounds when packaged on output.
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

        self._enforce_numeric_metric = enforce_numeric_metric
        self._replace_nan_with_zero = replace_nan_with_zero

        self._confidence_level = confidence_level

        self._num_bootstrap_samples = num_bootstrap_samples

        self._round_decimals = round_decimals

        if not truncate_values:
            truncate_values = {
                "lower_bound": None,
                "upper_bound": None,
            }
        truncate_values_keys: set = set(truncate_values.keys())
        if (
            not truncate_values_keys
            <= NumericMetricRangeMultiBatchParameterBuilder.RECOGNIZED_TRUNCATE_DISTRIBUTION_KEYS
        ):
            raise ge_exceptions.ProfilerExecutionError(
                message=f"""Unrecognized truncate_values key(s) in {self.__class__.__name__}:
"{str(truncate_values_keys - NumericMetricRangeMultiBatchParameterBuilder.RECOGNIZED_TRUNCATE_DISTRIBUTION_KEYS)}" detected.
"""
            )
        self._truncate_values = truncate_values

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
         2. Set up metric_domain_kwargs and metric_value_kwargs (using configuration and/or variables and parameters).
         3. Instantiate the Validator object corresponding to BatchRequest (with a temporary expectation_suite_name) in
            order to have access to all Batch objects, on each of which the specified metric_name will be computed.
         4. Perform metric computations and obtain the result in the array-like form (one metric value per each Batch).
         5. Using the configured directives and heuristics, determine whether or not the ranges should be clipped.
         6. Using the configured directives and heuristics, determine if return values should be rounded to an integer.
         7. Convert the list of floating point metric computation results to a numpy array (for further computations).
         Steps 8 -- 10 are for the "oneshot" sampling method only (the "bootstrap" method achieves same automatically):
         8. Compute the mean and the standard deviation of the metric (aggregated over all the gathered Batch objects).
         9. Compute number of standard deviations (as floating point) needed (around the mean) to achieve the specified
            confidence_level (note that confidence_level of 1.0 would result in infinite number of standard deviations,
            hence it is "nudged" by small quantity "epsilon" below 1.0 if confidence_level of 1.0 appears as argument).
            (Please refer to "https://en.wikipedia.org/wiki/Normal_distribution" and references therein for background.)
        10. Compute the "band" around the mean as the min_value and max_value (to be used in ExpectationConfiguration).
        11. Return ConfidenceInterval([low, high]) for the desired metric as estimated by the specified sampling method.
        12. Set up the arguments and call build_parameter_container() to store the parameter as part of "rule state".
        """
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

        # Obtain confidence_level from rule state (i.e., variables and parameters); from instance variable otherwise.
        confidence_level: Union[
            Any, str
        ] = get_parameter_value_and_validate_return_type(
            domain=domain,
            parameter_reference=self._confidence_level,
            expected_return_type=(int, float),
            variables=variables,
            parameters=parameters,
        )
        if not (0.0 <= confidence_level <= 1.0):
            raise ge_exceptions.ProfilerExecutionError(
                message=f"False-Positive Rate for {self.__class__.__name__} is outside of [0.0, 1.0] closed interval."
            )

        if np.isclose(confidence_level, 1.0):
            confidence_level = confidence_level - NP_EPSILON

        validator: Validator = self.get_validator(
            domain=domain,
            variables=variables,
            parameters=parameters,
        )

        batch_ids: Optional[List[str]] = self.get_batch_ids(
            domain=domain,
            variables=variables,
            parameters=parameters,
        )
        if not batch_ids:
            raise ge_exceptions.ProfilerExecutionError(
                message=f"Utilizing a {self.__class__.__name__} requires a non-empty list of batch identifiers."
            )

        metric_computation_result: Dict[
            str, Union[Union[np.ndarray, List[Union[Any, Number]]], Dict[str, Any]]
        ] = self.get_metrics(
            batch_ids=batch_ids,
            validator=validator,
            metric_name=self._metric_name,
            metric_domain_kwargs=self._metric_domain_kwargs,
            metric_value_kwargs=self._metric_value_kwargs,
            enforce_numeric_metric=self._enforce_numeric_metric,
            replace_nan_with_zero=self._replace_nan_with_zero,
            domain=domain,
            variables=variables,
            parameters=parameters,
        )
        metric_values: Union[
            np.ndarray, List[Union[Any, Number]]
        ] = metric_computation_result["value"]
        details: Dict[str, Any] = metric_computation_result["details"]

        truncate_values: Dict[
            str, Union[Optional[int], Optional[float]]
        ] = self._get_truncate_values_using_heuristics(
            metric_values=metric_values,
            domain=domain,
            variables=variables,
            parameters=parameters,
        )
        lower_bound: Optional[Union[int, float]] = truncate_values.get("lower_bound")
        upper_bound: Optional[Union[int, float]] = truncate_values.get("upper_bound")

        round_decimals: int = self._get_round_decimals_using_heuristics(
            metric_values=metric_values,
            domain=domain,
            variables=variables,
            parameters=parameters,
        )

        metric_values = np.array(metric_values, dtype=np.float64)

        confidence_interval: ConfidenceInterval
        if sampling_method == "bootstrap":
            confidence_interval = self._get_bootstrap_confidence_interval(
                metric_values=metric_values,
                confidence_level=confidence_level,
                domain=domain,
                variables=variables,
                parameters=parameters,
            )
        else:
            confidence_interval = self._get_oneshot_confidence_interval(
                metric_values=metric_values,
                confidence_level=confidence_level,
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
                "details": details,
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
        confidence_level: np.float64,
        domain: Domain,
        *,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
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

        # noinspection PyPep8Naming
        BootstrapResult = make_dataclass(
            "BootstrapResult", ["confidence_interval", "standard_error"]
        )

        bootstrap_result: BootstrapResult
        if num_bootstrap_samples is None:
            bootstrap_result = bootstrap(
                bootstrap_samples,
                np.mean,
                vectorized=False,
                confidence_level=confidence_level,
                random_state=rng,
            )
        else:
            bootstrap_result = bootstrap(
                bootstrap_samples,
                np.mean,
                vectorized=False,
                confidence_level=confidence_level,
                n_resamples=num_bootstrap_samples,
                random_state=rng,
            )

        confidence_interval: ConfidenceInterval = bootstrap_result.confidence_interval

        confidence_interval_low: np.float64 = confidence_interval.low
        confidence_interval_high: np.float64 = confidence_interval.high

        std: Union[np.ndarray, np.float64] = bootstrap_result.standard_error

        stds_multiplier: np.float64 = NP_SQRT_2 * special.erfinv(confidence_level)
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
        confidence_level: np.float64,
    ):
        mean: Union[np.ndarray, np.float64] = np.mean(metric_values)
        std: Union[np.ndarray, np.float64] = self._standard_error(samples=metric_values)

        stds_multiplier: np.float64 = NP_SQRT_2 * special.erfinv(confidence_level)
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

    def _get_truncate_values_using_heuristics(
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
        # Obtain truncate_values directive from rule state (i.e., variables and parameters); from instance variable otherwise.
        truncate_values: Dict[
            str, Union[Optional[int], Optional[float]]
        ] = get_parameter_value_and_validate_return_type(
            domain=domain,
            parameter_reference=self._truncate_values,
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
                for distribution_boundary in truncate_values.values()
            ]
        ):
            raise ge_exceptions.ProfilerExecutionError(
                message=f"""The directive "truncate_values" for {self.__class__.__name__} must specify the
[lower_bound, upper_bound] closed interval, where either boundary is a numeric value (or None).
"""
            )

        lower_bound: Optional[Union[int, float]] = truncate_values.get("lower_bound")
        upper_bound: Optional[Union[int, float]] = truncate_values.get("upper_bound")
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
