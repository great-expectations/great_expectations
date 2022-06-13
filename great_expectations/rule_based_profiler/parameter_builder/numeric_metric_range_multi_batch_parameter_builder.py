import copy
import itertools
import warnings
from numbers import Number
from typing import Any, Callable, Dict, List, Optional, Union, cast

import numpy as np

import great_expectations.exceptions as ge_exceptions
from great_expectations.rule_based_profiler.config import ParameterBuilderConfig
from great_expectations.rule_based_profiler.helpers.util import (
    NP_EPSILON,
    compute_bootstrap_quantiles_point_estimate,
    compute_kde_quantiles_point_estimate,
    compute_quantiles,
    get_parameter_value_and_validate_return_type,
    integer_semantic_domain_type,
)
from great_expectations.rule_based_profiler.parameter_builder import (
    MetricMultiBatchParameterBuilder,
)
from great_expectations.rule_based_profiler.types import (
    FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY,
    FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY,
    Domain,
    MetricValues,
    NumericRangeEstimationResult,
    ParameterContainer,
    ParameterNode,
)
from great_expectations.rule_based_profiler.types.numeric_range_estimation_result import (
    NUM_HISTOGRAM_BINS,
)
from great_expectations.types.attributes import Attributes
from great_expectations.util import is_numeric

MAX_DECIMALS: int = 9


class NumericMetricRangeMultiBatchParameterBuilder(MetricMultiBatchParameterBuilder):
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
        "kde",
    }

    RECOGNIZED_N_RESAMPLES_SAMPLING_METHOD_NAMES: set = {
        "bootstrap",
        "kde",
    }

    DEFAULT_BOOTSTRAP_NUM_RESAMPLES: int = 9999
    DEFAULT_KDE_NUM_RESAMPLES: int = 9999

    DEFAULT_KDE_BW_METHOD: Union[str, float, Callable] = "scott"

    RECOGNIZED_TRUNCATE_DISTRIBUTION_KEYS: set = {
        "lower_bound",
        "upper_bound",
    }

    RECOGNIZED_QUANTILE_STATISTIC_INTERPOLATION_METHODS: set = {
        "auto",
        "nearest",
        "linear",
    }

    def __init__(
        self,
        name: str,
        metric_name: Optional[str] = None,
        metric_domain_kwargs: Optional[Union[str, dict]] = None,
        metric_value_kwargs: Optional[Union[str, dict]] = None,
        enforce_numeric_metric: Union[str, bool] = True,
        replace_nan_with_zero: Union[str, bool] = True,
        reduce_scalar_metric: Union[str, bool] = True,
        false_positive_rate: Union[str, float] = 5.0e-2,
        quantile_statistic_interpolation_method: str = "auto",
        estimator: str = "bootstrap",
        n_resamples: Optional[Union[str, int]] = None,
        bw_method: Optional[Union[str, float, Callable]] = None,
        random_seed: Optional[Union[str, int]] = None,
        include_estimator_samples_histogram_in_details: Union[str, bool] = False,
        truncate_values: Optional[
            Union[str, Dict[str, Union[Optional[int], Optional[float]]]]
        ] = None,
        round_decimals: Optional[Union[str, int]] = None,
        evaluation_parameter_builder_configs: Optional[
            List[ParameterBuilderConfig]
        ] = None,
        json_serialize: Union[str, bool] = True,
        data_context: Optional["BaseDataContext"] = None,  # noqa: F821
    ) -> None:
        """
        Args:
            name: the name of this parameter -- this is user-specified parameter name (from configuration); it is not
                the fully-qualified parameter name; a fully-qualified parameter name must start with "$parameter."
                and may contain one or more subsequent parts (e.g., "$parameter.<my_param_from_config>.<metric_name>").
            metric_name: the name of a metric used in MetricConfiguration (must be a supported and registered metric)
            metric_domain_kwargs: used in MetricConfiguration
            metric_value_kwargs: used in MetricConfiguration
            enforce_numeric_metric: used in MetricConfiguration to insure that metric computations return numeric values
            replace_nan_with_zero: if False, then if the computed metric gives NaN, then exception is raised; otherwise,
                if True (default), then if the computed metric gives NaN, then it is converted to the 0.0 (float) value.
            reduce_scalar_metric: if True (default), then reduces computation of 1-dimensional metric to scalar value.
            false_positive_rate: user-configured fraction between 0 and 1 expressing desired false positive rate for
                identifying unexpected values as judged by the upper- and lower- quantiles of the observed metric data.
            quantile_statistic_interpolation_method: Applicable only for the "bootstrap" sampling method --
                supplies value of (interpolation) "method" to "np.quantile()" statistic, used for confidence intervals.
            estimator: choice of the estimation algorithm: "oneshot" (one observation), "bootstrap" (default),
                or "kde" (kernel density estimation).
            n_resamples: Applicable only for the "bootstrap" and "kde" sampling methods -- if omitted (default), then
                9999 is used (default in
                "https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.bootstrap.html").
            bw_method: Applicable only for the "kde" sampling method -- if omitted (default), then "scott" is used.
                Possible values for the estimator bandwidth method are described at:
                https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.gaussian_kde.html
            random_seed: Applicable only for the "bootstrap" and "kde" sampling methods -- if omitted (default), then
                uses "np.random.choice"; otherwise, utilizes "np.random.Generator(np.random.PCG64(random_seed))".
            include_estimator_samples_histogram_in_details: Applicable only for the "bootstrap" sampling method -- if
                True, then add 10-bin histogram of bootstraps to "details"; otherwise, omit this information (default).
            truncate_values: user-configured directive for whether or not to allow the computed parameter values
                (i.e., lower_bound, upper_bound) to take on values outside the specified bounds when packaged on output.
            round_decimals: user-configured non-negative integer indicating the number of decimals of the
                rounding precision of the computed parameter values (i.e., min_value, max_value) prior to packaging them
                on output.  If omitted, then no rounding is performed, unless the computed value is already an integer.
            evaluation_parameter_builder_configs: ParameterBuilder configurations, executing and making whose respective
                ParameterBuilder objects' outputs available (as fully-qualified parameter names) is pre-requisite.
                These "ParameterBuilder" configurations help build parameters needed for this "ParameterBuilder".
            json_serialize: If True (default), convert computed value to JSON prior to saving results.
            data_context: BaseDataContext associated with this ParameterBuilder
        """
        super().__init__(
            name=name,
            metric_name=metric_name,
            metric_domain_kwargs=metric_domain_kwargs,
            metric_value_kwargs=metric_value_kwargs,
            enforce_numeric_metric=enforce_numeric_metric,
            replace_nan_with_zero=replace_nan_with_zero,
            reduce_scalar_metric=reduce_scalar_metric,
            evaluation_parameter_builder_configs=evaluation_parameter_builder_configs,
            json_serialize=json_serialize,
            data_context=data_context,
        )

        self._false_positive_rate = false_positive_rate

        self._quantile_statistic_interpolation_method = (
            quantile_statistic_interpolation_method
        )

        self._estimator = estimator

        self._n_resamples = n_resamples

        self._bw_method = bw_method

        self._random_seed = random_seed

        self._include_estimator_samples_histogram_in_details = (
            include_estimator_samples_histogram_in_details
        )

        self._round_decimals = round_decimals

        if not truncate_values:
            truncate_values = {
                "lower_bound": None,
                "upper_bound": None,
            }
        else:
            if not isinstance(truncate_values, str):
                truncate_values_keys: set = set(truncate_values.keys())
                if (
                    not truncate_values_keys
                    <= NumericMetricRangeMultiBatchParameterBuilder.RECOGNIZED_TRUNCATE_DISTRIBUTION_KEYS
                ):
                    raise ge_exceptions.ProfilerExecutionError(
                        message=f"""Unrecognized truncate_values key(s) in {self.__class__.__name__}:
"{str(truncate_values_keys - NumericMetricRangeMultiBatchParameterBuilder.RECOGNIZED_TRUNCATE_DISTRIBUTION_KEYS)}" \
detected.
"""
                    )

        self._truncate_values = truncate_values

    """
    Full getter/setter accessors for needed properties are for configuring MetricMultiBatchParameterBuilder dynamically.
    """

    @property
    def false_positive_rate(self) -> Union[str, float]:
        return self._false_positive_rate

    @property
    def quantile_statistic_interpolation_method(self) -> str:
        return self._quantile_statistic_interpolation_method

    @property
    def estimator(self) -> str:
        return self._estimator

    @property
    def n_resamples(self) -> Optional[Union[str, int]]:
        return self._n_resamples

    @property
    def bw_method(self) -> Optional[Union[str, float, Callable]]:
        return self._bw_method

    @property
    def random_seed(self) -> Optional[Union[str, int]]:
        return self._random_seed

    @property
    def include_estimator_samples_histogram_in_details(self) -> Union[str, bool]:
        return self._include_estimator_samples_histogram_in_details

    @property
    def truncate_values(
        self,
    ) -> Optional[Union[str, Dict[str, Union[Optional[int], Optional[float]]]]]:
        return self._truncate_values

    @property
    def round_decimals(self) -> Optional[Union[str, int]]:
        return self._round_decimals

    def _build_parameters(
        self,
        domain: Domain,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
        recompute_existing_parameter_values: bool = False,
    ) -> Attributes:
        """
        Builds ParameterContainer object that holds ParameterNode objects with attribute name-value pairs and details.

        Returns:
            Attributes object, containing computed parameter values and parameter computation details metadata.

         The algorithm operates according to the following steps:
         1. Obtain batch IDs of interest using BaseDataContext and BatchRequest (unless passed explicitly as argument).
         2. Set up metric_domain_kwargs and metric_value_kwargs (using configuration and/or variables and parameters).
         3. Instantiate the Validator object corresponding to BatchRequest (with a temporary expectation_suite_name) in
            order to have access to all Batch objects, on each of which the specified metric_name will be computed.
         4. Perform metric computations and obtain the result in the array-like form (one metric value per each Batch).
         5. Using the configured directives and heuristics, determine whether or not the ranges should be clipped.
         6. Using the configured directives and heuristics, determine if return values should be rounded to an integer.
         7. Convert the multi-dimensional metric computation results to a numpy array (for further computations).
         8. Compute [low, high] for the desired metric using the chosen estimator method.
         9. Return [low, high] for the desired metric as estimated by the specified sampling method.
        10. Set up the arguments and call build_parameter_container() to store the parameter as part of "rule state".
        """
        # Obtain false_positive_rate from "rule state" (i.e., variables and parameters); from instance variable otherwise.
        false_positive_rate: np.float64 = get_parameter_value_and_validate_return_type(
            domain=domain,
            parameter_reference=self.false_positive_rate,
            expected_return_type=(float, np.float64),
            variables=variables,
            parameters=parameters,
        )

        if not (0.0 <= false_positive_rate <= 1.0):
            raise ge_exceptions.ProfilerExecutionError(
                f"""false_positive_rate must be a positive decimal number between 0 and 1 inclusive [0, 1],
but {false_positive_rate} was provided."""
            )
        elif false_positive_rate <= NP_EPSILON:
            warnings.warn(
                f"""You have chosen a false_positive_rate of {false_positive_rate}, which is too close to 0.
A false_positive_rate of {NP_EPSILON} has been selected instead."""
            )
            false_positive_rate = NP_EPSILON
        elif false_positive_rate >= (1.0 - NP_EPSILON):
            warnings.warn(
                f"""You have chosen a false_positive_rate of {false_positive_rate}, which is too close to 1.
A false_positive_rate of {1.0-NP_EPSILON} has been selected instead."""
            )
            false_positive_rate = np.float64(1.0 - NP_EPSILON)

        # Compute metric value for each Batch object.
        super().build_parameters(
            domain=domain,
            variables=variables,
            parameters=parameters,
            parameter_computation_impl=super()._build_parameters,
            json_serialize=False,
            recompute_existing_parameter_values=recompute_existing_parameter_values,
        )

        # Retrieve metric values for all Batch objects.
        parameter_node: ParameterNode = get_parameter_value_and_validate_return_type(
            domain=domain,
            parameter_reference=self.fully_qualified_parameter_name,
            expected_return_type=None,
            variables=variables,
            parameters=parameters,
        )
        metric_values: MetricValues = parameter_node[
            FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY
        ]

        # Obtain estimator directive from "rule state" (i.e., variables and parameters); from instance variable otherwise.
        estimator: str = get_parameter_value_and_validate_return_type(
            domain=domain,
            parameter_reference=self.estimator,
            expected_return_type=str,
            variables=variables,
            parameters=parameters,
        )
        if (
            estimator
            not in NumericMetricRangeMultiBatchParameterBuilder.RECOGNIZED_SAMPLING_METHOD_NAMES
        ):
            raise ge_exceptions.ProfilerExecutionError(
                message=f"""The directive "estimator" for {self.__class__.__name__} can be only one of
{NumericMetricRangeMultiBatchParameterBuilder.RECOGNIZED_SAMPLING_METHOD_NAMES} ("{estimator}" was detected).
"""
            )

        round_decimals: int

        # Obtain quantile_statistic_interpolation_method directive from "rule state" (i.e., variables and parameters); from instance variable otherwise.
        quantile_statistic_interpolation_method: str = (
            get_parameter_value_and_validate_return_type(
                domain=domain,
                parameter_reference=self.quantile_statistic_interpolation_method,
                expected_return_type=str,
                variables=variables,
                parameters=parameters,
            )
        )
        if (
            quantile_statistic_interpolation_method
            not in NumericMetricRangeMultiBatchParameterBuilder.RECOGNIZED_QUANTILE_STATISTIC_INTERPOLATION_METHODS
        ):
            raise ge_exceptions.ProfilerExecutionError(
                message=f"""The directive "quantile_statistic_interpolation_method" for {self.__class__.__name__} can \
be only one of {NumericMetricRangeMultiBatchParameterBuilder.RECOGNIZED_QUANTILE_STATISTIC_INTERPOLATION_METHODS} \
("{quantile_statistic_interpolation_method}" was detected).
"""
            )

        if integer_semantic_domain_type(domain=domain):
            round_decimals = 0
        else:
            round_decimals = self._get_round_decimals_using_heuristics(
                metric_values=metric_values,
                domain=domain,
                variables=variables,
                parameters=parameters,
            )

        if quantile_statistic_interpolation_method == "auto":
            if round_decimals == 0:
                quantile_statistic_interpolation_method = "nearest"
            else:
                quantile_statistic_interpolation_method = "linear"

        estimator_func: Callable
        estimator_kwargs: dict
        if estimator == "bootstrap":
            estimator_func = self._get_bootstrap_estimate
            estimator_kwargs = {
                "false_positive_rate": false_positive_rate,
                "quantile_statistic_interpolation_method": quantile_statistic_interpolation_method,
                "n_resamples": self.n_resamples,
                "random_seed": self.random_seed,
            }
        elif estimator == "kde":
            estimator_func = self._get_kde_estimate
            estimator_kwargs = {
                "false_positive_rate": false_positive_rate,
                "quantile_statistic_interpolation_method": quantile_statistic_interpolation_method,
                "n_resamples": self.n_resamples,
                "bw_method": self.bw_method,
                "random_seed": self.random_seed,
            }
        else:
            estimator_func = self._get_deterministic_estimate
            estimator_kwargs = {
                "false_positive_rate": false_positive_rate,
                "quantile_statistic_interpolation_method": quantile_statistic_interpolation_method,
            }

        numeric_range_estimation_result: NumericRangeEstimationResult = (
            self._estimate_metric_value_range(
                metric_values=metric_values,
                estimator_func=estimator_func,
                round_decimals=round_decimals,
                domain=domain,
                variables=variables,
                parameters=parameters,
                **estimator_kwargs,
            )
        )

        value_range: np.ndarray = numeric_range_estimation_result.value_range
        details: Dict[str, Any] = copy.deepcopy(
            parameter_node[FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY]
        )

        # Obtain include_estimator_samples_histogram_in_details from "rule state" (i.e., variables and parameters); from instance variable otherwise.
        include_estimator_samples_histogram_in_details: bool = (
            get_parameter_value_and_validate_return_type(
                domain=domain,
                parameter_reference=self.include_estimator_samples_histogram_in_details,
                expected_return_type=bool,
                variables=variables,
                parameters=parameters,
            )
        )

        if include_estimator_samples_histogram_in_details:
            details[
                "estimation_histogram"
            ] = numeric_range_estimation_result.estimation_histogram

        return Attributes(
            {
                FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY: value_range,
                FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY: details,
            }
        )

    def _estimate_metric_value_range(
        self,
        metric_values: np.ndarray,
        estimator_func: Callable,
        round_decimals: int,
        domain: Optional[Domain] = None,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
        **kwargs,
    ) -> NumericRangeEstimationResult:
        """
        This method accepts an estimator Callable and data samples in the format "N x R^m", where "N" (most significant
        dimension) is the number of measurements (e.g., one per Batch of data), while "R^m" is the multi-dimensional
        metric, whose values are being estimated.  Thus, for each element in the "R^m" hypercube, an "N"-dimensional
        vector of sample measurements is constructed and given to the estimator to apply its specific algorithm for
        computing the range of values in this vector.  Estimator algorithms differ based on their use of data samples.
        """
        truncate_values: Dict[str, Number] = self._get_truncate_values_using_heuristics(
            metric_values=metric_values,
            domain=domain,
            variables=variables,
            parameters=parameters,
        )
        lower_bound: Optional[float] = truncate_values.get("lower_bound")
        upper_bound: Optional[float] = truncate_values.get("upper_bound")

        min_value: Number
        max_value: Number

        # Outer-most dimension is data samples (e.g., one per Batch); the rest are dimensions of the actual metric.
        metric_value_shape: tuple = metric_values.shape[1:]

        # Generate all permutations of indexes for accessing every element of the multi-dimensional metric.
        metric_value_shape_idx: int
        axes: List[np.ndarray] = [
            np.indices(dimensions=(metric_value_shape_idx,))[0]
            for metric_value_shape_idx in metric_value_shape
        ]
        metric_value_indices: List[tuple] = list(itertools.product(*tuple(axes)))

        # Generate all permutations of indexes for accessing estimates of every element of the multi-dimensional metric.
        # Prefixing multi-dimensional index with "(slice(None, None, None),)" is equivalent to "[:,]" access.
        metric_value_idx: tuple
        metric_value_vector_indices: List[tuple] = [
            (slice(None, None, None),) + metric_value_idx
            for metric_value_idx in metric_value_indices
        ]

        # Initialize value range estimate for multi-dimensional metric to all trivial values (to be updated in situ).
        # Since range includes min and max values, value range estimate contains 2-element least-significant dimension.
        metric_value_range_shape: tuple = metric_value_shape + (2,)
        metric_value_range: np.ndarray = np.zeros(shape=metric_value_range_shape)
        # Initialize observed_values for multi-dimensional metric to all trivial values (to be updated in situ).
        # Since "numpy.histogram()" uses 10 bins by default, histogram contains 10-element least-significant dimension.
        estimation_histogram_shape: tuple = metric_value_shape + (10,)
        estimation_histogram: np.ndarray = np.zeros(shape=estimation_histogram_shape)

        metric_value_vector: np.ndarray
        metric_value_range_min_idx: tuple
        metric_value_range_max_idx: tuple
        numeric_range_estimation_result: NumericRangeEstimationResult
        # Traverse indices of sample vectors corresponding to every element of multi-dimensional metric.
        for metric_value_idx in metric_value_vector_indices:
            # Obtain "N"-element-long vector of samples for each element of multi-dimensional metric.
            metric_value_vector = metric_values[metric_value_idx]
            if np.all(np.isclose(metric_value_vector, metric_value_vector[0])):
                # Computation is unnecessary if distribution is degenerate.
                numeric_range_estimation_result = NumericRangeEstimationResult(
                    estimation_histogram=np.histogram(
                        a=metric_value_vector, bins=NUM_HISTOGRAM_BINS
                    )[0],
                    value_range=np.asarray(
                        [metric_value_vector[0], metric_value_vector[0]]
                    ),
                )
            else:
                # Compute low and high estimates for vector of samples for given element of multi-dimensional metric.
                numeric_range_estimation_result = estimator_func(
                    metric_values=metric_value_vector,
                    domain=domain,
                    variables=variables,
                    parameters=parameters,
                    **kwargs,
                )

            min_value = numeric_range_estimation_result.value_range[0]
            if lower_bound is not None:
                min_value = max(cast(float, min_value), lower_bound)

            max_value = numeric_range_estimation_result.value_range[1]
            if upper_bound is not None:
                max_value = min(cast(float, max_value), upper_bound)

            # Obtain index of metric element (by discarding "N"-element samples dimension).
            metric_value_idx = metric_value_idx[1:]

            # Compute indices for min and max value range estimates.
            metric_value_range_min_idx = metric_value_idx + (
                slice(0, 1, None),
            )  # appends "[0]" element
            metric_value_range_max_idx = metric_value_idx + (
                slice(1, 2, None),
            )  # appends "[1]" element

            # Store computed estimation_histogram into allocated range estimate for multi-dimensional metric.
            estimation_histogram[
                metric_value_idx
            ] = numeric_range_estimation_result.estimation_histogram
            # Store computed min and max value estimates into allocated range estimate for multi-dimensional metric.
            metric_value_range[metric_value_range_min_idx] = round(
                cast(float, min_value), round_decimals
            )
            metric_value_range[metric_value_range_max_idx] = round(
                cast(float, max_value), round_decimals
            )

        # As a simplification, apply reduction to scalar in case of one-dimensional metric (for convenience).
        if metric_value_range.shape[0] == 1:
            estimation_histogram = estimation_histogram[0]
            metric_value_range = metric_value_range[0]

        if round_decimals == 0:
            metric_value_range = metric_value_range.astype(np.int64)

        return NumericRangeEstimationResult(
            estimation_histogram=estimation_histogram,
            value_range=metric_value_range,
        )

    def _get_truncate_values_using_heuristics(
        self,
        metric_values: np.ndarray,
        domain: Domain,
        *,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
    ) -> Dict[str, Union[Optional[int], Optional[float]]]:
        # Obtain truncate_values directive from "rule state" (i.e., variables and parameters); from instance variable otherwise.
        truncate_values: Dict[
            str, Optional[Number]
        ] = get_parameter_value_and_validate_return_type(
            domain=domain,
            parameter_reference=self.truncate_values,
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

        lower_bound: Optional[Number] = truncate_values.get("lower_bound")
        upper_bound: Optional[Number] = truncate_values.get("upper_bound")

        if lower_bound is None and np.all(np.greater(metric_values, NP_EPSILON)):
            lower_bound = 0.0

        if upper_bound is None and np.all(np.less(metric_values, (-NP_EPSILON))):
            upper_bound = 0.0

        return {
            "lower_bound": lower_bound,
            "upper_bound": upper_bound,
        }

    def _get_round_decimals_using_heuristics(
        self,
        metric_values: np.ndarray,
        domain: Domain,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
    ) -> int:
        # Obtain round_decimals directive from "rule state" (i.e., variables and parameters); from instance variable otherwise.
        round_decimals: Optional[int] = get_parameter_value_and_validate_return_type(
            domain=domain,
            parameter_reference=self.round_decimals,
            expected_return_type=None,
            variables=variables,
            parameters=parameters,
        )
        if round_decimals is None:
            round_decimals = MAX_DECIMALS
        else:
            if not isinstance(round_decimals, int) or (round_decimals < 0):
                raise ge_exceptions.ProfilerExecutionError(
                    message=f"""The directive "round_decimals" for {self.__class__.__name__} can be 0 or a
positive integer, or must be omitted (or set to None).
"""
                )

        if np.issubdtype(metric_values.dtype, np.integer):
            round_decimals = 0

        return round_decimals

    @staticmethod
    def _get_bootstrap_estimate(
        metric_values: np.ndarray,
        domain: Domain,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
        **kwargs,
    ) -> NumericRangeEstimationResult:
        # Obtain n_resamples override from "rule state" (i.e., variables and parameters); from instance variable otherwise.
        n_resamples: Optional[int] = get_parameter_value_and_validate_return_type(
            domain=domain,
            parameter_reference=kwargs.get("n_resamples"),
            expected_return_type=None,
            variables=variables,
            parameters=parameters,
        )

        if n_resamples is None:
            n_resamples = (
                NumericMetricRangeMultiBatchParameterBuilder.DEFAULT_BOOTSTRAP_NUM_RESAMPLES
            )

        # Obtain random_seed override from "rule state" (i.e., variables and parameters); from instance variable otherwise.
        random_seed: Optional[int] = get_parameter_value_and_validate_return_type(
            domain=domain,
            parameter_reference=kwargs.get("random_seed"),
            expected_return_type=None,
            variables=variables,
            parameters=parameters,
        )

        false_positive_rate: np.float64 = kwargs.get("false_positive_rate", 5.0e-2)
        quantile_statistic_interpolation_method: str = kwargs.get(
            "quantile_statistic_interpolation_method"
        )

        return compute_bootstrap_quantiles_point_estimate(
            metric_values=metric_values,
            false_positive_rate=false_positive_rate,
            quantile_statistic_interpolation_method=quantile_statistic_interpolation_method,
            n_resamples=n_resamples,
            random_seed=random_seed,
        )

    @staticmethod
    def _get_kde_estimate(
        metric_values: np.ndarray,
        domain: Domain,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
        **kwargs,
    ) -> NumericRangeEstimationResult:
        # Obtain n_resamples override from "rule state" (i.e., variables and parameters); from instance variable otherwise.
        n_resamples: Optional[int] = get_parameter_value_and_validate_return_type(
            domain=domain,
            parameter_reference=kwargs.get("n_resamples"),
            expected_return_type=None,
            variables=variables,
            parameters=parameters,
        )

        if n_resamples is None:
            n_resamples = (
                NumericMetricRangeMultiBatchParameterBuilder.DEFAULT_KDE_NUM_RESAMPLES
            )

        # Obtain bw_method override from "rule state" (i.e., variables and parameters); from instance variable otherwise.
        bw_method: Optional[
            Union[str, float, Callable]
        ] = get_parameter_value_and_validate_return_type(
            domain=domain,
            parameter_reference=kwargs.get("bw_method"),
            expected_return_type=None,
            variables=variables,
            parameters=parameters,
        )

        if bw_method is None:
            bw_method = (
                NumericMetricRangeMultiBatchParameterBuilder.DEFAULT_KDE_BW_METHOD
            )

        # Obtain random_seed override from "rule state" (i.e., variables and parameters); from instance variable otherwise.
        random_seed: Optional[int] = get_parameter_value_and_validate_return_type(
            domain=domain,
            parameter_reference=kwargs.get("random_seed"),
            expected_return_type=None,
            variables=variables,
            parameters=parameters,
        )

        false_positive_rate: np.float64 = kwargs.get("false_positive_rate", 5.0e-2)
        quantile_statistic_interpolation_method: str = kwargs.get(
            "quantile_statistic_interpolation_method"
        )

        return compute_kde_quantiles_point_estimate(
            metric_values=metric_values,
            false_positive_rate=false_positive_rate,
            quantile_statistic_interpolation_method=quantile_statistic_interpolation_method,
            n_resamples=n_resamples,
            bw_method=bw_method,
            random_seed=random_seed,
        )

    @staticmethod
    def _get_deterministic_estimate(
        metric_values: np.ndarray,
        **kwargs,
    ) -> NumericRangeEstimationResult:
        false_positive_rate: np.float64 = kwargs.get("false_positive_rate", 5.0e-2)
        quantile_statistic_interpolation_method: str = kwargs.get(
            "quantile_statistic_interpolation_method"
        )

        return compute_quantiles(
            metric_values=metric_values,
            false_positive_rate=false_positive_rate,
            quantile_statistic_interpolation_method=quantile_statistic_interpolation_method,
        )
