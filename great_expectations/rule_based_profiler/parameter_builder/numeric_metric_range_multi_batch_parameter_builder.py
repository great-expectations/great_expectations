from __future__ import annotations

import copy
import datetime
import itertools
import logging
from numbers import Number
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Dict,
    List,
    Optional,
    Set,
    Union,
)

import numpy as np

import great_expectations.exceptions as gx_exceptions
from great_expectations.core.domain import Domain  # noqa: TCH001
from great_expectations.rule_based_profiler.config import (
    ParameterBuilderConfig,  # noqa: TCH001
)
from great_expectations.rule_based_profiler.estimators.bootstrap_numeric_range_estimator import (
    BootstrapNumericRangeEstimator,
)
from great_expectations.rule_based_profiler.estimators.exact_numeric_range_estimator import (
    ExactNumericRangeEstimator,
)
from great_expectations.rule_based_profiler.estimators.kde_numeric_range_estimator import (
    KdeNumericRangeEstimator,
)
from great_expectations.rule_based_profiler.estimators.numeric_range_estimation_result import (
    NUM_HISTOGRAM_BINS,
    NumericRangeEstimationResult,
)
from great_expectations.rule_based_profiler.estimators.numeric_range_estimator import (
    NumericRangeEstimator,  # noqa: TCH001
)
from great_expectations.rule_based_profiler.estimators.quantiles_numeric_range_estimator import (
    QuantilesNumericRangeEstimator,
)
from great_expectations.rule_based_profiler.helpers.util import (
    NP_EPSILON,
    build_numeric_range_estimation_result,
    datetime_semantic_domain_type,
    get_parameter_value_and_validate_return_type,
    integer_semantic_domain_type,
)
from great_expectations.rule_based_profiler.metric_computation_result import (
    MetricValues,  # noqa: TCH001
)
from great_expectations.rule_based_profiler.parameter_builder import (
    MetricMultiBatchParameterBuilder,
)
from great_expectations.rule_based_profiler.parameter_container import (
    FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY,
    FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY,
    RAW_PARAMETER_KEY,
    ParameterContainer,
    ParameterNode,
)
from great_expectations.types.attributes import Attributes
from great_expectations.util import (
    convert_ndarray_decimal_to_float_dtype,
    does_ndarray_contain_decimal_dtype,
    is_ndarray_datetime_dtype,
    is_numeric,
)

if TYPE_CHECKING:
    from great_expectations.data_context.data_context.abstract_data_context import (
        AbstractDataContext,
    )


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

MAX_DECIMALS: int = 9


class NumericMetricRangeMultiBatchParameterBuilder(MetricMultiBatchParameterBuilder):
    """
    A Multi-Batch implementation for obtaining the range estimation bounds for a resolved (evaluated) numeric metric,
    using domain_kwargs, value_kwargs, metric_name, and false_positive_rate (tolerance) as arguments.

    This Multi-Batch ParameterBuilder is general in the sense that any metric that computes numbers can be accommodated.
    On the other hand, it is specific in the sense that the parameter names will always have the semantics of numeric
    ranges, which will incorporate the requirements, imposed by the configured false_positive_rate tolerances.

    The implementation supports four methods of estimating parameter values from data:
    * quantiles -- assumes that metric values, computed on batch data, are normally distributed and computes the mean
      and the standard error using the queried batches as the single sample of the distribution.
    * exact -- uses the minimum and maximum observations for range boundaries.
    * bootstrap -- a statistical resampling technique (see "https://en.wikipedia.org/wiki/Bootstrapping_(statistics)").
    * kde -- a statistical technique that fits a gaussian to the distribution and resamples from it.
    """

    RECOGNIZED_SAMPLING_METHOD_NAMES: set = {
        "bootstrap",
        "exact",
        "kde",
        "quantiles",
    }

    RECOGNIZED_TRUNCATE_DISTRIBUTION_KEYS: set = {
        "lower_bound",
        "upper_bound",
    }

    METRIC_NAMES_EXEMPT_FROM_VALUE_ROUNDING: set = {
        "column.unique_proportion",
    }

    exclude_field_names: ClassVar[
        Set[str]
    ] = MetricMultiBatchParameterBuilder.exclude_field_names | {
        "single_batch_mode",
    }

    def __init__(  # noqa: PLR0913
        self,
        name: str,
        metric_name: Optional[str] = None,
        metric_multi_batch_parameter_builder_name: Optional[str] = None,
        metric_domain_kwargs: Optional[Union[str, dict]] = None,
        metric_value_kwargs: Optional[Union[str, dict]] = None,
        enforce_numeric_metric: Union[str, bool] = True,
        replace_nan_with_zero: Union[str, bool] = True,
        reduce_scalar_metric: Union[str, bool] = True,
        false_positive_rate: Optional[Union[str, float]] = None,
        estimator: str = "bootstrap",
        n_resamples: Optional[Union[str, int]] = None,
        random_seed: Optional[Union[str, int]] = None,
        quantile_statistic_interpolation_method: str = "auto",
        quantile_bias_correction: Union[str, bool] = False,
        quantile_bias_std_error_ratio_threshold: Optional[Union[str, float]] = None,
        bw_method: Optional[Union[str, float, Callable]] = None,
        include_estimator_samples_histogram_in_details: Union[str, bool] = False,
        truncate_values: Optional[
            Union[str, Dict[str, Union[Optional[int], Optional[float]]]]
        ] = None,
        round_decimals: Optional[Union[str, int]] = None,
        evaluation_parameter_builder_configs: Optional[
            List[ParameterBuilderConfig]
        ] = None,
        data_context: Optional[AbstractDataContext] = None,
    ) -> None:
        """
        Args:
            name: the name of this parameter -- this is user-specified parameter name (from configuration); it is not
                the fully-qualified parameter name; a fully-qualified parameter name must start with "$parameter."
                and may contain one or more subsequent parts (e.g., "$parameter.<my_param_from_config>.<metric_name>").
            metric_name: the name of a metric used in MetricConfiguration (must be a supported and registered metric)
            metric_multi_batch_parameter_builder_name: name of parameter that computes "metric_name" (for every Batch).
            metric_domain_kwargs: used in MetricConfiguration
            metric_value_kwargs: used in MetricConfiguration
            enforce_numeric_metric: used in MetricConfiguration to insure that metric computations return numeric values
            replace_nan_with_zero: if False, then if the computed metric gives NaN, then exception is raised; otherwise,
                if True (default), then if the computed metric gives NaN, then it is converted to the 0.0 (float) value.
            reduce_scalar_metric: if True (default), then reduces computation of 1-dimensional metric to scalar value.
            false_positive_rate: user-configured fraction between 0 and 1 expressing desired false positive rate for
                identifying unexpected values as judged by the upper- and lower- quantiles of the observed metric data.
            estimator: choice of the estimation algorithm: "quantiles", "bootstrap", "exact"
                (deterministic, incorporating entire observed value range), or "kde" (kernel density estimation).
            n_resamples: Applicable only for the "bootstrap" and "kde" sampling methods -- if omitted (default), then
                9999 is used (default in
                "https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.bootstrap.html").
            random_seed: Applicable only for the "bootstrap" and "kde" sampling methods -- if omitted (default), then
                uses "np.random.choice"; otherwise, utilizes "np.random.Generator(np.random.PCG64(random_seed))".
            quantile_statistic_interpolation_method: Supplies value of (interpolation) "method" to "np.quantile()"
                statistic, used for confidence intervals.
            quantile_bias_correction: Applicable only for the "bootstrap" sampling method -- if omitted (default), then
                False is used (bias correction is disabled) and quantile_bias_std_error_ratio_threshold will have no
                effect.
            quantile_bias_std_error_ratio_threshold: Applicable only for the "bootstrap" sampling method -- if omitted
                (default), then 0.25 is used (as minimum ratio of bias to standard error for applying bias correction).
            bw_method: Applicable only for the "kde" sampling method -- if omitted (default), then "scott" is used.
                Possible values for the estimator bandwidth method are described at:
                https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.gaussian_kde.html
            include_estimator_samples_histogram_in_details: Applicable only for the "bootstrap" and "kde" sampling
                methods -- if True, then add 10-bin histogram of metric samples to "details"; otherwise, omit (default).
            truncate_values: user-configured directive for whether or not to allow the computed parameter values
                (i.e., lower_bound, upper_bound) to take on values outside the specified bounds when packaged on output.
            round_decimals: user-configured non-negative integer indicating the number of decimals of the
                rounding precision of the computed parameter values (i.e., min_value, max_value) prior to packaging them
                on output.  If omitted, then no rounding is performed, unless the computed value is already an integer.
            evaluation_parameter_builder_configs: ParameterBuilder configurations, executing and making whose respective
                ParameterBuilder objects' outputs available (as fully-qualified parameter names) is pre-requisite.
                These "ParameterBuilder" configurations help build parameters needed for this "ParameterBuilder".
            data_context: AbstractDataContext associated with this ParameterBuilder
        """
        super().__init__(
            name=name,
            metric_name=metric_name,
            metric_domain_kwargs=metric_domain_kwargs,
            metric_value_kwargs=metric_value_kwargs,
            single_batch_mode=False,
            enforce_numeric_metric=enforce_numeric_metric,
            replace_nan_with_zero=replace_nan_with_zero,
            reduce_scalar_metric=reduce_scalar_metric,
            evaluation_parameter_builder_configs=evaluation_parameter_builder_configs,
            data_context=data_context,
        )

        self._metric_multi_batch_parameter_builder_name = (
            metric_multi_batch_parameter_builder_name
        )

        if false_positive_rate is None:
            false_positive_rate = 5.0e-2

        self._false_positive_rate = false_positive_rate

        self._estimator = estimator

        self._n_resamples = n_resamples

        self._random_seed = random_seed

        self._quantile_statistic_interpolation_method = (
            quantile_statistic_interpolation_method
        )

        self._quantile_bias_correction = quantile_bias_correction

        self._quantile_bias_std_error_ratio_threshold = (
            quantile_bias_std_error_ratio_threshold
        )

        self._bw_method = bw_method

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
            if not isinstance(truncate_values, str):  # noqa: PLR5501
                truncate_values_keys: set = set(truncate_values.keys())
                if (
                    not truncate_values_keys
                    <= NumericMetricRangeMultiBatchParameterBuilder.RECOGNIZED_TRUNCATE_DISTRIBUTION_KEYS
                ):
                    raise gx_exceptions.ProfilerExecutionError(
                        message=f"""Unrecognized truncate_values key(s) in {self.__class__.__name__}:
"{str(truncate_values_keys - NumericMetricRangeMultiBatchParameterBuilder.RECOGNIZED_TRUNCATE_DISTRIBUTION_KEYS)}" \
detected.
"""
                    )

        self._truncate_values = truncate_values

    @property
    def metric_multi_batch_parameter_builder_name(self) -> str:
        return self._metric_multi_batch_parameter_builder_name

    @property
    def false_positive_rate(self) -> Union[str, float]:
        return self._false_positive_rate

    @property
    def estimator(self) -> str:
        return self._estimator

    @property
    def n_resamples(self) -> Optional[Union[str, int]]:
        return self._n_resamples

    @property
    def random_seed(self) -> Optional[Union[str, int]]:
        return self._random_seed

    @property
    def quantile_statistic_interpolation_method(self) -> str:
        return self._quantile_statistic_interpolation_method

    @property
    def quantile_bias_correction(self) -> Union[str, bool]:
        return self._quantile_bias_correction

    @property
    def quantile_bias_std_error_ratio_threshold(self) -> Optional[Union[str, float]]:
        return self._quantile_bias_std_error_ratio_threshold

    @property
    def bw_method(self) -> Optional[Union[str, float, Callable]]:
        return self._bw_method

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
        runtime_configuration: Optional[dict] = None,
    ) -> Attributes:
        """
        Builds ParameterContainer object that holds ParameterNode objects with attribute name-value pairs and details.

        Returns:
            Attributes object, containing computed parameter values and parameter computation details metadata.

         The algorithm operates according to the following steps:
         1. Obtain batch IDs of interest using AbstractDataContext and BatchRequest (unless passed explicitly as argument).
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
        parameter_reference: str
        if self.metric_multi_batch_parameter_builder_name:
            # Obtain metric_multi_batch_parameter_builder_name from "rule state" (i.e., variables and parameters); from instance variable otherwise.
            metric_multi_batch_parameter_builder_name: str = (
                get_parameter_value_and_validate_return_type(
                    domain=domain,
                    parameter_reference=self.metric_multi_batch_parameter_builder_name,
                    expected_return_type=str,
                    variables=variables,
                    parameters=parameters,
                )
            )
            parameter_reference = (
                f"{RAW_PARAMETER_KEY}{metric_multi_batch_parameter_builder_name}"
            )
        else:
            # Compute metric value for each Batch object.
            super().build_parameters(
                domain=domain,
                variables=variables,
                parameters=parameters,
                parameter_computation_impl=super()._build_parameters,
                runtime_configuration=runtime_configuration,
            )
            parameter_reference = self.raw_fully_qualified_parameter_name

        # Retrieve metric values for all Batch objects.
        parameter_node: ParameterNode = get_parameter_value_and_validate_return_type(
            domain=domain,
            parameter_reference=parameter_reference,
            expected_return_type=None,
            variables=variables,
            parameters=parameters,
        )
        metric_values: MetricValues = parameter_node[
            FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY
        ]

        round_decimals: int
        if (
            self.metric_name
            not in NumericMetricRangeMultiBatchParameterBuilder.METRIC_NAMES_EXEMPT_FROM_VALUE_ROUNDING
            and integer_semantic_domain_type(domain=domain)
        ):
            round_decimals = 0
        else:
            round_decimals = self._get_round_decimals_using_heuristics(
                metric_values=metric_values,
                domain=domain,
                variables=variables,
                parameters=parameters,
            )

        numeric_range_estimator: NumericRangeEstimator = (
            self._build_numeric_range_estimator(
                round_decimals=round_decimals,
                domain=domain,
                variables=variables,
                parameters=parameters,
            )
        )
        numeric_range_estimation_result: NumericRangeEstimationResult = (
            self._estimate_metric_value_range(
                metric_values=metric_values,
                numeric_range_estimator=numeric_range_estimator,
                round_decimals=round_decimals,
                domain=domain,
                variables=variables,
                parameters=parameters,
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

    def _build_numeric_range_estimator(
        self,
        round_decimals: int,
        domain: Domain,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
    ) -> NumericRangeEstimator:
        """
        Determines "estimator" name and returns appropriate configured "NumericRangeEstimator" subclass instance.
        """
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
            raise gx_exceptions.ProfilerExecutionError(
                message=f"""The directive "estimator" for {self.__class__.__name__} can be only one of
{NumericMetricRangeMultiBatchParameterBuilder.RECOGNIZED_SAMPLING_METHOD_NAMES} ("{estimator}" was detected).
"""
            )

        if estimator == "exact":
            return ExactNumericRangeEstimator()

        if estimator == "quantiles":
            return QuantilesNumericRangeEstimator(
                configuration=Attributes(
                    {
                        "false_positive_rate": self.false_positive_rate,
                        "round_decimals": round_decimals,
                        "quantile_statistic_interpolation_method": self.quantile_statistic_interpolation_method,
                    }
                )
            )

        # Since complex numerical calculations do not support DateTime/TimeStamp data types, use "quantiles" estimator.
        if datetime_semantic_domain_type(domain=domain):
            logger.info(
                f'Estimator "{estimator}" does not support DateTime/TimeStamp data types (downgrading to "quantiles").'
            )
            return QuantilesNumericRangeEstimator(
                configuration=Attributes(
                    {
                        "false_positive_rate": self.false_positive_rate,
                        "round_decimals": round_decimals,
                        "quantile_statistic_interpolation_method": self.quantile_statistic_interpolation_method,
                    }
                )
            )

        if estimator == "bootstrap":
            return BootstrapNumericRangeEstimator(
                configuration=Attributes(
                    {
                        "false_positive_rate": self.false_positive_rate,
                        "round_decimals": round_decimals,
                        "n_resamples": self.n_resamples,
                        "random_seed": self.random_seed,
                        "quantile_statistic_interpolation_method": self.quantile_statistic_interpolation_method,
                        "quantile_bias_correction": self.quantile_bias_correction,
                        "quantile_bias_std_error_ratio_threshold": self.quantile_bias_std_error_ratio_threshold,
                    }
                )
            )

        if estimator == "kde":
            return KdeNumericRangeEstimator(
                configuration=Attributes(
                    {
                        "false_positive_rate": self.false_positive_rate,
                        "round_decimals": round_decimals,
                        "n_resamples": self.n_resamples,
                        "random_seed": self.random_seed,
                        "quantile_statistic_interpolation_method": self.quantile_statistic_interpolation_method,
                        "bw_method": self.bw_method,
                    }
                )
            )

        return ExactNumericRangeEstimator()

    def _estimate_metric_value_range(  # noqa: PLR0913, PLR0915
        self,
        metric_values: np.ndarray,
        numeric_range_estimator: NumericRangeEstimator,
        round_decimals: int,
        domain: Optional[Domain] = None,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
    ) -> NumericRangeEstimationResult:
        """
        This method accepts "NumericRangeEstimator" and data samples in format "N x R^m", where "N" (most significant
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
        # Initialize observed_values for multi-dimensional metric to all trivial values (to be updated in situ).
        # Return values of "numpy.histogram()", histogram and bin edges, are packaged in least-significant dimensions.
        estimation_histogram_shape: tuple = metric_value_shape + (
            2,
            NUM_HISTOGRAM_BINS + 1,
        )

        datetime_detected: bool = datetime_semantic_domain_type(
            domain=domain
        ) or self._is_metric_values_ndarray_datetime_dtype(
            metric_values=metric_values,
            metric_value_vector_indices=metric_value_vector_indices,
        )

        metric_value_range: np.ndarray
        estimation_histogram: np.ndarray
        if datetime_detected:
            metric_value_range = np.full(
                shape=metric_value_range_shape, fill_value=datetime.datetime.min
            )
            estimation_histogram = np.full(
                shape=estimation_histogram_shape, fill_value=datetime.datetime.min
            )
        else:
            if self._is_metric_values_ndarray_decimal_dtype(
                metric_values=metric_values,
                metric_value_vector_indices=metric_value_vector_indices,
            ):
                metric_values = convert_ndarray_decimal_to_float_dtype(
                    data=metric_values
                )

            metric_value_range = np.zeros(shape=metric_value_range_shape)
            estimation_histogram = np.empty(shape=estimation_histogram_shape)

        # Traverse indices of sample vectors corresponding to every element of multi-dimensional metric.
        metric_value_vector: np.ndarray
        metric_value_range_min_idx: tuple
        metric_value_range_max_idx: tuple
        metric_value_estimation_histogram_idx: tuple
        numeric_range_estimation_result: NumericRangeEstimationResult
        for metric_value_idx in metric_value_vector_indices:
            # Obtain "N"-element-long vector of samples for each element of multi-dimensional metric.
            metric_value_vector = metric_values[metric_value_idx]
            if not datetime_detected and np.all(
                np.isclose(metric_value_vector, metric_value_vector[0])
            ):
                # Computation is unnecessary if distribution is degenerate.
                numeric_range_estimation_result = build_numeric_range_estimation_result(
                    metric_values=metric_value_vector,
                    min_value=metric_value_vector[0],
                    max_value=metric_value_vector[0],
                )
            else:
                # Compute low and high estimates for vector of samples for given element of multi-dimensional metric.
                numeric_range_estimation_result = (
                    numeric_range_estimator.get_numeric_range_estimate(
                        metric_values=metric_value_vector,
                        domain=domain,
                        variables=variables,
                        parameters=parameters,
                    )
                )

            min_value = numeric_range_estimation_result.value_range[0]
            if lower_bound is not None:
                min_value = max(np.float64(min_value), np.float64(lower_bound))

            max_value = numeric_range_estimation_result.value_range[1]
            if upper_bound is not None:
                max_value = min(np.float64(max_value), np.float64(upper_bound))

            # Obtain index of metric element (by discarding "N"-element samples dimension).
            metric_value_idx = metric_value_idx[1:]  # noqa: PLW2901

            # Compute indices for metric value range min and max estimates.
            metric_value_range_min_idx = metric_value_idx + (
                slice(0, 1, None),
            )  # appends "[0]" element
            metric_value_range_max_idx = metric_value_idx + (
                slice(1, 2, None),
            )  # appends "[1]" element

            # Compute index for metric value estimation histogram.
            metric_value_estimation_histogram_idx = metric_value_idx

            # Store computed min and max value estimates into allocated range estimate for multi-dimensional metric.
            if datetime_detected:
                metric_value_range[metric_value_range_min_idx] = min_value
                metric_value_range[metric_value_range_max_idx] = max_value
            else:
                if round_decimals is None:  # noqa: PLR5501
                    metric_value_range[metric_value_range_min_idx] = np.float64(
                        min_value
                    )
                    metric_value_range[metric_value_range_max_idx] = np.float64(
                        max_value
                    )
                else:
                    metric_value_range[metric_value_range_min_idx] = round(
                        np.float64(min_value), round_decimals
                    )
                    metric_value_range[metric_value_range_max_idx] = round(
                        np.float64(max_value), round_decimals
                    )

            # Store computed estimation_histogram into allocated range estimate for multi-dimensional metric.
            estimation_histogram[
                metric_value_estimation_histogram_idx
            ] = numeric_range_estimation_result.estimation_histogram

        # As a simplification, apply reduction to scalar in case of one-dimensional metric (for convenience).
        if metric_value_range.shape[0] == 1:
            metric_value_range = metric_value_range[0]
            estimation_histogram = estimation_histogram[0]

        if round_decimals == 0:
            metric_value_range = metric_value_range.astype(np.int64)

        return NumericRangeEstimationResult(
            estimation_histogram=estimation_histogram,
            value_range=metric_value_range,
        )

    @staticmethod
    def _is_metric_values_ndarray_datetime_dtype(
        metric_values: np.ndarray,
        metric_value_vector_indices: List[tuple],
    ) -> bool:
        metric_value_vector: np.ndarray
        for metric_value_idx in metric_value_vector_indices:
            metric_value_vector = metric_values[metric_value_idx]
            if not is_ndarray_datetime_dtype(
                data=metric_value_vector,
                parse_strings_as_datetimes=True,
                fuzzy=False,
            ):
                return False

        return True

    @staticmethod
    def _is_metric_values_ndarray_decimal_dtype(
        metric_values: np.ndarray,
        metric_value_vector_indices: List[tuple],
    ) -> bool:
        metric_value_vector: np.ndarray
        for metric_value_idx in metric_value_vector_indices:
            metric_value_vector = metric_values[metric_value_idx]
            if not does_ndarray_contain_decimal_dtype(data=metric_value_vector):
                return False

        return True

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
            (
                distribution_boundary is None
                or is_numeric(value=distribution_boundary)
                or isinstance(distribution_boundary, datetime.datetime)
            )
            for distribution_boundary in truncate_values.values()
        ):
            raise gx_exceptions.ProfilerExecutionError(
                message=f"""The directive "truncate_values" for {self.__class__.__name__} must specify the
[lower_bound, upper_bound] closed interval, where either boundary is a numeric value (or None).
"""
            )

        lower_bound: Optional[Number] = truncate_values.get("lower_bound")
        upper_bound: Optional[Number] = truncate_values.get("upper_bound")

        if metric_values.shape == 1 and np.issubdtype(metric_values.dtype, np.number):
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
        if not (
            round_decimals is None
            or (isinstance(round_decimals, int) and (round_decimals >= 0))
        ):
            raise gx_exceptions.ProfilerExecutionError(
                message=f"""The directive "round_decimals" for {self.__class__.__name__} can be 0 or a
positive integer, or must be omitted (or set to None).
"""
            )

        if np.issubdtype(metric_values.dtype, np.integer):
            round_decimals = 0

        return round_decimals
