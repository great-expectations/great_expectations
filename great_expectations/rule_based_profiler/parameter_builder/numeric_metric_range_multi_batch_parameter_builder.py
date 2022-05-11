
import copy
import itertools
import warnings
from numbers import Number
from typing import Any, Callable, Dict, List, Optional, Union, cast
import numpy as np
import great_expectations.exceptions as ge_exceptions
from great_expectations.rule_based_profiler.config import ParameterBuilderConfig
from great_expectations.rule_based_profiler.helpers.util import NP_EPSILON, compute_bootstrap_quantiles_point_estimate, compute_quantiles, get_parameter_value_and_validate_return_type, integer_semantic_domain_type
from great_expectations.rule_based_profiler.parameter_builder import AttributedResolvedMetrics, MetricMultiBatchParameterBuilder, MetricValues
from great_expectations.rule_based_profiler.types import FULLY_QUALIFIED_PARAMETER_NAME_ATTRIBUTED_VALUE_KEY, FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY, FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY, Domain, NumericRangeEstimationResult, ParameterContainer, ParameterNode
from great_expectations.rule_based_profiler.types.numeric_range_estimation_result import NUM_HISTOGRAM_BINS
from great_expectations.types.attributes import Attributes
from great_expectations.util import is_numeric
MAX_DECIMALS: int = 9

class NumericMetricRangeMultiBatchParameterBuilder(MetricMultiBatchParameterBuilder):
    '\n    A Multi-Batch implementation for obtaining the range estimation bounds for a resolved (evaluated) numeric metric,\n    using domain_kwargs, value_kwargs, metric_name, and false_positive_rate (tolerance) as arguments.\n\n    This Multi-Batch ParameterBuilder is general in the sense that any metric that computes numbers can be accommodated.\n    On the other hand, it is specific in the sense that the parameter names will always have the semantics of numeric\n    ranges, which will incorporate the requirements, imposed by the configured false_positive_rate tolerances.\n\n    The implementation supports two methods of estimating parameter values from data:\n    * bootstrapped (default) -- a statistical technique (see "https://en.wikipedia.org/wiki/Bootstrapping_(statistics)")\n    * one-shot -- assumes that metric values, computed on batch data, are normally distributed and computes the mean\n      and the standard error using the queried batches as the single sample of the distribution (fast, but inaccurate).\n    '
    RECOGNIZED_SAMPLING_METHOD_NAMES: set = {'oneshot', 'bootstrap'}
    DEFAULT_BOOTSTRAP_NUM_RESAMPLES: int = 9999
    RECOGNIZED_TRUNCATE_DISTRIBUTION_KEYS: set = {'lower_bound', 'upper_bound'}
    RECOGNIZED_QUANTILE_STATISTIC_INTERPOLATION_METHODS: set = {'auto', 'nearest', 'linear'}

    def __init__(self, name: str, metric_name: Optional[str]=None, metric_domain_kwargs: Optional[Union[(str, dict)]]=None, metric_value_kwargs: Optional[Union[(str, dict)]]=None, enforce_numeric_metric: Union[(str, bool)]=True, replace_nan_with_zero: Union[(str, bool)]=True, reduce_scalar_metric: Union[(str, bool)]=True, false_positive_rate: Union[(str, float)]=0.05, quantile_statistic_interpolation_method: str='auto', estimator: str='bootstrap', num_bootstrap_samples: Optional[Union[(str, int)]]=None, bootstrap_random_seed: Optional[Union[(str, int)]]=None, include_bootstrap_samples_histogram_in_details: Union[(str, bool)]=False, truncate_values: Optional[Union[(str, Dict[(str, Union[(Optional[int], Optional[float])])])]]=None, round_decimals: Optional[Union[(str, int)]]=None, evaluation_parameter_builder_configs: Optional[List[ParameterBuilderConfig]]=None, json_serialize: Union[(str, bool)]=True, data_context: Optional['BaseDataContext']=None) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        Args:\n            name: the name of this parameter -- this is user-specified parameter name (from configuration);\n            it is not the fully-qualified parameter name; a fully-qualified parameter name must start with "$parameter."\n            and may contain one or more subsequent parts (e.g., "$parameter.<my_param_from_config>.<metric_name>").\n            metric_name: the name of a metric used in MetricConfiguration (must be a supported and registered metric)\n            metric_domain_kwargs: used in MetricConfiguration\n            metric_value_kwargs: used in MetricConfiguration\n            enforce_numeric_metric: used in MetricConfiguration to insure that metric computations return numeric values\n            replace_nan_with_zero: if False, then if the computed metric gives NaN, then exception is raised; otherwise,\n            if True (default), then if the computed metric gives NaN, then it is converted to the 0.0 (float) value.\n            reduce_scalar_metric: if True (default), then reduces computation of 1-dimensional metric to scalar value.\n            false_positive_rate: user-configured fraction between 0 and 1 expressing desired false positive rate for\n            identifying unexpected values as judged by the upper- and lower- quantiles of the observed metric data.\n            quantile_statistic_interpolation_method: Applicable only for the "bootstrap" sampling method --\n            supplies value of (interpolation) "method" to "np.quantile()" statistic, used for confidence intervals.\n            estimator: choice of the estimation algorithm: "oneshot" (one observation) or "bootstrap" (default)\n            num_bootstrap_samples: Applicable only for the "bootstrap" sampling method -- if omitted (default), then\n            9999 is used (default in "https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.bootstrap.html").\n            bootstrap_random_seed: Applicable only for the "bootstrap" sampling method -- if omitted (default), then\n            uses "np.random.choice"; otherwise, utilizes "np.random.Generator(np.random.PCG64(bootstrap_random_seed))".\n            include_bootstrap_samples_histogram_in_details: Applicable only for the "bootstrap" sampling method -- if\n            True, then add 10-bin histogram of bootstraps to "details"; otherwise, omit this information (default).\n            truncate_values: user-configured directive for whether or not to allow the computed parameter values\n            (i.e., lower_bound, upper_bound) to take on values outside the specified bounds when packaged on output.\n            round_decimals: user-configured non-negative integer indicating the number of decimals of the\n            rounding precision of the computed parameter values (i.e., min_value, max_value) prior to packaging them on\n            output.  If omitted, then no rounding is performed, unless the computed value is already an integer.\n            evaluation_parameter_builder_configs: ParameterBuilder configurations, executing and making whose respective\n            ParameterBuilder objects\' outputs available (as fully-qualified parameter names) is pre-requisite.\n            These "ParameterBuilder" configurations help build parameters needed for this "ParameterBuilder".\n            json_serialize: If True (default), convert computed value to JSON prior to saving results.\n            data_context: BaseDataContext associated with this ParameterBuilder\n        '
        super().__init__(name=name, metric_name=metric_name, metric_domain_kwargs=metric_domain_kwargs, metric_value_kwargs=metric_value_kwargs, enforce_numeric_metric=enforce_numeric_metric, replace_nan_with_zero=replace_nan_with_zero, reduce_scalar_metric=reduce_scalar_metric, evaluation_parameter_builder_configs=evaluation_parameter_builder_configs, json_serialize=json_serialize, data_context=data_context)
        self._false_positive_rate = false_positive_rate
        self._quantile_statistic_interpolation_method = quantile_statistic_interpolation_method
        self._estimator = estimator
        self._num_bootstrap_samples = num_bootstrap_samples
        self._bootstrap_random_seed = bootstrap_random_seed
        self._include_bootstrap_samples_histogram_in_details = include_bootstrap_samples_histogram_in_details
        self._round_decimals = round_decimals
        if (not truncate_values):
            truncate_values = {'lower_bound': None, 'upper_bound': None}
        elif (not isinstance(truncate_values, str)):
            truncate_values_keys: set = set(truncate_values.keys())
            if (not (truncate_values_keys <= NumericMetricRangeMultiBatchParameterBuilder.RECOGNIZED_TRUNCATE_DISTRIBUTION_KEYS)):
                raise ge_exceptions.ProfilerExecutionError(message=f'''Unrecognized truncate_values key(s) in {self.__class__.__name__}:
"{str((truncate_values_keys - NumericMetricRangeMultiBatchParameterBuilder.RECOGNIZED_TRUNCATE_DISTRIBUTION_KEYS))}" detected.
''')
        self._truncate_values = truncate_values
    '\n    Full getter/setter accessors for needed properties are for configuring MetricMultiBatchParameterBuilder dynamically.\n    '

    @property
    def false_positive_rate(self) -> Union[(str, float)]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return self._false_positive_rate

    @property
    def quantile_statistic_interpolation_method(self) -> str:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return self._quantile_statistic_interpolation_method

    @property
    def estimator(self) -> str:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return self._estimator

    @property
    def num_bootstrap_samples(self) -> Optional[Union[(str, int)]]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return self._num_bootstrap_samples

    @property
    def bootstrap_random_seed(self) -> Optional[Union[(str, int)]]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return self._bootstrap_random_seed

    @property
    def include_bootstrap_samples_histogram_in_details(self) -> Union[(str, bool)]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return self._include_bootstrap_samples_histogram_in_details

    @property
    def truncate_values(self) -> Optional[Union[(str, Dict[(str, Union[(Optional[int], Optional[float])])])]]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return self._truncate_values

    @property
    def round_decimals(self) -> Optional[Union[(str, int)]]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return self._round_decimals

    def _build_parameters(self, domain: Domain, variables: Optional[ParameterContainer]=None, parameters: Optional[Dict[(str, ParameterContainer)]]=None, recompute_existing_parameter_values: bool=False) -> Attributes:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        Builds ParameterContainer object that holds ParameterNode objects with attribute name-value pairs and details.\n\n        Returns:\n            Attributes object, containing computed parameter values and parameter computation details metadata.\n\n         The algorithm operates according to the following steps:\n         1. Obtain batch IDs of interest using BaseDataContext and BatchRequest (unless passed explicitly as argument).\n         2. Set up metric_domain_kwargs and metric_value_kwargs (using configuration and/or variables and parameters).\n         3. Instantiate the Validator object corresponding to BatchRequest (with a temporary expectation_suite_name) in\n            order to have access to all Batch objects, on each of which the specified metric_name will be computed.\n         4. Perform metric computations and obtain the result in the array-like form (one metric value per each Batch).\n         5. Using the configured directives and heuristics, determine whether or not the ranges should be clipped.\n         6. Using the configured directives and heuristics, determine if return values should be rounded to an integer.\n         7. Convert the multi-dimensional metric computation results to a numpy array (for further computations).\n         8. Compute [low, high] for the desired metric using the chosen estimator method.\n         9. Return [low, high] for the desired metric as estimated by the specified sampling method.\n        10. Set up the arguments and call build_parameter_container() to store the parameter as part of "rule state".\n        '
        false_positive_rate: np.float64 = get_parameter_value_and_validate_return_type(domain=domain, parameter_reference=self.false_positive_rate, expected_return_type=(float, np.float64), variables=variables, parameters=parameters)
        if (not (0.0 <= false_positive_rate <= 1.0)):
            raise ge_exceptions.ProfilerExecutionError(f'''false_positive_rate must be a positive decimal number between 0 and 1 inclusive [0, 1],
but {false_positive_rate} was provided.''')
        elif (false_positive_rate <= NP_EPSILON):
            warnings.warn(f'''You have chosen a false_positive_rate of {false_positive_rate}, which is too close to 0.
A false_positive_rate of {NP_EPSILON} has been selected instead.''')
            false_positive_rate = NP_EPSILON
        elif (false_positive_rate >= (1.0 - NP_EPSILON)):
            warnings.warn(f'''You have chosen a false_positive_rate of {false_positive_rate}, which is too close to 1.
A false_positive_rate of {(1.0 - NP_EPSILON)} has been selected instead.''')
            false_positive_rate = np.float64((1.0 - NP_EPSILON))
        super().build_parameters(domain=domain, variables=variables, parameters=parameters, parameter_computation_impl=super()._build_parameters, json_serialize=False, recompute_existing_parameter_values=recompute_existing_parameter_values)
        parameter_node: ParameterNode = get_parameter_value_and_validate_return_type(domain=domain, parameter_reference=self.fully_qualified_parameter_name, expected_return_type=None, variables=variables, parameters=parameters)
        metric_values: MetricValues = AttributedResolvedMetrics.get_metric_values_from_attributed_metric_values(attributed_metric_values=parameter_node[FULLY_QUALIFIED_PARAMETER_NAME_ATTRIBUTED_VALUE_KEY])
        estimator: str = get_parameter_value_and_validate_return_type(domain=domain, parameter_reference=self.estimator, expected_return_type=str, variables=variables, parameters=parameters)
        if (estimator not in NumericMetricRangeMultiBatchParameterBuilder.RECOGNIZED_SAMPLING_METHOD_NAMES):
            raise ge_exceptions.ProfilerExecutionError(message=f'''The directive "estimator" for {self.__class__.__name__} can be only one of
{NumericMetricRangeMultiBatchParameterBuilder.RECOGNIZED_SAMPLING_METHOD_NAMES} ("{estimator}" was detected).
''')
        round_decimals: int
        quantile_statistic_interpolation_method: str = get_parameter_value_and_validate_return_type(domain=domain, parameter_reference=self.quantile_statistic_interpolation_method, expected_return_type=str, variables=variables, parameters=parameters)
        if (quantile_statistic_interpolation_method not in NumericMetricRangeMultiBatchParameterBuilder.RECOGNIZED_QUANTILE_STATISTIC_INTERPOLATION_METHODS):
            raise ge_exceptions.ProfilerExecutionError(message=f'''The directive "quantile_statistic_interpolation_method" for {self.__class__.__name__} can be only one of {NumericMetricRangeMultiBatchParameterBuilder.RECOGNIZED_QUANTILE_STATISTIC_INTERPOLATION_METHODS} ("{quantile_statistic_interpolation_method}" was detected).
''')
        if integer_semantic_domain_type(domain=domain):
            round_decimals = 0
        else:
            round_decimals = self._get_round_decimals_using_heuristics(metric_values=metric_values, domain=domain, variables=variables, parameters=parameters)
        if (quantile_statistic_interpolation_method == 'auto'):
            if (round_decimals == 0):
                quantile_statistic_interpolation_method = 'nearest'
            else:
                quantile_statistic_interpolation_method = 'linear'
        estimator_func: Callable
        estimator_kwargs: dict
        if (estimator == 'bootstrap'):
            estimator_func = self._get_bootstrap_estimate
            estimator_kwargs = {'false_positive_rate': false_positive_rate, 'quantile_statistic_interpolation_method': quantile_statistic_interpolation_method, 'num_bootstrap_samples': self.num_bootstrap_samples, 'bootstrap_random_seed': self.bootstrap_random_seed}
        else:
            estimator_func = self._get_deterministic_estimate
            estimator_kwargs = {'false_positive_rate': false_positive_rate, 'quantile_statistic_interpolation_method': quantile_statistic_interpolation_method}
        numeric_range_estimation_result: NumericRangeEstimationResult = self._estimate_metric_value_range(metric_values=metric_values, estimator_func=estimator_func, round_decimals=round_decimals, domain=domain, variables=variables, parameters=parameters, **estimator_kwargs)
        value_range: np.ndarray = numeric_range_estimation_result.value_range
        details: Dict[(str, Any)] = copy.deepcopy(parameter_node[FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY])
        include_bootstrap_samples_histogram_in_details: bool = get_parameter_value_and_validate_return_type(domain=domain, parameter_reference=self.include_bootstrap_samples_histogram_in_details, expected_return_type=bool, variables=variables, parameters=parameters)
        if include_bootstrap_samples_histogram_in_details:
            details['estimation_histogram'] = numeric_range_estimation_result.estimation_histogram
        return Attributes({FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY: value_range, FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY: details})

    def _estimate_metric_value_range(self, metric_values: np.ndarray, estimator_func: Callable, round_decimals: int, domain: Optional[Domain]=None, variables: Optional[ParameterContainer]=None, parameters: Optional[Dict[(str, ParameterContainer)]]=None, **kwargs) -> NumericRangeEstimationResult:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        This method accepts an estimator Callable and data samples in the format "N x R^m", where "N" (most significant\n        dimension) is the number of measurements (e.g., one per Batch of data), while "R^m" is the multi-dimensional\n        metric, whose values are being estimated.  Thus, for each element in the "R^m" hypercube, an "N"-dimensional\n        vector of sample measurements is constructed and given to the estimator to apply its specific algorithm for\n        computing the range of values in this vector.  Estimator algorithms differ based on their use of data samples.\n        '
        truncate_values: Dict[(str, Number)] = self._get_truncate_values_using_heuristics(metric_values=metric_values, domain=domain, variables=variables, parameters=parameters)
        lower_bound: Optional[float] = truncate_values.get('lower_bound')
        upper_bound: Optional[float] = truncate_values.get('upper_bound')
        min_value: Number
        max_value: Number
        metric_value_shape: tuple = metric_values.shape[1:]
        metric_value_shape_idx: int
        axes: List[np.ndarray] = [np.indices(dimensions=(metric_value_shape_idx,))[0] for metric_value_shape_idx in metric_value_shape]
        metric_value_indices: List[tuple] = list(itertools.product(*tuple(axes)))
        metric_value_idx: tuple
        metric_value_vector_indices: List[tuple] = [((slice(None, None, None),) + metric_value_idx) for metric_value_idx in metric_value_indices]
        metric_value_range_shape: tuple = (metric_value_shape + (2,))
        metric_value_range: np.ndarray = np.zeros(shape=metric_value_range_shape)
        estimation_histogram_shape: tuple = (metric_value_shape + (10,))
        estimation_histogram: np.ndarray = np.zeros(shape=estimation_histogram_shape)
        metric_value_vector: np.ndarray
        metric_value_range_min_idx: tuple
        metric_value_range_max_idx: tuple
        numeric_range_estimation_result: NumericRangeEstimationResult
        for metric_value_idx in metric_value_vector_indices:
            metric_value_vector = metric_values[metric_value_idx]
            if np.all(np.isclose(metric_value_vector, metric_value_vector[0])):
                numeric_range_estimation_result = NumericRangeEstimationResult(estimation_histogram=np.histogram(a=metric_value_vector, bins=NUM_HISTOGRAM_BINS)[0], value_range=np.array([metric_value_vector[0], metric_value_vector[0]]))
            else:
                numeric_range_estimation_result = estimator_func(metric_values=metric_value_vector, domain=domain, variables=variables, parameters=parameters, **kwargs)
            min_value = numeric_range_estimation_result.value_range[0]
            if (lower_bound is not None):
                min_value = max(cast(float, min_value), lower_bound)
            max_value = numeric_range_estimation_result.value_range[1]
            if (upper_bound is not None):
                max_value = min(cast(float, max_value), upper_bound)
            metric_value_idx = metric_value_idx[1:]
            metric_value_range_min_idx = (metric_value_idx + (slice(0, 1, None),))
            metric_value_range_max_idx = (metric_value_idx + (slice(1, 2, None),))
            estimation_histogram[metric_value_idx] = numeric_range_estimation_result.estimation_histogram
            metric_value_range[metric_value_range_min_idx] = round(cast(float, min_value), round_decimals)
            metric_value_range[metric_value_range_max_idx] = round(cast(float, max_value), round_decimals)
        if (metric_value_range.shape[0] == 1):
            estimation_histogram = estimation_histogram[0]
            metric_value_range = metric_value_range[0]
        if (round_decimals == 0):
            metric_value_range = metric_value_range.astype(np.int64)
        return NumericRangeEstimationResult(estimation_histogram=estimation_histogram, value_range=metric_value_range)

    def _get_truncate_values_using_heuristics(self, metric_values: np.ndarray, domain: Domain, *, variables: Optional[ParameterContainer]=None, parameters: Optional[Dict[(str, ParameterContainer)]]=None) -> Dict[(str, Union[(Optional[int], Optional[float])])]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        truncate_values: Dict[(str, Optional[Number])] = get_parameter_value_and_validate_return_type(domain=domain, parameter_reference=self.truncate_values, expected_return_type=dict, variables=variables, parameters=parameters)
        distribution_boundary: Optional[Union[(int, float)]]
        if (not all([((distribution_boundary is None) or is_numeric(value=distribution_boundary)) for distribution_boundary in truncate_values.values()])):
            raise ge_exceptions.ProfilerExecutionError(message=f'''The directive "truncate_values" for {self.__class__.__name__} must specify the
[lower_bound, upper_bound] closed interval, where either boundary is a numeric value (or None).
''')
        lower_bound: Optional[Number] = truncate_values.get('lower_bound')
        upper_bound: Optional[Number] = truncate_values.get('upper_bound')
        if ((lower_bound is None) and np.all(np.greater(metric_values, NP_EPSILON))):
            lower_bound = 0.0
        if ((upper_bound is None) and np.all(np.less(metric_values, (- NP_EPSILON)))):
            upper_bound = 0.0
        return {'lower_bound': lower_bound, 'upper_bound': upper_bound}

    def _get_round_decimals_using_heuristics(self, metric_values: np.ndarray, domain: Domain, variables: Optional[ParameterContainer]=None, parameters: Optional[Dict[(str, ParameterContainer)]]=None) -> int:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        round_decimals: Optional[int] = get_parameter_value_and_validate_return_type(domain=domain, parameter_reference=self.round_decimals, expected_return_type=None, variables=variables, parameters=parameters)
        if (round_decimals is None):
            round_decimals = MAX_DECIMALS
        elif ((not isinstance(round_decimals, int)) or (round_decimals < 0)):
            raise ge_exceptions.ProfilerExecutionError(message=f'''The directive "round_decimals" for {self.__class__.__name__} can be 0 or a
positive integer, or must be omitted (or set to None).
''')
        if np.issubdtype(metric_values.dtype, np.integer):
            round_decimals = 0
        return round_decimals

    @staticmethod
    def _get_bootstrap_estimate(metric_values: np.ndarray, domain: Domain, variables: Optional[ParameterContainer]=None, parameters: Optional[Dict[(str, ParameterContainer)]]=None, **kwargs) -> NumericRangeEstimationResult:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        num_bootstrap_samples: Optional[int] = get_parameter_value_and_validate_return_type(domain=domain, parameter_reference=kwargs.get('num_bootstrap_samples'), expected_return_type=None, variables=variables, parameters=parameters)
        n_resamples: int
        if (num_bootstrap_samples is None):
            n_resamples = NumericMetricRangeMultiBatchParameterBuilder.DEFAULT_BOOTSTRAP_NUM_RESAMPLES
        else:
            n_resamples = num_bootstrap_samples
        random_seed: Optional[int] = get_parameter_value_and_validate_return_type(domain=domain, parameter_reference=kwargs.get('bootstrap_random_seed'), expected_return_type=None, variables=variables, parameters=parameters)
        false_positive_rate: np.float64 = kwargs.get('false_positive_rate', 0.05)
        quantile_statistic_interpolation_method: str = kwargs.get('quantile_statistic_interpolation_method')
        return compute_bootstrap_quantiles_point_estimate(metric_values=metric_values, false_positive_rate=false_positive_rate, n_resamples=n_resamples, random_seed=random_seed, quantile_statistic_interpolation_method=quantile_statistic_interpolation_method)

    @staticmethod
    def _get_deterministic_estimate(metric_values: np.ndarray, **kwargs) -> NumericRangeEstimationResult:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        false_positive_rate: np.float64 = kwargs.get('false_positive_rate', 0.05)
        quantile_statistic_interpolation_method: str = kwargs.get('quantile_statistic_interpolation_method')
        return compute_quantiles(metric_values=metric_values, false_positive_rate=false_positive_rate, quantile_statistic_interpolation_method=quantile_statistic_interpolation_method)
