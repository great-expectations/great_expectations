
import logging
from typing import Dict, Iterable, List, Optional, Set, Union
import great_expectations.exceptions as ge_exceptions
from great_expectations.rule_based_profiler.config import ParameterBuilderConfig
from great_expectations.rule_based_profiler.helpers.util import get_parameter_value_and_validate_return_type
from great_expectations.rule_based_profiler.parameter_builder import AttributedResolvedMetrics, MetricComputationResult, MetricValues, ParameterBuilder
from great_expectations.rule_based_profiler.types import FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY, FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY, Domain, ParameterContainer
from great_expectations.types.attributes import Attributes
logger = logging.getLogger(__name__)

class RegexPatternStringParameterBuilder(ParameterBuilder):
    '\n    Detects the domain REGEX from a set of candidate REGEX strings by computing the\n    column_values.match_regex_format.unexpected_count metric for each candidate format and returning the format that\n    has the lowest unexpected_count ratio.\n    '
    CANDIDATE_REGEX: Set[str] = {'\\d+', '-?\\d+', '-?\\d+(\\.\\d*)?', '[A-Za-z0-9\\.,;:!?()\\"\'%\\-]+', '^\\s+', '\\s+$', 'https?:\\/\\/(www\\.)?[-a-zA-Z0-9@:%._\\+~#=]{2,256}\\.[a-z]{2,6}\\b([-a-zA-Z0-9@:%_\\+.~#()?&//=]*)', '<\\/?(?:p|a|b|img)(?: \\/)?>', '(?:25[0-5]|2[0-4]\\d|[01]\\d{2}|\\d{1,2})(?:.(?:25[0-5]|2[0-4]\\d|[01]\\d{2}|\\d{1,2})){3}', '(?:[A-Fa-f0-9]){0,4}(?: ?:? ?(?:[A-Fa-f0-9]){0,4}){0,7}', '\\b[0-9a-fA-F]{8}\\b-[0-9a-fA-F]{4}-[0-5][0-9a-fA-F]{3}-[089ab][0-9a-fA-F]{3}-\\b[0-9a-fA-F]{12}\\b '}

    def __init__(self, name: str, metric_domain_kwargs: Optional[Union[(str, dict)]]=None, metric_value_kwargs: Optional[Union[(str, dict)]]=None, threshold: Union[(str, float)]=1.0, candidate_regexes: Optional[Union[(str, Iterable[str])]]=None, evaluation_parameter_builder_configs: Optional[List[ParameterBuilderConfig]]=None, json_serialize: Union[(str, bool)]=True, data_context: Optional['BaseDataContext']=None) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        Configure this RegexPatternStringParameterBuilder\n        Args:\n            name: the name of this parameter -- this is user-specified parameter name (from configuration);\n            it is not the fully-qualified parameter name; a fully-qualified parameter name must start with "$parameter."\n            and may contain one or more subsequent parts (e.g., "$parameter.<my_param_from_config>.<metric_name>").\n            threshold: the ratio of values that must match a format string for it to be accepted\n            candidate_regexes: a list of candidate regex strings that will REPLACE the default\n            evaluation_parameter_builder_configs: ParameterBuilder configurations, executing and making whose respective\n            ParameterBuilder objects\' outputs available (as fully-qualified parameter names) is pre-requisite.\n            These "ParameterBuilder" configurations help build parameters needed for this "ParameterBuilder".\n            json_serialize: If True (default), convert computed value to JSON prior to saving results.\n            data_context: BaseDataContext associated with this ParameterBuilder\n        '
        super().__init__(name=name, evaluation_parameter_builder_configs=evaluation_parameter_builder_configs, json_serialize=json_serialize, data_context=data_context)
        self._metric_domain_kwargs = metric_domain_kwargs
        self._metric_value_kwargs = metric_value_kwargs
        self._threshold = threshold
        self._candidate_regexes = candidate_regexes

    @property
    def metric_domain_kwargs(self) -> Optional[Union[(str, dict)]]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return self._metric_domain_kwargs

    @property
    def metric_value_kwargs(self) -> Optional[Union[(str, dict)]]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return self._metric_value_kwargs

    @metric_value_kwargs.setter
    def metric_value_kwargs(self, value: Optional[Union[(str, dict)]]) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        self._metric_value_kwargs = value

    @property
    def threshold(self) -> Union[(str, float)]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return self._threshold

    @property
    def candidate_regexes(self) -> Union[(str, Union[(List[str], Set[str])])]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return self._candidate_regexes

    def _build_parameters(self, domain: Domain, variables: Optional[ParameterContainer]=None, parameters: Optional[Dict[(str, ParameterContainer)]]=None, recompute_existing_parameter_values: bool=False) -> Attributes:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        Builds ParameterContainer object that holds ParameterNode objects with attribute name-value pairs and details.\n\n        Check the percentage of values matching the REGEX string, and return the best fit, or None if no string exceeds\n        the configured threshold.\n\n        Builds ParameterContainer object that holds ParameterNode objects with attribute name-value pairs and details.\n        '
        metric_computation_result: MetricComputationResult
        metric_computation_result = self.get_metrics(metric_name='column_values.nonnull.count', metric_domain_kwargs=self.metric_domain_kwargs, metric_value_kwargs=self.metric_value_kwargs, domain=domain, variables=variables, parameters=parameters)
        if (len(metric_computation_result.attributed_resolved_metrics) != 1):
            raise ge_exceptions.ProfilerExecutionError(message=f'Result of metric computations for {self.__class__.__name__} must be a list with exactly 1 element of type "AttributedResolvedMetrics" ({metric_computation_result.attributed_resolved_metrics} found).')
        attributed_resolved_metrics: AttributedResolvedMetrics
        attributed_resolved_metrics = metric_computation_result.attributed_resolved_metrics[0]
        metric_values: MetricValues
        metric_values = attributed_resolved_metrics.metric_values
        if (metric_values is None):
            raise ge_exceptions.ProfilerExecutionError(message=f'Result of metric computations for {self.__class__.__name__} is empty.')
        metric_values = metric_values[:, 0]
        nonnull_count: int = sum(metric_values)
        candidate_regexes: Union[(List[str], Set[str])] = get_parameter_value_and_validate_return_type(domain=domain, parameter_reference=self.candidate_regexes, expected_return_type=None, variables=variables, parameters=parameters)
        if ((candidate_regexes is not None) and isinstance(candidate_regexes, list)):
            candidate_regexes = set(candidate_regexes)
        else:
            candidate_regexes = RegexPatternStringParameterBuilder.CANDIDATE_REGEX
        regex_string: str
        match_regex_metric_value_kwargs_list: List[dict] = []
        match_regex_metric_value_kwargs: dict
        for regex_string in candidate_regexes:
            if self.metric_value_kwargs:
                match_regex_metric_value_kwargs: dict = {**self._metric_value_kwargs, **{'regex': regex_string}}
            else:
                match_regex_metric_value_kwargs = {'regex': regex_string}
            match_regex_metric_value_kwargs_list.append(match_regex_metric_value_kwargs)
        metric_computation_result = self.get_metrics(metric_name='column_values.match_regex.unexpected_count', metric_domain_kwargs=self.metric_domain_kwargs, metric_value_kwargs=match_regex_metric_value_kwargs_list, domain=domain, variables=variables, parameters=parameters)
        regex_string_success_ratios: dict = {}
        for attributed_resolved_metrics in metric_computation_result.attributed_resolved_metrics:
            metric_values = attributed_resolved_metrics.metric_values[:, 0]
            match_regex_unexpected_count: int = sum(metric_values)
            success_ratio: float = ((nonnull_count - match_regex_unexpected_count) / nonnull_count)
            regex_string_success_ratios[attributed_resolved_metrics.metric_attributes['regex']] = success_ratio
        threshold: float = get_parameter_value_and_validate_return_type(domain=domain, parameter_reference=self._threshold, expected_return_type=float, variables=variables, parameters=parameters)
        (best_regex_string, best_ratio) = ParameterBuilder._get_best_candidate_above_threshold(regex_string_success_ratios, threshold)
        sorted_regex_candidates_and_ratios: dict = ParameterBuilder._get_sorted_candidates_and_ratios(regex_string_success_ratios)
        return Attributes({FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY: best_regex_string, FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY: {'success_ratio': best_ratio, 'evaluated_regexes': sorted_regex_candidates_and_ratios}})
