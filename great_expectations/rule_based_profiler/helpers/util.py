
import copy
import itertools
import logging
import re
import uuid
from numbers import Number
from typing import Any, Dict, List, Optional, Tuple, Union
import numpy as np
import great_expectations.exceptions as ge_exceptions
from great_expectations.core import ExpectationSuite
from great_expectations.core.batch import Batch, BatchRequest, BatchRequestBase, RuntimeBatchRequest, materialize_batch_request
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.rule_based_profiler.types import INFERRED_SEMANTIC_TYPE_KEY, VARIABLES_PREFIX, Domain, NumericRangeEstimationResult, ParameterContainer, ParameterNode, SemanticDomainTypes, get_parameter_value_by_fully_qualified_parameter_name, is_fully_qualified_parameter_name_literal_string_format
from great_expectations.rule_based_profiler.types.numeric_range_estimation_result import NUM_HISTOGRAM_BINS
from great_expectations.types import safe_deep_copy
from great_expectations.validator.metric_configuration import MetricConfiguration
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
NP_EPSILON: Union[(Number, np.float64)] = np.finfo(float).eps
TEMPORARY_EXPECTATION_SUITE_NAME_PREFIX: str = 'tmp'
TEMPORARY_EXPECTATION_SUITE_NAME_STEM: str = 'suite'
TEMPORARY_EXPECTATION_SUITE_NAME_PATTERN: re.Pattern = re.compile(f'^{TEMPORARY_EXPECTATION_SUITE_NAME_PREFIX}\..+\.{TEMPORARY_EXPECTATION_SUITE_NAME_STEM}\.\w{8}')

def get_validator(purpose: str, *, data_context: Optional['BaseDataContext']=None, batch_list: Optional[List[Batch]]=None, batch_request: Optional[Union[(str, BatchRequestBase, dict)]]=None, domain: Optional[Domain]=None, variables: Optional[ParameterContainer]=None, parameters: Optional[Dict[(str, ParameterContainer)]]=None) -> Optional['Validator']:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    validator: Optional['Validator']
    expectation_suite_name: str = f'tmp.{purpose}'
    if (domain is None):
        expectation_suite_name = f'{expectation_suite_name}_suite_{str(uuid.uuid4())[:8]}'
    else:
        expectation_suite_name = f'{expectation_suite_name}_{domain.id}_suite_{str(uuid.uuid4())[:8]}'
    batch: Batch
    if ((batch_list is None) or all([(batch is None) for batch in batch_list])):
        if (batch_request is None):
            return None
        batch_request = build_batch_request(domain=domain, batch_request=batch_request, variables=variables, parameters=parameters)
        validator = data_context.get_validator(batch_request=batch_request, create_expectation_suite_with_name=expectation_suite_name)
    else:
        num_batches: int = len(batch_list)
        if (num_batches == 0):
            raise ge_exceptions.ProfilerExecutionError(message=f'''{__name__}.get_validator() must utilize at least one Batch ({num_batches} are available).
''')
        expectation_suite: ExpectationSuite = data_context.create_expectation_suite(expectation_suite_name=expectation_suite_name)
        validator = data_context.get_validator_using_batch_list(expectation_suite=expectation_suite, batch_list=batch_list)
    return validator

def get_batch_ids(data_context: Optional['BaseDataContext']=None, batch_list: Optional[List[Batch]]=None, batch_request: Optional[Union[(str, BatchRequestBase, dict)]]=None, domain: Optional[Domain]=None, variables: Optional[ParameterContainer]=None, parameters: Optional[Dict[(str, ParameterContainer)]]=None) -> Optional[List[str]]:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    batch: Batch
    if ((batch_list is None) or all([(batch is None) for batch in batch_list])):
        if (batch_request is None):
            return None
        batch_request = build_batch_request(domain=domain, batch_request=batch_request, variables=variables, parameters=parameters)
        batch_list = data_context.get_batch_list(batch_request=batch_request)
    batch_ids: List[str] = [batch.id for batch in batch_list]
    num_batch_ids: int = len(batch_ids)
    if (num_batch_ids == 0):
        raise ge_exceptions.ProfilerExecutionError(message=f'''{__name__}.get_batch_ids() must return at least one batch_id ({num_batch_ids} were retrieved).
''')
    return batch_ids

def build_batch_request(batch_request: Optional[Union[(str, BatchRequestBase, dict)]]=None, domain: Optional[Domain]=None, variables: Optional[ParameterContainer]=None, parameters: Optional[Dict[(str, ParameterContainer)]]=None) -> Optional[Union[(BatchRequest, RuntimeBatchRequest)]]:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    if (batch_request is None):
        return None
    effective_batch_request: Optional[Union[(BatchRequestBase, dict)]] = get_parameter_value_and_validate_return_type(domain=domain, parameter_reference=batch_request, expected_return_type=(BatchRequestBase, dict), variables=variables, parameters=parameters)
    materialized_batch_request: Optional[Union[(BatchRequest, RuntimeBatchRequest)]] = materialize_batch_request(batch_request=effective_batch_request)
    return materialized_batch_request

def build_metric_domain_kwargs(batch_id: Optional[str]=None, metric_domain_kwargs: Optional[Union[(str, dict)]]=None, domain: Optional[Domain]=None, variables: Optional[ParameterContainer]=None, parameters: Optional[Dict[(str, ParameterContainer)]]=None):
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    metric_domain_kwargs = get_parameter_value_and_validate_return_type(domain=domain, parameter_reference=metric_domain_kwargs, expected_return_type=None, variables=variables, parameters=parameters)
    if (metric_domain_kwargs is None):
        metric_domain_kwargs = {}
    metric_domain_kwargs = copy.deepcopy(metric_domain_kwargs)
    if batch_id:
        metric_domain_kwargs['batch_id'] = batch_id
    return metric_domain_kwargs

def get_parameter_value_and_validate_return_type(domain: Optional[Domain]=None, parameter_reference: Optional[Union[(Any, str)]]=None, expected_return_type: Optional[Union[(type, tuple)]]=None, variables: Optional[ParameterContainer]=None, parameters: Optional[Dict[(str, ParameterContainer)]]=None) -> Optional[Any]:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    '\n    This method allows for the parameter_reference to be specified as an object (literal, dict, any typed object, etc.)\n    or as a fully-qualified parameter name.  In either case, it can optionally validate the type of the return value.\n    '
    if isinstance(parameter_reference, dict):
        parameter_reference = safe_deep_copy(data=parameter_reference)
    parameter_reference = get_parameter_value(domain=domain, parameter_reference=parameter_reference, variables=variables, parameters=parameters)
    if (expected_return_type is not None):
        if (not isinstance(parameter_reference, expected_return_type)):
            raise ge_exceptions.ProfilerExecutionError(message=f'''Argument "{parameter_reference}" must be of type "{str(expected_return_type)}" (value of type "{str(type(parameter_reference))}" was encountered).
''')
    return parameter_reference

def get_parameter_value(domain: Optional[Domain]=None, parameter_reference: Optional[Union[(Any, str)]]=None, variables: Optional[ParameterContainer]=None, parameters: Optional[Dict[(str, ParameterContainer)]]=None) -> Optional[Any]:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    '\n    This method allows for the parameter_reference to be specified as an object (literal, dict, any typed object, etc.)\n    or as a fully-qualified parameter name.  Moreover, if the parameter_reference argument is an object of type "dict",\n    it will recursively detect values using the fully-qualified parameter name format and evaluate them accordingly.\n    '
    if isinstance(parameter_reference, dict):
        for (key, value) in parameter_reference.items():
            parameter_reference[key] = get_parameter_value(domain=domain, parameter_reference=value, variables=variables, parameters=parameters)
    elif isinstance(parameter_reference, (list, set, tuple)):
        parameter_reference_type: type = type(parameter_reference)
        element: Any
        return parameter_reference_type([get_parameter_value(domain=domain, parameter_reference=element, variables=variables, parameters=parameters) for element in parameter_reference])
    elif (isinstance(parameter_reference, str) and is_fully_qualified_parameter_name_literal_string_format(fully_qualified_parameter_name=parameter_reference)):
        parameter_reference = get_parameter_value_by_fully_qualified_parameter_name(fully_qualified_parameter_name=parameter_reference, domain=domain, variables=variables, parameters=parameters)
        parameter_reference = get_parameter_value(domain=domain, parameter_reference=parameter_reference, variables=variables, parameters=parameters)
    return parameter_reference

def get_resolved_metrics_by_key(validator: 'Validator', metric_configurations_by_key: Dict[(str, List[MetricConfiguration])]) -> Dict[(str, Dict[(Tuple[(str, str, str)], Any)])]:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    '\n    Compute (resolve) metrics for every column name supplied on input.\n\n    Args:\n        validator: Validator used to compute column cardinality.\n        metric_configurations_by_key: metric configurations used to compute figures of merit.\n        Dictionary of the form {\n            "my_key": List[MetricConfiguration],  # examples of "my_key" are: "my_column_name", "my_batch_id", etc.\n        }\n\n    Returns:\n        Dictionary of the form {\n            "my_key": Dict[Tuple[str, str, str], Any],\n        }\n    '
    key: str
    metric_configuration: MetricConfiguration
    metric_configurations_for_key: List[MetricConfiguration]
    resolved_metrics: Dict[(Tuple[(str, str, str)], Any)] = validator.compute_metrics(metric_configurations=[metric_configuration for (key, metric_configurations_for_key) in metric_configurations_by_key.items() for metric_configuration in metric_configurations_for_key])
    metric_configuration_ids_by_key: Dict[(str, List[Tuple[(str, str, str)]])] = {key: [metric_configuration.id for metric_configuration in metric_configurations_for_key] for (key, metric_configurations_for_key) in metric_configurations_by_key.items()}
    metric_configuration_ids: List[Tuple[(str, str, str)]]
    metric_configuration_ids_all_keys: List[Tuple[(str, str, str)]] = list(itertools.chain(*[metric_configuration_ids for metric_configuration_ids in metric_configuration_ids_by_key.values()]))
    metric_configuration_id: Tuple[(str, str, str)]
    metric_value: Any
    resolved_metrics = {metric_configuration_id: metric_value for (metric_configuration_id, metric_value) in resolved_metrics.items() if (metric_configuration_id in metric_configuration_ids_all_keys)}
    metric_configuration_ids_resolved_metrics: List[Tuple[(str, str, str)]] = list(resolved_metrics.keys())
    candidate_keys: List[str] = [key for (key, metric_configuration_ids) in metric_configuration_ids_by_key.items() if all([(metric_configuration_id in metric_configuration_ids_resolved_metrics) for metric_configuration_id in metric_configuration_ids])]
    resolved_metrics_by_key: Dict[(str, Dict[(Tuple[(str, str, str)], Any)])] = {key: {metric_configuration.id: resolved_metrics[metric_configuration.id] for metric_configuration in metric_configurations_by_key[key]} for key in candidate_keys}
    return resolved_metrics_by_key

def build_domains_from_column_names(rule_name: str, column_names: List[str], domain_type: MetricDomainTypes, table_column_name_to_inferred_semantic_domain_type_map: Optional[Dict[(str, SemanticDomainTypes)]]=None) -> List[Domain]:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    '\n    This utility method builds "simple" Domain objects (i.e., required fields only, no "details" metadata accepted).\n\n    :param rule_name: name of Rule object, for which "Domain" objects are obtained.\n    :param column_names: list of column names to serve as values for "column" keys in "domain_kwargs" dictionary\n    :param domain_type: type of Domain objects (same "domain_type" must be applicable to all Domain objects returned)\n    :param table_column_name_to_inferred_semantic_domain_type_map: map from column name to inferred semantic type\n    :return: list of resulting Domain objects\n    '
    column_name: str
    domains: List[Domain] = [Domain(domain_type=domain_type, domain_kwargs={'column': column_name}, details={INFERRED_SEMANTIC_TYPE_KEY: ({column_name: table_column_name_to_inferred_semantic_domain_type_map[column_name]} if table_column_name_to_inferred_semantic_domain_type_map else None)}, rule_name=rule_name) for column_name in column_names]
    return domains

def convert_variables_to_dict(variables: Optional[ParameterContainer]=None) -> Dict[(str, Any)]:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    variables_as_dict: Optional[Union[(ParameterNode, Dict[(str, Any)])]] = get_parameter_value_and_validate_return_type(domain=None, parameter_reference=VARIABLES_PREFIX, expected_return_type=None, variables=variables, parameters=None)
    if isinstance(variables_as_dict, ParameterNode):
        return variables_as_dict.to_dict()
    if (variables_as_dict is None):
        return {}
    return variables_as_dict

def integer_semantic_domain_type(domain: Domain) -> bool:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    '\n    This method examines "INFERRED_SEMANTIC_TYPE_KEY" attribute of "Domain" argument to check whether or not underlying\n    "SemanticDomainTypes" enum value is an "integer".  Because explicitly designated "SemanticDomainTypes.INTEGER" type\n    is unavaiable, "SemanticDomainTypes.LOGIC" and "SemanticDomainTypes.IDENTIFIER" are intepreted as "integer" values.\n\n    This method can be used "NumericMetricRangeMultiBatchParameterBuilder._get_round_decimals_using_heuristics()".\n\n    Note: Inability to assess underlying "SemanticDomainTypes" details of "Domain" object produces "False" return value.\n\n    Args:\n        domain: "Domain" object to inspect for underlying "SemanticDomainTypes" details\n\n    Returns:\n        Boolean value indicating whether or not specified "Domain" is inferred to denote "integer" values\n\n    '
    inferred_semantic_domain_type: Dict[(str, SemanticDomainTypes)] = domain.details.get(INFERRED_SEMANTIC_TYPE_KEY)
    semantic_domain_type: SemanticDomainTypes
    return (inferred_semantic_domain_type and all([(semantic_domain_type in [SemanticDomainTypes.LOGIC, SemanticDomainTypes.IDENTIFIER]) for semantic_domain_type in inferred_semantic_domain_type.values()]))

def compute_quantiles(metric_values: np.ndarray, false_positive_rate: np.float64, quantile_statistic_interpolation_method: str) -> NumericRangeEstimationResult:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    lower_quantile = np.quantile(metric_values, q=(false_positive_rate / 2), axis=0, interpolation=quantile_statistic_interpolation_method)
    upper_quantile = np.quantile(metric_values, q=(1.0 - (false_positive_rate / 2)), axis=0, interpolation=quantile_statistic_interpolation_method)
    return NumericRangeEstimationResult(estimation_histogram=np.histogram(a=metric_values, bins=NUM_HISTOGRAM_BINS)[0], value_range=np.array([lower_quantile, upper_quantile]))

def compute_bootstrap_quantiles_point_estimate(metric_values: np.ndarray, false_positive_rate: np.float64, quantile_statistic_interpolation_method: str, n_resamples: int, random_seed: Optional[int]=None) -> NumericRangeEstimationResult:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    '\n    ML Flow Experiment: parameter_builders_bootstrap/bootstrap_quantiles\n    ML Flow Experiment ID: 4129654509298109\n\n    An internal implementation of the "bootstrap" estimator method, returning a point estimate for a population\n    parameter of interest (lower and upper quantiles in this case). See\n    https://en.wikipedia.org/wiki/Bootstrapping_(statistics) for an introduction to "bootstrapping" in statistics.\n\n    The methods implemented here can be found in:\n    Efron, B., & Tibshirani, R. J. (1993). Estimates of bias. An Introduction to the Bootstrap (pp. 124-130).\n        Springer Science and Business Media Dordrecht. DOI 10.1007/978-1-4899-4541-9\n\n    This implementation is sub-par compared to the one available from the "SciPy" standard library\n    ("https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.bootstrap.html"), in that it does not handle\n    multi-dimensional statistics. "scipy.stats.bootstrap" is vectorized, thus having the ability to accept a\n    multi-dimensional statistic function and process all dimensions.\n\n    Unfortunately, as of March 4th, 2022, the SciPy implementation has two issues: 1) it only returns a confidence\n    interval and not a point estimate for the population parameter of interest, which is what we require for our use\n    cases. 2) It can not handle multi-dimensional statistics and correct for bias simultaneously. You must either use\n    one feature or the other.\n\n    This implementation could only be replaced by "scipy.stats.bootstrap" if Great Expectations drops support for\n    Python 3.6, thereby enabling us to use a more up-to-date version of the "scipy" Python package (the currently used\n    version does not have "bootstrap"). Also, as discussed above, two contributions would need to be made to the SciPy\n    package to enable 1) bias correction for multi-dimensional statistics and 2) a return value of a point estimate for\n    the population parameter of interest (lower and upper quantiles in this case).\n\n    Additional future direction could include developing enhancements to bootstrapped estimator based on theory\n    presented in "http://dido.econ.yale.edu/~dwka/pub/p1001.pdf":\n    @article{Andrews2000a,\n        added-at = {2008-04-25T10:38:44.000+0200},\n        author = {Andrews, Donald W. K. and Buchinsky, Moshe},\n        biburl = {https://www.bibsonomy.org/bibtex/28e2f0a58cdb95e39659921f989a17bdd/smicha},\n        day = 01,\n        interhash = {778746398daa9ba63bdd95391f1efd37},\n        intrahash = {8e2f0a58cdb95e39659921f989a17bdd},\n        journal = {Econometrica},\n        keywords = {imported},\n        month = Jan,\n        note = {doi: 10.1111/1468-0262.00092},\n        number = 1,\n        pages = {23--51},\n        timestamp = {2008-04-25T10:38:52.000+0200},\n        title = {A Three-step Method for Choosing the Number of Bootstrap Repetitions},\n        url = {http://www.blackwell-synergy.com/doi/abs/10.1111/1468-0262.00092},\n        volume = 68,\n        year = 2000\n    }\n    The article outlines a three-step minimax procedure that relies on the Central Limit Theorem (C.L.T.) along with the\n    bootstrap sampling technique (see https://en.wikipedia.org/wiki/Bootstrapping_(statistics) for background) for\n    computing the stopping criterion, expressed as the optimal number of bootstrap samples, needed to achieve a maximum\n    probability that the value of the statistic of interest will be minimally deviating from its actual (ideal) value.\n    '
    lower_quantile_pct: float = (false_positive_rate / 2)
    upper_quantile_pct: float = (1.0 - (false_positive_rate / 2))
    sample_lower_quantile: np.ndarray = np.quantile(metric_values, q=lower_quantile_pct, interpolation=quantile_statistic_interpolation_method)
    sample_upper_quantile: np.ndarray = np.quantile(metric_values, q=upper_quantile_pct, interpolation=quantile_statistic_interpolation_method)
    bootstraps: np.ndarray
    if random_seed:
        random_state: np.random.Generator = np.random.Generator(np.random.PCG64(random_seed))
        bootstraps = random_state.choice(metric_values, size=(n_resamples, metric_values.size))
    else:
        bootstraps = np.random.choice(metric_values, size=(n_resamples, metric_values.size))
    bootstrap_lower_quantiles: Union[(np.ndarray, Number)] = np.quantile(bootstraps, q=lower_quantile_pct, axis=1, interpolation=quantile_statistic_interpolation_method)
    bootstrap_lower_quantile_point_estimate: float = np.mean(bootstrap_lower_quantiles)
    bootstrap_lower_quantile_standard_error: float = np.std(bootstrap_lower_quantiles)
    bootstrap_lower_quantile_bias: float = (bootstrap_lower_quantile_point_estimate - sample_lower_quantile)
    lower_quantile_bias_corrected_point_estimate: Number
    if ((bootstrap_lower_quantile_bias / bootstrap_lower_quantile_standard_error) <= 0.25):
        lower_quantile_bias_corrected_point_estimate = bootstrap_lower_quantile_point_estimate
    else:
        lower_quantile_bias_corrected_point_estimate = (bootstrap_lower_quantile_point_estimate - bootstrap_lower_quantile_bias)
    bootstrap_upper_quantiles: Union[(np.ndarray, Number)] = np.quantile(bootstraps, q=upper_quantile_pct, axis=1, interpolation=quantile_statistic_interpolation_method)
    bootstrap_upper_quantile_point_estimate: np.ndarray = np.mean(bootstrap_upper_quantiles)
    bootstrap_upper_quantile_standard_error: np.ndarray = np.std(bootstrap_upper_quantiles)
    bootstrap_upper_quantile_bias: float = (bootstrap_upper_quantile_point_estimate - sample_upper_quantile)
    upper_quantile_bias_corrected_point_estimate: Number
    if ((bootstrap_upper_quantile_bias / bootstrap_upper_quantile_standard_error) <= 0.25):
        upper_quantile_bias_corrected_point_estimate = bootstrap_upper_quantile_point_estimate
    else:
        upper_quantile_bias_corrected_point_estimate = (bootstrap_upper_quantile_point_estimate - bootstrap_upper_quantile_bias)
    return NumericRangeEstimationResult(estimation_histogram=np.histogram(a=bootstraps.flatten(), bins=NUM_HISTOGRAM_BINS)[0], value_range=[lower_quantile_bias_corrected_point_estimate, upper_quantile_bias_corrected_point_estimate])

def get_validator_with_expectation_suite(batch_request: Union[(BatchRequestBase, dict)], data_context: 'BaseDataContext', expectation_suite: Optional['ExpectationSuite']=None, expectation_suite_name: Optional[str]=None, component_name: str='test') -> 'Validator':
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    '\n    Instantiates and returns "Validator" object using "data_context", "batch_request", and other available information.\n    Use "expectation_suite" if provided.  If not, then if "expectation_suite_name" is specified, then create\n    "ExpectationSuite" from it.  Otherwise, generate temporary "expectation_suite_name" using supplied "component_name".\n    '
    assert ((expectation_suite is None) or isinstance(expectation_suite, ExpectationSuite))
    assert ((expectation_suite_name is None) or isinstance(expectation_suite_name, str))
    expectation_suite = get_or_create_expectation_suite(data_context=data_context, expectation_suite=expectation_suite, expectation_suite_name=expectation_suite_name, component_name=component_name)
    batch_request = materialize_batch_request(batch_request=batch_request)
    validator: 'Validator' = data_context.get_validator(batch_request=batch_request, expectation_suite=expectation_suite)
    return validator

def get_or_create_expectation_suite(data_context: 'BaseDataContext', expectation_suite: Optional['ExpectationSuite']=None, expectation_suite_name: Optional[str]=None, component_name: Optional[str]=None) -> 'ExpectationSuite':
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    '\n    Use "expectation_suite" if provided.  If not, then if "expectation_suite_name" is specified, then create\n    "ExpectationSuite" from it.  Otherwise, generate temporary "expectation_suite_name" using supplied "component_name".\n    '
    generate_temp_expectation_suite_name: bool
    create_expectation_suite: bool
    if ((expectation_suite is not None) and (expectation_suite_name is not None)):
        if (expectation_suite.expectation_suite_name != expectation_suite_name):
            raise ValueError('Mutually inconsistent "expectation_suite" and "expectation_suite_name" were specified.')
        return expectation_suite
    elif ((expectation_suite is None) and (expectation_suite_name is not None)):
        generate_temp_expectation_suite_name = False
        create_expectation_suite = True
    elif ((expectation_suite is not None) and (expectation_suite_name is None)):
        generate_temp_expectation_suite_name = False
        create_expectation_suite = False
    else:
        generate_temp_expectation_suite_name = True
        create_expectation_suite = True
    if generate_temp_expectation_suite_name:
        if (not component_name):
            component_name = 'test'
        expectation_suite_name = f'{TEMPORARY_EXPECTATION_SUITE_NAME_PREFIX}.{component_name}.{TEMPORARY_EXPECTATION_SUITE_NAME_STEM}.{str(uuid.uuid4())[:8]}'
    if create_expectation_suite:
        try:
            expectation_suite = data_context.get_expectation_suite(expectation_suite_name=expectation_suite_name)
        except ge_exceptions.DataContextError:
            expectation_suite = data_context.create_expectation_suite(expectation_suite_name=expectation_suite_name)
            logger.info(f'Created ExpectationSuite "{expectation_suite.expectation_suite_name}".')
    return expectation_suite
