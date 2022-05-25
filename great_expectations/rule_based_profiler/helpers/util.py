import copy
import itertools
import logging
import re
import uuid
from numbers import Number
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import numpy as np
import scipy.stats as stats

import great_expectations.exceptions as ge_exceptions
from great_expectations.core import ExpectationSuite
from great_expectations.core.batch import (
    Batch,
    BatchRequest,
    BatchRequestBase,
    RuntimeBatchRequest,
    materialize_batch_request,
)
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.rule_based_profiler.types import (
    FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER,
    INFERRED_SEMANTIC_TYPE_KEY,
    VARIABLES_PREFIX,
    Domain,
    NumericRangeEstimationResult,
    ParameterContainer,
    ParameterNode,
    SemanticDomainTypes,
    get_parameter_value_by_fully_qualified_parameter_name,
    is_fully_qualified_parameter_name_literal_string_format,
)
from great_expectations.rule_based_profiler.types.numeric_range_estimation_result import (
    NUM_HISTOGRAM_BINS,
)
from great_expectations.types import safe_deep_copy
from great_expectations.util import numpy_quantile
from great_expectations.validator.metric_configuration import MetricConfiguration

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

NP_EPSILON: Union[Number, np.float64] = np.finfo(float).eps

TEMPORARY_EXPECTATION_SUITE_NAME_PREFIX: str = "tmp"
TEMPORARY_EXPECTATION_SUITE_NAME_STEM: str = "suite"
TEMPORARY_EXPECTATION_SUITE_NAME_PATTERN: re.Pattern = re.compile(
    rf"^{TEMPORARY_EXPECTATION_SUITE_NAME_PREFIX}\..+\.{TEMPORARY_EXPECTATION_SUITE_NAME_STEM}\.\w{8}"
)


def get_validator(
    purpose: str,
    *,
    data_context: Optional["BaseDataContext"] = None,  # noqa: F821
    batch_list: Optional[List[Batch]] = None,
    batch_request: Optional[Union[str, BatchRequestBase, dict]] = None,
    domain: Optional[Domain] = None,
    variables: Optional[ParameterContainer] = None,
    parameters: Optional[Dict[str, ParameterContainer]] = None,
) -> Optional["Validator"]:  # noqa: F821
    validator: Optional["Validator"]  # noqa: F821

    expectation_suite_name: str = f"tmp.{purpose}"
    if domain is None:
        expectation_suite_name = (
            f"{expectation_suite_name}_suite_{str(uuid.uuid4())[:8]}"
        )
    else:
        expectation_suite_name = (
            f"{expectation_suite_name}_{domain.id}_suite_{str(uuid.uuid4())[:8]}"
        )

    batch: Batch
    if batch_list is None or all([batch is None for batch in batch_list]):
        if batch_request is None:
            return None

        batch_request = build_batch_request(
            domain=domain,
            batch_request=batch_request,
            variables=variables,
            parameters=parameters,
        )

        validator = data_context.get_validator(
            batch_request=batch_request,
            create_expectation_suite_with_name=expectation_suite_name,
        )
    else:
        num_batches: int = len(batch_list)
        if num_batches == 0:
            raise ge_exceptions.ProfilerExecutionError(
                message=f"""{__name__}.get_validator() must utilize at least one Batch ({num_batches} are available).
"""
            )

        expectation_suite: ExpectationSuite = data_context.create_expectation_suite(
            expectation_suite_name=expectation_suite_name
        )
        validator = data_context.get_validator_using_batch_list(
            expectation_suite=expectation_suite,
            batch_list=batch_list,
        )

    # Always disabled for RBP and DataAssistants due to volume of metric calculations
    validator.show_progress_bars = False
    return validator


def get_batch_ids(
    data_context: Optional["BaseDataContext"] = None,  # noqa: F821
    batch_list: Optional[List[Batch]] = None,
    batch_request: Optional[Union[str, BatchRequestBase, dict]] = None,
    domain: Optional[Domain] = None,
    variables: Optional[ParameterContainer] = None,
    parameters: Optional[Dict[str, ParameterContainer]] = None,
) -> Optional[List[str]]:
    batch: Batch
    if batch_list is None or all([batch is None for batch in batch_list]):
        if batch_request is None:
            return None

        batch_request = build_batch_request(
            domain=domain,
            batch_request=batch_request,
            variables=variables,
            parameters=parameters,
        )

        batch_list = data_context.get_batch_list(batch_request=batch_request)

    batch_ids: List[str] = [batch.id for batch in batch_list]

    num_batch_ids: int = len(batch_ids)
    if num_batch_ids == 0:
        raise ge_exceptions.ProfilerExecutionError(
            message=f"""{__name__}.get_batch_ids() must return at least one batch_id ({num_batch_ids} were retrieved).
"""
        )

    return batch_ids


def build_batch_request(
    batch_request: Optional[Union[str, BatchRequestBase, dict]] = None,
    domain: Optional[Domain] = None,
    variables: Optional[ParameterContainer] = None,
    parameters: Optional[Dict[str, ParameterContainer]] = None,
) -> Optional[Union[BatchRequest, RuntimeBatchRequest]]:
    if batch_request is None:
        return None

    # Obtain BatchRequest from "rule state" (i.e., variables and parameters); from instance variable otherwise.
    effective_batch_request: Optional[
        Union[BatchRequestBase, dict]
    ] = get_parameter_value_and_validate_return_type(
        domain=domain,
        parameter_reference=batch_request,
        expected_return_type=(BatchRequestBase, dict),
        variables=variables,
        parameters=parameters,
    )
    materialized_batch_request: Optional[
        Union[BatchRequest, RuntimeBatchRequest]
    ] = materialize_batch_request(batch_request=effective_batch_request)

    return materialized_batch_request


def build_metric_domain_kwargs(
    batch_id: Optional[str] = None,
    metric_domain_kwargs: Optional[Union[str, dict]] = None,
    domain: Optional[Domain] = None,
    variables: Optional[ParameterContainer] = None,
    parameters: Optional[Dict[str, ParameterContainer]] = None,
):
    # Obtain domain kwargs from "rule state" (i.e., variables and parameters); from instance variable otherwise.
    metric_domain_kwargs = get_parameter_value_and_validate_return_type(
        domain=domain,
        parameter_reference=metric_domain_kwargs,
        expected_return_type=None,
        variables=variables,
        parameters=parameters,
    )
    if metric_domain_kwargs is None:
        metric_domain_kwargs = {}

    metric_domain_kwargs = copy.deepcopy(metric_domain_kwargs)

    if batch_id:
        metric_domain_kwargs["batch_id"] = batch_id

    return metric_domain_kwargs


def get_parameter_value_and_validate_return_type(
    domain: Optional[Domain] = None,
    parameter_reference: Optional[Union[Any, str]] = None,
    expected_return_type: Optional[Union[type, tuple]] = None,
    variables: Optional[ParameterContainer] = None,
    parameters: Optional[Dict[str, ParameterContainer]] = None,
) -> Optional[Any]:
    """
    This method allows for the parameter_reference to be specified as an object (literal, dict, any typed object, etc.)
    or as a fully-qualified parameter name.  In either case, it can optionally validate the type of the return value.
    """
    if isinstance(parameter_reference, dict):
        parameter_reference = safe_deep_copy(data=parameter_reference)

    parameter_reference = get_parameter_value(
        domain=domain,
        parameter_reference=parameter_reference,
        variables=variables,
        parameters=parameters,
    )

    if expected_return_type is not None:
        if not isinstance(parameter_reference, expected_return_type):
            raise ge_exceptions.ProfilerExecutionError(
                message=f"""Argument "{parameter_reference}" must be of type "{str(expected_return_type)}" \
(value of type "{str(type(parameter_reference))}" was encountered).
"""
            )

    return parameter_reference


def get_parameter_value(
    domain: Optional[Domain] = None,
    parameter_reference: Optional[Union[Any, str]] = None,
    variables: Optional[ParameterContainer] = None,
    parameters: Optional[Dict[str, ParameterContainer]] = None,
) -> Optional[Any]:
    """
    This method allows for the parameter_reference to be specified as an object (literal, dict, any typed object, etc.)
    or as a fully-qualified parameter name.  Moreover, if the parameter_reference argument is an object of type "dict",
    it will recursively detect values using the fully-qualified parameter name format and evaluate them accordingly.
    """
    if isinstance(parameter_reference, dict):
        for key, value in parameter_reference.items():
            parameter_reference[key] = get_parameter_value(
                domain=domain,
                parameter_reference=value,
                variables=variables,
                parameters=parameters,
            )
    elif isinstance(parameter_reference, (list, set, tuple)):
        parameter_reference_type: type = type(parameter_reference)
        element: Any
        return parameter_reference_type(
            [
                get_parameter_value(
                    domain=domain,
                    parameter_reference=element,
                    variables=variables,
                    parameters=parameters,
                )
                for element in parameter_reference
            ]
        )
    elif isinstance(
        parameter_reference, str
    ) and is_fully_qualified_parameter_name_literal_string_format(
        fully_qualified_parameter_name=parameter_reference
    ):
        parameter_reference = get_parameter_value_by_fully_qualified_parameter_name(
            fully_qualified_parameter_name=parameter_reference,
            domain=domain,
            variables=variables,
            parameters=parameters,
        )
        parameter_reference = get_parameter_value(
            domain=domain,
            parameter_reference=parameter_reference,
            variables=variables,
            parameters=parameters,
        )

    return parameter_reference


def get_resolved_metrics_by_key(
    validator: "Validator",  # noqa: F821
    metric_configurations_by_key: Dict[str, List[MetricConfiguration]],
) -> Dict[str, Dict[Tuple[str, str, str], Any]]:
    """
    Compute (resolve) metrics for every column name supplied on input.

    Args:
        validator: Validator used to compute column cardinality.
        metric_configurations_by_key: metric configurations used to compute figures of merit.
        Dictionary of the form {
            "my_key": List[MetricConfiguration],  # examples of "my_key" are: "my_column_name", "my_batch_id", etc.
        }

    Returns:
        Dictionary of the form {
            "my_key": Dict[Tuple[str, str, str], Any],
        }
    """
    key: str
    metric_configuration: MetricConfiguration
    metric_configurations_for_key: List[MetricConfiguration]

    # Step 1: Gather "MetricConfiguration" objects corresponding to all possible key values/combinations.
    # and compute all metric values (resolve "MetricConfiguration" objects ) using a single method call.
    resolved_metrics: Dict[Tuple[str, str, str], Any] = validator.compute_metrics(
        metric_configurations=[
            metric_configuration
            for key, metric_configurations_for_key in metric_configurations_by_key.items()
            for metric_configuration in metric_configurations_for_key
        ]
    )

    # Step 2: Gather "MetricConfiguration" ID values for each key (one element per batch_id in every list).
    metric_configuration_ids_by_key: Dict[str, List[Tuple[str, str, str]]] = {
        key: [
            metric_configuration.id
            for metric_configuration in metric_configurations_for_key
        ]
        for key, metric_configurations_for_key in metric_configurations_by_key.items()
    }

    metric_configuration_ids: List[Tuple[str, str, str]]
    # Step 3: Obtain flattened list of "MetricConfiguration" ID values across all key values/combinations.
    metric_configuration_ids_all_keys: List[Tuple[str, str, str]] = list(
        itertools.chain(
            *[
                metric_configuration_ids
                for metric_configuration_ids in metric_configuration_ids_by_key.values()
            ]
        )
    )

    # Step 4: Retain only those metric computation results that both, correspond to "MetricConfiguration" objects of
    # interest (reflecting specified key values/combinations).
    metric_configuration_id: Tuple[str, str, str]
    metric_value: Any
    resolved_metrics = {
        metric_configuration_id: metric_value
        for metric_configuration_id, metric_value in resolved_metrics.items()
        if metric_configuration_id in metric_configuration_ids_all_keys
    }

    # Step 5: Gather "MetricConfiguration" ID values for effective collection of resolved metrics.
    metric_configuration_ids_resolved_metrics: List[Tuple[str, str, str]] = list(
        resolved_metrics.keys()
    )

    # Step 6: Produce "key" list, corresponding to effective "MetricConfiguration" ID values.
    candidate_keys: List[str] = [
        key
        for key, metric_configuration_ids in metric_configuration_ids_by_key.items()
        if all(
            [
                metric_configuration_id in metric_configuration_ids_resolved_metrics
                for metric_configuration_id in metric_configuration_ids
            ]
        )
    ]

    resolved_metrics_by_key: Dict[str, Dict[Tuple[str, str, str], Any]] = {
        key: {
            metric_configuration.id: resolved_metrics[metric_configuration.id]
            for metric_configuration in metric_configurations_by_key[key]
        }
        for key in candidate_keys
    }

    return resolved_metrics_by_key


def build_domains_from_column_names(
    rule_name: str,
    column_names: List[str],
    domain_type: MetricDomainTypes,
    table_column_name_to_inferred_semantic_domain_type_map: Optional[
        Dict[str, SemanticDomainTypes]
    ] = None,
) -> List[Domain]:
    """
    This utility method builds "simple" Domain objects (i.e., required fields only, no "details" metadata accepted).

    :param rule_name: name of Rule object, for which "Domain" objects are obtained.
    :param column_names: list of column names to serve as values for "column" keys in "domain_kwargs" dictionary
    :param domain_type: type of Domain objects (same "domain_type" must be applicable to all Domain objects returned)
    :param table_column_name_to_inferred_semantic_domain_type_map: map from column name to inferred semantic type
    :return: list of resulting Domain objects
    """
    column_name: str
    domains: List[Domain] = [
        Domain(
            domain_type=domain_type,
            domain_kwargs={
                "column": column_name,
            },
            details={
                INFERRED_SEMANTIC_TYPE_KEY: {
                    column_name: table_column_name_to_inferred_semantic_domain_type_map[
                        column_name
                    ],
                }
                if table_column_name_to_inferred_semantic_domain_type_map
                else None,
            },
            rule_name=rule_name,
        )
        for column_name in column_names
    ]

    return domains


def convert_variables_to_dict(
    variables: Optional[ParameterContainer] = None,
) -> Dict[str, Any]:
    variables_as_dict: Optional[
        Union[ParameterNode, Dict[str, Any]]
    ] = get_parameter_value_and_validate_return_type(
        domain=None,
        parameter_reference=VARIABLES_PREFIX,
        expected_return_type=None,
        variables=variables,
        parameters=None,
    )
    if isinstance(variables_as_dict, ParameterNode):
        return variables_as_dict.to_dict()

    if variables_as_dict is None:
        return {}

    return variables_as_dict


def integer_semantic_domain_type(domain: Domain) -> bool:
    """
    This method examines "INFERRED_SEMANTIC_TYPE_KEY" attribute of "Domain" argument to check whether or not underlying
    "SemanticDomainTypes" enum value is an "integer".  Because explicitly designated "SemanticDomainTypes.INTEGER" type
    is unavaiable, "SemanticDomainTypes.LOGIC" and "SemanticDomainTypes.IDENTIFIER" are intepreted as "integer" values.

    This method can be used "NumericMetricRangeMultiBatchParameterBuilder._get_round_decimals_using_heuristics()".

    Note: Inability to assess underlying "SemanticDomainTypes" details of "Domain" object produces "False" return value.

    Args:
        domain: "Domain" object to inspect for underlying "SemanticDomainTypes" details

    Returns:
        Boolean value indicating whether or not specified "Domain" is inferred to denote "integer" values

    """

    inferred_semantic_domain_type: Dict[str, SemanticDomainTypes] = domain.details.get(
        INFERRED_SEMANTIC_TYPE_KEY
    )

    semantic_domain_type: SemanticDomainTypes
    return inferred_semantic_domain_type and all(
        [
            semantic_domain_type
            in [
                SemanticDomainTypes.LOGIC,
                SemanticDomainTypes.IDENTIFIER,
            ]
            for semantic_domain_type in (inferred_semantic_domain_type.values())
        ]
    )


def compute_quantiles(
    metric_values: np.ndarray,
    false_positive_rate: np.float64,
    quantile_statistic_interpolation_method: str,
) -> NumericRangeEstimationResult:
    lower_quantile = numpy_quantile(
        a=metric_values,
        q=(false_positive_rate / 2),
        axis=0,
        method=quantile_statistic_interpolation_method,
    )
    upper_quantile = numpy_quantile(
        a=metric_values,
        q=1.0 - (false_positive_rate / 2),
        axis=0,
        method=quantile_statistic_interpolation_method,
    )
    return NumericRangeEstimationResult(
        estimation_histogram=np.histogram(a=metric_values, bins=NUM_HISTOGRAM_BINS)[0],
        value_range=np.asarray([lower_quantile, upper_quantile]),
    )


def compute_kde_quantiles_point_estimate(
    metric_values: np.ndarray,
    false_positive_rate: np.float64,
    quantile_statistic_interpolation_method: str,
    n_resamples: int,
    bw_method: Union[str, float, Callable],
    random_seed: Optional[int] = None,
) -> NumericRangeEstimationResult:
    """
    ML Flow Experiment: parameter_builders_bootstrap/kde_quantiles
    ML Flow Experiment ID: 721280826919117

    An internal implementation of the "kernel density estimation" estimator method, returning a point estimate for a
    population parameter of interest (lower and upper quantiles in this case).

    Overview: https://en.wikipedia.org/wiki/Kernel_density_estimation
    Bandwidth Effect: https://en.wikipedia.org/wiki/Kernel_density_estimation#Bandwidth_selection
    Bandwidth Method: https://stats.stackexchange.com/questions/90656/kernel-bandwidth-scotts-vs-silvermans-rules

    Args:
        false_positive_rate: user-configured fraction between 0 and 1 expressing desired false positive rate for
            identifying unexpected values as judged by the upper- and lower- quantiles of the observed metric data.
        quantile_statistic_interpolation_method: Supplies value of (interpolation) "method" to "np.quantile()"
            statistic, used for confidence intervals.
        n_resamples: A positive integer indicating the sample size resulting from the sampling with replacement
            procedure.
        bw_method: The estimator bandwidth as described in:
            https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.gaussian_kde.html
        random_seed: An optional random_seed to pass to "np.random.Generator(np.random.PCG64(random_seed))"
            for making probabilistic sampling deterministic.
    """
    lower_quantile_pct: float = false_positive_rate / 2.0
    upper_quantile_pct: float = 1.0 - (false_positive_rate / 2.0)

    metric_values_density_estimate: stats.gaussian_kde = stats.gaussian_kde(
        metric_values, bw_method=bw_method
    )
    metric_values_gaussian_sample: float
    if random_seed:
        metric_values_gaussian_sample = metric_values_density_estimate.resample(
            n_resamples,
            seed=random_seed,
        )
    else:
        metric_values_gaussian_sample = metric_values_density_estimate.resample(
            n_resamples,
        )

    lower_quantile_point_estimate: float = np.quantile(
        metric_values_gaussian_sample,
        q=lower_quantile_pct,
        interpolation=quantile_statistic_interpolation_method,
    )
    upper_quantile_point_estimate: float = np.quantile(
        metric_values_gaussian_sample,
        q=upper_quantile_pct,
        interpolation=quantile_statistic_interpolation_method,
    )

    return NumericRangeEstimationResult(
        estimation_histogram=np.histogram(
            a=metric_values_gaussian_sample, bins=NUM_HISTOGRAM_BINS
        )[0],
        value_range=[
            lower_quantile_point_estimate,
            upper_quantile_point_estimate,
        ],
    )


def compute_bootstrap_quantiles_point_estimate(
    metric_values: np.ndarray,
    false_positive_rate: np.float64,
    quantile_statistic_interpolation_method: str,
    n_resamples: int,
    random_seed: Optional[int] = None,
) -> NumericRangeEstimationResult:
    """
    ML Flow Experiment: parameter_builders_bootstrap/bootstrap_quantiles
    ML Flow Experiment ID: 4129654509298109

    An internal implementation of the "bootstrap" estimator method, returning a point estimate for a population
    parameter of interest (lower and upper quantiles in this case). See
    https://en.wikipedia.org/wiki/Bootstrapping_(statistics) for an introduction to "bootstrapping" in statistics.

    The methods implemented here can be found in:
    Efron, B., & Tibshirani, R. J. (1993). Estimates of bias. An Introduction to the Bootstrap (pp. 124-130).
        Springer Science and Business Media Dordrecht. DOI 10.1007/978-1-4899-4541-9

    This implementation is sub-par compared to the one available from the "SciPy" standard library
    ("https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.bootstrap.html"), in that it does not handle
    multi-dimensional statistics. "scipy.stats.bootstrap" is vectorized, thus having the ability to accept a
    multi-dimensional statistic function and process all dimensions.

    Unfortunately, as of March 4th, 2022, the SciPy implementation has two issues: 1) it only returns a confidence
    interval and not a point estimate for the population parameter of interest, which is what we require for our use
    cases. 2) It can not handle multi-dimensional statistics and correct for bias simultaneously. You must either use
    one feature or the other.

    This implementation could only be replaced by "scipy.stats.bootstrap" if Great Expectations drops support for
    Python 3.6, thereby enabling us to use a more up-to-date version of the "scipy" Python package (the currently used
    version does not have "bootstrap"). Also, as discussed above, two contributions would need to be made to the SciPy
    package to enable 1) bias correction for multi-dimensional statistics and 2) a return value of a point estimate for
    the population parameter of interest (lower and upper quantiles in this case).

    Additional future direction could include developing enhancements to bootstrapped estimator based on theory
    presented in "http://dido.econ.yale.edu/~dwka/pub/p1001.pdf":
    @article{Andrews2000a,
        added-at = {2008-04-25T10:38:44.000+0200},
        author = {Andrews, Donald W. K. and Buchinsky, Moshe},
        biburl = {https://www.bibsonomy.org/bibtex/28e2f0a58cdb95e39659921f989a17bdd/smicha},
        day = 01,
        interhash = {778746398daa9ba63bdd95391f1efd37},
        intrahash = {8e2f0a58cdb95e39659921f989a17bdd},
        journal = {Econometrica},
        keywords = {imported},
        month = Jan,
        note = {doi: 10.1111/1468-0262.00092},
        number = 1,
        pages = {23--51},
        timestamp = {2008-04-25T10:38:52.000+0200},
        title = {A Three-step Method for Choosing the Number of Bootstrap Repetitions},
        url = {http://www.blackwell-synergy.com/doi/abs/10.1111/1468-0262.00092},
        volume = 68,
        year = 2000
    }
    The article outlines a three-step minimax procedure that relies on the Central Limit Theorem (C.L.T.) along with the
    bootstrap sampling technique (see https://en.wikipedia.org/wiki/Bootstrapping_(statistics) for background) for
    computing the stopping criterion, expressed as the optimal number of bootstrap samples, needed to achieve a maximum
    probability that the value of the statistic of interest will be minimally deviating from its actual (ideal) value.
    """
    lower_quantile_pct: float = false_positive_rate / 2
    upper_quantile_pct: float = 1.0 - false_positive_rate / 2

    sample_lower_quantile: np.ndarray = numpy_quantile(
        a=metric_values,
        q=lower_quantile_pct,
        method=quantile_statistic_interpolation_method,
    )
    sample_upper_quantile: np.ndarray = numpy_quantile(
        a=metric_values,
        q=upper_quantile_pct,
        method=quantile_statistic_interpolation_method,
    )

    bootstraps: np.ndarray
    if random_seed:
        random_state: np.random.Generator = np.random.Generator(
            np.random.PCG64(random_seed)
        )
        bootstraps = random_state.choice(
            metric_values, size=(n_resamples, metric_values.size)
        )
    else:
        bootstraps = np.random.choice(
            metric_values, size=(n_resamples, metric_values.size)
        )

    lower_quantile_bias_corrected_point_estimate: Number = _determine_quantile_bias_corrected_point_estimate(
        bootstraps=bootstraps,
        quantile_pct=lower_quantile_pct,
        quantile_statistic_interpolation_method=quantile_statistic_interpolation_method,
        sample_quantile=sample_lower_quantile,
    )

    upper_quantile_bias_corrected_point_estimate: Number = _determine_quantile_bias_corrected_point_estimate(
        bootstraps=bootstraps,
        quantile_pct=upper_quantile_pct,
        quantile_statistic_interpolation_method=quantile_statistic_interpolation_method,
        sample_quantile=sample_upper_quantile,
    )

    return NumericRangeEstimationResult(
        estimation_histogram=np.histogram(
            a=bootstraps.flatten(), bins=NUM_HISTOGRAM_BINS
        )[0],
        value_range=[
            lower_quantile_bias_corrected_point_estimate,
            upper_quantile_bias_corrected_point_estimate,
        ],
    )


def _determine_quantile_bias_corrected_point_estimate(
    bootstraps: np.ndarray,
    quantile_pct: float,
    quantile_statistic_interpolation_method: str,
    sample_quantile: np.ndarray,
) -> Number:
    bootstrap_quantiles: Union[np.ndarray, Number] = numpy_quantile(
        bootstraps,
        q=quantile_pct,
        axis=1,
        method=quantile_statistic_interpolation_method,
    )
    bootstrap_quantile_point_estimate: np.ndarray = np.mean(bootstrap_quantiles)
    bootstrap_quantile_standard_error: np.ndarray = np.std(bootstrap_quantiles)
    bootstrap_quantile_bias: float = bootstrap_quantile_point_estimate - sample_quantile

    # Bias / Standard Error > 0.25 is a rule of thumb for when to apply bias correction.
    # See:
    # Efron, B., & Tibshirani, R. J. (1993). Estimates of bias. An Introduction to the Bootstrap (pp. 128).
    #         Springer Science and Business Media Dordrecht. DOI 10.1007/978-1-4899-4541-9
    quantile_bias_corrected_point_estimate: Number

    if (
        bootstrap_quantile_standard_error > 0
        and bootstrap_quantile_bias / bootstrap_quantile_standard_error <= 0.25
    ):
        quantile_bias_corrected_point_estimate = bootstrap_quantile_point_estimate
    else:
        quantile_bias_corrected_point_estimate = (
            bootstrap_quantile_point_estimate - bootstrap_quantile_bias
        )
    return quantile_bias_corrected_point_estimate


def get_validator_with_expectation_suite(
    batch_request: Union[BatchRequestBase, dict],
    data_context: "BaseDataContext",  # noqa: F821
    expectation_suite: Optional["ExpectationSuite"] = None,  # noqa: F821
    expectation_suite_name: Optional[str] = None,
    component_name: str = "test",
    persist: bool = False,
) -> "Validator":  # noqa: F821
    """
    Instantiates and returns "Validator" object using "data_context", "batch_request", and other available information.
    Use "expectation_suite" if provided.  If not, then if "expectation_suite_name" is specified, then create
    "ExpectationSuite" from it.  Otherwise, generate temporary "expectation_suite_name" using supplied "component_name".
    """
    assert expectation_suite is None or isinstance(expectation_suite, ExpectationSuite)
    assert expectation_suite_name is None or isinstance(expectation_suite_name, str)

    expectation_suite = get_or_create_expectation_suite(
        data_context=data_context,
        expectation_suite=expectation_suite,
        expectation_suite_name=expectation_suite_name,
        component_name=component_name,
        persist=persist,
    )

    batch_request = materialize_batch_request(batch_request=batch_request)
    validator: "Validator" = data_context.get_validator(  # noqa: F821
        batch_request=batch_request,
        expectation_suite=expectation_suite,
    )

    return validator


def get_or_create_expectation_suite(
    data_context: "BaseDataContext",  # noqa: F821
    expectation_suite: Optional["ExpectationSuite"] = None,  # noqa: F821
    expectation_suite_name: Optional[str] = None,
    component_name: Optional[str] = None,
    persist: bool = False,
) -> "ExpectationSuite":  # noqa: F821
    """
    Use "expectation_suite" if provided.  If not, then if "expectation_suite_name" is specified, then create
    "ExpectationSuite" from it.  Otherwise, generate temporary "expectation_suite_name" using supplied "component_name".
    """
    generate_temp_expectation_suite_name: bool
    create_expectation_suite: bool

    if expectation_suite is not None and expectation_suite_name is not None:
        if expectation_suite.expectation_suite_name != expectation_suite_name:
            raise ValueError(
                'Mutually inconsistent "expectation_suite" and "expectation_suite_name" were specified.'
            )

        return expectation_suite
    elif expectation_suite is None and expectation_suite_name is not None:
        generate_temp_expectation_suite_name = False
        create_expectation_suite = True
    elif expectation_suite is not None and expectation_suite_name is None:
        generate_temp_expectation_suite_name = False
        create_expectation_suite = False
    else:
        generate_temp_expectation_suite_name = True
        create_expectation_suite = True

    if generate_temp_expectation_suite_name:
        if not component_name:
            component_name = "test"

        expectation_suite_name = f"{TEMPORARY_EXPECTATION_SUITE_NAME_PREFIX}.{component_name}.{TEMPORARY_EXPECTATION_SUITE_NAME_STEM}.{str(uuid.uuid4())[:8]}"

    if create_expectation_suite:
        if persist:
            try:
                # noinspection PyUnusedLocal
                expectation_suite = data_context.get_expectation_suite(
                    expectation_suite_name=expectation_suite_name
                )
            except ge_exceptions.DataContextError:
                expectation_suite = data_context.create_expectation_suite(
                    expectation_suite_name=expectation_suite_name
                )
                logger.info(
                    f'Created ExpectationSuite "{expectation_suite.expectation_suite_name}".'
                )
        else:
            expectation_suite = ExpectationSuite(
                expectation_suite_name=expectation_suite_name,
                data_context=data_context,
            )

    return expectation_suite


def sanitize_parameter_name(name: str) -> str:
    """
    This method provides display-friendly version of "name" argument.
    """
    return name.replace(FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER, "_")
