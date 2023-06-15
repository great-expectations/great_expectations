from __future__ import annotations

import copy
import datetime
import hashlib
import itertools
import logging
import re
import uuid
import warnings
from numbers import Number
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Protocol,
    Tuple,
    Union,
)

import numpy as np
from scipy import stats

import great_expectations.exceptions as gx_exceptions
from great_expectations.compatibility import numpy
from great_expectations.core import ExpectationSuite
from great_expectations.core.batch import (
    Batch,
    BatchRequest,
    BatchRequestBase,
    RuntimeBatchRequest,
    materialize_batch_request,
)
from great_expectations.core.domain import (
    INFERRED_SEMANTIC_TYPE_KEY,
    SemanticDomainTypes,
)
from great_expectations.core.metric_domain_types import (
    MetricDomainTypes,  # noqa: TCH001
)
from great_expectations.rule_based_profiler.estimators.numeric_range_estimation_result import (
    NUM_HISTOGRAM_BINS,
    NumericRangeEstimationResult,
)
from great_expectations.rule_based_profiler.parameter_container import (
    FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER,
    VARIABLES_PREFIX,
    Domain,
    ParameterContainer,
    ParameterNode,
    get_parameter_value_by_fully_qualified_parameter_name,
    is_fully_qualified_parameter_name_prefix_in_literal,
)
from great_expectations.types import safe_deep_copy
from great_expectations.util import (
    convert_ndarray_datetime_to_float_dtype_utc_timezone,
    convert_ndarray_float_to_datetime_dtype,
    convert_ndarray_to_datetime_dtype_best_effort,
)
from great_expectations.validator.computed_metric import MetricValue  # noqa: TCH001
from great_expectations.validator.metric_configuration import (
    MetricConfiguration,  # noqa: TCH001
)

if TYPE_CHECKING:
    from typing_extensions import TypeGuard

    from great_expectations.data_context.data_context.abstract_data_context import (
        AbstractDataContext,
    )
    from great_expectations.validator.validator import Validator

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

NP_EPSILON: Union[Number, np.float64] = np.finfo(float).eps

TEMPORARY_EXPECTATION_SUITE_NAME_PREFIX: str = "tmp"
TEMPORARY_EXPECTATION_SUITE_NAME_STEM: str = "suite"
TEMPORARY_EXPECTATION_SUITE_NAME_PATTERN: re.Pattern = re.compile(
    rf"^{TEMPORARY_EXPECTATION_SUITE_NAME_PREFIX}\..+\.{TEMPORARY_EXPECTATION_SUITE_NAME_STEM}\.\w{8}"
)

RECOGNIZED_QUANTILE_STATISTIC_INTERPOLATION_METHODS: set = {
    "auto",
    "nearest",
    "linear",
}


def get_validator(  # noqa: PLR0913
    purpose: str,
    *,
    data_context: Optional[AbstractDataContext] = None,
    batch_list: Optional[List[Batch]] = None,
    batch_request: Optional[Union[str, BatchRequestBase, dict]] = None,
    domain: Optional[Domain] = None,
    variables: Optional[ParameterContainer] = None,
    parameters: Optional[Dict[str, ParameterContainer]] = None,
) -> Optional[Validator]:
    validator: Optional[Validator]

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
    if batch_list is None or all(batch is None for batch in batch_list):
        if batch_request is None:
            return None

        batch_request = build_batch_request(
            domain=domain,
            batch_request=batch_request,
            variables=variables,
            parameters=parameters,
        )
    else:
        num_batches: int = len(batch_list)
        if num_batches == 0:
            raise gx_exceptions.ProfilerExecutionError(
                message=f"""{__name__}.get_validator() must utilize at least one Batch ({num_batches} are available).
"""
            )

    validator = get_validator_with_expectation_suite(
        data_context=data_context,
        batch_list=batch_list,
        batch_request=batch_request,
        expectation_suite=None,
        expectation_suite_name=expectation_suite_name,
        component_name=f"rule_based_profiler-{expectation_suite_name}",
        persist=False,
    )

    # Always disabled for RBP and DataAssistants due to volume of metric calculations
    validator.show_progress_bars = False

    return validator


def get_batch_ids(  # noqa: PLR0913
    data_context: Optional[AbstractDataContext] = None,
    batch_list: Optional[List[Batch]] = None,
    batch_request: Optional[Union[str, BatchRequestBase, dict]] = None,
    limit: Optional[int] = None,
    domain: Optional[Domain] = None,
    variables: Optional[ParameterContainer] = None,
    parameters: Optional[Dict[str, ParameterContainer]] = None,
) -> Optional[List[str]]:
    batch: Batch
    if batch_list is None or all(batch is None for batch in batch_list):
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

    if limit is not None:
        # No need to verify that type of "limit" is "integer", because static type checking already ascertains this.
        if not (0 <= limit <= num_batch_ids):
            raise gx_exceptions.ProfilerExecutionError(
                message=f"""{__name__}.get_batch_ids() allows integer limit values between 0 and {num_batch_ids} \
({limit} was requested).
"""
            )
        batch_ids = batch_ids[-limit:]

    if num_batch_ids == 0:
        raise gx_exceptions.ProfilerExecutionError(
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
) -> Any:
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
            raise gx_exceptions.ProfilerExecutionError(
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
    ) and is_fully_qualified_parameter_name_prefix_in_literal(
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
    validator: Validator,
    metric_configurations_by_key: Dict[str, List[MetricConfiguration]],
    runtime_configuration: Optional[dict] = None,
) -> Dict[str, Dict[Tuple[str, str, str], MetricValue]]:
    """
    Compute (resolve) metrics for every column name supplied on input.

    Args:
        validator: Validator used to compute column cardinality.
        metric_configurations_by_key: metric configurations used to compute figures of merit.
        Dictionary of the form {
            "my_key": List[MetricConfiguration],  # examples of "my_key" are: "my_column_name", "my_batch_id", etc.
        }
        runtime_configuration: Additional run-time settings (see "Validator.DEFAULT_RUNTIME_CONFIGURATION").

    Returns:
        Dictionary of the form {
            "my_key": Dict[Tuple[str, str, str], MetricValue],
        }
    """
    key: str
    metric_configuration: MetricConfiguration
    metric_configurations_for_key: List[MetricConfiguration]

    # Step 1: Gather "MetricConfiguration" objects corresponding to all possible key values/combinations.
    # and compute all metric values (resolve "MetricConfiguration" objects ) using a single method call.
    resolved_metrics: Dict[
        Tuple[str, str, str], MetricValue
    ] = validator.compute_metrics(
        metric_configurations=[
            metric_configuration
            for key, metric_configurations_for_key in metric_configurations_by_key.items()
            for metric_configuration in metric_configurations_for_key
        ],
        runtime_configuration=runtime_configuration,
        min_graph_edges_pbar_enable=0,
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
            metric_configuration_id in metric_configuration_ids_resolved_metrics
            for metric_configuration_id in metric_configuration_ids
        )
    ]

    resolved_metrics_by_key: Dict[str, Dict[Tuple[str, str, str], MetricValue]] = {
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
        semantic_domain_type
        in [
            SemanticDomainTypes.LOGIC,
            SemanticDomainTypes.IDENTIFIER,
        ]
        for semantic_domain_type in inferred_semantic_domain_type.values()
    )


def datetime_semantic_domain_type(domain: Domain) -> bool:
    """
    This method examines "INFERRED_SEMANTIC_TYPE_KEY" attribute of "Domain" argument to check whether or not underlying
    "SemanticDomainTypes" enum value is "SemanticDomainTypes.DATETIME".

    Note: Inability to assess underlying "SemanticDomainTypes" details of "Domain" object produces "False" return value.

    Args:
        domain: "Domain" object to inspect for underlying "SemanticDomainTypes" details

    Returns:
        Boolean value indicating whether or not specified "Domain" is inferred as "SemanticDomainTypes.DATETIME"
    """

    inferred_semantic_domain_type: Dict[str, SemanticDomainTypes] = domain.details.get(
        INFERRED_SEMANTIC_TYPE_KEY
    )

    semantic_domain_type: SemanticDomainTypes
    return inferred_semantic_domain_type and all(
        semantic_domain_type == SemanticDomainTypes.DATETIME
        for semantic_domain_type in inferred_semantic_domain_type.values()
    )


def get_false_positive_rate_from_rule_state(
    false_positive_rate: Union[str, float],
    domain: Domain,
    variables: Optional[ParameterContainer] = None,
    parameters: Optional[Dict[str, ParameterContainer]] = None,
) -> Union[float, np.float64]:
    """
    This method obtains false_positive_rate from "rule state" (i.e., variables and parameters) and validates the result.
    """
    if false_positive_rate is None:
        return 5.0e-2

    # Obtain false_positive_rate from "rule state" (i.e., variables and parameters); from instance variable otherwise.
    false_positive_rate = get_parameter_value_and_validate_return_type(
        domain=domain,
        parameter_reference=false_positive_rate,
        expected_return_type=(float, np.float64),
        variables=variables,
        parameters=parameters,
    )
    if not (0.0 <= false_positive_rate <= 1.0):  # noqa: PLR2004
        raise gx_exceptions.ProfilerExecutionError(
            f"""false_positive_rate must be a positive decimal number between 0 and 1 inclusive [0, 1], but \
{false_positive_rate} was provided.
"""
        )
    elif false_positive_rate <= NP_EPSILON:
        warnings.warn(
            f"""You have chosen a false_positive_rate of {false_positive_rate}, which is too close to 0.  A \
false_positive_rate of {NP_EPSILON} has been selected instead.
"""
        )
        false_positive_rate = np.float64(NP_EPSILON)
    elif false_positive_rate >= (1.0 - NP_EPSILON):
        warnings.warn(
            f"""You have chosen a false_positive_rate of {false_positive_rate}, which is too close to 1.  A \
false_positive_rate of {1.0 - NP_EPSILON} has been selected instead.
"""
        )
        false_positive_rate = np.float64(1.0 - NP_EPSILON)

    return false_positive_rate


def get_quantile_statistic_interpolation_method_from_rule_state(
    quantile_statistic_interpolation_method: str,
    round_decimals: int,
    domain: Domain,
    variables: Optional[ParameterContainer] = None,
    parameters: Optional[Dict[str, ParameterContainer]] = None,
) -> str:
    """
    This method obtains quantile_statistic_interpolation_method from "rule state" (i.e., variables and parameters) and
    validates the result.
    """
    # Obtain quantile_statistic_interpolation_method directive from "rule state" (i.e., variables and parameters); from instance variable otherwise.
    quantile_statistic_interpolation_method = (
        get_parameter_value_and_validate_return_type(
            domain=domain,
            parameter_reference=quantile_statistic_interpolation_method,
            expected_return_type=str,
            variables=variables,
            parameters=parameters,
        )
    )
    if (
        quantile_statistic_interpolation_method
        not in RECOGNIZED_QUANTILE_STATISTIC_INTERPOLATION_METHODS
    ):
        raise gx_exceptions.ProfilerExecutionError(
            message=f"""The directive "quantile_statistic_interpolation_method" can be only one of \
{RECOGNIZED_QUANTILE_STATISTIC_INTERPOLATION_METHODS} ("{quantile_statistic_interpolation_method}" was detected).
"""
        )

    if quantile_statistic_interpolation_method == "auto":
        if round_decimals == 0:
            quantile_statistic_interpolation_method = "nearest"
        else:
            quantile_statistic_interpolation_method = "linear"

    return quantile_statistic_interpolation_method


def compute_quantiles(
    metric_values: np.ndarray,
    false_positive_rate: np.float64,
    quantile_statistic_interpolation_method: str,
) -> NumericRangeEstimationResult:
    lower_quantile = numpy.numpy_quantile(
        a=metric_values,
        q=(false_positive_rate / 2.0),
        axis=0,
        method=quantile_statistic_interpolation_method,
    )
    upper_quantile = numpy.numpy_quantile(
        a=metric_values,
        q=1.0 - (false_positive_rate / 2.0),
        axis=0,
        method=quantile_statistic_interpolation_method,
    )
    return build_numeric_range_estimation_result(
        metric_values=metric_values,
        min_value=lower_quantile,
        max_value=upper_quantile,
    )


def compute_kde_quantiles_point_estimate(  # noqa: PLR0913
    metric_values: np.ndarray,
    false_positive_rate: np.float64,
    n_resamples: int,
    quantile_statistic_interpolation_method: str,
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
        metric_values: "numpy.ndarray" of "dtype.float" values with elements corresponding to "Batch" data samples.
        false_positive_rate: user-configured fraction between 0 and 1 expressing desired false positive rate for
            identifying unexpected values as judged by the upper- and lower- quantiles of the observed metric data.
        n_resamples: A positive integer indicating the sample size resulting from the sampling with replacement
            procedure.
        quantile_statistic_interpolation_method: Supplies value of (interpolation) "method" to "np.quantile()"
            statistic, used for confidence intervals.
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

    metric_values_gaussian_sample: np.ndarray
    if random_seed:
        metric_values_gaussian_sample = metric_values_density_estimate.resample(
            n_resamples,
            seed=random_seed,
        )
    else:
        metric_values_gaussian_sample = metric_values_density_estimate.resample(
            n_resamples,
        )

    lower_quantile_point_estimate: Union[
        np.float64, datetime.datetime
    ] = numpy.numpy_quantile(
        metric_values_gaussian_sample,
        q=lower_quantile_pct,
        method=quantile_statistic_interpolation_method,
    )
    upper_quantile_point_estimate: Union[
        np.float64, datetime.datetime
    ] = numpy.numpy_quantile(
        metric_values_gaussian_sample,
        q=upper_quantile_pct,
        method=quantile_statistic_interpolation_method,
    )

    return build_numeric_range_estimation_result(
        metric_values=metric_values,
        min_value=lower_quantile_point_estimate,
        max_value=upper_quantile_point_estimate,
    )


def compute_bootstrap_quantiles_point_estimate(  # noqa: PLR0913
    metric_values: np.ndarray,
    false_positive_rate: np.float64,
    n_resamples: int,
    quantile_statistic_interpolation_method: str,
    quantile_bias_correction: bool,
    quantile_bias_std_error_ratio_threshold: float,
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
    lower_quantile_pct: float = false_positive_rate / 2.0
    upper_quantile_pct: float = 1.0 - false_positive_rate / 2.0

    sample_lower_quantile: np.ndarray = numpy.numpy_quantile(
        a=metric_values,
        q=lower_quantile_pct,
        method=quantile_statistic_interpolation_method,
    )
    sample_upper_quantile: np.ndarray = numpy.numpy_quantile(
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

    lower_quantile_bias_corrected_point_estimate: Union[
        np.float64, datetime.datetime
    ] = _determine_quantile_bias_corrected_point_estimate(
        bootstraps=bootstraps,
        quantile_pct=lower_quantile_pct,
        quantile_statistic_interpolation_method=quantile_statistic_interpolation_method,
        quantile_bias_correction=quantile_bias_correction,
        quantile_bias_std_error_ratio_threshold=quantile_bias_std_error_ratio_threshold,
        sample_quantile=sample_lower_quantile,
    )
    upper_quantile_bias_corrected_point_estimate: Union[
        np.float64, datetime.datetime
    ] = _determine_quantile_bias_corrected_point_estimate(
        bootstraps=bootstraps,
        quantile_pct=upper_quantile_pct,
        quantile_statistic_interpolation_method=quantile_statistic_interpolation_method,
        quantile_bias_correction=quantile_bias_correction,
        quantile_bias_std_error_ratio_threshold=quantile_bias_std_error_ratio_threshold,
        sample_quantile=sample_upper_quantile,
    )

    return build_numeric_range_estimation_result(
        metric_values=metric_values,
        min_value=lower_quantile_bias_corrected_point_estimate,
        max_value=upper_quantile_bias_corrected_point_estimate,
    )


def build_numeric_range_estimation_result(
    metric_values: np.ndarray,
    min_value: Number,
    max_value: Number,
) -> NumericRangeEstimationResult:
    """
    Computes histogram of 1-dimensional set of data points and packages it together with value range as returned output.

    Args:
        metric_values: "numpy.ndarray" of "dtype.float" values with elements corresponding to "Batch" data samples.
        min_value: pre-computed supremum of "metric_values" (properly conditioned for output).
        max_value: pre-computed infimum of "metric_values" (properly conditioned for output).

    Returns:
        Structured "NumericRangeEstimationResult" object, containing histogram and value_range attributes.
    """
    ndarray_is_datetime_type: bool
    metric_values_converted: np.ndarray
    (
        ndarray_is_datetime_type,
        metric_values_converted,
    ) = convert_metric_values_to_float_dtype_best_effort(metric_values=metric_values)

    histogram: Tuple[np.ndarray, np.ndarray]
    bin_edges: np.ndarray
    if ndarray_is_datetime_type:
        histogram = np.histogram(a=metric_values_converted, bins=NUM_HISTOGRAM_BINS)
        # Use "UTC" TimeZone normalization in "bin_edges" when "metric_values" consists of "datetime.datetime" objects.
        bin_edges = convert_ndarray_float_to_datetime_dtype(data=histogram[1])
    else:
        histogram = np.histogram(a=metric_values, bins=NUM_HISTOGRAM_BINS)
        bin_edges = histogram[1]

    return NumericRangeEstimationResult(
        estimation_histogram=np.vstack(
            (
                np.pad(
                    array=histogram[0],
                    pad_width=(0, 1),
                    mode="constant",
                    constant_values=0,
                ),
                bin_edges,
            )
        ),
        value_range=np.asarray([min_value, max_value]),
    )


def _determine_quantile_bias_corrected_point_estimate(  # noqa: PLR0913
    bootstraps: np.ndarray,
    quantile_pct: float,
    quantile_statistic_interpolation_method: str,
    quantile_bias_correction: bool,
    quantile_bias_std_error_ratio_threshold: float,
    sample_quantile: np.ndarray,
) -> np.float64:
    bootstrap_quantiles: Union[np.ndarray, np.float64] = numpy.numpy_quantile(
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
    quantile_bias_corrected_point_estimate: np.float64

    if (
        not quantile_bias_correction
        and bootstrap_quantile_standard_error > 0.0  # noqa: PLR2004
        and bootstrap_quantile_bias / bootstrap_quantile_standard_error
        <= quantile_bias_std_error_ratio_threshold
    ):
        quantile_bias_corrected_point_estimate = bootstrap_quantile_point_estimate
    else:
        quantile_bias_corrected_point_estimate = (
            bootstrap_quantile_point_estimate - bootstrap_quantile_bias
        )

    return quantile_bias_corrected_point_estimate


def convert_metric_values_to_float_dtype_best_effort(
    metric_values: np.ndarray,
) -> Tuple[bool, np.ndarray]:
    """
    Makes best effort attempt to discern element type of 1-D "np.ndarray" and convert it to "float" "np.ndarray" type.

    Note: Conversion of "datetime.datetime" to "float" uses "UTC" TimeZone to normalize all "datetime.datetime" values.

    Return:
        Boolean flag -- True, if conversion of original "np.ndarray" to "datetime.datetime" occurred; False, otherwise.
    """
    original_ndarray_is_datetime_type: bool
    conversion_ndarray_to_datetime_type_performed: bool
    metric_values_converted: np.ndaarray
    (
        original_ndarray_is_datetime_type,
        conversion_ndarray_to_datetime_type_performed,
        metric_values_converted,
    ) = convert_ndarray_to_datetime_dtype_best_effort(
        data=metric_values,
        datetime_detected=False,
        parse_strings_as_datetimes=True,
        fuzzy=False,
    )
    ndarray_is_datetime_type: bool = (
        original_ndarray_is_datetime_type
        or conversion_ndarray_to_datetime_type_performed
    )
    if ndarray_is_datetime_type:
        metric_values_converted = convert_ndarray_datetime_to_float_dtype_utc_timezone(
            data=metric_values_converted
        )
    else:
        metric_values_converted = metric_values

    return ndarray_is_datetime_type, metric_values_converted


def get_validator_with_expectation_suite(  # noqa: PLR0913
    data_context: AbstractDataContext,
    batch_list: Optional[List[Batch]] = None,
    batch_request: Optional[Union[BatchRequestBase, dict]] = None,
    expectation_suite: Optional[ExpectationSuite] = None,
    expectation_suite_name: Optional[str] = None,
    component_name: str = "test",
    persist: bool = False,
) -> Validator:
    """
    Instantiates and returns "Validator" using "data_context", "batch_list" or "batch_request", and other information.
    Use "expectation_suite" if provided; otherwise, if "expectation_suite_name" is specified, then create
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
    validator: Validator = data_context.get_validator(
        batch_list=batch_list,
        batch_request=batch_request,
        expectation_suite=expectation_suite,
    )

    return validator


def get_or_create_expectation_suite(
    data_context: Optional[AbstractDataContext],
    expectation_suite: Optional[ExpectationSuite] = None,
    expectation_suite_name: Optional[str] = None,
    component_name: Optional[str] = None,
    persist: bool = False,
) -> ExpectationSuite:
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
            except gx_exceptions.DataContextError:
                expectation_suite = data_context.add_expectation_suite(
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


def sanitize_parameter_name(
    name: str,
    suffix: Optional[str] = None,
) -> str:
    """
    This method provides display-friendly version of "name" argument (with optional "suffix" argument).

    In most situations, "suffix" is not needed.  However, in certain use cases, "name" argument is same among different
    calling structures, whereby "name" can be disambiguated by addition of distinguishing "suffix" argument.  Using
    list of "MetricConfiguration" objects as example, "metric_name" and "metric_domain_kwargs" are commonly same among
    them, while "metric_value_kwargs" are different (i.e., calculating values of same metric with different parameters).
    In this case, supplying "MetricConfiguration.metric_domain_kwargs_id" as "suffix" makes sanitized names unique.

    Args:
        name: string-valued "name" argument to be sanitized
        suffix: additional component (i.e., "suffix") argument to be appended to "name" and sanitized together

    Returns:
        string-valued sanitized concatenation of "name" and MD5-digest of "suffix" arguments.
    """
    if suffix:
        suffix = hashlib.md5(suffix.encode("utf-8")).hexdigest()
        name = f"{name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{suffix}"

    return name.replace(FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER, "_")


class _NumericIterableWithDtype(Iterable, Protocol):
    @property
    def dtype(self) -> Any:
        ...


def _is_iterable_of_numeric_dtypes(
    obj: Any,
) -> TypeGuard[_NumericIterableWithDtype]:
    if hasattr(obj, "dtype") and np.issubdtype(obj.dtype, np.number):
        return True
    return False
