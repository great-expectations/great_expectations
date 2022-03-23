import copy
import itertools
import logging
import uuid
from numbers import Number
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
from scipy import stats

import great_expectations.exceptions as ge_exceptions
from great_expectations.core import ExpectationSuite
from great_expectations.core.batch import (
    Batch,
    BatchRequest,
    RuntimeBatchRequest,
    materialize_batch_request,
)
from great_expectations.data_context.util import instantiate_class_from_config
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.rule_based_profiler.types import (
    PARAMETER_KEY,
    VARIABLES_PREFIX,
    Builder,
    Domain,
    ParameterContainer,
    get_fully_qualified_parameter_names,
    get_parameter_value_by_fully_qualified_parameter_name,
    is_fully_qualified_parameter_name_literal_string_format,
)
from great_expectations.validator.metric_configuration import MetricConfiguration

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

NP_EPSILON: Union[Number, np.float64] = np.finfo(float).eps


def get_validator(
    purpose: str,
    *,
    data_context: Optional["DataContext"] = None,  # noqa: F821
    batch_list: Optional[List[Batch]] = None,
    batch_request: Optional[Union[str, BatchRequest, RuntimeBatchRequest, dict]] = None,
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

    return validator


def get_batch_ids(
    data_context: Optional["DataContext"] = None,  # noqa: F821
    batch_list: Optional[List[Batch]] = None,
    batch_request: Optional[Union[str, BatchRequest, RuntimeBatchRequest, dict]] = None,
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
    batch_request: Optional[Union[str, BatchRequest, RuntimeBatchRequest, dict]] = None,
    domain: Optional[Domain] = None,
    variables: Optional[ParameterContainer] = None,
    parameters: Optional[Dict[str, ParameterContainer]] = None,
) -> Optional[Union[BatchRequest, RuntimeBatchRequest]]:
    if batch_request is None:
        return None

    # Obtain BatchRequest from "rule state" (i.e., variables and parameters); from instance variable otherwise.
    effective_batch_request: Optional[
        Union[BatchRequest, RuntimeBatchRequest, dict]
    ] = get_parameter_value_and_validate_return_type(
        domain=domain,
        parameter_reference=batch_request,
        expected_return_type=(BatchRequest, RuntimeBatchRequest, dict),
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
        parameter_reference = dict(copy.deepcopy(parameter_reference))

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
        if isinstance(parameter_reference, dict):
            for key, value in parameter_reference.items():
                parameter_reference[key] = get_parameter_value(
                    domain=domain,
                    parameter_reference=value,
                    variables=variables,
                    parameters=parameters,
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


def build_simple_domains_from_column_names(
    column_names: List[str],
    domain_type: MetricDomainTypes = MetricDomainTypes.COLUMN,
) -> List[Domain]:
    """
    This utility method builds "simple" Domain objects (i.e., required fields only, no "details" metadata accepted).

    :param column_names: list of column names to serve as values for "column" keys in "domain_kwargs" dictionary
    :param domain_type: type of Domain objects (same "domain_type" must be applicable to all Domain objects returned)
    :return: list of resulting Domain objects
    """
    column_name: str
    domains: List[Domain] = [
        Domain(
            domain_type=domain_type,
            domain_kwargs={
                "column": column_name,
            },
        )
        for column_name in column_names
    ]

    return domains


def convert_variables_to_dict(
    variables: Optional[ParameterContainer] = None,
) -> Dict[str, Any]:
    variables: Optional[Dict[str, Any]] = get_parameter_value_and_validate_return_type(
        domain=None,
        parameter_reference=VARIABLES_PREFIX,
        expected_return_type=None,
        variables=variables,
        parameters=None,
    )

    return variables or {}


def init_rule_parameter_builders(
    parameter_builder_configs: Optional[List[dict]] = None,
    data_context: Optional["DataContext"] = None,  # noqa: F821
) -> Optional[List["ParameterBuilder"]]:  # noqa: F821
    if parameter_builder_configs is None:
        return None

    return [
        init_parameter_builder(
            parameter_builder_config=parameter_builder_config,
            data_context=data_context,
        )
        for parameter_builder_config in parameter_builder_configs
    ]


def init_parameter_builder(
    parameter_builder_config: Union["ParameterBuilderConfig", dict],  # noqa: F821
    data_context: Optional["DataContext"] = None,  # noqa: F821
) -> "ParameterBuilder":  # noqa: F821
    if not isinstance(parameter_builder_config, dict):
        parameter_builder_config = parameter_builder_config.to_dict()

    parameter_builder: "ParameterBuilder" = instantiate_class_from_config(  # noqa: F821
        config=parameter_builder_config,
        runtime_environment={"data_context": data_context},
        config_defaults={
            "module_name": "great_expectations.rule_based_profiler.parameter_builder"
        },
    )
    return parameter_builder


def init_rule_expectation_configuration_builders(
    expectation_configuration_builder_configs: List[dict],
    data_context: Optional["DataContext"] = None,  # noqa: F821
) -> List["ExpectationConfigurationBuilder"]:  # noqa: F821
    expectation_configuration_builder_config: dict
    return [
        init_expectation_configuration_builder(
            expectation_configuration_builder_config=expectation_configuration_builder_config,
            data_context=data_context,
        )
        for expectation_configuration_builder_config in expectation_configuration_builder_configs
    ]


def init_expectation_configuration_builder(
    expectation_configuration_builder_config: Union[
        "ExpectationConfigurationBuilder", dict  # noqa: F821
    ],
    data_context: Optional["DataContext"] = None,  # noqa: F821
) -> "ExpectationConfigurationBuilder":  # noqa: F821
    if not isinstance(expectation_configuration_builder_config, dict):
        expectation_configuration_builder_config = (
            expectation_configuration_builder_config.to_dict()
        )

    expectation_configuration_builder: "ExpectationConfigurationBuilder" = instantiate_class_from_config(  # noqa: F821
        config=expectation_configuration_builder_config,
        runtime_environment={"data_context": data_context},
        config_defaults={
            "class_name": "DefaultExpectationConfigurationBuilder",
            "module_name": "great_expectations.rule_based_profiler.expectation_configuration_builder",
        },
    )
    return expectation_configuration_builder


def resolve_evaluation_dependencies(
    parameter_builder: "ParameterBuilder",  # noqa: F821
    parameter_container: ParameterContainer,
    domain: Domain,
    variables: Optional[ParameterContainer] = None,
    parameters: Optional[Dict[str, ParameterContainer]] = None,
) -> None:
    evaluation_parameter_builders: List[
        "ParameterBuilder"  # noqa: F821
    ] = parameter_builder.evaluation_parameter_builders
    if not evaluation_parameter_builders:
        return

    fully_qualified_parameter_names: List[str] = get_fully_qualified_parameter_names(
        domain=domain,
        variables=variables,
        parameters=parameters,
    )

    evaluation_parameter_builder: "ParameterBuilder"  # noqa: F821
    for evaluation_parameter_builder in evaluation_parameter_builders:
        fully_qualified_evaluation_parameter_builder_name: str = (
            f"{PARAMETER_KEY}{evaluation_parameter_builder.name}"
        )

        if (
            fully_qualified_evaluation_parameter_builder_name
            not in fully_qualified_parameter_names
        ):
            set_batch_list_or_batch_request_on_builder(
                builder=evaluation_parameter_builder,
                batch_list=parameter_builder.batch_list,
                batch_request=parameter_builder.batch_request,
                force_batch_data=False,
            )

            evaluation_parameter_builder.build_parameters(
                parameter_container=parameter_container,
                domain=domain,
                variables=variables,
                parameters=parameters,
            )

            resolve_evaluation_dependencies(
                parameter_builder=evaluation_parameter_builder,
                parameter_container=parameter_container,
                domain=domain,
                variables=variables,
                parameters=parameters,
            )


def set_batch_list_or_batch_request_on_builder(
    builder: Builder,
    batch_list: Optional[List[Batch]] = None,
    batch_request: Optional[Union[BatchRequest, RuntimeBatchRequest, dict]] = None,
    force_batch_data: bool = False,
) -> None:
    if force_batch_data or builder.batch_request is None:
        builder.set_batch_data(
            batch_list=batch_list,
            batch_request=batch_request,
        )


def compute_quantiles(
    metric_values: np.ndarray,
    false_positive_rate: np.float64,
) -> Tuple[Number, Number]:
    lower_quantile = np.quantile(
        metric_values,
        q=(false_positive_rate / 2),
        axis=0,
    )
    upper_quantile = np.quantile(
        metric_values,
        q=1.0 - (false_positive_rate / 2),
        axis=0,
    )
    return lower_quantile, upper_quantile


def compute_bootstrap_quantiles_point_estimate(
    metric_values: np.ndarray,
    false_positive_rate: np.float64,
    n_resamples: int,
    random_seed: Optional[int] = None,
) -> Tuple[Number, Number]:
    """The winner of our performance testing is selected from the possible candidates:
    - _compute_bootstrap_quantiles_point_estimate_custom_bias_corrected_method
    - _compute_bootstrap_quantiles_point_estimate_custom_mean_method
    - _compute_bootstrap_quantiles_point_estimate_scipy_confidence_interval_midpoint_method"""
    return _compute_bootstrap_quantiles_point_estimate_custom_bias_corrected_method(
        metric_values=metric_values,
        false_positive_rate=false_positive_rate,
        n_resamples=n_resamples,
        random_seed=random_seed,
    )


def _compute_bootstrap_quantiles_point_estimate_custom_mean_method(
    metric_values: np.ndarray,
    false_positive_rate: np.float64,
    n_resamples: int,
    random_seed: Optional[int] = None,
) -> Tuple[Number, Number]:
    """
    An internal implementation of the "bootstrap" estimator method, returning a point estimate for a population
    parameter of interest (lower and upper quantiles in this case). See
    https://en.wikipedia.org/wiki/Bootstrapping_(statistics) for an introduction to "bootstrapping" in statistics.

    This implementation has been replaced by "_compute_bootstrap_quantiles_point_estimate_custom_bias_corrected_method"
    and only remains to demonstrate the performance improvement achieved by correcting for bias. Upon the implementation
    of a Machine Learning Lifecycle framework, the performance improvement can be documented and this legacy method can
    be removed from the codebase.
    """
    if random_seed:
        random_state: np.random.Generator = np.random.Generator(
            np.random.PCG64(random_seed)
        )
        bootstraps: np.ndarray = random_state.choice(
            metric_values, size=(n_resamples, metric_values.size)
        )
    else:
        bootstraps: np.ndarray = np.random.choice(
            metric_values, size=(n_resamples, metric_values.size)
        )

    lower_quantiles: Union[np.ndarray, Number] = np.quantile(
        bootstraps,
        q=false_positive_rate / 2,
        axis=1,
    )
    lower_quantile_point_estimate: Number = np.mean(lower_quantiles)
    upper_quantiles: Union[np.ndarray, Number] = np.quantile(
        bootstraps,
        q=1.0 - (false_positive_rate / 2),
        axis=1,
    )
    upper_quantile_point_estimate: Number = np.mean(upper_quantiles)
    return lower_quantile_point_estimate, upper_quantile_point_estimate


def _compute_bootstrap_quantiles_point_estimate_custom_bias_corrected_method(
    metric_values: np.ndarray,
    false_positive_rate: np.float64,
    n_resamples: int,
    random_seed: Optional[int] = None,
) -> Tuple[Number, Number]:
    """
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

    sample_lower_quantile: np.ndarray = np.quantile(metric_values, q=lower_quantile_pct)
    sample_upper_quantile: np.ndarray = np.quantile(metric_values, q=upper_quantile_pct)

    if random_seed:
        random_state: np.random.Generator = np.random.Generator(
            np.random.PCG64(random_seed)
        )
        bootstraps: np.ndarray = random_state.choice(
            metric_values, size=(n_resamples, metric_values.size)
        )
    else:
        bootstraps: np.ndarray = np.random.choice(
            metric_values, size=(n_resamples, metric_values.size)
        )

    bootstrap_lower_quantiles: Union[np.ndarray, Number] = np.quantile(
        bootstraps,
        q=lower_quantile_pct,
        axis=1,
    )
    bootstrap_lower_quantile_point_estimate: float = np.mean(bootstrap_lower_quantiles)
    bootstrap_lower_quantile_standard_error: float = np.std(bootstrap_lower_quantiles)
    bootstrap_lower_quantile_bias: float = (
        bootstrap_lower_quantile_point_estimate - sample_lower_quantile
    )

    # Bias / Standard Error > 0.25 is a rule of thumb for when to apply bias correction.
    # See:
    # Efron, B., & Tibshirani, R. J. (1993). Estimates of bias. An Introduction to the Bootstrap (pp. 128).
    #         Springer Science and Business Media Dordrecht. DOI 10.1007/978-1-4899-4541-9
    lower_quantile_bias_corrected_point_estimate: Number
    if bootstrap_lower_quantile_bias / bootstrap_lower_quantile_standard_error <= 0.25:
        lower_quantile_bias_corrected_point_estimate = (
            bootstrap_lower_quantile_point_estimate
        )
    else:
        lower_quantile_bias_corrected_point_estimate = (
            bootstrap_lower_quantile_point_estimate - bootstrap_lower_quantile_bias
        )

    bootstrap_upper_quantiles: Union[np.ndarray, Number] = np.quantile(
        bootstraps,
        q=upper_quantile_pct,
        axis=1,
    )
    bootstrap_upper_quantile_point_estimate: np.ndarray = np.mean(
        bootstrap_upper_quantiles
    )
    bootstrap_upper_quantile_standard_error: np.ndarray = np.std(
        bootstrap_upper_quantiles
    )
    bootstrap_upper_quantile_bias: float = (
        bootstrap_upper_quantile_point_estimate - sample_upper_quantile
    )

    # Bias / Standard Error > 0.25 is a rule of thumb for when to apply bias correction.
    # See:
    # Efron, B., & Tibshirani, R. J. (1993). Estimates of bias. An Introduction to the Bootstrap (pp. 128).
    #         Springer Science and Business Media Dordrecht. DOI 10.1007/978-1-4899-4541-9
    upper_quantile_bias_corrected_point_estimate: Number
    if bootstrap_upper_quantile_bias / bootstrap_upper_quantile_standard_error <= 0.25:
        upper_quantile_bias_corrected_point_estimate = (
            bootstrap_upper_quantile_point_estimate
        )
    else:
        upper_quantile_bias_corrected_point_estimate = (
            bootstrap_upper_quantile_point_estimate - bootstrap_upper_quantile_bias
        )

    return (
        lower_quantile_bias_corrected_point_estimate,
        upper_quantile_bias_corrected_point_estimate,
    )


def _compute_bootstrap_quantiles_point_estimate_scipy_confidence_interval_midpoint_method(
    metric_values: np.ndarray,
    false_positive_rate: np.float64,
    n_resamples: int,
    method: Optional[str] = "BCa",
    random_seed: Optional[int] = None,
):
    """
    SciPy implementation of the BCa confidence interval for the population quantile. Unfortunately, as of
    March 4th, 2022, this implementation has two issues:
        1) it only returns a confidence interval and not a point estimate for the population parameter of interest,
           which is what we require for our use cases (the attempt below tries to "back out" the statistic from the
           confidece interval by taking the midpoint of the interval).
        2) It can not handle multi-dimensional statistics and correct for bias simultaneously. You must either use
           one feature or the other.

    This implementation could only be used if Great Expectations drops support for Python 3.6, thereby enabling us
    to use a more up-to-date version of the "scipy" Python package (the currently used version does not have
    "bootstrap"). Also, as discussed above, two contributions would need to be made to the SciPy package to enable
    1) bias correction for multi-dimensional statistics and 2) a return value of a point estimate for the population
    parameter of interest (lower and upper quantiles in this case).
    """
    bootstraps: tuple = (metric_values,)  # bootstrap samples must be in a sequence

    if random_seed:
        random_state = np.random.Generator(np.random.PCG64(random_seed))
    else:
        random_state = None

    lower_quantile_bootstrap_result: stats._bootstrap.BootstrapResult = stats.bootstrap(
        bootstraps,
        lambda data: np.quantile(
            data,
            q=false_positive_rate / 2,
        ),
        vectorized=False,
        confidence_level=1.0 - false_positive_rate,
        n_resamples=n_resamples,
        method=method,
        random_state=random_state,
    )
    upper_quantile_bootstrap_result: stats._bootstrap.BootstrapResult = stats.bootstrap(
        bootstraps,
        lambda data: np.quantile(
            data,
            q=1.0 - (false_positive_rate / 2),
        ),
        vectorized=False,
        confidence_level=1.0 - false_positive_rate,
        n_resamples=n_resamples,
        method=method,
        random_state=random_state,
    )

    # The idea that we can take the midpoint of the confidence interval is based on the fact that we think the
    # confidence interval was built from a symmetrical distribution. We think the distribution is normal due to
    # the implications of the Central Limit Theorem (CLT) (https://en.wikipedia.org/wiki/Central_limit_theorem) on
    # the bootstrap samples.
    # Unfortunately, the assumption that the CLT applies, does not hold in all cases. The bias-corrected and accelerated
    # (BCa) confidence interval computed using scipy.stats.bootstrap attempts to compute the "acceleration" as a
    # correction, because the standard normal approximation (CLT) assumes that the standard error of the bootstrap
    # quantiles (theta-hat) is the same for all parameters (theta). The acceleration (which is the rate of change of
    # the standard error of the quantile point estimate) is not a perfect correction, and therefore the assumption that
    # this interval is built from a normal distribution does not always hold.
    # See:
    #
    # Efron, B., & Tibshirani, R. J. (1993). The BCa method. An Introduction to the Bootstrap (pp. 184-188).
    #     Springer Science and Business Media Dordrecht. DOI 10.1007/978-1-4899-4541-9
    #
    # For an in-depth look at how the BCa interval is constructed and you will find the points made above on page 186.
    lower_quantile_confidence_interval: stats._bootstrap.BootstrapResult.ConfidenceInterval = (
        lower_quantile_bootstrap_result.confidence_interval
    )
    lower_quantile_point_estimate: np.float64 = np.mean(
        [
            lower_quantile_confidence_interval.low,
            lower_quantile_confidence_interval.high,
        ]
    )
    upper_quantile_confidence_interal: stats._bootstrap.BootstrapResult.ConfidenceInterval = (
        upper_quantile_bootstrap_result.confidence_interval
    )
    upper_quantile_point_estimate: np.float64 = np.mean(
        [upper_quantile_confidence_interal.low, upper_quantile_confidence_interal.high]
    )

    return lower_quantile_point_estimate, upper_quantile_point_estimate
