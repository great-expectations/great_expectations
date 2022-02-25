import copy
import logging
import uuid
from numbers import Number
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
import scipy
import statsmodels

import great_expectations.exceptions as ge_exceptions
from great_expectations.core import ExpectationSuite
from great_expectations.core.batch import (
    Batch,
    BatchRequest,
    RuntimeBatchRequest,
    materialize_batch_request,
)
from great_expectations.rule_based_profiler.types import (
    Domain,
    ParameterContainer,
    get_parameter_value_by_fully_qualified_parameter_name,
)

logger = logging.getLogger(__name__)

NP_EPSILON: Union[Number, np.float64] = np.finfo(float).eps


def get_validator(
    purpose: str,
    *,
    data_context: Optional["DataContext"] = None,  # noqa: F821
    batch_list: Optional[List[Batch]] = None,
    batch_request: Optional[Union[BatchRequest, RuntimeBatchRequest, dict, str]] = None,
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
    batch_request: Optional[Union[BatchRequest, RuntimeBatchRequest, dict, str]] = None,
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
    batch_request: Optional[Union[BatchRequest, RuntimeBatchRequest, dict, str]] = None,
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
    elif isinstance(parameter_reference, str) and parameter_reference.startswith("$"):
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

    return parameter_reference


def compute_quantiles(
    metric_values: np.ndarray,
    false_positive_rate: np.float64,
) -> Tuple[Number, Number]:
    lower_quantile = np.quantile(
        metric_values,
        q=(false_positive_rate / 2),
        axis=0,
        interpolation="linear",  # can be omitted ("linear" is default)
    )
    upper_quantile = np.quantile(
        metric_values,
        q=1.0 - (false_positive_rate / 2),
        axis=0,
        interpolation="linear",  # can be omitted ("linear" is default)
    )
    return lower_quantile, upper_quantile


def compute_bootstrap_quantiles_mean(
    metric_values: np.ndarray,
    false_positive_rate: np.float64,
    n_resamples: int,
) -> Tuple[Number, Number]:
    """
    Internal implementation of the "bootstrap" estimator method, returning confidence interval for a distribution.
    See https://en.wikipedia.org/wiki/Bootstrapping_(statistics) for an introduction to "bootstrapping" in statistics.

    This implementation is sub-par compared to the one available from the "SciPy" standard library
    ("https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.bootstrap.html"), because it introduces bias and
    does not handle multi-dimensional statistics (unlike "scipy.stats.bootstrap", which corrects for bias and is
    vectorized, thus having the ability to accept a multi-dimensional statistic function and process all dimensions).

    This implementation will be replaced by "scipy.stats.bootstrap" when Great Expectations can be upgraded to use a
    more up-to-date version of the "scipy" Python package (the currently used version does not have "bootstrap").

    Additional future direction (potentially as a contribution submission to the "SciPy" community) include developing
    enhancements to bootstrapped estimator based on theory presented in "http://dido.econ.yale.edu/~dwka/pub/p1001.pdf":
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
    bootstraps: np.ndarray = np.random.choice(
        metric_values, size=(n_resamples, metric_values.size)
    )
    lower_quantiles: Union[np.nd_array, Number] = np.quantile(
        bootstraps,
        q=false_positive_rate / 2,
        axis=1,
        interpolation="linear",  # can be omitted ("linear" is default)
    )
    lower_quantile_mean: Number = np.mean(lower_quantiles)
    upper_quantiles: Union[np.nd_array, Number] = np.quantile(
        bootstraps,
        q=1.0 - (false_positive_rate / 2),
        axis=1,
        interpolation="linear",  # can be omitted ("linear" is default)
    )
    upper_quantile_mean: Number = np.mean(upper_quantiles)
    return lower_quantile_mean, upper_quantile_mean


def compute_bootstrap_quantiles(
    metric_values: np.ndarray,
    false_positive_rate: np.float64,
    n_resamples: int,
) -> Tuple[Number, Number]:
    """
    SciPy implementation of the "bootstrap" estimator method, returning confidence interval for a distribution.
    See https://en.wikipedia.org/wiki/Bootstrapping_(statistics) for an introduction to "bootstrapping" in statistics.
    """
    data: tuple[np.ndarray] = (metric_values,)
    lower_quantile_pct: float = false_positive_rate / 2
    upper_quantile_pct: float = 1.0 - (false_positive_rate / 2)
    axis: int = 1
    interpolation: str = "linear"

    # "method" parameter tells scipy.stats.bootstrap whether to return the ‘percentile’ bootstrap confidence interval
    # ('percentile'), the ‘reverse’ or the bias - corrected and accelerated bootstrap confidence interval ('BCa').
    # Note that even though we prefer to use 'BCa', only 'percentile' and 'basic' support multi-sample statistics as of
    # February 25th, 2022. For a more complete write-up regarding the available methods, see:
    # https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.bootstrap.html
    method: str = "basic"

    bootstrap_result: scipy.stats.bootstrap.BootstrapResult = scipy.stats.bootstrap(
        data=data,
        statistic=lambda data, axis=axis: np.quantile(
            a=data,
            q=[lower_quantile_pct, upper_quantile_pct],
            axis=axis,
            interpolation=interpolation,
        ),
        n_resamples=n_resamples,
        method=method,
    )
    lower_ci: Union[np.nd_array, Number]
    upper_ci: Union[np.nd_array, Number]
    lower_ci, upper_ci = bootstrap_result.confidence_interval

    # compute the mean of the bootstrapped quantile samples
    lower_quantile_mean, upper_quantile_mean = compute_bootstrap_quantiles_mean(
        metric_values=metric_values,
        false_positive_rate=false_positive_rate,
        n_resamples=n_resamples,
    )

    # Hypothesis test that the actual quantiles are equal to the computed lower and upper quantile means
    # The central limit theorem states that the distribution of the bootstrapped sample statistics
    # will be approximately normal
    alternative: str = "two-sided"  # alternative hypothesis is that they are not equal
    lower_quantile_tstat: Number
    lower_quantile_pvalue: float
    upper_quantile_tstat: Number
    lower_quantile_pvalue: float

    lower_quantile_tstat, lower_quantile_pvalue = statsmodels.stats.weightstats.ztest(
        x1=data, value=lower_quantile_mean, alternative=alternative
    )
    upper_quantile_tstat, upper_quantile_pvalue = statsmodels.stats.weightstats.ztest(
        x1=data, value=upper_quantile_mean, alternative=alternative
    )

    if lower_quantile_pvalue < 0.05:
        pass

    return lower_quantile_mean, upper_quantile_mean
