import copy
import uuid
from numbers import Number
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.batch import Batch, BatchRequest, RuntimeBatchRequest
from great_expectations.rule_based_profiler.types import (
    Domain,
    ParameterContainer,
    get_parameter_value_by_fully_qualified_parameter_name,
)
from great_expectations.validator.validator import Validator

NP_EPSILON: Union[Number, np.float64] = np.finfo(float).eps


def get_validator(
    purpose: str,
    *,
    data_context: Optional["DataContext"] = None,  # noqa: F821
    batch_request: Optional[Union[BatchRequest, RuntimeBatchRequest, dict, str]] = None,
    domain: Optional[Domain] = None,
    variables: Optional[ParameterContainer] = None,
    parameters: Optional[Dict[str, ParameterContainer]] = None,
) -> Optional[Validator]:
    if batch_request is None:
        return None

    batch_request = build_batch_request(
        domain=domain,
        batch_request=batch_request,
        variables=variables,
        parameters=parameters,
    )

    expectation_suite_name: str = f"tmp.{purpose}"
    if domain is None:
        expectation_suite_name = (
            f"{expectation_suite_name}_suite_{str(uuid.uuid4())[:8]}"
        )
    else:
        expectation_suite_name = (
            f"{expectation_suite_name}_{domain.id}_suite_{str(uuid.uuid4())[:8]}"
        )

    return data_context.get_validator(
        batch_request=batch_request,
        create_expectation_suite_with_name=expectation_suite_name,
    )


def get_batch_ids(
    data_context: Optional["DataContext"] = None,  # noqa: F821
    batch_request: Optional[Union[BatchRequest, RuntimeBatchRequest, dict, str]] = None,
    domain: Optional[Domain] = None,
    variables: Optional[ParameterContainer] = None,
    parameters: Optional[Dict[str, ParameterContainer]] = None,
) -> Optional[List[str]]:
    if batch_request is None:
        return None

    batch_request = build_batch_request(
        domain=domain,
        batch_request=batch_request,
        variables=variables,
        parameters=parameters,
    )

    batch_list: List[Batch] = data_context.get_batch_list(batch_request=batch_request)

    batch: Batch
    batch_ids: List[str] = [batch.id for batch in batch_list]

    return batch_ids


def build_batch_request(
    batch_request: Optional[Union[BatchRequest, RuntimeBatchRequest, dict, str]] = None,
    domain: Optional[Domain] = None,
    variables: Optional[ParameterContainer] = None,
    parameters: Optional[Dict[str, ParameterContainer]] = None,
) -> Optional[BatchRequest]:
    if batch_request is None:
        return None

    # Obtain BatchRequest from "rule state" (i.e., variables and parameters); from instance variable otherwise.
    materialized_batch_request: Optional[
        Union[BatchRequest, dict]
    ] = get_parameter_value_and_validate_return_type(
        domain=domain,
        parameter_reference=batch_request,
        expected_return_type=dict,
        variables=variables,
        parameters=parameters,
    )
    materialized_batch_request = BatchRequest(**materialized_batch_request)

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


def compute_bootstrap_quantiles(
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
    lower_quantile = np.mean(
        np.quantile(
            bootstraps,
            q=false_positive_rate / 2,
            axis=1,
            interpolation="linear",  # can be omitted ("linear" is default)
        )
    )
    upper_quantile = np.mean(
        np.quantile(
            bootstraps,
            q=1.0 - (false_positive_rate / 2),
            axis=1,
            interpolation="linear",  # can be omitted ("linear" is default)
        )
    )
    return lower_quantile, upper_quantile
