import logging
from typing import Any, Dict, Iterable, List, Optional, Set, Union

import numpy as np

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest
from great_expectations.rule_based_profiler.parameter_builder.parameter_builder import (
    MetricComputationResult,
    ParameterBuilder,
)
from great_expectations.rule_based_profiler.types import (
    Domain,
    ParameterContainer,
    build_parameter_container,
)
from great_expectations.validator.validator import Validator

logger = logging.getLogger(__name__)


class SimpleDateFormatStringParameterBuilder(ParameterBuilder):
    """
    Detects the domain date format from a set of candidate date format strings by computing the
    column_values.match_strftime_format.unexpected_count metric for each candidate format and returning the format that
    has the lowest unexpected_count ratio.
    """

    CANDIDATE_STRINGS: Set[str] = {
        "%Y-%m-%d",
        "%m-%d-%Y",
        "%y-%m-%d",
        "%Y-%m-%dT%z",
        "%Y-%m-%d %H:%M:%S",
    }

    def __init__(
        self,
        name: str,
        metric_domain_kwargs: Optional[Union[str, dict]] = None,
        metric_value_kwargs: Optional[Union[str, dict]] = None,
        threshold: float = 1.0,
        candidate_strings: Optional[Iterable[str]] = None,
        data_context: Optional["DataContext"] = None,
        batch_request: Optional[Union[BatchRequest, RuntimeBatchRequest, dict]] = None,
    ):
        """
        Configure this SimpleDateFormatStringParameterBuilder
        Args:
            name: the name of this parameter -- this is user-specified parameter name (from configuration);
            it is not the fully-qualified parameter name; a fully-qualified parameter name must start with "$parameter."
            and may contain one or more subsequent parts (e.g., "$parameter.<my_param_from_config>.<metric_name>").
            metric_domain_kwargs: used in MetricConfiguration
            metric_value_kwargs: used in MetricConfiguration
            threshold: the ratio of values that must match a format string for it to be accepted
            candidate_strings: a list of candidate date format strings that will replace the default
            data_context: DataContext
            batch_request: specified in ParameterBuilder configuration to get Batch objects for parameter computation.
        """
        super().__init__(
            name=name,
            data_context=data_context,
            batch_request=batch_request,
        )

        self._metric_domain_kwargs = metric_domain_kwargs
        self._metric_value_kwargs = metric_value_kwargs

        self._threshold = threshold
        if candidate_strings is not None:
            self._candidate_strings = set(candidate_strings)
        else:
            self._candidate_strings = (
                SimpleDateFormatStringParameterBuilder.CANDIDATE_STRINGS
            )

    def _build_parameters(
        self,
        parameter_container: ParameterContainer,
        domain: Domain,
        *,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
    ):
        """
        Check the percentage of values matching each string, and return the best fit, or None if no
        string exceeds the configured threshold.

        :return: ParameterContainer object that holds ParameterNode objects with attribute name-value pairs and
        ptional details
        """
        validator: Validator = self.get_validator(
            domain=domain,
            variables=variables,
            parameters=parameters,
        )

        batch_ids: Optional[List[str]] = self.get_batch_ids(
            domain=domain,
            variables=variables,
            parameters=parameters,
        )
        if not batch_ids:
            raise ge_exceptions.ProfilerExecutionError(
                message=f"Utilizing a {self.__class__.__name__} requires a non-empty list of batch identifiers."
            )

        metric_computation_result: MetricComputationResult

        metric_values: np.ndarray

        metric_computation_result = self.get_metrics(
            batch_ids=batch_ids,
            validator=validator,
            metric_name="column_values.nonnull.count",
            metric_domain_kwargs=self._metric_domain_kwargs,
            metric_value_kwargs=self._metric_value_kwargs,
            domain=domain,
            variables=variables,
            parameters=parameters,
        )
        metric_values = metric_computation_result.metric_values
        # Now obtain 1-dimensional vector of values of computed metric (each element corresponds to a Batch ID).
        metric_values = metric_values[:, 0]

        nonnull_count: int = sum(metric_values)

        format_string_success_ratios: dict = {}

        fmt_string: str
        match_strftime_metric_value_kwargs: dict
        for fmt_string in self._candidate_strings:
            if self._metric_value_kwargs:
                match_strftime_metric_value_kwargs = {
                    **self._metric_value_kwargs,
                    **{"strftime_format": fmt_string},
                }
            else:
                match_strftime_metric_value_kwargs = {
                    "strftime_format": fmt_string,
                }

            metric_computation_result = self.get_metrics(
                batch_ids=batch_ids,
                validator=validator,
                metric_name="column_values.match_strftime_format.unexpected_count",
                metric_domain_kwargs=self._metric_domain_kwargs,
                metric_value_kwargs=match_strftime_metric_value_kwargs,
                domain=domain,
                variables=variables,
                parameters=parameters,
            )
            metric_values = metric_computation_result.metric_values
            # Now obtain 1-dimensional vector of values of computed metric (each element corresponds to a Batch ID).
            metric_values = metric_values[:, 0]

            match_strftime_unexpected_count: int = sum(metric_values)
            format_string_success_ratios[fmt_string] = (
                nonnull_count - match_strftime_unexpected_count
            ) / nonnull_count

        best_fmt_string: Optional[str] = None
        best_ratio: int = 0
        for fmt_string, ratio in format_string_success_ratios.items():
            if ratio > best_ratio and ratio >= self._threshold:
                best_fmt_string = fmt_string
                best_ratio = ratio

        parameter_values: Dict[str, Any] = {
            f"$parameter.{self.name}": {
                "value": best_fmt_string,
                "details": {"success_ratio": best_ratio},
            },
        }

        build_parameter_container(
            parameter_container=parameter_container, parameter_values=parameter_values
        )
