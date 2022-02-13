import logging
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, Union

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


class RegexPatternStringParameterBuilder(ParameterBuilder):
    """
    Detects the domain REGEX from a set of candidate REGEX strings by computing the
    column_values.match_regex_format.unexpected_count metric for each candidate format and returning the format that
    has the lowest unexpected_count ratio.
    """

    CANDIDATE_STRINGS: Set[str] = {
        r"^\d{1}$",
        r"^\d{2}$",
        r"^\S{8}-\S{4}-\S{4}-\S{4}-\S{12}$",
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
        Configure this RegexPatternStringParameterBuilder
        Args:
            name: the name of this parameter -- this is user-specified parameter name (from configuration);
            it is not the fully-qualified parameter name; a fully-qualified parameter name must start with "$parameter."
            and may contain one or more subsequent parts (e.g., "$parameter.<my_param_from_config>.<metric_name>").
            threshold: the ratio of values that must match a format string for it to be accepted
            candidate_strings: a list of candidate date format strings that will REPLACE the default
            additional_candidate_strings: a list of candidate date format strings that will SUPPLEMENT the default
            data_context: DataContext
            batch_request: specified in ParameterBuilder configuration to get Batch objects for parameter computation.
        """

        super().__init__(
            name=name,
            data_context=data_context,
            batch_request=batch_request,
        )
        print(metric_value_kwargs)
        print("metric value kwargs")

        print(metric_domain_kwargs)
        print("metric metric_domain_kwargs kwargs")
        self._metric_domain_kwargs = metric_domain_kwargs
        self._metric_value_kwargs = metric_value_kwargs

        self._threshold = threshold
        if candidate_strings is not None:
            self._candidate_strings = set(candidate_strings)
        else:
            self._candidate_strings = (
                RegexPatternStringParameterBuilder.CANDIDATE_STRINGS
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
        Check the percentage of values matching the REGEX string, and return the best fit, or None if no
        string exceeds the configured threshold.

        :return: ParameterContainer object that holds ParameterNode objects with attribute name-value pairs and optional details
        """
        validator: Validator = self.get_validator(
            domain=domain, variables=variables, parameters=parameters
        )

        batch_ids: Optional[List[str]] = self.get_batch_ids(
            domain=domain, variables=variables, parameters=parameters
        )
        if not batch_ids:
            raise ge_exceptions.ProfilerExecutionError(
                message=f"Utilizing a {self.__class__.__name__} requires a non-empty list of batch identifiers."
            )

        nonnull_metrics: MetricComputationResult = self.get_metrics(
            batch_ids=batch_ids,
            validator=validator,
            metric_name="column_values.nonnull.count",
            metric_domain_kwargs=self._metric_domain_kwargs,
            domain=domain,
            variables=variables,
            parameters=parameters,
        )
        nonnull_count: int = sum(nonnull_metrics.metric_values)

        regex_string_success_ratios: dict = {}
        for regex_string in self._candidate_strings:
            if self._metric_value_kwargs:
                match_regex_metric_value_kwargs: Tuple[dict] = (
                    {
                        **self._metric_value_kwargs,
                        **{"regex": regex_string},
                    },
                )

            else:
                match_regex_metric_value_kwargs: dict = {"regex": regex_string}
            try:
                match_regex_metrics: MetricComputationResult = self.get_metrics(
                    batch_ids=batch_ids,
                    validator=validator,
                    metric_name="column_values.match_regex.unexpected_count",
                    metric_domain_kwargs=self._metric_domain_kwargs,
                    metric_value_kwargs=match_regex_metric_value_kwargs,
                    domain=domain,
                    variables=variables,
                    parameters=parameters,
                )
            except KeyError as e:
                raise ge_exceptions.ProfilerConfigurationError(
                    "Unable to find configured Metric %s" % str(e)
                )
            match_regex_unexpected_count: int = sum(match_regex_metrics.metric_values)
            regex_string_success_ratios[regex_string] = (
                nonnull_count - match_regex_unexpected_count
            ) / nonnull_count

        print(regex_string_success_ratios)
        print("~~~~~~~~~~~~")
        best_regex_string: Optional[str] = None
        best_ratio: int = 0
        for regex_string, ratio in regex_string_success_ratios.items():
            if ratio > best_ratio and ratio >= self._threshold:
                best_regex_string = regex_string
                best_ratio = ratio
        parameter_values: Dict[str, Any] = {
            f"$parameter.{self.name}": {
                "value": best_regex_string,
                "details": {"success_ratio": best_ratio},
            },
        }
        print(parameter_values)
        print("parameter-values")

        build_parameter_container(
            parameter_container=parameter_container, parameter_values=parameter_values
        )
