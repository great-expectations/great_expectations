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
from great_expectations.rule_based_profiler.util import (
    get_parameter_value_and_validate_return_type,
)
from great_expectations.validator.validator import Validator

logger = logging.getLogger(__name__)


class RegexPatternStringParameterBuilder(ParameterBuilder):
    """
    Detects the domain REGEX from a set of candidate REGEX strings by computing the
    column_values.match_regex_format.unexpected_count metric for each candidate format and returning the format that
    has the lowest unexpected_count ratio.
    """

    # list of candidate strings that are most commonly used
    # source: https://regexland.com/most-common-regular-expressions/
    CANDIDATE_REGEX: Set[str] = {
        r"/\d+/",  # whole number with 1 or more digits ExpectValuesToBeNumeric? (.. youw oudl want to emit that expectation)?
        r"/-?\d+/",  # negative whole numbers
        r"/-?\d+(\.\d*)?/",  # decimal numbers with . (period) separator
        r"/[A-Za-z0-9\.,;:!?()\"'%\-]+/",  # general text
        r"^ +/",  # leading space
        r" +/$",  # trailing space
        r"/https?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{2,256}\.[a-z]{2,6}\b([-a-zA-Z0-9@:%_\+.~#()?&//=]*)/",  #  Matching URL (including http(s) protocol)
        r"/<\/?(?:p|a|b|img)(?: \/)?>/",  # HTML tags
        r"/(?:25[0-5]|2[0-4]\d|[01]\d{2}|\d{1,2})(?:.(?:25[0-5]|2[0-4]\d|[01]\d{2}|\d{1,2})){3}/",  # IPv4 IP address
        r"/(?:[A-Fa-f0-9]){0,4}(?: ?:? ?(?:[A-Fa-f0-9]){0,4}){0,7}/",  # IPv6 IP address,
    }

    def __init__(
        self,
        name: str,
        candidate_regexes: Optional[Iterable[str]] = None,
        metric_domain_kwargs: Optional[Union[str, dict]] = None,
        metric_value_kwargs: Optional[Union[str, dict]] = None,
        threshold: float = 0.9,
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
            candidate_regexes: a list of candidate regex strings that will REPLACE the default
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

        if candidate_regexes is not None:
            self._candidate_regexes = set(candidate_regexes)
        else:
            self._candidate_regexes = RegexPatternStringParameterBuilder.CANDIDATE_REGEX

    def _build_parameters(
        self,
        parameter_container: ParameterContainer,
        domain: Domain,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
    ):
        """
        Check the percentage of values matching the REGEX string, and return the best fit, or None if no
        string exceeds the configured threshold.

        :return: ParameterContainer object that holds ParameterNode objects with attribute name-value pairs and optional details
        """
        validator: Union[Validator, None] = self.get_validator(
            domain=domain, variables=variables, parameters=parameters
        )
        if not validator:
            raise ge_exceptions.ProfilerExecutionError(
                message=f"{self.__class__.__name__} was not able to get Validator using domain, variables and parameters provided."
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

        regex_string: str
        match_regex_metric_value_kwargs: dict

        for regex_string in self._candidate_regexes:
            if self._metric_value_kwargs:
                match_regex_metric_value_kwargs = {
                    **self._metric_value_kwargs,
                    **{"regex": regex_string},
                }
            else:
                match_regex_metric_value_kwargs: dict = {"regex": regex_string}

            metric_computation_result: MetricComputationResult = self.get_metrics(
                batch_ids=batch_ids,
                validator=validator,
                metric_name="column_values.match_regex.unexpected_count",
                metric_domain_kwargs=self._metric_domain_kwargs,
                metric_value_kwargs=match_regex_metric_value_kwargs,
                domain=domain,
                variables=variables,
                parameters=parameters,
            )
            metric_values = metric_computation_result.metric_values
            # Now obtain 1-dimensional vector of values of computed metric (each element corresponds to a Batch ID).
            metric_values = metric_values[:, 0]

            match_regex_unexpected_count: int = sum(metric_values)
            regex_string_success_ratios[regex_string] = (
                nonnull_count - match_regex_unexpected_count
            ) / nonnull_count

        best_regex_string: Optional[str] = None
        best_ratio: float = 0.0

        threshold: float = get_parameter_value_and_validate_return_type(
            domain=domain,
            parameter_reference=self._threshold,
            expected_return_type=float,
            variables=variables,
            parameters=parameters,
        )
        regex_string: str
        ratio: float

        for regex_string, ratio in regex_string_success_ratios.items():
            if ratio > best_ratio and ratio >= threshold:
                best_regex_string = regex_string
                best_ratio = ratio
        parameter_values: Dict[str, Any] = {
            f"$parameter.{self.name}": {
                "value": best_regex_string,
                "details": {"success_ratio": best_ratio},
            },
        }
        build_parameter_container(
            parameter_container=parameter_container, parameter_values=parameter_values
        )
