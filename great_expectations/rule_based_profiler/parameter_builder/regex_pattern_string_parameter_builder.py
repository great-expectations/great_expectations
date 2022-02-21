import logging
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, Union

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
    # source for UUID: https://stackoverflow.com/questions/7905929/how-to-test-valid-uuid-guid/13653180#13653180
    CANDIDATE_REGEX: Set[str] = {
        r"/\d+/",  # whole number with 1 or more digits
        r"/-?\d+/",  # negative whole numbers
        r"/-?\d+(\.\d*)?/",  # decimal numbers with . (period) separator
        r"/[A-Za-z0-9\.,;:!?()\"'%\-]+/",  # general text
        r"^\s+/",  # leading space
        r"\s+/$",  # trailing space
        r"/https?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{2,256}\.[a-z]{2,6}\b([-a-zA-Z0-9@:%_\+.~#()?&//=]*)/",  #  Matching URL (including http(s) protocol)
        r"/<\/?(?:p|a|b|img)(?: \/)?>/",  # HTML tags
        r"/(?:25[0-5]|2[0-4]\d|[01]\d{2}|\d{1,2})(?:.(?:25[0-5]|2[0-4]\d|[01]\d{2}|\d{1,2})){3}/",  # IPv4 IP address
        r"/(?:[A-Fa-f0-9]){0,4}(?: ?:? ?(?:[A-Fa-f0-9]){0,4}){0,7}/",  # IPv6 IP address,
        r"\b[0-9a-fA-F]{8}\b-[0-9a-fA-F]{4}-[0-5][0-9a-fA-F]{3}-[089ab][0-9a-fA-F]{3}-\b[0-9a-fA-F]{12}\b ",  # UUID
    }

    def __init__(
        self,
        name: str,
        metric_domain_kwargs: Optional[Union[str, dict]] = None,
        metric_value_kwargs: Optional[Union[str, dict]] = None,
        threshold: Union[float, str] = 1.0,
        candidate_regexes: Optional[Union[Iterable[str], str]] = None,
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

        self._candidate_regexes = candidate_regexes

    @property
    def metric_domain_kwargs(self) -> Optional[Union[str, dict]]:
        return self._metric_domain_kwargs

    @property
    def metric_value_kwargs(self) -> Optional[Union[str, dict]]:
        return self._metric_value_kwargs

    @property
    def threshold(self) -> Union[str, float]:
        return self._threshold

    @property
    def candidate_regexes(
        self,
    ) -> Union[
        str,
        Union[
            Set[str], List[str], "RegexPatternStringParameterBuilder.CANDIDATE_REGEX"
        ],
    ]:  # noqa: F821
        return self._candidate_regexes

    def _build_parameters(
        self,
        parameter_container: ParameterContainer,
        domain: Domain,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
    ) -> ParameterContainer:
        """
        Check the percentage of values matching the REGEX string, and return the best fit, or None if no
        string exceeds the configured threshold.

        :return: ParameterContainer object that holds ParameterNode objects with attribute name-value pairs and optional details
        """
        metric_computation_result: MetricComputationResult

        metric_values: np.ndarray

        metric_computation_result: MetricComputationResult = self.get_metrics(
            metric_name="column_values.nonnull.count",
            metric_domain_kwargs=self.metric_domain_kwargs,
            metric_value_kwargs=self.metric_value_kwargs,
            domain=domain,
            variables=variables,
            parameters=parameters,
        )
        metric_values = metric_computation_result.metric_values
        # Now obtain 1-dimensional vector of values of computed metric (each element corresponds to a Batch ID).
        metric_values = metric_values[:, 0]

        nonnull_count: int = sum(metric_values)

        regex_string_success_ratios: dict = {}

        # Obtain candidate_regexes from "rule state" (i.e, variables and parameters); from instance variable otherwise.
        candidate_regexes: Union[
            Set[str],
            List[str],
            "RegexPatternStringParameterBuilder.CANDIDATE_REGEX",  # noqa: F821
        ] = get_parameter_value_and_validate_return_type(
            domain=domain,
            parameter_reference=self.candidate_regexes,
            expected_return_type=None,
            variables=variables,
            parameters=parameters,
        )
        if candidate_regexes is not None and isinstance(candidate_regexes, list):
            candidate_regexes = set(candidate_regexes)
        else:
            candidate_regexes = RegexPatternStringParameterBuilder.CANDIDATE_REGEX

        regex_string: str
        match_regex_metric_value_kwargs: dict
        for regex_string in candidate_regexes:
            if self.metric_value_kwargs:
                match_regex_metric_value_kwargs: dict = {
                    **self._metric_value_kwargs,
                    **{"regex": regex_string},
                }
            else:
                match_regex_metric_value_kwargs: dict = {"regex": regex_string}

            metric_computation_result: MetricComputationResult = self.get_metrics(
                metric_name="column_values.match_regex.unexpected_count",
                metric_domain_kwargs=self.metric_domain_kwargs,
                metric_value_kwargs=match_regex_metric_value_kwargs,
                domain=domain,
                variables=variables,
                parameters=parameters,
            )
            metric_values = metric_computation_result.metric_values
            # Now obtain 1-dimensional vector of values of computed metric (each element corresponds to a Batch ID).

            metric_values = metric_values[:, 0]
            match_regex_unexpected_count: int = sum(metric_values)
            success_ratio: float = (
                nonnull_count - match_regex_unexpected_count
            ) / nonnull_count
            regex_string_success_ratios[regex_string] = success_ratio

        # Obtain threshold from "rule state" (i.e., variables and parameters); from instance variable otherwise.
        threshold: float = get_parameter_value_and_validate_return_type(
            domain=domain,
            parameter_reference=self._threshold,
            expected_return_type=float,
            variables=variables,
            parameters=parameters,
        )

        # get list of regex_strings that match greater than threshold
        regex_string_success_list: List[
            str
        ] = self._get_regex_matched_greater_than_threshold(
            regex_string_success_ratios, threshold
        )
        # sorted regex and ratios for all evaluated candidates
        sorted_ratio_list, sorted_regex_string_list = self._get_sorted_regex_and_ratios(
            regex_string_success_ratios
        )

        parameter_values: Dict[str, Any] = {
            f"$parameter.{self.name}": {
                "value": regex_string_success_list,
                "details": {
                    "evaluated_regexes": dict(
                        zip(sorted_regex_string_list, sorted_ratio_list)
                    ),
                    "threshold": threshold,
                },
            },
        }
        build_parameter_container(
            parameter_container=parameter_container, parameter_values=parameter_values
        )
        return parameter_container

    def _get_regex_matched_greater_than_threshold(
        self, regex_string_success_ratio_dict: dict, threshold: float
    ) -> List[str]:
        """
        Helper method to calculate which regex_strings match greater than threshold
        """
        regex_string_success_list: List[str] = []
        for regex_string, ratio in regex_string_success_ratio_dict.items():
            if ratio >= threshold:
                regex_string_success_list.append(regex_string)
        return regex_string_success_list

    def _get_sorted_regex_and_ratios(
        self, regex_string_success_ratio_dict: dict
    ) -> Tuple[List[float], List[str]]:
        """
        Helper method to sort all regexes that were evaluated by their success ratio. Returns Tuple(ratio, sorted_strings)
        """
        regex_string = regex_string_success_ratio_dict.keys()
        ratio = list(regex_string_success_ratio_dict.values())
        sorted_regex_strings = [
            i for _, i in sorted(zip(ratio, regex_string), reverse=True)
        ]
        ratio.sort(reverse=True)
        return ratio, sorted_regex_strings
