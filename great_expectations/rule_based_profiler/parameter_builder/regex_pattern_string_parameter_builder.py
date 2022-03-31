import logging
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, Union

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.batch import Batch, BatchRequest, RuntimeBatchRequest
from great_expectations.rule_based_profiler.helpers.util import (
    get_parameter_value_and_validate_return_type,
)
from great_expectations.rule_based_profiler.parameter_builder import (
    AttributedResolvedMetrics,
    MetricComputationResult,
    MetricValues,
    ParameterBuilder,
)
from great_expectations.rule_based_profiler.types import (
    PARAMETER_KEY,
    Domain,
    ParameterContainer,
)

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
        threshold: Union[str, float] = 1.0,
        candidate_regexes: Optional[Union[str, Iterable[str]]] = None,
        evaluation_parameter_builder_configs: Optional[List[dict]] = None,
        json_serialize: Union[str, bool] = True,
        batch_list: Optional[List[Batch]] = None,
        batch_request: Optional[
            Union[str, BatchRequest, RuntimeBatchRequest, dict]
        ] = None,
        data_context: Optional["DataContext"] = None,  # noqa: F821
    ):
        """
        Configure this RegexPatternStringParameterBuilder
        Args:
            name: the name of this parameter -- this is user-specified parameter name (from configuration);
            it is not the fully-qualified parameter name; a fully-qualified parameter name must start with "$parameter."
            and may contain one or more subsequent parts (e.g., "$parameter.<my_param_from_config>.<metric_name>").
            threshold: the ratio of values that must match a format string for it to be accepted
            candidate_regexes: a list of candidate regex strings that will REPLACE the default
            evaluation_parameter_builder_configs: ParameterBuilder configurations, executing and making whose respective
            ParameterBuilder objects' outputs available (as fully-qualified parameter names) is pre-requisite.
            These "ParameterBuilder" configurations help build parameters needed for this "ParameterBuilder".
            json_serialize: If True (default), convert computed value to JSON prior to saving results.
            batch_list: Optional[List[Batch]] = None,
            batch_request: specified in ParameterBuilder configuration to get Batch objects for parameter computation.
            data_context: DataContext
        """
        super().__init__(
            name=name,
            evaluation_parameter_builder_configs=evaluation_parameter_builder_configs,
            json_serialize=json_serialize,
            batch_list=batch_list,
            batch_request=batch_request,
            data_context=data_context,
        )

        self._metric_domain_kwargs = metric_domain_kwargs
        self._metric_value_kwargs = metric_value_kwargs

        self._threshold = threshold

        self._candidate_regexes = candidate_regexes

    @property
    def fully_qualified_parameter_name(self) -> str:
        return f"{PARAMETER_KEY}{self.name}"

    """
    Full getter/setter accessors for needed properties are for configuring MetricMultiBatchParameterBuilder dynamically.
    """

    @property
    def metric_domain_kwargs(self) -> Optional[Union[str, dict]]:
        return self._metric_domain_kwargs

    @property
    def metric_value_kwargs(self) -> Optional[Union[str, dict]]:
        return self._metric_value_kwargs

    @metric_value_kwargs.setter
    def metric_value_kwargs(self, value: Optional[Union[str, dict]]) -> None:
        self._metric_value_kwargs = value

    @property
    def threshold(self) -> Union[str, float]:
        return self._threshold

    @property
    def candidate_regexes(
        self,
    ) -> Union[str, Union[List[str], Set[str]]]:
        return self._candidate_regexes

    def _build_parameters(
        self,
        parameter_container: ParameterContainer,
        domain: Domain,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
    ) -> Tuple[Any, dict]:
        """
        Check the percentage of values matching the REGEX string, and return the best fit, or None if no
        string exceeds the configured threshold.

        return: Tuple containing computed_parameter_value and parameter_computation_details metadata.
        """
        metric_computation_result: MetricComputationResult

        metric_computation_result = self.get_metrics(
            metric_name="column_values.nonnull.count",
            metric_domain_kwargs=self.metric_domain_kwargs,
            metric_value_kwargs=self.metric_value_kwargs,
            domain=domain,
            variables=variables,
            parameters=parameters,
        )

        # This should never happen.
        if not (
            isinstance(metric_computation_result.metric_values, list)
            and len(metric_computation_result.metric_values) == 1
        ):
            raise ge_exceptions.ProfilerExecutionError(
                message=f'Result of metric computations for {self.__class__.__name__} must be a list with exactly 1 element of type "AttributedResolvedMetrics" ({metric_computation_result.metric_values} found).'
            )

        attributed_resolved_metrics: AttributedResolvedMetrics

        attributed_resolved_metrics = metric_computation_result.metric_values[0]

        metric_values: MetricValues

        metric_values = attributed_resolved_metrics.metric_values

        # Now obtain 1-dimensional vector of values of computed metric (each element corresponds to a Batch ID).
        metric_values = metric_values[:, 0]

        nonnull_count: int = sum(metric_values)

        # Obtain candidate_regexes from "rule state" (i.e, variables and parameters); from instance variable otherwise.
        candidate_regexes: Union[
            List[str],
            Set[str],
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

        # Gather "metric_value_kwargs" for all candidate "regex" strings.
        regex_string: str
        match_regex_metric_value_kwargs_list: List[dict] = []
        match_regex_metric_value_kwargs: dict
        for regex_string in candidate_regexes:
            if self.metric_value_kwargs:
                match_regex_metric_value_kwargs: dict = {
                    **self._metric_value_kwargs,
                    **{"regex": regex_string},
                }
            else:
                match_regex_metric_value_kwargs = {
                    "regex": regex_string,
                }

            match_regex_metric_value_kwargs_list.append(match_regex_metric_value_kwargs)

        # Obtain resolved metrics and metadata for all metric configurations and available Batch objects simultaneously.
        metric_computation_result = self.get_metrics(
            metric_name="column_values.match_regex.unexpected_count",
            metric_domain_kwargs=self.metric_domain_kwargs,
            metric_value_kwargs=match_regex_metric_value_kwargs_list,
            domain=domain,
            variables=variables,
            parameters=parameters,
        )

        regex_string_success_ratios: dict = {}

        for attributed_resolved_metrics in metric_computation_result.metric_values:
            # Now obtain 1-dimensional vector of values of computed metric (each element corresponds to a Batch ID).
            metric_values = attributed_resolved_metrics.metric_values[:, 0]

            match_regex_unexpected_count: int = sum(metric_values)
            success_ratio: float = (
                nonnull_count - match_regex_unexpected_count
            ) / nonnull_count
            regex_string_success_ratios[
                attributed_resolved_metrics.metric_attributes["regex"]
            ] = success_ratio

        # Obtain threshold from "rule state" (i.e., variables and parameters); from instance variable otherwise.
        threshold: float = get_parameter_value_and_validate_return_type(
            domain=domain,
            parameter_reference=self._threshold,
            expected_return_type=float,
            variables=variables,
            parameters=parameters,
        )

        # get best-matching regex_string that match greater than threshold
        (
            best_regex_string,
            best_ratio,
        ) = ParameterBuilder._get_best_candidate_above_threshold(
            regex_string_success_ratios, threshold
        )
        # dict of sorted regex and ratios for all evaluated candidates
        sorted_regex_candidates_and_ratios: dict = (
            ParameterBuilder._get_sorted_candidates_and_ratios(
                regex_string_success_ratios
            )
        )

        return (
            best_regex_string,
            {
                "success_ratio": best_ratio,
                "evaluated_regexes": sorted_regex_candidates_and_ratios,
            },
        )
