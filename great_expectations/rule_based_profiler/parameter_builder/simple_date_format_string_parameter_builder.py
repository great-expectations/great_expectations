from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Dict, Iterable, List, Optional, Set, Union

import great_expectations.exceptions as gx_exceptions
from great_expectations.core.domain import Domain  # noqa: TCH001
from great_expectations.core.metric_function_types import (
    SummarizationMetricNameSuffixes,
)
from great_expectations.rule_based_profiler.attributed_resolved_metrics import (
    AttributedResolvedMetrics,  # noqa: TCH001
)
from great_expectations.rule_based_profiler.config import (
    ParameterBuilderConfig,  # noqa: TCH001
)
from great_expectations.rule_based_profiler.helpers.util import (
    NP_EPSILON,
    get_parameter_value_and_validate_return_type,
)
from great_expectations.rule_based_profiler.metric_computation_result import (
    MetricComputationResult,  # noqa: TCH001
    MetricValues,  # noqa: TCH001
)
from great_expectations.rule_based_profiler.parameter_builder import ParameterBuilder
from great_expectations.rule_based_profiler.parameter_container import (
    FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY,
    FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY,
    ParameterContainer,
)
from great_expectations.types.attributes import Attributes

if TYPE_CHECKING:
    from great_expectations.data_context.data_context.abstract_data_context import (
        AbstractDataContext,
    )


logger = logging.getLogger(__name__)

DEFAULT_CANDIDATE_STRINGS: Set[str] = {
    "%H:%M:%S",
    "%H:%M:%S,%f",
    "%H:%M:%S.%f",
    "%Y %b %d %H:%M:%S.%f",
    "%Y %b %d %H:%M:%S.%f %Z",
    "%Y %b %d %H:%M:%S.%f*%Z",
    "%Y%m%d %H:%M:%S.%f",
    "%Y-%m-%d",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M:%S %z",
    "%Y-%m-%d %H:%M:%S%z",
    "%Y-%m-%d %H:%M:%S,%f",
    "%Y-%m-%d %H:%M:%S,%f%z",
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S.%f%z",
    "%Y-%m-%d'T'%H:%M:%S",
    "%Y-%m-%d'T'%H:%M:%S%z",
    "%Y-%m-%d'T'%H:%M:%S'%z'",
    "%Y-%m-%d'T'%H:%M:%S.%f",
    "%Y-%m-%d'T'%H:%M:%S.%f'%z'",
    "%Y-%m-%d*%H:%M:%S",
    "%Y-%m-%d*%H:%M:%S:%f",
    "%Y-%m-%dT%z",
    "%Y/%m/%d",
    "%Y/%m/%d*%H:%M:%S",
    "%b %d %H:%M:%S",
    "%b %d %H:%M:%S %Y",
    "%b %d %H:%M:%S %z",
    "%b %d %H:%M:%S %z %Y",
    "%b %d %Y %H:%M:%S",
    "%b %d, %Y %H:%M:%S %p",
    "%d %b %Y %H:%M:%S",
    "%d %b %Y %H:%M:%S*%f",
    "%d-%b-%Y %H:%M:%S",
    "%d-%b-%Y %H:%M:%S.%f",
    "%d-%m-%Y",
    "%d/%b %H:%M:%S,%f",
    "%d/%b/%Y %H:%M:%S",
    "%d/%b/%Y:%H:%M:%S",
    "%d/%b/%Y:%H:%M:%S %z",
    "%d/%m/%Y",
    "%m%d_%H:%M:%S",
    "%m%d_%H:%M:%S.%f",
    "%m-%d-%Y",
    "%m/%d/%Y",
    "%m/%d/%Y %H:%M:%S %p",
    "%m/%d/%Y %H:%M:%S %p:%f",
    "%m/%d/%Y %H:%M:%S %z",
    "%m/%d/%Y*%H:%M:%S",
    "%m/%d/%Y*%H:%M:%S*%f",
    "%m/%d/%y %H:%M:%S %z",
    "%m/%d/%y*%H:%M:%S",
    "%y%m%d %H:%M:%S",
    "%y-%m-%d",
    "%y-%m-%d %H:%M:%S",
    "%y-%m-%d %H:%M:%S,%f",
    "%y-%m-%d %H:%M:%S,%f %z",
    "%y/%m/%d",
    "%y/%m/%d %H:%M:%S",
}


class SimpleDateFormatStringParameterBuilder(ParameterBuilder):
    """
    Detects the domain date format from a set of candidate date format strings by computing the
    column_values.match_strftime_format.unexpected_count metric for each candidate format and returning the format that
    has the lowest unexpected_count ratio.
    """

    def __init__(  # noqa: PLR0913
        self,
        name: str,
        metric_domain_kwargs: Optional[Union[str, dict]] = None,
        metric_value_kwargs: Optional[Union[str, dict]] = None,
        threshold: Union[str, float] = 1.0,
        candidate_strings: Optional[Union[Iterable[str], str]] = None,
        evaluation_parameter_builder_configs: Optional[
            List[ParameterBuilderConfig]
        ] = None,
        data_context: Optional[AbstractDataContext] = None,
    ) -> None:
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
            evaluation_parameter_builder_configs: ParameterBuilder configurations, executing and making whose respective
            ParameterBuilder objects' outputs available (as fully-qualified parameter names) is pre-requisite.
            These "ParameterBuilder" configurations help build parameters needed for this "ParameterBuilder".
            data_context: AbstractDataContext associated with this ParameterBuilder
        """
        super().__init__(
            name=name,
            evaluation_parameter_builder_configs=evaluation_parameter_builder_configs,
            data_context=data_context,
        )

        self._metric_domain_kwargs = metric_domain_kwargs
        self._metric_value_kwargs = metric_value_kwargs

        self._threshold = threshold

        if candidate_strings is not None and isinstance(candidate_strings, list):
            self._candidate_strings = set(candidate_strings)
        else:
            self._candidate_strings = DEFAULT_CANDIDATE_STRINGS

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
    def candidate_strings(
        self,
    ) -> Union[str, Union[List[str], Set[str]]]:
        return self._candidate_strings

    def _build_parameters(
        self,
        domain: Domain,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
        runtime_configuration: Optional[dict] = None,
    ) -> Attributes:
        """
        Builds ParameterContainer object that holds ParameterNode objects with attribute name-value pairs and details.

        Check the percentage of values matching each string, and return the best fit, or None if no string exceeds the
        configured threshold.

        Returns:
            Attributes object, containing computed parameter values and parameter computation details metadata.
        """
        metric_computation_result: MetricComputationResult

        metric_computation_result = self.get_metrics(
            metric_name="column_values.nonnull.count",
            metric_domain_kwargs=self.metric_domain_kwargs,
            metric_value_kwargs=self.metric_value_kwargs,
            limit=None,
            enforce_numeric_metric=False,
            replace_nan_with_zero=False,
            runtime_configuration=runtime_configuration,
            domain=domain,
            variables=variables,
            parameters=parameters,
        )

        # This should never happen.
        if len(metric_computation_result.attributed_resolved_metrics) != 1:
            raise gx_exceptions.ProfilerExecutionError(
                message=f'Result of metric computations for {self.__class__.__name__} must be a list with exactly 1 element of type "AttributedResolvedMetrics" ({metric_computation_result.attributed_resolved_metrics} found).'
            )

        attributed_resolved_metrics: AttributedResolvedMetrics

        attributed_resolved_metrics = (
            metric_computation_result.attributed_resolved_metrics[0]
        )

        metric_values: MetricValues

        metric_values = attributed_resolved_metrics.conditioned_metric_values

        if metric_values is None:
            raise gx_exceptions.ProfilerExecutionError(
                message=f"Result of metric computations for {self.__class__.__name__} is empty."
            )

        # Now obtain 1-dimensional vector of values of computed metric (each element corresponds to a Batch ID).
        metric_values = metric_values[:, 0]

        nonnull_count: int = sum(metric_values)

        # Obtain candidate_strings from "rule state" (i.e., variables and parameters); from instance variable otherwise.
        candidate_strings: Union[
            List[str],
            Set[str],
        ] = get_parameter_value_and_validate_return_type(
            domain=domain,
            parameter_reference=self.candidate_strings,
            expected_return_type=None,
            variables=variables,
            parameters=parameters,
        )

        # Gather "metric_value_kwargs" for all candidate "strftime_format" strings.
        format_string: str
        match_strftime_metric_value_kwargs_list: List[dict] = []
        match_strftime_metric_value_kwargs: dict
        for format_string in candidate_strings:
            if self.metric_value_kwargs:
                match_strftime_metric_value_kwargs = {
                    **self.metric_value_kwargs,
                    **{"strftime_format": format_string},
                }
            else:
                match_strftime_metric_value_kwargs = {
                    "strftime_format": format_string,
                }

            match_strftime_metric_value_kwargs_list.append(
                match_strftime_metric_value_kwargs
            )

        # Obtain resolved metrics and metadata for all metric configurations and available Batch objects simultaneously.
        metric_computation_result = self.get_metrics(
            metric_name=f"column_values.match_strftime_format.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}",
            metric_domain_kwargs=self.metric_domain_kwargs,
            metric_value_kwargs=match_strftime_metric_value_kwargs_list,
            limit=None,
            enforce_numeric_metric=False,
            replace_nan_with_zero=False,
            runtime_configuration=runtime_configuration,
            domain=domain,
            variables=variables,
            parameters=parameters,
        )

        format_string_success_ratios: dict = {}

        for (
            attributed_resolved_metrics
        ) in metric_computation_result.attributed_resolved_metrics:
            # Now obtain 1-dimensional vector of values of computed metric (each element corresponds to a Batch ID).
            metric_values = attributed_resolved_metrics.conditioned_metric_values[:, 0]

            match_strftime_unexpected_count: int = sum(metric_values)
            success_ratio: float = (nonnull_count - match_strftime_unexpected_count) / (
                nonnull_count + NP_EPSILON
            )
            format_string_success_ratios[
                attributed_resolved_metrics.metric_attributes["strftime_format"]
            ] = success_ratio

        # Obtain threshold from "rule state" (i.e., variables and parameters); from instance variable otherwise.
        threshold: float = get_parameter_value_and_validate_return_type(
            domain=domain,
            parameter_reference=self.threshold,
            expected_return_type=float,
            variables=variables,
            parameters=parameters,
        )

        # get best-matching datetime string that matches greater than threshold
        best_format_string: str
        best_ratio: float
        (
            best_format_string,
            best_ratio,
        ) = ParameterBuilder._get_best_candidate_above_threshold(
            format_string_success_ratios, threshold
        )
        # dict of sorted datetime and ratios for all evaluated candidates
        sorted_format_strings_and_ratios: dict = (
            ParameterBuilder._get_sorted_candidates_and_ratios(
                format_string_success_ratios
            )
        )

        return Attributes(
            {
                FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY: best_format_string,
                FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY: {
                    "success_ratio": best_ratio,
                    "candidate_strings": sorted_format_strings_and_ratios,
                },
            }
        )
