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

    def __init__(
        self,
        name: str,
        metric_domain_kwargs: Optional[Union[str, dict]] = None,
        metric_value_kwargs: Optional[Union[str, dict]] = None,
        threshold: Union[str, float] = 1.0,
        candidate_strings: Optional[Union[Iterable[str], str]] = None,
        evaluation_parameter_builder_configs: Optional[List[dict]] = None,
        json_serialize: bool = True,
        batch_list: Optional[List[Batch]] = None,
        batch_request: Optional[
            Union[str, BatchRequest, RuntimeBatchRequest, dict]
        ] = None,
        data_context: Optional["DataContext"] = None,  # noqa: F821
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
            evaluation_parameter_builder_configs: ParameterBuilder configurations, executing and making whose respective
            ParameterBuilder objects' outputs available (as fully-qualified parameter names) is pre-requisite.
            json_serialize: If True (default), convert computed value to JSON prior to saving results.
            batch_list: explicitly passed Batch objects for parameter computation (take precedence over batch_request).
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

        if candidate_strings is not None and isinstance(candidate_strings, list):
            self._candidate_strings = set(candidate_strings)
        else:
            self._candidate_strings = DEFAULT_CANDIDATE_STRINGS

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
    def candidate_strings(
        self,
    ) -> Union[str, Union[List[str], Set[str]]]:
        return self._candidate_strings

    def _build_parameters(
        self,
        parameter_container: ParameterContainer,
        domain: Domain,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
    ) -> Tuple[Any, dict]:
        """
        Check the percentage of values matching each string, and return the best fit, or None if no
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
        fmt_string: str
        match_strftime_metric_value_kwargs_list: List[dict] = []
        match_strftime_metric_value_kwargs: dict
        for fmt_string in candidate_strings:
            if self.metric_value_kwargs:
                match_strftime_metric_value_kwargs = {
                    **self.metric_value_kwargs,
                    **{"strftime_format": fmt_string},
                }
            else:
                match_strftime_metric_value_kwargs = {
                    "strftime_format": fmt_string,
                }

            match_strftime_metric_value_kwargs_list.append(
                match_strftime_metric_value_kwargs
            )

        # Obtain resolved metrics and metadata for all metric configurations and available Batch objects simultaneously.
        metric_computation_result = self.get_metrics(
            metric_name="column_values.match_strftime_format.unexpected_count",
            metric_domain_kwargs=self.metric_domain_kwargs,
            metric_value_kwargs=match_strftime_metric_value_kwargs_list,
            domain=domain,
            variables=variables,
            parameters=parameters,
        )

        format_string_success_ratios: dict = {}

        for attributed_resolved_metrics in metric_computation_result.metric_values:
            # Now obtain 1-dimensional vector of values of computed metric (each element corresponds to a Batch ID).
            metric_values = attributed_resolved_metrics.metric_values[:, 0]

            match_strftime_unexpected_count: int = sum(metric_values)
            success_ratio: float = (
                nonnull_count - match_strftime_unexpected_count
            ) / nonnull_count
            format_string_success_ratios[
                attributed_resolved_metrics.metric_attributes["strftime_format"]
            ] = success_ratio

        best_fmt_string: Optional[str] = None
        best_ratio: float = 0.0

        # Obtain threshold from "rule state" (i.e., variables and parameters); from instance variable otherwise.
        threshold: float = get_parameter_value_and_validate_return_type(
            domain=domain,
            parameter_reference=self.threshold,
            expected_return_type=float,
            variables=variables,
            parameters=parameters,
        )

        fmt_string: str
        ratio: float
        for fmt_string, ratio in format_string_success_ratios.items():
            if ratio > best_ratio and ratio >= threshold:
                best_fmt_string = fmt_string
                best_ratio = ratio

        return (
            best_fmt_string,
            {
                "success_ratio": best_ratio,
                "candidate_strings": sorted(candidate_strings),
            },
        )
