import logging
from typing import Dict, List, Optional, Union

import great_expectations.exceptions as ge_exceptions
from great_expectations.rule_based_profiler.attributed_resolved_metrics import (
    AttributedResolvedMetrics,
)
from great_expectations.rule_based_profiler.config import ParameterBuilderConfig
from great_expectations.rule_based_profiler.domain import Domain
from great_expectations.rule_based_profiler.metric_computation_result import (
    MetricComputationDetails,
    MetricComputationResult,
    MetricValues,
)
from great_expectations.rule_based_profiler.parameter_builder import ParameterBuilder
from great_expectations.rule_based_profiler.parameter_container import (
    FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY,
    FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY,
    ParameterContainer,
)
from great_expectations.types.attributes import Attributes

logger = logging.getLogger(__name__)


class DataProfilerProfileReportParameterBuilder(ParameterBuilder):
    """
    This "prerequisite" ParameterBuilder implementation obtains results of "data_profiler.profile_report" metric and
    makes them available in shared memory (ParameterContainer object for Domain of execution) for downstream purposes.
    """

    def __init__(
        self,
        name: str,
        metric_domain_kwargs: Optional[Union[str, dict]] = None,
        metric_value_kwargs: Optional[Union[str, dict]] = None,
        evaluation_parameter_builder_configs: Optional[
            List[ParameterBuilderConfig]
        ] = None,
        data_context: Optional["BaseDataContext"] = None,  # noqa: F821
    ) -> None:
        """
        Configure this SimpleDateFormatStringParameterBuilder
        Args:
            name: the name of this parameter -- this is user-specified parameter name (from configuration);
            it is not the fully-qualified parameter name; a fully-qualified parameter name must start with "$parameter."
            and may contain one or more subsequent parts (e.g., "$parameter.<my_param_from_config>.<metric_name>").
            metric_domain_kwargs: used in MetricConfiguration
            metric_value_kwargs: used in MetricConfiguration
            evaluation_parameter_builder_configs: ParameterBuilder configurations, executing and making whose respective
            ParameterBuilder objects' outputs available (as fully-qualified parameter names) is pre-requisite.
            These "ParameterBuilder" configurations help build parameters needed for this "ParameterBuilder".
            data_context: BaseDataContext associated with this ParameterBuilder
        """
        super().__init__(
            name=name,
            evaluation_parameter_builder_configs=evaluation_parameter_builder_configs,
            data_context=data_context,
        )

        self._metric_domain_kwargs = metric_domain_kwargs
        self._metric_value_kwargs = metric_value_kwargs

    @property
    def metric_domain_kwargs(self) -> Optional[Union[str, dict]]:
        return self._metric_domain_kwargs

    @property
    def metric_value_kwargs(self) -> Optional[Union[str, dict]]:
        return self._metric_value_kwargs

    @metric_value_kwargs.setter
    def metric_value_kwargs(self, value: Optional[Union[str, dict]]) -> None:
        self._metric_value_kwargs = value

    def _build_parameters(
        self,
        domain: Domain,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
        recompute_existing_parameter_values: bool = False,
    ) -> Attributes:
        """
        Builds ParameterContainer object that holds ParameterNode objects with attribute name-value pairs and details.

        Run "data_profiler.profile_report" metric.

        Returns:
            Attributes object, containing computed parameter values and parameter computation details metadata.
        """
        metric_computation_result: MetricComputationResult = self.get_metrics(
            metric_name="data_profiler.profile_report",
            metric_domain_kwargs=self.metric_domain_kwargs,
            metric_value_kwargs=self.metric_value_kwargs,
            domain=domain,
            variables=variables,
            parameters=parameters,
        )
        details: MetricComputationDetails = metric_computation_result.details

        # This should never happen.
        if len(metric_computation_result.attributed_resolved_metrics) != 1:
            raise ge_exceptions.ProfilerExecutionError(
                message=f'Result of metric computations for {self.__class__.__name__} must be a list with exactly 1 element of type "AttributedResolvedMetrics" ({metric_computation_result.attributed_resolved_metrics} found).'
            )

        attributed_resolved_metrics: AttributedResolvedMetrics = (
            metric_computation_result.attributed_resolved_metrics[0]
        )

        metric_values: MetricValues = (
            attributed_resolved_metrics.conditioned_metric_values
        )

        if metric_values is None:
            raise ge_exceptions.ProfilerExecutionError(
                message=f"Result of metric computations for {self.__class__.__name__} is empty."
            )

        # Now obtain 1-dimensional vector of values of computed metric (each element corresponds to a Batch ID).
        metric_values = metric_values[:, 0]

        profile_report: dict = metric_values[0]

        return Attributes(
            {
                FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY: profile_report,
                FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY: details,
            }
        )
