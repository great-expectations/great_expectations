from typing import TYPE_CHECKING, Any, Dict, List, Optional

from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.rule_based_profiler.data_assistant import DataAssistant
from great_expectations.rule_based_profiler.data_assistant_result import (
    DataAssistantResult,
    MetricMultiBatchDataAssistantResult,
)

# TODO: <Alex>ALEX</Alex>
# from great_expectations.validator.validator import Validator
# TODO: <Alex>ALEX</Alex>
# TODO: <Alex>ALEX</Alex>
from great_expectations.rule_based_profiler.domain_builder import (
    ColumnDomainBuilder,
    ColumnPairDomainBuilder,
    DomainBuilder,
    MultiColumnDomainBuilder,
    TableDomainBuilder,
)
from great_expectations.rule_based_profiler.parameter_builder import ParameterBuilder
from great_expectations.rule_based_profiler.parameter_builder.parameter_builder import (
    MetricsComputationResultFormat,
)
from great_expectations.rule_based_profiler.rule import Rule
from great_expectations.validator.metric_configuration import MetricConfiguration

# TODO: <Alex>ALEX</Alex>

# TODO: <Alex>ALEX</Alex>
# if TYPE_CHECKING:
#     from great_expectations.rule_based_profiler.domain_builder import DomainBuilder, ColumnDomainBuilder, ColumnPairDomainBuilder, MultiColumnDomainBuilder, TableDomainBuilder
# TODO: <Alex>ALEX</Alex>


class MetricMultiBatchDataAssistant(DataAssistant):
    """
    MetricMultiBatchDataAssistant provides "ValidationGraph" or metric values for multi-Batch metrics computations.
    """

    __alias__: str = "metrics"

    def __init__(
        self,
        name: str,
        validator: "Validator",  # noqa: F821
        metric_configuration: MetricConfiguration,
        result_format: MetricsComputationResultFormat,
    ) -> None:
        self._metric_configuration = metric_configuration
        self._result_format = result_format

        super().__init__(
            name=name,
            validator=validator,
        )

    def get_variables(self) -> Optional[Dict[str, Any]]:
        """
        Returns:
            Optional "variables" configuration attribute name/value pairs (overrides), commonly-used in Builder objects.
        """
        return None

    def get_rules(self) -> Optional[List[Rule]]:
        """
        Returns:
            Optional custom list of "Rule" objects implementing particular "DataAssistant" functionality.
        """
        metric_multi_batch_rule: Rule = self._build_metric_multi_batch_rule()

        return [
            metric_multi_batch_rule,
        ]

    def _build_data_assistant_result(
        self, data_assistant_result: DataAssistantResult
    ) -> DataAssistantResult:
        return MetricMultiBatchDataAssistantResult(
            _batch_id_to_batch_identifier_display_name_map=data_assistant_result._batch_id_to_batch_identifier_display_name_map,
            profiler_config=data_assistant_result.profiler_config,
            profiler_execution_time=data_assistant_result.profiler_execution_time,
            rule_domain_builder_execution_time=data_assistant_result.rule_domain_builder_execution_time,
            rule_execution_time=data_assistant_result.rule_execution_time,
            metrics_by_domain=data_assistant_result.metrics_by_domain,
            expectation_configurations=data_assistant_result.expectation_configurations,
            citation=data_assistant_result.citation,
            _usage_statistics_handler=data_assistant_result._usage_statistics_handler,
        )

    def _build_metric_multi_batch_rule(self) -> Rule:
        """
        This method builds "Rule" object, which emits mult-Batch "ValidationGraph" object or resolved metrics.
        """
        domain_builder: DomainBuilder

        domain_type: MetricDomainTypes = self._metric_configuration.get_domain_type()
        if domain_type == MetricDomainTypes.TABLE:
            domain_builder: DomainBuilder = TableDomainBuilder(
                data_context=None,
            )
        elif domain_type == MetricDomainTypes.COLUMN:
            domain_builder = ColumnDomainBuilder(
                include_column_names=[
                    self._metric_configuration.metric_domain_kwargs["column"]
                ],
                exclude_column_names=None,
                include_column_name_suffixes=None,
                exclude_column_name_suffixes=None,
                semantic_type_filter_module_name=None,
                semantic_type_filter_class_name=None,
                include_semantic_types=None,
                exclude_semantic_types=None,
                data_context=None,
            )
        elif domain_type == MetricDomainTypes.COLUMN_PAIR:
            domain_builder = ColumnPairDomainBuilder(
                include_column_names=[
                    self._metric_configuration.metric_domain_kwargs["column_A"],
                    self._metric_configuration.metric_domain_kwargs["column_B"],
                ],
                data_context=None,
            )
        elif domain_type == MetricDomainTypes.MULTICOLUMN:
            domain_builder = MultiColumnDomainBuilder(
                include_column_names=self._metric_configuration.metric_domain_kwargs[
                    "column_list"
                ],
                data_context=None,
            )
        else:
            raise ValueError(f"""Domain type "{domain_type}" is not recognized.""")

        # Step-2: Declare "ParameterBuilder" for every metric of interest.

        metric_multi_batch_parameter_builder_for_metrics: ParameterBuilder
        if self._result_format == MetricsComputationResultFormat.VALIDATION_GRAPH:
            metric_multi_batch_parameter_builder_for_metrics: ParameterBuilder = DataAssistant.CommonlyUsedParameterBuilders.build_metric_multi_batch_validation_graph_parameter_builder(
                metric_name=self._metric_configuration.metric_name,
                metric_domain_kwargs=self._metric_configuration.metric_domain_kwargs,
                metric_value_kwargs=self._metric_configuration.metric_value_kwargs,
            )
        elif self._result_format == MetricsComputationResultFormat.RESOLVED_METRICS:
            metric_multi_batch_parameter_builder_for_metrics: ParameterBuilder = DataAssistant.CommonlyUsedParameterBuilders.build_metric_multi_batch_parameter_builder(
                metric_name=self._metric_configuration.metric_name,
                metric_domain_kwargs=self._metric_configuration.metric_domain_kwargs,
                metric_value_kwargs=self._metric_configuration.metric_value_kwargs,
            )
        else:
            raise ValueError(
                f"""Metric computation result format "{self._result_format}" is not recognized."""
            )

        parameter_builders: List[ParameterBuilder] = [
            metric_multi_batch_parameter_builder_for_metrics,
        ]
        rule = Rule(
            name="metric_multi_batch_rule",
            variables=None,
            domain_builder=domain_builder,
            parameter_builders=parameter_builders,
            expectation_configuration_builders=None,
        )

        return rule
