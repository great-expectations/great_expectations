from typing import Any, Dict, List, Optional

from great_expectations.core import Domain
from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.rule_based_profiler.data_assistant import DataAssistant
from great_expectations.rule_based_profiler.data_assistant_result import (
    DataAssistantResult,
    MetricMultiBatchDataAssistantResult,
)
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


class MetricMultiBatchDataAssistant(DataAssistant):
    """
    MetricMultiBatchDataAssistant provides "ValidationGraph" or metric values for multi-Batch metrics computations.
    """

    __alias__: str = "metrics"

    def __init__(
        self,
        name: str,
        validator: "Validator",  # noqa: F821
        metric_configurations: List[MetricConfiguration],
        result_format: MetricsComputationResultFormat,
    ) -> None:
        self._metric_configurations = metric_configurations
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
        domain_metric_configurations_map: Dict[
            Domain, List[MetricConfiguration]
        ] = self._build_domain_metric_configurations_map()

        domain: Domain
        metric_configurations: List[MetricConfiguration]
        return [
            self._build_metric_multi_batch_rule(
                domain=domain, metric_configurations=metric_configurations
            )
            for domain, metric_configurations in domain_metric_configurations_map.items()
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

    def _build_domain_metric_configurations_map(
        self,
    ) -> Dict[Domain, List[MetricConfiguration]]:
        domain_metric_configurations_map: Dict[Domain, List[MetricConfiguration]] = {}
        metric_configurations_for_domain: List[MetricConfiguration]
        metric_configuration: MetricConfiguration
        domain: Domain
        for metric_configuration in self._metric_configurations:
            domain = metric_configuration.get_domain()
            metric_configurations_for_domain = domain_metric_configurations_map.get(
                domain
            )
            if metric_configurations_for_domain is None:
                metric_configurations_for_domain = []
                domain_metric_configurations_map[
                    domain
                ] = metric_configurations_for_domain

            metric_configurations_for_domain.append(metric_configuration)

        return domain_metric_configurations_map

    def _build_metric_multi_batch_rule(
        self, domain: Domain, metric_configurations: List[MetricConfiguration]
    ) -> Rule:
        """
        This method builds "Rule" object, which emits mult-Batch "ValidationGraph" object or resolved metrics.
        """
        # Step-1: Obtain "DomainBuilder" object.

        domain_builder: DomainBuilder = (
            self._get_single_domain_passthrough_domain_builder(domain=domain)
        )

        # Step-2: Declare "ParameterBuilder" for every metric of interest.

        parameter_builders: List[
            ParameterBuilder
        ] = self._get_metric_multi_batch_parameter_builders(
            metric_configurations=metric_configurations,
            result_format=self._result_format,
        )

        # Step-3: Instantiate and return "Rule" object, comprised of "variables", "domain_builder", "parameter_builders", and "expectation_configuration_builders" components.

        variables: dict = {
            "table": domain.domain_kwargs.get("table"),
        }
        rule = Rule(
            name=f"metric_multi_batch_rule_{domain.id}",
            variables=variables,
            domain_builder=domain_builder,
            parameter_builders=parameter_builders,
            expectation_configuration_builders=None,
        )

        return rule

    @staticmethod
    def _get_single_domain_passthrough_domain_builder(domain: Domain) -> DomainBuilder:
        """
        This method returns "DomainBuilder" object, corresponding to and appropriate for "domain" argument.
        """
        domain_type: MetricDomainTypes = domain.domain_type
        if domain_type == MetricDomainTypes.TABLE:
            return TableDomainBuilder(
                data_context=None,
            )

        if domain_type == MetricDomainTypes.COLUMN:
            return ColumnDomainBuilder(
                include_column_names=[domain.domain_kwargs["column"]],
                exclude_column_names=None,
                include_column_name_suffixes=None,
                exclude_column_name_suffixes=None,
                semantic_type_filter_module_name=None,
                semantic_type_filter_class_name=None,
                include_semantic_types=None,
                exclude_semantic_types=None,
                data_context=None,
            )

        if domain_type == MetricDomainTypes.COLUMN_PAIR:
            return ColumnPairDomainBuilder(
                include_column_names=[
                    domain.domain_kwargs["column_A"],
                    domain.domain_kwargs["column_B"],
                ],
                data_context=None,
            )

        if domain_type == MetricDomainTypes.MULTICOLUMN:
            return MultiColumnDomainBuilder(
                include_column_names=domain.domain_kwargs["column_list"],
                data_context=None,
            )

        raise ValueError(f"""Domain type "{domain_type}" is not recognized.""")

    def _get_metric_multi_batch_parameter_builders(
        self,
        metric_configurations: List[MetricConfiguration],
        result_format: MetricsComputationResultFormat,
    ) -> List[ParameterBuilder]:
        """
        This method returns "ParameterBuilder" objects, which emit mult-Batch "ValidationGraph" or resolved metrics.
        """
        if result_format == MetricsComputationResultFormat.VALIDATION_GRAPH:
            return self._get_metric_multi_batch_validation_graph_parameter_builders(
                metric_configurations=metric_configurations
            )

        if result_format == MetricsComputationResultFormat.RESOLVED_METRICS:
            return (
                self._get_metric_multi_batch_graph_resolved_metrics_parameter_builders(
                    metric_configurations=metric_configurations
                )
            )

        raise ValueError(
            f"""Metric computation result format "{result_format}" is not recognized."""
        )

    @staticmethod
    def _get_metric_multi_batch_validation_graph_parameter_builders(
        metric_configurations: List[MetricConfiguration],
    ) -> List[ParameterBuilder]:
        """
        This method returns list of "ParameterBuilder" objects, each of which emits mult-Batch "ValidationGraph" object.
        """
        metric_configuration: MetricConfiguration
        return [
            DataAssistant.CommonlyUsedParameterBuilders.build_metric_multi_batch_validation_graph_parameter_builder(
                metric_name=metric_configuration.metric_name,
                suffix=metric_configuration.metric_value_kwargs_id,
                metric_domain_kwargs=metric_configuration.metric_domain_kwargs,
                metric_value_kwargs=metric_configuration.metric_value_kwargs,
            )
            for metric_configuration in metric_configurations
        ]

    @staticmethod
    def _get_metric_multi_batch_graph_resolved_metrics_parameter_builders(
        metric_configurations: List[MetricConfiguration],
    ) -> List[ParameterBuilder]:
        """
        This method returns list of "ParameterBuilder" objects, each of which emits mult-Batch resolved metric.
        """
        metric_configuration: MetricConfiguration
        return [
            DataAssistant.CommonlyUsedParameterBuilders.build_metric_multi_batch_parameter_builder(
                metric_name=metric_configuration.metric_name,
                suffix=metric_configuration.metric_value_kwargs_id,
                metric_domain_kwargs=metric_configuration.metric_domain_kwargs,
                metric_value_kwargs=metric_configuration.metric_value_kwargs,
            )
            for metric_configuration in metric_configurations
        ]
