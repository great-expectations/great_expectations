from typing import Any, Dict, List, Optional

from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.rule_based_profiler.config import ParameterBuilderConfig
from great_expectations.rule_based_profiler.data_assistant import DataAssistant
from great_expectations.rule_based_profiler.data_assistant.data_assistant import (
    set_parameter_builders_json_serialize,
)
from great_expectations.rule_based_profiler.domain_builder import (
    MapMetricColumnDomainBuilder,
)
from great_expectations.rule_based_profiler.expectation_configuration_builder import (
    DefaultExpectationConfigurationBuilder,
    ExpectationConfigurationBuilder,
)
from great_expectations.rule_based_profiler.parameter_builder import (
    MeanUnexpectedMapMetricMultiBatchParameterBuilder,
    ParameterBuilder,
)
from great_expectations.rule_based_profiler.rule import Rule
from great_expectations.rule_based_profiler.types import (
    DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
    FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY,
    FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY,
    Domain,
)
from great_expectations.rule_based_profiler.types.data_assistant_result import (
    DataAssistantResult,
    OnboardingDataAssistantResult,
)
from great_expectations.validator.validator import Validator


class OnboardingDataAssistant(DataAssistant):
    """
    OnboardingDataAssistant provides dataset exploration and validation to help with Great Expectations "Onboarding".
    """

    __alias__: str = "onboarding"

    def __init__(
        self,
        name: str,
        validator: Validator,
    ) -> None:
        super().__init__(
            name=name,
            validator=validator,
        )

    @property
    def expectation_kwargs_by_expectation_type(self) -> Dict[str, Dict[str, Any]]:
        return {
            "expect_table_row_count_to_be_between": {
                "auto": True,
                "profiler_config": None,
            },
        }

    @property
    def metrics_parameter_builders_by_domain(
        self,
    ) -> Dict[Domain, List[ParameterBuilder]]:
        table_row_count_metric_multi_batch_parameter_builder: ParameterBuilder = (
            DataAssistant.COMMONLY_USED_PARAMETER_BUILDERS.table_row_count_metric_multi_batch_parameter_builder
        )
        column_values_unique_unexpected_count_metric_multi_batch_parameter_builder: ParameterBuilder = (
            DataAssistant.COMMONLY_USED_PARAMETER_BUILDERS.column_values_unique_unexpected_count_metric_multi_batch_parameter_builder
        )

        set_parameter_builders_json_serialize(
            parameter_builders=[
                table_row_count_metric_multi_batch_parameter_builder,
                column_values_unique_unexpected_count_metric_multi_batch_parameter_builder,
            ],
            json_serialize=True,
        )

        return {
            Domain(domain_type=MetricDomainTypes.TABLE,): [
                table_row_count_metric_multi_batch_parameter_builder,
            ],
            Domain(domain_type=MetricDomainTypes.COLUMN,): [
                column_values_unique_unexpected_count_metric_multi_batch_parameter_builder,
            ],
        }

    @property
    def variables(self) -> Optional[Dict[str, Any]]:
        return None

    @property
    def rules(self) -> Optional[List[Rule]]:
        uniqueness_rule: Rule = self._build_uniqueness_rule()

        return [
            uniqueness_rule,
        ]

    def _build_data_assistant_result(
        self, data_assistant_result: DataAssistantResult
    ) -> DataAssistantResult:
        return OnboardingDataAssistantResult(
            batch_id_to_batch_identifier_display_name_map=data_assistant_result.batch_id_to_batch_identifier_display_name_map,
            profiler_config=data_assistant_result.profiler_config,
            metrics_by_domain=data_assistant_result.metrics_by_domain,
            expectation_configurations=data_assistant_result.expectation_configurations,
            citation=data_assistant_result.citation,
            execution_time=data_assistant_result.execution_time,
        )

    def _build_uniqueness_rule(self) -> Rule:
        """
        This method builds "Rule" object focused on emitting "ExpectationConfiguration" objects focused on "uniqueness".
        """
        column_values_unique_domain_builder: MapMetricColumnDomainBuilder = (
            MapMetricColumnDomainBuilder(
                map_metric_name="column_values.unique",
                include_column_names=None,
                exclude_column_names=None,
                include_column_name_suffixes=None,
                exclude_column_name_suffixes=None,
                semantic_type_filter_module_name=None,
                semantic_type_filter_class_name=None,
                include_semantic_types=None,
                exclude_semantic_types=None,
                max_unexpected_values=0,
                max_unexpected_ratio=None,
                min_max_unexpected_values_proportion=9.75e-1,
                data_context=self._validator.data_context,
            )
        )
        total_count_metric_multi_batch_parameter_builder: ParameterBuilder = (
            DataAssistant.COMMONLY_USED_PARAMETER_BUILDERS.table_row_count_metric_multi_batch_parameter_builder
        )
        column_values_nonnull_unexpected_count_metric_multi_batch_parameter_builder: ParameterBuilder = (
            DataAssistant.COMMONLY_USED_PARAMETER_BUILDERS.column_values_nonnull_unexpected_count_metric_multi_batch_parameter_builder
        )

        set_parameter_builders_json_serialize(
            parameter_builders=[
                total_count_metric_multi_batch_parameter_builder,
                column_values_nonnull_unexpected_count_metric_multi_batch_parameter_builder,
            ],
            json_serialize=False,
        )

        evaluation_parameter_builder_configs: Optional[List[ParameterBuilderConfig]] = [
            ParameterBuilderConfig(
                **total_count_metric_multi_batch_parameter_builder.to_json_dict()
            ),
            ParameterBuilderConfig(
                **column_values_nonnull_unexpected_count_metric_multi_batch_parameter_builder.to_json_dict()
            ),
        ]
        column_values_unique_mean_unexpected_value_multi_batch_parameter_builder: MeanUnexpectedMapMetricMultiBatchParameterBuilder = MeanUnexpectedMapMetricMultiBatchParameterBuilder(
            name="column_values_unique_mean_unexpected_value",
            map_metric_name="column_values.unique",
            total_count_parameter_builder_name=total_count_metric_multi_batch_parameter_builder.name,
            null_count_parameter_builder_name=column_values_nonnull_unexpected_count_metric_multi_batch_parameter_builder.name,
            metric_domain_kwargs=DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
            metric_value_kwargs=None,
            evaluation_parameter_builder_configs=evaluation_parameter_builder_configs,
            json_serialize=True,
            data_context=self._validator.data_context,
        )

        validation_parameter_builder_configs: Optional[List[ParameterBuilderConfig]] = [
            ParameterBuilderConfig(
                **column_values_unique_mean_unexpected_value_multi_batch_parameter_builder.to_json_dict()
            ),
        ]
        max_column_values_unique_mean_unexpected_value_ratio: float = 1.0e-2
        expect_column_values_to_be_unique_expectation_configuration_builder: DefaultExpectationConfigurationBuilder = DefaultExpectationConfigurationBuilder(
            expectation_type="expect_column_values_to_be_unique",
            condition=f"{column_values_unique_mean_unexpected_value_multi_batch_parameter_builder.fully_qualified_parameter_name}.{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY} <= {max_column_values_unique_mean_unexpected_value_ratio}",
            validation_parameter_builder_configs=validation_parameter_builder_configs,
            meta={
                "profiler_details": f"{column_values_unique_mean_unexpected_value_multi_batch_parameter_builder.fully_qualified_parameter_name}.{FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY}",
            },
        )

        parameter_builders = []
        expectation_configuration_builders: List[ExpectationConfigurationBuilder] = [
            expect_column_values_to_be_unique_expectation_configuration_builder,
        ]
        uniqueness_rule: Rule = Rule(
            name="uniqueness_rule",
            variables=None,
            domain_builder=column_values_unique_domain_builder,
            parameter_builders=parameter_builders,
            expectation_configuration_builders=expectation_configuration_builders,
        )

        return uniqueness_rule
