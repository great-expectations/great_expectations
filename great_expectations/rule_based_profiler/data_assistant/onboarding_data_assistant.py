from typing import Any, Dict, List, Optional

from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.rule_based_profiler.data_assistant import DataAssistant
from great_expectations.rule_based_profiler.data_assistant.data_assistant import (
    build_map_metric_rule,
)
from great_expectations.rule_based_profiler.parameter_builder import ParameterBuilder
from great_expectations.rule_based_profiler.rule import Rule
from great_expectations.rule_based_profiler.types import Domain
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
        table_row_count_metric_multi_batch_parameter_builder: ParameterBuilder = DataAssistant.COMMONLY_USED_PARAMETER_BUILDERS.get_table_row_count_metric_multi_batch_parameter_builder(
            json_serialize=True
        )
        column_values_unique_unexpected_count_metric_multi_batch_parameter_builder: ParameterBuilder = DataAssistant.COMMONLY_USED_PARAMETER_BUILDERS.get_column_values_unique_unexpected_count_metric_multi_batch_parameter_builder(
            json_serialize=True
        )
        column_values_nonnull_unexpected_count_metric_multi_batch_parameter_builder: ParameterBuilder = DataAssistant.COMMONLY_USED_PARAMETER_BUILDERS.get_column_values_nonnull_unexpected_count_metric_multi_batch_parameter_builder(
            json_serialize=True
        )
        column_values_null_unexpected_count_metric_multi_batch_parameter_builder: ParameterBuilder = DataAssistant.COMMONLY_USED_PARAMETER_BUILDERS.get_column_values_null_unexpected_count_metric_multi_batch_parameter_builder(
            json_serialize=True
        )

        return {
            Domain(domain_type=MetricDomainTypes.TABLE,): [
                table_row_count_metric_multi_batch_parameter_builder,
            ],
            Domain(domain_type=MetricDomainTypes.COLUMN,): [
                column_values_unique_unexpected_count_metric_multi_batch_parameter_builder,
                column_values_nonnull_unexpected_count_metric_multi_batch_parameter_builder,
                column_values_null_unexpected_count_metric_multi_batch_parameter_builder,
            ],
        }

    @property
    def variables(self) -> Optional[Dict[str, Any]]:
        return None

    @property
    def rules(self) -> Optional[List[Rule]]:
        column_value_uniqueness_rule: Rule = build_map_metric_rule(
            rule_name="column_value_uniqueness_rule",
            expectation_type="expect_column_values_to_be_unique",
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
        )
        column_value_nullity_rule: Rule = build_map_metric_rule(
            rule_name="column_value_nullity_rule",
            expectation_type="expect_column_values_to_be_null",
            map_metric_name="column_values.null",
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
        )
        column_value_nonnullity_rule: Rule = build_map_metric_rule(
            rule_name="column_value_nonnullity_rule",
            expectation_type="expect_column_values_to_not_be_null",
            map_metric_name="column_values.nonnull",
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
        )

        return [
            column_value_uniqueness_rule,
            column_value_nullity_rule,
            column_value_nonnullity_rule,
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
