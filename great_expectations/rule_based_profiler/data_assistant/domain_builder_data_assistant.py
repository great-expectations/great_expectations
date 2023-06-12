from typing import TYPE_CHECKING, Any, Dict, List, Optional

from great_expectations.core.domain import SemanticDomainTypes
from great_expectations.rule_based_profiler.data_assistant import DataAssistant
from great_expectations.rule_based_profiler.data_assistant.data_assistant import (
    build_map_metric_domain_builder_rule,
)
from great_expectations.rule_based_profiler.data_assistant_result import (
    DataAssistantResult,
    DomainBuilderDataAssistantResult,
)
from great_expectations.rule_based_profiler.domain_builder import (
    CategoricalColumnDomainBuilder,
    ColumnDomainBuilder,
    TableDomainBuilder,
)
from great_expectations.rule_based_profiler.helpers.cardinality_checker import (
    CardinalityLimitMode,
)
from great_expectations.rule_based_profiler.rule import Rule
from great_expectations.validator.validator import Validator  # noqa: TCH001

if TYPE_CHECKING:
    from great_expectations.rule_based_profiler.domain_builder import DomainBuilder


class DomainBuilderDataAssistant(DataAssistant):
    """
    DomainBuilderDataAssistant is internal component that helps determine which "Domain" types exist in training set.

    DomainBuilderDataAssistant.run() Returns:
        DomainBuilderDataAssistantResult
    """

    __alias__: str = "domains"

    def __init__(
        self,
        name: str,
        validator: Validator,
    ) -> None:
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
        table_domain_rule: Rule = self._build_table_domain_rule()

        column_value_uniqueness_domain_rule: Rule = (
            build_map_metric_domain_builder_rule(
                rule_name="column_value_uniqueness_domain_rule",
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
        )
        column_value_nullity_domain_rule: Rule = build_map_metric_domain_builder_rule(
            rule_name="column_value_nullity_domain_rule",
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
        column_value_nonnullity_domain_rule: Rule = (
            build_map_metric_domain_builder_rule(
                rule_name="column_value_nonnullity_domain_rule",
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
        )
        numeric_columns_domain_rule: Rule = self._build_numeric_columns_domain_rule()
        datetime_columns_domain_rule: Rule = self._build_datetime_columns_domain_rule()
        text_columns_domain_rule: Rule = self._build_text_columns_domain_rule()
        categorical_columns_domain_rule_zero: Rule = (
            self._build_categorical_columns_domain_rule(
                rule_name="categorical_columns_domain_rule_zero",
                cardinality_limit_mode=CardinalityLimitMode.ZERO,
            )
        )
        categorical_columns_domain_rule_one: Rule = (
            self._build_categorical_columns_domain_rule(
                rule_name="categorical_columns_domain_rule_one",
                cardinality_limit_mode=CardinalityLimitMode.ONE,
            )
        )
        categorical_columns_domain_rule_two: Rule = (
            self._build_categorical_columns_domain_rule(
                rule_name="categorical_columns_domain_rule_two",
                cardinality_limit_mode=CardinalityLimitMode.TWO,
            )
        )
        categorical_columns_domain_rule_very_few: Rule = (
            self._build_categorical_columns_domain_rule(
                rule_name="categorical_columns_domain_rule_very_few",
                cardinality_limit_mode=CardinalityLimitMode.VERY_FEW,
            )
        )
        categorical_columns_domain_rule_few: Rule = (
            self._build_categorical_columns_domain_rule(
                rule_name="categorical_columns_domain_rule_few",
                cardinality_limit_mode=CardinalityLimitMode.FEW,
            )
        )
        categorical_columns_domain_rule_some: Rule = (
            self._build_categorical_columns_domain_rule(
                rule_name="categorical_columns_domain_rule_some",
                cardinality_limit_mode=CardinalityLimitMode.SOME,
            )
        )

        return [
            table_domain_rule,
            column_value_uniqueness_domain_rule,
            column_value_nullity_domain_rule,
            column_value_nonnullity_domain_rule,
            numeric_columns_domain_rule,
            datetime_columns_domain_rule,
            text_columns_domain_rule,
            categorical_columns_domain_rule_zero,
            categorical_columns_domain_rule_one,
            categorical_columns_domain_rule_two,
            categorical_columns_domain_rule_very_few,
            categorical_columns_domain_rule_few,
            categorical_columns_domain_rule_some,
        ]

    def _build_data_assistant_result(
        self, data_assistant_result: DataAssistantResult
    ) -> DataAssistantResult:
        return DomainBuilderDataAssistantResult(
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

    @staticmethod
    def _build_table_domain_rule() -> Rule:
        """
        This method builds "Rule" object focused on emitting "Domain" object for table "Domain" type.
        """
        table_domain_builder: DomainBuilder = TableDomainBuilder(
            data_context=None,
        )

        rule = Rule(
            name="table_domain_rule",
            variables=None,
            domain_builder=table_domain_builder,
            parameter_builders=None,
            expectation_configuration_builders=None,
        )

        return rule

    @staticmethod
    def _build_numeric_columns_domain_rule() -> Rule:
        """
        This method builds "Rule" object focused on emitting "Domain" objects for numeric columns.
        """
        numeric_column_type_domain_builder: DomainBuilder = ColumnDomainBuilder(
            include_column_names=None,
            exclude_column_names=None,
            include_column_name_suffixes=None,
            exclude_column_name_suffixes=[
                "_id",
                "_ID",
            ],
            semantic_type_filter_module_name=None,
            semantic_type_filter_class_name=None,
            include_semantic_types=[
                SemanticDomainTypes.NUMERIC,
            ],
            exclude_semantic_types=[
                SemanticDomainTypes.IDENTIFIER,
            ],
            data_context=None,
        )

        rule = Rule(
            name="numeric_columns_domain_rule",
            variables=None,
            domain_builder=numeric_column_type_domain_builder,
            parameter_builders=None,
            expectation_configuration_builders=None,
        )

        return rule

    @staticmethod
    def _build_datetime_columns_domain_rule() -> Rule:
        """
        This method builds "Rule" object focused on emitting "Domain" objects for datetime columns.
        """
        datetime_column_type_domain_builder: DomainBuilder = ColumnDomainBuilder(
            include_column_names=None,
            exclude_column_names=None,
            include_column_name_suffixes=None,
            exclude_column_name_suffixes=None,
            semantic_type_filter_module_name=None,
            semantic_type_filter_class_name=None,
            include_semantic_types=[
                SemanticDomainTypes.DATETIME,
            ],
            exclude_semantic_types=[
                SemanticDomainTypes.TEXT,
            ],
            data_context=None,
        )

        rule = Rule(
            name="datetime_columns_domain_rule",
            variables=None,
            domain_builder=datetime_column_type_domain_builder,
            parameter_builders=None,
            expectation_configuration_builders=None,
        )

        return rule

    @staticmethod
    def _build_text_columns_domain_rule() -> Rule:
        """
        This method builds "Rule" object focused on emitting "Domain" objects for text columns.
        """
        text_column_type_domain_builder: DomainBuilder = ColumnDomainBuilder(
            include_column_names=None,
            exclude_column_names=None,
            include_column_name_suffixes=None,
            exclude_column_name_suffixes=None,
            semantic_type_filter_module_name=None,
            semantic_type_filter_class_name=None,
            include_semantic_types=[
                SemanticDomainTypes.TEXT,
            ],
            exclude_semantic_types=[
                SemanticDomainTypes.NUMERIC,
                SemanticDomainTypes.DATETIME,
            ],
            data_context=None,
        )

        rule = Rule(
            name="text_columns_domain_rule",
            variables=None,
            domain_builder=text_column_type_domain_builder,
            parameter_builders=None,
            expectation_configuration_builders=None,
        )

        return rule

    @staticmethod
    def _build_categorical_columns_domain_rule(
        rule_name: str,
        cardinality_limit_mode: CardinalityLimitMode,
    ) -> Rule:
        """
        This method builds "Rule" object focused on emitting "Domain" objects for categorical columns.
        """

        categorical_column_type_domain_builder: DomainBuilder = (
            CategoricalColumnDomainBuilder(
                include_column_names=None,
                exclude_column_names=None,
                include_column_name_suffixes=None,
                exclude_column_name_suffixes=None,
                semantic_type_filter_module_name=None,
                semantic_type_filter_class_name=None,
                include_semantic_types=None,
                exclude_semantic_types=None,
                allowed_semantic_types_passthrough=None,
                cardinality_limit_mode=cardinality_limit_mode,
                max_unique_values=None,
                max_proportion_unique=None,
                data_context=None,
            )
        )

        rule = Rule(
            name=rule_name,
            variables=None,
            domain_builder=categorical_column_type_domain_builder,
            parameter_builders=None,
            expectation_configuration_builders=None,
        )

        return rule
