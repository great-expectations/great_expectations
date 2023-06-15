from typing import TYPE_CHECKING, Any, Dict, List, Optional

from great_expectations.rule_based_profiler.data_assistant import DataAssistant
from great_expectations.rule_based_profiler.data_assistant.data_assistant import (
    build_map_metric_rule,
)
from great_expectations.rule_based_profiler.data_assistant_result import (
    ColumnValueNullityDataAssistantResult,
    DataAssistantResult,
)
from great_expectations.rule_based_profiler.rule import Rule
from great_expectations.validator.validator import Validator

if TYPE_CHECKING:
    from great_expectations.rule_based_profiler.parameter_builder import (
        ParameterBuilder,
    )


class ColumnValueNullityDataAssistant(DataAssistant):
    """
    ColumnValueNullitylDataAssistant provides dataset exploration and validation for columns with NULL values.

    ColumnValueNullityDataAssistant.run() Args:
        - batch_request (BatchRequestBase or dict): The Batch Request to be passed to the Data Assistant.
        - estimation (str): One of "exact" (default) or "flag_outliers" indicating the type of data you believe the
            Batch Request to contain. Valid or trusted data should use "exact", while Expectations produced with data
            that is suspected to have quality issues may benefit from "flag_outliers".
        - include_column_names (list): A list containing the column names you wish to include.
        - exclude_column_names (list): A list containing the column names you with to exclude.
        - include_column_name_suffixes (list): A list containing the column name suffixes you wish to include.
        - exclude_column_name_suffixes (list): A list containing the column name suffixes you wish to exclude.

    ColumnValueNullityDataAssistant.run() Returns:
        ColumnValueNullityDataAssistantResult
    """

    __alias__: str = "column_value_nullity"

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
        total_count_metric_multi_batch_parameter_builder_for_evaluations: ParameterBuilder = (
            DataAssistant.commonly_used_parameter_builders.get_table_row_count_metric_multi_batch_parameter_builder()
        )

        column_value_nullity_rule: Rule = build_map_metric_rule(
            data_assistant_class_name=self.__class__.__name__,
            rule_name="column_value_nullity_rule",
            expectation_type="expect_column_values_to_be_null",
            map_metric_name="column_values.null",
            total_count_metric_multi_batch_parameter_builder_for_evaluations=total_count_metric_multi_batch_parameter_builder_for_evaluations,
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
            column_value_nullity_rule,
        ]

    def _build_data_assistant_result(
        self, data_assistant_result: DataAssistantResult
    ) -> DataAssistantResult:
        return ColumnValueNullityDataAssistantResult(
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
