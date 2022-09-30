from typing import Any, Dict, List, Optional

from contrib.experimental.great_expectations_experimental.rule_based_profiler.data_assistant_result import (
    StatisticsDataAssistantResult,
)
from great_expectations.rule_based_profiler.config import ParameterBuilderConfig
from great_expectations.rule_based_profiler.data_assistant import DataAssistant
from great_expectations.rule_based_profiler.data_assistant_result import (
    DataAssistantResult,
)
from great_expectations.rule_based_profiler.domain import SemanticDomainTypes
from great_expectations.rule_based_profiler.domain_builder import (
    ColumnDomainBuilder,
    TableDomainBuilder,
)
from great_expectations.rule_based_profiler.helpers.cardinality_checker import (
    CardinalityLimitMode,
)
from great_expectations.rule_based_profiler.parameter_builder import (
    MeanTableColumnsSetMatchMultiBatchParameterBuilder,
    MeanUnexpectedMapMetricMultiBatchParameterBuilder,
    ParameterBuilder,
)
from great_expectations.rule_based_profiler.parameter_container import (
    DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
    VARIABLES_KEY,
)
from great_expectations.rule_based_profiler.rule import Rule
from great_expectations.validator.validator import Validator


class StatisticsDataAssistant(DataAssistant):
    """
    StatisticsDataAssistant provides metrics for dataset exploration purposes.

    Fundamentally, StatisticsDataAssistant is "OnboardingDataAssistant minus Expectations -- only Metrics", the intended
    usecase being obtaining description of data via metrics as well as comparing metrics between sub-sampeled datasets
    to determine the smallest dataset, whose statistics represent the overall data distribution sufficiantly adequately.
    """

    __alias__: str = "statistics"

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

        column_integrity_rule: Rule = self._build_column_integrity_rule(
            total_count_metric_multi_batch_parameter_builder_for_evaluations=total_count_metric_multi_batch_parameter_builder_for_evaluations
        )
        numeric_columns_rule: Rule = self._build_numeric_columns_rule()
        datetime_columns_rule: Rule = self._build_datetime_columns_rule()
        text_columns_rule: Rule = self._build_text_columns_rule()
        categorical_columns_rule: Rule = self._build_categorical_columns_rule()

        return [
            column_integrity_rule,
            numeric_columns_rule,
            datetime_columns_rule,
            text_columns_rule,
            categorical_columns_rule,
        ]

    def _build_data_assistant_result(
        self, data_assistant_result: DataAssistantResult
    ) -> DataAssistantResult:
        return StatisticsDataAssistantResult(
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
    def _build_table_rule() -> Rule:
        """
        This method builds "Rule" object focused on emitting "ExpectationConfiguration" objects for table "Domain" type.
        """
        # Step-1: Instantiate "TableDomainBuilder" object.

        table_domain_builder = TableDomainBuilder(
            data_context=None,
        )

        # Step-2: Declare "ParameterBuilder" for every metric of interest.

        table_row_count_metric_multi_batch_parameter_builder_for_metrics: ParameterBuilder = (
            DataAssistant.commonly_used_parameter_builders.get_table_row_count_metric_multi_batch_parameter_builder()
        )
        table_columns_metric_multi_batch_parameter_builder_for_metrics: ParameterBuilder = (
            DataAssistant.commonly_used_parameter_builders.get_table_columns_metric_multi_batch_parameter_builder()
        )

        # Step-3: Declare "ParameterBuilder" configurations for all additional statistics needed.

        evaluation_parameter_builder_configs: Optional[List[ParameterBuilderConfig]] = [
            ParameterBuilderConfig(
                **table_row_count_metric_multi_batch_parameter_builder_for_metrics.to_json_dict()
            ),
        ]
        table_row_count_range_parameter_builder_for_validations: ParameterBuilder = DataAssistant.commonly_used_parameter_builders.build_numeric_metric_range_multi_batch_parameter_builder(
            metric_name=None,
            metric_value_kwargs=None,
            evaluation_parameter_builder_configs=evaluation_parameter_builder_configs,
        )
        mean_table_columns_set_match_multi_batch_parameter_builder_for_validations = (
            MeanTableColumnsSetMatchMultiBatchParameterBuilder(
                name="column_names_set_estimator",
                metric_domain_kwargs=DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
                metric_value_kwargs=None,
                evaluation_parameter_builder_configs=None,
            )
        )

        # Step-4: Instantiate and return "Rule" object, comprised of "variables", "domain_builder", "parameter_builders", and "expectation_configuration_builders" components.

        variables: dict = {
            "false_positive_rate": 0.05,
            "estimator": "bootstrap",
            "n_resamples": 9999,
            "random_seed": None,
            "quantile_statistic_interpolation_method": "nearest",
            "quantile_bias_correction": False,
            "quantile_bias_std_error_ratio_threshold": None,
            "include_estimator_samples_histogram_in_details": False,
            "truncate_values": {
                "lower_bound": 0,
                "upper_bound": None,
            },
            "round_decimals": 0,
            "exact_match": None,
            "success_ratio": 1.0,
        }
        parameter_builders: List[ParameterBuilder] = [
            table_row_count_metric_multi_batch_parameter_builder_for_metrics,
            table_columns_metric_multi_batch_parameter_builder_for_metrics,
            table_row_count_range_parameter_builder_for_validations,
            mean_table_columns_set_match_multi_batch_parameter_builder_for_validations,
        ]
        rule = Rule(
            name="table_rule",
            variables=variables,
            domain_builder=table_domain_builder,
            parameter_builders=parameter_builders,
            expectation_configuration_builders=None,
        )

        return rule

    @staticmethod
    def _build_column_integrity_rule(
        total_count_metric_multi_batch_parameter_builder_for_evaluations: Optional[
            ParameterBuilder
        ] = None,
    ) -> Rule:
        """
        This method builds "Rule" object focused on emitting "map" style column integrity metrics.
        """

        # Step-1: Instantiate "ColumnDomainBuilder" for selecting all columns.

        every_column_domain_builder = ColumnDomainBuilder(
            include_column_names=None,
            exclude_column_names=None,
            include_column_name_suffixes=None,
            exclude_column_name_suffixes=None,
            semantic_type_filter_module_name=None,
            semantic_type_filter_class_name=None,
            include_semantic_types=None,
            exclude_semantic_types=None,
            data_context=None,
        )

        # Step-2: Declare "ParameterBuilder" for every metric of interest.

        column_values_nonnull_unexpected_count_metric_multi_batch_parameter_builder_for_metrics: ParameterBuilder = (
            DataAssistant.commonly_used_parameter_builders.get_column_values_nonnull_unexpected_count_metric_multi_batch_parameter_builder()
        )

        # Step-3: Declare "ParameterBuilder" configurations for all additional statistics needed.

        evaluation_parameter_builder_configs: Optional[List[ParameterBuilderConfig]]

        if total_count_metric_multi_batch_parameter_builder_for_evaluations is None:
            total_count_metric_multi_batch_parameter_builder_for_evaluations = (
                DataAssistant.commonly_used_parameter_builders.get_table_row_count_metric_multi_batch_parameter_builder()
            )

        column_values_nonnull_unexpected_count_metric_multi_batch_parameter_builder_for_evaluations = column_values_nonnull_unexpected_count_metric_multi_batch_parameter_builder_for_metrics

        evaluation_parameter_builder_configs: Optional[List[ParameterBuilderConfig]] = [
            ParameterBuilderConfig(
                **total_count_metric_multi_batch_parameter_builder_for_evaluations.to_json_dict()
            ),
            ParameterBuilderConfig(
                **column_values_nonnull_unexpected_count_metric_multi_batch_parameter_builder_for_evaluations.to_json_dict()
            ),
        ]

        map_metric_name: str

        map_metric_name = "column_values.unique"
        column_values_unique_mean_unexpected_value_multi_batch_parameter_builder_for_validations = MeanUnexpectedMapMetricMultiBatchParameterBuilder(
            name=f"{map_metric_name}.unexpected_value",
            map_metric_name=map_metric_name,
            total_count_parameter_builder_name=total_count_metric_multi_batch_parameter_builder_for_evaluations.name,
            null_count_parameter_builder_name=column_values_nonnull_unexpected_count_metric_multi_batch_parameter_builder_for_evaluations.name,
            metric_domain_kwargs=DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
            metric_value_kwargs=None,
            evaluation_parameter_builder_configs=evaluation_parameter_builder_configs,
            data_context=None,
        )
        map_metric_name = "column_values.null"
        column_values_null_mean_unexpected_value_multi_batch_parameter_builder_for_validations = MeanUnexpectedMapMetricMultiBatchParameterBuilder(
            name=f"{map_metric_name}.unexpected_value",
            map_metric_name=map_metric_name,
            total_count_parameter_builder_name=total_count_metric_multi_batch_parameter_builder_for_evaluations.name,
            null_count_parameter_builder_name=column_values_nonnull_unexpected_count_metric_multi_batch_parameter_builder_for_evaluations.name,
            metric_domain_kwargs=DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
            metric_value_kwargs=None,
            evaluation_parameter_builder_configs=evaluation_parameter_builder_configs,
            data_context=None,
        )
        map_metric_name = "column_values.nonnull"
        column_values_nonnull_mean_unexpected_value_multi_batch_parameter_builder_for_validations = MeanUnexpectedMapMetricMultiBatchParameterBuilder(
            name=f"{map_metric_name}.unexpected_value",
            map_metric_name=map_metric_name,
            total_count_parameter_builder_name=total_count_metric_multi_batch_parameter_builder_for_evaluations.name,
            null_count_parameter_builder_name=column_values_nonnull_unexpected_count_metric_multi_batch_parameter_builder_for_evaluations.name,
            metric_domain_kwargs=DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
            metric_value_kwargs=None,
            evaluation_parameter_builder_configs=evaluation_parameter_builder_configs,
            data_context=None,
        )

        # Step-4: Instantiate and return "Rule" object, comprised of "variables", "domain_builder", "parameter_builders", and "expectation_configuration_builders" components.

        variables: dict = {
            "success_ratio": 7.5e-1,
        }
        parameter_builders: List[ParameterBuilder] = [
            column_values_unique_mean_unexpected_value_multi_batch_parameter_builder_for_validations,
            column_values_null_mean_unexpected_value_multi_batch_parameter_builder_for_validations,
            column_values_nonnull_mean_unexpected_value_multi_batch_parameter_builder_for_validations,
        ]
        rule = Rule(
            name="column_integrity_rule",
            variables=variables,
            domain_builder=every_column_domain_builder,
            parameter_builders=parameter_builders,
            expectation_configuration_builders=None,
        )

        return rule

    @staticmethod
    def _build_numeric_columns_rule() -> Rule:
        """
        This method builds "Rule" object focused on emitting "ExpectationConfiguration" objects for numeric columns.
        """

        # Step-1: Instantiate "ColumnDomainBuilder" for selecting numeric columns (but not "ID-type" columns).

        numeric_column_type_domain_builder = ColumnDomainBuilder(
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

        # Step-2: Declare "ParameterBuilder" for every metric of interest.

        column_min_metric_multi_batch_parameter_builder_for_metrics: ParameterBuilder = (
            DataAssistant.commonly_used_parameter_builders.get_column_min_metric_multi_batch_parameter_builder()
        )
        column_max_metric_multi_batch_parameter_builder_for_metrics: ParameterBuilder = (
            DataAssistant.commonly_used_parameter_builders.get_column_max_metric_multi_batch_parameter_builder()
        )
        column_quantile_values_metric_multi_batch_parameter_builder_for_metrics: ParameterBuilder = (
            DataAssistant.commonly_used_parameter_builders.get_column_quantile_values_metric_multi_batch_parameter_builder()
        )
        column_median_metric_multi_batch_parameter_builder_for_metrics: ParameterBuilder = (
            DataAssistant.commonly_used_parameter_builders.get_column_median_metric_multi_batch_parameter_builder()
        )
        column_mean_metric_multi_batch_parameter_builder_for_metrics: ParameterBuilder = (
            DataAssistant.commonly_used_parameter_builders.get_column_mean_metric_multi_batch_parameter_builder()
        )
        column_standard_deviation_metric_multi_batch_parameter_builder_for_metrics: ParameterBuilder = (
            DataAssistant.commonly_used_parameter_builders.get_column_standard_deviation_metric_multi_batch_parameter_builder()
        )

        # Step-3: Declare "ParameterBuilder" configurations for all additional statistics needed.

        evaluation_parameter_builder_configs: Optional[List[ParameterBuilderConfig]]

        evaluation_parameter_builder_configs = [
            ParameterBuilderConfig(
                **column_min_metric_multi_batch_parameter_builder_for_metrics.to_json_dict()
            ),
        ]
        column_min_values_range_parameter_builder_for_validations: ParameterBuilder = DataAssistant.commonly_used_parameter_builders.build_numeric_metric_range_multi_batch_parameter_builder(
            metric_name=None,
            metric_value_kwargs=None,
            evaluation_parameter_builder_configs=evaluation_parameter_builder_configs,
        )

        evaluation_parameter_builder_configs = [
            ParameterBuilderConfig(
                **column_max_metric_multi_batch_parameter_builder_for_metrics.to_json_dict()
            ),
        ]
        column_max_values_range_parameter_builder_for_validations: ParameterBuilder = DataAssistant.commonly_used_parameter_builders.build_numeric_metric_range_multi_batch_parameter_builder(
            metric_name=None,
            metric_value_kwargs=None,
            evaluation_parameter_builder_configs=evaluation_parameter_builder_configs,
        )

        evaluation_parameter_builder_configs = [
            ParameterBuilderConfig(
                **column_quantile_values_metric_multi_batch_parameter_builder_for_metrics.to_json_dict()
            ),
        ]
        column_quantile_values_range_parameter_builder_for_validations: ParameterBuilder = DataAssistant.commonly_used_parameter_builders.build_numeric_metric_range_multi_batch_parameter_builder(
            metric_name=None,
            metric_value_kwargs={
                "quantiles": f"{VARIABLES_KEY}quantiles",
                "allow_relative_error": f"{VARIABLES_KEY}allow_relative_error",
            },
            evaluation_parameter_builder_configs=evaluation_parameter_builder_configs,
        )

        evaluation_parameter_builder_configs = [
            ParameterBuilderConfig(
                **column_median_metric_multi_batch_parameter_builder_for_metrics.to_json_dict()
            ),
        ]
        column_median_values_range_parameter_builder_for_validations: ParameterBuilder = DataAssistant.commonly_used_parameter_builders.build_numeric_metric_range_multi_batch_parameter_builder(
            metric_name=None,
            metric_value_kwargs=None,
            evaluation_parameter_builder_configs=evaluation_parameter_builder_configs,
        )

        evaluation_parameter_builder_configs = [
            ParameterBuilderConfig(
                **column_mean_metric_multi_batch_parameter_builder_for_metrics.to_json_dict()
            ),
        ]
        column_mean_values_range_parameter_builder_for_validations: ParameterBuilder = DataAssistant.commonly_used_parameter_builders.build_numeric_metric_range_multi_batch_parameter_builder(
            metric_name=None,
            metric_value_kwargs=None,
            evaluation_parameter_builder_configs=evaluation_parameter_builder_configs,
        )

        evaluation_parameter_builder_configs = [
            ParameterBuilderConfig(
                **column_standard_deviation_metric_multi_batch_parameter_builder_for_metrics.to_json_dict()
            ),
        ]
        column_standard_deviation_values_range_parameter_builder_for_validations: ParameterBuilder = DataAssistant.commonly_used_parameter_builders.build_numeric_metric_range_multi_batch_parameter_builder(
            metric_name=None,
            metric_value_kwargs=None,
            evaluation_parameter_builder_configs=evaluation_parameter_builder_configs,
        )

        # Step-4: Instantiate and return "Rule" object, comprised of "variables", "domain_builder", "parameter_builders", and "expectation_configuration_builders" components.

        variables: dict = {
            "mostly": 1.0,
            "strict_min": False,
            "strict_max": False,
            "quantiles": [
                0.25,
                0.5,
                0.75,
            ],
            "allow_relative_error": False,
            "false_positive_rate": 0.05,
            "estimator": "bootstrap",
            "n_resamples": 9999,
            "random_seed": None,
            "quantile_statistic_interpolation_method": "nearest",
            "quantile_bias_correction": False,
            "quantile_bias_std_error_ratio_threshold": None,
            "include_estimator_samples_histogram_in_details": False,
            "truncate_values": {
                "lower_bound": None,
                "upper_bound": None,
            },
            "round_decimals": 15,
        }
        parameter_builders: List[ParameterBuilder] = [
            column_min_values_range_parameter_builder_for_validations,
            column_max_values_range_parameter_builder_for_validations,
            column_quantile_values_range_parameter_builder_for_validations,
            column_median_values_range_parameter_builder_for_validations,
            column_mean_values_range_parameter_builder_for_validations,
            column_standard_deviation_values_range_parameter_builder_for_validations,
        ]
        rule = Rule(
            name="numeric_columns_rule",
            variables=variables,
            domain_builder=numeric_column_type_domain_builder,
            parameter_builders=parameter_builders,
            expectation_configuration_builders=None,
        )

        return rule

    @staticmethod
    def _build_datetime_columns_rule() -> Rule:
        """
        This method builds "Rule" object focused on emitting "ExpectationConfiguration" objects for datetime columns.
        """

        # Step-1: Instantiate "ColumnDomainBuilder" for selecting proper datetime columns (not "datetime-looking" text).

        datetime_column_type_domain_builder = ColumnDomainBuilder(
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

        # Step-2: Declare "ParameterBuilder" for every metric of interest.

        column_min_metric_multi_batch_parameter_builder_for_metrics: ParameterBuilder = (
            DataAssistant.commonly_used_parameter_builders.get_column_min_metric_multi_batch_parameter_builder()
        )
        column_max_metric_multi_batch_parameter_builder_for_metrics: ParameterBuilder = (
            DataAssistant.commonly_used_parameter_builders.get_column_max_metric_multi_batch_parameter_builder()
        )

        # Step-3: Declare "ParameterBuilder" configurations for all additional statistics needed.

        evaluation_parameter_builder_configs: Optional[List[ParameterBuilderConfig]]

        evaluation_parameter_builder_configs = [
            ParameterBuilderConfig(
                **column_min_metric_multi_batch_parameter_builder_for_metrics.to_json_dict()
            ),
        ]
        column_min_values_range_parameter_builder_for_validations: ParameterBuilder = DataAssistant.commonly_used_parameter_builders.build_numeric_metric_range_multi_batch_parameter_builder(
            metric_name=None,
            metric_value_kwargs=None,
            evaluation_parameter_builder_configs=evaluation_parameter_builder_configs,
        )

        evaluation_parameter_builder_configs = [
            ParameterBuilderConfig(
                **column_max_metric_multi_batch_parameter_builder_for_metrics.to_json_dict()
            ),
        ]
        column_max_values_range_parameter_builder_for_validations: ParameterBuilder = DataAssistant.commonly_used_parameter_builders.build_numeric_metric_range_multi_batch_parameter_builder(
            metric_name=None,
            metric_value_kwargs=None,
            evaluation_parameter_builder_configs=evaluation_parameter_builder_configs,
        )

        # Step-4: Instantiate and return "Rule" object, comprised of "variables", "domain_builder", "parameter_builders", and "expectation_configuration_builders" components.

        variables: dict = {
            "mostly": 1.0,
            "strict_min": False,
            "strict_max": False,
            "false_positive_rate": 0.05,
            "estimator": "bootstrap",
            "n_resamples": 9999,
            "random_seed": None,
            "quantile_statistic_interpolation_method": "nearest",
            "quantile_bias_correction": False,
            "quantile_bias_std_error_ratio_threshold": None,
            "include_estimator_samples_histogram_in_details": False,
            "truncate_values": {
                "lower_bound": None,
                "upper_bound": None,
            },
            "round_decimals": 1,
        }
        parameter_builders: List[ParameterBuilder] = [
            column_min_values_range_parameter_builder_for_validations,
            column_max_values_range_parameter_builder_for_validations,
        ]
        rule = Rule(
            name="datetime_columns_rule",
            variables=variables,
            domain_builder=datetime_column_type_domain_builder,
            parameter_builders=parameter_builders,
            expectation_configuration_builders=None,
        )

        return rule

    @staticmethod
    def _build_text_columns_rule() -> Rule:

        # Step-1: Instantiate "ColumnDomainBuilder" for selecting proper text columns.

        text_column_type_domain_builder = ColumnDomainBuilder(
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

        # Step-2: Declare "ParameterBuilder" for every metric of interest.

        column_min_length_metric_multi_batch_parameter_builder_for_metrics: ParameterBuilder = (
            DataAssistant.commonly_used_parameter_builders.get_column_min_length_metric_multi_batch_parameter_builder()
        )
        column_max_length_metric_multi_batch_parameter_builder_for_metrics: ParameterBuilder = (
            DataAssistant.commonly_used_parameter_builders.get_column_max_length_metric_multi_batch_parameter_builder()
        )

        # Step-3: Declare "ParameterBuilder" configurations for all additional statistics needed.

        evaluation_parameter_builder_configs: Optional[List[ParameterBuilderConfig]]

        evaluation_parameter_builder_configs = [
            ParameterBuilderConfig(
                **column_min_length_metric_multi_batch_parameter_builder_for_metrics.to_json_dict()
            ),
        ]
        column_min_length_range_parameter_builder_for_validations: ParameterBuilder = DataAssistant.commonly_used_parameter_builders.build_numeric_metric_range_multi_batch_parameter_builder(
            metric_name=None,
            metric_value_kwargs=None,
            evaluation_parameter_builder_configs=evaluation_parameter_builder_configs,
        )

        evaluation_parameter_builder_configs = [
            ParameterBuilderConfig(
                **column_max_length_metric_multi_batch_parameter_builder_for_metrics.to_json_dict()
            ),
        ]
        column_max_length_range_parameter_builder_for_validations: ParameterBuilder = DataAssistant.commonly_used_parameter_builders.build_numeric_metric_range_multi_batch_parameter_builder(
            metric_name=None,
            metric_value_kwargs=None,
            evaluation_parameter_builder_configs=evaluation_parameter_builder_configs,
        )

        # Step-4: Instantiate and return "Rule" object, comprised of "variables", "domain_builder", "parameter_builders", and "expectation_configuration_builders" components.

        variables: dict = {
            "mostly": 1.0,
            "strict_min": False,
            "strict_max": False,
            "false_positive_rate": 0.05,
            "estimator": "bootstrap",
            "n_resamples": 9999,
            "random_seed": None,
            "quantile_statistic_interpolation_method": "nearest",
            "quantile_bias_correction": False,
            "quantile_bias_std_error_ratio_threshold": None,
            "include_estimator_samples_histogram_in_details": False,
            "truncate_values": {
                "lower_bound": 0,
                "upper_bound": None,
            },
            "round_decimals": 0,
            "success_ratio": 7.5e-1,
        }
        parameter_builders: List[ParameterBuilder] = [
            column_min_length_range_parameter_builder_for_validations,
            column_max_length_range_parameter_builder_for_validations,
        ]
        rule = Rule(
            name="text_columns_rule",
            variables=variables,
            domain_builder=text_column_type_domain_builder,
            parameter_builders=parameter_builders,
            expectation_configuration_builders=None,
        )

        return rule

    @staticmethod
    def _build_categorical_columns_rule() -> Rule:
        """
        This method builds "Rule" object focused on emitting "ExpectationConfiguration" objects for categorical columns.
        """

        # Step-1: Instantiate "CategoricalColumnDomainBuilder" for selecting columns containing "FEW" discrete values.

        categorical_column_type_domain_builder = ColumnDomainBuilder(
            include_column_names=None,
            exclude_column_names=None,
            include_column_name_suffixes=None,
            exclude_column_name_suffixes=[
                "_id",
            ],
            semantic_type_filter_module_name=None,
            semantic_type_filter_class_name=None,
            include_semantic_types=[
                SemanticDomainTypes.LOGIC,
                SemanticDomainTypes.TEXT,
            ],
            exclude_semantic_types=[
                SemanticDomainTypes.BINARY,
                SemanticDomainTypes.CURRENCY,
                SemanticDomainTypes.IDENTIFIER,
            ],
            data_context=None,
        )

        # Step-2: Declare "ParameterBuilder" for every metric of interest.

        # Step-3: Declare "ParameterBuilder" configurations for all additional statistics needed.

        column_unique_proportion_range_parameter_builder_for_validations: ParameterBuilder = DataAssistant.commonly_used_parameter_builders.build_numeric_metric_range_multi_batch_parameter_builder(
            metric_name="column.unique_proportion",
            metric_value_kwargs=None,
        )

        # Step-4: Instantiate and return "Rule" object, comprised of "variables", "domain_builder", "parameter_builders", and "expectation_configuration_builders" components.

        variables: dict = {
            "cardinality_limit_mode": CardinalityLimitMode.FEW.name,
            "mostly": 1.0,
            "strict_min": False,
            "strict_max": False,
            "false_positive_rate": 0.05,
            "estimator": "bootstrap",
            "n_resamples": 9999,
            "random_seed": None,
            "quantile_statistic_interpolation_method": "nearest",
            "quantile_bias_correction": False,
            "quantile_bias_std_error_ratio_threshold": None,
            "include_estimator_samples_histogram_in_details": False,
            "truncate_values": {
                "lower_bound": 0.0,
                "upper_bound": None,
            },
            "round_decimals": 15,
        }
        parameter_builders: List[ParameterBuilder] = [
            column_unique_proportion_range_parameter_builder_for_validations,
        ]
        rule = Rule(
            name="categorical_columns_rule",
            variables=variables,
            domain_builder=categorical_column_type_domain_builder,
            parameter_builders=parameter_builders,
            expectation_configuration_builders=None,
        )

        return rule
