from typing import TYPE_CHECKING, Any, Dict, List, Optional

from great_expectations.core.domain import SemanticDomainTypes
from great_expectations.rule_based_profiler.config import ParameterBuilderConfig
from great_expectations.rule_based_profiler.data_assistant import DataAssistant
from great_expectations.rule_based_profiler.data_assistant_result import (
    DataAssistantResult,
    NumericDataAssistantResult,
)
from great_expectations.rule_based_profiler.domain_builder import (
    ColumnDomainBuilder,
)
from great_expectations.rule_based_profiler.expectation_configuration_builder import (
    DefaultExpectationConfigurationBuilder,
    ExpectationConfigurationBuilder,
)
from great_expectations.rule_based_profiler.parameter_container import (
    DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
    FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY,
    FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER,
    FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY,
    VARIABLES_KEY,
)
from great_expectations.rule_based_profiler.rule import Rule
from great_expectations.validator.validator import Validator

if TYPE_CHECKING:
    from great_expectations.rule_based_profiler.domain_builder import DomainBuilder
    from great_expectations.rule_based_profiler.parameter_builder import (
        ParameterBuilder,
    )


class NumericDataAssistant(DataAssistant):
    """
    NumericDataAssistant provides dataset exploration and validation for numeric type columns.

    NumericDataAssistant.run() Args:
        - batch_request (BatchRequestBase or dict): The Batch Request to be passed to the Data Assistant.
        - estimation (str): One of "exact" (default) or "flag_outliers" indicating the type of data you believe the
            Batch Request to contain. Valid or trusted data should use "exact", while Expectations produced with data
            that is suspected to have quality issues may benefit from "flag_outliers".
        - include_column_names (list): A list containing the column names you wish to include.
        - exclude_column_names (list): A list containing the column names you with to exclude.
        - include_column_name_suffixes (list): A list containing the column name suffixes you wish to include.
        - exclude_column_name_suffixes (list): A list containing the column name suffixes you wish to exclude.

    NumericDataAssistant.run() Returns:
        NumericDataAssistantResult
    """

    __alias__: str = "numeric"

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
        numeric_columns_rule: Rule = self._build_numeric_columns_rule()

        return [
            numeric_columns_rule,
        ]

    def _build_data_assistant_result(
        self, data_assistant_result: DataAssistantResult
    ) -> DataAssistantResult:
        return NumericDataAssistantResult(
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
    def _build_numeric_columns_rule() -> Rule:
        """
        This method builds "Rule" object focused on emitting "ExpectationConfiguration" objects for numeric columns.
        """

        # Step-1: Instantiate "ColumnDomainBuilder" for selecting numeric columns (but not "ID-type" columns).

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

        # Step-2: Declare "ParameterBuilder" for every metric of interest.

        column_histogram_single_batch_parameter_builder_for_metrics: ParameterBuilder = DataAssistant.commonly_used_parameter_builders.build_histogram_single_batch_parameter_builder(
            name="column_values.partition",
        )
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

        # Step-3: Declare "ParameterBuilder" for every "validation" need in "ExpectationConfigurationBuilder" objects.

        evaluation_parameter_builder_configs: Optional[List[ParameterBuilderConfig]]

        evaluation_parameter_builder_configs = [
            ParameterBuilderConfig(
                **column_min_metric_multi_batch_parameter_builder_for_metrics.to_json_dict()
            ),
        ]
        column_min_values_range_parameter_builder_for_validations: ParameterBuilder = DataAssistant.commonly_used_parameter_builders.build_numeric_metric_range_multi_batch_parameter_builder(
            metric_name=None,
            suffix=None,
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
            suffix=None,
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
            suffix=None,
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
            suffix=None,
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
            suffix=None,
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
            suffix=None,
            metric_value_kwargs=None,
            evaluation_parameter_builder_configs=evaluation_parameter_builder_configs,
        )

        # Step-4: Pass "validation" "ParameterBuilderConfig" objects to every "DefaultExpectationConfigurationBuilder", responsible for emitting "ExpectationConfiguration" (with specified "expectation_type").

        validation_parameter_builder_configs: Optional[List[ParameterBuilderConfig]]

        validation_parameter_builder_configs = [
            ParameterBuilderConfig(
                **column_min_values_range_parameter_builder_for_validations.to_json_dict(),
            ),
        ]
        expect_column_min_to_be_between_expectation_configuration_builder = DefaultExpectationConfigurationBuilder(
            expectation_type="expect_column_min_to_be_between",
            validation_parameter_builder_configs=validation_parameter_builder_configs,
            column=f"{DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}column",
            min_value=f"{column_min_values_range_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}[0]",
            max_value=f"{column_min_values_range_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}[1]",
            strict_min=f"{VARIABLES_KEY}strict_min",
            strict_max=f"{VARIABLES_KEY}strict_max",
            meta={
                "profiler_details": f"{column_min_values_range_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY}",
            },
        )

        validation_parameter_builder_configs = [
            ParameterBuilderConfig(
                **column_max_values_range_parameter_builder_for_validations.to_json_dict(),
            ),
        ]
        expect_column_max_to_be_between_expectation_configuration_builder = DefaultExpectationConfigurationBuilder(
            expectation_type="expect_column_max_to_be_between",
            validation_parameter_builder_configs=validation_parameter_builder_configs,
            column=f"{DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}column",
            min_value=f"{column_max_values_range_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}[0]",
            max_value=f"{column_max_values_range_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}[1]",
            strict_min=f"{VARIABLES_KEY}strict_min",
            strict_max=f"{VARIABLES_KEY}strict_max",
            meta={
                "profiler_details": f"{column_max_values_range_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY}",
            },
        )

        validation_parameter_builder_configs = [
            ParameterBuilderConfig(
                **column_min_values_range_parameter_builder_for_validations.to_json_dict(),
            ),
            ParameterBuilderConfig(
                **column_max_values_range_parameter_builder_for_validations.to_json_dict(),
            ),
        ]
        expect_column_values_to_be_between_expectation_configuration_builder = DefaultExpectationConfigurationBuilder(
            expectation_type="expect_column_values_to_be_between",
            validation_parameter_builder_configs=validation_parameter_builder_configs,
            column=f"{DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}column",
            min_value=f"{column_min_values_range_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}[0]",
            max_value=f"{column_max_values_range_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}[1]",
            mostly=f"{VARIABLES_KEY}mostly",
            strict_min=f"{VARIABLES_KEY}strict_min",
            strict_max=f"{VARIABLES_KEY}strict_max",
            meta={
                "profiler_details": {
                    "column_min_values_range_estimator": f"{column_min_values_range_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY}",
                    "column_max_values_range_estimator": f"{column_max_values_range_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY}",
                },
            },
        )

        validation_parameter_builder_configs = [
            ParameterBuilderConfig(
                **column_quantile_values_range_parameter_builder_for_validations.to_json_dict(),
            ),
        ]
        expect_column_quantile_values_to_be_between_expectation_configuration_builder = DefaultExpectationConfigurationBuilder(
            expectation_type="expect_column_quantile_values_to_be_between",
            validation_parameter_builder_configs=validation_parameter_builder_configs,
            column=f"{DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}column",
            quantile_ranges={
                "quantiles": f"{VARIABLES_KEY}quantiles",
                "value_ranges": f"{column_quantile_values_range_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}",
            },
            allow_relative_error=f"{VARIABLES_KEY}allow_relative_error",
            meta={
                "profiler_details": f"{column_quantile_values_range_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY}",
            },
        )

        validation_parameter_builder_configs = [
            ParameterBuilderConfig(
                **column_median_values_range_parameter_builder_for_validations.to_json_dict(),
            ),
        ]
        expect_column_median_to_be_between_expectation_configuration_builder = DefaultExpectationConfigurationBuilder(
            expectation_type="expect_column_median_to_be_between",
            validation_parameter_builder_configs=validation_parameter_builder_configs,
            column=f"{DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}column",
            min_value=f"{column_median_values_range_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}[0]",
            max_value=f"{column_median_values_range_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}[1]",
            strict_min=f"{VARIABLES_KEY}strict_min",
            strict_max=f"{VARIABLES_KEY}strict_max",
            meta={
                "profiler_details": f"{column_median_values_range_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY}",
            },
        )

        validation_parameter_builder_configs = [
            ParameterBuilderConfig(
                **column_mean_values_range_parameter_builder_for_validations.to_json_dict(),
            ),
        ]
        expect_column_mean_to_be_between_expectation_configuration_builder = DefaultExpectationConfigurationBuilder(
            expectation_type="expect_column_mean_to_be_between",
            validation_parameter_builder_configs=validation_parameter_builder_configs,
            column=f"{DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}column",
            min_value=f"{column_mean_values_range_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}[0]",
            max_value=f"{column_mean_values_range_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}[1]",
            strict_min=f"{VARIABLES_KEY}strict_min",
            strict_max=f"{VARIABLES_KEY}strict_max",
            meta={
                "profiler_details": f"{column_mean_values_range_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY}",
            },
        )

        validation_parameter_builder_configs = [
            ParameterBuilderConfig(
                **column_standard_deviation_values_range_parameter_builder_for_validations.to_json_dict(),
            ),
        ]
        expect_column_stdev_to_be_between_expectation_configuration_builder = DefaultExpectationConfigurationBuilder(
            expectation_type="expect_column_stdev_to_be_between",
            validation_parameter_builder_configs=validation_parameter_builder_configs,
            column=f"{DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}column",
            min_value=f"{column_standard_deviation_values_range_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}[0]",
            max_value=f"{column_standard_deviation_values_range_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}[1]",
            strict_min=f"{VARIABLES_KEY}strict_min",
            strict_max=f"{VARIABLES_KEY}strict_max",
            meta={
                "profiler_details": f"{column_standard_deviation_values_range_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY}",
            },
        )

        # Step-5: Instantiate and return "Rule" object, comprised of "variables", "domain_builder", "parameter_builders", and "expectation_configuration_builders" components.

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
            "round_decimals": None,
        }
        parameter_builders: List[ParameterBuilder] = [
            column_histogram_single_batch_parameter_builder_for_metrics,
            column_min_metric_multi_batch_parameter_builder_for_metrics,
            column_max_metric_multi_batch_parameter_builder_for_metrics,
            column_quantile_values_metric_multi_batch_parameter_builder_for_metrics,
            column_median_metric_multi_batch_parameter_builder_for_metrics,
            column_mean_metric_multi_batch_parameter_builder_for_metrics,
            column_standard_deviation_metric_multi_batch_parameter_builder_for_metrics,
        ]
        expectation_configuration_builders: List[ExpectationConfigurationBuilder] = [
            expect_column_min_to_be_between_expectation_configuration_builder,
            expect_column_max_to_be_between_expectation_configuration_builder,
            expect_column_values_to_be_between_expectation_configuration_builder,
            expect_column_quantile_values_to_be_between_expectation_configuration_builder,
            expect_column_median_to_be_between_expectation_configuration_builder,
            expect_column_mean_to_be_between_expectation_configuration_builder,
            expect_column_stdev_to_be_between_expectation_configuration_builder,
        ]
        rule = Rule(
            name="numeric_columns_rule",
            variables=variables,
            domain_builder=numeric_column_type_domain_builder,
            parameter_builders=parameter_builders,
            expectation_configuration_builders=expectation_configuration_builders,
        )

        return rule
