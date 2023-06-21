from typing import TYPE_CHECKING, Any, Dict, List, Optional

from great_expectations.core.metric_function_types import (
    SummarizationMetricNameSuffixes,
)
from great_expectations.rule_based_profiler.config import ParameterBuilderConfig
from great_expectations.rule_based_profiler.data_assistant import DataAssistant

# TODO: <Alex>06/21/2023: This approach is currently disfavored, because it determines domains automatically.</Alex>
# TODO: <Alex>ALEX</Alex>
# from great_expectations.rule_based_profiler.data_assistant.data_assistant import (
#     build_map_metric_rule,
# )
# TODO: <Alex>ALEX</Alex>
from great_expectations.rule_based_profiler.data_assistant_result import (
    ColumnValueNonNullityDataAssistantResult,
    DataAssistantResult,
)
from great_expectations.rule_based_profiler.domain_builder import ColumnDomainBuilder
from great_expectations.rule_based_profiler.expectation_configuration_builder import (
    DefaultExpectationConfigurationBuilder,
    ExpectationConfigurationBuilder,
)
from great_expectations.rule_based_profiler.parameter_builder.unexpected_map_metric_multi_batch_parameter_builder import (
    UnexpectedMapMetricMultiBatchParameterBuilder,
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


class ColumnValueNonNullityDataAssistant(DataAssistant):
    """
    ColumnValueNonNullitylDataAssistant provides dataset exploration and validation for columns with non-NULL values.

    ColumnValueNonNullityDataAssistant.run() Args:
        - batch_request (BatchRequestBase or dict): The Batch Request to be passed to the Data Assistant.
        - estimation (str): One of "exact" (default) or "flag_outliers" indicating the type of data you believe the
            Batch Request to contain. Valid or trusted data should use "exact", while Expectations produced with data
            that is suspected to have quality issues may benefit from "flag_outliers".
        - include_column_names (list): A list containing the column names you wish to include.
        - exclude_column_names (list): A list containing the column names you with to exclude.
        - include_column_name_suffixes (list): A list containing the column name suffixes you wish to include.
        - exclude_column_name_suffixes (list): A list containing the column name suffixes you wish to exclude.

    ColumnValueNonNullityDataAssistant.run() Returns:
        ColumnValueNonNullityDataAssistantResult
    """

    __alias__: str = "column_value_nonnullity"

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

    # TODO: <Alex>06/21/2023: This approach is currently disfavored, because it determines domains automatically.</Alex>
    # TODO: <Alex>ALEX</Alex>
    # def get_rules(self) -> Optional[List[Rule]]:
    #     """
    #     Returns:
    #         Optional custom list of "Rule" objects implementing particular "DataAssistant" functionality.
    #     """
    #     total_count_metric_multi_batch_parameter_builder_for_evaluations: ParameterBuilder = (
    #         DataAssistant.commonly_used_parameter_builders.get_table_row_count_metric_multi_batch_parameter_builder()
    #     )
    #
    #     column_value_nonnullity_rule: Rule = build_map_metric_rule(
    #         data_assistant_class_name=self.__class__.__name__,
    #         rule_name="column_value_nonnullity_rule",
    #         expectation_type="expect_column_values_to_not_be_null",
    #         map_metric_name="column_values.nonnull",
    #         total_count_metric_multi_batch_parameter_builder_for_evaluations=total_count_metric_multi_batch_parameter_builder_for_evaluations,
    #         include_column_names=None,
    #         exclude_column_names=None,
    #         include_column_name_suffixes=None,
    #         exclude_column_name_suffixes=None,
    #         semantic_type_filter_module_name=None,
    #         semantic_type_filter_class_name=None,
    #         include_semantic_types=None,
    #         exclude_semantic_types=None,
    #         max_unexpected_values=0,
    #         max_unexpected_ratio=None,
    #         min_max_unexpected_values_proportion=9.75e-1,
    #     )
    #
    #     return [
    #         column_value_nonnullity_rule,
    #     ]
    # TODO: <Alex>ALEX</Alex>

    def get_rules(self) -> Optional[List[Rule]]:
        """
        Returns:
            Optional custom list of "Rule" objects implementing particular "DataAssistant" functionality.
        """
        column_value_nonnullity_rule: Rule = self._build_nonnullity_columns_rule()

        return [
            column_value_nonnullity_rule,
        ]

    def _build_data_assistant_result(
        self, data_assistant_result: DataAssistantResult
    ) -> DataAssistantResult:
        return ColumnValueNonNullityDataAssistantResult(
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
    def _build_nonnullity_columns_rule() -> Rule:
        """
        This method builds "Rule" object focused on emitting "ExpectationConfiguration" objects for nonnulity columns.
        """

        # Step-1: Instantiate "ColumnDomainBuilder" for selecting user-supplied columns.

        column_type_domain_builder: DomainBuilder = ColumnDomainBuilder(
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

        # Step-2: Declare "ParameterBuilder" for every relevant metric of interest.

        parameter_builders: List[ParameterBuilder] = []

        column_values_nonnull_unexpected_count_metric_multi_batch_parameter_builder_for_metrics: ParameterBuilder = (
            DataAssistant.commonly_used_parameter_builders.get_column_values_nonnull_unexpected_count_metric_multi_batch_parameter_builder()
        )
        parameter_builders.append(
            column_values_nonnull_unexpected_count_metric_multi_batch_parameter_builder_for_metrics
        )
        column_values_null_unexpected_count_metric_multi_batch_parameter_builder_for_metrics: ParameterBuilder = (
            DataAssistant.commonly_used_parameter_builders.get_column_values_null_unexpected_count_metric_multi_batch_parameter_builder()
        )
        parameter_builders.append(
            column_values_null_unexpected_count_metric_multi_batch_parameter_builder_for_metrics
        )

        total_count_metric_multi_batch_parameter_builder_for_evaluations: ParameterBuilder = (
            DataAssistant.commonly_used_parameter_builders.get_table_row_count_metric_multi_batch_parameter_builder()
        )
        column_values_nonnull_unexpected_count_metric_multi_batch_parameter_builder_for_evaluations: ParameterBuilder = column_values_nonnull_unexpected_count_metric_multi_batch_parameter_builder_for_metrics

        evaluation_parameter_builder_configs: Optional[List[ParameterBuilderConfig]] = [
            ParameterBuilderConfig(
                **total_count_metric_multi_batch_parameter_builder_for_evaluations.to_json_dict()
            ),
            ParameterBuilderConfig(
                **column_values_nonnull_unexpected_count_metric_multi_batch_parameter_builder_for_evaluations.to_json_dict()
            ),
        ]
        map_metric_name: str = "column_values.nonnull"

        column_values_nonnull_unexpected_count_fraction_metric_multi_batch_parameter_builder_for_metrics: ParameterBuilder = UnexpectedMapMetricMultiBatchParameterBuilder(
            name=f"{map_metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}",
            map_metric_name=map_metric_name,
            total_count_parameter_builder_name=total_count_metric_multi_batch_parameter_builder_for_evaluations.name,
            null_count_parameter_builder_name=column_values_nonnull_unexpected_count_metric_multi_batch_parameter_builder_for_evaluations.name,
            aggregation_method=None,
            false_positive_rate=None,
            quantile_statistic_interpolation_method=None,
            round_decimals=None,
            metric_domain_kwargs=DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
            metric_value_kwargs=None,
            evaluation_parameter_builder_configs=evaluation_parameter_builder_configs,
            data_context=None,
        )
        parameter_builders.append(
            column_values_nonnull_unexpected_count_fraction_metric_multi_batch_parameter_builder_for_metrics
        )

        # Step-3: Declare "ParameterBuilder" for every "validation" need in "ExpectationConfigurationBuilder" objects.

        column_values_nonnull_unexpected_count_quantile_fraction_metric_multi_batch_parameter_builder_for_validations: ParameterBuilder = UnexpectedMapMetricMultiBatchParameterBuilder(
            name=f"{map_metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}",
            map_metric_name=map_metric_name,
            total_count_parameter_builder_name=total_count_metric_multi_batch_parameter_builder_for_evaluations.name,
            null_count_parameter_builder_name=column_values_nonnull_unexpected_count_metric_multi_batch_parameter_builder_for_evaluations.name,
            aggregation_method="quantile",
            false_positive_rate=f"{VARIABLES_KEY}false_positive_rate",
            quantile_statistic_interpolation_method=f"{VARIABLES_KEY}quantile_statistic_interpolation_method",
            round_decimals=f"{VARIABLES_KEY}round_decimals",
            metric_domain_kwargs=DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
            metric_value_kwargs=None,
            evaluation_parameter_builder_configs=evaluation_parameter_builder_configs,
            data_context=None,
        )

        # Step-4: Set up "UnexpectedMapMetricMultiBatchParameterBuilder" to compute "condition" for emitting "ExpectationConfiguration" (based on "Domain" data).

        column_values_nonnull_unexpected_count_median_fraction_metric_multi_batch_parameter_builder_for_validations: ParameterBuilder = UnexpectedMapMetricMultiBatchParameterBuilder(
            name=f"{map_metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}",
            map_metric_name=map_metric_name,
            total_count_parameter_builder_name=total_count_metric_multi_batch_parameter_builder_for_evaluations.name,
            null_count_parameter_builder_name=column_values_nonnull_unexpected_count_metric_multi_batch_parameter_builder_for_evaluations.name,
            aggregation_method="median",
            false_positive_rate=None,
            quantile_statistic_interpolation_method=None,
            round_decimals=None,
            metric_domain_kwargs=DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
            metric_value_kwargs=None,
            evaluation_parameter_builder_configs=evaluation_parameter_builder_configs,
            data_context=None,
        )

        # Step-5: Pass "validation" "ParameterBuilderConfig" objects to every "DefaultExpectationConfigurationBuilder", responsible for emitting "ExpectationConfiguration" (with specified "expectation_type").

        validation_parameter_builder_configs: Optional[List[ParameterBuilderConfig]] = [
            ParameterBuilderConfig(
                **column_values_nonnull_unexpected_count_median_fraction_metric_multi_batch_parameter_builder_for_validations.to_json_dict()
            ),
            ParameterBuilderConfig(
                **column_values_nonnull_unexpected_count_quantile_fraction_metric_multi_batch_parameter_builder_for_validations.to_json_dict()
            ),
        ]
        expect_column_values_to_not_be_null_expectation_configuration_builder = DefaultExpectationConfigurationBuilder(
            expectation_type="expect_column_values_to_not_be_null",
            validation_parameter_builder_configs=validation_parameter_builder_configs,
            column=f"{DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}column",
            mostly=f"{column_values_nonnull_unexpected_count_quantile_fraction_metric_multi_batch_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}",
            condition=f"{column_values_nonnull_unexpected_count_median_fraction_metric_multi_batch_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY} <= {VARIABLES_KEY}max_nonnull_unexpected_count_median_fraction",
            meta={
                "profiler_details": {
                    "condition_estimator": f"{column_values_nonnull_unexpected_count_median_fraction_metric_multi_batch_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}.{FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY}",
                    "mostly_estimator": f"{column_values_nonnull_unexpected_count_quantile_fraction_metric_multi_batch_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}.{FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY}",
                },
            },
        )

        # Step-6: Instantiate and return "Rule" object, comprised of "variables", "domain_builder", "parameter_builders", and "expectation_configuration_builders" components.

        variables: dict = {
            "false_positive_rate": 9.8e-1,
            "quantile_statistic_interpolation_method": "auto",
            "round_decimals": 2,
            "max_nonnull_unexpected_count_median_fraction": 2.0e-1,
        }
        parameter_builders: List[ParameterBuilder] = [
            column_values_nonnull_unexpected_count_metric_multi_batch_parameter_builder_for_metrics,
            column_values_null_unexpected_count_metric_multi_batch_parameter_builder_for_metrics,
            column_values_nonnull_unexpected_count_fraction_metric_multi_batch_parameter_builder_for_metrics,
        ]
        expectation_configuration_builders: List[ExpectationConfigurationBuilder] = [
            expect_column_values_to_not_be_null_expectation_configuration_builder,
        ]
        rule = Rule(
            name="column_value_nonnullity_rule",
            variables=variables,
            domain_builder=column_type_domain_builder,
            parameter_builders=parameter_builders,
            expectation_configuration_builders=expectation_configuration_builders,
        )

        return rule
