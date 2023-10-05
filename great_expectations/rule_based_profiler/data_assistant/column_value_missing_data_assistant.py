from typing import TYPE_CHECKING, Any, Dict, List, Optional

from great_expectations.compatibility.typing_extensions import override
from great_expectations.core.metric_function_types import (
    SummarizationMetricNameSuffixes,
)
from great_expectations.rule_based_profiler.config import ParameterBuilderConfig
from great_expectations.rule_based_profiler.data_assistant import DataAssistant
from great_expectations.rule_based_profiler.data_assistant_result import (
    ColumnValueMissingDataAssistantResult,
    DataAssistantResult,
)
from great_expectations.rule_based_profiler.domain_builder import ColumnDomainBuilder
from great_expectations.rule_based_profiler.expectation_configuration_builder import (
    DefaultExpectationConfigurationBuilder,
    ExpectationConfigurationBuilder,
)
from great_expectations.rule_based_profiler.parameter_builder import (
    UnexpectedCountStatisticsMultiBatchParameterBuilder,
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


class ColumnValueMissingDataAssistant(DataAssistant):
    """
    ColumnValueMissingDataAssistant provides dataset exploration and validation for columns with missing values.

    ColumnValueMissingDataAssistant.run() Args:
        - batch_request (BatchRequestBase or dict): The Batch Request to be passed to the Data Assistant.
        - estimation (str): One of "exact" (default) or "flag_outliers" indicating the type of data you believe the
            Batch Request to contain. Valid or trusted data should use "exact", while Expectations produced with data
            that is suspected to have quality issues may benefit from "flag_outliers".
        - include_column_names (list): A list containing the column names you wish to include.
        - exclude_column_names (list): A list containing the column names you with to exclude.
        - include_column_name_suffixes (list): A list containing the column name suffixes you wish to include.
        - exclude_column_name_suffixes (list): A list containing the column name suffixes you wish to exclude.

    ColumnValueMissingDataAssistant.run() Returns:
        ColumnValueMissingDataAssistantResult

    WARNING: ColumnValueMissingDataAssistant is experimental and may change in future releases.
    """

    __alias__: str = "missingness"

    def __init__(
        self,
        name: str,
        validator: Validator,
    ) -> None:
        super().__init__(
            name=name,
            validator=validator,
        )

    @override
    def get_variables(self) -> Optional[Dict[str, Any]]:
        """
        Returns:
            Optional "variables" configuration attribute name/value pairs (overrides), commonly-used in Builder objects.
        """
        return None

    @override
    def get_rules(self) -> Optional[List[Rule]]:
        """
        Returns:
            Optional custom list of "Rule" objects implementing particular "DataAssistant" functionality.
        """
        column_value_missing_rule: Rule = self._build_column_value_missing_rule()

        return [
            column_value_missing_rule,
        ]

    @override
    def _build_data_assistant_result(
        self, data_assistant_result: DataAssistantResult
    ) -> DataAssistantResult:
        return ColumnValueMissingDataAssistantResult(
            _batch_id_to_batch_identifier_display_name_map=data_assistant_result._batch_id_to_batch_identifier_display_name_map,
            profiler_config=data_assistant_result.profiler_config,
            profiler_execution_time=data_assistant_result.profiler_execution_time,
            rule_domain_builder_execution_time=data_assistant_result.rule_domain_builder_execution_time,
            rule_execution_time=data_assistant_result.rule_execution_time,
            rule_exception_tracebacks=data_assistant_result.rule_exception_tracebacks,
            metrics_by_domain=data_assistant_result.metrics_by_domain,
            expectation_configurations=data_assistant_result.expectation_configurations,
            citation=data_assistant_result.citation,
            _usage_statistics_handler=data_assistant_result._usage_statistics_handler,
        )

    def _build_column_value_missing_rule(self) -> Rule:
        """
        This method builds "Rule" object focused on emitting "ExpectationConfiguration" objects for columns missingness.
        """

        # Step-1: Instantiate "ColumnDomainBuilder" for selecting user-supplied columns.
        # Step-2: Declare "ParameterBuilder" for every relevant metric of interest.
        # Step-3: Declare "ParameterBuilder" for every "validation" need in "ExpectationConfigurationBuilder" objects.
        # Step-3.1: Set up "UnexpectedCountStatisticsMultiBatchParameterBuilder" to compute "condition" for emitting "ExpectationConfiguration" (based on "Domain" data).
        # Step-3.2: Set up "UnexpectedCountStatisticsMultiBatchParameterBuilder" to compute "mostly" for emitting "ExpectationConfiguration" (based on "Domain" data).
        # Step-4: Pass "validation" "ParameterBuilderConfig" objects to every "DefaultExpectationConfigurationBuilder", responsible for emitting "ExpectationConfiguration" (with specified "expectation_type").
        # Step-5: Instantiate and return "Rule" object, comprised of "variables", "domain_builder", "parameter_builders", and "expectation_configuration_builders" components.

        is_multi_batch: bool = len(self._batches or []) > 1

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

        column_values_nonnull_unexpected_count_metric_multi_batch_parameter_builder_for_metrics: ParameterBuilder = (
            DataAssistant.commonly_used_parameter_builders.get_column_values_nonnull_unexpected_count_metric_multi_batch_parameter_builder()
        )
        column_values_null_unexpected_count_metric_multi_batch_parameter_builder_for_metrics: ParameterBuilder = (
            DataAssistant.commonly_used_parameter_builders.get_column_values_null_unexpected_count_metric_multi_batch_parameter_builder()
        )

        column_values_nonnull_unexpected_count_metric_multi_batch_parameter_builder_for_evaluations: ParameterBuilder = column_values_nonnull_unexpected_count_metric_multi_batch_parameter_builder_for_metrics
        column_values_null_unexpected_count_metric_multi_batch_parameter_builder_for_evaluations: ParameterBuilder = column_values_null_unexpected_count_metric_multi_batch_parameter_builder_for_metrics
        total_count_metric_multi_batch_parameter_builder_for_evaluations: ParameterBuilder = (
            DataAssistant.commonly_used_parameter_builders.get_table_row_count_metric_multi_batch_parameter_builder()
        )

        evaluation_parameter_builder_configs: Optional[List[ParameterBuilderConfig]]

        map_metric_name: str
        mode: str

        expectation_type: str

        validation_parameter_builder_configs: Optional[List[ParameterBuilderConfig]]

        condition: str

        map_metric_name = "column_values.nonnull"
        evaluation_parameter_builder_configs = [
            ParameterBuilderConfig(
                **column_values_nonnull_unexpected_count_metric_multi_batch_parameter_builder_for_evaluations.to_json_dict()
            ),
            ParameterBuilderConfig(
                **total_count_metric_multi_batch_parameter_builder_for_evaluations.to_json_dict()
            ),
        ]

        mode = "unexpected_count_fraction_values"

        column_values_nonnull_unexpected_count_fraction_multi_batch_parameter_builder_for_metrics: ParameterBuilder = UnexpectedCountStatisticsMultiBatchParameterBuilder(
            name=f"{mode}.{map_metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}",
            unexpected_count_parameter_builder_name=column_values_nonnull_unexpected_count_metric_multi_batch_parameter_builder_for_evaluations.name,
            total_count_parameter_builder_name=total_count_metric_multi_batch_parameter_builder_for_evaluations.name,
            mode=mode,
            max_error_rate=None,
            expectation_type=None,
            evaluation_parameter_builder_configs=evaluation_parameter_builder_configs,
            data_context=None,
        )

        mode = "multi_batch" if is_multi_batch else "single_batch"

        expectation_type = "expect_column_values_to_not_be_null"

        column_values_nonnull_unexpected_count_fraction_multi_batch_parameter_builder_for_validations: ParameterBuilder = UnexpectedCountStatisticsMultiBatchParameterBuilder(
            name=f"{mode}.{map_metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}",
            unexpected_count_parameter_builder_name=column_values_nonnull_unexpected_count_metric_multi_batch_parameter_builder_for_evaluations.name,
            total_count_parameter_builder_name=total_count_metric_multi_batch_parameter_builder_for_evaluations.name,
            mode=mode,
            max_error_rate=f"{VARIABLES_KEY}max_error_rate",
            expectation_type=expectation_type,
            evaluation_parameter_builder_configs=evaluation_parameter_builder_configs,
            data_context=None,
        )

        validation_parameter_builder_configs = [
            ParameterBuilderConfig(
                **column_values_nonnull_unexpected_count_fraction_multi_batch_parameter_builder_for_validations.to_json_dict()
            ),
        ]

        condition = f"""
        ({column_values_nonnull_unexpected_count_fraction_multi_batch_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}single_batch_mode == True
        &
        ({column_values_nonnull_unexpected_count_fraction_multi_batch_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}unexpected_count_fraction_active_batch_value < {VARIABLES_KEY}max_unexpected_count_fraction))
        |
        ({column_values_nonnull_unexpected_count_fraction_multi_batch_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}single_batch_mode != True
        &
        ({column_values_nonnull_unexpected_count_fraction_multi_batch_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}error_rate < {VARIABLES_KEY}max_error_rate))
        """
        expect_column_values_to_not_be_null_expectation_configuration_builder = DefaultExpectationConfigurationBuilder(
            expectation_type=expectation_type,
            validation_parameter_builder_configs=validation_parameter_builder_configs,
            column=f"{DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}column",
            mostly=f"{column_values_nonnull_unexpected_count_fraction_multi_batch_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}mostly",
            condition=condition,
            meta={
                "profiler_details": f"{column_values_nonnull_unexpected_count_fraction_multi_batch_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}.{FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY}",
            },
        )

        map_metric_name = "column_values.null"
        evaluation_parameter_builder_configs = [
            ParameterBuilderConfig(
                **column_values_null_unexpected_count_metric_multi_batch_parameter_builder_for_evaluations.to_json_dict()
            ),
            ParameterBuilderConfig(
                **total_count_metric_multi_batch_parameter_builder_for_evaluations.to_json_dict()
            ),
        ]

        mode = "unexpected_count_fraction_values"

        column_values_null_unexpected_count_fraction_multi_batch_parameter_builder_for_metrics: ParameterBuilder = UnexpectedCountStatisticsMultiBatchParameterBuilder(
            name=f"{mode}.{map_metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}",
            unexpected_count_parameter_builder_name=column_values_null_unexpected_count_metric_multi_batch_parameter_builder_for_evaluations.name,
            total_count_parameter_builder_name=total_count_metric_multi_batch_parameter_builder_for_evaluations.name,
            mode=mode,
            max_error_rate=None,
            expectation_type=None,
            evaluation_parameter_builder_configs=evaluation_parameter_builder_configs,
            data_context=None,
        )

        mode = "multi_batch" if is_multi_batch else "single_batch"

        expectation_type = "expect_column_values_to_be_null"

        column_values_null_unexpected_count_fraction_multi_batch_parameter_builder_for_validations: ParameterBuilder = UnexpectedCountStatisticsMultiBatchParameterBuilder(
            name=f"{mode}.{map_metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}",
            unexpected_count_parameter_builder_name=column_values_null_unexpected_count_metric_multi_batch_parameter_builder_for_evaluations.name,
            total_count_parameter_builder_name=total_count_metric_multi_batch_parameter_builder_for_evaluations.name,
            mode=mode,
            max_error_rate=f"{VARIABLES_KEY}max_error_rate",
            expectation_type=expectation_type,
            evaluation_parameter_builder_configs=evaluation_parameter_builder_configs,
            data_context=None,
        )

        validation_parameter_builder_configs = [
            ParameterBuilderConfig(
                **column_values_null_unexpected_count_fraction_multi_batch_parameter_builder_for_validations.to_json_dict()
            ),
        ]

        condition = f"""\
        ({column_values_null_unexpected_count_fraction_multi_batch_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}single_batch_mode == True
        &
        ({column_values_null_unexpected_count_fraction_multi_batch_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}unexpected_count_fraction_active_batch_value < {VARIABLES_KEY}max_unexpected_count_fraction))
        |
        ({column_values_null_unexpected_count_fraction_multi_batch_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}single_batch_mode != True
        &
        ({column_values_null_unexpected_count_fraction_multi_batch_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}error_rate < {VARIABLES_KEY}max_error_rate))
        """
        expect_column_values_to_be_null_expectation_configuration_builder = DefaultExpectationConfigurationBuilder(
            expectation_type=expectation_type,
            validation_parameter_builder_configs=validation_parameter_builder_configs,
            column=f"{DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}column",
            mostly=f"{column_values_null_unexpected_count_fraction_multi_batch_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}mostly",
            condition=condition,
            meta={
                "profiler_details": f"{column_values_null_unexpected_count_fraction_multi_batch_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}.{FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY}",
            },
        )

        variables: dict = {
            "max_unexpected_count_fraction": 5.0e-1,
            "max_error_rate": 2.5e-2,  # min per-Batch Hamming expectation validation success/failure distance tolerance
        }
        parameter_builders: List[ParameterBuilder] = [
            column_values_nonnull_unexpected_count_metric_multi_batch_parameter_builder_for_metrics,
            column_values_nonnull_unexpected_count_fraction_multi_batch_parameter_builder_for_metrics,
            column_values_null_unexpected_count_metric_multi_batch_parameter_builder_for_metrics,
            column_values_null_unexpected_count_fraction_multi_batch_parameter_builder_for_metrics,
        ]
        expectation_configuration_builders: List[ExpectationConfigurationBuilder] = [
            expect_column_values_to_not_be_null_expectation_configuration_builder,
            expect_column_values_to_be_null_expectation_configuration_builder,
        ]
        rule = Rule(
            name="column_value_missing_rule",
            variables=variables,
            domain_builder=column_type_domain_builder,
            parameter_builders=parameter_builders,
            expectation_configuration_builders=expectation_configuration_builders,
        )

        return rule
