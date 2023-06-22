from typing import Any, Dict, List, Optional

from capitalone_dataprofiler_expectations.rule_based_profiler.data_assistant_result import (
    DataProfilerStructuredDataAssistantResult,
)
from capitalone_dataprofiler_expectations.rule_based_profiler.domain_builder.data_profiler_column_domain_builder import (
    DataProfilerColumnDomainBuilder,
)

from great_expectations.rule_based_profiler.config import ParameterBuilderConfig
from great_expectations.rule_based_profiler.data_assistant import DataAssistant
from great_expectations.rule_based_profiler.data_assistant_result import (
    DataAssistantResult,
)
from great_expectations.rule_based_profiler.domain_builder import (
    DomainBuilder,
)
from great_expectations.rule_based_profiler.expectation_configuration_builder import (
    DefaultExpectationConfigurationBuilder,
    ExpectationConfigurationBuilder,
)
from great_expectations.rule_based_profiler.parameter_builder import ParameterBuilder
from great_expectations.rule_based_profiler.parameter_container import (
    DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
    FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY,
    FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER,
    FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY,
    VARIABLES_KEY,
)
from great_expectations.rule_based_profiler.rule import Rule
from great_expectations.validator.validator import Validator


class DataProfilerStructuredDataAssistant(DataAssistant):
    """
    ProfileReportBasedColumnsDataAssistant provides dataset exploration and validation for columns of tabular data.
    """

    __alias__: str = "data_profiler"

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
        numeric_rule: Rule = self._build_numeric_rule()
        float_rule: Rule = self._build_float_rule()

        return [
            numeric_rule,
            float_rule,
        ]

    def _build_data_assistant_result(
        self, data_assistant_result: DataAssistantResult
    ) -> DataAssistantResult:
        return DataProfilerStructuredDataAssistantResult(
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
    def _build_numeric_rule() -> Rule:
        """
        This method builds "Rule" object configured to emit "ExpectationConfiguration" objects for column "Domain" type.

        This rule holds expectations which are associated with the numeric metrics in the data profiler report. There
        are additional rules which are planned to be created, such as timestamp_rule, text_rule, categorical_rule, etc.
        Currently, the numeric_rule uses ColumnDomainBuilder, so it doesn't discriminate by data type when applying the
        rule.
        """

        """
        Subject to inclusion/exclusion arguments, "DataProfilerColumnDomainBuilder" emits "Domain" object for every
        column name in profiler report; GreatExpectations "table.columns" metric is used to validate column existence.
        """
        data_profiler_column_domain_builder: DomainBuilder = (
            DataProfilerColumnDomainBuilder()
        )

        data_profiler_profile_report_metric_single_batch_parameter_builder_for_metrics: ParameterBuilder = DataAssistant.commonly_used_parameter_builders.build_metric_single_batch_parameter_builder(
            metric_name="data_profiler.column_profile_report",
            suffix=None,
            metric_domain_kwargs=DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
            metric_value_kwargs={
                "profile_path": f"{VARIABLES_KEY}profile_path",
            },
        )

        data_profiler_profile_report_metric_single_batch_parameter_builder_for_validations: ParameterBuilder = data_profiler_profile_report_metric_single_batch_parameter_builder_for_metrics

        validation_parameter_builder_configs: Optional[List[ParameterBuilderConfig]]

        validation_parameter_builder_configs = [
            ParameterBuilderConfig(
                **data_profiler_profile_report_metric_single_batch_parameter_builder_for_validations.to_json_dict(),
            ),
        ]
        expect_column_min_to_be_between_expectation_configuration_builder: ExpectationConfigurationBuilder = DefaultExpectationConfigurationBuilder(
            expectation_type="expect_column_min_to_be_between",
            validation_parameter_builder_configs=validation_parameter_builder_configs,
            column=f"{DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}column",
            min_value=f"{data_profiler_profile_report_metric_single_batch_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}.statistics.min",
            max_value=None,
            strict_min=f"{VARIABLES_KEY}strict_min",
            strict_max=None,
            meta={
                "profiler_details": f"{data_profiler_profile_report_metric_single_batch_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY}",
            },
        )

        expect_column_max_to_be_between_expectation_configuration_builder: ExpectationConfigurationBuilder = DefaultExpectationConfigurationBuilder(
            expectation_type="expect_column_max_to_be_between",
            validation_parameter_builder_configs=validation_parameter_builder_configs,
            column=f"{DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}column",
            min_value=None,
            max_value=f"{data_profiler_profile_report_metric_single_batch_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}.statistics.max",
            strict_min=None,
            strict_max=f"{VARIABLES_KEY}strict_max",
            meta={
                "profiler_details": f"{data_profiler_profile_report_metric_single_batch_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY}",
            },
        )

        expect_column_mean_to_be_between_expectation_configuration_builder: ExpectationConfigurationBuilder = DefaultExpectationConfigurationBuilder(
            expectation_type="expect_column_mean_to_be_between",
            validation_parameter_builder_configs=validation_parameter_builder_configs,
            column=f"{DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}column",
            min_value=f"{data_profiler_profile_report_metric_single_batch_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}.statistics.mean",
            max_value=f"{data_profiler_profile_report_metric_single_batch_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}.statistics.mean",
            strict_min=f"{VARIABLES_KEY}strict_min",
            strict_max=f"{VARIABLES_KEY}strict_max",
            meta={
                "profiler_details": f"{data_profiler_profile_report_metric_single_batch_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY}",
            },
        )

        expect_column_stddev_to_be_between_expectation_configuration_builder: ExpectationConfigurationBuilder = DefaultExpectationConfigurationBuilder(
            expectation_type="expect_column_stdev_to_be_between",
            validation_parameter_builder_configs=validation_parameter_builder_configs,
            column=f"{DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}column",
            min_value=f"{data_profiler_profile_report_metric_single_batch_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}.statistics.stddev",
            max_value=f"{data_profiler_profile_report_metric_single_batch_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}.statistics.stddev",
            strict_min=f"{VARIABLES_KEY}strict_min",
            strict_max=f"{VARIABLES_KEY}strict_max",
            meta={
                "profiler_details": f"{data_profiler_profile_report_metric_single_batch_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY}",
            },
        )

        variables: dict = {
            "strict_min": False,
            "strict_max": False,
            "profile_path": "default_profiler_path",
            "profile_report_filtering_key": "data_type",
            "profile_report_accepted_filtering_values": ["int", "float", "string"],
        }

        parameter_builders: List[ParameterBuilder] = [
            data_profiler_profile_report_metric_single_batch_parameter_builder_for_metrics,
        ]

        expectation_configuration_builders: List[ExpectationConfigurationBuilder] = [
            expect_column_min_to_be_between_expectation_configuration_builder,
            expect_column_max_to_be_between_expectation_configuration_builder,
            expect_column_mean_to_be_between_expectation_configuration_builder,
            expect_column_stddev_to_be_between_expectation_configuration_builder,
        ]

        rule = Rule(
            name="numeric_rule",
            variables=variables,
            domain_builder=data_profiler_column_domain_builder,
            parameter_builders=parameter_builders,
            expectation_configuration_builders=expectation_configuration_builders,
        )

        return rule

    @staticmethod
    def _build_float_rule() -> Rule:
        """
        This method builds "Rule" object configured to emit "ExpectationConfiguration" objects for column "Domain" type.

        This rule holds expectations which are associated with the float metrics in the data profiler report. There
        are additional rules which are planned to be created, such as timestamp_rule, text_rule, categorical_rule, etc.
        Currently, the float_rule uses DataProfilerColumnDomainBuilder, so it doesn't discriminate by data type when applying the
        rule.
        """
        data_profiler_column_domain_builder: DomainBuilder = (
            DataProfilerColumnDomainBuilder()
        )

        data_profiler_profile_report_metric_single_batch_parameter_builder_for_metrics: ParameterBuilder = DataAssistant.commonly_used_parameter_builders.build_metric_single_batch_parameter_builder(
            metric_name="data_profiler.column_profile_report",
            suffix=None,
            metric_domain_kwargs=DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
            metric_value_kwargs={
                "profile_path": f"{VARIABLES_KEY}profile_path",
            },
        )

        data_profiler_profile_report_metric_single_batch_parameter_builder_for_validations: ParameterBuilder = data_profiler_profile_report_metric_single_batch_parameter_builder_for_metrics

        validation_parameter_builder_configs: Optional[List[ParameterBuilderConfig]]

        validation_parameter_builder_configs = [
            ParameterBuilderConfig(
                **data_profiler_profile_report_metric_single_batch_parameter_builder_for_validations.to_json_dict(),
            ),
        ]
        expect_column_min_to_be_between_expectation_configuration_builder: ExpectationConfigurationBuilder = DefaultExpectationConfigurationBuilder(
            expectation_type="expect_column_min_to_be_between",
            validation_parameter_builder_configs=validation_parameter_builder_configs,
            column=f"{DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}column",
            min_value=f"{data_profiler_profile_report_metric_single_batch_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}.statistics.precision.min",
            max_value=None,
            strict_min=f"{VARIABLES_KEY}strict_min",
            strict_max=None,
            meta={
                "profiler_details": f"{data_profiler_profile_report_metric_single_batch_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY}",
            },
        )

        expect_column_max_to_be_between_expectation_configuration_builder: ExpectationConfigurationBuilder = DefaultExpectationConfigurationBuilder(
            expectation_type="expect_column_max_to_be_between",
            validation_parameter_builder_configs=validation_parameter_builder_configs,
            column=f"{DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}column",
            min_value=None,
            max_value=f"{data_profiler_profile_report_metric_single_batch_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}.statistics.precision.max",
            strict_min=None,
            strict_max=f"{VARIABLES_KEY}strict_max",
            meta={
                "profiler_details": f"{data_profiler_profile_report_metric_single_batch_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY}",
            },
        )

        expect_column_mean_to_be_between_expectation_configuration_builder: ExpectationConfigurationBuilder = DefaultExpectationConfigurationBuilder(
            expectation_type="expect_column_mean_to_be_between",
            validation_parameter_builder_configs=validation_parameter_builder_configs,
            column=f"{DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}column",
            min_value=f"{data_profiler_profile_report_metric_single_batch_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}.statistics.precision.mean",
            max_value=f"{data_profiler_profile_report_metric_single_batch_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}.statistics.precision.mean",
            strict_min=f"{VARIABLES_KEY}strict_min",
            strict_max=f"{VARIABLES_KEY}strict_max",
            meta={
                "profiler_details": f"{data_profiler_profile_report_metric_single_batch_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY}",
            },
        )

        expect_column_stddev_to_be_between_expectation_configuration_builder: ExpectationConfigurationBuilder = DefaultExpectationConfigurationBuilder(
            expectation_type="expect_column_stdev_to_be_between",
            validation_parameter_builder_configs=validation_parameter_builder_configs,
            column=f"{DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}column",
            min_value=f"{data_profiler_profile_report_metric_single_batch_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}.statistics.precision.std",
            max_value=f"{data_profiler_profile_report_metric_single_batch_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}.statistics.precision.std",
            strict_min=f"{VARIABLES_KEY}strict_min",
            strict_max=f"{VARIABLES_KEY}strict_max",
            meta={
                "profiler_details": f"{data_profiler_profile_report_metric_single_batch_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY}",
            },
        )

        variables: dict = {
            "strict_min": False,
            "strict_max": False,
            "profile_path": "default_profiler_path",
            "profile_report_filtering_key": "data_type",
            "profile_report_accepted_filtering_values": ["float"],
        }

        parameter_builders: List[ParameterBuilder] = [
            data_profiler_profile_report_metric_single_batch_parameter_builder_for_metrics,
        ]

        expectation_configuration_builders: List[ExpectationConfigurationBuilder] = [
            expect_column_min_to_be_between_expectation_configuration_builder,
            expect_column_max_to_be_between_expectation_configuration_builder,
            expect_column_mean_to_be_between_expectation_configuration_builder,
            expect_column_stddev_to_be_between_expectation_configuration_builder,
        ]

        rule = Rule(
            name="float_rule",
            variables=variables,
            domain_builder=data_profiler_column_domain_builder,
            parameter_builders=parameter_builders,
            expectation_configuration_builders=expectation_configuration_builders,
        )

        return rule
