from typing import Any, Dict, List, Optional

from contrib.capitalone_dataprofiler_expectations.rule_based_profiler.data_assistant_result.profile_numeric_columns_data_assistant_result import (
    ProfileNumericColumnsDataAssistantResult,
)
from great_expectations.rule_based_profiler.config import ParameterBuilderConfig
from great_expectations.rule_based_profiler.data_assistant import DataAssistant
from great_expectations.rule_based_profiler.data_assistant_result import (
    DataAssistantResult,
)
from great_expectations.rule_based_profiler.domain_builder import TableDomainBuilder
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


class ProfileNumericColumnsDataAssistant(DataAssistant):
    """
    ProfileNumericColumnsDataAssistant provides dataset exploration and validation for numeric columns data.
    """

    __alias__: str = "profile_numeric_columns"

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
        table_rule: Rule = self._build_table_rule()

        return [
            table_rule,
        ]

    def _build_data_assistant_result(
        self, data_assistant_result: DataAssistantResult
    ) -> DataAssistantResult:
        return ProfileNumericColumnsDataAssistantResult(
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

        data_profiler_profile_numeric_columns_diff_between_inclusive_threshold_range_metric_multi_batch_parameter_builder_for_metrics: ParameterBuilder = DataAssistant.commonly_used_parameter_builders.build_metric_single_batch_parameter_builder(
            metric_name="data_profiler.profile_numeric_columns_diff_between_inclusive_threshold_range",
            metric_domain_kwargs=DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
            metric_value_kwargs=None,
        )

        # Step-3: Declare "ParameterBuilder" for every "validation" need in "ExpectationConfigurationBuilder" objects.

        data_profiler_profile_numeric_columns_diff_between_inclusive_threshold_range_metric_multi_batch_parameter_builder_for_validations: ParameterBuilder = data_profiler_profile_numeric_columns_diff_between_inclusive_threshold_range_metric_multi_batch_parameter_builder_for_metrics

        # Step-4: Pass "validation" "ParameterBuilderConfig" objects to every "DefaultExpectationConfigurationBuilder", responsible for emitting "ExpectationConfiguration" (with specified "expectation_type").

        validation_parameter_builder_configs: Optional[List[ParameterBuilderConfig]]

        validation_parameter_builder_configs = [
            ParameterBuilderConfig(
                **data_profiler_profile_numeric_columns_diff_between_inclusive_threshold_range_metric_multi_batch_parameter_builder_for_validations.to_json_dict(),
            ),
        ]
        expect_profile_numerical_columns_diff_between_threshold_range_expectation_configuration_builder = DefaultExpectationConfigurationBuilder(
            expectation_type="expect_profile_numerical_columns_diff_between_threshold_range",
            validation_parameter_builder_configs=validation_parameter_builder_configs,
            profile_path=f"{VARIABLES_KEY}profile_path",
            limit_check_report_keys=f"{data_profiler_profile_numeric_columns_diff_between_inclusive_threshold_range_metric_multi_batch_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}",
            meta={
                "profiler_details": f"{data_profiler_profile_numeric_columns_diff_between_inclusive_threshold_range_metric_multi_batch_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY}",
            },
        )
        # Step-5: Instantiate and return "Rule" object, comprised of "variables", "domain_builder", "parameter_builders", and "expectation_configuration_builders" components.

        variables: dict = {
            "profile_path": "default_profiler_path",
        }
        parameter_builders: List[ParameterBuilder] = [
            data_profiler_profile_numeric_columns_diff_between_inclusive_threshold_range_metric_multi_batch_parameter_builder_for_metrics,
        ]
        expectation_configuration_builders: List[ExpectationConfigurationBuilder] = [
            expect_profile_numerical_columns_diff_between_threshold_range_expectation_configuration_builder,
        ]
        rule = Rule(
            name="table_rule",
            variables=variables,
            domain_builder=table_domain_builder,
            parameter_builders=parameter_builders,
            expectation_configuration_builders=expectation_configuration_builders,
        )

        return rule
