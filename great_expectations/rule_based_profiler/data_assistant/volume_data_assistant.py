from typing import Any, Dict, List, Optional

from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.rule_based_profiler.config import ParameterBuilderConfig
from great_expectations.rule_based_profiler.data_assistant import DataAssistant
from great_expectations.rule_based_profiler.domain_builder import (
    CategoricalColumnDomainBuilder,
)
from great_expectations.rule_based_profiler.expectation_configuration_builder import (
    DefaultExpectationConfigurationBuilder,
    ExpectationConfigurationBuilder,
)
from great_expectations.rule_based_profiler.helpers.cardinality_checker import (
    CardinalityLimitMode,
)
from great_expectations.rule_based_profiler.parameter_builder import ParameterBuilder
from great_expectations.rule_based_profiler.rule import Rule
from great_expectations.rule_based_profiler.types import (
    DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
    FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY,
    FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER,
    FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY,
    VARIABLES_KEY,
    Domain,
)
from great_expectations.rule_based_profiler.types.data_assistant_result import (
    DataAssistantResult,
    VolumeDataAssistantResult,
)
from great_expectations.validator.validator import Validator


class VolumeDataAssistant(DataAssistant):
    """
    VolumeDataAssistant provides exploration and validation of "Data Volume" aspects of specified data Batch objects.

    Self-Initializing Expectations relevant for assessing "Data Volume" include:
        - "expect_table_row_count_to_be_between";
        - "expect_column_unique_value_count_to_be_between";
        - Others in the future.
    """

    __alias__: str = "volume"

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
        table_row_count_metric_multi_batch_parameter_builder: ParameterBuilder = DataAssistant.commonly_used_parameter_builders.get_table_row_count_metric_multi_batch_parameter_builder(
            json_serialize=True
        )
        column_distinct_values_count_metric_multi_batch_parameter_builder: ParameterBuilder = DataAssistant.commonly_used_parameter_builders.get_column_distinct_values_count_metric_multi_batch_parameter_builder(
            json_serialize=True
        )
        return {
            Domain(domain_type=MetricDomainTypes.TABLE,): [
                table_row_count_metric_multi_batch_parameter_builder,
            ],
            Domain(domain_type=MetricDomainTypes.COLUMN,): [
                column_distinct_values_count_metric_multi_batch_parameter_builder,
            ],
        }

    @property
    def variables(self) -> Optional[Dict[str, Any]]:
        return None

    @property
    def rules(self) -> Optional[List[Rule]]:
        categorical_columns_rule: Rule = self._build_categorical_columns_rule()

        return [
            categorical_columns_rule,
        ]

    def _build_data_assistant_result(
        self, data_assistant_result: DataAssistantResult
    ) -> DataAssistantResult:
        return VolumeDataAssistantResult(
            batch_id_to_batch_identifier_display_name_map=data_assistant_result.batch_id_to_batch_identifier_display_name_map,
            profiler_config=data_assistant_result.profiler_config,
            metrics_by_domain=data_assistant_result.metrics_by_domain,
            expectation_configurations=data_assistant_result.expectation_configurations,
            citation=data_assistant_result.citation,
            execution_time=data_assistant_result.execution_time,
        )

    @staticmethod
    def _build_categorical_columns_rule() -> Rule:
        """
        This method builds "Rule" object focused on emitting "ExpectationConfiguration" objects for categorical columns.
        """
        # Step-1: Instantiate "CategoricalColumnDomainBuilder" for selecting columns containing "FEW" discrete values.

        categorical_column_type_domain_builder: CategoricalColumnDomainBuilder = (
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
                limit_mode=CardinalityLimitMode.REL_100,
                max_unique_values=None,
                max_proportion_unique=None,
                data_context=None,
            )
        )

        # Step-2: Declare "ParameterBuilder" for every metric of interest.

        column_distinct_values_count_metric_multi_batch_parameter_builder_for_metrics: ParameterBuilder = DataAssistant.commonly_used_parameter_builders.get_column_distinct_values_count_metric_multi_batch_parameter_builder(
            json_serialize=True
        )

        # Step-3: Declare "ParameterBuilder" for every "validation" need in "ExpectationConfigurationBuilder" objects.

        column_distinct_values_count_range_parameter_builder_for_validations: ParameterBuilder = DataAssistant.commonly_used_parameter_builders.build_numeric_metric_range_multi_batch_parameter_builder(
            metric_name="column.distinct_values.count",
            metric_value_kwargs=None,
            json_serialize=True,
        )

        validation_parameter_builder_configs: Optional[List[ParameterBuilderConfig]]

        # Step-4: Pass "validation" "ParameterBuilderConfig" objects to every "DefaultExpectationConfigurationBuilder", responsible for emitting "ExpectationConfiguration" (with specified "expectation_type").

        validation_parameter_builder_configs = [
            ParameterBuilderConfig(
                **column_distinct_values_count_range_parameter_builder_for_validations.to_json_dict(),
            ),
        ]
        expect_column_unique_value_count_to_be_between_expectation_configuration_builder: DefaultExpectationConfigurationBuilder = DefaultExpectationConfigurationBuilder(
            expectation_type="expect_column_unique_value_count_to_be_between",
            validation_parameter_builder_configs=validation_parameter_builder_configs,
            column=f"{DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}column",
            min_value=f"{column_distinct_values_count_range_parameter_builder_for_validations.fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}[0]",
            max_value=f"{column_distinct_values_count_range_parameter_builder_for_validations.fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}[1]",
            strict_min=f"{VARIABLES_KEY}strict_min",
            strict_max=f"{VARIABLES_KEY}strict_max",
            meta={
                "profiler_details": f"{column_distinct_values_count_range_parameter_builder_for_validations.fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY}",
            },
        )

        # Step-5: Instantiate and return "Rule" object, comprised of "variables", "domain_builder", "parameter_builders", and "expectation_configuration_builders" components.

        variables: dict = {
            "mostly": 1.0,
            "strict_min": False,
            "strict_max": False,
            "false_positive_rate": 0.05,
            "quantile_statistic_interpolation_method": "auto",
            "estimator": "bootstrap",
            "n_resamples": 9999,
            "random_seed": None,
            "include_estimator_samples_histogram_in_details": False,
            "truncate_values": {
                "lower_bound": 0.0,
                "upper_bound": None,
            },
            "round_decimals": 1,
        }
        parameter_builders: List[ParameterBuilder] = [
            column_distinct_values_count_metric_multi_batch_parameter_builder_for_metrics,
        ]
        expectation_configuration_builders: List[ExpectationConfigurationBuilder] = [
            expect_column_unique_value_count_to_be_between_expectation_configuration_builder,
        ]
        rule: Rule = Rule(
            name="categorical_columns_rule",
            variables=variables,
            domain_builder=categorical_column_type_domain_builder,
            parameter_builders=parameter_builders,
            expectation_configuration_builders=expectation_configuration_builders,
        )

        return rule
