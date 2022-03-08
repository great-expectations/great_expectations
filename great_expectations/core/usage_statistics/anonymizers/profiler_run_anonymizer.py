import logging
from typing import Dict, List, Optional

from great_expectations.core.usage_statistics.anonymizers.anonymizer import Anonymizer
from great_expectations.core.usage_statistics.anonymizers.batch_request_anonymizer import (
    BatchRequestAnonymizer,
)
from great_expectations.core.usage_statistics.anonymizers.expectation_suite_anonymizer import (
    ExpectationSuiteAnonymizer,
)
from great_expectations.core.usage_statistics.util import (
    aggregate_all_core_expectation_types,
)
from great_expectations.rule_based_profiler.config.base import RuleBasedProfilerConfig
from great_expectations.rule_based_profiler.domain_builder.categorical_column_domain_builder import (
    CategoricalColumnDomainBuilder,
)
from great_expectations.rule_based_profiler.domain_builder.column_domain_builder import (
    ColumnDomainBuilder,
)
from great_expectations.rule_based_profiler.domain_builder.domain_builder import (
    DomainBuilder,
)
from great_expectations.rule_based_profiler.domain_builder.map_metric_domain_builder import (
    MapMetricDomainBuilder,
)
from great_expectations.rule_based_profiler.domain_builder.simple_column_suffix_domain_builder import (
    SimpleColumnSuffixDomainBuilder,
)
from great_expectations.rule_based_profiler.domain_builder.simple_semantic_type_domain_builder import (
    SimpleSemanticTypeColumnDomainBuilder,
)
from great_expectations.rule_based_profiler.domain_builder.table_domain_builder import (
    TableDomainBuilder,
)
from great_expectations.rule_based_profiler.expectation_configuration_builder.default_expectation_configuration_builder import (
    DefaultExpectationConfigurationBuilder,
)
from great_expectations.rule_based_profiler.expectation_configuration_builder.expectation_configuration_builder import (
    ExpectationConfigurationBuilder,
)
from great_expectations.rule_based_profiler.parameter_builder.mean_unexpected_map_metric_multi_batch_parameter_builder import (
    MeanUnexpectedMapMetricMultiBatchParameterBuilder,
)
from great_expectations.rule_based_profiler.parameter_builder.metric_multi_batch_parameter_builder import (
    MetricMultiBatchParameterBuilder,
)
from great_expectations.rule_based_profiler.parameter_builder.numeric_metric_range_multi_batch_parameter_builder import (
    NumericMetricRangeMultiBatchParameterBuilder,
)
from great_expectations.rule_based_profiler.parameter_builder.parameter_builder import (
    ParameterBuilder,
)
from great_expectations.rule_based_profiler.parameter_builder.regex_pattern_string_parameter_builder import (
    RegexPatternStringParameterBuilder,
)
from great_expectations.rule_based_profiler.parameter_builder.simple_date_format_string_parameter_builder import (
    SimpleDateFormatStringParameterBuilder,
)
from great_expectations.rule_based_profiler.parameter_builder.value_set_multi_batch_parameter_builder import (
    ValueSetMultiBatchParameterBuilder,
)
from great_expectations.util import deep_filter_properties_iterable

logger = logging.getLogger(__name__)


class ProfilerRunAnonymizer(Anonymizer):
    def __init__(self, salt: Optional[str] = None) -> None:
        super().__init__(salt=salt)

        # ordered bottom up in terms of inheritance order
        self._ge_domain_builders = [
            MapMetricDomainBuilder,
            CategoricalColumnDomainBuilder,
            SimpleColumnSuffixDomainBuilder,
            SimpleSemanticTypeColumnDomainBuilder,
            ColumnDomainBuilder,
            TableDomainBuilder,
            DomainBuilder,
        ]
        self._ge_parameter_builders = [
            MeanUnexpectedMapMetricMultiBatchParameterBuilder,
            ValueSetMultiBatchParameterBuilder,
            NumericMetricRangeMultiBatchParameterBuilder,
            MetricMultiBatchParameterBuilder,
            RegexPatternStringParameterBuilder,
            SimpleDateFormatStringParameterBuilder,
            ParameterBuilder,
        ]
        self._ge_expectation_configuration_builders = [
            DefaultExpectationConfigurationBuilder,
            ExpectationConfigurationBuilder,
        ]

        self._ge_expectation_types = aggregate_all_core_expectation_types()

        self._salt = salt
        self._batch_request_anonymizer = BatchRequestAnonymizer(self._salt)
        self._expectation_suite_anonymizer = ExpectationSuiteAnonymizer(self._salt)

    def anonymize_profiler_run(self, profiler_config: RuleBasedProfilerConfig) -> dict:
        """
        Traverse the entire RuleBasedProfiler configuration structure (as per its formal, validated Marshmallow schema) and
        anonymize every field that can be customized by a user (public fields are recorded as their original names).
        """
        name: str = profiler_config.name
        anonymized_name: Optional[str] = self.anonymize(name)

        config_version: float = profiler_config.config_version

        rules: Dict[str, dict] = profiler_config.rules
        anonymized_rules: List[dict] = self._anonymize_rules(rules=rules)
        rule_count: int = len(rules)

        variables: dict = profiler_config.variables or {}
        variable_count: int = len(variables)

        anonymized_profiler_run_properties_dict: dict = {
            "anonymized_name": anonymized_name,
            "config_version": config_version,
            "anonymized_rules": anonymized_rules,
            "rule_count": rule_count,
            "variable_count": variable_count,
        }

        deep_filter_properties_iterable(
            properties=anonymized_profiler_run_properties_dict,
            clean_falsy=True,
            inplace=True,
        )

        return anonymized_profiler_run_properties_dict

    def _anonymize_rules(self, rules: Dict[str, dict]) -> List[dict]:
        anonymized_rules: List[dict] = []

        for name, rule in rules.items():
            anonymized_rule: dict = self._anonymize_rule(name, rule)
            anonymized_rules.append(anonymized_rule)
            logger.debug("Anonymized rule %s", name)

        return anonymized_rules

    def _anonymize_rule(self, name: str, rule: dict) -> dict:
        anonymized_rule: dict = {}
        anonymized_rule["anonymized_name"] = self.anonymize(name)

        domain_builder: Optional[dict] = rule.get("domain_builder")
        if domain_builder is not None:
            anonymized_rule[
                "anonymized_domain_builder"
            ] = self._anonymize_domain_builder(domain_builder)

        parameter_builders: List[dict] = rule.get("parameter_builders", [])
        anonymized_rule[
            "anonymized_parameter_builders"
        ] = self._anonymize_parameter_builders(parameter_builders)

        expectation_configuration_builders: List[dict] = rule.get(
            "expectation_configuration_builders", []
        )
        anonymized_rule[
            "anonymized_expectation_configuration_builders"
        ] = self._anonymize_expectation_configuration_builders(
            expectation_configuration_builders
        )

        return anonymized_rule

    def _anonymize_domain_builder(self, domain_builder: dict) -> dict:
        anonymized_domain_builder: dict = self.anonymize_object_info(
            object_config=domain_builder,
            anonymized_info_dict={},
            ge_classes=self._ge_domain_builders,
            runtime_environment={
                "module_name": "great_expectations.rule_based_profiler.domain_builder"
            },
        )

        batch_request: Optional[dict] = domain_builder.get("batch_request")
        if batch_request:
            anonymized_batch_request: Optional[
                dict
            ] = self._batch_request_anonymizer.anonymize_batch_request(**batch_request)
            anonymized_domain_builder[
                "anonymized_batch_request"
            ] = anonymized_batch_request
            logger.debug("Anonymized batch request in DomainBuilder")

        return anonymized_domain_builder

    def _anonymize_parameter_builders(
        self, parameter_builders: List[dict]
    ) -> List[dict]:
        anonymized_parameter_builders: List[dict] = []

        for parameter_builder in parameter_builders:
            anonymized_parameter_builder: dict = self._anonymize_parameter_builder(
                parameter_builder
            )
            anonymized_parameter_builders.append(anonymized_parameter_builder)

        return anonymized_parameter_builders

    def _anonymize_parameter_builder(self, parameter_builder: dict) -> dict:
        anonymized_parameter_builder: dict = self.anonymize_object_info(
            object_config=parameter_builder,
            anonymized_info_dict={},
            ge_classes=self._ge_parameter_builders,
            runtime_environment={
                "module_name": "great_expectations.rule_based_profiler.parameter_builder"
            },
        )

        anonymized_parameter_builder["anonymized_name"] = self.anonymize(
            parameter_builder.get("name")
        )

        batch_request: Optional[dict] = parameter_builder.get("batch_request")
        if batch_request:
            anonymized_batch_request: Optional[
                dict
            ] = self._batch_request_anonymizer.anonymize_batch_request(**batch_request)
            anonymized_parameter_builder[
                "anonymized_batch_request"
            ] = anonymized_batch_request
            logger.debug("Anonymized batch request in ParameterBuilder")

        return anonymized_parameter_builder

    def _anonymize_expectation_configuration_builders(
        self, expectation_configuration_builders: List[dict]
    ) -> List[dict]:
        anonymized_expectation_configuration_builders: List[dict] = []

        for expectation_configuration_builder in expectation_configuration_builders:
            anonymized_expectation_configuration_builder: dict = (
                self._anonymize_expectation_configuration_builder(
                    expectation_configuration_builder
                )
            )
            anonymized_expectation_configuration_builders.append(
                anonymized_expectation_configuration_builder
            )

        return anonymized_expectation_configuration_builders

    def _anonymize_expectation_configuration_builder(
        self, expectation_configuration_builder: dict
    ) -> dict:
        anonymized_expectation_configuration_builder: dict = self.anonymize_object_info(
            object_config=expectation_configuration_builder,
            anonymized_info_dict={},
            ge_classes=self._ge_expectation_configuration_builders,
            runtime_environment={
                "module_name": "great_expectations.rule_based_profiler.expectation_configuration_builder"
            },
        )

        expectation_type: Optional[str] = expectation_configuration_builder.get(
            "expectation_type"
        )
        self._expectation_suite_anonymizer.anonymize_expectation(
            expectation_type, anonymized_expectation_configuration_builder
        )

        condition: Optional[str] = expectation_configuration_builder.get("condition")
        if condition:
            anonymized_expectation_configuration_builder[
                "anonymized_condition"
            ] = self.anonymize(condition)
            logger.debug("Anonymized condition in ExpectationConfigurationBuilder")

        return anonymized_expectation_configuration_builder
