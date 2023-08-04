from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from great_expectations.core.usage_statistics.anonymizers.base import BaseAnonymizer
from great_expectations.rule_based_profiler.config.base import RuleBasedProfilerConfig
from great_expectations.rule_based_profiler.rule_based_profiler import RuleBasedProfiler
from great_expectations.util import deep_filter_properties_iterable

if TYPE_CHECKING:
    from great_expectations.core.usage_statistics.anonymizers.anonymizer import (
        Anonymizer,
    )

logger = logging.getLogger(__name__)


class ProfilerAnonymizer(BaseAnonymizer):
    def __init__(
        self,
        aggregate_anonymizer: Anonymizer,
        salt: Optional[str] = None,
    ) -> None:
        super().__init__(salt=salt)

        self._aggregate_anonymizer = aggregate_anonymizer

    def anonymize(self, obj: Optional[object] = None, **kwargs) -> Any:
        if obj and isinstance(obj, RuleBasedProfiler):
            return self._anonymize_profiler_info(**kwargs)

        return self._anonymize_profiler_run(obj=obj, **kwargs)

    def _anonymize_profiler_info(self, name: str, config: dict) -> dict:
        """Anonymize RuleBasedProfiler objs from the 'great_expectations.rule_based_profiler' module.

        Args:
            name (str): The name of the given profiler.
            config (dict): The dictionary configuration of the given profiler.

        Returns:
            An anonymized dictionary payload that obfuscates user-specific details.
        """
        anonymized_info_dict: dict = {
            "anonymized_name": self._anonymize_string(name),
        }
        self._anonymize_object_info(
            anonymized_info_dict=anonymized_info_dict,
            object_config=config,
        )
        return anonymized_info_dict

    def _anonymize_profiler_run(self, obj: object, **kwargs) -> dict:
        """
        Traverse the entire RuleBasedProfiler configuration structure (as per its formal, validated Marshmallow schema) and
        anonymize every field that can be customized by a user (public fields are recorded as their original names).
        """
        assert isinstance(
            obj, RuleBasedProfilerConfig
        ), "ProfilerAnonymizer can only handle objects of type RuleBasedProfilerConfig"
        profiler_config: RuleBasedProfilerConfig = obj

        name: str | None = profiler_config.name
        anonymized_name: Optional[str] = self._anonymize_string(name)

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
            logger.debug(f"Anonymized rule {name}")

        return anonymized_rules

    def _anonymize_rule(self, name: str, rule: dict) -> dict:
        anonymized_rule: dict = {}
        anonymized_rule["anonymized_name"] = self._anonymize_string(name)

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
        anonymized_domain_builder: dict = self._anonymize_object_info(
            object_config=domain_builder,
            anonymized_info_dict={},
            runtime_environment={
                "module_name": "great_expectations.rule_based_profiler.domain_builder"
            },
        )

        batch_request: Optional[dict] = domain_builder.get("batch_request")
        if batch_request:
            anonymized_batch_request: Optional[
                dict
            ] = self._aggregate_anonymizer.anonymize(**batch_request)
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
        anonymized_parameter_builder: dict = self._anonymize_object_info(
            object_config=parameter_builder,
            anonymized_info_dict={},
            runtime_environment={
                "module_name": "great_expectations.rule_based_profiler.parameter_builder"
            },
        )

        anonymized_parameter_builder["anonymized_name"] = self._anonymize_string(
            parameter_builder.get("name")
        )

        batch_request: Optional[dict] = parameter_builder.get("batch_request")
        if batch_request:
            anonymized_batch_request: Optional[
                dict
            ] = self._aggregate_anonymizer.anonymize(**batch_request)
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
        anonymized_expectation_configuration_builder: dict = self._anonymize_object_info(
            object_config=expectation_configuration_builder,
            anonymized_info_dict={},
            runtime_environment={
                "module_name": "great_expectations.rule_based_profiler.expectation_configuration_builder"
            },
        )

        expectation_type: Optional[str] = expectation_configuration_builder.get(
            "expectation_type"
        )
        self._anonymize_expectation(
            expectation_type, anonymized_expectation_configuration_builder
        )

        condition: Optional[str] = expectation_configuration_builder.get("condition")
        if condition:
            anonymized_expectation_configuration_builder[
                "anonymized_condition"
            ] = self._anonymize_string(condition)
            logger.debug("Anonymized condition in ExpectationConfigurationBuilder")

        return anonymized_expectation_configuration_builder

    def _anonymize_expectation(
        self, expectation_type: Optional[str], info_dict: dict
    ) -> None:
        """Anonymize Expectation objs from 'great_expectations.expectations'.

        Args:
            expectation_type (Optional[str]): The string name of the Expectation.
            info_dict (dict): A dictionary to update within this function.
        """
        if expectation_type in self.CORE_GX_EXPECTATION_TYPES:
            info_dict["expectation_type"] = expectation_type
        else:
            info_dict["anonymized_expectation_type"] = self._anonymize_string(
                expectation_type
            )

    def can_handle(self, obj: Optional[object] = None, **kwargs) -> bool:
        return obj is not None and isinstance(
            obj, (RuleBasedProfilerConfig, RuleBasedProfiler)
        )
