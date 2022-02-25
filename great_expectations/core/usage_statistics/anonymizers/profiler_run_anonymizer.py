import logging
from typing import Any, Dict, List, Optional, Union

from great_expectations.core.usage_statistics.anonymizers.anonymizer import Anonymizer
from great_expectations.core.usage_statistics.anonymizers.batch_request_anonymizer import (
    BatchRequestAnonymizer,
)
from great_expectations.util import deep_filter_properties_iterable

logger = logging.getLogger(__name__)


class ProfilerRunAnonymizer(Anonymizer):
    def __init__(self, salt: Optional[str] = None) -> None:
        super().__init__(salt=salt)

        self._salt = salt
        self._batch_request_anonymizer = BatchRequestAnonymizer(self._salt)

    def anonymize_profiler_run(self, *args: List[Any], **kwargs: dict) -> dict:
        """
        Traverse the entire RuleBasedProfiler configuration structure (as per its formal, validated Marshmallow schema) and
        anonymize every field that can be customized by a user (public fields are recorded as their original names).
        """

        name: Optional[str] = kwargs.get("name")
        anonymized_name: Optional[str] = self.anonymize(name)

        config_version: Union[float, str] = kwargs.get("config_version", 1.0)
        variable_count: int = kwargs.get("variable_count", 0)
        rule_count: int = kwargs.get("rule_count", 0)

        rules: Dict[str, dict] = kwargs.get("rules", {})
        anonymized_rules: List[dict] = self._anonymize_rules(rules)

        anonymized_profiler_run_properties_dict: dict = {
            "anonymized_name": anonymized_name,
            "config_version": config_version,
            "variable_count": variable_count,
            "rule_count": rule_count,
            "anonymized_rules": anonymized_rules,
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
        anonymized_domain_builder: dict = {}
        anonymized_domain_builder["class_name"] = domain_builder.get("class_name")

        batch_request: Optional[dict] = domain_builder.get("batch_request")
        if batch_request:
            anonymized_domain_builder[
                "anonymized_batch_request"
            ] = self._batch_request_anonymizer.anonymize_batch_request(
                *(), **batch_request
            )
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
        anonymized_parameter_builder: dict = {}

        anonymized_parameter_builder["anonymized_name"] = self.anonymize(
            parameter_builder.get("name")
        )

        anonymized_parameter_builder["class_name"] = parameter_builder.get("class_name")

        batch_request: Optional[dict] = parameter_builder.get("batch_request")
        if batch_request:
            anonymized_parameter_builder[
                "anonymized_batch_request"
            ] = self._batch_request_anonymizer.anonymize_batch_request(
                *(), **batch_request
            )
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
        anonymized_expectation_configuration_builder: dict = {}

        anonymized_expectation_configuration_builder[
            "class_name"
        ] = expectation_configuration_builder.get("class_name")
        anonymized_expectation_configuration_builder[
            "expectation_type"
        ] = expectation_configuration_builder.get("expectation_type")

        condition: Optional[str] = expectation_configuration_builder.get("condition")
        if condition:
            anonymized_expectation_configuration_builder[
                "anonymized_condition"
            ] = self.anonymize(condition)
            logger.debug("Anonymized condition in ExpectationConfigurationBuilder")

        return anonymized_expectation_configuration_builder

    # noinspection PyUnusedLocal,PyUnresolvedReferences
    @staticmethod
    def resolve_config_using_acceptable_arguments(
        profiler: "RuleBasedProfiler",  # noqa: F821
        variables: Optional[Dict[str, Any]] = None,
        rules: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> dict:
        runtime_config: dict = profiler.config.to_dict()

        # If applicable, override config attributes with runtime args
        if variables:
            runtime_config["variables"] = variables
        if rules:
            runtime_config["rules"] = rules

        runtime_config["variable_count"] = len(runtime_config["variables"])
        runtime_config["rule_count"] = len(runtime_config["rules"])

        for attr in ("class_name", "module_name", "variables"):
            runtime_config.pop(attr)
            logger.debug("Removed unnecessary attr %s from profiler config", attr)

        return runtime_config
