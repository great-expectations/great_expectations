import importlib
from types import ModuleType
from typing import Any, Dict, List

import pytest

from great_expectations.core.usage_statistics.anonymizers.profiler_run_anonymizer import (
    ProfilerRunAnonymizer,
)
from great_expectations.rule_based_profiler.config.base import RuleBasedProfilerConfig


@pytest.fixture
def profiler_run_anonymizer() -> ProfilerRunAnonymizer:
    # Standardize the salt so our tests are deterinistic
    salt: str = "00000000-0000-0000-0000-00000000a004"
    anonymizer: ProfilerRunAnonymizer = ProfilerRunAnonymizer(salt=salt)
    return anonymizer


@pytest.fixture
def usage_stats_profiler_config_custom_values() -> RuleBasedProfilerConfig:
    config: RuleBasedProfilerConfig = RuleBasedProfilerConfig(
        name="my_profiler",
        config_version=1.0,
        rules={
            "rule_1": {
                "domain_builder": {"class_name": "MyCustomDomainBuilder"},
                "expectation_configuration_builders": [
                    {
                        "class_name": "MyCustomExpectationConfigurationBuilder",
                        "expectation_type": "expect_custom_expectation",
                        "meta": {"details": {"note": "My custom config"}},
                    }
                ],
                "parameter_builders": [
                    {
                        "class_name": "MyCustomParameterBuilder",
                        "metric_name": "my_metric",
                        "name": "my_parameter",
                    }
                ],
            }
        },
        variables={"my_variable": "my_value"},
    )
    return config


@pytest.fixture
def usage_stats_profiler_config_multiple_rules(
    usage_stats_profiler_config: RuleBasedProfilerConfig,
) -> RuleBasedProfilerConfig:
    rules: dict = usage_stats_profiler_config.rules
    rule: dict = {
        "domain_builder": {"class_name": "TableDomainBuilder"},
        "parameter_builders": [
            {
                "class_name": "MetricMultiBatchParameterBuilder",
                "metric_name": "my_other_metric",
                "name": "my_additional_parameter",
            }
        ],
        "expectation_configuration_builders": [
            {
                "class_name": "DefaultExpectationConfigurationBuilder",
                "expectation_type": "expect_column_values_to_be_between",
                "meta": {"details": {"note": "Here's another rule"}},
            }
        ],
    }
    rules["rule_2"] = rule
    return RuleBasedProfilerConfig(
        name=usage_stats_profiler_config.name,
        config_version=usage_stats_profiler_config.config_version,
        rules=rules,
        variables=usage_stats_profiler_config.variables,
    )


@pytest.fixture
def usage_stats_profiler_config_multiple_rules_custom_values(
    usage_stats_profiler_config_custom_values: RuleBasedProfilerConfig,
) -> RuleBasedProfilerConfig:
    rules: dict = usage_stats_profiler_config_custom_values.rules
    rule: dict = {
        "domain_builder": {"class_name": "MyAdditionalCustomDomainBuilder"},
        "parameter_builders": [
            {
                "class_name": "MyAdditionalCustomParameterBuilder",
                "metric_name": "yet_another_metric",
                "name": "yet_another_parameter",
            }
        ],
        "expectation_configuration_builders": [
            {
                "class_name": "MyAdditionalCustomExpectationConfigurationBuilder",
                "expectation_type": "expect_additional_custom_expectation",
                "meta": {"details": {"note": "Here's another rule"}},
            }
        ],
    }
    rules["rule_2"] = rule
    return RuleBasedProfilerConfig(
        name=usage_stats_profiler_config_custom_values.name,
        config_version=usage_stats_profiler_config_custom_values.config_version,
        rules=rules,
        variables=usage_stats_profiler_config_custom_values.variables,
    )


def test_anonymize_profiler_run(
    profiler_run_anonymizer: ProfilerRunAnonymizer,
    usage_stats_profiler_config: RuleBasedProfilerConfig,
) -> None:
    anonymized_result: dict = profiler_run_anonymizer.anonymize_profiler_run(
        usage_stats_profiler_config
    )
    assert anonymized_result == {
        "anonymized_name": "5b6c98e19e21e77191fb071bb9e80070",
        "config_version": 1.0,
        "anonymized_rules": [
            {
                "anonymized_domain_builder": {"parent_class": "TableDomainBuilder"},
                "anonymized_expectation_configuration_builders": [
                    {
                        "parent_class": "DefaultExpectationConfigurationBuilder",
                        "expectation_type": "expect_column_values_to_match_regex",
                    }
                ],
                "anonymized_name": "5a83f3728393d6519a197cffdccd50ff",
                "anonymized_parameter_builders": [
                    {
                        "anonymized_name": "9349ed253aba01f4ecf190af61018a11",
                        "parent_class": "MetricMultiBatchParameterBuilder",
                    }
                ],
            }
        ],
        "variable_count": 1,
        "rule_count": 1,
    }


def test_anonymize_profiler_run_custom_values(
    profiler_run_anonymizer: ProfilerRunAnonymizer,
    usage_stats_profiler_config_custom_values: RuleBasedProfilerConfig,
) -> None:
    anonymized_result: dict = profiler_run_anonymizer.anonymize_profiler_run(
        usage_stats_profiler_config_custom_values
    )
    assert anonymized_result == {
        "anonymized_name": "5b6c98e19e21e77191fb071bb9e80070",
        "anonymized_rules": [
            {
                "anonymized_domain_builder": {
                    "anonymized_class": "d2972bccf7a2a0ff91ba9369a86dcbe1",
                    "parent_class": "__not_recognized__",
                },
                "anonymized_expectation_configuration_builders": [
                    {
                        "anonymized_class": "0d70a2037f19cf1764afad97c7395167",
                        "anonymized_expectation_type": "c7c23fbf56041786bf024a2407031b27",
                        "parent_class": "__not_recognized__",
                    }
                ],
                "anonymized_name": "5a83f3728393d6519a197cffdccd50ff",
                "anonymized_parameter_builders": [
                    {
                        "anonymized_class": "c73849d7016ce7ab68e24465361a717a",
                        "anonymized_name": "9349ed253aba01f4ecf190af61018a11",
                        "parent_class": "__not_recognized__",
                    }
                ],
            }
        ],
        "config_version": 1.0,
        "rule_count": 1,
        "variable_count": 1,
    }


def test_anonymize_profiler_run_multiple_rules(
    profiler_run_anonymizer: ProfilerRunAnonymizer,
    usage_stats_profiler_config_multiple_rules: RuleBasedProfilerConfig,
) -> None:
    anonymized_result: dict = profiler_run_anonymizer.anonymize_profiler_run(
        usage_stats_profiler_config_multiple_rules
    )
    assert anonymized_result == {
        "anonymized_name": "5b6c98e19e21e77191fb071bb9e80070",
        "anonymized_rules": [
            {
                "anonymized_domain_builder": {"parent_class": "TableDomainBuilder"},
                "anonymized_expectation_configuration_builders": [
                    {
                        "parent_class": "DefaultExpectationConfigurationBuilder",
                        "expectation_type": "expect_column_values_to_match_regex",
                    }
                ],
                "anonymized_name": "5a83f3728393d6519a197cffdccd50ff",
                "anonymized_parameter_builders": [
                    {
                        "anonymized_name": "9349ed253aba01f4ecf190af61018a11",
                        "parent_class": "MetricMultiBatchParameterBuilder",
                    }
                ],
            },
            {
                "anonymized_domain_builder": {"parent_class": "TableDomainBuilder"},
                "anonymized_expectation_configuration_builders": [
                    {
                        "parent_class": "DefaultExpectationConfigurationBuilder",
                        "expectation_type": "expect_column_values_to_be_between",
                    }
                ],
                "anonymized_name": "0bac2cecbb0cf8bb704e86710941434e",
                "anonymized_parameter_builders": [
                    {
                        "anonymized_name": "b7719efec76c6ebe30230fc1ec023beb",
                        "parent_class": "MetricMultiBatchParameterBuilder",
                    }
                ],
            },
        ],
        "config_version": 1.0,
        "rule_count": 2,
        "variable_count": 1,
    }


def test_anonymize_profiler_run_multiple_rules_custom_values(
    profiler_run_anonymizer: ProfilerRunAnonymizer,
    usage_stats_profiler_config_multiple_rules_custom_values: RuleBasedProfilerConfig,
) -> None:
    anonymized_result: dict = profiler_run_anonymizer.anonymize_profiler_run(
        usage_stats_profiler_config_multiple_rules_custom_values
    )
    assert anonymized_result == {
        "anonymized_name": "5b6c98e19e21e77191fb071bb9e80070",
        "anonymized_rules": [
            {
                "anonymized_domain_builder": {
                    "anonymized_class": "d2972bccf7a2a0ff91ba9369a86dcbe1",
                    "parent_class": "__not_recognized__",
                },
                "anonymized_expectation_configuration_builders": [
                    {
                        "anonymized_class": "0d70a2037f19cf1764afad97c7395167",
                        "anonymized_expectation_type": "c7c23fbf56041786bf024a2407031b27",
                        "parent_class": "__not_recognized__",
                    }
                ],
                "anonymized_name": "5a83f3728393d6519a197cffdccd50ff",
                "anonymized_parameter_builders": [
                    {
                        "anonymized_class": "c73849d7016ce7ab68e24465361a717a",
                        "anonymized_name": "9349ed253aba01f4ecf190af61018a11",
                        "parent_class": "__not_recognized__",
                    }
                ],
            },
            {
                "anonymized_domain_builder": {
                    "anonymized_class": "df79fd715bf3ea514c3f4e3006025b24",
                    "parent_class": "__not_recognized__",
                },
                "anonymized_expectation_configuration_builders": [
                    {
                        "anonymized_class": "71128204dee66972b5cfc8851b216508",
                        "anonymized_expectation_type": "828b498d29af626836697ba1622ca234",
                        "parent_class": "__not_recognized__",
                    }
                ],
                "anonymized_name": "0bac2cecbb0cf8bb704e86710941434e",
                "anonymized_parameter_builders": [
                    {
                        "anonymized_class": "fcbd493bd096d894cf83506bc23a0729",
                        "anonymized_name": "5af4c6b6dedc5f9b3b840709e957c4ed",
                        "parent_class": "__not_recognized__",
                    }
                ],
            },
        ],
        "config_version": 1.0,
        "rule_count": 2,
        "variable_count": 1,
    }


def test_anonymize_profiler_run_with_batch_requests_in_builder_attrs(
    profiler_run_anonymizer: ProfilerRunAnonymizer,
    usage_stats_profiler_config: RuleBasedProfilerConfig,
) -> None:
    # Add batch requests to fixture before running method
    batch_request: dict = {
        "datasource_name": "my_datasource",
        "data_connector_name": "my_basic_data_connector",
        "data_asset_name": "my_data_asset",
    }
    rules: Dict[str, dict] = usage_stats_profiler_config.rules
    rule: dict = rules["rule_1"]
    rule["domain_builder"]["batch_request"] = batch_request
    rule["parameter_builders"][0]["batch_request"] = batch_request

    anonymized_result: dict = profiler_run_anonymizer.anonymize_profiler_run(
        usage_stats_profiler_config
    )
    assert anonymized_result == {
        "anonymized_name": "5b6c98e19e21e77191fb071bb9e80070",
        "config_version": 1.0,
        "anonymized_rules": [
            {
                "anonymized_domain_builder": {
                    "anonymized_batch_request": {
                        "anonymized_batch_request_required_top_level_properties": {
                            "anonymized_data_asset_name": "eac128c5824b698c22b441ada61022d4",
                            "anonymized_data_connector_name": "123a3221fc4b65014d061cce4a71782e",
                            "anonymized_datasource_name": "df78ebde1957385a02d8736cd2c9a6d9",
                        },
                        "batch_request_optional_top_level_keys": [
                            "data_connector_query"
                        ],
                    },
                    "parent_class": "TableDomainBuilder",
                },
                "anonymized_expectation_configuration_builders": [
                    {
                        "parent_class": "DefaultExpectationConfigurationBuilder",
                        "expectation_type": "expect_column_values_to_match_regex",
                    }
                ],
                "anonymized_name": "5a83f3728393d6519a197cffdccd50ff",
                "anonymized_parameter_builders": [
                    {
                        "anonymized_batch_request": {
                            "anonymized_batch_request_required_top_level_properties": {
                                "anonymized_data_asset_name": "eac128c5824b698c22b441ada61022d4",
                                "anonymized_data_connector_name": "123a3221fc4b65014d061cce4a71782e",
                                "anonymized_datasource_name": "df78ebde1957385a02d8736cd2c9a6d9",
                            },
                            "batch_request_optional_top_level_keys": [
                                "data_connector_query"
                            ],
                        },
                        "anonymized_name": "9349ed253aba01f4ecf190af61018a11",
                        "parent_class": "MetricMultiBatchParameterBuilder",
                    }
                ],
            }
        ],
        "variable_count": 1,
        "rule_count": 1,
    }


def test_anonymize_profiler_run_with_condition_in_expectation_configuration_builder(
    profiler_run_anonymizer: ProfilerRunAnonymizer,
    usage_stats_profiler_config: RuleBasedProfilerConfig,
) -> None:
    rules: Dict[str, dict] = usage_stats_profiler_config.rules
    expectation_configuration_builder: dict = rules["rule_1"][
        "expectation_configuration_builders"
    ][0]
    expectation_configuration_builder["condition"] = "my_condition"

    anonymized_result: dict = profiler_run_anonymizer.anonymize_profiler_run(
        usage_stats_profiler_config
    )
    assert anonymized_result == {
        "anonymized_name": "5b6c98e19e21e77191fb071bb9e80070",
        "anonymized_rules": [
            {
                "anonymized_domain_builder": {"parent_class": "TableDomainBuilder"},
                "anonymized_expectation_configuration_builders": [
                    {
                        "anonymized_condition": "553b1c035d9b602798d64d23d63abd32",
                        "parent_class": "DefaultExpectationConfigurationBuilder",
                        "expectation_type": "expect_column_values_to_match_regex",
                    }
                ],
                "anonymized_name": "5a83f3728393d6519a197cffdccd50ff",
                "anonymized_parameter_builders": [
                    {
                        "anonymized_name": "9349ed253aba01f4ecf190af61018a11",
                        "parent_class": "MetricMultiBatchParameterBuilder",
                    }
                ],
            }
        ],
        "config_version": 1.0,
        "rule_count": 1,
        "variable_count": 1,
    }


def test_all_builder_classes_are_recognized_as_profiler_run_anonymizer_attrs(
    profiler_run_anonymizer: ProfilerRunAnonymizer,
) -> None:
    """
    Ensure that every ParameterBuilder, DomainBuilder, and ExpectationConfigurationBuilder
    is included within the `self._ge_...` attributes of the ProfilerRunAnonymizer.

    This test is designed to catch instances where a new builder class is introduced but
    not included in the ProfilerRunAnonymizer.
    """

    def gather_all_builder_classes(module_path: str, builder_type: str) -> List[Any]:
        module: ModuleType = importlib.import_module(module_path)
        builders: List[Any] = [
            obj for name, obj in module.__dict__.items() if name.endswith(builder_type)
        ]
        return builders

    domain_builders: List[str] = gather_all_builder_classes(
        "great_expectations.rule_based_profiler.domain_builder", "DomainBuilder"
    )
    assert all(
        d in profiler_run_anonymizer._ge_domain_builders for d in domain_builders
    )

    parameter_builders: List[str] = gather_all_builder_classes(
        "great_expectations.rule_based_profiler.parameter_builder", "ParameterBuilder"
    )
    assert all(
        p in profiler_run_anonymizer._ge_parameter_builders for p in parameter_builders
    )

    expectation_configuration_builders: List[str] = gather_all_builder_classes(
        "great_expectations.rule_based_profiler.expectation_configuration_builder",
        "ExpectationConfigurationBuilder",
    )
    assert all(
        e in profiler_run_anonymizer._ge_expectation_configuration_builders
        for e in expectation_configuration_builders
    )
