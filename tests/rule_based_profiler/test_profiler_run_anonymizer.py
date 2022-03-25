from typing import Dict

import pytest

from great_expectations.core.usage_statistics.anonymizers.profiler_anonymizer import (
    ProfilerAnonymizer,
)
from great_expectations.rule_based_profiler.config.base import RuleBasedProfilerConfig


@pytest.fixture
def profiler_anonymizer() -> ProfilerAnonymizer:
    # Standardize the salt so our tests are deterimistic
    salt: str = "00000000-0000-0000-0000-00000000a004"
    anonymizer: ProfilerAnonymizer = ProfilerAnonymizer(salt=salt)
    return anonymizer


def test_anonymize_profiler_run(
    profiler_anonymizer: ProfilerAnonymizer,
    profiler_config_with_placeholder_args: RuleBasedProfilerConfig,
) -> None:
    anonymized_result: dict = profiler_anonymizer.anonymize(
        profiler_config_with_placeholder_args
    )
    assert anonymized_result == {
        "anonymized_name": "4baf3d43f149c9f9e87d5cfe36074f49",
        "config_version": 1.0,
        "anonymized_rules": [
            {
                "anonymized_domain_builder": {"parent_class": "TableDomainBuilder"},
                "anonymized_expectation_configuration_builders": [
                    {
                        "parent_class": "DefaultExpectationConfigurationBuilder",
                        "expectation_type": "expect_column_pair_values_A_to_be_greater_than_B",
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
        "rule_count": 1,
        "variable_count": 1,
    }


def test_anonymize_profiler_run_custom_values(
    profiler_anonymizer: ProfilerAnonymizer,
    profiler_config_with_placeholder_args_custom_values: RuleBasedProfilerConfig,
) -> None:
    anonymized_result: dict = profiler_anonymizer.anonymize(
        profiler_config_with_placeholder_args_custom_values
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
    profiler_anonymizer: ProfilerAnonymizer,
    profiler_config_with_placeholder_args_multiple_rules: RuleBasedProfilerConfig,
) -> None:
    anonymized_result: dict = profiler_anonymizer.anonymize(
        profiler_config_with_placeholder_args_multiple_rules
    )
    assert anonymized_result == {
        "anonymized_name": "4baf3d43f149c9f9e87d5cfe36074f49",
        "config_version": 1.0,
        "anonymized_rules": [
            {
                "anonymized_domain_builder": {"parent_class": "TableDomainBuilder"},
                "anonymized_expectation_configuration_builders": [
                    {
                        "expectation_type": "expect_column_pair_values_A_to_be_greater_than_B",
                        "parent_class": "DefaultExpectationConfigurationBuilder",
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
                        "expectation_type": "expect_column_values_to_be_between",
                        "parent_class": "DefaultExpectationConfigurationBuilder",
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
        "rule_count": 2,
        "variable_count": 1,
    }


def test_anonymize_profiler_run_multiple_rules_custom_values(
    profiler_anonymizer: ProfilerAnonymizer,
    profiler_config_with_placeholder_args_multiple_rules_custom_values: RuleBasedProfilerConfig,
) -> None:
    anonymized_result: dict = profiler_anonymizer.anonymize(
        profiler_config_with_placeholder_args_multiple_rules_custom_values
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
    profiler_anonymizer: ProfilerAnonymizer,
    profiler_config_with_placeholder_args: RuleBasedProfilerConfig,
) -> None:
    # Add batch requests to fixture before running method
    batch_request: dict = {
        "datasource_name": "my_datasource",
        "data_connector_name": "my_basic_data_connector",
        "data_asset_name": "my_data_asset",
    }
    rules: Dict[str, dict] = profiler_config_with_placeholder_args.rules
    rule: dict = rules["rule_1"]
    rule["domain_builder"]["batch_request"] = batch_request
    rule["parameter_builders"][0]["batch_request"] = batch_request

    anonymized_result: dict = profiler_anonymizer.anonymize(
        profiler_config_with_placeholder_args
    )
    assert anonymized_result == {
        "anonymized_name": "4baf3d43f149c9f9e87d5cfe36074f49",
        "config_version": 1.0,
        "anonymized_rules": [
            {
                "anonymized_domain_builder": {
                    "anonymized_batch_request": {
                        "anonymized_batch_request_required_top_level_properties": {
                            "anonymized_data_asset_name": "eac128c5824b698c22b441ada61022d4",
                            "anonymized_data_connector_name": "123a3221fc4b65014d061cce4a71782e",
                            "anonymized_datasource_name": "df78ebde1957385a02d8736cd2c9a6d9",
                        }
                    },
                    "parent_class": "TableDomainBuilder",
                },
                "anonymized_expectation_configuration_builders": [
                    {
                        "expectation_type": "expect_column_pair_values_A_to_be_greater_than_B",
                        "parent_class": "DefaultExpectationConfigurationBuilder",
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
                            }
                        },
                        "anonymized_name": "9349ed253aba01f4ecf190af61018a11",
                        "parent_class": "MetricMultiBatchParameterBuilder",
                    }
                ],
            }
        ],
        "rule_count": 1,
        "variable_count": 1,
    }


def test_anonymize_profiler_run_with_condition_in_expectation_configuration_builder(
    profiler_anonymizer: ProfilerAnonymizer,
    profiler_config_with_placeholder_args: RuleBasedProfilerConfig,
) -> None:
    rules: Dict[str, dict] = profiler_config_with_placeholder_args.rules
    expectation_configuration_builder: dict = rules["rule_1"][
        "expectation_configuration_builders"
    ][0]
    expectation_configuration_builder["condition"] = "my_condition"

    anonymized_result: dict = profiler_anonymizer.anonymize(
        profiler_config_with_placeholder_args
    )
    assert anonymized_result == {
        "anonymized_name": "4baf3d43f149c9f9e87d5cfe36074f49",
        "config_version": 1.0,
        "anonymized_rules": [
            {
                "anonymized_domain_builder": {"parent_class": "TableDomainBuilder"},
                "anonymized_expectation_configuration_builders": [
                    {
                        "anonymized_condition": "553b1c035d9b602798d64d23d63abd32",
                        "expectation_type": "expect_column_pair_values_A_to_be_greater_than_B",
                        "parent_class": "DefaultExpectationConfigurationBuilder",
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
        "rule_count": 1,
        "variable_count": 1,
    }
