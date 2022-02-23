from typing import Dict

import pytest

from great_expectations.core.batch import BatchRequest
from great_expectations.core.usage_statistics.anonymizers.profiler_run_anonymizer import (
    ProfilerRunAnonymizer,
)
from great_expectations.rule_based_profiler.rule_based_profiler import RuleBasedProfiler


@pytest.fixture
def profiler_run_anonymizer() -> ProfilerRunAnonymizer:
    # Standardize the salt so our tests are deterinistic
    salt: str = "00000000-0000-0000-0000-00000000a004"
    anonymizer: ProfilerRunAnonymizer = ProfilerRunAnonymizer(salt=salt)
    return anonymizer


@pytest.fixture
def usage_stats_profiler_config() -> dict:
    config: dict = {
        "name": "my_profiler",
        "config_version": 1.0,
        "rules": {
            "rule_1": {
                "domain_builder": {"class_name": "TableDomainBuilder"},
                "expectation_configuration_builders": [
                    {
                        "class_name": "DefaultExpectationConfigurationBuilder",
                        "expectation_type": "expect_column_pair_values_A_to_be_greater_than_B",
                        "meta": {"details": {"note": "Hello World"}},
                    }
                ],
                "parameter_builders": [
                    {
                        "class_name": "MetricMultiBatchParameterBuilder",
                        "metric_name": "my_metric",
                        "name": "my_parameter",
                    }
                ],
            }
        },
        "variable_count": 1,
        "rule_count": 1,
    }
    return config


@pytest.fixture
def usage_stats_profiler_config_multiple_rules(
    usage_stats_profiler_config: dict,
) -> dict:
    rule: dict = {
        "domain_builder": {"class_name": "TableDomainBuilder"},
        "expectation_configuration_builders": [
            {
                "class_name": "DefaultExpectationConfigurationBuilder",
                "expectation_type": "expect_column_values_to_be_between",
                "meta": {"details": {"note": "Here's another rule"}},
            }
        ],
        "parameter_builders": [
            {
                "class_name": "MetricMultiBatchParameterBuilder",
                "metric_name": "my_other_metric",
                "name": "my_additional_parameter",
            }
        ],
    }
    usage_stats_profiler_config["rules"]["rule_2"] = rule
    usage_stats_profiler_config["rule_count"] += 1
    return usage_stats_profiler_config


def test_anonymize_profiler_run(
    profiler_run_anonymizer: ProfilerRunAnonymizer,
    usage_stats_profiler_config: dict,
):

    anonymized_result: dict = profiler_run_anonymizer.anonymize_profiler_run(
        **usage_stats_profiler_config
    )
    assert anonymized_result == {
        "anonymized_name": "5b6c98e19e21e77191fb071bb9e80070",
        "config_version": 1.0,
        "anonymized_rules": [
            {
                "anonymized_domain_builder": {"class_name": "TableDomainBuilder"},
                "anonymized_expectation_configuration_builders": [
                    {
                        "class_name": "DefaultExpectationConfigurationBuilder",
                        "expectation_type": "expect_column_pair_values_A_to_be_greater_than_B",
                    }
                ],
                "anonymized_name": "5a83f3728393d6519a197cffdccd50ff",
                "anonymized_parameter_builders": [
                    {
                        "anonymized_name": "9349ed253aba01f4ecf190af61018a11",
                        "class_name": "MetricMultiBatchParameterBuilder",
                    }
                ],
            }
        ],
        "variable_count": 1,
        "rule_count": 1,
    }


def test_anonymize_profiler_run_multiple_rules(
    profiler_run_anonymizer: ProfilerRunAnonymizer,
    usage_stats_profiler_config_multiple_rules: dict,
):
    anonymized_result: dict = profiler_run_anonymizer.anonymize_profiler_run(
        **usage_stats_profiler_config_multiple_rules
    )
    assert anonymized_result == {
        "anonymized_name": "5b6c98e19e21e77191fb071bb9e80070",
        "anonymized_rules": [
            {
                "anonymized_domain_builder": {"class_name": "TableDomainBuilder"},
                "anonymized_expectation_configuration_builders": [
                    {
                        "class_name": "DefaultExpectationConfigurationBuilder",
                        "expectation_type": "expect_column_pair_values_A_to_be_greater_than_B",
                    }
                ],
                "anonymized_name": "5a83f3728393d6519a197cffdccd50ff",
                "anonymized_parameter_builders": [
                    {
                        "anonymized_name": "9349ed253aba01f4ecf190af61018a11",
                        "class_name": "MetricMultiBatchParameterBuilder",
                    }
                ],
            },
            {
                "anonymized_domain_builder": {"class_name": "TableDomainBuilder"},
                "anonymized_expectation_configuration_builders": [
                    {
                        "class_name": "DefaultExpectationConfigurationBuilder",
                        "expectation_type": "expect_column_values_to_be_between",
                    }
                ],
                "anonymized_name": "0bac2cecbb0cf8bb704e86710941434e",
                "anonymized_parameter_builders": [
                    {
                        "anonymized_name": "b7719efec76c6ebe30230fc1ec023beb",
                        "class_name": "MetricMultiBatchParameterBuilder",
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
    usage_stats_profiler_config: dict,
):
    # Add batch requests to fixture before running method
    batch_request: dict = {
        "datasource_name": "my_datasource",
        "data_connector_name": "my_basic_data_connector",
        "data_asset_name": "my_data_asset",
    }
    rules: Dict[str, dict] = usage_stats_profiler_config["rules"]
    rule: dict = rules["rule_1"]
    rule["domain_builder"]["batch_request"] = batch_request
    rule["parameter_builders"][0]["batch_request"] = batch_request

    anonymized_result: dict = profiler_run_anonymizer.anonymize_profiler_run(
        **usage_stats_profiler_config
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
                    "class_name": "TableDomainBuilder",
                },
                "anonymized_expectation_configuration_builders": [
                    {
                        "class_name": "DefaultExpectationConfigurationBuilder",
                        "expectation_type": "expect_column_pair_values_A_to_be_greater_than_B",
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
                        "class_name": "MetricMultiBatchParameterBuilder",
                    }
                ],
            }
        ],
        "variable_count": 1,
        "rule_count": 1,
    }


def test_anonymize_profiler_run_with_condition_in_expectation_configuration_builder(
    profiler_run_anonymizer: ProfilerRunAnonymizer,
    usage_stats_profiler_config: dict,
):
    rules: Dict[str, dict] = usage_stats_profiler_config["rules"]
    expectation_configuration_builder: dict = rules["rule_1"][
        "expectation_configuration_builders"
    ][0]
    expectation_configuration_builder["condition"] = "my_condition"

    anonymized_result: dict = profiler_run_anonymizer.anonymize_profiler_run(
        **usage_stats_profiler_config
    )
    assert anonymized_result == {
        "anonymized_name": "5b6c98e19e21e77191fb071bb9e80070",
        "anonymized_rules": [
            {
                "anonymized_domain_builder": {"class_name": "TableDomainBuilder"},
                "anonymized_expectation_configuration_builders": [
                    {
                        "anonymized_condition": "553b1c035d9b602798d64d23d63abd32",
                        "class_name": "DefaultExpectationConfigurationBuilder",
                        "expectation_type": "expect_column_pair_values_A_to_be_greater_than_B",
                    }
                ],
                "anonymized_name": "5a83f3728393d6519a197cffdccd50ff",
                "anonymized_parameter_builders": [
                    {
                        "anonymized_name": "9349ed253aba01f4ecf190af61018a11",
                        "class_name": "MetricMultiBatchParameterBuilder",
                    }
                ],
            }
        ],
        "config_version": 1.0,
        "rule_count": 1,
        "variable_count": 1,
    }


def test_resolve_config_using_acceptable_arguments(
    profiler_with_placeholder_args: RuleBasedProfiler,
):
    config: dict = ProfilerRunAnonymizer.resolve_config_using_acceptable_arguments(
        profiler=profiler_with_placeholder_args
    )

    # Ensure we have expected keys while also removing unnecessary ones
    assert all(
        attr in config
        for attr in ("name", "config_version", "rules", "variable_count", "rule_count")
    )
    assert all(
        attr not in config for attr in ("class_name", "module_name", "variables")
    )

    assert config["variable_count"] == 1 and config["rule_count"] == 1


def test_resolve_config_using_acceptable_arguments_with_runtime_overrides(
    profiler_with_placeholder_args: RuleBasedProfiler,
):
    rule_name: str = "my_rule"
    assert all(rule.name != rule_name for rule in profiler_with_placeholder_args.rules)

    rules: Dict[str, dict] = {rule_name: {"foo": "bar"}}
    config: dict = ProfilerRunAnonymizer.resolve_config_using_acceptable_arguments(
        profiler=profiler_with_placeholder_args, rules=rules
    )

    assert len(config["rules"]) == 1 and rule_name == config["rules"][0]["name"]


def test_resolve_config_using_acceptable_arguments_with_runtime_overrides_with_batch_requests(
    profiler_with_placeholder_args: RuleBasedProfiler, usage_stats_profiler_config: dict
):
    datasource_name = "my_datasource"
    data_connector_name = "my_basic_data_connector"
    data_asset_name = "my_data_asset"

    batch_request: BatchRequest = BatchRequest(
        datasource_name=datasource_name,
        data_connector_name=data_connector_name,
        data_asset_name=data_asset_name,
    )

    # Add batch requests to fixture before running method
    rules: Dict[str, dict] = usage_stats_profiler_config["rules"]
    rules["rule_1"]["domain_builder"]["batch_request"] = batch_request

    config: dict = ProfilerRunAnonymizer.resolve_config_using_acceptable_arguments(
        profiler=profiler_with_placeholder_args, rules=rules
    )

    assert all(
        attr in config
        for attr in ("name", "config_version", "rules", "variable_count", "rule_count")
    )

    domain_builder: dict = config["rules"][0]["domain_builder"]
    converted_batch_request: dict = domain_builder["batch_request"]
    assert converted_batch_request["datasource_name"] == datasource_name
    assert converted_batch_request["data_connector_name"] == data_connector_name
    assert converted_batch_request["data_asset_name"] == data_asset_name
