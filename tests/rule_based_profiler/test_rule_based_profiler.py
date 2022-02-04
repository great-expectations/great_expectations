from typing import Any, Dict, List, Optional

from great_expectations.rule_based_profiler.rule import Rule
from great_expectations.rule_based_profiler.types import ParameterContainer
from great_expectations.util import deep_filter_properties_iterable


def test_reconcile_profiler_variables_no_overrides(
    profiler_with_placeholder_args,
    variables_multi_part_name_parameter_container,
):
    variables: Dict[str, Any] = {}
    effective_variables: Optional[
        ParameterContainer
    ] = profiler_with_placeholder_args.reconcile_profiler_variables(variables=variables)
    assert effective_variables == variables_multi_part_name_parameter_container


def test_reconcile_profiler_variables_with_overrides(
    profiler_with_placeholder_args,
):
    variables: Dict[str, Any] = {
        "false_positive_threshold": 2.0e-2,
        "sampling_method": "bootstrap",
        "mostly": 8.0e-1,
    }
    effective_variables: Optional[
        ParameterContainer
    ] = profiler_with_placeholder_args.reconcile_profiler_variables(variables=variables)
    assert effective_variables.to_dict()["parameter_nodes"]["variables"][
        "variables"
    ] == {
        "false_positive_threshold": 2.0e-2,
        "sampling_method": "bootstrap",
        "mostly": 8.0e-1,
    }


def test_reconcile_profiler_rules_no_overrides(
    profiler_with_placeholder_args,
):
    rules: Dict[str, Dict[str, Any]] = {}

    effective_rules: List[
        Rule
    ] = profiler_with_placeholder_args.reconcile_profiler_rules(rules=rules)
    assert effective_rules == profiler_with_placeholder_args.rules


def test_reconcile_profiler_rules_new_rule_override(
    profiler_with_placeholder_args,
):
    rules: Dict[str, Dict[str, Any]] = {
        "rule_0": {
            "domain_builder": {
                "class_name": "ColumnDomainBuilder",
                "module_name": "great_expectations.rule_based_profiler.domain_builder",
            },
            "parameter_builders": [
                {
                    "class_name": "MetricMultiBatchParameterBuilder",
                    "module_name": "great_expectations.rule_based_profiler.parameter_builder",
                    "name": "my_parameter",
                    "metric_name": "my_metric",
                },
                {
                    "class_name": "NumericMetricRangeMultiBatchParameterBuilder",
                    "module_name": "great_expectations.rule_based_profiler.parameter_builder",
                    "name": "my_other_parameter",
                    "metric_name": "my_other_metric",
                },
            ],
            "expectation_configuration_builders": [
                {
                    "class_name": "DefaultExpectationConfigurationBuilder",
                    "module_name": "great_expectations.rule_based_profiler.expectation_configuration_builder",
                    "expectation_type": "expect_column_pair_values_A_to_be_greater_than_B",
                },
                {
                    "class_name": "DefaultExpectationConfigurationBuilder",
                    "module_name": "great_expectations.rule_based_profiler.expectation_configuration_builder",
                    "expectation_type": "expect_column_min_to_be_between",
                },
            ],
        },
    }

    expected_rules: List[dict] = [
        {
            "name": "rule_0",
            "domain_builder": {},
            "parameter_builders": [
                {
                    "name": "my_parameter",
                    "metric_name": "my_metric",
                    "enforce_numeric_metric": False,
                    "replace_nan_with_zero": False,
                },
                {
                    "name": "my_other_parameter",
                    "metric_name": "my_other_metric",
                    "sampling_method": "bootstrap",
                    "enforce_numeric_metric": True,
                    "replace_nan_with_zero": True,
                    "false_positive_rate": 0.05,
                    "truncate_values": {},
                },
            ],
            "expectation_configuration_builders": [
                {
                    "expectation_type": "expect_column_pair_values_A_to_be_greater_than_B",
                },
                {
                    "expectation_type": "expect_column_min_to_be_between",
                },
            ],
        },
        {
            "name": "rule_1",
            "domain_builder": {},
            "parameter_builders": [
                {
                    "name": "my_parameter",
                    "metric_name": "my_metric",
                    "enforce_numeric_metric": False,
                    "replace_nan_with_zero": False,
                },
            ],
            "expectation_configuration_builders": [
                {
                    "expectation_type": "expect_column_pair_values_A_to_be_greater_than_B",
                },
            ],
        },
    ]

    effective_rules: List[
        Rule
    ] = profiler_with_placeholder_args.reconcile_profiler_rules(rules=rules)

    rule: Rule
    effective_rule_configs_actual: dict = {
        rule.name: rule.to_json_dict() for rule in effective_rules
    }
    deep_filter_properties_iterable(effective_rule_configs_actual, inplace=True)

    rule_config: dict
    effective_rule_configs_expected: dict = {
        rule_config["name"]: rule_config for rule_config in expected_rules
    }

    assert effective_rule_configs_actual == effective_rule_configs_expected


def test_reconcile_profiler_rules_existing_rule_domain_builder_override(
    profiler_with_placeholder_args,
):
    rules: Dict[str, Dict[str, Any]] = {
        "rule_1": {
            "domain_builder": {
                "class_name": "SimpleColumnSuffixDomainBuilder",
                "module_name": "great_expectations.rule_based_profiler.domain_builder",
                "column_name_suffixes": [
                    "_ts",
                ],
            },
        },
    }

    expected_rules: List[dict] = [
        {
            "name": "rule_1",
            "domain_builder": {
                "column_name_suffixes": [
                    "_ts",
                ],
            },
            "parameter_builders": [
                {
                    "name": "my_parameter",
                    "metric_name": "my_metric",
                    "enforce_numeric_metric": False,
                    "replace_nan_with_zero": False,
                },
            ],
            "expectation_configuration_builders": [
                {
                    "expectation_type": "expect_column_pair_values_A_to_be_greater_than_B",
                },
            ],
        },
    ]

    effective_rules: List[
        Rule
    ] = profiler_with_placeholder_args.reconcile_profiler_rules(rules=rules)

    rule: Rule
    effective_rule_configs_actual: dict = {
        rule.name: rule.to_json_dict() for rule in effective_rules
    }
    deep_filter_properties_iterable(effective_rule_configs_actual, inplace=True)

    rule_config: dict
    effective_rule_configs_expected: dict = {
        rule_config["name"]: rule_config for rule_config in expected_rules
    }

    assert effective_rule_configs_actual == effective_rule_configs_expected


def test_reconcile_profiler_rules_existing_rule_parameter_builder_overrides(
    profiler_with_placeholder_args,
):
    rules: Dict[str, Dict[str, Any]] = {
        "rule_1": {
            "parameter_builders": [
                {
                    "class_name": "MetricMultiBatchParameterBuilder",
                    "module_name": "great_expectations.rule_based_profiler.parameter_builder",
                    "name": "my_parameter",
                    "metric_name": "my_special_metric",
                    "enforce_numeric_metric": True,
                    "replace_nan_with_zero": True,
                },
                {
                    "class_name": "NumericMetricRangeMultiBatchParameterBuilder",
                    "module_name": "great_expectations.rule_based_profiler.parameter_builder",
                    "name": "my_other_parameter",
                    "metric_name": "my_other_metric",
                    "enforce_numeric_metric": True,
                    "replace_nan_with_zero": False,
                    "false_positive_rate": 0.025,
                },
            ],
        },
    }

    expected_rules: List[dict] = [
        {
            "name": "rule_1",
            "domain_builder": {},
            "parameter_builders": [
                {
                    "name": "my_parameter",
                    "metric_name": "my_special_metric",
                    "enforce_numeric_metric": True,
                    "replace_nan_with_zero": True,
                },
                {
                    "name": "my_other_parameter",
                    "metric_name": "my_other_metric",
                    "sampling_method": "bootstrap",
                    "enforce_numeric_metric": True,
                    "replace_nan_with_zero": False,
                    "false_positive_rate": 0.025,
                    "truncate_values": {},
                },
            ],
            "expectation_configuration_builders": [
                {
                    "expectation_type": "expect_column_pair_values_A_to_be_greater_than_B",
                },
            ],
        },
    ]

    effective_rules: List[
        Rule
    ] = profiler_with_placeholder_args.reconcile_profiler_rules(rules=rules)

    rule: Rule
    effective_rule_configs_actual: dict = {
        rule.name: rule.to_json_dict() for rule in effective_rules
    }
    deep_filter_properties_iterable(effective_rule_configs_actual, inplace=True)

    rule_config: dict
    effective_rule_configs_expected: dict = {
        rule_config["name"]: rule_config for rule_config in expected_rules
    }

    assert effective_rule_configs_actual == effective_rule_configs_expected


def test_reconcile_profiler_rules_existing_rule_expectation_configuration_builder_overrides(
    profiler_with_placeholder_args,
):
    rules: Dict[str, Dict[str, Any]] = {
        "rule_1": {
            "expectation_configuration_builders": [
                {
                    "class_name": "DefaultExpectationConfigurationBuilder",
                    "module_name": "great_expectations.rule_based_profiler.expectation_configuration_builder",
                    "expectation_type": "expect_column_pair_values_A_to_be_greater_than_B",
                },
                {
                    "class_name": "DefaultExpectationConfigurationBuilder",
                    "module_name": "great_expectations.rule_based_profiler.expectation_configuration_builder",
                    "expectation_type": "expect_column_min_to_be_between",
                },
            ],
        },
    }

    expected_rules: List[dict] = [
        {
            "name": "rule_1",
            "domain_builder": {},
            "parameter_builders": [
                {
                    "name": "my_parameter",
                    "metric_name": "my_metric",
                    "enforce_numeric_metric": False,
                    "replace_nan_with_zero": False,
                },
            ],
            "expectation_configuration_builders": [
                {
                    "expectation_type": "expect_column_pair_values_A_to_be_greater_than_B",
                },
                {
                    "expectation_type": "expect_column_min_to_be_between",
                },
            ],
        },
    ]

    effective_rules: List[
        Rule
    ] = profiler_with_placeholder_args.reconcile_profiler_rules(rules=rules)

    rule: Rule
    effective_rule_configs_actual: dict = {
        rule.name: rule.to_json_dict() for rule in effective_rules
    }
    deep_filter_properties_iterable(effective_rule_configs_actual, inplace=True)

    rule_config: dict
    effective_rule_configs_expected: dict = {
        rule_config["name"]: rule_config for rule_config in expected_rules
    }

    assert effective_rule_configs_actual == effective_rule_configs_expected


def test_reconcile_profiler_rules_existing_rule_full_rule_override(
    profiler_with_placeholder_args,
):
    rules: Dict[str, Dict[str, Any]] = {
        "rule_1": {
            "domain_builder": {
                "class_name": "ColumnDomainBuilder",
                "module_name": "great_expectations.rule_based_profiler.domain_builder",
            },
            "parameter_builders": [
                {
                    "class_name": "MetricMultiBatchParameterBuilder",
                    "module_name": "great_expectations.rule_based_profiler.parameter_builder",
                    "name": "my_parameter",
                    "metric_name": "my_metric",
                },
                {
                    "class_name": "NumericMetricRangeMultiBatchParameterBuilder",
                    "module_name": "great_expectations.rule_based_profiler.parameter_builder",
                    "name": "my_other_parameter",
                    "metric_name": "my_other_metric",
                },
            ],
            "expectation_configuration_builders": [
                {
                    "class_name": "DefaultExpectationConfigurationBuilder",
                    "module_name": "great_expectations.rule_based_profiler.expectation_configuration_builder",
                    "expectation_type": "expect_column_pair_values_A_to_be_greater_than_B",
                },
                {
                    "class_name": "DefaultExpectationConfigurationBuilder",
                    "module_name": "great_expectations.rule_based_profiler.expectation_configuration_builder",
                    "expectation_type": "expect_column_min_to_be_between",
                },
            ],
        },
    }

    expected_rules: List[dict] = [
        {
            "name": "rule_1",
            "domain_builder": {},
            "parameter_builders": [
                {
                    "name": "my_parameter",
                    "metric_name": "my_metric",
                    "enforce_numeric_metric": False,
                    "replace_nan_with_zero": False,
                },
                {
                    "name": "my_other_parameter",
                    "metric_name": "my_other_metric",
                    "sampling_method": "bootstrap",
                    "enforce_numeric_metric": True,
                    "replace_nan_with_zero": True,
                    "false_positive_rate": 0.05,
                    "truncate_values": {},
                },
            ],
            "expectation_configuration_builders": [
                {
                    "expectation_type": "expect_column_pair_values_A_to_be_greater_than_B",
                },
                {
                    "expectation_type": "expect_column_min_to_be_between",
                },
            ],
        },
    ]

    effective_rules: List[
        Rule
    ] = profiler_with_placeholder_args.reconcile_profiler_rules(rules=rules)

    rule: Rule
    effective_rule_configs_actual: dict = {
        rule.name: rule.to_json_dict() for rule in effective_rules
    }
    deep_filter_properties_iterable(effective_rule_configs_actual, inplace=True)

    rule_config: dict
    effective_rule_configs_expected: dict = {
        rule_config["name"]: rule_config for rule_config in expected_rules
    }

    assert effective_rule_configs_actual == effective_rule_configs_expected
