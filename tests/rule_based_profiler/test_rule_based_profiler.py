import logging
from typing import Any, Dict, List, Optional
from unittest import mock

import pandas as pd
import pytest

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.batch import BatchRequest
from great_expectations.data_context.store.profiler_store import ProfilerStore
from great_expectations.data_context.types.resource_identifiers import (
    ConfigurationIdentifier,
    GeCloudIdentifier,
)
from great_expectations.exceptions.exceptions import InvalidConfigError
from great_expectations.rule_based_profiler import RuleBasedProfiler
from great_expectations.rule_based_profiler.config import RuleBasedProfilerConfig
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
                    "column_A": "$domain.domain_kwargs.column_A",
                    "column_B": "$domain.domain_kwargs.column_B",
                    "my_one_arg": "$parameter.my_parameter.value[0]",
                    "meta": {
                        "details": {
                            "my_parameter_estimator": "$parameter.my_parameter.details",
                            "note": "Important remarks about estimation algorithm.",
                        },
                    },
                },
                {
                    "class_name": "DefaultExpectationConfigurationBuilder",
                    "module_name": "great_expectations.rule_based_profiler.expectation_configuration_builder",
                    "expectation_type": "expect_column_min_to_be_between",
                    "column": "$domain.domain_kwargs.column",
                    "my_another_arg": "$parameter.my_other_parameter.value[0]",
                    "meta": {
                        "details": {
                            "my_other_parameter_estimator": "$parameter.my_other_parameter.details",
                            "note": "Important remarks about estimation algorithm.",
                        },
                    },
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
                    "reduce_scalar_metric": True,
                },
                {
                    "name": "my_other_parameter",
                    "metric_name": "my_other_metric",
                    "sampling_method": "bootstrap",
                    "enforce_numeric_metric": True,
                    "replace_nan_with_zero": True,
                    "reduce_scalar_metric": True,
                    "false_positive_rate": 0.05,
                    "truncate_values": {},
                },
            ],
            "expectation_configuration_builders": [
                {
                    "expectation_type": "expect_column_pair_values_A_to_be_greater_than_B",
                    "column_A": "$domain.domain_kwargs.column_A",
                    "column_B": "$domain.domain_kwargs.column_B",
                    "my_one_arg": "$parameter.my_parameter.value[0]",
                    "meta": {
                        "details": {
                            "my_parameter_estimator": "$parameter.my_parameter.details",
                            "note": "Important remarks about estimation algorithm.",
                        },
                    },
                },
                {
                    "expectation_type": "expect_column_min_to_be_between",
                    "column": "$domain.domain_kwargs.column",
                    "my_another_arg": "$parameter.my_other_parameter.value[0]",
                    "meta": {
                        "details": {
                            "my_other_parameter_estimator": "$parameter.my_other_parameter.details",
                            "note": "Important remarks about estimation algorithm.",
                        },
                    },
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
                    "reduce_scalar_metric": True,
                },
            ],
            "expectation_configuration_builders": [
                {
                    "expectation_type": "expect_column_pair_values_A_to_be_greater_than_B",
                    "column_A": "$domain.domain_kwargs.column_A",
                    "column_B": "$domain.domain_kwargs.column_B",
                    "my_arg": "$parameter.my_parameter.value[0]",
                    "my_other_arg": "$parameter.my_parameter.value[1]",
                    "meta": {
                        "details": {
                            "my_parameter_estimator": "$parameter.my_parameter.details",
                            "note": "Important remarks about estimation algorithm.",
                        },
                    },
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
                    "reduce_scalar_metric": True,
                },
            ],
            "expectation_configuration_builders": [
                {
                    "expectation_type": "expect_column_pair_values_A_to_be_greater_than_B",
                    "column_A": "$domain.domain_kwargs.column_A",
                    "column_B": "$domain.domain_kwargs.column_B",
                    "my_arg": "$parameter.my_parameter.value[0]",
                    "my_other_arg": "$parameter.my_parameter.value[1]",
                    "meta": {
                        "details": {
                            "my_parameter_estimator": "$parameter.my_parameter.details",
                            "note": "Important remarks about estimation algorithm.",
                        },
                    },
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
                    "reduce_scalar_metric": True,
                },
                {
                    "class_name": "NumericMetricRangeMultiBatchParameterBuilder",
                    "module_name": "great_expectations.rule_based_profiler.parameter_builder",
                    "name": "my_other_parameter",
                    "metric_name": "my_other_metric",
                    "enforce_numeric_metric": True,
                    "replace_nan_with_zero": False,
                    "reduce_scalar_metric": True,
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
                    "reduce_scalar_metric": True,
                },
                {
                    "name": "my_other_parameter",
                    "metric_name": "my_other_metric",
                    "sampling_method": "bootstrap",
                    "enforce_numeric_metric": True,
                    "replace_nan_with_zero": False,
                    "reduce_scalar_metric": True,
                    "false_positive_rate": 0.025,
                    "truncate_values": {},
                },
            ],
            "expectation_configuration_builders": [
                {
                    "expectation_type": "expect_column_pair_values_A_to_be_greater_than_B",
                    "column_A": "$domain.domain_kwargs.column_A",
                    "column_B": "$domain.domain_kwargs.column_B",
                    "my_arg": "$parameter.my_parameter.value[0]",
                    "my_other_arg": "$parameter.my_parameter.value[1]",
                    "meta": {
                        "details": {
                            "my_parameter_estimator": "$parameter.my_parameter.details",
                            "note": "Important remarks about estimation algorithm.",
                        },
                    },
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
                    "column_A": "$domain.domain_kwargs.column_A",
                    "column_B": "$domain.domain_kwargs.column_B",
                    "my_one_arg": "$parameter.my_parameter.value[0]",
                    "meta": {
                        "details": {
                            "my_parameter_estimator": "$parameter.my_parameter.details",
                            "note": "Important remarks about estimation algorithm.",
                        },
                    },
                },
                {
                    "class_name": "DefaultExpectationConfigurationBuilder",
                    "module_name": "great_expectations.rule_based_profiler.expectation_configuration_builder",
                    "expectation_type": "expect_column_min_to_be_between",
                    "column": "$domain.domain_kwargs.column",
                    "my_another_arg": "$parameter.my_other_parameter.value[0]",
                    "meta": {
                        "details": {
                            "my_other_parameter_estimator": "$parameter.my_other_parameter.details",
                            "note": "Important remarks about estimation algorithm.",
                        },
                    },
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
                    "reduce_scalar_metric": True,
                },
            ],
            "expectation_configuration_builders": [
                {
                    "expectation_type": "expect_column_pair_values_A_to_be_greater_than_B",
                    "column_A": "$domain.domain_kwargs.column_A",
                    "column_B": "$domain.domain_kwargs.column_B",
                    "my_arg": "$parameter.my_parameter.value[0]",
                    "my_one_arg": "$parameter.my_parameter.value[0]",
                    "my_other_arg": "$parameter.my_parameter.value[1]",
                    "meta": {
                        "details": {
                            "my_parameter_estimator": "$parameter.my_parameter.details",
                            "note": "Important remarks about estimation algorithm.",
                        },
                    },
                },
                {
                    "expectation_type": "expect_column_min_to_be_between",
                    "column": "$domain.domain_kwargs.column",
                    "my_another_arg": "$parameter.my_other_parameter.value[0]",
                    "meta": {
                        "details": {
                            "my_other_parameter_estimator": "$parameter.my_other_parameter.details",
                            "note": "Important remarks about estimation algorithm.",
                        },
                    },
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
                    "column_A": "$domain.domain_kwargs.column_A",
                    "column_B": "$domain.domain_kwargs.column_B",
                    "my_one_arg": "$parameter.my_parameter.value[0]",
                    "meta": {
                        "details": {
                            "my_parameter_estimator": "$parameter.my_parameter.details",
                            "note": "Important remarks about estimation algorithm.",
                        },
                    },
                },
                {
                    "class_name": "DefaultExpectationConfigurationBuilder",
                    "module_name": "great_expectations.rule_based_profiler.expectation_configuration_builder",
                    "expectation_type": "expect_column_min_to_be_between",
                    "column": "$domain.domain_kwargs.column",
                    "my_another_arg": "$parameter.my_other_parameter.value[0]",
                    "meta": {
                        "details": {
                            "my_other_parameter_estimator": "$parameter.my_other_parameter.details",
                            "note": "Important remarks about estimation algorithm.",
                        },
                    },
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
                    "reduce_scalar_metric": True,
                },
                {
                    "name": "my_other_parameter",
                    "metric_name": "my_other_metric",
                    "sampling_method": "bootstrap",
                    "enforce_numeric_metric": True,
                    "replace_nan_with_zero": True,
                    "reduce_scalar_metric": True,
                    "false_positive_rate": 0.05,
                    "truncate_values": {},
                },
            ],
            "expectation_configuration_builders": [
                {
                    "expectation_type": "expect_column_pair_values_A_to_be_greater_than_B",
                    "column_A": "$domain.domain_kwargs.column_A",
                    "column_B": "$domain.domain_kwargs.column_B",
                    "my_arg": "$parameter.my_parameter.value[0]",
                    "my_one_arg": "$parameter.my_parameter.value[0]",
                    "my_other_arg": "$parameter.my_parameter.value[1]",
                    "meta": {
                        "details": {
                            "my_parameter_estimator": "$parameter.my_parameter.details",
                            "note": "Important remarks about estimation algorithm.",
                        },
                    },
                },
                {
                    "expectation_type": "expect_column_min_to_be_between",
                    "column": "$domain.domain_kwargs.column",
                    "my_another_arg": "$parameter.my_other_parameter.value[0]",
                    "meta": {
                        "details": {
                            "my_other_parameter_estimator": "$parameter.my_other_parameter.details",
                            "note": "Important remarks about estimation algorithm.",
                        },
                    },
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


def test_run_emits_proper_usage_stats():
    pass


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
@mock.patch("great_expectations.rule_based_profiler.RuleBasedProfiler.run")
@mock.patch("great_expectations.data_context.data_context.DataContext")
def test_run_profiler_without_dynamic_args(
    mock_data_context: mock.MagicMock,
    mock_profiler_run: mock.MagicMock,
    mock_emit: mock.MagicMock,
    populated_profiler_store: ProfilerStore,
    profiler_name: str,
):
    RuleBasedProfiler.run_profiler(
        data_context=mock_data_context,
        profiler_store=populated_profiler_store,
        name=profiler_name,
    )

    assert mock_profiler_run.called
    assert mock_profiler_run.call_args == mock.call(
        variables=None, rules=None, expectation_suite_name=None, include_citation=True
    )

    assert mock_emit.call_count == 1


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
@mock.patch("great_expectations.rule_based_profiler.RuleBasedProfiler.run")
@mock.patch("great_expectations.data_context.data_context.DataContext")
def test_run_profiler_with_dynamic_args(
    mock_data_context: mock.MagicMock,
    mock_profiler_run: mock.MagicMock,
    mock_emit: mock.MagicMock,
    populated_profiler_store: ProfilerStore,
    profiler_name: str,
):
    # Dynamic arguments used to override the profiler's attributes
    variables = {"foo": "bar"}
    rules = {"baz": "qux"}
    expectation_suite_name = "my_expectation_suite_name"
    include_citation = False

    RuleBasedProfiler.run_profiler(
        data_context=mock_data_context,
        profiler_store=populated_profiler_store,
        name=profiler_name,
        variables=variables,
        rules=rules,
        expectation_suite_name=expectation_suite_name,
        include_citation=include_citation,
    )

    assert mock_profiler_run.called
    assert mock_profiler_run.call_args == mock.call(
        variables=variables,
        rules=rules,
        expectation_suite_name=expectation_suite_name,
        include_citation=include_citation,
    )

    assert mock_emit.call_count == 1


@mock.patch("great_expectations.rule_based_profiler.RuleBasedProfiler.run")
@mock.patch("great_expectations.data_context.data_context.DataContext")
def test_run_profiler_on_data_emits_appropriate_logging(
    mock_data_context: mock.MagicMock,
    mock_profiler_run: mock.MagicMock,
    populated_profiler_store: ProfilerStore,
    profiler_name: str,
    caplog: Any,
):
    batch_request: BatchRequest = BatchRequest(
        datasource_name="my_datasource",
        data_connector_name="my_data_connector",
        data_asset_name="my_data_asset",
    )

    with caplog.at_level(logging.INFO):
        RuleBasedProfiler.run_profiler_on_data(
            data_context=mock_data_context,
            profiler_store=populated_profiler_store,
            name=profiler_name,
            batch_request=batch_request,
        )

    assert "Converted batch request" in caplog.text


@mock.patch("great_expectations.rule_based_profiler.RuleBasedProfiler.run")
@mock.patch("great_expectations.data_context.data_context.DataContext")
def test_run_profiler_on_data_creates_suite_with_dict_arg(
    mock_data_context: mock.MagicMock,
    mock_profiler_run: mock.MagicMock,
    populated_profiler_store: ProfilerStore,
    profiler_name: str,
):
    batch_request: Dict[str, str] = {
        "datasource_name": "my_datasource",
        "data_connector_name": "my_data_connector",
        "data_asset_name": "my_data_asset",
    }

    RuleBasedProfiler.run_profiler_on_data(
        data_context=mock_data_context,
        profiler_store=populated_profiler_store,
        name=profiler_name,
        batch_request=batch_request,
    )

    assert mock_profiler_run.called

    rule = mock_profiler_run.call_args[1]["rules"]["rule_1"]
    resulting_batch_request = rule["parameter_builders"][0]["batch_request"]
    assert resulting_batch_request == batch_request


@mock.patch("great_expectations.rule_based_profiler.RuleBasedProfiler.run")
@mock.patch("great_expectations.data_context.data_context.DataContext")
def test_run_profiler_on_data_creates_suite_with_batch_request_arg(
    mock_data_context: mock.MagicMock,
    mock_profiler_run: mock.MagicMock,
    populated_profiler_store: ProfilerStore,
    profiler_name: str,
):
    batch_request: BatchRequest = BatchRequest(
        datasource_name="my_datasource",
        data_connector_name="my_data_connector",
        data_asset_name="my_data_asset",
    )

    RuleBasedProfiler.run_profiler_on_data(
        data_context=mock_data_context,
        profiler_store=populated_profiler_store,
        name=profiler_name,
        batch_request=batch_request,
    )

    assert mock_profiler_run.called

    rule = mock_profiler_run.call_args[1]["rules"]["rule_1"]
    resulting_batch_request = rule["parameter_builders"][0]["batch_request"]
    assert resulting_batch_request == batch_request.to_dict()


@mock.patch("great_expectations.data_context.data_context.DataContext")
def test_get_profiler_with_too_many_args_raises_error(
    mock_data_context: mock.MagicMock,
    populated_profiler_store: ProfilerStore,
):
    with pytest.raises(AssertionError) as e:
        RuleBasedProfiler.get_profiler(
            data_context=mock_data_context,
            profiler_store=populated_profiler_store,
            name="my_profiler",
            ge_cloud_id="my_ge_cloud_id",
        )

    assert "either name or ge_cloud_id" in str(e.value)


@mock.patch("great_expectations.data_context.data_context.DataContext")
def test_add_profiler(
    mock_data_context: mock.MagicMock,
    profiler_key: ConfigurationIdentifier,
    profiler_config_with_placeholder_args: RuleBasedProfilerConfig,
):
    mock_data_context.ge_cloud_mode.return_value = False
    profiler = RuleBasedProfiler.add_profiler(
        profiler_config_with_placeholder_args,
        data_context=mock_data_context,
        profiler_store=mock_data_context.profiler_store,
    )

    assert isinstance(profiler, RuleBasedProfiler)
    assert profiler.name == profiler_config_with_placeholder_args.name
    assert mock_data_context.profiler_store.set.call_args == mock.call(
        key=profiler_key, value=profiler_config_with_placeholder_args
    )


@mock.patch("great_expectations.data_context.data_context.DataContext")
def test_add_profiler_ge_cloud_mode(
    mock_data_context: mock.MagicMock,
    ge_cloud_profiler_id: str,
    ge_cloud_profiler_key: GeCloudIdentifier,
    profiler_config_with_placeholder_args: RuleBasedProfilerConfig,
):
    mock_data_context.ge_cloud_mode.return_value = True
    profiler = RuleBasedProfiler.add_profiler(
        profiler_config_with_placeholder_args,
        data_context=mock_data_context,
        profiler_store=mock_data_context.profiler_store,
        ge_cloud_id=ge_cloud_profiler_id,
    )

    assert isinstance(profiler, RuleBasedProfiler)
    assert profiler.name == profiler_config_with_placeholder_args.name
    assert mock_data_context.profiler_store.set.call_args == mock.call(
        key=ge_cloud_profiler_key, value=profiler_config_with_placeholder_args
    )


@mock.patch("great_expectations.data_context.data_context.DataContext")
def test_add_profiler_with_batch_request_containing_batch_data_raises_error(
    mock_data_context: mock.MagicMock,
):
    profiler_config = RuleBasedProfilerConfig(
        name="my_profiler_config",
        class_name="RuleBasedProfiler",
        module_name="great_expectations.rule_based_profiler",
        config_version=1.0,
        rules={
            "rule_1": {
                "domain_builder": {
                    "class_name": "TableDomainBuilder",
                    "batch_request": {
                        "runtime_parameters": {
                            "batch_data": pd.DataFrame()  # Cannot be serialized in store
                        }
                    },
                },
                "parameter_builders": [
                    {
                        "class_name": "MetricMultiBatchParameterBuilder",
                        "name": "my_parameter",
                        "metric_name": "my_metric",
                    },
                ],
                "expectation_configuration_builders": [
                    {
                        "class_name": "DefaultExpectationConfigurationBuilder",
                        "expectation_type": "expect_column_pair_values_A_to_be_greater_than_B",
                    },
                ],
            }
        },
    )

    with pytest.raises(InvalidConfigError) as e:
        RuleBasedProfiler.add_profiler(
            profiler_config,
            data_context=mock_data_context,
            profiler_store=mock_data_context.profiler_store,
        )

    assert "batch_data found in batch_request" in str(e.value)


@mock.patch("great_expectations.data_context.data_context.DataContext")
def test_get_profiler(
    mock_data_context: mock.MagicMock,
    populated_profiler_store: ProfilerStore,
    profiler_config_with_placeholder_args: RuleBasedProfilerConfig,
):
    with mock.patch(
        "great_expectations.data_context.store.profiler_store.ProfilerStore.get",
        return_value=profiler_config_with_placeholder_args,
    ):
        profiler = RuleBasedProfiler.get_profiler(
            data_context=mock_data_context,
            profiler_store=populated_profiler_store,
            name="my_profiler",
            ge_cloud_id=None,
        )

    assert isinstance(profiler, RuleBasedProfiler)


@mock.patch("great_expectations.data_context.data_context.DataContext")
def test_get_profiler_non_existent_profiler_raises_error(
    mock_data_context: mock.MagicMock, empty_profiler_store: ProfilerStore
):
    with pytest.raises(ge_exceptions.ProfilerNotFoundError) as e:
        RuleBasedProfiler.get_profiler(
            data_context=mock_data_context,
            profiler_store=empty_profiler_store,
            name="my_profiler",
            ge_cloud_id=None,
        )

    assert "Non-existent Profiler" in str(e.value)


def test_delete_profiler(
    populated_profiler_store: ProfilerStore,
):
    with mock.patch(
        "great_expectations.data_context.store.profiler_store.ProfilerStore.remove_key",
    ) as mock_remove_key:
        RuleBasedProfiler.delete_profiler(
            profiler_store=populated_profiler_store,
            name="my_profiler",
            ge_cloud_id=None,
        )

    assert mock_remove_key.call_count == 1
    assert mock_remove_key.call_args == mock.call(
        key=ConfigurationIdentifier("my_profiler")
    )


def test_delete_profiler_with_too_many_args_raises_error(
    populated_profiler_store: ProfilerStore,
):
    with pytest.raises(AssertionError) as e:
        RuleBasedProfiler.delete_profiler(
            profiler_store=populated_profiler_store,
            name="my_profiler",
            ge_cloud_id="my_ge_cloud_id",
        )

    assert "either name or ge_cloud_id" in str(e.value)


def test_delete_profiler_non_existent_profiler_raises_error(
    populated_profiler_store: ProfilerStore,
):
    with pytest.raises(ge_exceptions.ProfilerNotFoundError) as e:
        RuleBasedProfiler.delete_profiler(
            profiler_store=populated_profiler_store,
            name="my_non_existent_profiler",
            ge_cloud_id=None,
        )

    assert "Non-existent Profiler" in str(e.value)


@mock.patch("great_expectations.data_context.store.ProfilerStore")
def test_list_profilers(mock_profiler_store: mock.MagicMock):
    store = mock_profiler_store()
    keys = ["a", "b", "c"]
    store.list_keys.return_value = [ConfigurationIdentifier(char) for char in keys]
    res = RuleBasedProfiler.list_profilers(store, ge_cloud_mode=False)

    assert res == keys
    assert store.list_keys.called


@mock.patch("great_expectations.data_context.store.ProfilerStore")
def test_list_profilers_in_cloud_mode(mock_profiler_store: mock.MagicMock):
    store = mock_profiler_store()
    keys = ["a", "b", "c"]
    store.list_keys.return_value = keys
    res = RuleBasedProfiler.list_profilers(store, ge_cloud_mode=True)

    assert res == keys
    assert store.list_keys.called
