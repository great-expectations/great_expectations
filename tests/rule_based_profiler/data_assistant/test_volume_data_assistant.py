from typing import Any, Dict, List

from freezegun import freeze_time

from great_expectations import DataContext
from great_expectations.core import ExpectationConfiguration, ExpectationSuite
from great_expectations.rule_based_profiler.config import RuleBasedProfilerConfig
from great_expectations.rule_based_profiler.data_assistant import (
    DataAssistant,
    VolumeDataAssistant,
)
from great_expectations.rule_based_profiler.types import DataAssistantResult, Domain
from great_expectations.util import deep_filter_properties_iterable


@freeze_time("09/26/2019 13:42:41")
def test_get_metrics(
    quentin_columnar_table_multi_batch_data_context,
):
    context: DataContext = quentin_columnar_table_multi_batch_data_context

    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }

    expected_metrics: Dict[Domain, Dict[str, Any]] = {
        Domain(domain_type="table",): {
            "$parameter.table_row_count.value": [
                10000,
                10000,
                10000,
                10000,
                10000,
                10000,
                10000,
                10000,
                10000,
                10000,
                10000,
                10000,
                10000,
                10000,
                10000,
                10000,
                10000,
                10000,
                10000,
                10000,
                10000,
                10000,
                10000,
                10000,
                10000,
                10000,
                10000,
                10000,
                10000,
                10000,
                10000,
                10000,
                10000,
                10000,
                10000,
                10000,
            ],
            "$parameter.table_row_count.details": {
                "metric_configuration": {
                    "metric_name": "table.row_count",
                    "domain_kwargs": {},
                    "metric_value_kwargs": None,
                    "metric_dependencies": None,
                },
                "num_batches": 36,
            },
        },
    }

    expected_expectation_configuration: ExpectationConfiguration = (
        ExpectationConfiguration(
            **{
                "kwargs": {
                    "min_value": 10000.0,
                    "max_value": 10000.0,
                },
                "expectation_type": "expect_table_row_count_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "table.row_count",
                            "domain_kwargs": {},
                            "metric_value_kwargs": None,
                            "metric_dependencies": None,
                        },
                        "num_batches": 36,
                    },
                },
            },
        )
    )

    expected_expectation_configurations: List[ExpectationConfiguration] = [
        expected_expectation_configuration,
    ]

    expectation_suite_name: str = "my_suite"

    expected_expectation_suite: ExpectationSuite = ExpectationSuite(
        expectation_suite_name=expectation_suite_name,
    )

    expectation_configuration: ExpectationConfiguration
    for expectation_configuration in expected_expectation_configurations:
        expected_expectation_suite._add_expectation(
            expectation_configuration=expectation_configuration, send_usage_event=False
        )

    expected_expectation_suite_meta: Dict[str, Any] = {
        "citations": [
            {
                "citation_date": "2019-09-26T13:42:41.000000Z",
                "profiler_config": {
                    "name": "test_volume_data_assistant",
                    "config_version": 1.0,
                    "variables": {
                        "false_positive_rate": 0.05,
                        "estimator": "bootstrap",
                        "num_bootstrap_samples": 9999,
                        "bootstrap_random_seed": None,
                        "round_decimals": 0,
                        "truncate_values": {
                            "lower_bound": 0,
                            "upper_bound": None,
                        },
                    },
                    "rules": {
                        "default_expect_table_row_count_to_be_between_rule": {
                            "domain_builder": {
                                "class_name": "TableDomainBuilder",
                                "module_name": "great_expectations.rule_based_profiler.domain_builder.table_domain_builder",
                                "batch_request": None,
                            },
                            "parameter_builders": [
                                {
                                    "module_name": "great_expectations.rule_based_profiler.parameter_builder.metric_multi_batch_parameter_builder",
                                    "batch_request": None,
                                    "metric_name": "table.row_count",
                                    "evaluation_parameter_builder_configs": None,
                                    "json_serialize": True,
                                    "class_name": "MetricMultiBatchParameterBuilder",
                                    "metric_value_kwargs": None,
                                    "enforce_numeric_metric": True,
                                    "metric_domain_kwargs": "$domain.domain_kwargs",
                                    "replace_nan_with_zero": True,
                                    "reduce_scalar_metric": True,
                                    "name": "table_row_count",
                                },
                            ],
                            "expectation_configuration_builders": [
                                {
                                    "max_value": "$parameter.row_count_range_estimator.value[1]",
                                    "module_name": "great_expectations.rule_based_profiler.expectation_configuration_builder.default_expectation_configuration_builder",
                                    "batch_request": None,
                                    "validation_parameter_builder_configs": [
                                        {
                                            "module_name": "great_expectations.rule_based_profiler.parameter_builder",
                                            "batch_request": None,
                                            "bootstrap_random_seed": "$variables.bootstrap_random_seed",
                                            "metric_name": "table.row_count",
                                            "evaluation_parameter_builder_configs": None,
                                            "json_serialize": True,
                                            "replace_nan_with_zero": True,
                                            "metric_value_kwargs": None,
                                            "false_positive_rate": "$variables.false_positive_rate",
                                            "class_name": "NumericMetricRangeMultiBatchParameterBuilder",
                                            "enforce_numeric_metric": True,
                                            "metric_domain_kwargs": None,
                                            "estimator": "$variables.estimator",
                                            "num_bootstrap_samples": "$variables.num_bootstrap_samples",
                                            "round_decimals": "$variables.round_decimals",
                                            "reduce_scalar_metric": True,
                                            "truncate_values": "$variables.truncate_values",
                                            "name": "row_count_range_estimator",
                                        },
                                    ],
                                    "class_name": "DefaultExpectationConfigurationBuilder",
                                    "condition": None,
                                    "meta": {
                                        "profiler_details": "$parameter.row_count_range_estimator.details",
                                    },
                                    "min_value": "$parameter.row_count_range_estimator.value[0]",
                                    "expectation_type": "expect_table_row_count_to_be_between",
                                },
                            ],
                        },
                    },
                },
                "comment": "Suite created by Rule-Based Profiler with the configuration included.",
            },
        ],
    }

    expected_expectation_suite.meta = expected_expectation_suite_meta

    expected_rule_based_profiler_config: RuleBasedProfilerConfig = RuleBasedProfilerConfig(
        class_name="RuleBasedProfiler",
        config_version=1.0,
        module_name="great_expectations.rule_based_profiler.rule_based_profiler",
        name="test_volume_data_assistant",
        variables={
            "false_positive_rate": 0.05,
            "estimator": "bootstrap",
            "num_bootstrap_samples": 9999,
            "round_decimals": 0,
            "truncate_values": {
                "lower_bound": 0,
            },
        },
        rules={
            "default_expect_table_row_count_to_be_between_rule": {
                "domain_builder": {
                    "module_name": "great_expectations.rule_based_profiler.domain_builder.table_domain_builder",
                    "class_name": "TableDomainBuilder",
                },
                "parameter_builders": [
                    {
                        "module_name": "great_expectations.rule_based_profiler.parameter_builder.metric_multi_batch_parameter_builder",
                        "class_name": "MetricMultiBatchParameterBuilder",
                        "name": "table_row_count",
                        "metric_name": "table.row_count",
                        "metric_domain_kwargs": "$domain.domain_kwargs",
                        "enforce_numeric_metric": True,
                        "replace_nan_with_zero": True,
                        "reduce_scalar_metric": True,
                        "json_serialize": True,
                    },
                ],
                "expectation_configuration_builders": [
                    {
                        "module_name": "great_expectations.rule_based_profiler.expectation_configuration_builder.default_expectation_configuration_builder",
                        "class_name": "DefaultExpectationConfigurationBuilder",
                        "validation_parameter_builder_configs": [
                            {
                                "module_name": "great_expectations.rule_based_profiler.parameter_builder",
                                "class_name": "NumericMetricRangeMultiBatchParameterBuilder",
                                "name": "row_count_range_estimator",
                                "metric_name": "table.row_count",
                                "enforce_numeric_metric": True,
                                "replace_nan_with_zero": True,
                                "reduce_scalar_metric": True,
                                "false_positive_rate": "$variables.false_positive_rate",
                                "estimator": "$variables.estimator",
                                "bootstrap_random_seed": "$variables.bootstrap_random_seed",
                                "num_bootstrap_samples": "$variables.num_bootstrap_samples",
                                "round_decimals": "$variables.round_decimals",
                                "truncate_values": "$variables.truncate_values",
                                "json_serialize": True,
                            },
                        ],
                        "expectation_type": "expect_table_row_count_to_be_between",
                        "min_value": "$parameter.row_count_range_estimator.value[0]",
                        "max_value": "$parameter.row_count_range_estimator.value[1]",
                        "meta": {
                            "profiler_details": "$parameter.row_count_range_estimator.details",
                        },
                    },
                ],
            },
        },
    )

    data_assistant: DataAssistant = VolumeDataAssistant(
        name="test_volume_data_assistant",
        batch_request=batch_request,
        data_context=context,
    )
    data_assistant.build()
    result: DataAssistantResult = data_assistant.run(
        expectation_suite_name=expectation_suite_name,
    )

    assert result.metrics == expected_metrics
    assert result.expectation_configurations == expected_expectation_configurations

    result.expectation_suite.meta.pop("great_expectations_version", None)

    assert result.expectation_suite == expected_expectation_suite

    assert result.expectation_suite.meta == expected_expectation_suite_meta

    assert deep_filter_properties_iterable(
        properties=result.profiler_config.to_json_dict()
    ) == deep_filter_properties_iterable(
        properties=expected_rule_based_profiler_config.to_json_dict()
    )

    assert 0.0 < result.execution_time <= 1.0  # Execution time (in seconds).
