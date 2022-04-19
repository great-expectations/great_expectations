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

try:
    from unittest import mock
except ImportError:
    from unittest import mock


@freeze_time("09/26/2019 13:42:41")
def test_get_metrics_and_expectations(
    quentin_columnar_table_multi_batch_data_context,
):
    context: DataContext = quentin_columnar_table_multi_batch_data_context

    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }

    expected_metrics_by_domain: Dict[Domain, Dict[str, Any]] = {
        Domain(domain_type="table",): {
            "$parameter.table_row_count": {
                "value": [
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
                "attributed_value": {
                    "0327cfb13205ec8512e1c28e438ab43b": [10000],
                    "08085632aff9ce4cebbb8023049e1aec": [10000],
                    "0808e185a52825d22356de2fe00a8f5f": [10000],
                    "33d910f95326c0c7dfe7536d1cfeba51": [10000],
                    "3692b23382fd4734215465251290c65b": [10000],
                    "44c1b1947c9049e7db62c5320dde4c63": [10000],
                    "47157bdaf05a7992473cd699cabaef74": [10000],
                    "562969eaef9c843cb4531aecbc13bbcb": [10000],
                    "569a4a80bf434c888593c651dbf2f157": [10000],
                    "57c04d62ada3a102248b48f34c755159": [10000],
                    "58ce3b40d384eacd9bad7d916eb8f705": [10000],
                    "61e4931d87cb627df2a19b8bc5819b7b": [10000],
                    "6c7e43619fe5e6963e8159cc84a28321": [10000],
                    "73612fdabd337d5a8279acc30ce22d00": [10000],
                    "7b3ce20a8e8cf3097bb9df270a7ae63a": [10000],
                    "816b147dcf3305839f723a131b9ad6af": [10000],
                    "84000630d1b69a0fe870c94fb26a32bc": [10000],
                    "8ce0d477f610ea18e2ea4fbbb46de857": [10000],
                    "90bb41c1fbd7c71c05dbc8695320af71": [10000],
                    "940576153c66af14a949fd19aedd5f5b": [10000],
                    "976b121b46db6967854b9c1a6628396b": [10000],
                    "9e58d3c72c7006b6f5800b623fbc9818": [10000],
                    "ab05b4fb82e37c8cf5b1ac40d0a37fe9": [10000],
                    "ad2ad2a70c3e0bf94ddef3f893e92291": [10000],
                    "b20800a7faafd2808d6c888577a2ba1d": [10000],
                    "bb54e4fa3906387218be10cff631a7c2": [10000],
                    "bb81456ec79522bf02f34b02762f95e0": [10000],
                    "c4fe9afce1cf3e83eb8518a9f5abc754": [10000],
                    "c92d0679f769ac83fef2bb5eaac5d12a": [10000],
                    "ce5f02ac408b7b5c500050190f549736": [10000],
                    "e20c38f98b9830a40b851939ca7189d4": [10000],
                    "eff8910cddcdff62e4741243099240d5": [10000],
                    "f2e4d3da6556638b55df8ce509b094c2": [10000],
                    "f67d274202366f6b976414c950ca14bd": [10000],
                    "f6c389dcef63c1f214c30f66b66945c0": [10000],
                    "ff5a6cc031dd2c98b8bccd4766af38c1": [10000],
                },
                "details": {
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
                        "truncate_values": {
                            "lower_bound": 0,
                            "upper_bound": None,
                        },
                        "round_decimals": 0,
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
                                    "class_name": "MetricMultiBatchParameterBuilder",
                                    "name": "table_row_count",
                                    "metric_name": "table.row_count",
                                    "metric_domain_kwargs": "$domain.domain_kwargs",
                                    "metric_value_kwargs": None,
                                    "enforce_numeric_metric": True,
                                    "replace_nan_with_zero": True,
                                    "reduce_scalar_metric": True,
                                    "evaluation_parameter_builder_configs": None,
                                    "json_serialize": True,
                                    "batch_request": None,
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
                                            "class_name": "NumericMetricRangeMultiBatchParameterBuilder",
                                            "name": "row_count_range_estimator",
                                            "metric_name": "table.row_count",
                                            "metric_domain_kwargs": None,
                                            "metric_value_kwargs": None,
                                            "estimator": "$variables.estimator",
                                            "num_bootstrap_samples": "$variables.num_bootstrap_samples",
                                            "bootstrap_random_seed": "$variables.bootstrap_random_seed",
                                            "replace_nan_with_zero": True,
                                            "false_positive_rate": "$variables.false_positive_rate",
                                            "enforce_numeric_metric": True,
                                            "reduce_scalar_metric": True,
                                            "round_decimals": "$variables.round_decimals",
                                            "truncate_values": "$variables.truncate_values",
                                            "evaluation_parameter_builder_configs": None,
                                            "json_serialize": True,
                                            "batch_request": None,
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
        config_version=1.0,
        name="test_volume_data_assistant",
        variables={
            "false_positive_rate": 0.05,
            "estimator": "bootstrap",
            "num_bootstrap_samples": 9999,
            "truncate_values": {
                "lower_bound": 0,
            },
            "round_decimals": 0,
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
                                "truncate_values": "$variables.truncate_values",
                                "round_decimals": "$variables.round_decimals",
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
    result: DataAssistantResult = data_assistant.run(
        expectation_suite_name=expectation_suite_name,
    )

    assert result.metrics_by_domain == expected_metrics_by_domain
    assert result.expectation_suite.expectations == expected_expectation_configurations

    result.expectation_suite.meta.pop("great_expectations_version", None)

    assert result.expectation_suite == expected_expectation_suite

    assert result.expectation_suite.meta == expected_expectation_suite_meta

    assert deep_filter_properties_iterable(
        properties=result.profiler_config.to_json_dict()
    ) == deep_filter_properties_iterable(
        properties=expected_rule_based_profiler_config.to_json_dict()
    )


def test_execution_time_within_proper_bounds(
    quentin_columnar_table_multi_batch_data_context,
):
    context: DataContext = quentin_columnar_table_multi_batch_data_context

    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }

    data_assistant: DataAssistant = VolumeDataAssistant(
        name="test_volume_data_assistant",
        batch_request=batch_request,
        data_context=context,
    )
    result: DataAssistantResult = data_assistant.run()

    assert 0.0 < result.execution_time <= 1.0  # Execution time (in seconds).


def test_volume_data_assistant_plot_descriptive(
    quentin_columnar_table_multi_batch_data_context,
):
    context: DataContext = quentin_columnar_table_multi_batch_data_context

    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }

    data_assistant: DataAssistant = VolumeDataAssistant(
        name="test_volume_data_assistant",
        batch_request=batch_request,
        data_context=context,
    )

    expectation_suite_name: str = "test_suite"
    result: DataAssistantResult = data_assistant.run(
        expectation_suite_name=expectation_suite_name,
    )

    with mock.patch(
        "great_expectations.rule_based_profiler.data_assistant.DataAssistant.plot",
        return_value=False,
    ) as data_assistant:
        data_assistant.plot(result=result)


def test_volume_data_assistant_plot_prescriptive(
    quentin_columnar_table_multi_batch_data_context,
):
    context: DataContext = quentin_columnar_table_multi_batch_data_context

    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }

    data_assistant: DataAssistant = VolumeDataAssistant(
        name="test_volume_data_assistant",
        batch_request=batch_request,
        data_context=context,
    )

    expectation_suite_name: str = "test_suite"
    result: DataAssistantResult = data_assistant.run(
        expectation_suite_name=expectation_suite_name,
    )

    with mock.patch(
        "great_expectations.rule_based_profiler.data_assistant.DataAssistant.plot",
        return_value=False,
    ) as data_assistant:
        data_assistant.plot(result=result, prescriptive=False)
