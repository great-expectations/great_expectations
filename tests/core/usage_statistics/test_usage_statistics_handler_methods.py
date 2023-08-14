import logging
from typing import Dict
from unittest import mock

import pytest

from great_expectations.core.usage_statistics.schemas import (
    anonymized_usage_statistics_record_schema,
)
from great_expectations.core.usage_statistics.usage_statistics import (
    UsageStatisticsHandler,
    get_profiler_run_usage_statistics,
)
from great_expectations.rule_based_profiler.rule_based_profiler import RuleBasedProfiler
from great_expectations.util import get_context
from tests.core.usage_statistics.util import usage_stats_invalid_messages_exist


@pytest.mark.unit
def test_usage_statistics_handler_validate_message_failure(
    caplog, in_memory_data_context_config_usage_stats_enabled, sample_partial_message
):
    # caplog default is WARNING and above, we want to see DEBUG level messages for this test
    caplog.set_level(
        level=logging.DEBUG,
        logger="great_expectations.core.usage_statistics.usage_statistics",
    )

    context = get_context(in_memory_data_context_config_usage_stats_enabled)

    usage_statistics_handler = UsageStatisticsHandler(
        data_context=context,
        data_context_id=in_memory_data_context_config_usage_stats_enabled.anonymous_usage_statistics.data_context_id,
        oss_id=None,
        usage_statistics_url=in_memory_data_context_config_usage_stats_enabled.anonymous_usage_statistics.usage_statistics_url,
    )

    assert (
        usage_statistics_handler._data_context_id
        == "00000000-0000-0000-0000-000000000001"
    )

    validated_message = usage_statistics_handler.validate_message(
        sample_partial_message, anonymized_usage_statistics_record_schema
    )
    assert not validated_message
    assert usage_stats_invalid_messages_exist(caplog.messages)


@pytest.fixture
def usage_statistics_envelope():
    return {
        "data_context_id": "00000000-0000-0000-0000-000000000001",
        "data_context_instance_id": "ba598f79-a3a7-4f15-b21e-496f7daae6ca",
        "event": "checkpoint.run",
        "event_payload": {},
        "event_time": "2023-06-07T13:10:16.260Z",
        "ge_version": "0.16.15+29.gcf5a3002a.dirty",
        "mac_address": "d510e8ebe029f3c526a5fa4b921f4589f7e78d28e7732a3559ecf6c84528f940",
        "oss_id": None,
        "success": True,
        "version": "2",
        "x-forwarded-for": "00.000.00.000, 00.000.000.000",
    }


@pytest.mark.unit
def test_usage_statistics_handler_validate_message_success(
    caplog,
    in_memory_data_context_config_usage_stats_enabled,
    usage_statistics_envelope,
):
    # caplog default is WARNING and above, we want to see DEBUG level messages for this test
    caplog.set_level(
        level=logging.DEBUG,
        logger="great_expectations.core.usage_statistics.usage_statistics",
    )

    context = get_context(in_memory_data_context_config_usage_stats_enabled)

    usage_statistics_handler = UsageStatisticsHandler(
        data_context=context,
        data_context_id=in_memory_data_context_config_usage_stats_enabled.anonymous_usage_statistics.data_context_id,
        oss_id=None,
        usage_statistics_url=in_memory_data_context_config_usage_stats_enabled.anonymous_usage_statistics.usage_statistics_url,
    )

    assert (
        usage_statistics_handler._data_context_id
        == "00000000-0000-0000-0000-000000000001"
    )

    validated_message = usage_statistics_handler.validate_message(
        usage_statistics_envelope, anonymized_usage_statistics_record_schema
    )

    assert validated_message
    assert not usage_stats_invalid_messages_exist(caplog.messages)


@pytest.mark.unit
@mock.patch("great_expectations.data_context.data_context.DataContext")
def test_get_profiler_run_usage_statistics_with_handler_valid_payload(
    mock_data_context: mock.MagicMock,
):
    # Ensure that real handler gets passed down by the context
    handler = UsageStatisticsHandler(
        data_context=mock_data_context,
        data_context_id="my_id",
        oss_id=None,
        usage_statistics_url="my_url",
    )
    mock_data_context.usage_statistics_handler = handler

    profiler: RuleBasedProfiler = RuleBasedProfiler(
        name="my_profiler", config_version=1.0, data_context=mock_data_context
    )

    override_rules: Dict[str, dict] = {
        "my_override_rule": {
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

    payload: dict = get_profiler_run_usage_statistics(
        profiler=profiler, rules=override_rules
    )

    assert payload == {
        "anonymized_name": "a0061ec021855cd2b3a994dd8d90fe5d",
        "anonymized_rules": [
            {
                "anonymized_domain_builder": {"parent_class": "ColumnDomainBuilder"},
                "anonymized_expectation_configuration_builders": [
                    {
                        "expectation_type": "expect_column_pair_values_A_to_be_greater_than_B",
                        "parent_class": "DefaultExpectationConfigurationBuilder",
                    },
                    {
                        "expectation_type": "expect_column_min_to_be_between",
                        "parent_class": "DefaultExpectationConfigurationBuilder",
                    },
                ],
                "anonymized_name": "bd8a8b4465a94b363caf2b307c080547",
                "anonymized_parameter_builders": [
                    {
                        "anonymized_name": "25dac9e56a1969727bc0f90db6eaa833",
                        "parent_class": "MetricMultiBatchParameterBuilder",
                    },
                    {
                        "anonymized_name": "be5baa3f1064e6e19356f2168968cbeb",
                        "parent_class": "NumericMetricRangeMultiBatchParameterBuilder",
                    },
                ],
            }
        ],
        "config_version": 1.0,
        "rule_count": 1,
        "variable_count": 0,
    }


@pytest.mark.unit
@mock.patch("great_expectations.data_context.data_context.DataContext")
def test_get_profiler_run_usage_statistics_with_handler_invalid_payload(
    mock_data_context: mock.MagicMock,
):
    # Ensure that real handler gets passed down by the context
    handler = UsageStatisticsHandler(
        data_context=mock_data_context,
        data_context_id="my_id",
        oss_id=None,
        usage_statistics_url="my_url",
    )
    mock_data_context.usage_statistics_handler = handler

    profiler: RuleBasedProfiler = RuleBasedProfiler(
        name="my_profiler", config_version=1.0, data_context=mock_data_context
    )

    payload: dict = get_profiler_run_usage_statistics(profiler=profiler)

    # Payload won't pass schema validation due to a lack of rules but we can confirm that it is anonymized
    assert payload == {
        "anonymized_name": "a0061ec021855cd2b3a994dd8d90fe5d",
        "config_version": 1.0,
        "rule_count": 0,
        "variable_count": 0,
    }


@pytest.mark.unit
def test_get_profiler_run_usage_statistics_without_handler():
    # Without a DataContext, the usage stats handler is not propogated down to the RBP
    profiler: RuleBasedProfiler = RuleBasedProfiler(
        name="my_profiler",
        config_version=1.0,
    )
    payload: dict = get_profiler_run_usage_statistics(profiler=profiler)
    assert payload == {}
