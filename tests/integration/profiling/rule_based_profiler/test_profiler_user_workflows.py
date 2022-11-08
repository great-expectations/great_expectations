import contextlib
import copy
import datetime
from numbers import Number
from typing import Any, Dict, Iterator, List, Optional, Tuple, Type, cast
from unittest import mock

import numpy as np
import pandas as pd
import pytest
from freezegun import freeze_time
from packaging import version
from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap

# To support python 3.7 we must import Protocol from typing_extensions instead of typing
from typing_extensions import Protocol

from great_expectations import DataContext
from great_expectations.core import (
    ExpectationConfiguration,
    ExpectationSuite,
    ExpectationValidationResult,
)
from great_expectations.core.batch import BatchRequest
from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.datasource import DataConnector, Datasource
from great_expectations.expectations.core import (
    expect_column_quantile_values_to_be_between,
    expect_column_values_to_be_between,
)
from great_expectations.expectations.registry import get_expectation_impl
from great_expectations.rule_based_profiler import RuleBasedProfilerResult
from great_expectations.rule_based_profiler.config.base import (
    RuleBasedProfilerConfig,
    ruleBasedProfilerConfigSchema,
)
from great_expectations.rule_based_profiler.domain import (
    INFERRED_SEMANTIC_TYPE_KEY,
    Domain,
    SemanticDomainTypes,
)
from great_expectations.rule_based_profiler.helpers.util import (
    get_validator_with_expectation_suite,
)
from great_expectations.rule_based_profiler.parameter_container import ParameterNode
from great_expectations.rule_based_profiler.rule_based_profiler import RuleBasedProfiler
from great_expectations.validator.metric_configuration import MetricConfiguration
from great_expectations.validator.validator import Validator
from tests.core.usage_statistics.util import (
    usage_stats_exceptions_exist,
    usage_stats_invalid_messages_exist,
)
from tests.rule_based_profiler.conftest import ATOL, RTOL

yaml = YAML()

TIMESTAMP: str = "09/26/2019 13:42:41"


@pytest.fixture
def alice_validator(alice_columnar_table_single_batch_context) -> Validator:
    context: DataContext = alice_columnar_table_single_batch_context

    batch_request: dict = {
        "datasource_name": "alice_columnar_table_single_batch_datasource",
        "data_connector_name": "alice_columnar_table_single_batch_data_connector",
        "data_asset_name": "alice_columnar_table_single_batch_data_asset",
    }

    validator: Validator = get_validator_with_expectation_suite(
        data_context=context,
        batch_list=None,
        batch_request=batch_request,
        expectation_suite_name=None,
        expectation_suite=None,
        component_name="profiler",
        persist=False,
    )

    assert len(validator.batches) == 1
    return validator


@pytest.fixture
def bobby_validator(
    bobby_columnar_table_multi_batch_deterministic_data_context: DataContext,
) -> Validator:
    context: DataContext = bobby_columnar_table_multi_batch_deterministic_data_context

    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }

    validator: Validator = get_validator_with_expectation_suite(
        data_context=context,
        batch_list=None,
        batch_request=batch_request,
        expectation_suite_name=None,
        expectation_suite=None,
        component_name="profiler",
        persist=False,
    )

    assert len(validator.batches) == 3
    return validator


@pytest.fixture
def bobster_validator(
    bobster_columnar_table_multi_batch_normal_mean_5000_stdev_1000_data_context,
    set_consistent_seed_within_numeric_metric_range_multi_batch_parameter_builder,
) -> Validator:
    """Utilizes a consistent bootstrap seed in its RBP NumericMetricRangeMultiBatchParameterBuilder."""
    context: DataContext = (
        bobster_columnar_table_multi_batch_normal_mean_5000_stdev_1000_data_context
    )

    # Use all batches, loaded by Validator, for estimating Expectation argument values.
    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }

    validator: Validator = get_validator_with_expectation_suite(
        data_context=context,
        batch_list=None,
        batch_request=batch_request,
        expectation_suite_name=None,
        expectation_suite=None,
        component_name="profiler",
        persist=False,
    )

    assert len(validator.batches) == 36
    return validator


@pytest.fixture
def quentin_validator(
    quentin_columnar_table_multi_batch_data_context,
    set_consistent_seed_within_numeric_metric_range_multi_batch_parameter_builder,
) -> Validator:
    """Utilizes a consistent bootstrap seed in its RBP NumericMetricRangeMultiBatchParameterBuilder."""
    context: DataContext = quentin_columnar_table_multi_batch_data_context

    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }

    validator: Validator = get_validator_with_expectation_suite(
        data_context=context,
        batch_list=None,
        batch_request=batch_request,
        expectation_suite_name=None,
        expectation_suite=None,
        component_name="profiler",
        persist=False,
    )

    assert len(validator.batches) == 36
    return validator


@pytest.mark.slow  # 1.15s
@pytest.mark.integration
def test_alice_columnar_table_single_batch_batches_are_accessible(
    alice_columnar_table_single_batch_context,
    alice_columnar_table_single_batch,
):
    """
    What does this test and why?
    Batches created in the multibatch_generic_csv_generator fixture should be available using the
    multibatch_generic_csv_generator_context
    This test most likely duplicates tests elsewhere, but it is more of a test of the configurable fixture.
    """

    context: DataContext = alice_columnar_table_single_batch_context

    datasource_name: str = "alice_columnar_table_single_batch_datasource"
    data_connector_name: str = "alice_columnar_table_single_batch_data_connector"
    data_asset_name: str = "alice_columnar_table_single_batch_data_asset"

    datasource: Datasource = cast(Datasource, context.datasources[datasource_name])
    data_connector: DataConnector = datasource.data_connectors[data_connector_name]

    file_list: List[str] = [
        alice_columnar_table_single_batch["sample_data_relative_path"]
    ]

    assert (
        data_connector._get_data_reference_list_from_cache_by_data_asset_name(
            data_asset_name=data_asset_name
        )
        == file_list
    )

    batch_request_1: BatchRequest = BatchRequest(
        datasource_name=datasource_name,
        data_connector_name=data_connector_name,
        data_asset_name=data_asset_name,
        data_connector_query={
            "index": -1,
        },
    )
    # Should give most recent batch
    validator_1: Validator = context.get_validator(
        batch_request=batch_request_1,
        create_expectation_suite_with_name="my_expectation_suite_name_1",
    )
    metric_max: int = validator_1.get_metric(
        MetricConfiguration("column.max", metric_domain_kwargs={"column": "event_type"})
    )
    assert metric_max == 73


@freeze_time(TIMESTAMP)
@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
@pytest.mark.slow  # 2.31s
@pytest.mark.integration
def test_alice_profiler_user_workflow_single_batch(
    mock_emit,
    caplog,
    alice_columnar_table_single_batch_context,
    alice_columnar_table_single_batch,
):
    # Load data context
    data_context: DataContext = alice_columnar_table_single_batch_context

    # Load profiler configs & loop (run tests for each one)
    yaml_config: str = alice_columnar_table_single_batch["profiler_config"]

    # Instantiate Profiler
    profiler_config: CommentedMap = yaml.load(yaml_config)

    # Roundtrip through schema validation to remove any illegal fields add/or restore any missing fields.
    deserialized_config: dict = ruleBasedProfilerConfigSchema.load(profiler_config)
    serialized_config: dict = ruleBasedProfilerConfigSchema.dump(deserialized_config)

    # `class_name`/`module_name` are generally consumed through `instantiate_class_from_config`
    # so we need to manually remove those values if we wish to use the **kwargs instantiation pattern
    serialized_config.pop("class_name")
    serialized_config.pop("module_name")

    profiler: RuleBasedProfiler = RuleBasedProfiler(
        **serialized_config,
        data_context=data_context,
    )

    # BatchRequest yielding exactly one batch
    alice_single_batch_data_batch_request: dict = {
        "datasource_name": "alice_columnar_table_single_batch_datasource",
        "data_connector_name": "alice_columnar_table_single_batch_data_connector",
        "data_asset_name": "alice_columnar_table_single_batch_data_asset",
    }
    result: RuleBasedProfilerResult = profiler.run(
        batch_request=alice_single_batch_data_batch_request
    )

    expectation_configuration: ExpectationConfiguration
    for expectation_configuration in result.expectation_configurations:
        if "profiler_details" in expectation_configuration.meta:
            expectation_configuration.meta["profiler_details"].pop(
                "estimation_histogram", None
            )

    assert (
        result.expectation_configurations
        == alice_columnar_table_single_batch["expected_expectation_suite"].expectations
    )

    assert mock_emit.call_count == 43

    assert all(
        payload[0][0]["event"] == "data_context.get_batch_list"
        for payload in mock_emit.call_args_list[:-1]
    )

    # noinspection PyUnresolvedReferences
    expected_profiler_run_event: mock._Call = mock.call(
        {
            "event_payload": {
                "anonymized_name": "0481bcf98600fd04aa24df03d05cdcf5",
                "config_version": 1.0,
                "anonymized_rules": [
                    {
                        "anonymized_name": "b9c8ed2ae9948de069a857628bb4291a",
                        "anonymized_domain_builder": {
                            "parent_class": "DomainBuilder",
                            "anonymized_class": "9c8f42e45ec72197d6fb4d0d5194c89d",
                        },
                        "anonymized_parameter_builders": [
                            {
                                "parent_class": "MetricSingleBatchParameterBuilder",
                                "anonymized_name": "2b4df3c7cf39207db3e08477e1ea8f79",
                            },
                            {
                                "parent_class": "MetricSingleBatchParameterBuilder",
                                "anonymized_name": "bea5e4c3943006d008899cdb1ebc3fb4",
                            },
                        ],
                        "anonymized_expectation_configuration_builders": [
                            {
                                "parent_class": "DefaultExpectationConfigurationBuilder",
                                "expectation_type": "expect_column_values_to_be_of_type",
                            },
                            {
                                "parent_class": "DefaultExpectationConfigurationBuilder",
                                "expectation_type": "expect_column_values_to_be_between",
                            },
                            {
                                "parent_class": "DefaultExpectationConfigurationBuilder",
                                "expectation_type": "expect_column_values_to_not_be_null",
                            },
                            {
                                "parent_class": "DefaultExpectationConfigurationBuilder",
                                "anonymized_expectation_type": "49e0013b377d4c7d9604d73fd672aa63",
                                "anonymized_condition": "5191ecaeb23644e402e68b1c641b1342",
                            },
                            {
                                "parent_class": "DefaultExpectationConfigurationBuilder",
                                "anonymized_expectation_type": "5a4993ff394c8cf957dbe7964798f5a5",
                                "anonymized_condition": "a7f49ffeced7b75c9e0d958e9d010ddd",
                            },
                        ],
                    },
                    {
                        "anonymized_name": "116c25bb5cf9b84958846024fe1c2b7b",
                        "anonymized_domain_builder": {
                            "parent_class": "ColumnDomainBuilder",
                        },
                        "anonymized_parameter_builders": [
                            {
                                "parent_class": "MetricSingleBatchParameterBuilder",
                                "anonymized_name": "fa3ce9b81f1acc2f2730005e05737ea7",
                            },
                            {
                                "parent_class": "MetricSingleBatchParameterBuilder",
                                "anonymized_name": "0bb947e516b26696a66787dc936570b7",
                            },
                            {
                                "parent_class": "MetricSingleBatchParameterBuilder",
                                "anonymized_name": "66093b34c0c2e4ff275edf1752bcd27e",
                            },
                            {
                                "parent_class": "SimpleDateFormatStringParameterBuilder",
                                "anonymized_name": "c5caa53c1ee64365b96ec96a285a6b3a",
                            },
                        ],
                        "anonymized_expectation_configuration_builders": [
                            {
                                "parent_class": "DefaultExpectationConfigurationBuilder",
                                "expectation_type": "expect_column_values_to_be_of_type",
                            },
                            {
                                "parent_class": "DefaultExpectationConfigurationBuilder",
                                "expectation_type": "expect_column_values_to_be_increasing",
                            },
                            {
                                "parent_class": "DefaultExpectationConfigurationBuilder",
                                "expectation_type": "expect_column_values_to_be_dateutil_parseable",
                            },
                            {
                                "parent_class": "DefaultExpectationConfigurationBuilder",
                                "expectation_type": "expect_column_min_to_be_between",
                            },
                            {
                                "parent_class": "DefaultExpectationConfigurationBuilder",
                                "expectation_type": "expect_column_max_to_be_between",
                            },
                            {
                                "parent_class": "DefaultExpectationConfigurationBuilder",
                                "expectation_type": "expect_column_values_to_match_strftime_format",
                            },
                        ],
                    },
                    {
                        "anonymized_name": "83a71ec7b61bbdb8eb728ebc428f8aea",
                        "anonymized_domain_builder": {
                            "parent_class": "CategoricalColumnDomainBuilder",
                        },
                        "anonymized_parameter_builders": [
                            {
                                "parent_class": "ValueSetMultiBatchParameterBuilder",
                                "anonymized_name": "46d405b0294a06e1335c04bf1441dabf",
                            }
                        ],
                        "anonymized_expectation_configuration_builders": [
                            {
                                "parent_class": "DefaultExpectationConfigurationBuilder",
                                "expectation_type": "expect_column_values_to_be_in_set",
                            }
                        ],
                    },
                ],
                "rule_count": 3,
                "variable_count": 5,
            },
            "event": "profiler.run",
            "success": True,
        }
    )
    assert mock_emit.call_args_list[-1] == expected_profiler_run_event

    # Confirm that logs do not contain any exceptions or invalid messages
    assert not usage_stats_exceptions_exist(messages=caplog.messages)
    assert not usage_stats_invalid_messages_exist(messages=caplog.messages)


@freeze_time(TIMESTAMP)
@pytest.mark.slow  # 0.86s
@pytest.mark.integration
def test_alice_expect_column_values_to_match_regex_auto_yes_default_profiler_config_yes_custom_profiler_config_no(
    alice_validator: Validator,
) -> None:
    validator: Validator = alice_validator

    result: ExpectationValidationResult = validator.expect_column_values_to_match_regex(
        column="id",
        result_format="SUMMARY",
        include_config=True,
        auto=True,
    )

    assert result.success

    expectation_config_kwargs: dict = result.expectation_config.kwargs
    assert expectation_config_kwargs == {
        "auto": True,
        "batch_id": "cf28d8229c247275c8cc0f41b4ceb62d",
        "column": "id",
        "include_config": True,
        "mostly": 1.0,
        "regex": "-?\\d+",
        "result_format": "SUMMARY",
    }


@freeze_time(TIMESTAMP)
@pytest.mark.integration
def test_alice_expect_column_values_to_not_match_regex_auto_yes_default_profiler_config_yes_custom_profiler_config_no(
    alice_validator: Validator,
) -> None:
    validator: Validator = alice_validator

    result: ExpectationValidationResult = (
        validator.expect_column_values_to_not_match_regex(
            column="id",
            result_format="SUMMARY",
            include_config=True,
            auto=True,
        )
    )

    assert not result.success

    expectation_config_kwargs: dict = result.expectation_config.kwargs
    assert expectation_config_kwargs == {
        "auto": True,
        "batch_id": "cf28d8229c247275c8cc0f41b4ceb62d",
        "column": "id",
        "include_config": True,
        "mostly": 1.0,
        "regex": "-?\\d+",
        "result_format": "SUMMARY",
    }


@freeze_time(TIMESTAMP)
@pytest.mark.slow  # 1.38s
@pytest.mark.integration
def test_alice_expect_column_values_to_match_stftime_format_auto_yes_default_profiler_config_yes_custom_profiler_config_no(
    alice_validator: Validator,
) -> None:
    validator: Validator = alice_validator

    result: ExpectationValidationResult = (
        validator.expect_column_values_to_match_strftime_format(
            column="event_ts",
            result_format="SUMMARY",
            include_config=True,
            auto=True,
        )
    )

    assert result.success

    expectation_config_kwargs: dict = result.expectation_config.kwargs
    assert expectation_config_kwargs == {
        "auto": True,
        "batch_id": "cf28d8229c247275c8cc0f41b4ceb62d",
        "column": "event_ts",
        "include_config": True,
        "mostly": 1.0,
        "strftime_format": "%Y-%m-%d %H:%M:%S",
        "result_format": "SUMMARY",
    }


@freeze_time(TIMESTAMP)
@pytest.mark.slow  # 1.26s
@pytest.mark.integration
def test_alice_expect_column_value_lengths_to_be_between_auto_yes_default_profiler_config_yes_custom_profiler_config_no(
    alice_validator: Validator,
) -> None:
    validator: Validator = alice_validator

    result: ExpectationValidationResult = (
        validator.expect_column_value_lengths_to_be_between(
            column="user_agent",
            result_format="SUMMARY",
            include_config=True,
            auto=True,
        )
    )

    assert result.success

    expectation_config_kwargs: dict = result.expectation_config.kwargs
    assert expectation_config_kwargs == {
        "auto": True,
        "batch_id": "cf28d8229c247275c8cc0f41b4ceb62d",
        "column": "user_agent",
        "include_config": True,
        "max_value": 115,  # Chetan - 20220516 - Note that all values in the dataset are of equal length
        "min_value": 115,  # TODO - we should add an additional test upon using an updated dataset (confirmed behavior through UAT)
        "mostly": 1.0,
        "result_format": "SUMMARY",
        "strict_max": False,
        "strict_min": False,
    }


# noinspection PyUnusedLocal
@pytest.mark.slow  # 1.16s
@pytest.mark.integration
def test_bobby_columnar_table_multi_batch_batches_are_accessible(
    monkeypatch,
    bobby_columnar_table_multi_batch_deterministic_data_context,
    bobby_columnar_table_multi_batch,
):
    """
    What does this test and why?
    Batches created in the multibatch_generic_csv_generator fixture should be available using the
    multibatch_generic_csv_generator_context
    This test most likely duplicates tests elsewhere, but it is more of a test of the configurable fixture.
    """

    context: DataContext = bobby_columnar_table_multi_batch_deterministic_data_context

    datasource_name: str = "taxi_pandas"
    data_connector_name: str = "monthly"
    data_asset_name: str = "my_reports"

    datasource: Datasource = cast(Datasource, context.datasources[datasource_name])
    data_connector: DataConnector = datasource.data_connectors[data_connector_name]

    file_list: List[str] = [
        "yellow_tripdata_sample_2019-01.csv",
        "yellow_tripdata_sample_2019-02.csv",
        "yellow_tripdata_sample_2019-03.csv",
    ]

    assert (
        data_connector._get_data_reference_list_from_cache_by_data_asset_name(
            data_asset_name=data_asset_name
        )
        == file_list
    )

    batch_request_latest: BatchRequest = BatchRequest(
        datasource_name=datasource_name,
        data_connector_name=data_connector_name,
        data_asset_name=data_asset_name,
        data_connector_query={
            "index": -1,
        },
    )
    validator_latest: Validator = context.get_validator(
        batch_request=batch_request_latest,
        create_expectation_suite_with_name="my_expectation_suite_name_1",
    )

    metric_configuration_arguments: Dict[str, Any] = {
        "metric_name": "table.row_count",
        "metric_domain_kwargs": {
            "batch_id": validator_latest.active_batch_id,
        },
        "metric_value_kwargs": None,
    }
    metric_value: int = validator_latest.get_metric(
        metric=MetricConfiguration(**metric_configuration_arguments)
    )
    assert metric_value == 9000

    # noinspection PyUnresolvedReferences
    pickup_datetime: datetime.datetime = pd.to_datetime(
        validator_latest.head(n_rows=1)["pickup_datetime"][0]
    ).to_pydatetime()
    month: int = pickup_datetime.month
    assert month == 3


@pytest.mark.skipif(
    version.parse(np.version.version) < version.parse("1.21.0"),
    reason="requires numpy version 1.21.0 or newer",
)
@freeze_time(TIMESTAMP)
@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
@pytest.mark.slow  # 13.08s
@pytest.mark.integration
def test_bobby_profiler_user_workflow_multi_batch_row_count_range_rule_and_column_ranges_rule_quantiles_estimator(
    mock_emit,
    caplog,
    bobby_columnar_table_multi_batch_deterministic_data_context,
    bobby_columnar_table_multi_batch,
):
    # Load data context
    data_context: DataContext = (
        bobby_columnar_table_multi_batch_deterministic_data_context
    )

    # Load profiler configs & loop (run tests for each one)
    yaml_config: str = bobby_columnar_table_multi_batch["profiler_config"]

    # Instantiate Profiler
    profiler_config: dict = yaml.load(yaml_config)

    # Roundtrip through schema validation to remove any illegal fields add/or restore any missing fields.
    deserialized_config: dict = ruleBasedProfilerConfigSchema.load(profiler_config)
    serialized_config: dict = ruleBasedProfilerConfigSchema.dump(deserialized_config)

    # `class_name`/`module_name` are generally consumed through `instantiate_class_from_config`
    # so we need to manually remove those values if we wish to use the **kwargs instantiation pattern
    serialized_config.pop("class_name")
    serialized_config.pop("module_name")

    profiler: RuleBasedProfiler = RuleBasedProfiler(
        **serialized_config,
        data_context=data_context,
    )

    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }

    result: RuleBasedProfilerResult = profiler.run(batch_request=batch_request)

    domain: Domain

    fixture_expectation_suite: ExpectationSuite = bobby_columnar_table_multi_batch[
        "test_configuration_quantiles_estimator"
    ]["expected_expectation_suite"]

    expectation_configuration: ExpectationConfiguration
    for expectation_configuration in result.expectation_configurations:
        if "profiler_details" in expectation_configuration.meta:
            expectation_configuration.meta["profiler_details"].pop(
                "estimation_histogram", None
            )

    assert result.expectation_configurations == fixture_expectation_suite.expectations

    profiled_fully_qualified_parameter_names_by_domain: Dict[
        Domain, List[str]
    ] = profiler.get_fully_qualified_parameter_names_by_domain()

    fixture_fully_qualified_parameter_names_by_domain: Dict[
        Domain, List[str]
    ] = bobby_columnar_table_multi_batch["test_configuration_quantiles_estimator"][
        "expected_fixture_fully_qualified_parameter_names_by_domain"
    ]

    assert (
        profiled_fully_qualified_parameter_names_by_domain
        == fixture_fully_qualified_parameter_names_by_domain
    )

    domain = Domain(
        domain_type=MetricDomainTypes.TABLE,
        rule_name="row_count_range_rule",
    )

    profiled_fully_qualified_parameter_names_for_domain_id: List[
        str
    ] = profiler.get_fully_qualified_parameter_names_for_domain_id(domain.id)

    fixture_fully_qualified_parameter_names_for_domain_id: List[
        str
    ] = bobby_columnar_table_multi_batch["test_configuration_quantiles_estimator"][
        "expected_fixture_fully_qualified_parameter_names_by_domain"
    ][
        domain
    ]

    assert (
        profiled_fully_qualified_parameter_names_for_domain_id
        == fixture_fully_qualified_parameter_names_for_domain_id
    )

    profiled_parameter_values_for_fully_qualified_parameter_names_by_domain: Dict[
        Domain, Dict[str, ParameterNode]
    ] = profiler.get_parameter_values_for_fully_qualified_parameter_names_by_domain()

    fixture_profiled_parameter_values_for_fully_qualified_parameter_names_by_domain: Dict[
        Domain, Dict[str, ParameterNode]
    ] = bobby_columnar_table_multi_batch[
        "test_configuration_quantiles_estimator"
    ][
        "expected_parameter_values_for_fully_qualified_parameter_names_by_domain"
    ]

    assert convert_to_json_serializable(
        data=profiled_parameter_values_for_fully_qualified_parameter_names_by_domain
    ) == convert_to_json_serializable(
        data=fixture_profiled_parameter_values_for_fully_qualified_parameter_names_by_domain
    )

    domain = Domain(
        domain_type="column",
        domain_kwargs={"column": "VendorID"},
        details={
            INFERRED_SEMANTIC_TYPE_KEY: {
                "VendorID": SemanticDomainTypes.NUMERIC,
            },
        },
        rule_name="column_ranges_rule",
    )

    profiled_parameter_values_for_fully_qualified_parameter_names_for_domain_id: Dict[
        str, ParameterNode
    ] = profiler.get_parameter_values_for_fully_qualified_parameter_names_for_domain_id(
        domain_id=domain.id
    )

    fixture_profiled_parameter_values_for_fully_qualified_parameter_names_for_domain_id: Dict[
        str, ParameterNode
    ] = bobby_columnar_table_multi_batch[
        "test_configuration_quantiles_estimator"
    ][
        "expected_parameter_values_for_fully_qualified_parameter_names_by_domain"
    ][
        domain
    ]

    assert convert_to_json_serializable(
        data=profiled_parameter_values_for_fully_qualified_parameter_names_for_domain_id
    ) == convert_to_json_serializable(
        data=fixture_profiled_parameter_values_for_fully_qualified_parameter_names_for_domain_id
    )

    assert mock_emit.call_count == 99

    assert all(
        payload[0][0]["event"] == "data_context.get_batch_list"
        for payload in mock_emit.call_args_list[:-1]
    )

    # noinspection PyUnresolvedReferences
    expected_profiler_run_event: mock._Call = mock.call(
        {
            "event_payload": {
                "anonymized_name": "43c8704c864dd10feed13219062f0228",
                "config_version": 1.0,
                "anonymized_rules": [
                    {
                        "anonymized_name": "7980584b8d0c7c8a66dbeaaf4b067885",
                        "anonymized_domain_builder": {
                            "parent_class": "TableDomainBuilder"
                        },
                        "anonymized_parameter_builders": [
                            {
                                "parent_class": "NumericMetricRangeMultiBatchParameterBuilder",
                                "anonymized_name": "dc1bc513697628c3a7f5b73494234a01",
                            }
                        ],
                        "anonymized_expectation_configuration_builders": [
                            {
                                "parent_class": "DefaultExpectationConfigurationBuilder",
                                "expectation_type": "expect_table_row_count_to_be_between",
                            }
                        ],
                    },
                    {
                        "anonymized_name": "9b95e917ab153669bcd32ec522604556",
                        "anonymized_domain_builder": {
                            "parent_class": "ColumnDomainBuilder",
                        },
                        "anonymized_parameter_builders": [
                            {
                                "parent_class": "NumericMetricRangeMultiBatchParameterBuilder",
                                "anonymized_name": "ace31374d026e07ec630c7459915e628",
                            },
                            {
                                "parent_class": "NumericMetricRangeMultiBatchParameterBuilder",
                                "anonymized_name": "f2dcb0a322c3ab161d0fd33e6bac3d7f",
                            },
                        ],
                        "anonymized_expectation_configuration_builders": [
                            {
                                "parent_class": "DefaultExpectationConfigurationBuilder",
                                "expectation_type": "expect_column_min_to_be_between",
                            },
                            {
                                "parent_class": "DefaultExpectationConfigurationBuilder",
                                "expectation_type": "expect_column_max_to_be_between",
                            },
                        ],
                    },
                    {
                        "anonymized_name": "716348d3985f11679de53ec2b6ce5987",
                        "anonymized_domain_builder": {
                            "parent_class": "ColumnDomainBuilder",
                        },
                        "anonymized_parameter_builders": [
                            {
                                "parent_class": "SimpleDateFormatStringParameterBuilder",
                                "anonymized_name": "49526fea6686e5f87be493967a5ba6b7",
                            }
                        ],
                        "anonymized_expectation_configuration_builders": [
                            {
                                "parent_class": "DefaultExpectationConfigurationBuilder",
                                "expectation_type": "expect_column_values_to_match_strftime_format",
                            }
                        ],
                    },
                    {
                        "anonymized_name": "38f59421a7c7b59a45b547a73c7714f9",
                        "anonymized_domain_builder": {
                            "parent_class": "ColumnDomainBuilder",
                        },
                        "anonymized_parameter_builders": [
                            {
                                "parent_class": "RegexPatternStringParameterBuilder",
                                "anonymized_name": "2a791a71865e26a98b3f9f89ef83556b",
                            }
                        ],
                        "anonymized_expectation_configuration_builders": [
                            {
                                "parent_class": "DefaultExpectationConfigurationBuilder",
                                "expectation_type": "expect_column_values_to_match_regex",
                            }
                        ],
                    },
                    {
                        "anonymized_name": "57a6fe0ec12e12107fc275b42855ab1c",
                        "anonymized_domain_builder": {
                            "parent_class": "CategoricalColumnDomainBuilder",
                        },
                        "anonymized_parameter_builders": [
                            {
                                "parent_class": "ValueSetMultiBatchParameterBuilder",
                                "anonymized_name": "73b050124a17a420f2fb3e605f7111b2",
                            }
                        ],
                        "anonymized_expectation_configuration_builders": [
                            {
                                "parent_class": "DefaultExpectationConfigurationBuilder",
                                "expectation_type": "expect_column_values_to_be_in_set",
                            }
                        ],
                    },
                ],
                "rule_count": 5,
                "variable_count": 3,
            },
            "event": "profiler.run",
            "success": True,
        }
    )
    assert mock_emit.call_args_list[-1] == expected_profiler_run_event

    # Confirm that logs do not contain any exceptions or invalid messages
    assert not usage_stats_exceptions_exist(messages=caplog.messages)
    assert not usage_stats_invalid_messages_exist(messages=caplog.messages)


@pytest.mark.skipif(
    version.parse(np.version.version) < version.parse("1.21.0"),
    reason="requires numpy version 1.21.0 or newer",
)
@freeze_time(TIMESTAMP)
@pytest.mark.integration
def test_bobby_expect_column_values_to_be_between_auto_yes_default_profiler_config_yes_custom_profiler_config_no(
    bobby_validator: Validator,
    set_consistent_seed_within_numeric_metric_range_multi_batch_parameter_builder,
):
    validator: Validator = bobby_validator

    column_name: str = "fare_amount"

    result: ExpectationValidationResult

    result = validator.expect_column_values_to_be_between(
        column=column_name,
        result_format="SUMMARY",
        include_config=True,
        auto=True,
        profiler_config=None,
    )
    assert result.success
    assert result.expectation_config["kwargs"] == {
        "column": "fare_amount",
        "min_value": -52.0,
        "max_value": 3004.0,
        "strict_min": False,
        "strict_max": False,
        "mostly": 1.0,
        "result_format": "SUMMARY",
        "include_config": True,
        "auto": True,
        "profiler_config": None,
        "batch_id": "90bb41c1fbd7c71c05dbc8695320af71",
    }

    result = validator.expect_column_values_to_be_between(
        column=column_name,
        min_value=0.0,
        result_format="SUMMARY",
        include_config=True,
        auto=True,
        profiler_config=None,
    )
    assert not result.success
    assert result.expectation_config["kwargs"] == {
        "column": "fare_amount",
        "min_value": 0.0,
        "max_value": 3004.0,
        "strict_min": False,
        "strict_max": False,
        "mostly": 1.0,
        "result_format": "SUMMARY",
        "include_config": True,
        "auto": True,
        "profiler_config": None,
        "batch_id": "90bb41c1fbd7c71c05dbc8695320af71",
    }

    result = validator.expect_column_values_to_be_between(
        column=column_name,
        min_value=0.0,
        mostly=8.75e-1,
        result_format="SUMMARY",
        include_config=True,
        auto=True,
        profiler_config=None,
    )
    assert result.success
    assert result.expectation_config["kwargs"] == {
        "column": "fare_amount",
        "min_value": 0.0,
        "max_value": 3004.0,
        "strict_min": False,
        "strict_max": False,
        "mostly": 8.75e-1,
        "result_format": "SUMMARY",
        "include_config": True,
        "auto": True,
        "profiler_config": None,
        "batch_id": "90bb41c1fbd7c71c05dbc8695320af71",
    }

    with pytest.raises(AssertionError) as e:
        # noinspection PyUnusedLocal
        result = validator.expect_column_values_to_be_between(
            column=column_name,
            mostly=1.0,
            result_format="SUMMARY",
            include_config=True,
            auto=False,
            profiler_config=None,
        )
    assert "min_value and max_value cannot both be None" in str(e.value)

    result = validator.expect_column_values_to_be_between(
        column=column_name,
        min_value=-52.0,
        max_value=3004.0,
        mostly=1.0,
        result_format="SUMMARY",
        include_config=True,
    )
    assert result.success
    assert result.expectation_config["kwargs"] == {
        "column": "fare_amount",
        "min_value": -52.0,
        "max_value": 3004.0,
        "mostly": 1.0,
        "result_format": "SUMMARY",
        "include_config": True,
        "batch_id": "90bb41c1fbd7c71c05dbc8695320af71",
    }


class HasStaticDefaultProfiler(Protocol):
    default_profiler_config: RuleBasedProfilerConfig
    # I'd like to force the key "profiler_config" to be present in the following dict.
    # While its absence doesn't break functionality, I do expect it to exist. TypeDicts
    # unfortunately don't help us here since one needs to list all potential keys in a
    # TypeDict since they don't allow extras keys to be present. See the discussion here:
    # https://github.com/python/mypy/issues/4617#issuecomment-367647383
    default_kwarg_values: Dict[str, Any]


# An expectations default profiler config is a static variable. Setting it to a custom value will
# actually overwrite the default. This will cause other tests in this session to fail that depend
# on the default value. This decorator stores the default value and restores it at the end of this test.
# We shouldn't be overwriting the default and that is ticketed:
# https://superconductive.atlassian.net/browse/GREAT-1127
@contextlib.contextmanager
def restore_profiler_config(
    expectation: Type[HasStaticDefaultProfiler],
) -> Iterator[None]:
    original_default_profiler_config = copy.deepcopy(
        expectation.default_profiler_config
    )
    try:
        yield
    finally:
        expectation.default_profiler_config = original_default_profiler_config
        expectation.default_kwarg_values[
            "profiler_config"
        ] = original_default_profiler_config


@pytest.mark.skipif(
    version.parse(np.version.version) < version.parse("1.21.0"),
    reason="requires numpy version 1.21.0 or newer",
)
@freeze_time(TIMESTAMP)
@pytest.mark.integration
def test_bobby_expect_column_values_to_be_between_auto_yes_default_profiler_config_yes_custom_profiler_config_yes(
    bobby_columnar_table_multi_batch_deterministic_data_context,
    bobby_validator: Validator,
):
    context: DataContext = bobby_columnar_table_multi_batch_deterministic_data_context
    validator: Validator = bobby_validator

    with restore_profiler_config(
        expect_column_values_to_be_between.ExpectColumnValuesToBeBetween
    ):
        custom_profiler_config = RuleBasedProfilerConfig(
            name="expect_column_values_to_be_between",  # Convention: use "expectation_type" as profiler name.
            config_version=1.0,
            rules={
                "custom_column_values_between_rule": {
                    "parameter_builders": [
                        {
                            "name": "my_min_estimator",
                            "class_name": "MetricMultiBatchParameterBuilder",
                            "module_name": "great_expectations.rule_based_profiler.parameter_builder",
                            "metric_name": "column.min",
                            "metric_domain_kwargs": "$domain.domain_kwargs",
                            "enforce_numeric_metric": True,
                            "replace_nan_with_zero": True,
                        },
                    ],
                    "expectation_configuration_builders": [
                        {
                            "class_name": "DefaultExpectationConfigurationBuilder",
                            "module_name": "great_expectations.rule_based_profiler.expectation_configuration_builder",
                            "expectation_type": "expect_column_values_to_be_between",
                            "column": "$domain.domain_kwargs.column",
                            "min_value": "$parameter.my_min_estimator.value[0]",
                            "mostly": "$variables.mostly",
                            "strict_min": "$variables.strict_min",
                            "meta": {
                                "details": {
                                    "my_min_estimator": "$parameter.my_min_estimator.details",
                                },
                            },
                        },
                    ],
                },
            },
        )

        column_name: str = "fare_amount"

        result = validator.expect_column_values_to_be_between(
            column=column_name,
            result_format="SUMMARY",
            include_config=True,
            auto=True,
            profiler_config=custom_profiler_config.to_json_dict(),
        )
        assert result.success
        assert result.expectation_config["kwargs"] == {
            "column": "fare_amount",
            "min_value": -52.0,
            "strict_min": False,
            "mostly": 1.0,
            "result_format": "SUMMARY",
            "include_config": True,
            "auto": True,
            "profiler_config": custom_profiler_config.to_json_dict(),
            "batch_id": "90bb41c1fbd7c71c05dbc8695320af71",
        }

        result = validator.expect_column_values_to_be_between(
            column=column_name,
            min_value=0.0,
            result_format="SUMMARY",
            include_config=True,
            auto=True,
            profiler_config=custom_profiler_config,
        )
        assert not result.success
        assert result.expectation_config["kwargs"] == {
            "column": "fare_amount",
            "min_value": 0.0,
            "strict_min": False,
            "mostly": 1.0,
            "result_format": "SUMMARY",
            "include_config": True,
            "auto": True,
            "profiler_config": custom_profiler_config.to_json_dict(),
            "batch_id": "90bb41c1fbd7c71c05dbc8695320af71",
        }

        result = validator.expect_column_values_to_be_between(
            column=column_name,
            min_value=0.0,
            mostly=8.75e-1,
            result_format="SUMMARY",
            include_config=True,
            auto=True,
            profiler_config=custom_profiler_config,
        )
        assert result.success
        assert result.expectation_config["kwargs"] == {
            "column": "fare_amount",
            "min_value": 0.0,
            "strict_min": False,
            "mostly": 8.75e-1,
            "result_format": "SUMMARY",
            "include_config": True,
            "auto": True,
            "profiler_config": custom_profiler_config.to_json_dict(),
            "batch_id": "90bb41c1fbd7c71c05dbc8695320af71",
        }

        with pytest.raises(AssertionError) as e:
            # noinspection PyUnusedLocal
            result = validator.expect_column_values_to_be_between(
                column=column_name,
                mostly=1.0,
                result_format="SUMMARY",
                include_config=True,
                auto=False,
                profiler_config=custom_profiler_config,
            )
        assert "min_value and max_value cannot both be None" in str(e.value)

        # Use one batch (at index "1"), loaded by Validator as active_batch, for estimating Expectation argument values.
        batch_request = {
            "datasource_name": "taxi_pandas",
            "data_connector_name": "monthly",
            "data_asset_name": "my_reports",
            "data_connector_query": {
                "index": 1,
            },
        }

        validator: Validator = get_validator_with_expectation_suite(
            data_context=context,
            batch_list=None,
            batch_request=batch_request,
            expectation_suite_name=None,
            expectation_suite=None,
            component_name="profiler",
            persist=False,
        )
        assert len(validator.batches) == 1

        custom_profiler_config = RuleBasedProfilerConfig(
            name="expect_column_values_to_be_between",  # Convention: use "expectation_type" as profiler name.
            config_version=1.0,
            rules={
                "custom_column_values_between_rule": {
                    "parameter_builders": [
                        {
                            "name": "my_min_estimator",
                            "class_name": "MetricMultiBatchParameterBuilder",
                            "module_name": "great_expectations.rule_based_profiler.parameter_builder",
                            "metric_name": "column.min",
                            "metric_domain_kwargs": "$domain.domain_kwargs",
                            "enforce_numeric_metric": True,
                            "replace_nan_with_zero": True,
                        },
                    ],
                    "expectation_configuration_builders": [
                        {
                            "class_name": "DefaultExpectationConfigurationBuilder",
                            "module_name": "great_expectations.rule_based_profiler.expectation_configuration_builder",
                            "expectation_type": "expect_column_values_to_be_between",
                            "column": "$domain.domain_kwargs.column",
                            "min_value": "$parameter.my_min_estimator.value[0]",
                            "mostly": "$variables.mostly",
                            "strict_min": "$variables.strict_min",
                            "meta": {
                                "details": {
                                    "my_min_estimator": "$parameter.my_min_estimator.details",
                                },
                            },
                        },
                    ],
                },
            },
        )

        result = validator.expect_column_values_to_be_between(
            column=column_name,
            mostly=1.0,
            result_format="SUMMARY",
            include_config=True,
            auto=True,
            profiler_config=custom_profiler_config,
        )
        assert result.success
        assert result.expectation_config["kwargs"] == {
            "column": "fare_amount",
            "min_value": -21.0,
            "strict_min": False,
            "mostly": 1.0,
            "result_format": "SUMMARY",
            "include_config": True,
            "auto": True,
            "profiler_config": custom_profiler_config.to_json_dict(),
            "batch_id": "0808e185a52825d22356de2fe00a8f5f",
        }

        result = validator.expect_column_values_to_be_between(
            column=column_name,
            min_value=-21.0,
            mostly=1.0,
            result_format="SUMMARY",
            include_config=True,
            auto=False,
        )
        assert result.success
        assert result.expectation_config["kwargs"] == {
            "column": "fare_amount",
            "min_value": -21.0,
            "mostly": 1.0,
            "result_format": "SUMMARY",
            "include_config": True,
            "auto": False,
            "batch_id": "0808e185a52825d22356de2fe00a8f5f",
        }


@pytest.mark.skipif(
    version.parse(np.version.version) < version.parse("1.21.0"),
    reason="requires numpy version 1.21.0 or newer",
)
@freeze_time(TIMESTAMP)
@pytest.mark.integration
def test_bobby_expect_column_values_to_be_between_auto_yes_default_profiler_config_no_custom_profiler_config_yes(
    bobby_columnar_table_multi_batch_deterministic_data_context,
    bobby_validator: Validator,
):
    # If Expectation already has default Rule-Based Profiler configured, delete it for this test.
    expectation_impl = get_expectation_impl(
        expectation_name="expect_column_values_to_be_between"
    )
    default_profiler_config: Optional[
        RuleBasedProfilerConfig
    ] = expectation_impl.default_kwarg_values.get("profiler_config")

    try:
        if default_profiler_config:
            del expectation_impl.default_kwarg_values["profiler_config"]

        assert "profiler_config" not in expectation_impl.default_kwarg_values

        context: DataContext = (
            bobby_columnar_table_multi_batch_deterministic_data_context
        )
        validator: Validator = bobby_validator

        batch_request: dict
        result: ExpectationValidationResult
        custom_profiler_config: RuleBasedProfilerConfig

        custom_profiler_config = RuleBasedProfilerConfig(
            name="expect_column_values_to_be_between",  # Convention: use "expectation_type" as profiler name.
            config_version=1.0,
            variables={
                "mostly": 1.0,
                "strict_min": False,
                "strict_max": False,
            },
            rules={
                "custom_column_values_between_rule": {
                    "domain_builder": {
                        "class_name": "ColumnDomainBuilder",
                        "module_name": "great_expectations.rule_based_profiler.domain_builder",
                    },
                    "parameter_builders": [
                        {
                            "name": "my_min_estimator",
                            "class_name": "MetricMultiBatchParameterBuilder",
                            "module_name": "great_expectations.rule_based_profiler.parameter_builder",
                            "metric_name": "column.min",
                            "metric_domain_kwargs": "$domain.domain_kwargs",
                            "enforce_numeric_metric": True,
                            "replace_nan_with_zero": True,
                        },
                    ],
                    "expectation_configuration_builders": [
                        {
                            "expectation_type": "expect_column_values_to_be_between",
                            "class_name": "DefaultExpectationConfigurationBuilder",
                            "module_name": "great_expectations.rule_based_profiler.expectation_configuration_builder",
                            "column": "$domain.domain_kwargs.column",
                            "min_value": "$parameter.my_min_estimator.value[0]",
                            "mostly": "$variables.mostly",
                            "strict_min": "$variables.strict_min",
                            "meta": {
                                "details": {
                                    "my_min_estimator": "$parameter.my_min_estimator.details",
                                },
                            },
                        },
                    ],
                },
            },
        )

        column_name: str = "fare_amount"

        result = validator.expect_column_values_to_be_between(
            column=column_name,
            result_format="SUMMARY",
            include_config=True,
            auto=True,
            profiler_config=custom_profiler_config,
        )
        assert result.success
        assert result.expectation_config["kwargs"] == {
            "column": "fare_amount",
            "min_value": -52.0,
            "strict_min": False,
            "mostly": 1.0,
            "result_format": "SUMMARY",
            "include_config": True,
            "auto": True,
            "profiler_config": custom_profiler_config.to_json_dict(),
            "batch_id": "90bb41c1fbd7c71c05dbc8695320af71",
        }

        result = validator.expect_column_values_to_be_between(
            column=column_name,
            min_value=0.0,
            result_format="SUMMARY",
            include_config=True,
            auto=True,
            profiler_config=custom_profiler_config,
        )
        assert not result.success
        assert result.expectation_config["kwargs"] == {
            "column": "fare_amount",
            "min_value": 0.0,
            "strict_min": False,
            "mostly": 1.0,
            "result_format": "SUMMARY",
            "include_config": True,
            "auto": True,
            "profiler_config": custom_profiler_config.to_json_dict(),
            "batch_id": "90bb41c1fbd7c71c05dbc8695320af71",
        }

        result = validator.expect_column_values_to_be_between(
            column=column_name,
            min_value=0.0,
            mostly=8.75e-1,
            result_format="SUMMARY",
            include_config=True,
            auto=True,
            profiler_config=custom_profiler_config,
        )
        assert result.success
        assert result.expectation_config["kwargs"] == {
            "column": "fare_amount",
            "min_value": 0.0,
            "strict_min": False,
            "mostly": 8.75e-1,
            "result_format": "SUMMARY",
            "include_config": True,
            "auto": True,
            "profiler_config": custom_profiler_config.to_json_dict(),
            "batch_id": "90bb41c1fbd7c71c05dbc8695320af71",
        }

        with pytest.raises(AssertionError) as e:
            # noinspection PyUnusedLocal
            result = validator.expect_column_values_to_be_between(
                column=column_name,
                mostly=1.0,
                result_format="SUMMARY",
                include_config=True,
                auto=False,
                profiler_config=custom_profiler_config,
            )
        assert "min_value and max_value cannot both be None" in str(e.value)

        batch_request = {
            "datasource_name": "taxi_pandas",
            "data_connector_name": "monthly",
            "data_asset_name": "my_reports",
            "data_connector_query": {
                "index": 1,
            },
        }

        validator: Validator = get_validator_with_expectation_suite(
            data_context=context,
            batch_list=None,
            batch_request=batch_request,
            expectation_suite_name=None,
            expectation_suite=None,
            component_name="profiler",
            persist=False,
        )
        assert len(validator.batches) == 1

        custom_profiler_config = RuleBasedProfilerConfig(
            name="expect_column_values_to_be_between",  # Convention: use "expectation_type" as profiler name.
            config_version=1.0,
            variables={
                "mostly": 1.0,
                "strict_min": False,
                "strict_max": False,
            },
            rules={
                "custom_column_values_between_rule": {
                    "domain_builder": {
                        "class_name": "ColumnDomainBuilder",
                        "module_name": "great_expectations.rule_based_profiler.domain_builder",
                    },
                    "parameter_builders": [
                        {
                            "name": "my_min_estimator",
                            "class_name": "MetricMultiBatchParameterBuilder",
                            "module_name": "great_expectations.rule_based_profiler.parameter_builder",
                            "metric_name": "column.min",
                            "metric_domain_kwargs": "$domain.domain_kwargs",
                            "enforce_numeric_metric": True,
                            "replace_nan_with_zero": True,
                        },
                    ],
                    "expectation_configuration_builders": [
                        {
                            "expectation_type": "expect_column_values_to_be_between",
                            "class_name": "DefaultExpectationConfigurationBuilder",
                            "module_name": "great_expectations.rule_based_profiler.expectation_configuration_builder",
                            "column": "$domain.domain_kwargs.column",
                            "min_value": "$parameter.my_min_estimator.value[0]",
                            "mostly": "$variables.mostly",
                            "strict_min": "$variables.strict_min",
                            "meta": {
                                "details": {
                                    "my_min_estimator": "$parameter.my_min_estimator.details",
                                },
                            },
                        },
                    ],
                },
            },
        )

        result = validator.expect_column_values_to_be_between(
            column=column_name,
            mostly=1.0,
            result_format="SUMMARY",
            include_config=True,
            auto=True,
            profiler_config=custom_profiler_config,
        )
        assert result.success
        assert result.expectation_config["kwargs"] == {
            "column": "fare_amount",
            "min_value": -21.0,
            "strict_min": False,
            "mostly": 1.0,
            "result_format": "SUMMARY",
            "include_config": True,
            "auto": True,
            "profiler_config": custom_profiler_config.to_json_dict(),
            "batch_id": "0808e185a52825d22356de2fe00a8f5f",
        }

        result = validator.expect_column_values_to_be_between(
            column=column_name,
            min_value=-21.0,
            mostly=1.0,
            result_format="SUMMARY",
            include_config=True,
            auto=False,
        )
        assert result.success
        assert result.expectation_config["kwargs"] == {
            "column": "fare_amount",
            "min_value": -21.0,
            "mostly": 1.0,
            "result_format": "SUMMARY",
            "include_config": True,
            "auto": False,
            "batch_id": "0808e185a52825d22356de2fe00a8f5f",
        }
    finally:
        expectation_impl.default_kwarg_values[
            "profiler_config"
        ] = default_profiler_config


@pytest.mark.skipif(
    version.parse(np.version.version) < version.parse("1.21.0"),
    reason="requires numpy version 1.21.0 or newer",
)
@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
@pytest.mark.slow  # 4.83s
@pytest.mark.integration
def test_bobster_profiler_user_workflow_multi_batch_row_count_range_rule_bootstrap_estimator(
    mock_emit,
    caplog,
    bobster_columnar_table_multi_batch_normal_mean_5000_stdev_1000_data_context,
    bobster_columnar_table_multi_batch_normal_mean_5000_stdev_1000,
    set_consistent_seed_within_numeric_metric_range_multi_batch_parameter_builder,
):
    # Load data context
    data_context: DataContext = (
        bobster_columnar_table_multi_batch_normal_mean_5000_stdev_1000_data_context
    )

    # Load profiler configs & loop (run tests for each one)
    yaml_config: str = bobster_columnar_table_multi_batch_normal_mean_5000_stdev_1000[
        "profiler_config"
    ]

    # Instantiate Profiler
    profiler_config: CommentedMap = yaml.load(yaml_config)

    # Roundtrip through schema validation to remove any illegal fields add/or restore any missing fields.
    deserialized_config: dict = ruleBasedProfilerConfigSchema.load(profiler_config)
    serialized_config: dict = ruleBasedProfilerConfigSchema.dump(deserialized_config)

    # `class_name`/`module_name` are generally consumed through `instantiate_class_from_config`
    # so we need to manually remove those values if we wish to use the **kwargs instantiation pattern
    serialized_config.pop("class_name")
    serialized_config.pop("module_name")

    profiler: RuleBasedProfiler = RuleBasedProfiler(
        **serialized_config,
        data_context=data_context,
    )

    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }

    result: RuleBasedProfilerResult = profiler.run(batch_request=batch_request)

    expect_table_row_count_to_be_between_expectation_configuration_kwargs: dict = (
        result.expectation_configurations[0].to_json_dict()["kwargs"]
    )
    min_value: int = (
        expect_table_row_count_to_be_between_expectation_configuration_kwargs[
            "min_value"
        ]
    )
    max_value: int = (
        expect_table_row_count_to_be_between_expectation_configuration_kwargs[
            "max_value"
        ]
    )

    assert (
        bobster_columnar_table_multi_batch_normal_mean_5000_stdev_1000[
            "test_configuration_bootstrap_estimator"
        ]["expect_table_row_count_to_be_between_min_value_mean_value"]
        < min_value
        < bobster_columnar_table_multi_batch_normal_mean_5000_stdev_1000[
            "test_configuration_bootstrap_estimator"
        ]["expect_table_row_count_to_be_between_mean_value"]
    )
    assert (
        bobster_columnar_table_multi_batch_normal_mean_5000_stdev_1000[
            "test_configuration_bootstrap_estimator"
        ]["expect_table_row_count_to_be_between_mean_value"]
        < max_value
        < bobster_columnar_table_multi_batch_normal_mean_5000_stdev_1000[
            "test_configuration_bootstrap_estimator"
        ]["expect_table_row_count_to_be_between_max_value_mean_value"]
    )

    assert mock_emit.call_count == 3

    assert all(
        payload[0][0]["event"] == "data_context.get_batch_list"
        for payload in mock_emit.call_args_list[:-1]
    )

    # noinspection PyUnresolvedReferences
    expected_profiler_run_event: mock._Call = mock.call(
        {
            "event_payload": {
                "anonymized_name": "510b23dfd19c492f33d114b184f245e8",
                "config_version": 1.0,
                "anonymized_rules": [
                    {
                        "anonymized_name": "7980584b8d0c7c8a66dbeaaf4b067885",
                        "anonymized_domain_builder": {
                            "parent_class": "TableDomainBuilder"
                        },
                        "anonymized_parameter_builders": [
                            {
                                "parent_class": "NumericMetricRangeMultiBatchParameterBuilder",
                                "anonymized_name": "dc1bc513697628c3a7f5b73494234a01",
                            }
                        ],
                        "anonymized_expectation_configuration_builders": [
                            {
                                "parent_class": "DefaultExpectationConfigurationBuilder",
                                "expectation_type": "expect_table_row_count_to_be_between",
                            }
                        ],
                    }
                ],
                "rule_count": 1,
                "variable_count": 6,
            },
            "event": "profiler.run",
            "success": True,
        }
    )
    assert mock_emit.call_args_list[-1] == expected_profiler_run_event

    # Confirm that logs do not contain any exceptions or invalid messages
    assert not usage_stats_exceptions_exist(messages=caplog.messages)
    assert not usage_stats_invalid_messages_exist(messages=caplog.messages)


@pytest.mark.skipif(
    version.parse(np.version.version) < version.parse("1.21.0"),
    reason="requires numpy version 1.21.0 or newer",
)
@freeze_time(TIMESTAMP)
@pytest.mark.slow  # 4.24s
@pytest.mark.integration
def test_bobster_expect_table_row_count_to_be_between_auto_yes_default_profiler_config_yes_custom_profiler_config_no(
    bobster_validator: Validator,
):
    validator: Validator = bobster_validator

    result: ExpectationValidationResult = (
        validator.expect_table_row_count_to_be_between(
            result_format="SUMMARY",
            include_config=True,
            auto=True,
        )
    )
    assert result.expectation_config.kwargs["auto"]


@pytest.mark.skipif(
    version.parse(np.version.version) < version.parse("1.21.0"),
    reason="requires numpy version 1.21.0 or newer",
)
@pytest.mark.slow  # 2.02s
@pytest.mark.integration
def test_quentin_expect_expect_table_columns_to_match_set_auto_yes_default_profiler_config_yes_custom_profiler_config_no(
    quentin_validator: Validator,
):
    validator: Validator = quentin_validator

    # Use all batches, loaded by Validator, for estimating Expectation argument values.
    result: ExpectationValidationResult = validator.expect_table_columns_to_match_set(
        result_format="SUMMARY",
        include_config=True,
        auto=True,
    )
    assert result.success

    value: Any
    expectation_config_kwargs: dict = {
        key: value
        for key, value in result.expectation_config["kwargs"].items()
        if key != "column_set"
    }
    assert expectation_config_kwargs == {
        "exact_match": None,
        "result_format": "SUMMARY",
        "include_config": True,
        "auto": True,
        "batch_id": "84000630d1b69a0fe870c94fb26a32bc",
    }

    column_set_expected: List[str] = [
        "total_amount",
        "tip_amount",
        "payment_type",
        "pickup_datetime",
        "trip_distance",
        "dropoff_location_id",
        "improvement_surcharge",
        "vendor_id",
        "tolls_amount",
        "congestion_surcharge",
        "rate_code_id",
        "pickup_location_id",
        "extra",
        "fare_amount",
        "mta_tax",
        "dropoff_datetime",
        "store_and_fwd_flag",
        "passenger_count",
    ]
    column_set_computed: List[str] = result.expectation_config["kwargs"]["column_set"]

    assert len(column_set_computed) == len(column_set_expected)
    assert set(column_set_computed) == set(column_set_expected)


@pytest.mark.skipif(
    version.parse(np.version.version) < version.parse("1.21.0"),
    reason="requires numpy version 1.21.0 or newer",
)
@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
@pytest.mark.slow  # 15.07s
@pytest.mark.integration
def test_quentin_profiler_user_workflow_multi_batch_quantiles_value_ranges_rule(
    mock_emit,
    caplog,
    quentin_columnar_table_multi_batch_data_context,
    quentin_columnar_table_multi_batch,
    set_consistent_seed_within_numeric_metric_range_multi_batch_parameter_builder,
):
    # Load data context
    data_context: DataContext = quentin_columnar_table_multi_batch_data_context

    # Load profiler configs & loop (run tests for each one)
    yaml_config: str = quentin_columnar_table_multi_batch["profiler_config"]

    # Instantiate Profiler
    profiler_config: CommentedMap = yaml.load(yaml_config)

    # Roundtrip through schema validation to remove any illegal fields add/or restore any missing fields.
    deserialized_config: dict = ruleBasedProfilerConfigSchema.load(profiler_config)
    serialized_config: dict = ruleBasedProfilerConfigSchema.dump(deserialized_config)

    # `class_name`/`module_name` are generally consumed through `instantiate_class_from_config`
    # so we need to manually remove those values if we wish to use the **kwargs instantiation pattern
    serialized_config.pop("class_name")
    serialized_config.pop("module_name")

    profiler: RuleBasedProfiler = RuleBasedProfiler(
        **serialized_config,
        data_context=data_context,
    )

    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }

    result: RuleBasedProfilerResult = profiler.run(batch_request=batch_request)

    expectation_configuration: ExpectationConfiguration
    expectation_configuration_dict: dict
    column_name: str
    expectation_kwargs: dict
    expect_column_quantile_values_to_be_between_expectation_configurations_kwargs_dict: Dict[
        str, dict
    ] = {
        expectation_configuration_dict["kwargs"][
            "column"
        ]: expectation_configuration_dict["kwargs"]
        for expectation_configuration_dict in [
            expectation_configuration.to_json_dict()
            for expectation_configuration in result.expectation_configurations
        ]
    }
    expect_column_quantile_values_to_be_between_expectation_configurations_value_ranges_by_column: Dict[
        str, List[List[Number]]
    ] = {
        column_name: expectation_kwargs["quantile_ranges"]["value_ranges"]
        for column_name, expectation_kwargs in expect_column_quantile_values_to_be_between_expectation_configurations_kwargs_dict.items()
    }

    assert (
        expect_column_quantile_values_to_be_between_expectation_configurations_value_ranges_by_column[
            "tolls_amount"
        ]
        == quentin_columnar_table_multi_batch["test_configuration"][
            "expect_column_quantile_values_to_be_between_quantile_ranges_by_column"
        ]["tolls_amount"]
    )

    value_ranges: List[Tuple[Tuple[float, float]]]
    paired_quantiles: zip
    column_quantiles: List[List[Number]]
    idx: int
    for (
        column_name,
        column_quantiles,
    ) in (
        expect_column_quantile_values_to_be_between_expectation_configurations_value_ranges_by_column.items()
    ):
        paired_quantiles = zip(
            column_quantiles,
            quentin_columnar_table_multi_batch["test_configuration"][
                "expect_column_quantile_values_to_be_between_quantile_ranges_by_column"
            ][column_name],
        )
        for value_ranges in list(paired_quantiles):
            for idx in range(2):
                np.testing.assert_allclose(
                    actual=value_ranges[0][idx],
                    desired=value_ranges[1][idx],
                    rtol=RTOL,
                    atol=ATOL,
                    err_msg=f"Actual value of {value_ranges[0][idx]} differs from expected value of {value_ranges[1][idx]} by more than {ATOL + RTOL * abs(value_ranges[1][idx])} tolerance.",
                )

    assert mock_emit.call_count == 11

    assert all(
        payload[0][0]["event"] == "data_context.get_batch_list"
        for payload in mock_emit.call_args_list[:-1]
    )

    # noinspection PyUnresolvedReferences
    expected_profiler_run_event: mock._Call = mock.call(
        {
            "event_payload": {
                "anonymized_name": "0592a9cacfa4ce642536161869d1ba19",
                "config_version": 1.0,
                "anonymized_rules": [
                    {
                        "anonymized_name": "f54fc6b216560f2a56a3cced587fb6e3",
                        "anonymized_domain_builder": {
                            "parent_class": "ColumnDomainBuilder",
                        },
                        "anonymized_parameter_builders": [
                            {
                                "parent_class": "NumericMetricRangeMultiBatchParameterBuilder",
                                "anonymized_name": "71243c854c0c04e9e014d02a793abe55",
                            }
                        ],
                        "anonymized_expectation_configuration_builders": [
                            {
                                "parent_class": "DefaultExpectationConfigurationBuilder",
                                "expectation_type": "expect_column_quantile_values_to_be_between",
                            }
                        ],
                    }
                ],
                "rule_count": 1,
                "variable_count": 8,
            },
            "event": "profiler.run",
            "success": True,
        }
    )
    assert mock_emit.call_args_list[-1] == expected_profiler_run_event

    # Confirm that logs do not contain any exceptions or invalid messages
    assert not usage_stats_exceptions_exist(messages=caplog.messages)
    assert not usage_stats_invalid_messages_exist(messages=caplog.messages)


@pytest.mark.skipif(
    version.parse(np.version.version) < version.parse("1.21.0"),
    reason="requires numpy version 1.21.0 or newer",
)
@freeze_time(TIMESTAMP)
@pytest.mark.slow  # 2.40s
@pytest.mark.integration
def test_quentin_expect_column_quantile_values_to_be_between_auto_yes_default_profiler_config_yes_custom_profiler_config_yes(
    quentin_validator: Validator,
):
    with restore_profiler_config(
        expect_column_quantile_values_to_be_between.ExpectColumnQuantileValuesToBeBetween
    ):
        validator: Validator = quentin_validator

        custom_profiler_config = RuleBasedProfilerConfig(
            name="expect_column_quantile_values_to_be_between",  # Convention: use "expectation_type" as profiler name.
            config_version=1.0,
            variables={
                "quantiles": [2.5e-1, 5.0e-1, 7.5e-1],
                "allow_relative_error": "linear",
                "n_resamples": 9139,
                "random_seed": 43792,
                "false_positive_rate": 5.0e-2,
                "quantile_statistic_interpolation_method": "auto",
                "quantile_bias_correction": True,
                "quantile_bias_std_error_ratio_threshold": 0.25,
            },
            rules={
                "column_quantiles_rule": {
                    "domain_builder": {
                        "class_name": "ColumnDomainBuilder",
                        "module_name": "great_expectations.rule_based_profiler.domain_builder",
                    },
                    "parameter_builders": [
                        {
                            "name": "quantile_value_ranges",
                            "class_name": "NumericMetricRangeMultiBatchParameterBuilder",
                            "module_name": "great_expectations.rule_based_profiler.parameter_builder",
                            "metric_name": "column.quantile_values",
                            "metric_domain_kwargs": "$domain.domain_kwargs",
                            "metric_value_kwargs": {
                                "quantiles": "$variables.quantiles",
                                "allow_relative_error": "$variables.allow_relative_error",
                            },
                            "n_resamples": "$variables.n_resamples",
                            "random_seed": "$variables.random_seed",
                            "false_positive_rate": "$variables.false_positive_rate",
                            "quantile_statistic_interpolation_method": "$variables.quantile_statistic_interpolation_method",
                            "quantile_bias_correction": "$variables.quantile_bias_correction",
                            "quantile_bias_std_error_ratio_threshold": "$variables.quantile_bias_std_error_ratio_threshold",
                            "round_decimals": None,
                        }
                    ],
                    "expectation_configuration_builders": [
                        {
                            "expectation_type": "expect_column_quantile_values_to_be_between",
                            "class_name": "DefaultExpectationConfigurationBuilder",
                            "module_name": "great_expectations.rule_based_profiler.expectation_configuration_builder",
                            "column": "$domain.domain_kwargs.column",
                            "quantile_ranges": {
                                "quantiles": "$variables.quantiles",
                                "value_ranges": "$parameter.quantile_value_ranges.value",
                            },
                            "allow_relative_error": "$variables.allow_relative_error",
                            "meta": {
                                "profiler_details": "$parameter.quantile_value_ranges.details"
                            },
                        }
                    ],
                }
            },
        )

        result = validator.expect_column_quantile_values_to_be_between(
            column="fare_amount",
            result_format="SUMMARY",
            include_config=True,
            auto=True,
            profiler_config=custom_profiler_config,
        )
        assert result.success

        value_ranges_expected = [
            [
                5.94,
                6.5,
            ],
            [
                8.44,
                9.56,
            ],
            [
                13.44,
                15.62,
            ],
        ]

        value_ranges_computed = result.expectation_config["kwargs"]["quantile_ranges"][
            "value_ranges"
        ]

        assert len(value_ranges_computed) == len(value_ranges_expected)

        paired_quantiles = zip(
            value_ranges_computed,
            value_ranges_expected,
        )
        for value_ranges in list(paired_quantiles):
            for idx in range(2):
                np.testing.assert_allclose(
                    actual=value_ranges[0][idx],
                    desired=value_ranges[1][idx],
                    rtol=RTOL,
                    atol=ATOL,
                    err_msg=f"Actual value of {value_ranges[0][idx]} differs from expected value of {value_ranges[1][idx]} by more than {ATOL + RTOL * abs(value_ranges[1][idx])} tolerance.",
                )

        custom_profiler_config = RuleBasedProfilerConfig(
            name="expect_column_quantile_values_to_be_between",  # Convention: use "expectation_type" as profiler name.
            config_version=1.0,
            variables={
                "quantiles": [2.5e-1, 5.0e-1, 7.5e-1],
                "allow_relative_error": "linear",
                "n_resamples": 9139,
                "random_seed": 43792,
                "false_positive_rate": 5.0e-2,
                "quantile_statistic_interpolation_method": "auto",
                "quantile_bias_correction": True,
                "quantile_bias_std_error_ratio_threshold": 0.25,
            },
            rules={
                "column_quantiles_rule": {
                    "domain_builder": {
                        "class_name": "ColumnDomainBuilder",
                        "module_name": "great_expectations.rule_based_profiler.domain_builder",
                    },
                    "parameter_builders": [
                        {
                            "name": "quantile_value_ranges",
                            "class_name": "NumericMetricRangeMultiBatchParameterBuilder",
                            "module_name": "great_expectations.rule_based_profiler.parameter_builder",
                            "metric_name": "column.quantile_values",
                            "metric_domain_kwargs": "$domain.domain_kwargs",
                            "metric_value_kwargs": {
                                "quantiles": "$variables.quantiles",
                                "allow_relative_error": "$variables.allow_relative_error",
                            },
                            "n_resamples": "$variables.n_resamples",
                            "random_seed": "$variables.random_seed",
                            "false_positive_rate": "$variables.false_positive_rate",
                            "quantile_statistic_interpolation_method": "$variables.quantile_statistic_interpolation_method",
                            "quantile_bias_correction": "$variables.quantile_bias_correction",
                            "quantile_bias_std_error_ratio_threshold": "$variables.quantile_bias_std_error_ratio_threshold",
                            "round_decimals": "$variables.round_decimals",
                        }
                    ],
                    "expectation_configuration_builders": [
                        {
                            "expectation_type": "expect_column_quantile_values_to_be_between",
                            "class_name": "DefaultExpectationConfigurationBuilder",
                            "module_name": "great_expectations.rule_based_profiler.expectation_configuration_builder",
                            "column": "$domain.domain_kwargs.column",
                            "quantile_ranges": {
                                "quantiles": "$variables.quantiles",
                                "value_ranges": "$parameter.quantile_value_ranges.value",
                            },
                            "allow_relative_error": "$variables.allow_relative_error",
                            "meta": {
                                "profiler_details": "$parameter.quantile_value_ranges.details"
                            },
                        }
                    ],
                }
            },
        )

        result = validator.expect_column_quantile_values_to_be_between(
            column="fare_amount",
            result_format="SUMMARY",
            include_config=True,
            auto=True,
            profiler_config=custom_profiler_config,
        )
        assert result.success


@pytest.mark.skipif(
    version.parse(np.version.version) < version.parse("1.21.0"),
    reason="requires numpy version 1.21.0 or newer",
)
@freeze_time(TIMESTAMP)
@pytest.mark.slow  # 2.15s
@pytest.mark.integration
def test_quentin_expect_column_values_to_be_in_set_auto_yes_default_profiler_config_yes_custom_profiler_config_no(
    quentin_validator: Validator,
):
    validator: Validator = quentin_validator

    # Use all batches, loaded by Validator, for estimating Expectation argument values.
    result: ExpectationValidationResult = validator.expect_column_values_to_be_in_set(
        column="passenger_count",
        result_format="SUMMARY",
        include_config=True,
        auto=True,
    )
    assert result.success

    key: str
    value: Any
    expectation_config_kwargs: dict = {
        key: value
        for key, value in result.expectation_config["kwargs"].items()
        if key != "value_set"
    }
    assert expectation_config_kwargs == {
        "column": "passenger_count",
        "mostly": 1.0,
        "result_format": "SUMMARY",
        "include_config": True,
        "auto": True,
        "batch_id": "84000630d1b69a0fe870c94fb26a32bc",
    }

    value_set_expected: List[int] = [0, 1, 2, 3, 4, 5, 6, 7]
    value_set_computed: List[int] = result.expectation_config["kwargs"]["value_set"]

    assert value_set_computed == value_set_expected


@pytest.mark.skipif(
    version.parse(np.version.version) < version.parse("1.21.0"),
    reason="requires numpy version 1.21.0 or newer",
)
@freeze_time(TIMESTAMP)
@pytest.mark.slow  # 3.44s
@pytest.mark.integration
def test_quentin_expect_column_min_to_be_between_auto_yes_default_profiler_config_yes_custom_profiler_config_no(
    quentin_validator: Validator,
):
    validator: Validator = quentin_validator

    # Use all batches, loaded by Validator, for estimating Expectation argument values.
    result: ExpectationValidationResult = validator.expect_column_min_to_be_between(
        column="fare_amount",
        result_format="SUMMARY",
        include_config=True,
        auto=True,
    )

    key: str
    value: Any
    expectation_config_kwargs: dict = {
        key: value
        for key, value in result.expectation_config["kwargs"].items()
        if key
        not in [
            "min_value",
            "max_value",
        ]
    }
    assert expectation_config_kwargs == {
        "column": "fare_amount",
        "strict_min": False,
        "strict_max": False,
        "result_format": "SUMMARY",
        "include_config": True,
        "auto": True,
        "batch_id": "84000630d1b69a0fe870c94fb26a32bc",
    }


@pytest.mark.skipif(
    version.parse(np.version.version) < version.parse("1.21.0"),
    reason="requires numpy version 1.21.0 or newer",
)
@freeze_time(TIMESTAMP)
@pytest.mark.slow  # 2.12s
@pytest.mark.integration
def test_quentin_expect_column_max_to_be_between_auto_yes_default_profiler_config_yes_custom_profiler_config_no(
    quentin_validator: Validator,
):
    validator: Validator = quentin_validator

    # Use all batches, loaded by Validator, for estimating Expectation argument values.
    result: ExpectationValidationResult = validator.expect_column_max_to_be_between(
        column="fare_amount",
        result_format="SUMMARY",
        include_config=True,
        auto=True,
    )
    assert result.success

    key: str
    value: Any
    expectation_config_kwargs: dict = {
        key: value
        for key, value in result.expectation_config["kwargs"].items()
        if key
        not in [
            "min_value",
            "max_value",
        ]
    }
    assert expectation_config_kwargs == {
        "column": "fare_amount",
        "strict_min": False,
        "strict_max": False,
        "result_format": "SUMMARY",
        "include_config": True,
        "auto": True,
        "batch_id": "84000630d1b69a0fe870c94fb26a32bc",
    }

    rtol: float = 2.0e1 * RTOL
    atol: float = 2.0e1 * ATOL

    min_value_actual: float = result.expectation_config["kwargs"]["min_value"]
    min_value_expected: float = 150.0

    np.testing.assert_allclose(
        actual=float(min_value_actual),
        desired=float(min_value_expected),
        rtol=rtol,
        atol=atol,
        err_msg=f"Actual value of {min_value_actual} differs from expected value of {min_value_expected} by more than {atol + rtol * abs(min_value_expected)} tolerance.",
    )

    max_value_actual: float = result.expectation_config["kwargs"]["max_value"]
    max_value_expected: float = 429490.2
    np.testing.assert_allclose(
        actual=float(max_value_actual),
        desired=float(max_value_expected),
        rtol=rtol,
        atol=atol,
        err_msg=f"Actual value of {max_value_actual} differs from expected value of {max_value_expected} by more than {atol + rtol * abs(max_value_expected)} tolerance.",
    )


@pytest.mark.skipif(
    version.parse(np.version.version) < version.parse("1.21.0"),
    reason="requires numpy version 1.21.0 or newer",
)
@freeze_time(TIMESTAMP)
@pytest.mark.slow  # 2.24s
@pytest.mark.integration
def test_quentin_expect_column_unique_value_count_to_be_between_auto_yes_default_profiler_config_yes_custom_profiler_config_no(
    quentin_validator: Validator,
) -> None:
    validator: Validator = quentin_validator

    test_cases: Tuple[Tuple[str, int, int], ...] = (
        ("pickup_location_id", 118, 214),
        ("dropoff_location_id", 184, 238),
    )

    for column_name, min_value_expected, max_value_expected in test_cases:
        # Use all batches, loaded by Validator, for estimating Expectation argument values.
        result = validator.expect_column_unique_value_count_to_be_between(
            column=column_name,
            result_format="SUMMARY",
            include_config=True,
            auto=True,
        )
        assert result.success

        key: str
        value: Any
        expectation_config_kwargs: dict = {
            key: value
            for key, value in result.expectation_config["kwargs"].items()
            if key
            not in [
                "min_value",
                "max_value",
            ]
        }
        assert expectation_config_kwargs == {
            "column": column_name,
            "strict_min": False,
            "strict_max": False,
            "result_format": "SUMMARY",
            "include_config": True,
            "auto": True,
            "batch_id": "84000630d1b69a0fe870c94fb26a32bc",
        }

        min_value_actual: int = result.expectation_config["kwargs"]["min_value"]
        assert min_value_expected - 1 <= min_value_actual <= min_value_expected + 1

        max_value_actual: int = result.expectation_config["kwargs"]["max_value"]
        assert max_value_expected - 1 <= max_value_actual <= max_value_expected + 1


@pytest.mark.skipif(
    version.parse(np.version.version) < version.parse("1.21.0"),
    reason="requires numpy version 1.21.0 or newer",
)
@freeze_time(TIMESTAMP)
@pytest.mark.slow  # 2.67s
@pytest.mark.integration
def test_quentin_expect_column_proportion_of_unique_values_to_be_between_auto_yes_default_profiler_config_yes_custom_profiler_config_no(
    quentin_validator: Validator,
) -> None:
    validator: Validator = quentin_validator

    test_cases: Tuple[Tuple[str, float, float], ...] = (
        ("pickup_datetime", 0.0, 1.0),
        ("dropoff_datetime", 0.0, 1.0),
    )

    column_name: str
    min_value_expected: float
    max_value_expected: float
    for column_name, min_value_expected, max_value_expected in test_cases:
        # Use all batches, loaded by Validator, for estimating Expectation argument values.
        result = validator.expect_column_proportion_of_unique_values_to_be_between(
            column=column_name,
            result_format="SUMMARY",
            include_config=True,
            auto=True,
        )
        assert result.success

        key: str
        value: Any
        expectation_config_kwargs: dict = {
            key: value
            for key, value in result.expectation_config["kwargs"].items()
            if key
            not in [
                "min_value",
                "max_value",
            ]
        }
        assert expectation_config_kwargs == {
            "column": column_name,
            "strict_min": False,
            "strict_max": False,
            "result_format": "SUMMARY",
            "include_config": True,
            "auto": True,
            "batch_id": "84000630d1b69a0fe870c94fb26a32bc",
        }

        min_value_actual: int = result.expectation_config["kwargs"]["min_value"]
        assert min_value_expected <= min_value_actual

        max_value_actual: int = result.expectation_config["kwargs"]["max_value"]
        assert max_value_expected >= max_value_actual


@pytest.mark.skipif(
    version.parse(np.version.version) < version.parse("1.21.0"),
    reason="requires numpy version 1.21.0 or newer",
)
@freeze_time(TIMESTAMP)
@pytest.mark.slow  # 2.26s
@pytest.mark.integration
def test_quentin_expect_column_sum_to_be_between_auto_yes_default_profiler_config_yes_custom_profiler_config_no(
    quentin_validator: Validator,
) -> None:
    validator: Validator = quentin_validator

    test_cases: Tuple[Tuple[str, int, int], ...] = (
        ("passenger_count", 0, 20000),
        ("congestion_surcharge", 0, 25000),
    )

    column_name: str
    min_value_expected: float
    max_value_expected: float
    for column_name, min_value_expected, max_value_expected in test_cases:
        # Use all batches, loaded by Validator, for estimating Expectation argument values.
        result = validator.expect_column_sum_to_be_between(
            column=column_name,
            result_format="SUMMARY",
            include_config=True,
            auto=True,
        )
        assert result.success

        key: str
        value: Any
        expectation_config_kwargs: dict = {
            key: value
            for key, value in result.expectation_config["kwargs"].items()
            if key
            not in [
                "min_value",
                "max_value",
            ]
        }
        assert expectation_config_kwargs == {
            "column": column_name,
            "strict_min": False,
            "strict_max": False,
            "result_format": "SUMMARY",
            "include_config": True,
            "auto": True,
            "batch_id": "84000630d1b69a0fe870c94fb26a32bc",
        }

        min_value_actual: int = result.expectation_config["kwargs"]["min_value"]
        assert min_value_expected <= min_value_actual

        max_value_actual: int = result.expectation_config["kwargs"]["max_value"]
        assert max_value_expected >= max_value_actual


@pytest.mark.skipif(
    version.parse(np.version.version) < version.parse("1.21.0"),
    reason="requires numpy version 1.21.0 or newer",
)
@freeze_time(TIMESTAMP)
@pytest.mark.slow  # 2.29s
@pytest.mark.integration
def test_quentin_expect_column_stdev_to_be_between_auto_yes_default_profiler_config_yes_custom_profiler_config_no(
    quentin_validator: Validator,
):
    validator: Validator = quentin_validator

    test_cases: Tuple[Tuple[str, float, float], ...] = (
        ("fare_amount", 10.0, 4294.79),
        ("passenger_count", 1.0, 2.0),
    )

    for column_name, min_value_expected, max_value_expected in test_cases:
        # Use all batches, loaded by Validator, for estimating Expectation argument values.
        result = validator.expect_column_stdev_to_be_between(
            column=column_name,
            result_format="SUMMARY",
            include_config=True,
            auto=True,
        )
        assert result.success

        key: str
        value: Any
        expectation_config_kwargs: dict = {
            key: value
            for key, value in result.expectation_config["kwargs"].items()
            if key
            not in [
                "min_value",
                "max_value",
            ]
        }
        assert expectation_config_kwargs == {
            "column": column_name,
            "strict_min": False,
            "strict_max": False,
            "result_format": "SUMMARY",
            "include_config": True,
            "auto": True,
            "batch_id": "84000630d1b69a0fe870c94fb26a32bc",
        }

        min_value_actual: int = result.expectation_config["kwargs"]["min_value"]
        assert min_value_expected - 1 <= min_value_actual <= min_value_expected + 1

        max_value_actual: int = result.expectation_config["kwargs"]["max_value"]
        assert max_value_expected - 1 <= max_value_actual <= max_value_expected + 1
