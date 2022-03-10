import datetime
import uuid
from numbers import Number
from typing import Any, Dict, List, Optional, Tuple, cast
from unittest import mock

import numpy as np
import pandas as pd
import pytest
from freezegun import freeze_time
from packaging import version
from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap

import great_expectations.exceptions as ge_exceptions
from great_expectations import DataContext
from great_expectations.core import ExpectationSuite, ExpectationValidationResult
from great_expectations.core.batch import BatchRequest
from great_expectations.datasource import DataConnector, Datasource
from great_expectations.expectations.registry import get_expectation_impl
from great_expectations.rule_based_profiler.config.base import (
    RuleBasedProfilerConfig,
    ruleBasedProfilerConfigSchema,
)
from great_expectations.rule_based_profiler.rule_based_profiler import RuleBasedProfiler
from great_expectations.validator.metric_configuration import MetricConfiguration
from great_expectations.validator.validator import Validator
from tests.core.usage_statistics.util import (
    usage_stats_exceptions_exist,
    usage_stats_invalid_messages_exist,
)
from tests.rule_based_profiler.conftest import ATOL, RTOL

yaml = YAML()


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


@freeze_time("09/26/2019 13:42:41")
@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
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

    expectation_suite: ExpectationSuite = profiler.run(
        expectation_suite_name=alice_columnar_table_single_batch[
            "expected_expectation_suite_name"
        ],
        include_citation=True,
    )
    assert (
        expectation_suite
        == alice_columnar_table_single_batch["expected_expectation_suite"]
    )

    assert mock_emit.call_count == 45

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
                            "anonymized_batch_request": {
                                "anonymized_batch_request_required_top_level_properties": {
                                    "anonymized_datasource_name": "aaea35c1421a0d3b7afe28cdfbd4b8d1",
                                    "anonymized_data_connector_name": "5bed7acb38185c12e3050a0c34f27ff4",
                                    "anonymized_data_asset_name": "d1c08815a25bc17c0493a50cd51ba31f",
                                }
                            },
                        },
                        "anonymized_parameter_builders": [
                            {
                                "parent_class": "MetricMultiBatchParameterBuilder",
                                "anonymized_name": "2b4df3c7cf39207db3e08477e1ea8f79",
                                "anonymized_batch_request": {
                                    "anonymized_batch_request_required_top_level_properties": {
                                        "anonymized_datasource_name": "aaea35c1421a0d3b7afe28cdfbd4b8d1",
                                        "anonymized_data_connector_name": "5bed7acb38185c12e3050a0c34f27ff4",
                                        "anonymized_data_asset_name": "d1c08815a25bc17c0493a50cd51ba31f",
                                    }
                                },
                            },
                            {
                                "parent_class": "MetricMultiBatchParameterBuilder",
                                "anonymized_name": "bea5e4c3943006d008899cdb1ebc3fb4",
                                "anonymized_batch_request": {
                                    "anonymized_batch_request_required_top_level_properties": {
                                        "anonymized_datasource_name": "aaea35c1421a0d3b7afe28cdfbd4b8d1",
                                        "anonymized_data_connector_name": "5bed7acb38185c12e3050a0c34f27ff4",
                                        "anonymized_data_asset_name": "d1c08815a25bc17c0493a50cd51ba31f",
                                    }
                                },
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
                                "anonymized_condition": "a2e517a17f9590295b4210da954796cf",
                            },
                            {
                                "parent_class": "DefaultExpectationConfigurationBuilder",
                                "anonymized_expectation_type": "5a4993ff394c8cf957dbe7964798f5a5",
                                "anonymized_condition": "567ccfc06fecff803aa8533476b84936",
                            },
                        ],
                    },
                    {
                        "anonymized_name": "116c25bb5cf9b84958846024fe1c2b7b",
                        "anonymized_domain_builder": {
                            "parent_class": "SimpleColumnSuffixDomainBuilder",
                            "anonymized_batch_request": {
                                "anonymized_batch_request_required_top_level_properties": {
                                    "anonymized_datasource_name": "aaea35c1421a0d3b7afe28cdfbd4b8d1",
                                    "anonymized_data_connector_name": "5bed7acb38185c12e3050a0c34f27ff4",
                                    "anonymized_data_asset_name": "d1c08815a25bc17c0493a50cd51ba31f",
                                }
                            },
                        },
                        "anonymized_parameter_builders": [
                            {
                                "parent_class": "MetricMultiBatchParameterBuilder",
                                "anonymized_name": "fa3ce9b81f1acc2f2730005e05737ea7",
                                "anonymized_batch_request": {
                                    "anonymized_batch_request_required_top_level_properties": {
                                        "anonymized_datasource_name": "aaea35c1421a0d3b7afe28cdfbd4b8d1",
                                        "anonymized_data_connector_name": "5bed7acb38185c12e3050a0c34f27ff4",
                                        "anonymized_data_asset_name": "d1c08815a25bc17c0493a50cd51ba31f",
                                    }
                                },
                            },
                            {
                                "parent_class": "MetricMultiBatchParameterBuilder",
                                "anonymized_name": "0bb947e516b26696a66787dc936570b7",
                                "anonymized_batch_request": {
                                    "anonymized_batch_request_required_top_level_properties": {
                                        "anonymized_datasource_name": "aaea35c1421a0d3b7afe28cdfbd4b8d1",
                                        "anonymized_data_connector_name": "5bed7acb38185c12e3050a0c34f27ff4",
                                        "anonymized_data_asset_name": "d1c08815a25bc17c0493a50cd51ba31f",
                                    }
                                },
                            },
                            {
                                "parent_class": "MetricMultiBatchParameterBuilder",
                                "anonymized_name": "66093b34c0c2e4ff275edf1752bcd27e",
                                "anonymized_batch_request": {
                                    "anonymized_batch_request_required_top_level_properties": {
                                        "anonymized_datasource_name": "aaea35c1421a0d3b7afe28cdfbd4b8d1",
                                        "anonymized_data_connector_name": "5bed7acb38185c12e3050a0c34f27ff4",
                                        "anonymized_data_asset_name": "d1c08815a25bc17c0493a50cd51ba31f",
                                    }
                                },
                            },
                            {
                                "parent_class": "SimpleDateFormatStringParameterBuilder",
                                "anonymized_name": "c5caa53c1ee64365b96ec96a285a6b3a",
                                "anonymized_batch_request": {
                                    "anonymized_batch_request_required_top_level_properties": {
                                        "anonymized_datasource_name": "aaea35c1421a0d3b7afe28cdfbd4b8d1",
                                        "anonymized_data_connector_name": "5bed7acb38185c12e3050a0c34f27ff4",
                                        "anonymized_data_asset_name": "d1c08815a25bc17c0493a50cd51ba31f",
                                    }
                                },
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
                            "anonymized_batch_request": {
                                "anonymized_batch_request_required_top_level_properties": {
                                    "anonymized_datasource_name": "aaea35c1421a0d3b7afe28cdfbd4b8d1",
                                    "anonymized_data_connector_name": "5bed7acb38185c12e3050a0c34f27ff4",
                                    "anonymized_data_asset_name": "d1c08815a25bc17c0493a50cd51ba31f",
                                }
                            },
                        },
                        "anonymized_parameter_builders": [
                            {
                                "parent_class": "ValueSetMultiBatchParameterBuilder",
                                "anonymized_name": "46d405b0294a06e1335c04bf1441dabf",
                                "anonymized_batch_request": {
                                    "anonymized_batch_request_required_top_level_properties": {
                                        "anonymized_datasource_name": "aaea35c1421a0d3b7afe28cdfbd4b8d1",
                                        "anonymized_data_connector_name": "5bed7acb38185c12e3050a0c34f27ff4",
                                        "anonymized_data_asset_name": "d1c08815a25bc17c0493a50cd51ba31f",
                                    }
                                },
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


# noinspection PyUnusedLocal
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
        "metric_dependencies": None,
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
@freeze_time("09/26/2019 13:42:41")
@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_bobby_profiler_user_workflow_multi_batch_row_count_range_rule_and_column_ranges_rule_oneshot_sampling_method(
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

    expectation_suite: ExpectationSuite = profiler.run(
        expectation_suite_name=bobby_columnar_table_multi_batch[
            "test_configuration_oneshot_sampling_method"
        ]["expectation_suite_name"],
        include_citation=True,
    )

    assert sorted(expectation_suite) == sorted(
        bobby_columnar_table_multi_batch["test_configuration_oneshot_sampling_method"][
            "expected_expectation_suite"
        ]
    )

    assert mock_emit.call_count == 103

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
                                "anonymized_batch_request": {
                                    "anonymized_batch_request_required_top_level_properties": {
                                        "anonymized_datasource_name": "12ed1b4af37ec138531bd721a8813a33",
                                        "anonymized_data_connector_name": "869034ebc8733404d9d6ac564012f441",
                                        "anonymized_data_asset_name": "57cd583969f4508907a12c880d78efd6",
                                    },
                                    "batch_request_optional_top_level_keys": [
                                        "data_connector_query"
                                    ],
                                    "data_connector_query_keys": ["index"],
                                },
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
                            "parent_class": "SimpleSemanticTypeColumnDomainBuilder",
                            "anonymized_batch_request": {
                                "anonymized_batch_request_required_top_level_properties": {
                                    "anonymized_datasource_name": "12ed1b4af37ec138531bd721a8813a33",
                                    "anonymized_data_connector_name": "869034ebc8733404d9d6ac564012f441",
                                    "anonymized_data_asset_name": "57cd583969f4508907a12c880d78efd6",
                                },
                                "batch_request_optional_top_level_keys": [
                                    "data_connector_query"
                                ],
                                "data_connector_query_keys": ["index"],
                            },
                        },
                        "anonymized_parameter_builders": [
                            {
                                "parent_class": "NumericMetricRangeMultiBatchParameterBuilder",
                                "anonymized_name": "ace31374d026e07ec630c7459915e628",
                                "anonymized_batch_request": {
                                    "anonymized_batch_request_required_top_level_properties": {
                                        "anonymized_datasource_name": "12ed1b4af37ec138531bd721a8813a33",
                                        "anonymized_data_connector_name": "869034ebc8733404d9d6ac564012f441",
                                        "anonymized_data_asset_name": "57cd583969f4508907a12c880d78efd6",
                                    },
                                    "batch_request_optional_top_level_keys": [
                                        "data_connector_query"
                                    ],
                                    "data_connector_query_keys": ["index"],
                                },
                            },
                            {
                                "parent_class": "NumericMetricRangeMultiBatchParameterBuilder",
                                "anonymized_name": "f2dcb0a322c3ab161d0fd33e6bac3d7f",
                                "anonymized_batch_request": {
                                    "anonymized_batch_request_required_top_level_properties": {
                                        "anonymized_datasource_name": "12ed1b4af37ec138531bd721a8813a33",
                                        "anonymized_data_connector_name": "869034ebc8733404d9d6ac564012f441",
                                        "anonymized_data_asset_name": "57cd583969f4508907a12c880d78efd6",
                                    },
                                    "batch_request_optional_top_level_keys": [
                                        "data_connector_query"
                                    ],
                                    "data_connector_query_keys": ["index"],
                                },
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
                            "parent_class": "SimpleColumnSuffixDomainBuilder",
                            "anonymized_batch_request": {
                                "anonymized_batch_request_required_top_level_properties": {
                                    "anonymized_datasource_name": "12ed1b4af37ec138531bd721a8813a33",
                                    "anonymized_data_connector_name": "869034ebc8733404d9d6ac564012f441",
                                    "anonymized_data_asset_name": "57cd583969f4508907a12c880d78efd6",
                                },
                                "batch_request_optional_top_level_keys": [
                                    "data_connector_query"
                                ],
                                "data_connector_query_keys": ["index"],
                            },
                        },
                        "anonymized_parameter_builders": [
                            {
                                "parent_class": "SimpleDateFormatStringParameterBuilder",
                                "anonymized_name": "49526fea6686e5f87be493967a5ba6b7",
                                "anonymized_batch_request": {
                                    "anonymized_batch_request_required_top_level_properties": {
                                        "anonymized_datasource_name": "12ed1b4af37ec138531bd721a8813a33",
                                        "anonymized_data_connector_name": "869034ebc8733404d9d6ac564012f441",
                                        "anonymized_data_asset_name": "57cd583969f4508907a12c880d78efd6",
                                    },
                                    "batch_request_optional_top_level_keys": [
                                        "data_connector_query"
                                    ],
                                    "data_connector_query_keys": ["index"],
                                },
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
                            "parent_class": "SimpleColumnSuffixDomainBuilder",
                            "anonymized_batch_request": {
                                "anonymized_batch_request_required_top_level_properties": {
                                    "anonymized_datasource_name": "12ed1b4af37ec138531bd721a8813a33",
                                    "anonymized_data_connector_name": "869034ebc8733404d9d6ac564012f441",
                                    "anonymized_data_asset_name": "57cd583969f4508907a12c880d78efd6",
                                },
                                "batch_request_optional_top_level_keys": [
                                    "data_connector_query"
                                ],
                                "data_connector_query_keys": ["index"],
                            },
                        },
                        "anonymized_parameter_builders": [
                            {
                                "parent_class": "RegexPatternStringParameterBuilder",
                                "anonymized_name": "2a791a71865e26a98b3f9f89ef83556b",
                                "anonymized_batch_request": {
                                    "anonymized_batch_request_required_top_level_properties": {
                                        "anonymized_datasource_name": "12ed1b4af37ec138531bd721a8813a33",
                                        "anonymized_data_connector_name": "869034ebc8733404d9d6ac564012f441",
                                        "anonymized_data_asset_name": "57cd583969f4508907a12c880d78efd6",
                                    },
                                    "batch_request_optional_top_level_keys": [
                                        "data_connector_query"
                                    ],
                                    "data_connector_query_keys": ["index"],
                                },
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
                            "anonymized_batch_request": {
                                "anonymized_batch_request_required_top_level_properties": {
                                    "anonymized_datasource_name": "12ed1b4af37ec138531bd721a8813a33",
                                    "anonymized_data_connector_name": "869034ebc8733404d9d6ac564012f441",
                                    "anonymized_data_asset_name": "57cd583969f4508907a12c880d78efd6",
                                },
                                "batch_request_optional_top_level_keys": [
                                    "data_connector_query"
                                ],
                                "data_connector_query_keys": ["index"],
                            },
                        },
                        "anonymized_parameter_builders": [
                            {
                                "parent_class": "ValueSetMultiBatchParameterBuilder",
                                "anonymized_name": "73b050124a17a420f2fb3e605f7111b2",
                                "anonymized_batch_request": {
                                    "anonymized_batch_request_required_top_level_properties": {
                                        "anonymized_datasource_name": "12ed1b4af37ec138531bd721a8813a33",
                                        "anonymized_data_connector_name": "869034ebc8733404d9d6ac564012f441",
                                        "anonymized_data_asset_name": "57cd583969f4508907a12c880d78efd6",
                                    },
                                    "batch_request_optional_top_level_keys": [
                                        "data_connector_query"
                                    ],
                                    "data_connector_query_keys": ["index"],
                                },
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
                "variable_count": 4,
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
@freeze_time("09/26/2019 13:42:41")
def test_bobby_expect_column_values_to_be_between_auto_yes_default_profiler_config_yes_custom_profiler_config_no(
    bobby_columnar_table_multi_batch_deterministic_data_context,
):
    context: DataContext = bobby_columnar_table_multi_batch_deterministic_data_context

    suite: ExpectationSuite

    expectation_suite_name: str = f"tmp.profiler_suite_{str(uuid.uuid4())[:8]}"
    try:
        # noinspection PyUnusedLocal
        suite = context.get_expectation_suite(
            expectation_suite_name=expectation_suite_name
        )
    except ge_exceptions.DataContextError:
        suite = context.create_expectation_suite(
            expectation_suite_name=expectation_suite_name
        )
        print(f'Created ExpectationSuite "{suite.expectation_suite_name}".')

    # Use all batches, loaded by Validator, for estimating Expectation argument values.
    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }

    validator: Validator = context.get_validator(
        batch_request=BatchRequest(**batch_request),
        expectation_suite_name=expectation_suite_name,
    )
    assert len(validator.batches) == 3

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
        "min_value": -22.5,
        "max_value": 250.0,
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
        "max_value": 250.0,
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
        "max_value": 250.0,
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


@pytest.mark.skipif(
    version.parse(np.version.version) < version.parse("1.21.0"),
    reason="requires numpy version 1.21.0 or newer",
)
@freeze_time("09/26/2019 13:42:41")
def test_bobby_expect_column_values_to_be_between_auto_yes_default_profiler_config_yes_custom_profiler_config_yes(
    bobby_columnar_table_multi_batch_deterministic_data_context,
):
    context: DataContext = bobby_columnar_table_multi_batch_deterministic_data_context

    suite: ExpectationSuite

    expectation_suite_name: str = f"tmp.profiler_suite_{str(uuid.uuid4())[:8]}"
    try:
        # noinspection PyUnusedLocal
        suite = context.get_expectation_suite(
            expectation_suite_name=expectation_suite_name
        )
    except ge_exceptions.DataContextError:
        suite = context.create_expectation_suite(
            expectation_suite_name=expectation_suite_name
        )
        print(f'Created ExpectationSuite "{suite.expectation_suite_name}".')

    batch_request: dict

    validator: Validator

    result: ExpectationValidationResult

    custom_profiler_config: RuleBasedProfilerConfig

    # Use all batches, loaded by Validator, for estimating Expectation argument values.
    batch_request = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }

    validator = context.get_validator(
        batch_request=BatchRequest(**batch_request),
        expectation_suite_name=expectation_suite_name,
    )
    assert len(validator.batches) == 3

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
        "data_connector_query": {"index": 1},
    }

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
                        "batch_request": batch_request,
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

    validator = context.get_validator(
        batch_request=BatchRequest(**batch_request),
        expectation_suite_name=expectation_suite_name,
    )
    assert len(validator.batches) == 1

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
@freeze_time("09/26/2019 13:42:41")
def test_bobby_expect_column_values_to_be_between_auto_yes_default_profiler_config_no_custom_profiler_config_yes(
    bobby_columnar_table_multi_batch_deterministic_data_context,
):
    # If Expectation already has default Rule-Based Profiler configured, delete it for this test.
    expectation_impl = get_expectation_impl(
        expectation_name="expect_column_values_to_be_between"
    )
    default_profiler_config: Optional[
        RuleBasedProfilerConfig
    ] = expectation_impl.default_kwarg_values.get("profiler_config")
    if default_profiler_config:
        del expectation_impl.default_kwarg_values["profiler_config"]

    assert "profiler_config" not in expectation_impl.default_kwarg_values

    context: DataContext = bobby_columnar_table_multi_batch_deterministic_data_context

    suite: ExpectationSuite

    expectation_suite_name: str = f"tmp.profiler_suite_{str(uuid.uuid4())[:8]}"
    try:
        # noinspection PyUnusedLocal
        suite = context.get_expectation_suite(
            expectation_suite_name=expectation_suite_name
        )
    except ge_exceptions.DataContextError:
        suite = context.create_expectation_suite(
            expectation_suite_name=expectation_suite_name
        )
        print(f'Created ExpectationSuite "{suite.expectation_suite_name}".')

    batch_request: dict

    validator: Validator

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

    # Use all batches, loaded by Validator, for estimating Expectation argument values.
    batch_request = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }

    validator = context.get_validator(
        batch_request=BatchRequest(**batch_request),
        expectation_suite_name=expectation_suite_name,
    )
    assert len(validator.batches) == 3

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

    validator = context.get_validator(
        batch_request=BatchRequest(**batch_request),
        expectation_suite_name=expectation_suite_name,
    )
    assert len(validator.batches) == 1

    # Use one batch (at index "1"), loaded by Validator as active_batch, for DomainBuilder purposes.
    domain_batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
        "data_connector_query": {
            "index": 1,
        },
    }

    # Use all batches, except ("latest") active_batch, loaded by Validator, for estimating Expectation argument values.
    parameter_builder_batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
        "data_connector_query": {
            "index": "1",
        },
    }

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
                    "batch_request": domain_batch_request,
                },
                "parameter_builders": [
                    {
                        "name": "my_min_estimator",
                        "class_name": "MetricMultiBatchParameterBuilder",
                        "module_name": "great_expectations.rule_based_profiler.parameter_builder",
                        "batch_request": parameter_builder_batch_request,
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


@pytest.mark.skipif(
    version.parse(np.version.version) < version.parse("1.21.0"),
    reason="requires numpy version 1.21.0 or newer",
)
@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_bobster_profiler_user_workflow_multi_batch_row_count_range_rule_bootstrap_sampling_method(
    mock_emit,
    caplog,
    bobster_columnar_table_multi_batch_normal_mean_5000_stdev_1000_data_context,
    bobster_columnar_table_multi_batch_normal_mean_5000_stdev_1000,
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

    expectation_suite: ExpectationSuite = profiler.run(
        expectation_suite_name=bobster_columnar_table_multi_batch_normal_mean_5000_stdev_1000[
            "test_configuration_bootstrap_sampling_method"
        ][
            "expectation_suite_name"
        ],
    )
    expect_table_row_count_to_be_between_expectation_configuration_kwargs: dict = (
        expectation_suite.to_json_dict()["expectations"][0]["kwargs"]
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
            "test_configuration_bootstrap_sampling_method"
        ]["expect_table_row_count_to_be_between_min_value_mean_value"]
        < min_value
        < bobster_columnar_table_multi_batch_normal_mean_5000_stdev_1000[
            "test_configuration_bootstrap_sampling_method"
        ]["expect_table_row_count_to_be_between_mean_value"]
    )
    assert (
        bobster_columnar_table_multi_batch_normal_mean_5000_stdev_1000[
            "test_configuration_bootstrap_sampling_method"
        ]["expect_table_row_count_to_be_between_mean_value"]
        < max_value
        < bobster_columnar_table_multi_batch_normal_mean_5000_stdev_1000[
            "test_configuration_bootstrap_sampling_method"
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
                                "anonymized_batch_request": {
                                    "anonymized_batch_request_required_top_level_properties": {
                                        "anonymized_datasource_name": "12ed1b4af37ec138531bd721a8813a33",
                                        "anonymized_data_connector_name": "869034ebc8733404d9d6ac564012f441",
                                        "anonymized_data_asset_name": "57cd583969f4508907a12c880d78efd6",
                                    },
                                    "batch_request_optional_top_level_keys": [
                                        "data_connector_query"
                                    ],
                                    "data_connector_query_keys": ["index"],
                                },
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
@freeze_time("09/26/2019 13:42:41")
def test_bobster_expect_table_row_count_to_be_between_auto_yes_default_profiler_config_yes_custom_profiler_config_no(
    bobster_columnar_table_multi_batch_normal_mean_5000_stdev_1000_data_context,
):
    context: DataContext = (
        bobster_columnar_table_multi_batch_normal_mean_5000_stdev_1000_data_context
    )

    result: ExpectationValidationResult

    suite: ExpectationSuite

    expectation_suite_name: str = f"tmp.profiler_suite_{str(uuid.uuid4())[:8]}"
    try:
        # noinspection PyUnusedLocal
        suite = context.get_expectation_suite(
            expectation_suite_name=expectation_suite_name
        )
    except ge_exceptions.DataContextError:
        suite = context.create_expectation_suite(
            expectation_suite_name=expectation_suite_name
        )
        print(f'Created ExpectationSuite "{suite.expectation_suite_name}".')

    # Use all batches, loaded by Validator, for estimating Expectation argument values.
    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }

    validator: Validator = context.get_validator(
        batch_request=BatchRequest(**batch_request),
        expectation_suite_name=expectation_suite_name,
    )
    assert len(validator.batches) == 36

    result = validator.expect_table_row_count_to_be_between(
        result_format="SUMMARY",
        include_config=True,
        auto=True,
    )
    assert result.expectation_config.kwargs["auto"]


@pytest.mark.skipif(
    version.parse(np.version.version) < version.parse("1.21.0"),
    reason="requires numpy version 1.21.0 or newer",
)
@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_quentin_profiler_user_workflow_multi_batch_quantiles_value_ranges_rule(
    mock_emit,
    caplog,
    quentin_columnar_table_multi_batch_data_context,
    quentin_columnar_table_multi_batch,
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

    expectation_suite: ExpectationSuite = profiler.run(
        expectation_suite_name=quentin_columnar_table_multi_batch["test_configuration"][
            "expectation_suite_name"
        ],
    )

    expectation_configuration_dict: dict
    column_name: str
    expectation_kwargs: dict
    expect_column_quantile_values_to_be_between_expectation_configurations_kwargs_dict: Dict[
        str, dict
    ] = {
        expectation_configuration_dict["kwargs"][
            "column"
        ]: expectation_configuration_dict["kwargs"]
        for expectation_configuration_dict in expectation_suite.to_json_dict()[
            "expectations"
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
                            "parent_class": "SimpleColumnSuffixDomainBuilder",
                            "anonymized_batch_request": {
                                "anonymized_batch_request_required_top_level_properties": {
                                    "anonymized_datasource_name": "12ed1b4af37ec138531bd721a8813a33",
                                    "anonymized_data_connector_name": "869034ebc8733404d9d6ac564012f441",
                                    "anonymized_data_asset_name": "57cd583969f4508907a12c880d78efd6",
                                },
                                "batch_request_optional_top_level_keys": [
                                    "data_connector_query"
                                ],
                                "data_connector_query_keys": ["index"],
                            },
                        },
                        "anonymized_parameter_builders": [
                            {
                                "parent_class": "NumericMetricRangeMultiBatchParameterBuilder",
                                "anonymized_name": "71243c854c0c04e9e014d02a793abe55",
                                "anonymized_batch_request": {
                                    "anonymized_batch_request_required_top_level_properties": {
                                        "anonymized_datasource_name": "12ed1b4af37ec138531bd721a8813a33",
                                        "anonymized_data_connector_name": "869034ebc8733404d9d6ac564012f441",
                                        "anonymized_data_asset_name": "57cd583969f4508907a12c880d78efd6",
                                    },
                                    "batch_request_optional_top_level_keys": [
                                        "data_connector_query"
                                    ],
                                    "data_connector_query_keys": ["index"],
                                },
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
@freeze_time("09/26/2019 13:42:41")
def test_quentin_expect_column_quantile_values_to_be_between_auto_yes_default_profiler_config_yes_custom_profiler_config_yes(
    quentin_columnar_table_multi_batch_data_context,
):
    context: DataContext = quentin_columnar_table_multi_batch_data_context

    result: ExpectationValidationResult

    custom_profiler_config: RuleBasedProfilerConfig

    suite: ExpectationSuite

    expectation_suite_name: str = f"tmp.profiler_suite_{str(uuid.uuid4())[:8]}"
    try:
        # noinspection PyUnusedLocal
        suite = context.get_expectation_suite(
            expectation_suite_name=expectation_suite_name
        )
    except ge_exceptions.DataContextError:
        suite = context.create_expectation_suite(
            expectation_suite_name=expectation_suite_name
        )
        print(f'Created ExpectationSuite "{suite.expectation_suite_name}".')

    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }

    validator: Validator = context.get_validator(
        batch_request=BatchRequest(**batch_request),
        expectation_suite_name=expectation_suite_name,
    )
    assert len(validator.batches) == 36

    parameter_builder_batch_request: dict

    # Use one batch (at index "1"), loaded by Validator as active_batch, for DomainBuilder purposes.
    domain_batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
        "data_connector_query": {
            "index": -1,
        },
    }

    # Use all batches, except ("latest") active_batch, loaded by Validator, for estimating Expectation argument values.
    parameter_builder_batch_request = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
        "data_connector_query": {
            "index": ":-1",
        },
    }

    custom_profiler_config = RuleBasedProfilerConfig(
        name="expect_column_quantile_values_to_be_between",  # Convention: use "expectation_type" as profiler name.
        config_version=1.0,
        rules={
            "column_quantiles_rule": {
                "domain_builder": {
                    "class_name": "ColumnDomainBuilder",
                    "module_name": "great_expectations.rule_based_profiler.domain_builder",
                    "batch_request": domain_batch_request,
                },
                "parameter_builders": [
                    {
                        "name": "quantile_value_ranges",
                        "class_name": "NumericMetricRangeMultiBatchParameterBuilder",
                        "module_name": "great_expectations.rule_based_profiler.parameter_builder",
                        "batch_request": parameter_builder_batch_request,
                        "metric_name": "column.quantile_values",
                        "metric_domain_kwargs": "$domain.domain_kwargs",
                        "metric_value_kwargs": {
                            "quantiles": "$variables.quantiles",
                            "allow_relative_error": "$variables.allow_relative_error",
                        },
                        "num_bootstrap_samples": "$variables.num_bootstrap_samples",
                        "bootstrap_random_seed": "$variables.bootstrap_random_seed",
                        "false_positive_rate": "$variables.false_positive_rate",
                        "round_decimals": 2,
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
                            "value_ranges": "$parameter.quantile_value_ranges.value.value_range",
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
    assert not result.success

    value_ranges_expected = [
        [
            5.842754275,
            6.5,
        ],
        [
            8.675167517,
            9.570000000,
        ],
        [
            13.344354435,
            15.650000000,
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

    # Use all batches, loaded by Validator, for estimating Expectation argument values.
    parameter_builder_batch_request = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }

    custom_profiler_config = RuleBasedProfilerConfig(
        name="expect_column_quantile_values_to_be_between",  # Convention: use "expectation_type" as profiler name.
        config_version=1.0,
        rules={
            "column_quantiles_rule": {
                "domain_builder": {
                    "class_name": "ColumnDomainBuilder",
                    "module_name": "great_expectations.rule_based_profiler.domain_builder",
                    "batch_request": domain_batch_request,
                },
                "parameter_builders": [
                    {
                        "name": "quantile_value_ranges",
                        "class_name": "NumericMetricRangeMultiBatchParameterBuilder",
                        "module_name": "great_expectations.rule_based_profiler.parameter_builder",
                        "batch_request": parameter_builder_batch_request,
                        "metric_name": "column.quantile_values",
                        "metric_domain_kwargs": "$domain.domain_kwargs",
                        "metric_value_kwargs": {
                            "quantiles": "$variables.quantiles",
                            "allow_relative_error": "$variables.allow_relative_error",
                        },
                        "num_bootstrap_samples": "$variables.num_bootstrap_samples",
                        "bootstrap_random_seed": "$variables.bootstrap_random_seed",
                        "false_positive_rate": "$variables.false_positive_rate",
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
                            "value_ranges": "$parameter.quantile_value_ranges.value.value_range",
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
@freeze_time("09/26/2019 13:42:41")
def test_quentin_expect_column_values_to_be_in_set_auto_yes_default_profiler_config_yes_custom_profiler_config_no(
    quentin_columnar_table_multi_batch_data_context,
):
    context: DataContext = quentin_columnar_table_multi_batch_data_context

    result: ExpectationValidationResult

    custom_profiler_config: RuleBasedProfilerConfig

    suite: ExpectationSuite

    expectation_suite_name: str = f"tmp.profiler_suite_{str(uuid.uuid4())[:8]}"
    try:
        # noinspection PyUnusedLocal
        suite = context.get_expectation_suite(
            expectation_suite_name=expectation_suite_name
        )
    except ge_exceptions.DataContextError:
        suite = context.create_expectation_suite(
            expectation_suite_name=expectation_suite_name
        )
        print(f'Created ExpectationSuite "{suite.expectation_suite_name}".')

    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }

    validator: Validator = context.get_validator(
        batch_request=BatchRequest(**batch_request),
        expectation_suite_name=expectation_suite_name,
    )
    assert len(validator.batches) == 36

    # Use all batches, loaded by Validator, for estimating Expectation argument values.
    result = validator.expect_column_values_to_be_in_set(
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
@freeze_time("09/26/2019 13:42:41")
def test_quentin_expect_column_min_to_be_between_auto_yes_default_profiler_config_yes_custom_profiler_config_no(
    quentin_columnar_table_multi_batch_data_context,
):
    context: DataContext = quentin_columnar_table_multi_batch_data_context

    result: ExpectationValidationResult

    custom_profiler_config: RuleBasedProfilerConfig

    suite: ExpectationSuite

    expectation_suite_name: str = f"tmp.profiler_suite_{str(uuid.uuid4())[:8]}"
    try:
        # noinspection PyUnusedLocal
        suite = context.get_expectation_suite(
            expectation_suite_name=expectation_suite_name
        )
    except ge_exceptions.DataContextError:
        suite = context.create_expectation_suite(
            expectation_suite_name=expectation_suite_name
        )
        print(f'Created ExpectationSuite "{suite.expectation_suite_name}".')

    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }

    validator: Validator = context.get_validator(
        batch_request=BatchRequest(**batch_request),
        expectation_suite_name=expectation_suite_name,
    )
    assert len(validator.batches) == 36

    # Use all batches, loaded by Validator, for estimating Expectation argument values.
    result = validator.expect_column_min_to_be_between(
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
    min_value_expected: float = -1.8796e2

    np.testing.assert_allclose(
        actual=min_value_actual,
        desired=min_value_expected,
        rtol=rtol,
        atol=atol,
        err_msg=f"Actual value of {min_value_actual} differs from expected value of {min_value_expected} by more than {atol + rtol * abs(min_value_expected)} tolerance.",
    )

    max_value_actual: float = result.expectation_config["kwargs"]["max_value"]
    max_value_expected: float = -5.79
    np.testing.assert_allclose(
        actual=max_value_actual,
        desired=max_value_expected,
        rtol=rtol,
        atol=atol,
        err_msg=f"Actual value of {max_value_actual} differs from expected value of {max_value_expected} by more than {atol + rtol * abs(max_value_expected)} tolerance.",
    )


@pytest.mark.skipif(
    version.parse(np.version.version) < version.parse("1.21.0"),
    reason="requires numpy version 1.21.0 or newer",
)
@freeze_time("09/26/2019 13:42:41")
def test_quentin_expect_column_max_to_be_between_auto_yes_default_profiler_config_yes_custom_profiler_config_no(
    quentin_columnar_table_multi_batch_data_context,
):
    context: DataContext = quentin_columnar_table_multi_batch_data_context

    result: ExpectationValidationResult

    custom_profiler_config: RuleBasedProfilerConfig

    suite: ExpectationSuite

    expectation_suite_name: str = f"tmp.profiler_suite_{str(uuid.uuid4())[:8]}"
    try:
        # noinspection PyUnusedLocal
        suite = context.get_expectation_suite(
            expectation_suite_name=expectation_suite_name
        )
    except ge_exceptions.DataContextError:
        suite = context.create_expectation_suite(
            expectation_suite_name=expectation_suite_name
        )
        print(f'Created ExpectationSuite "{suite.expectation_suite_name}".')

    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }

    validator: Validator = context.get_validator(
        batch_request=BatchRequest(**batch_request),
        expectation_suite_name=expectation_suite_name,
    )
    assert len(validator.batches) == 36

    # Use all batches, loaded by Validator, for estimating Expectation argument values.
    result = validator.expect_column_max_to_be_between(
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
    min_value_expected: float = 1.5438e2

    np.testing.assert_allclose(
        actual=min_value_actual,
        desired=min_value_expected,
        rtol=rtol,
        atol=atol,
        err_msg=f"Actual value of {min_value_actual} differs from expected value of {min_value_expected} by more than {atol + rtol * abs(min_value_expected)} tolerance.",
    )

    max_value_actual: float = result.expectation_config["kwargs"]["max_value"]
    max_value_expected: float = 5.6314775e4
    np.testing.assert_allclose(
        actual=max_value_actual,
        desired=max_value_expected,
        rtol=rtol,
        atol=atol,
        err_msg=f"Actual value of {max_value_actual} differs from expected value of {max_value_expected} by more than {atol + rtol * abs(max_value_expected)} tolerance.",
    )
