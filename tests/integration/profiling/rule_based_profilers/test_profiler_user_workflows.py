import datetime
from typing import Any, Dict, List, cast

import numpy as np
import pandas as pd
import pytest
from freezegun import freeze_time
from packaging import version
from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap

from great_expectations import DataContext
from great_expectations.core import ExpectationSuite
from great_expectations.core.batch import BatchRequest
from great_expectations.datasource import DataConnector, Datasource
from great_expectations.rule_based_profiler.config.base import (
    ruleBasedProfilerConfigSchema,
)
from great_expectations.rule_based_profiler.rule_based_profiler import RuleBasedProfiler
from great_expectations.validator.metric_configuration import MetricConfiguration
from great_expectations.validator.validator import Validator

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
def test_alice_profiler_user_workflow_single_batch(
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
def test_bobby_profiler_user_workflow_multi_batch_row_count_range_rule_and_column_ranges_rule_oneshot_sampling_method(
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
    assert (
        expectation_suite
        == bobby_columnar_table_multi_batch[
            "test_configuration_oneshot_sampling_method"
        ]["expected_expectation_suite"]
    )


@pytest.mark.skipif(
    version.parse(np.version.version) < version.parse("1.21.0"),
    reason="requires numpy version 1.21.0 or newer",
)
def test_bobster_profiler_user_workflow_multi_batch_row_count_range_rule_bootstrap_sampling_method(
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
