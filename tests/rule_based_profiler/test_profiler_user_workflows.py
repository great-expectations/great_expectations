import datetime
from typing import Any, Dict, List, cast

import pandas as pd
from ruamel.yaml import YAML

from great_expectations import DataContext
from great_expectations.core import ExpectationSuite
from great_expectations.core.batch import BatchRequest
from great_expectations.datasource import DataConnector, Datasource
from great_expectations.rule_based_profiler.profiler import Profiler
from great_expectations.validator.validation_graph import MetricConfiguration
from great_expectations.validator.validator import Validator

yaml = YAML()


def test_alice_columnar_table_single_batch_batches_are_accessible(
    monkeypatch,
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


def test_alice_profiler_user_workflow_single_batch(
    alice_columnar_table_single_batch_context,
    alice_columnar_table_single_batch,
):
    # Load data context
    data_context: DataContext = alice_columnar_table_single_batch_context
    # Load profiler configs & loop (run tests for each one)
    profiler_config: str = alice_columnar_table_single_batch["profiler_config"]

    # Instantiate Profiler
    full_profiler_config_dict: dict = yaml.load(profiler_config)
    rules_configs: dict = full_profiler_config_dict.get("rules")
    variables_configs: dict = full_profiler_config_dict.get("variables")

    profiler: Profiler = Profiler(
        rules_configs=rules_configs,
        variables_configs=variables_configs,
        data_context=data_context,
    )

    expectation_suite: ExpectationSuite = profiler.profile(
        expectation_suite_name=alice_columnar_table_single_batch[
            "expected_expectation_suite_name"
        ],
    )

    assert (
        expectation_suite
        == alice_columnar_table_single_batch["expected_expectation_suite"]
    )


def test_bobby_columnar_table_multi_batch_batches_are_accessible(
    monkeypatch,
    bobby_columnar_table_multi_batch_context,
    bobby_columnar_table_multi_batch,
):
    """
    # TODO: <Alex>ALEX -- Provide DocString</Alex>
    What does this test and why?
    """

    context: DataContext = bobby_columnar_table_multi_batch_context

    datasource_name: str = "taxi_pandas"
    data_connector_name: str = "monthly"
    data_asset_name: str = "my_reports"

    datasource: Datasource = cast(Datasource, context.datasources[datasource_name])
    data_connector: DataConnector = datasource.data_connectors[data_connector_name]

    file_list: List[str] = [
        "yellow_trip_data_sample_2019-01.csv",
        "yellow_trip_data_sample_2019-02.csv",
        "yellow_trip_data_sample_2019-03.csv",
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


def test_bobby_profiler_user_workflow_multi_batch_row_count_range_rule_and_column_ranges_rule_oneshot_sampling_method(
    bobby_columnar_table_multi_batch_context,
    bobby_columnar_table_multi_batch,
):
    # Load data context
    data_context: DataContext = bobby_columnar_table_multi_batch_context
    # Load profiler configs & loop (run tests for each one)
    profiler_config: str = bobby_columnar_table_multi_batch["profiler_config"]

    # Instantiate Profiler
    full_profiler_config_dict: dict = yaml.load(profiler_config)
    rules_configs: dict = full_profiler_config_dict.get("rules")
    variables_configs: dict = full_profiler_config_dict.get("variables")

    profiler: Profiler = Profiler(
        rules_configs=rules_configs,
        variables_configs=variables_configs,
        data_context=data_context,
    )

    expectation_suite: ExpectationSuite = profiler.profile(
        expectation_suite_name=bobby_columnar_table_multi_batch[
            "test_configuration_oneshot_sampling_method"
        ]["expectation_suite_name"],
    )

    assert (
        expectation_suite
        == bobby_columnar_table_multi_batch[
            "test_configuration_oneshot_sampling_method"
        ]["expected_expectation_suite"]
    )


def test_bobby_profiler_user_workflow_multi_batch_row_count_range_rule_bootstrap_sampling_method(
    bobby_columnar_table_multi_batch_context,
    bobby_columnar_table_multi_batch,
):
    # Load data context
    data_context: DataContext = bobby_columnar_table_multi_batch_context
    # Load profiler configs & loop (run tests for each one)
    profiler_config: str = bobby_columnar_table_multi_batch["profiler_config"]

    # Instantiate Profiler
    full_profiler_config_dict: dict = yaml.load(profiler_config)
    rules_configs: dict = full_profiler_config_dict.get("rules")
    variables_configs: dict = full_profiler_config_dict.get("variables")

    # Extract only the "row_count_range_rule" from the configuration and set the "sampling_method" of its only
    # "parameter_builder" (an instance of NumericMetricRangeMultiBatchParameterBuilder) to be the "bootstrap" method.
    # The reason for this manipulation is that even though the default value of the "sampling_method" is "bootstrap",
    # it is set to "oneshot" in the test configuration of every "parameter_builder" (for those that are instances of
    # NumericMetricRangeMultiBatchParameterBuilder) in order for the large expectation suite fixture, created for the
    # parametric ("oneshot") mode of the NumericMetricRangeMultiBatchParameterBuilder class to be applicable (coverage).
    # The "Bobby" profiler use case, applied to the Taxi data, outputs 1 table expectation and 30 column expectations.
    # The present test of the "bootstrap" sampling method uses the expectation suite containing only one expectation,
    # because setting up the statistical assertions (and the required fixtures with the tolerances) is very laborious.
    # In the future, additional of the "bootstrap" sampling method should be created using domain builders that generate
    # a small number of domains (ideally, only one domain) so as to make the building of test fixtures more manageable.
    row_count_range_rule: dict = rules_configs["row_count_range_rule"]
    parameter_builders: dict = row_count_range_rule["parameter_builders"]
    row_count_range_parameter: dict = parameter_builders[0]
    row_count_range_parameter["sampling_method"] = "bootstrap"
    rules_configs = {
        "row_count_range_rule": row_count_range_rule,
    }

    profiler: Profiler = Profiler(
        rules_configs=rules_configs,
        variables_configs=variables_configs,
        data_context=data_context,
    )

    expectation_suite: ExpectationSuite = profiler.profile(
        expectation_suite_name=bobby_columnar_table_multi_batch[
            "test_configuration_bootstrap_sampling_method"
        ]["expectation_suite_name"],
    )
    expect_table_row_count_to_be_between_expectation_configuration_kwargs: dict = (
        expectation_suite.to_json_dict()["expectations"][0]["kwargs"]
    )
    min_value = expect_table_row_count_to_be_between_expectation_configuration_kwargs[
        "min_value"
    ]
    max_value = expect_table_row_count_to_be_between_expectation_configuration_kwargs[
        "max_value"
    ]

    assert (
        bobby_columnar_table_multi_batch[
            "test_configuration_bootstrap_sampling_method"
        ]["expect_table_row_count_to_be_between_min_value_min_value"]
        < min_value
        < bobby_columnar_table_multi_batch[
            "test_configuration_bootstrap_sampling_method"
        ]["expect_table_row_count_to_be_between_min_value_max_value"]
    )
    assert (
        bobby_columnar_table_multi_batch[
            "test_configuration_bootstrap_sampling_method"
        ]["expect_table_row_count_to_be_between_max_value_min_value"]
        < max_value
        < bobby_columnar_table_multi_batch[
            "test_configuration_bootstrap_sampling_method"
        ]["expect_table_row_count_to_be_between_max_value_max_value"]
    )
