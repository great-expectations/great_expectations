import os
import unittest
from typing import Dict, List, Optional, cast
from unittest import mock

import pytest
from freezegun import freeze_time

import great_expectations as gx

# noinspection PyUnresolvedReferences
from contrib.experimental.great_expectations_experimental.rule_based_profiler.data_assistant_result import (
    GrowthNumericDataAssistantResult,
)
from contrib.experimental.great_expectations_experimental.tests.test_utils import (
    CONNECTION_STRING,
    load_data_into_postgres_database,
)
from great_expectations import DataContext
from great_expectations.core import ExpectationSuite
from great_expectations.core.batch import BatchRequest
from great_expectations.core.domain import Domain
from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.core.usage_statistics.events import UsageStatsEvents
from great_expectations.data_context.util import file_relative_path
from great_expectations.rule_based_profiler.data_assistant_result import (
    DataAssistantResult,
)
from great_expectations.rule_based_profiler.parameter_container import (
    FULLY_QUALIFIED_PARAMETER_NAME_ATTRIBUTED_VALUE_KEY,
    ParameterNode,
)

# noinspection PyUnresolvedReferences


@pytest.fixture
def bobby_growth_numeric_data_assistant_result_usage_stats_enabled(
    no_usage_stats,
    bobby_columnar_table_multi_batch_deterministic_data_context: DataContext,
) -> GrowthNumericDataAssistantResult:
    context: DataContext = bobby_columnar_table_multi_batch_deterministic_data_context

    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }

    data_assistant_result: DataAssistantResult = context.assistants.growth_numeric.run(
        batch_request=batch_request,
        estimation="flag_outliers",
    )

    return cast(GrowthNumericDataAssistantResult, data_assistant_result)


@pytest.fixture(scope="module")
def bobby_growth_numeric_data_assistant_result(
    bobby_columnar_table_multi_batch_probabilistic_data_context: DataContext,
) -> GrowthNumericDataAssistantResult:
    context: DataContext = bobby_columnar_table_multi_batch_probabilistic_data_context

    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }

    data_assistant_result: DataAssistantResult = context.assistants.growth_numeric.run(
        batch_request=batch_request,
        estimation="flag_outliers",
    )

    return cast(GrowthNumericDataAssistantResult, data_assistant_result)


@pytest.fixture(scope="module")
def quentin_implicit_invocation_result_actual_time(
    quentin_columnar_table_multi_batch_data_context: DataContext,
) -> GrowthNumericDataAssistantResult:
    context: DataContext = quentin_columnar_table_multi_batch_data_context

    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }

    data_assistant_result: DataAssistantResult = context.assistants.growth_numeric.run(
        batch_request=batch_request,
        estimation="flag_outliers",
    )

    return cast(GrowthNumericDataAssistantResult, data_assistant_result)


@pytest.fixture(scope="module")
@freeze_time("09/26/2019 13:42:41")
def quentin_implicit_invocation_result_frozen_time(
    quentin_columnar_table_multi_batch_data_context: DataContext,
):
    context: DataContext = quentin_columnar_table_multi_batch_data_context

    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }

    data_assistant_result: DataAssistantResult = context.assistants.growth_numeric.run(
        batch_request=batch_request,
        estimation="flag_outliers",
    )

    return cast(GrowthNumericDataAssistantResult, data_assistant_result)


@pytest.mark.integration
@pytest.mark.slow  # 6.90s
def test_growth_numeric_data_assistant_result_serialization(
    bobby_growth_numeric_data_assistant_result: GrowthNumericDataAssistantResult,
) -> None:
    growth_numeric_data_assistant_result_as_dict: dict = (
        bobby_growth_numeric_data_assistant_result.to_dict()
    )
    assert (
        set(growth_numeric_data_assistant_result_as_dict.keys())
        == DataAssistantResult.ALLOWED_KEYS
    )
    assert (
        bobby_growth_numeric_data_assistant_result.to_json_dict()
        == growth_numeric_data_assistant_result_as_dict
    )
    assert len(bobby_growth_numeric_data_assistant_result.profiler_config.rules) == 4


@pytest.mark.integration
@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
@pytest.mark.slow  # 7.34s
def test_growth_numeric_data_assistant_result_get_expectation_suite(
    mock_emit,
    bobby_growth_numeric_data_assistant_result_usage_stats_enabled: GrowthNumericDataAssistantResult,
):
    expectation_suite_name: str = "my_suite"

    suite: ExpectationSuite = bobby_growth_numeric_data_assistant_result_usage_stats_enabled.get_expectation_suite(
        expectation_suite_name=expectation_suite_name
    )

    assert suite is not None and len(suite.expectations) > 0

    assert mock_emit.call_count == 1

    # noinspection PyUnresolvedReferences
    actual_events: List[unittest.mock._Call] = mock_emit.call_args_list
    assert (
        actual_events[-1][0][0]["event"]
        == UsageStatsEvents.DATA_ASSISTANT_RESULT_GET_EXPECTATION_SUITE
    )


@pytest.mark.integration
def test_growth_numeric_data_assistant_metrics_count(
    bobby_growth_numeric_data_assistant_result: GrowthNumericDataAssistantResult,
) -> None:
    domain: Domain
    parameter_values_for_fully_qualified_parameter_names: Dict[str, ParameterNode]
    num_metrics: int

    domain_key = Domain(
        domain_type=MetricDomainTypes.TABLE,
    )

    num_metrics = 0
    for (
        domain,
        parameter_values_for_fully_qualified_parameter_names,
    ) in bobby_growth_numeric_data_assistant_result.metrics_by_domain.items():
        if domain.is_superset(other=domain_key):
            num_metrics += len(parameter_values_for_fully_qualified_parameter_names)

    assert num_metrics == 2

    num_metrics = 0
    for (
        domain,
        parameter_values_for_fully_qualified_parameter_names,
    ) in bobby_growth_numeric_data_assistant_result.metrics_by_domain.items():
        num_metrics += len(parameter_values_for_fully_qualified_parameter_names)

    assert num_metrics == 121


@pytest.mark.integration
def test_growth_numeric_data_assistant_result_batch_id_to_batch_identifier_display_name_map_coverage(
    bobby_growth_numeric_data_assistant_result: GrowthNumericDataAssistantResult,
):
    metrics_by_domain: Optional[
        Dict[Domain, Dict[str, ParameterNode]]
    ] = bobby_growth_numeric_data_assistant_result.metrics_by_domain

    parameter_values_for_fully_qualified_parameter_names: Dict[str, ParameterNode]
    parameter_node: ParameterNode
    batch_id: str
    assert all(
        bobby_growth_numeric_data_assistant_result._batch_id_to_batch_identifier_display_name_map[
            batch_id
        ]
        is not None
        for parameter_values_for_fully_qualified_parameter_names in metrics_by_domain.values()
        for parameter_node in parameter_values_for_fully_qualified_parameter_names.values()
        for batch_id in (
            parameter_node[FULLY_QUALIFIED_PARAMETER_NAME_ATTRIBUTED_VALUE_KEY]
            if FULLY_QUALIFIED_PARAMETER_NAME_ATTRIBUTED_VALUE_KEY in parameter_node
            else {}
        ).keys()
    )


@pytest.mark.integration
@pytest.mark.slow  # 39.26s
def test_growth_numeric_data_assistant_get_metrics_and_expectations_using_implicit_invocation_with_variables_directives(
    bobby_columnar_table_multi_batch_deterministic_data_context,
):
    context: DataContext = bobby_columnar_table_multi_batch_deterministic_data_context

    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }

    data_assistant_result: DataAssistantResult = context.assistants.growth_numeric.run(
        batch_request=batch_request,
        estimation="flag_outliers",
        numeric_columns_rule={
            "round_decimals": 15,
            "false_positive_rate": 0.1,
            "random_seed": 43792,
        },
        categorical_columns_rule={
            "false_positive_rate": 0.1,
            # "round_decimals": 4,
        },
    )
    assert (
        data_assistant_result.profiler_config.rules["numeric_columns_rule"][
            "variables"
        ]["round_decimals"]
        == 15
    )
    assert (
        data_assistant_result.profiler_config.rules["numeric_columns_rule"][
            "variables"
        ]["false_positive_rate"]
        == 1.0e-1
    )
    assert (
        data_assistant_result.profiler_config.rules["categorical_columns_rule"][
            "variables"
        ]["false_positive_rate"]
        == 1.0e-1
    )


@pytest.mark.integration
@pytest.mark.slow  # 38.26s
def test_growth_numeric_data_assistant_get_metrics_and_expectations_using_implicit_invocation_with_estimation_directive(
    quentin_columnar_table_multi_batch_data_context,
):
    context: DataContext = quentin_columnar_table_multi_batch_data_context

    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }

    data_assistant_result: DataAssistantResult = context.assistants.growth_numeric.run(
        batch_request=batch_request,
    )

    rule_config: dict
    assert all(
        rule_config["variables"]["estimator"] == "exact"
        if "estimator" in rule_config["variables"]
        else True
        for rule_config in data_assistant_result.profiler_config.rules.values()
    )


@pytest.mark.integration
@pytest.mark.slow  # 19s
def test_pandas_happy_path_growth_numeric_data_assistant(empty_data_context) -> None:
    """
    The intent of this test is to ensure that our "happy path", exercised by notebooks is in working order.

    1. Setting up Datasource to load 2019 taxi data and 2020 taxi data
    2. Configuring BatchRequest to load 2019 data as multiple batches
    3. Running GrowthNumericDataAssistantResult and saving resulting ExpectationSuite as 'taxi_data_2019_suite'
    4. Configuring BatchRequest to load 2020 January data
    """
    data_context: gx.DataContext = empty_data_context
    taxi_data_path: str = file_relative_path(
        __file__,
        os.path.join(  # noqa: PTH118
            "..",
            "..",
            "..",
            "..",
            "..",
            "..",
            "tests",
            "test_sets",
            "taxi_yellow_tripdata_samples",
        ),
    )

    datasource_config: dict = {
        "name": "taxi_data",
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "module_name": "great_expectations.execution_engine",
            "class_name": "PandasExecutionEngine",
        },
        "data_connectors": {
            "configured_data_connector_multi_batch_asset": {
                "class_name": "ConfiguredAssetFilesystemDataConnector",
                "base_directory": taxi_data_path,
                "assets": {
                    "yellow_tripdata_2019": {
                        "group_names": ["year", "month"],
                        "pattern": "yellow_tripdata_sample_(2019)-(\\d.*)\\.csv",
                    },
                    "yellow_tripdata_2020": {
                        "group_names": ["year", "month"],
                        "pattern": "yellow_tripdata_sample_(2020)-(\\d.*)\\.csv",
                    },
                },
            },
        },
    }
    data_context.add_datasource(**datasource_config)

    # Batch Request
    multi_batch_batch_request: BatchRequest = BatchRequest(
        datasource_name="taxi_data",
        data_connector_name="configured_data_connector_multi_batch_asset",
        data_asset_name="yellow_tripdata_2019",
    )
    batch_request: BatchRequest = multi_batch_batch_request
    batch_list = data_context.get_batch_list(batch_request=batch_request)
    assert len(batch_list) == 12

    # Running growth_numeric data assistant
    result = data_context.assistants.growth_numeric.run(
        batch_request=multi_batch_batch_request
    )

    # saving resulting ExpectationSuite
    suite: ExpectationSuite = ExpectationSuite(
        expectation_suite_name="taxi_data_2019_suite"
    )
    suite.add_expectation_configurations(
        expectation_configurations=result.expectation_configurations
    )
    data_context.save_expectation_suite(expectation_suite=suite)

    # batch_request for checkpoint
    single_batch_batch_request: BatchRequest = BatchRequest(
        datasource_name="taxi_data",
        data_connector_name="configured_data_connector_multi_batch_asset",
        data_asset_name="yellow_tripdata_2020",
        data_connector_query={
            "batch_filter_parameters": {"year": "2020", "month": "01"}
        },
    )

    # configuring and running checkpoint
    checkpoint_config: dict = {
        "name": "my_checkpoint",
        "config_version": 1,
        "class_name": "SimpleCheckpoint",
        "validations": [
            {
                "batch_request": single_batch_batch_request,
                "expectation_suite_name": "taxi_data_2019_suite",
            }
        ],
    }
    data_context.add_checkpoint(**checkpoint_config)
    results = data_context.run_checkpoint(checkpoint_name="my_checkpoint")
    assert results.success is False


@pytest.mark.integration
@pytest.mark.slow  # 149 seconds
def test_spark_happy_path_growth_numeric_data_assistant(
    empty_data_context, spark_df_taxi_data_schema
) -> None:
    """
    The intent of this test is to ensure that our "happy path", exercised by notebooks is in working order.

    1. Setting up Datasource to load 2019 taxi data and 2020 taxi data
    2. Configuring BatchRequest to load 2019 data as multiple batches
    3. Running GrowthNumericDataAssistantResult and saving resulting ExpectationSuite as 'taxi_data_2019_suite'
    4. Configuring BatchRequest to load 2020 January data
    5. Configuring and running Checkpoint using BatchRequest for 2020-01, and 'taxi_data_2019_suite'.
    """
    from great_expectations.compatibility import pyspark

    schema: pyspark.types.StructType = spark_df_taxi_data_schema
    data_context: gx.DataContext = empty_data_context
    taxi_data_path: str = file_relative_path(
        __file__,
        os.path.join(  # noqa: PTH118
            "..", "..", "test_sets", "taxi_yellow_tripdata_samples"
        ),
    )

    datasource_config: dict = {
        "name": "taxi_data",
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "module_name": "great_expectations.execution_engine",
            "class_name": "SparkDFExecutionEngine",
        },
        "data_connectors": {
            "configured_data_connector_multi_batch_asset": {
                "class_name": "ConfiguredAssetFilesystemDataConnector",
                "base_directory": taxi_data_path,
                "assets": {
                    "yellow_tripdata_2019": {
                        "group_names": ["year", "month"],
                        "pattern": "yellow_tripdata_sample_(2019)-(\\d.*)\\.csv",
                    },
                    "yellow_tripdata_2020": {
                        "group_names": ["year", "month"],
                        "pattern": "yellow_tripdata_sample_(2020)-(\\d.*)\\.csv",
                    },
                },
            },
        },
    }
    data_context.add_datasource(**datasource_config)
    multi_batch_batch_request: BatchRequest = BatchRequest(
        datasource_name="taxi_data",
        data_connector_name="configured_data_connector_multi_batch_asset",
        data_asset_name="yellow_tripdata_2019",
        batch_spec_passthrough={
            "reader_method": "csv",
            "reader_options": {"header": True, "schema": schema},
        },
        data_connector_query={
            "batch_filter_parameters": {"year": "2019", "month": "01"}
        },
    )
    batch_request: BatchRequest = multi_batch_batch_request
    batch_list = data_context.get_batch_list(batch_request=batch_request)
    assert len(batch_list) == 1

    result = data_context.assistants.growth_numeric.run(
        batch_request=multi_batch_batch_request
    )
    suite: ExpectationSuite = ExpectationSuite(
        expectation_suite_name="taxi_data_2019_suite"
    )
    suite.add_expectation_configurations(
        expectation_configurations=result.expectation_configurations
    )
    data_context.save_expectation_suite(expectation_suite=suite)
    # batch_request for checkpoint
    single_batch_batch_request: BatchRequest = BatchRequest(
        datasource_name="taxi_data",
        data_connector_name="configured_data_connector_multi_batch_asset",
        data_asset_name="yellow_tripdata_2020",
        data_connector_query={
            "batch_filter_parameters": {"year": "2020", "month": "01"}
        },
    )
    checkpoint_config: dict = {
        "name": "my_checkpoint",
        "config_version": 1,
        "class_name": "SimpleCheckpoint",
        "validations": [
            {
                "batch_request": single_batch_batch_request,
                "expectation_suite_name": "taxi_data_2019_suite",
            }
        ],
    }
    data_context.add_checkpoint(**checkpoint_config)
    results = data_context.run_checkpoint(checkpoint_name="my_checkpoint")
    assert results.success is False


@pytest.mark.integration
@pytest.mark.slow  # 104 seconds
def test_sql_happy_path_growth_numeric_data_assistant(
    empty_data_context, test_backends, sa
) -> None:
    """
    The intent of this test is to ensure that our "happy path", exercised by notebooks is in working order.

    1. Loading tables into postgres Docker container by calling helper method load_data_into_postgres_database()
    2. Setting up Datasource to load 2019 taxi data and 2020 taxi data
    3. Configuring BatchRequest to load 2019 data as multiple batches
    4. Running GrowthNumericDataAssistantResult and saving resulting ExpectationSuite as 'taxi_data_2019_suite'
    5. Configuring BatchRequest to load 2020 January data
    6. Configuring and running Checkpoint using BatchRequest for 2020-01, and 'taxi_data_2019_suite'.
    """
    if "postgresql" not in test_backends:
        pytest.skip("testing data assistant in sql requires postgres backend")
    else:
        load_data_into_postgres_database(sa)

    data_context: gx.DataContext = empty_data_context

    datasource_config = {
        "name": "taxi_multi_batch_sql_datasource",
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "module_name": "great_expectations.execution_engine",
            "class_name": "SqlAlchemyExecutionEngine",
            "connection_string": CONNECTION_STRING,
        },
        "data_connectors": {
            "configured_data_connector_multi_batch_asset": {
                "class_name": "ConfiguredAssetSqlDataConnector",
                "assets": {
                    "yellow_tripdata_sample_2019": {
                        "splitter_method": "split_on_year_and_month",
                        "splitter_kwargs": {
                            "column_name": "pickup_datetime",
                        },
                    },
                    "yellow_tripdata_sample_2020": {
                        "splitter_method": "split_on_year_and_month",
                        "splitter_kwargs": {
                            "column_name": "pickup_datetime",
                        },
                    },
                },
            },
        },
    }
    data_context.add_datasource(**datasource_config)

    multi_batch_batch_request: BatchRequest = BatchRequest(
        datasource_name="taxi_multi_batch_sql_datasource",
        data_connector_name="configured_data_connector_multi_batch_asset",
        data_asset_name="yellow_tripdata_sample_2019",
    )

    batch_request: BatchRequest = multi_batch_batch_request
    batch_list = data_context.get_batch_list(batch_request=batch_request)
    assert len(batch_list) == 13

    result = data_context.assistants.growth_numeric.run(
        batch_request=multi_batch_batch_request
    )
    suite: ExpectationSuite = ExpectationSuite(
        expectation_suite_name="taxi_data_2019_suite"
    )
    suite.add_expectation_configurations(
        expectation_configurations=result.expectation_configurations
    )
    data_context.save_expectation_suite(expectation_suite=suite)
    # batch_request for checkpoint
    single_batch_batch_request: BatchRequest = BatchRequest(
        datasource_name="taxi_multi_batch_sql_datasource",
        data_connector_name="configured_data_connector_multi_batch_asset",
        data_asset_name="yellow_tripdata_sample_2020",
        data_connector_query={
            "batch_filter_parameters": {"pickup_datetime": {"year": 2020, "month": 1}},
        },
    )
    checkpoint_config: dict = {
        "name": "my_checkpoint",
        "config_version": 1,
        "class_name": "SimpleCheckpoint",
        "validations": [
            {
                "batch_request": single_batch_batch_request,
                "expectation_suite_name": "taxi_data_2019_suite",
            }
        ],
    }
    data_context.add_checkpoint(**checkpoint_config)
    results = data_context.run_checkpoint(checkpoint_name="my_checkpoint")
    assert results.success is False
