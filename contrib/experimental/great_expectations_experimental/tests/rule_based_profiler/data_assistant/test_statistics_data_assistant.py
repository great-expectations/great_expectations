import math
import os
from typing import Any, Dict, Optional, cast

import numpy as np
import pytest

import great_expectations as gx

# noinspection PyUnresolvedReferences
from contrib.experimental.great_expectations_experimental.rule_based_profiler.data_assistant import (
    StatisticsDataAssistant,
)
from contrib.experimental.great_expectations_experimental.rule_based_profiler.data_assistant_result import (
    StatisticsDataAssistantResult,
)
from contrib.experimental.great_expectations_experimental.tests.test_utils import (
    CONNECTION_STRING,
    load_data_into_postgres_database,
)
from great_expectations import DataContext
from great_expectations.core.batch import BatchRequest
from great_expectations.core.domain import Domain
from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context.util import file_relative_path
from great_expectations.rule_based_profiler.data_assistant_result import (
    DataAssistantResult,
)
from great_expectations.rule_based_profiler.helpers.util import (
    convert_metric_values_to_float_dtype_best_effort,
)
from great_expectations.rule_based_profiler.metric_computation_result import (
    MetricValues,
)
from great_expectations.rule_based_profiler.parameter_container import (
    FULLY_QUALIFIED_PARAMETER_NAME_ATTRIBUTED_VALUE_KEY,
    ParameterNode,
)

# noinspection PyUnresolvedReferences
from tests.conftest import (
    bobby_columnar_table_multi_batch_deterministic_data_context,
    empty_data_context,
    no_usage_stats,
    sa,
    set_consistent_seed_within_numeric_metric_range_multi_batch_parameter_builder,
    spark_df_taxi_data_schema,
    spark_session,
)

yaml: YAMLHandler = YAMLHandler()


@pytest.fixture()
def bobby_statistics_data_assistant_result(
    monkeypatch,
    no_usage_stats,
    set_consistent_seed_within_numeric_metric_range_multi_batch_parameter_builder,
    bobby_columnar_table_multi_batch_deterministic_data_context: DataContext,
) -> StatisticsDataAssistantResult:
    context: DataContext = bobby_columnar_table_multi_batch_deterministic_data_context

    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }

    data_assistant_result: DataAssistantResult = context.assistants.statistics.run(
        batch_request=batch_request,
        estimation="flag_outliers",
    )

    return cast(StatisticsDataAssistantResult, data_assistant_result)


@pytest.mark.slow  # 6.90s
def test_statistics_data_assistant_result_serialization(
    bobby_statistics_data_assistant_result: StatisticsDataAssistantResult,
) -> None:
    statistics_data_assistant_result_as_dict: dict = (
        bobby_statistics_data_assistant_result.to_dict()
    )
    assert (
        set(statistics_data_assistant_result_as_dict.keys())
        == DataAssistantResult.ALLOWED_KEYS
    )
    assert (
        bobby_statistics_data_assistant_result.to_json_dict()
        == statistics_data_assistant_result_as_dict
    )
    assert len(bobby_statistics_data_assistant_result.profiler_config.rules) == 5


@pytest.mark.integration
def test_statistics_data_assistant_metrics_count(
    bobby_statistics_data_assistant_result: StatisticsDataAssistantResult,
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
    ) in bobby_statistics_data_assistant_result.metrics_by_domain.items():
        if domain.is_superset(other=domain_key):
            num_metrics += len(parameter_values_for_fully_qualified_parameter_names)

    assert num_metrics == 0

    num_metrics = 0
    for (
        domain,
        parameter_values_for_fully_qualified_parameter_names,
    ) in bobby_statistics_data_assistant_result.metrics_by_domain.items():
        num_metrics += len(parameter_values_for_fully_qualified_parameter_names)

    assert num_metrics == 153


@pytest.mark.integration
def test_statistics_data_assistant_result_batch_id_to_batch_identifier_display_name_map_coverage(
    bobby_statistics_data_assistant_result: StatisticsDataAssistantResult,
):
    metrics_by_domain: Optional[
        Dict[Domain, Dict[str, ParameterNode]]
    ] = bobby_statistics_data_assistant_result.metrics_by_domain

    parameter_values_for_fully_qualified_parameter_names: Dict[str, ParameterNode]
    parameter_node: ParameterNode
    batch_id: str
    assert all(
        bobby_statistics_data_assistant_result._batch_id_to_batch_identifier_display_name_map[
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
def test_statistics_data_assistant_result_normalized_metrics_vector_output(
    bobby_statistics_data_assistant_result: StatisticsDataAssistantResult,
):
    """
    This test is a template for compposing a vector of all metrics, computed as part of the run of the effective
    underlying Rule-Based Profiler and normalizing it (so that the magnitute of this vector is unity).  This operation
    can be applied to adjacent runs (e.g., corresponding to dataset being sub-sampled with different sampling fractions)
    and normalized metrics vectors compared (e.g., using square root of mean squared error as difference measure).  Then
    if the error is below a threshold (e.g., 0.1%), then the smaller dataset exhibits the same statistical properties as
    larger dataset, but working with smaller datasets is more efficient (data exploration, Expectation authoring, etc.).
    """
    domain: Domain
    metrics: Dict[str, ParameterNode]
    parameter_name: str
    parameter_node: ParameterNode
    parameter_value: Any
    ndarray_is_datetime_type: bool
    parameter_value_magnitude: float
    metrics_magnitude: float = 0.0
    num_elements: int = 0
    for (
        domain,
        metrics,
    ) in bobby_statistics_data_assistant_result.metrics_by_domain.items():
        for parameter_name, parameter_node in metrics.items():
            parameter_value = np.asarray(parameter_node.value)
            if parameter_value.ndim == 0:
                parameter_value = np.asarray([parameter_node.value])

            (
                ndarray_is_datetime_type,
                parameter_value,
            ) = convert_metric_values_to_float_dtype_best_effort(
                metric_values=parameter_value
            )
            parameter_value_magnitude = np.linalg.norm(parameter_value)
            metrics_magnitude += parameter_value_magnitude * parameter_value_magnitude
            num_elements += 1

    metrics_magnitude = math.sqrt(metrics_magnitude)

    assert np.allclose(metrics_magnitude, 3.331205802908463e3)
    assert (
        num_elements == 153
    )  # This quantity must be equal to the total number of metrics.

    normalized_metrics_vector: MetricValues = []
    for (
        domain,
        metrics,
    ) in bobby_statistics_data_assistant_result.metrics_by_domain.items():
        for parameter_name, parameter_node in metrics.items():
            parameter_value = np.asarray(parameter_node.value)
            if parameter_value.ndim == 0:
                parameter_value = np.asarray([parameter_node.value])

            (
                ndarray_is_datetime_type,
                parameter_value,
            ) = convert_metric_values_to_float_dtype_best_effort(
                metric_values=parameter_value
            )
            parameter_value = parameter_value / metrics_magnitude
            normalized_metrics_vector.append(parameter_value)

    normalized_metrics_vector = np.asarray(normalized_metrics_vector)

    assert normalized_metrics_vector.ndim == 1

    normalized_metrics_vector_magnitude: float = 0.0
    for parameter_value in normalized_metrics_vector:
        parameter_value_magnitude = np.linalg.norm(parameter_value)
        normalized_metrics_vector_magnitude += (
            parameter_value_magnitude * parameter_value_magnitude
        )

    assert np.allclose(normalized_metrics_vector_magnitude, 1.0)


@pytest.mark.integration
@pytest.mark.slow  # 19s
def test_pandas_happy_path_statistics_data_assistant(empty_data_context) -> None:
    """
    The intent of this test is to ensure that our "happy path", exercised by notebooks is in working order.

    1. Setting up Datasource to load 2019 taxi data and 2020 taxi data
    2. Configuring BatchRequest to load 2019 data as multiple batches
    3. Running StatisticsDataAssistant and making sure that StatisticsDataAssistantResult contains relevant fields
    4. Configuring BatchRequest to load 2020 January data
    """
    data_context: gx.DataContext = empty_data_context
    taxi_data_path: str = file_relative_path(
        __file__,
        os.path.join(
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

    # Running statistics data assistant
    result = data_context.assistants.statistics.run(
        batch_request=multi_batch_batch_request
    )

    assert len(result.metrics_by_domain) == 35


@pytest.mark.integration
@pytest.mark.slow  # 104 seconds
def test_sql_happy_path_statistics_data_assistant(
    empty_data_context, test_backends, sa
) -> None:
    """
    The intent of this test is to ensure that our "happy path", exercised by notebooks is in working order.

    1. Loading tables into postgres Docker container by calling helper method load_data_into_postgres_database()
    2. Setting up Datasource to load 2019 taxi data and 2020 taxi data
    3. Configuring BatchRequest to load 2019 data as multiple batches
    4. Running StatisticsDataAssistant and making sure that StatisticsDataAssistantResult contains relevant fields
    5. Configuring BatchRequest to load 2020 January data
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

    # Running statistics data assistant
    result = data_context.assistants.statistics.run(
        batch_request=multi_batch_batch_request
    )

    assert len(result.metrics_by_domain) == 35


@pytest.mark.integration
@pytest.mark.slow  # 149 seconds
def test_spark_happy_path_statistics_data_assistant(
    empty_data_context, spark_df_taxi_data_schema
) -> None:
    """
    The intent of this test is to ensure that our "happy path", exercised by notebooks is in working order.

    1. Setting up Datasource to load 2019 taxi data and 2020 taxi data
    2. Configuring BatchRequest to load 2019 data as multiple batches
    3. Running StatisticsDataAssistant and making sure that StatisticsDataAssistantResult contains relevant fields
    4. Configuring BatchRequest to load 2020 January data
    """
    from pyspark.sql.types import StructType

    schema: StructType = spark_df_taxi_data_schema
    data_context: gx.DataContext = empty_data_context
    taxi_data_path: str = file_relative_path(
        __file__,
        os.path.join(
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

    # Running statistics data assistant
    result = data_context.assistants.statistics.run(
        batch_request=multi_batch_batch_request
    )

    assert len(result.metrics_by_domain) == 35
