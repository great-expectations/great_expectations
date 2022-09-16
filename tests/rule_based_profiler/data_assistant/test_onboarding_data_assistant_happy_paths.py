import os
from typing import List

import pytest

import great_expectations as ge
from great_expectations.core import ExpectationSuite
from great_expectations.core.batch import BatchRequest
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context.util import file_relative_path

yaml = YAMLHandler()


@pytest.mark.integration
@pytest.mark.slow
def test_pandas_happy_path(empty_data_context) -> None:
    data_context: ge.DataContext = empty_data_context
    taxi_data_path: str = file_relative_path(
        __file__, os.path.join("..", "..", "test_sets", "taxi_yellow_tripdata_samples")
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

    # data_context.test_yaml_config(yaml.dump(datasource_config))
    data_context.add_datasource(**datasource_config)
    multi_batch_batch_request: BatchRequest = BatchRequest(
        datasource_name="taxi_data",
        data_connector_name="configured_data_connector_multi_batch_asset",
        data_asset_name="yellow_tripdata_2019",
    )
    batch_request: BatchRequest = multi_batch_batch_request
    batch_list = data_context.get_batch_list(batch_request=batch_request)
    assert len(batch_list) == 12

    result = data_context.assistants.onboarding.run(
        batch_request=multi_batch_batch_request
    )
    suite: ExpectationSuite = ExpectationSuite(
        expectation_suite_name="taxi_data_2019_suite"
    )
    resulting_configurations: List[
        ExpectationConfiguration
    ] = suite.add_expectation_configurations(
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
@pytest.mark.slow  # 149 seconds
def test_spark_happy_path(empty_data_context, spark_session) -> None:
    from pyspark.sql.types import (
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    schema: StructType = StructType(
        [
            StructField("vendor_id", IntegerType(), True),
            StructField("pickup_datetime", TimestampType(), True),
            StructField("dropoff_datetime", TimestampType(), True),
            StructField("passenger_count", IntegerType(), True),
            StructField("trip_distance", DoubleType(), True),
            StructField("rate_code_id", IntegerType(), True),
            StructField("store_and_fwd_flag", StringType(), True),
            StructField("pickup_location_id", IntegerType(), True),
            StructField("dropoff_location_id", IntegerType(), True),
            StructField("payment_type", IntegerType(), True),
            StructField("fare_amount", DoubleType(), True),
            StructField("extra", DoubleType(), True),
            StructField("mta_tax", DoubleType(), True),
            StructField("tip_amount", DoubleType(), True),
            StructField("tolls_amount", DoubleType(), True),
            StructField("improvement_surcharge", DoubleType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("congestion_surcharge", DoubleType(), True),
        ]
    )
    data_context: ge.DataContext = empty_data_context
    taxi_data_path: str = file_relative_path(
        __file__, os.path.join("..", "..", "test_sets", "taxi_yellow_tripdata_samples")
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
                    # "yellow_tripdata_2020": {
                    #     "group_names": ["year", "month"],
                    #     "pattern": "yellow_tripdata_sample_(2020)-(\\d.*)\\.csv",
                    # },
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

    result = data_context.assistants.onboarding.run(
        batch_request=multi_batch_batch_request
    )
    suite: ExpectationSuite = ExpectationSuite(
        expectation_suite_name="taxi_data_2019_suite"
    )
    # resulting_configurations: List[
    #     ExpectationConfiguration
    # ] = suite.add_expectation_configurations(
    #     expectation_configurations=result.expectation_configurations
    # )
    # data_context.save_expectation_suite(expectation_suite=suite)
    # # batch_request for checkpoint
    # single_batch_batch_request: BatchRequest = BatchRequest(
    #     datasource_name="taxi_data",
    #     data_connector_name="configured_data_connector_multi_batch_asset",
    #     data_asset_name="yellow_tripdata_2020",
    #     data_connector_query={
    #         "batch_filter_parameters": {"year": "2020", "month": "01"}
    #     },
    # )
    # checkpoint_config: dict = {
    #     "name": "my_checkpoint",
    #     "config_version": 1,
    #     "class_name": "SimpleCheckpoint",
    #     "validations": [
    #         {
    #             "batch_request": single_batch_batch_request,
    #             "expectation_suite_name": "taxi_data_2019_suite",
    #         }
    #     ],
    # }
    # data_context.add_checkpoint(**checkpoint_config)
    # results = data_context.run_checkpoint(checkpoint_name="my_checkpoint")
    # assert results.success is False


@pytest.mark.integration
@pytest.mark.slow
def test_sql_happy_path(empty_data_context) -> None:
    pass
