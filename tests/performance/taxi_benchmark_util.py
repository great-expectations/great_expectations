"""
Helper utilities for creating and testing benchmarks using NYC Taxi data (yellow_trip_data_sample_2019-01.csv)
    found in the tests/test_sets/taxi_yellow_trip_data_samples directory, and used extensively in unittest and
    integration tests for Great Expectations.
"""
import os
from typing import List, Optional

from great_expectations import DataContext
from great_expectations.checkpoint import SimpleCheckpoint
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import (
    DataContextConfig,
    InMemoryStoreBackendDefaults,
)


def create_checkpoint(
    number_of_tables: int, html_dir: Optional[str] = None
) -> SimpleCheckpoint:
    """Create a checkpoint from scratch, including setting up data sources/etc.

    Args:
        number_of_tables: Number of tables validated in the checkpoint. The tables are assumed to be created by
          "setup_bigquery_tables_for_performance_test.sh", which creates 100 tables, so this number must be <= 100.
        html_dir: Directory path to write the HTML Data Docs to. If not specified, Data Docs are not written.

    Returns:
    """
    checkpoint_name = "my_checkpoint"
    datasource_name = "my_datasource"
    data_connector_name = "my_data_connector"

    # These tables are created by "setup_bigquery_tables_for_performance_test.sh", with numbering from 1 to 100.
    assert 1 <= number_of_tables <= 100
    suite_and_asset_names = [f"taxi_trips_{i}" for i in range(1, number_of_tables + 1)]

    context = _create_context(
        datasource_name, data_connector_name, suite_and_asset_names, html_dir
    )
    for suite_name in suite_and_asset_names:
        _add_expectation_configuration(context=context, suite_name=suite_name)

    return _add_checkpoint(
        context,
        datasource_name,
        data_connector_name,
        checkpoint_name,
        suite_and_asset_names,
    )


def expected_validation_results() -> List[dict]:
    return [
        {
            "meta": {},
            "expectation_config": {
                "meta": {},
                "kwargs": {
                    "column_set": [
                        "vendor_id",
                        "pickup_datetime",
                        "dropoff_datetime",
                        "passenger_count",
                        "trip_distance",
                        "rate_code_id",
                        "store_and_fwd_flag",
                        "pickup_location_id",
                        "dropoff_location_id",
                        "payment_type",
                        "fare_amount",
                        "extra",
                        "mta_tax",
                        "tip_amount",
                        "tolls_amount",
                        "improvement_surcharge",
                        "total_amount",
                        "congestion_surcharge",
                    ]
                },
                "expectation_type": "expect_table_columns_to_match_set",
            },
            "result": {
                "observed_value": [
                    "vendor_id",
                    "pickup_datetime",
                    "dropoff_datetime",
                    "passenger_count",
                    "trip_distance",
                    "rate_code_id",
                    "store_and_fwd_flag",
                    "pickup_location_id",
                    "dropoff_location_id",
                    "payment_type",
                    "fare_amount",
                    "extra",
                    "mta_tax",
                    "tip_amount",
                    "tolls_amount",
                    "improvement_surcharge",
                    "total_amount",
                    "congestion_surcharge",
                ]
            },
            "exception_info": {
                "raised_exception": False,
                "exception_traceback": None,
                "exception_message": None,
            },
            "success": True,
        },
        {
            "meta": {},
            "expectation_config": {
                "meta": {},
                "kwargs": {"column": "vendor_id"},
                "expectation_type": "expect_column_values_to_not_be_null",
            },
            "result": {
                "element_count": 10000,
                "unexpected_count": 0,
                "unexpected_percent": 0.0,
                "partial_unexpected_list": [],
                "partial_unexpected_index_list": None,
                "partial_unexpected_counts": [],
            },
            "exception_info": {
                "raised_exception": False,
                "exception_traceback": None,
                "exception_message": None,
            },
            "success": True,
        },
        {
            "meta": {},
            "expectation_config": {
                "meta": {},
                "kwargs": {"column": "vendor_id", "type_": "INTEGER"},
                "expectation_type": "expect_column_values_to_be_of_type",
            },
            "result": {"observed_value": "Integer"},
            "exception_info": {
                "raised_exception": False,
                "exception_traceback": None,
                "exception_message": None,
            },
            "success": True,
        },
        {
            "meta": {},
            "expectation_config": {
                "meta": {},
                "kwargs": {"column": "pickup_datetime", "type_": "STRING"},
                "expectation_type": "expect_column_values_to_be_of_type",
            },
            "result": {"observed_value": "String"},
            "exception_info": {
                "raised_exception": False,
                "exception_traceback": None,
                "exception_message": None,
            },
            "success": True,
        },
        {
            "meta": {},
            "expectation_config": {
                "meta": {},
                "kwargs": {
                    "column": "rate_code_id",
                    "value_set": [1, 2, 3, 4, 5, 6, 99],
                },
                "expectation_type": "expect_column_values_to_be_in_set",
            },
            "result": {
                "element_count": 10000,
                "unexpected_count": 0,
                "unexpected_percent": 0,
                "partial_unexpected_list": [],
                "missing_count": 0,
                "missing_percent": 0.0,
                "unexpected_percent_total": 0.0,
                "unexpected_percent_nonmissing": 0.0,
                "partial_unexpected_index_list": None,
                "partial_unexpected_counts": [],
            },
            "exception_info": {
                "raised_exception": False,
                "exception_traceback": None,
                "exception_message": None,
            },
            "success": True,
        },
        {
            "meta": {},
            "expectation_config": {
                "meta": {},
                "kwargs": {
                    "column": "trip_distance",
                    "max_value": 1000.0,
                    "min_value": 0,
                },
                "expectation_type": "expect_column_values_to_be_between",
            },
            "result": {
                "element_count": 10000,
                "unexpected_count": 0,
                "unexpected_percent": 0.0,
                "partial_unexpected_list": [],
                "missing_count": 0,
                "missing_percent": 0.0,
                "unexpected_percent_total": 0.0,
                "unexpected_percent_nonmissing": 0.0,
                "partial_unexpected_index_list": None,
                "partial_unexpected_counts": [],
            },
            "exception_info": {
                "raised_exception": False,
                "exception_traceback": None,
                "exception_message": None,
            },
            "success": True,
        },
    ]


def _create_context(
    datasource_name: str,
    data_connector_name: str,
    asset_names: List[str],
    html_dir: Optional[str] = None,
) -> DataContext:

    data_docs_sites = (
        {
            "local_site": {
                "class_name": "SiteBuilder",
                "show_how_to_buttons": False,
                "store_backend": {
                    "class_name": "TupleFilesystemStoreBackend",
                    "base_directory": html_dir,
                },
            }
        }
        if html_dir
        else None
    )
    bigquery_project = os.environ["GE_TEST_BIGQUERY_PROJECT"]
    bigquery_dataset = os.environ.get(
        "GE_TEST_BIGQUERY_PERFORMANCE_DATASET", "performance_ci"
    )

    data_context_config = DataContextConfig(
        store_backend_defaults=InMemoryStoreBackendDefaults(),
        data_docs_sites=data_docs_sites,
        anonymous_usage_statistics={"enabled": False},
    )

    context = BaseDataContext(project_config=data_context_config)

    datasource_config = {
        "name": datasource_name,
        "class_name": "Datasource",
        "execution_engine": {
            "class_name": "SqlAlchemyExecutionEngine",
            "connection_string": f"bigquery://{bigquery_project}/{bigquery_dataset}",
        },
        "data_connectors": {
            data_connector_name: {
                "class_name": "ConfiguredAssetSqlDataConnector",
                "name": "whole_table",
                "assets": {asset_name: {} for asset_name in asset_names},
            },
        },
    }
    context.add_datasource(**datasource_config)
    return context


def _add_checkpoint(
    context: BaseDataContext,
    datasource_name: str,
    data_connector_name: str,
    checkpoint_name: str,
    suite_and_asset_names=[],
) -> SimpleCheckpoint:
    validations = [
        {
            "expectation_suite_name": suite_and_asset_name,
            "batch_request": {
                "datasource_name": datasource_name,
                "data_connector_name": data_connector_name,
                "data_asset_name": suite_and_asset_name,
                "batch_spec_passthrough": {"create_temp_table": False},
            },
        }
        for suite_and_asset_name in suite_and_asset_names
    ]
    return context.add_checkpoint(
        name=checkpoint_name,
        class_name="SimpleCheckpoint",
        validations=validations,
        run_name_template="my_run_name",
    )


def _add_expectation_configuration(context: BaseDataContext, suite_name: str):
    suite = context.create_expectation_suite(expectation_suite_name=suite_name)
    suite.add_expectation(
        expectation_configuration=ExpectationConfiguration(
            expectation_type="expect_table_columns_to_match_set",
            kwargs={
                "column_set": [
                    "vendor_id",
                    "pickup_datetime",
                    "dropoff_datetime",
                    "passenger_count",
                    "trip_distance",
                    "rate_code_id",
                    "store_and_fwd_flag",
                    "pickup_location_id",
                    "dropoff_location_id",
                    "payment_type",
                    "fare_amount",
                    "extra",
                    "mta_tax",
                    "tip_amount",
                    "tolls_amount",
                    "improvement_surcharge",
                    "total_amount",
                    "congestion_surcharge",
                ]
            },
        )
    )
    suite.add_expectation(
        expectation_configuration=ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": "vendor_id"},
        )
    )
    suite.add_expectation(
        expectation_configuration=ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_of_type",
            kwargs={"column": "vendor_id", "type_": "INTEGER"},
        )
    )
    suite.add_expectation(
        expectation_configuration=ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_of_type",
            kwargs={"column": "pickup_datetime", "type_": "STRING"},
        )
    )
    suite.add_expectation(
        expectation_configuration=ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            # rate_code_id refers to the final rate code in effect at the end of the trip
            # (https://www1.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_recordsgyellow.pdf)
            # 1=Standard rate
            # 2=JFK
            # 3=Newark
            # 4=Nassau or Westchester
            # 5=Negotiated fare
            # 6=Group ride
            # 99=NA
            kwargs={"column": "rate_code_id", "value_set": [1, 2, 3, 4, 5, 6, 99]},
        )
    )
    suite.add_expectation(
        expectation_configuration=ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={
                "column": "trip_distance",
                "min_value": 0,
                "max_value": 1000.0,
            },
        )
    )

    # Save the expectation suite or else it doesn't show up in the data docs.
    context.save_expectation_suite(
        expectation_suite=suite, expectation_suite_name=suite_name
    )
