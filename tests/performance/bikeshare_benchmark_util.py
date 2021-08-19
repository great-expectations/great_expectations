"""
Helper utilities for creating and testing benchmarks using the BigQuery public dataset
bigquery-public-data.austin_bikeshare.bikeshare_trips.
"""
import os
from typing import List

from great_expectations.checkpoint import SimpleCheckpoint
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import (
    DataContextConfig,
    InMemoryStoreBackendDefaults,
)


def create_checkpoint(number_of_tables: int, html_dir: str) -> SimpleCheckpoint:
    """Create a checkpoint from scratch, including setting up data sources/etc.

    Args:
        number_of_tables: Number of tables validated in the checkpoint. The tables are assumed to be created by
          "setup_bigquery_tables_for_performance_test.sh", which creates 100 tables, so this number must be <= 100.
        html_dir: Directory path to write the HTML data docs to.

    Returns:
    """
    checkpoint_name = "my_checkpoint"
    datasource_and_dataconnector_name = "my_datasource_and_dataconnector"

    # These tables are created by "setup_bigquery_tables_for_performance_test.sh", with numbering from 1 to 100.
    assert 1 <= number_of_tables <= 100
    suite_and_asset_names = [
        f"bikeshare_trips_{i}" for i in range(1, number_of_tables + 1)
    ]

    context = _create_context(
        datasource_and_dataconnector_name, suite_and_asset_names, html_dir
    )
    for suite_name in suite_and_asset_names:
        _add_expectation_configuration(context=context, suite_name=suite_name)

    return _add_checkpoint(
        context,
        datasource_and_dataconnector_name,
        checkpoint_name,
        suite_and_asset_names,
    )


def expected_validation_results() -> List[dict]:
    """The correct validation results for each table, intended to be used for testing benchmark runs."""
    return [
        {
            "exception_info": {
                "raised_exception": False,
                "exception_traceback": None,
                "exception_message": None,
            },
            "success": True,
            "result": {
                "observed_value": [
                    "trip_id",
                    "subscriber_type",
                    "bikeid",
                    "start_time",
                    "start_station_id",
                    "start_station_name",
                    "end_station_id",
                    "end_station_name",
                    "duration_minutes",
                ]
            },
            "meta": {},
            "expectation_config": {
                "meta": {},
                "kwargs": {
                    "column_set": [
                        "trip_id",
                        "subscriber_type",
                        "bikeid",
                        "start_time",
                        "start_station_id",
                        "start_station_name",
                        "end_station_id",
                        "end_station_name",
                        "duration_minutes",
                    ]
                },
                "expectation_type": "expect_table_columns_to_match_set",
            },
        },
        {
            "exception_info": {
                "raised_exception": False,
                "exception_traceback": None,
                "exception_message": None,
            },
            "success": True,
            "result": {
                "element_count": 1342066,
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
            "meta": {},
            "expectation_config": {
                "meta": {},
                "kwargs": {"column": "trip_id"},
                "expectation_type": "expect_column_values_to_be_unique",
            },
        },
        {
            "exception_info": {
                "raised_exception": False,
                "exception_traceback": None,
                "exception_message": None,
            },
            "success": True,
            "result": {
                "element_count": 1342066,
                "unexpected_count": 0,
                "unexpected_percent": 0.0,
                "partial_unexpected_list": [],
                "partial_unexpected_index_list": None,
                "partial_unexpected_counts": [],
            },
            "meta": {},
            "expectation_config": {
                "meta": {},
                "kwargs": {"column": "trip_id"},
                "expectation_type": "expect_column_values_to_not_be_null",
            },
        },
        {
            "exception_info": {
                "raised_exception": False,
                "exception_traceback": None,
                "exception_message": None,
            },
            "success": True,
            "result": {"observed_value": "Integer"},
            "meta": {},
            "expectation_config": {
                "meta": {},
                "kwargs": {"column": "trip_id", "type_": "INTEGER"},
                "expectation_type": "expect_column_values_to_be_of_type",
            },
        },
        {
            "exception_info": {
                "raised_exception": False,
                "exception_traceback": None,
                "exception_message": None,
            },
            "success": True,
            "result": {"observed_value": "String"},
            "meta": {},
            "expectation_config": {
                "meta": {},
                "kwargs": {"column": "bikeid", "type_": "STRING"},
                "expectation_type": "expect_column_values_to_be_of_type",
            },
        },
        {
            "exception_info": {
                "raised_exception": False,
                "exception_traceback": None,
                "exception_message": None,
            },
            "success": True,
            "result": {
                "element_count": 1342066,
                "unexpected_count": 0,
                "unexpected_percent": 0.0,
                "partial_unexpected_list": [],
                "missing_count": 6551,
                "missing_percent": 0.48812800562714503,
                "unexpected_percent_total": 0.0,
                "unexpected_percent_nonmissing": 0.0,
                "partial_unexpected_index_list": None,
                "partial_unexpected_counts": [],
            },
            "meta": {},
            "expectation_config": {
                "meta": {},
                "kwargs": {
                    "column": "subscriber_type",
                    "value_set": _subscriber_types(),
                },
                "expectation_type": "expect_column_values_to_be_in_set",
            },
        },
        {
            "exception_info": {
                "raised_exception": False,
                "exception_traceback": None,
                "exception_message": None,
            },
            "success": True,
            "result": {
                "element_count": 1342066,
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
            "meta": {},
            "expectation_config": {
                "meta": {},
                "kwargs": {
                    "column": "duration_minutes",
                    "max_value": 36000,
                    "min_value": 0,
                },
                "expectation_type": "expect_column_values_to_be_between",
            },
        },
    ]


def _create_context(
    datasource_and_dataconnector_name: str,
    asset_names: List[str],
    html_dir: str,
) -> BaseDataContext:
    store_backend = {
        "class_name": "TupleFilesystemStoreBackend",
        "base_directory": html_dir,
    }
    data_docs_sites = {
        "local_site": {
            "class_name": "SiteBuilder",
            "show_how_to_buttons": False,
            "store_backend": store_backend,
        }
    }
    bigquery_project = os.environ["GE_TEST_BIGQUERY_PROJECT"]
    bigquery_dataset = os.environ.get(
        "GE_TEST_BIGQUERY_PEFORMANCE_DATASET", "performance_ci"
    )

    data_context_config = DataContextConfig(
        store_backend_defaults=InMemoryStoreBackendDefaults(),
        data_docs_sites=data_docs_sites,
        anonymous_usage_statistics={"enabled": False},
        datasources={
            datasource_and_dataconnector_name: {
                "class_name": "Datasource",
                "execution_engine": {
                    "class_name": "SqlAlchemyExecutionEngine",
                    "connection_string": f"bigquery://{bigquery_project}/{bigquery_dataset}",
                },
                "data_connectors": {
                    datasource_and_dataconnector_name: {
                        "class_name": "ConfiguredAssetSqlDataConnector",
                        "name": "whole_table",
                        "assets": {asset_name: {} for asset_name in asset_names},
                    },
                },
            },
        },
    )
    return BaseDataContext(project_config=data_context_config)


def _add_checkpoint(
    context: BaseDataContext,
    datasource_and_dataconnector_name: str,
    checkpoint_name: str,
    suite_and_asset_names=[],
) -> SimpleCheckpoint:
    validations = [
        {
            "expectation_suite_name": suite_and_asset_name,
            "batch_request": {
                "datasource_name": datasource_and_dataconnector_name,
                "data_connector_name": datasource_and_dataconnector_name,
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
                    "trip_id",
                    "subscriber_type",
                    "bikeid",
                    "start_time",
                    "start_station_id",
                    "start_station_name",
                    "end_station_id",
                    "end_station_name",
                    "duration_minutes",
                ]
            },
        )
    )
    suite.add_expectation(
        expectation_configuration=ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_unique",
            kwargs={"column": "trip_id"},
        )
    )
    suite.add_expectation(
        expectation_configuration=ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": "trip_id"},
        )
    )
    suite.add_expectation(
        expectation_configuration=ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_of_type",
            kwargs={"column": "trip_id", "type_": "INTEGER"},
        )
    )
    suite.add_expectation(
        expectation_configuration=ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_of_type",
            kwargs={"column": "bikeid", "type_": "STRING"},
        )
    )
    suite.add_expectation(
        expectation_configuration=ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={
                "column": "subscriber_type",
                "value_set": _subscriber_types(),
            },
        )
    )
    suite.add_expectation(
        expectation_configuration=ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={
                "column": "duration_minutes",
                "min_value": 0,
                "max_value": 36000,
            },
        )
    )

    # Save the expectation suite or else it doesn't show up in the data docs.
    context.save_expectation_suite(
        expectation_suite=suite, expectation_suite_name=suite_name
    )


def _subscriber_types() -> List[str]:
    return [
        "Walk Up",
        "24-Hour Kiosk (Austin B-cycle)",
        "Annual Membership (Austin B-cycle)",
        "Founding Member (Austin B-cycle)",
        "Local365",
        "Weekender",
        "Local30",
        "Annual",
        "Explorer",
        "Founding Member",
        "RideScout Single Ride",
        "Annual Member",
        "7-Day",
        "Annual (Denver B-cycle)",
        "Semester Membership",
        "Local365+Guest Pass",
        "Annual Pass",
        "Republic Rider (Annual)",
        "null",
        "Local365 Youth with helmet (age 13-17 riders)",
        "Try Before You Buy Special",
        "Annual Membership",
        "Local365 ($80 plus tax)",
        "Weekender ($15 plus tax)",
        "Explorer ($8 plus tax)",
        "Local30 ($11 plus tax)",
        "Semester Membership (Austin B-cycle)",
        "24-Hour-Online (Austin B-cycle)",
        "Annual (Madison B-cycle)",
        "Annual (Nashville B-cycle)",
        "7-Day Membership (Austin B-cycle)",
        "Annual (San Antonio B-cycle)",
        "Annual Member (Houston B-cycle)",
        "Annual (Boulder B-cycle)",
        "Annual Membership (Fort Worth Bike Sharing)",
        "Annual (Denver Bike Sharing)",
        "Annual Plus",
        "U.T. Student Membership",
        "Local365 Youth (age 13-17 riders)",
        "Madtown Monthly",
        "Annual Plus Membership",
        "HT Ram Membership",
        "24 Hour Walk Up Pass",
        "3-Day Weekender",
        "3-Day Explorer",
        "$1 Pay by Trip Fall Special",
        "Single Trip",
        "Single Trip (Pay-as-you-ride)",
        "Pay-as-you-ride",
        "Single Trip Ride",
        "Annual ",
        "Local365+Guest Pass- 1/2 off Anniversary Special",
        "Local365- 1/2 off Anniversary Special",
        "$1 Pay by Trip Winter Special",
        "Single Trip ",
        "Annual Membership ",
        "ACL 2019 Pass",
        "Local31",
        "ACL Weekend Pass Special (Austin B-cycle)",
        "Annual (Cincy Red Bike)",
        "Membership: pay once  one-year commitment",
        "Annual (Kansas City B-cycle)",
        "Annual (Broward B-cycle)",
        "Annual Pass (30 minute)",
        "Annual Membership (Charlotte B-cycle)",
        "FunFunFun Fest 3 Day Pass",
        "Local365 Youth (age 13-17 riders)- 1/2 off Special",
        "24-Hour Membership (Austin B-cycle)",
        "Annual Membership (Indy - Pacers Bikeshare )",
        "Annual Membership (GREENbike)",
        "Denver B-cycle Founder",
        "Republic Rider",
        "Aluminum Access",
        "Heartland Pass (Annual Pay)",
        "UT Student Membership",
        "PROHIBITED",
        "RESTRICTED",
        "Annual (Omaha B-cycle)",
        "Heartland Pass (Monthly Pay)",
    ]
