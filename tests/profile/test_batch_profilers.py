import os
import json
import pytest

from great_expectations.profile.basic_dataset_profiler import BasicDatasetProfiler
from great_expectations.core.batch import BatchRequest

def test_BasicDatasetProfiler_with_sql_based_Validator(
    data_context_with_sql_datasource_for_testing_get_batch,
):
    context = data_context_with_sql_datasource_for_testing_get_batch

    my_profiler = BasicDatasetProfiler()
    my_validator = context.get_validator(
        datasource_name="my_sqlite_db",
        data_connector_name="daily",
        data_asset_name="table_partitioned_by_date_column__A",
        partition_identifiers={"date": "2020-01-15"},
        create_expectation_suite_with_name="my_expectation_suite",
    )

    # NOTE: Abe 20201203: This test fails somewhere in the SQL resolve_metrics stack.
    # To my knowledge, I'm not doing anything crazy or new---just running BasicDatasetProfiler on a Batch.
    # Can you debug this?
    my_profiler.profile(my_validator)

    # CAUTION: Abe 20201203: Also, I think this test somehow alters the contents of test_sets/test_cases_for_sql_data_connector.db
    # This file is committed as a test fixture---please be careful not to commit a changed version.
    # Also, tests should be read-only.

def test_BasicDatasetProfiler_with_pandas_based_Validator(
    data_context_with_pandas_datasource_for_testing_get_batch,
):
    context = data_context_with_pandas_datasource_for_testing_get_batch

    # batches = context.get_batch_list_from_new_style_datasource({
    #     "datasource_name": "my_pandas_datasource",
    #     "data_connector_name": "my_filesystem_data_connector",
    #     "data_asset_name": "A",
    # })
    # for batch in batches:
    #     print(batch.batch_definition)

    my_profiler = BasicDatasetProfiler()
    my_validator = context.get_validator(
        datasource_name="my_pandas_datasource",
        data_connector_name="my_filesystem_data_connector",
        data_asset_name="A",
        subdirectory="A",
        number="1",
        create_expectation_suite_with_name="my_expectation_suite",
    )

    expectations, validation_results = my_profiler.profile(my_validator)

    print(json.dumps(expectations.to_json_dict(), indent=2))
    print(json.dumps(validation_results.to_json_dict(), indent=2))
