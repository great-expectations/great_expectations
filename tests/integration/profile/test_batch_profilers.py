import hashlib
import json
import os

import pytest

from great_expectations.core.batch import BatchRequest
from great_expectations.data_context.util import file_relative_path
from great_expectations.profile.basic_dataset_profiler import BasicDatasetProfiler

from ...test_utils import get_sqlite_temp_table_names


def test_BasicDatasetProfiler_with_sql_based_Validator(
    data_context_with_sql_datasource_for_testing_get_batch,
    sa,
):
    context = data_context_with_sql_datasource_for_testing_get_batch

    filepath = file_relative_path(
        __file__,
        "../../test_sets/test_cases_for_sql_data_connector.db",
    )
    file_text = open(filepath, "rb").read()
    checksum_pre = hashlib.md5(file_text).hexdigest()

    my_profiler = BasicDatasetProfiler()
    my_validator = context.get_validator(
        datasource_name="my_sqlite_db",
        data_connector_name="daily",
        data_asset_name="table_partitioned_by_date_column__A",
        partition_identifiers={"date": "2020-01-15"},
        expectation_suite_name="my_expectation_suite",
        overwrite_existing_expectation_suite=True,
    )
    sqlite_engine = my_validator.execution_engine.engine
    temp_table_names_pre = get_sqlite_temp_table_names(sqlite_engine)
    assert temp_table_names_pre == set()

    my_profiler.profile(my_validator)

    file_text = open(filepath, "rb").read()
    temp_table_names_post = get_sqlite_temp_table_names(sqlite_engine)

    # Verify that Profiling did not add a temp table.
    assert temp_table_names_pre == temp_table_names_post

    checksum_post = hashlib.md5(file_text).hexdigest()
    # NOTE: Abe 20201203: This test somehow alters the contents of test_sets/test_cases_for_sql_data_connector.db
    # This is not the desired behavior. Tests should be read-only.
    # This file is committed as a test fixture---please be careful not to commit a changed version.
    # For the moment, the best way to achieve this is to `chmod 440 tests/test_sets/test_cases_for_sql_data_connector.db`


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
        expectation_suite_name="my_expectation_suite",
        overwrite_existing_expectation_suite=True,
    )

    expectations, validation_results = my_profiler.profile(my_validator)

    print(json.dumps(expectations.to_json_dict(), indent=2))
    print(json.dumps(validation_results.to_json_dict(), indent=2))
