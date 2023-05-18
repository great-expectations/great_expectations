#!/usr/bin/env python3

"""
Test performance using bigquery.
"""

import cProfile
import os
import sys
from collections.abc import Mapping
from pathlib import Path

import _pytest.config
import py.path
import pytest
from pytest_benchmark.fixture import BenchmarkFixture

from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
from great_expectations.core.async_executor import patch_https_connection_pool
from tests.performance import taxi_benchmark_util


@pytest.mark.parametrize(
    "backend_api",
    [
        "V2",  # Batch Kwargs API
        "V3",  # Batch Request API
    ],
)
@pytest.mark.parametrize("write_data_docs", [False, True])
@pytest.mark.parametrize("number_of_tables", [1, 2, 4, 8, 16, 100])
def test_taxi_trips_benchmark(
    benchmark: BenchmarkFixture,
    tmpdir: py.path.local,
    pytestconfig: _pytest.config.Config,
    number_of_tables: int,
    write_data_docs: bool,
    backend_api: str,
):
    """Benchmark performance with a variety of expectations using NYC Taxi data (yellow_tripdata_sample_2019-01.csv)
    found in the tests/test_sets/taxi_yellow_tripdata_samples directory, and used extensively in unittest and
    integration tests for Great Expectations.

    To simulate a more realistic usage of Great Expectations with several tables, this benchmark is run with 1 or more
    copies of the table, and each table has multiple expectations run on them. For simplicity, the expectations run on
    each table are identical. The specific expectations are somewhat arbitrary but were chosen to be representative of
    a (non-public) real use case of Great Expectations.

    Note: This data being tested in this benchmark generally shouldn't be changed over time, because consistent
    benchmarks are more useful to compare trends over time. Please do not change the tables being tested with nor change
    the expectations being used by this benchmark. Instead of changing this benchmark's data/expectations, please
    consider adding a new benchmark (or at least rename this benchmark to provide clarity that results are not directly
    comparable because of the data change).
    """
    _skip_if_bigquery_performance_tests_not_enabled(pytestconfig)

    _optionally_patch_connection_pool(pytestconfig)

    html_dir = (
        os.environ.get("GE_BENCHMARK_HTML_DIRECTORY", tmpdir.strpath)
        if write_data_docs
        else None
    )

    checkpoint = taxi_benchmark_util.create_checkpoint(
        number_of_tables=number_of_tables,
        html_dir=html_dir,
        backend_api=backend_api,
    )
    if os.environ.get("GE_PROFILE_FILE_PATH"):
        cProfile.runctx(
            "checkpoint.run()",
            None,
            locals(),
            filename=os.environ["GE_PROFILE_FILE_PATH"],
        )
        return
    else:
        result: CheckpointResult = benchmark.pedantic(
            checkpoint.run,
            iterations=1,
            rounds=1,
        )

    # Do some basic sanity checks.
    assert result.success, result
    assert len(result.run_results) == number_of_tables
    if write_data_docs:
        html_file_paths = list(Path(html_dir).glob("validations/**/*.html"))
        assert len(html_file_paths) == number_of_tables

    # Check that run results contain the right number of suites, assets, and table names.
    assert (
        len(
            {
                run_result["validation_result"]["meta"]["expectation_suite_name"]
                for run_result in result.run_results.values()
            }
        )
        == number_of_tables
    )
    batch_key = "batch_spec" if backend_api == "V3" else "batch_kwargs"
    for field in ["data_asset_name", "table_name" if backend_api == "V3" else "table"]:
        assert (
            len(
                {
                    run_result["validation_result"]["meta"][batch_key][field]
                    for run_result in result.run_results.values()
                }
            )
            == number_of_tables
        )

    # Check that every expectation result was correct.
    expected_results = taxi_benchmark_util.expected_validation_results()

    for run_result in result.run_results.values():
        actual_results = [
            result.to_json_dict()
            for result in run_result["validation_result"]["results"]
        ]
        assert len(expected_results) == len(actual_results)
        for expected_result, actual_result in zip(expected_results, actual_results):
            description_for_error_reporting = (
                f'{expected_result["expectation_config"]["expectation_type"]} result'
            )
            _recursively_assert_actual_result_matches_expected_result_keys(
                expected_result, actual_result, description_for_error_reporting
            )


def _recursively_assert_actual_result_matches_expected_result_keys(
    expected, actual, description_for_error_reporting
):
    """Assert that actual equals expected while ignoring key order and extra keys not present in expected.

    Expected mappings may be a subset of actual mappings -- this can be useful to make tests less fragile so that they
    don't incorrectly fail when new keys are added.

    Args:
        expected: The expected result. For mappings, every key in expected must be present in actual with the same value
            (while recursively ignoring nested key order and extra nested keys in any nested values).
        actual: The actual result for comparison with expected result.
        description_for_error_reporting: Description to provide context during error reporting. For each recursive call,
            the description is updated to also include the key. For example, if the initial description is
            "expect_table_columns_to_match_set result" and the assertion fails with the "raised_exception" key nested in
            the "exception_info" key then pytest will report with a description as shown in the following:

            >           assert expected == actual, description_for_error_reporting
            E           AssertionError: expect_table_columns_to_match_set result["exception_info"]["raised_exception"]
            E           assert True == False
            E             +True
            E             -False
    """
    if isinstance(expected, Mapping):
        for expected_key in expected.keys():
            assert expected_key in actual.keys(), description_for_error_reporting
            _recursively_assert_actual_result_matches_expected_result_keys(
                expected[expected_key],
                actual[expected_key],
                description_for_error_reporting + f'["{expected_key}"]',
            )
    else:
        assert expected == actual, description_for_error_reporting


def _skip_if_bigquery_performance_tests_not_enabled(
    pytestconfig: _pytest.config.Config,
):
    if not pytestconfig.getoption("bigquery") or not pytestconfig.getoption(
        "performance_tests"
    ):
        pytest.skip(
            "This test requires --bigquery and --performance-tests flags to run."
        )


def _optionally_patch_connection_pool(pytestconfig: _pytest.config.Config):
    """Increase the connection pool size if we can, to avoid connection pool exhaustion."""
    gcp_project = os.environ.get("GE_TEST_GCP_PROJECT")
    if not gcp_project:
        raise ValueError(
            "Environment Variable GE_TEST_GCP_PROJECT is required to run BigQuery integration tests"
        )

    patch_https_connection_pool(
        concurrency_config=taxi_benchmark_util.concurrency_config(),
        google_cloud_project=gcp_project,
    )


if __name__ == "__main__":
    # For profiling, it can be useful to support running this script directly instead of using pytest to run.
    sys.exit(pytest.main(sys.argv))
