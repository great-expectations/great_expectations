#!/usr/bin/env python3

"""
Test performance using bigquery.
"""

import cProfile
import os
import sys
from pathlib import Path

import _pytest.config
import py.path
import pytest
from pytest_benchmark.fixture import BenchmarkFixture

from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
from tests.performance import taxi_benchmark_util


@pytest.mark.parametrize("write_data_docs", [False, True])
@pytest.mark.parametrize("number_of_tables", [1, 2, 4, 8, 16, 100])
def test_taxi_trips_benchmark(
    benchmark: BenchmarkFixture,
    tmpdir: py.path.local,
    pytestconfig: _pytest.config.Config,
    number_of_tables: int,
    write_data_docs: bool,
):
    """Benchmark performance with a variety of expectations using NYC Taxi data (yellow_trip_data_sample_2019-01.csv)
    found in the tests/test_sets/taxi_yellow_trip_data_samples directory, and used extensively in unittest and
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

    checkpoint = taxi_benchmark_util.create_checkpoint(
        number_of_tables=number_of_tables,
        html_dir=tmpdir.strpath if write_data_docs else None,
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
    html_file_paths = list(Path(tmpdir).glob("validations/**/*.html"))
    assert len(html_file_paths) == (number_of_tables if write_data_docs else 0)

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
    for field in ["data_asset_name", "table_name"]:
        assert (
            len(
                {
                    run_result["validation_result"]["meta"]["batch_spec"][field]
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
            # Assert individual keys so that test doesn't fail if new keys are added.
            # Note: if this proves too fragile, consider enhancing logic to ignore extra nested keys and/or only check
            # specific keys.
            for expected_key in expected_result.keys():
                assert expected_key in actual_result
                assert expected_result[expected_key] == actual_result[expected_key]


def _skip_if_bigquery_performance_tests_not_enabled(
    pytestconfig: _pytest.config.Config,
):
    if not pytestconfig.getoption("bigquery") or not pytestconfig.getoption(
        "performance_tests"
    ):
        pytest.skip(
            "This test requires --bigquery and --performance-tests flags to run."
        )


if __name__ == "__main__":
    # For profiling, it can be useful to support running this script directly instead of using pytest to run.
    sys.exit(pytest.main(sys.argv))
