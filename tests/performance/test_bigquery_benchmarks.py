"""Provide performance benchmarks to quantify the impact of PRs that are intended to improve performance and measure
trends over time to identify/prevent performance regressions.
"""

import time
from pathlib import Path

import _pytest.config
import py.path
import pytest
from pytest_benchmark.fixture import BenchmarkFixture

from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
from tests.performance import bigquery_util


@pytest.mark.parametrize("seconds", [0.01, 0.02, 0.04])
def test_fake_sleep_passing(benchmark: BenchmarkFixture, seconds: float):
    # todo(jdimatteo) remove this test
    benchmark.pedantic(
        time.sleep,
        args=(seconds,),
        iterations=1,
        rounds=1,
    )


@pytest.mark.parametrize("number_of_tables", [1, 2, 4, 100])
def test_bikeshare_trips_benchmark(
    benchmark: BenchmarkFixture,
    tmpdir: py.path.local,
    number_of_tables: int,
    pytestconfig: _pytest.config.Config,
):
    """Benchmark performance with a variety of expectations using the BigQuery public dataset
    bigquery-public-data.austin_bikeshare.bikeshare_trips.

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

    checkpoint = bigquery_util.setup_checkpoint(
        number_of_tables=number_of_tables,
        html_dir=tmpdir.strpath,
    )

    result: CheckpointResult = benchmark.pedantic(
        checkpoint.run,
        iterations=1,
        rounds=1,
    )

    # Do some basic sanity checks.
    assert result.success, result
    assert len(result.run_results) == number_of_tables
    html_file_paths = list(Path(tmpdir).glob("validations/**/*.html"))
    # todo(jdimatteo): assert len(html_file_paths) == number_of_tables

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
    # import pydevd_pycharm
    # pydevd_pycharm.settrace('localhost', port=5324, stdoutToServer=True, stderrToServer=True)
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
    expected_validation_results = bigquery_util.expected_validation_results()
    for run_result in result.run_results.values():
        actual_results = [
            result.to_json_dict()
            for result in run_result["validation_result"]["results"]
        ]
        # todo(jdimatteo) ignore order? ignore extra keys?
        assert actual_results == expected_validation_results


def _skip_if_bigquery_performance_tests_not_enabled(
    pytestconfig: _pytest.config.Config,
):
    if not pytestconfig.getoption("bigquery") or not pytestconfig.getoption(
        "performance_tests"
    ):
        pytest.skip(
            "This test requires --bigquery and --performance-tests flags to run."
        )
