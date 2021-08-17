# todo(jdimatteo): add a performance test change log and only run performance test when that file changes.
#  include git describe output in json
import os
from pathlib import Path

import py.path
import pytest
from pytest_benchmark.fixture import BenchmarkFixture

from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import (
    DataContextConfig,
    InMemoryStoreBackendDefaults,
)
from tests.performance import bigquery_util


@pytest.mark.parametrize("number_of_tables", [1, 2, 4, 100])
def test_bikeshare_trips(
    benchmark: BenchmarkFixture, tmpdir: py.path.local, number_of_tables: int
):
    checkpoint = bigquery_util.setup_checkpoint(
        number_of_tables=number_of_tables,
        html_dir=tmpdir.strpath,
    )
    result: CheckpointResult = benchmark.pedantic(
        checkpoint.run,
        iterations=1,
        rounds=1,
    )

    import pydevd_pycharm

    pydevd_pycharm.settrace(
        "localhost", port=5324, stdoutToServer=True, stderrToServer=True
    )

    # Do some basic sanity checks.
    assert result.success, result
    assert len(result.run_results) == number_of_tables
    html_file_paths = list(Path(tmpdir).glob("validations/**/*.html"))
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
        assert actual_results == expected_validation_results, actual_results
