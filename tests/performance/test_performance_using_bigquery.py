# todo(jdimatteo): add a performance test change log and only run performance test when that file changes.
#  include git describe output in json
import os

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
def test_bikeshare_trips(benchmark: BenchmarkFixture, number_of_tables):
    checkpoint = bigquery_util.setup_checkpoint(
        number_of_tables=number_of_tables,
        html_dir=_html_dir(),
    )
    result: CheckpointResult = benchmark.pedantic(
        checkpoint.run,
        iterations=1,
        rounds=1,
    )
    assert result.success, result


def _html_dir() -> str:
    return os.path.join(os.path.abspath(os.path.dirname(__file__)), "html")
