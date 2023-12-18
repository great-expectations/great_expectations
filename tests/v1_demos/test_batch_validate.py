"""
Some form of these tests will live in doc snippets for our quickstart guides. This is here for now to gate closing our
current epic that begins refactoring how we author expectation suites for V1.
"""
from typing import Callable

import pytest

import great_expectations as gx
import great_expectations.expectations as gxe
from great_expectations.data_context import AbstractDataContext
from great_expectations.datasource.fluent.interfaces import Batch


def _get_csv_batch(context: AbstractDataContext) -> Batch:
    return context.sources.pandas_default.read_csv(
        "https://raw.githubusercontent.com/great-expectations/gx_tutorials/main/data/yellow_tripdata_sample_2019-01.csv"
    )


def _get_sql_batch(context: AbstractDataContext) -> Batch:
    return context.sources.pandas_default.read_sql(
        "SELECT * FROM yellow_tripdata_sample_2019_01",
        con="postgresql://postgres:postgres@localhost:5432/test_ci",
    )


@pytest.fixture(
    params=[
        pytest.param(_get_csv_batch, marks=[pytest.mark.filesystem]),
        pytest.param(_get_sql_batch, marks=[pytest.mark.postgresql]),
    ]
)
def get_batch(request: pytest.FixtureRequest) -> Callable[[AbstractDataContext], Batch]:
    return request.param


def test_batch_validate(get_batch: Callable[[AbstractDataContext], Batch]):
    context = gx.get_context()
    batch = get_batch(context)
    expectation = gxe.ExpectColumnValuesToNotBeNull(
        "pu_datetime",
        notes="These are filtered out upstream, because the entire record is garbage if there is no pu_datetime",
    )
    result = batch.validate(expectation)
    assert not result.success
    expectation.mostly = 0.8
    result = batch.validate(expectation)
    assert result.success
    suite = context.add_expectation_suite("quickstart")
    suite.add(expectation)
    suite.add(
        gxe.ExpectColumnValuesToBeBetween("passenger_count", min_value=1, max_value=6)
    )
    suite_result = batch.validate(suite)
    assert suite_result
