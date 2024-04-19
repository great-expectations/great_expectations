"""Tests to ensure core validation flows work with sql assets

NOTE: assertions here take the form of asserting that expectations pass
based on knowledge of the data in the test set.
"""

from typing import List

import pytest

import great_expectations as gx
import great_expectations.expectations as gxe
from great_expectations.checkpoint.v1_checkpoint import Checkpoint
from great_expectations.core.batch_definition import BatchDefinition
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.validation_definition import ValidationDefinition
from great_expectations.data_context.data_context.abstract_data_context import AbstractDataContext
from great_expectations.datasource.fluent.sql_datasource import _SQLAsset

# constants for what we know about the test data
COLUMN_NAME = "x"
ALL_VALUES = list(range(1, 11))
INVALID_VALUES = list(range(1, 12))
VALUES_WITH_NO_DATE = [9]
VALUES_FOR_MOST_RECENT_DATE = [10]
MY_FAVORITE_DAY = {"year": 2000, "month": 6, "day": 1}
VALUES_ON_MY_FAVORITE_DAY = [8]


def _create_expectation(values: List[int]):
    return gxe.ExpectColumnDistinctValuesToEqualSet(column=COLUMN_NAME, value_set=values)


@pytest.fixture
def context() -> AbstractDataContext:
    return gx.get_context(mode="ephemeral")


@pytest.fixture
def postgres_asset(context: AbstractDataContext) -> _SQLAsset:
    CONNECTION_STRING = "postgresql+psycopg2://postgres:@localhost:5432/test_ci"
    DATASOURCE_NAME = "postgres"
    ASSET_NAME = "ten trips"
    TABLE_NAME = "ct_column_values_to_be_between__evaluation_parameters_dataset_1"
    datasource = context.sources.add_postgres(DATASOURCE_NAME, connection_string=CONNECTION_STRING)
    data_asset = datasource.add_table_asset(name=ASSET_NAME, table_name=TABLE_NAME)

    return data_asset


@pytest.fixture
def postgres_whole_table_batch_definition(postgres_asset: _SQLAsset) -> BatchDefinition:
    return postgres_asset.add_batch_definition_whole_table("the whole table")


@pytest.fixture
def postgres_daily_batch_definition(postgres_asset: _SQLAsset) -> BatchDefinition:
    return postgres_asset.add_batch_definition_daily(name="daily", column="ts")


@pytest.fixture
def postgres_daily_batch_definition_descending(postgres_asset: _SQLAsset) -> BatchDefinition:
    return postgres_asset.add_batch_definition_daily(
        name="daily", column="ts", sort_ascending=False
    )


@pytest.mark.postgresql
def test_validate_expectation(postgres_whole_table_batch_definition: BatchDefinition) -> None:
    batch = postgres_whole_table_batch_definition.get_batch()
    expectation = _create_expectation(ALL_VALUES)
    result = batch.validate(expectation)

    assert result.success


@pytest.mark.postgresql
def test_validate_expectation_suite(postgres_whole_table_batch_definition: BatchDefinition) -> None:
    batch = postgres_whole_table_batch_definition.get_batch()
    good_expectation = _create_expectation(ALL_VALUES)
    bad_expectation = _create_expectation(INVALID_VALUES)
    suite = ExpectationSuite(
        "my_suite",
        expectations=[good_expectation, bad_expectation],
    )
    result = batch.validate(suite)

    assert not result.success
    assert [r.success for r in result.results] == [True, False]


@pytest.mark.xfail(reason="TODO: Fix in V1-297")
@pytest.mark.postgresql
def test_validate_daily_expectation(postgres_daily_batch_definition: BatchDefinition) -> None:
    expectation = _create_expectation(VALUES_FOR_MOST_RECENT_DATE)
    batch = postgres_daily_batch_definition.get_batch(batch_parameters=MY_FAVORITE_DAY)
    result = batch.validate(expectation)

    assert result.success


@pytest.mark.postgresql
def test_validation_definition_run_whole_asset(
    postgres_whole_table_batch_definition: BatchDefinition,
) -> None:
    expectation = _create_expectation(ALL_VALUES)
    suite = ExpectationSuite("my_suite", expectations=[expectation])
    validation_definition = ValidationDefinition(
        name="whatever", data=postgres_whole_table_batch_definition, suite=suite
    )
    result = validation_definition.run()

    assert result.success


@pytest.mark.postgresql
def test_validation_definition_daily_ascending(
    postgres_daily_batch_definition: BatchDefinition,
) -> None:
    expectation = _create_expectation(VALUES_FOR_MOST_RECENT_DATE)
    suite = ExpectationSuite("my_suite", expectations=[expectation])
    validation_definition = ValidationDefinition(
        name="whatever", data=postgres_daily_batch_definition, suite=suite
    )
    result = validation_definition.run()

    assert result.success


@pytest.mark.postgresql
def test_validation_definition_daily_descending(
    postgres_daily_batch_definition_descending: BatchDefinition,
) -> None:
    expectation = _create_expectation(VALUES_WITH_NO_DATE)
    suite = ExpectationSuite("my_suite", expectations=[expectation])
    validation_definition = ValidationDefinition(
        name="whatever", data=postgres_daily_batch_definition_descending, suite=suite
    )
    result = validation_definition.run()

    assert result.success


@pytest.mark.postgresql
def test_validation_definition_daily_with_batch_params(
    postgres_daily_batch_definition: BatchDefinition,
) -> None:
    expectation = _create_expectation(VALUES_ON_MY_FAVORITE_DAY)
    suite = ExpectationSuite("my_suite", expectations=[expectation])
    validation_definition = ValidationDefinition(
        name="whatever", data=postgres_daily_batch_definition, suite=suite
    )
    result = validation_definition.run(batch_parameters=MY_FAVORITE_DAY)

    assert result.success


@pytest.mark.postgresql
def test_checkpoint_run_whole_asset(
    context: AbstractDataContext, postgres_whole_table_batch_definition: BatchDefinition
) -> None:
    expectation = _create_expectation(ALL_VALUES)
    suite = ExpectationSuite("my_suite", expectations=[expectation])
    validation_definition = ValidationDefinition(
        name="whatever", data=postgres_whole_table_batch_definition, suite=suite
    )
    checkpoint = context.checkpoints.add(
        Checkpoint(name="whatever", validation_definitions=[validation_definition])
    )
    result = checkpoint.run()

    assert result.success


@pytest.mark.postgresql
def test_checkpoint_daily_ascending(
    context: AbstractDataContext, postgres_daily_batch_definition: BatchDefinition
) -> None:
    expectation = _create_expectation(VALUES_FOR_MOST_RECENT_DATE)
    suite = ExpectationSuite("my_suite", expectations=[expectation])
    validation_definition = ValidationDefinition(
        name="whatever", data=postgres_daily_batch_definition, suite=suite
    )
    checkpoint = context.checkpoints.add(
        Checkpoint(name="whatever", validation_definitions=[validation_definition])
    )
    result = checkpoint.run()

    assert result.success


@pytest.mark.postgresql
def test_checkpoint_daily_descending(
    context: AbstractDataContext,
    postgres_daily_batch_definition_descending: BatchDefinition,
) -> None:
    expectation = _create_expectation(VALUES_WITH_NO_DATE)
    suite = ExpectationSuite("my_suite", expectations=[expectation])
    validation_definition = ValidationDefinition(
        name="whatever", data=postgres_daily_batch_definition_descending, suite=suite
    )
    checkpoint = context.checkpoints.add(
        Checkpoint(name="whatever", validation_definitions=[validation_definition])
    )
    result = checkpoint.run(batch_parameters=None)

    assert result.success


@pytest.mark.postgresql
def test_checkpoint_daily_with_batch_params(
    context: AbstractDataContext, postgres_daily_batch_definition: BatchDefinition
) -> None:
    expectation = _create_expectation(VALUES_ON_MY_FAVORITE_DAY)
    suite = ExpectationSuite("my_suite", expectations=[expectation])
    validation_definition = ValidationDefinition(
        name="whatever", data=postgres_daily_batch_definition, suite=suite
    )
    checkpoint = context.checkpoints.add(
        Checkpoint(name="whatever", validation_definitions=[validation_definition])
    )
    result = checkpoint.run(batch_parameters=MY_FAVORITE_DAY)

    assert result.success
