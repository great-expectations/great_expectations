"""Tests to ensure core validation flows work with sql assets

NOTE: assertions here take the form of asserting that expectations pass
based on knowledge of the data in the test set.
"""

from typing import Dict, Optional

import pytest

import great_expectations as gx
import great_expectations.expectations as gxe
from great_expectations.checkpoint.checkpoint import Checkpoint
from great_expectations.core.batch_definition import BatchDefinition
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.validation_definition import ValidationDefinition
from great_expectations.data_context.data_context.abstract_data_context import AbstractDataContext
from great_expectations.datasource.fluent.sql_datasource import _SQLAsset
from tests.integration.common_workflows.conftest import CONNECTION_STRING, TABLE_NAME

pytestmark = pytest.mark.postgresql

# constants for what we know about the test data
COLUMN_NAME = "x"
ALL_VALUES = list(range(1, 11))
INVALID_VALUES = list(range(1, 12))
VALUES_WITH_NO_DATE = [9]
VALUES_FOR_MOST_RECENT_DATE = [10]
MY_FAVORITE_DAY = {"year": 2000, "month": 6, "day": 1}
VALUES_ON_MY_FAVORITE_DAY = [8]


@pytest.fixture
def context() -> AbstractDataContext:
    return gx.get_context(mode="ephemeral")


@pytest.fixture
def postgres_asset(context: AbstractDataContext) -> _SQLAsset:
    DATASOURCE_NAME = "postgres"
    ASSET_NAME = "ten trips"
    datasource = context.data_sources.add_postgres(
        DATASOURCE_NAME, connection_string=CONNECTION_STRING
    )
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


def _create_test_cases():
    """Create our test cases.

    With each flow, we want to see that we can validate an entire asset,
    as well as subsets of the asset, including sorting and using batch parameters.

    The positional arguments are:
    - An Expectation
    - The fixture name for the batch definition
    - The batch_parameters to pass in during validation
    """
    return [
        pytest.param(
            gxe.ExpectColumnDistinctValuesToEqualSet(column=COLUMN_NAME, value_set=ALL_VALUES),
            "postgres_whole_table_batch_definition",
            None,  # no batch parameters
            id="whole asset",
        ),
        pytest.param(
            gxe.ExpectColumnDistinctValuesToEqualSet(
                column=COLUMN_NAME, value_set=VALUES_FOR_MOST_RECENT_DATE
            ),
            "postgres_daily_batch_definition",
            None,  # no batch parameters
            id="ascending",
        ),
        pytest.param(
            gxe.ExpectColumnDistinctValuesToEqualSet(
                column=COLUMN_NAME, value_set=VALUES_WITH_NO_DATE
            ),
            "postgres_daily_batch_definition_descending",
            None,  # no batch parameters
            id="descending",
        ),
        pytest.param(
            gxe.ExpectColumnDistinctValuesToEqualSet(
                column=COLUMN_NAME, value_set=VALUES_ON_MY_FAVORITE_DAY
            ),
            "postgres_daily_batch_definition",
            MY_FAVORITE_DAY,
            id="batch params",
        ),
    ]


def test_get_batch_identifiers_list__simple(
    postgres_daily_batch_definition: BatchDefinition,
) -> None:
    batch_definition = postgres_daily_batch_definition
    batch_identifiers_list = batch_definition.get_batch_identifiers_list()

    assert len(batch_identifiers_list) == 10
    # just spot check the edges
    assert batch_identifiers_list[0] == {"year": None, "month": None, "day": None}
    assert batch_identifiers_list[-1] == {"year": 2001, "month": 1, "day": 1}


def test_get_batch_identifiers_list__respects_order(
    postgres_daily_batch_definition_descending: BatchDefinition,
) -> None:
    batch_definition = postgres_daily_batch_definition_descending
    batch_identifiers_list = batch_definition.get_batch_identifiers_list()

    assert len(batch_identifiers_list) == 10
    # just spot check the edges
    assert batch_identifiers_list[0] == {"year": 2001, "month": 1, "day": 1}
    assert batch_identifiers_list[-1] == {"year": None, "month": None, "day": None}


def test_get_batch_identifiers_list__respects_batch_params(
    postgres_daily_batch_definition: BatchDefinition,
) -> None:
    batch_definition = postgres_daily_batch_definition
    batch_identifiers_list = batch_definition.get_batch_identifiers_list(
        batch_parameters={"year": 2000}
    )

    assert len(batch_identifiers_list) == 6
    # just spot check the edges
    assert batch_identifiers_list[0] == {"year": 2000, "month": 1, "day": 1}
    assert batch_identifiers_list[-1] == {"year": 2000, "month": 6, "day": 1}


def test_get_batch_identifiers_list__no_batches(
    postgres_daily_batch_definition: BatchDefinition,
) -> None:
    batch_definition = postgres_daily_batch_definition
    batch_identifiers_list = batch_definition.get_batch_identifiers_list(
        batch_parameters={"year": 1900}
    )

    assert batch_identifiers_list == []


@pytest.mark.parametrize(
    ("expectation", "batch_definition_fixture_name", "batch_parameters"),
    _create_test_cases(),
)
def test_batch_validate_expectation(
    expectation: gxe.Expectation,
    batch_definition_fixture_name: str,
    batch_parameters: Optional[Dict],
    request: pytest.FixtureRequest,
) -> None:
    """Ensure Batch::validate(Epectation) works"""

    batch_definition: BatchDefinition = request.getfixturevalue(batch_definition_fixture_name)
    batch = batch_definition.get_batch(batch_parameters=batch_parameters)
    result = batch.validate(expectation)

    assert result.success


@pytest.mark.parametrize(
    ("expectation", "batch_definition_fixture_name", "batch_parameters"),
    _create_test_cases(),
)
def test_batch_validate_expectation_suite(
    expectation: gxe.Expectation,
    batch_definition_fixture_name: str,
    batch_parameters: Optional[Dict],
    request: pytest.FixtureRequest,
) -> None:
    """Ensure Batch::validate(EpectationSuite) works"""

    suite = ExpectationSuite("my suite", expectations=[expectation])
    batch_definition = request.getfixturevalue(batch_definition_fixture_name)
    batch = batch_definition.get_batch(batch_parameters=batch_parameters)
    result = batch.validate(suite)

    assert result.success


@pytest.mark.parametrize(
    ("expectation", "batch_definition_fixture_name", "batch_parameters"),
    _create_test_cases(),
)
def test_validation_definition_run(
    expectation: gxe.Expectation,
    batch_definition_fixture_name: str,
    batch_parameters: Optional[Dict],
    context: AbstractDataContext,
    request: pytest.FixtureRequest,
) -> None:
    """Ensure ValidationDefinition::run works"""

    batch_definition = request.getfixturevalue(batch_definition_fixture_name)
    suite = context.suites.add(ExpectationSuite("my_suite", expectations=[expectation]))
    validation_definition = context.validation_definitions.add(
        ValidationDefinition(name="whatever", data=batch_definition, suite=suite)
    )
    result = validation_definition.run(batch_parameters=batch_parameters)

    assert result.success


@pytest.mark.parametrize(
    ("expectation", "batch_definition_fixture_name", "batch_parameters"),
    _create_test_cases(),
)
def test_checkpoint_run(
    expectation: gxe.Expectation,
    batch_definition_fixture_name: str,
    batch_parameters: Optional[Dict],
    context: AbstractDataContext,
    request: pytest.FixtureRequest,
) -> None:
    """Ensure Checkpoint::run works"""

    batch_definition = request.getfixturevalue(batch_definition_fixture_name)
    suite = context.suites.add(ExpectationSuite("my_suite", expectations=[expectation]))
    validation_definition = context.validation_definitions.add(
        ValidationDefinition(name="whatever", data=batch_definition, suite=suite)
    )
    checkpoint = context.checkpoints.add(
        Checkpoint(name="whatever", validation_definitions=[validation_definition])
    )
    result = checkpoint.run(batch_parameters=batch_parameters)

    assert result.success
