"""Tests to ensure core validation flows work with filesystem assets

NOTE: assertions here take the form of asserting that expectations pass
based on knowledge of the data in the test set.

Suites also assert that we only get the expected number of rows (they should all have 10)
"""

import re
from typing import Dict, Optional

import pytest

import great_expectations as gx
import great_expectations.expectations as gxe
from great_expectations.checkpoint.checkpoint import Checkpoint
from great_expectations.core.batch_definition import BatchDefinition
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.validation_definition import ValidationDefinition
from great_expectations.data_context.data_context.abstract_data_context import AbstractDataContext
from great_expectations.datasource.fluent.pandas_file_path_datasource import (
    CSVAsset as PandasCSVAsset,
)

pytestmark = pytest.mark.filesystem


DATASOURCE_NAME = "spark file system"
ASSET_NAME = "first ten trips in each file"

# constants for what we know about the test data
BATCHING_REGEX = r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv"
COLUMN_NAME = "passenger_count"
ALL_VALUES = list(range(1, 11))
VALUES_FOR_OLDEST_DATE = [1, 2, 3, 6]
VALUES_FOR_MOST_RECENT_DATE = [1, 2, 3, 4]
MY_FAVORITE_MONTH = {"year": "2020", "month": "09"}
VALUES_ON_MY_FAVORITE_MONTH = [0, 1]


@pytest.fixture
def context() -> AbstractDataContext:
    return gx.get_context(mode="ephemeral")


EXPECT_10_ROWS = gxe.ExpectTableRowCountToEqual(value=10)


@pytest.fixture
def file_system_asset(context: AbstractDataContext) -> PandasCSVAsset:
    datasource = context.sources.add_pandas_filesystem(
        DATASOURCE_NAME,
        base_directory="tests/test_sets/taxi_yellow_tripdata_samples/first_ten_trips_in_each_file",  # type: ignore [arg-type]
    )
    data_asset = datasource.add_csv_asset(name=ASSET_NAME, batching_regex=BATCHING_REGEX)

    return data_asset


@pytest.fixture
def filesystem_whole_table_batch_definition(file_system_asset: PandasCSVAsset) -> BatchDefinition:
    return file_system_asset.add_batch_definition("no batching regex")


@pytest.fixture
def filesystem_monthly_batch_definition(file_system_asset: PandasCSVAsset) -> BatchDefinition:
    return file_system_asset.add_batch_definition_monthly(
        "monthly",
        re.compile(BATCHING_REGEX),
    )


@pytest.fixture
def filesystem_monthly_batch_definition_descending(
    file_system_asset: PandasCSVAsset,
) -> BatchDefinition:
    return file_system_asset.add_batch_definition_monthly(
        "monthly",
        re.compile(BATCHING_REGEX),
        sort_ascending=False,
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
            gxe.ExpectColumnDistinctValuesToEqualSet(
                column=COLUMN_NAME, value_set=VALUES_FOR_MOST_RECENT_DATE
            ),
            "filesystem_whole_table_batch_definition",
            None,  # no batch parameters
            id="pandas: no batching regex - takes the last file",
        ),
        pytest.param(
            gxe.ExpectColumnDistinctValuesToEqualSet(
                column=COLUMN_NAME, value_set=VALUES_FOR_MOST_RECENT_DATE
            ),
            "filesystem_monthly_batch_definition",
            None,  # no batch parameters
            id="pandas: ascending",
        ),
        pytest.param(
            gxe.ExpectColumnDistinctValuesToEqualSet(
                column=COLUMN_NAME, value_set=VALUES_FOR_OLDEST_DATE
            ),
            "filesystem_monthly_batch_definition_descending",
            None,  # no batch parameters
            id="pandas: descending",
        ),
        pytest.param(
            gxe.ExpectColumnDistinctValuesToEqualSet(
                column=COLUMN_NAME, value_set=VALUES_ON_MY_FAVORITE_MONTH
            ),
            "filesystem_monthly_batch_definition",
            MY_FAVORITE_MONTH,
            id="pandas: batch params",
            marks=[pytest.mark.xfail(reason="Fix in V1-299", strict=True)],
        ),
    ]


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
    batch_definition = request.getfixturevalue(batch_definition_fixture_name)
    batch = batch_definition.get_batch(batch_parameters=batch_parameters)

    result = batch.validate(expectation)
    row_count_result = batch.validate(gxe.ExpectTableRowCountToEqual(value=10))

    assert row_count_result.success
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

    suite = ExpectationSuite("my suite", expectations=[expectation, EXPECT_10_ROWS])
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
    request: pytest.FixtureRequest,
) -> None:
    """Ensure ValidationDefinition::run works"""

    batch_definition = request.getfixturevalue(batch_definition_fixture_name)
    suite = ExpectationSuite("my suite", expectations=[expectation, EXPECT_10_ROWS])
    validation_definition = ValidationDefinition(
        name="whatever", data=batch_definition, suite=suite
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
    suite = ExpectationSuite("my suite", expectations=[expectation, EXPECT_10_ROWS])
    validation_definition = ValidationDefinition(
        name="whatever", data=batch_definition, suite=suite
    )
    checkpoint = context.checkpoints.add(
        Checkpoint(name="whatever", validation_definitions=[validation_definition])
    )
    result = checkpoint.run(batch_parameters=batch_parameters)

    assert result.success
