import re
from typing import Dict, Optional

import pytest

import great_expectations as gx
import great_expectations.expectations as gxe
from great_expectations.core.batch_definition import BatchDefinition
from great_expectations.core.partitioners import PartitionerYearAndMonth
from great_expectations.data_context.data_context.abstract_data_context import AbstractDataContext
from great_expectations.datasource.fluent.pandas_file_path_datasource import CSVAsset

DATASOURCE_NAME = "spark file system"
ASSET_NAME = "first ten trips in each file"

# constants for what we know about the test data
BATCHING_REGEX = r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv"
COLUMN_NAME = "passenger_count"
ALL_VALUES = list(range(1, 11))
VALUES_FOR_OLDEST_DATE = [1, 2, 3, 6]
VALUES_FOR_MOST_RECENT_DATE = [1, 2, 3, 4]
MY_FAVORITE_DAY = {"year": "2020", "month": "09"}
VALUES_ON_MY_FAVORITE_MONTH = [0, 1]


@pytest.fixture
def context() -> AbstractDataContext:
    return gx.get_context(mode="ephemeral")


@pytest.fixture
def file_system_asset(context: AbstractDataContext) -> CSVAsset:
    datasource = context.sources.add_pandas_filesystem(
        DATASOURCE_NAME,
        base_directory="tests/test_sets/taxi_yellow_tripdata_samples/first_ten_trips_in_each_file",  # type: ignore [arg-type]
    )
    data_asset = datasource.add_csv_asset(name=ASSET_NAME, batching_regex=BATCHING_REGEX)

    return data_asset


@pytest.fixture
def filesystem_whole_table_batch_definition(file_system_asset: CSVAsset) -> BatchDefinition:
    return file_system_asset.add_batch_definition("no batching regex")


@pytest.fixture
def filesystem_monthly_batch_definition(file_system_asset: CSVAsset) -> BatchDefinition:
    return file_system_asset.add_batch_definition(
        name="monthly",
        partitioner=PartitionerYearAndMonth(column_name="passenger_count"),
        batching_regex=re.compile(BATCHING_REGEX),
    )


@pytest.fixture
def filesystem_monthly_batch_definition_descending(file_system_asset: CSVAsset) -> BatchDefinition:
    return file_system_asset.add_batch_definition(
        name="monthly",
        partitioner=PartitionerYearAndMonth(column_name="passenger_count", sort_ascending=False),
        batching_regex=re.compile(BATCHING_REGEX),
    )


def _create_test_cases():
    """Create our test cases.
    With each flow, we want to see that we can validate an entire asset,
    as well as subsets of the asset, including sorting and using batch parameters.
    """
    return [
        pytest.param(
            gxe.ExpectColumnDistinctValuesToEqualSet(
                column=COLUMN_NAME, value_set=VALUES_FOR_MOST_RECENT_DATE
            ),
            "filesystem_whole_table_batch_definition",
            None,
            id="no batching regex - takes the last file",
        ),
        pytest.param(
            gxe.ExpectColumnDistinctValuesToEqualSet(
                column=COLUMN_NAME, value_set=VALUES_FOR_MOST_RECENT_DATE
            ),
            "filesystem_monthly_batch_definition",
            None,
            id="ascending",
        ),
        pytest.param(
            gxe.ExpectColumnDistinctValuesToEqualSet(
                column=COLUMN_NAME, value_set=VALUES_FOR_OLDEST_DATE
            ),
            "filesystem_monthly_batch_definition_descending",
            None,
            id="descending",
        ),
        pytest.param(
            gxe.ExpectColumnDistinctValuesToEqualSet(
                column=COLUMN_NAME, value_set=VALUES_ON_MY_FAVORITE_MONTH
            ),
            "filesystem_monthly_batch_definition",
            MY_FAVORITE_DAY,
            id="batch params",
        ),
    ]


@pytest.mark.parametrize(
    ("expectation", "batch_definition_fixture_name", "batch_parameters"),
    _create_test_cases(),
)
@pytest.mark.filesystem
def test_batch_validate_expectation(
    expectation: gxe.Expectation,
    batch_definition_fixture_name: str,
    batch_parameters: Optional[Dict],
    request: pytest.FixtureRequest,
) -> None:
    """Ensure Batch::validate(Epectation) works

    Note: There's also a bonus assertion here to make it clear we are not concatenating files.
    """
    batch_definition = request.getfixturevalue(batch_definition_fixture_name)
    batch = batch_definition.get_batch(batch_parameters=batch_parameters)

    result = batch.validate(expectation)
    row_count_result = batch.validate(gxe.ExpectTableRowCountToEqual(value=10))

    assert row_count_result.success
    assert result.success
