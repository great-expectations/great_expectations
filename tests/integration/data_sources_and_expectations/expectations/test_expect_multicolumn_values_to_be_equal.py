import pandas as pd

import great_expectations.expectations as gxe
from great_expectations.datasource.fluent.interfaces import Batch
from tests.integration.conftest import parameterize_batch_for_data_sources
from tests.integration.data_sources_and_expectations.data_source_lists import (
    JUST_PANDAS_DATA_SOURCES,
)
from tests.integration.test_utils.data_source_config import (
    ALL_DATA_SOURCES,
)

COL_A = "COL_A"
COL_B = "COL_B"
COL_C = "COL_C"

DATA = pd.DataFrame(
    {
        COL_A: ["a", "b", "c"],
        COL_B: ["a", "b", "c"],
        COL_C: ["a", "b", "c"],
    }
)

# Row 1 mismatches on two non-null values, so it exercises the value comparison itself
# rather than a null-vs-non-null guard.
MISMATCHED_DATA = pd.DataFrame(
    {
        COL_A: ["a", "b", "c"],
        COL_B: ["a", "different", "c"],
        COL_C: ["a", "b", "c"],
    }
)

# Row 0 is entirely null, row 1 mixes a null with a value, row 2 matches.
NULL_DATA = pd.DataFrame(
    {
        COL_A: [None, None, "same"],
        COL_B: [None, "value", "same"],
        COL_C: [None, None, "same"],
    }
)


@parameterize_batch_for_data_sources(data_source_configs=ALL_DATA_SOURCES, data=DATA)
def test_golden_path(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectMulticolumnValuesToBeEqual(column_list=[COL_A, COL_B, COL_C])

    result = batch_for_datasource.validate(expectation)

    assert result.success


@parameterize_batch_for_data_sources(data_source_configs=ALL_DATA_SOURCES, data=MISMATCHED_DATA)
def test_mostly_allows_a_partial_match(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectMulticolumnValuesToBeEqual(
        column_list=[COL_A, COL_B, COL_C], mostly=0.6
    )

    result = batch_for_datasource.validate(expectation)

    assert result.success


@parameterize_batch_for_data_sources(data_source_configs=ALL_DATA_SOURCES, data=MISMATCHED_DATA)
def test_fails_when_a_row_contains_different_values(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectMulticolumnValuesToBeEqual(column_list=[COL_A, COL_B, COL_C])

    result = batch_for_datasource.validate(expectation)

    assert not result.success
    assert result.result["unexpected_count"] == 1


@parameterize_batch_for_data_sources(data_source_configs=ALL_DATA_SOURCES, data=NULL_DATA)
def test_nulls_are_compared_as_values(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectMulticolumnValuesToBeEqual(
        column_list=[COL_A, COL_B, COL_C], ignore_row_if="never"
    )

    result = batch_for_datasource.validate(expectation)

    # The all-null row is equal; only the row mixing a null with a value is not.
    assert not result.success
    assert result.result["unexpected_count"] == 1


@parameterize_batch_for_data_sources(data_source_configs=ALL_DATA_SOURCES, data=NULL_DATA)
def test_all_null_rows_are_ignored_by_default(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectMulticolumnValuesToBeEqual(
        column_list=[COL_A, COL_B, COL_C], mostly=0.6
    )

    result = batch_for_datasource.validate(expectation)

    # The default ignore_row_if drops the all-null row, leaving two scored rows of which
    # one matches. Scoring the all-null row as a match instead would give two of three
    # and clear the threshold.
    assert not result.success
    assert result.result["missing_count"] == 1


@parameterize_batch_for_data_sources(
    data_source_configs=JUST_PANDAS_DATA_SOURCES,
    data=pd.DataFrame(
        {
            COL_A: pd.Series([1.0, None], dtype="float64"),
            COL_B: pd.Series(["1", None], dtype="object"),
        }
    ),
)
def test_missing_value_sentinels_are_the_same_null(batch_for_datasource: Batch) -> None:
    """A float NaN and an object None are one missing value, not two distinct ones.

    Pandas-only because a numeric column compared against a text column is not a
    comparison strict SQL dialects accept.
    """
    expectation = gxe.ExpectMulticolumnValuesToBeEqual(
        column_list=[COL_A, COL_B], ignore_row_if="never"
    )

    result = batch_for_datasource.validate(expectation)

    # Row 0 (1.0 against "1") is unequal; row 1 is a row of nulls, so it is equal.
    assert not result.success
    assert result.result["unexpected_count"] == 1
