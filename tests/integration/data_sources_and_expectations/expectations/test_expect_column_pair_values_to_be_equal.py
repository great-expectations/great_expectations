from datetime import datetime
from unittest.mock import ANY

import pandas as pd
import pytest

import great_expectations.expectations as gxe
from great_expectations.core.result_format import ResultFormat
from great_expectations.datasource.fluent.interfaces import Batch
from tests.integration.conftest import parameterize_batch_for_data_sources
from tests.integration.data_sources_and_expectations.data_source_lists import (
    JUST_PANDAS_DATA_SOURCES,
    NON_SQL_DATA_SOURCES,
)
from tests.integration.test_utils.data_source_config import (
    SQL_DATA_SOURCES,
    PostgreSQLDatasourceTestConfig,
)

EQUAL_STRINGS_A = "equal_strings_a"
EQUAL_STRINGS_B = "equal_strings_b"
UNEQUAL_STRINGS = "unequal_strings"
EQUAL_DATES_A = "equal_dates_a"
EQUAL_DATES_B = "equal_dates_b"
UNEQUAL_DATES = "unequal_dates"
ALL_EQUAL_NUMS_A = "all_equal_nums_a"
ALL_EQUAL_NUMS_B = "all_equal_nums_b"
SOME_EQUAL_NUMS = "some_equal_nums"
NULLS_A = "nulls_a"
NULLS_B = "nulls_b"
ALL_NULLS = "all_nulls"

DATA = pd.DataFrame(
    {
        EQUAL_STRINGS_A: ["foo", "bar", "baz"],
        EQUAL_STRINGS_B: ["foo", "bar", "baz"],
        UNEQUAL_STRINGS: ["foo", "bar", "wat"],
        EQUAL_DATES_A: [
            datetime(2024, 1, 1).date(),  # noqa: DTZ001 # FIXME CoP
            datetime(2024, 2, 1).date(),  # noqa: DTZ001 # FIXME CoP
            datetime(2024, 3, 1).date(),  # noqa: DTZ001 # FIXME CoP
        ],
        EQUAL_DATES_B: [
            datetime(2024, 1, 1).date(),  # noqa: DTZ001 # FIXME CoP
            datetime(2024, 2, 1).date(),  # noqa: DTZ001 # FIXME CoP
            datetime(2024, 3, 1).date(),  # noqa: DTZ001 # FIXME CoP
        ],
        UNEQUAL_DATES: [
            datetime(2024, 1, 1).date(),  # noqa: DTZ001 # FIXME CoP
            datetime(2024, 2, 1).date(),  # noqa: DTZ001 # FIXME CoP
            datetime(2024, 4, 1).date(),  # noqa: DTZ001 # FIXME CoP
        ],
        ALL_EQUAL_NUMS_A: [1, 2, 3],
        ALL_EQUAL_NUMS_B: [1, 2, 3],
        SOME_EQUAL_NUMS: [1, 2, 4],
        NULLS_A: [1, None, 3],
        NULLS_B: [None, 2, 3],
    },
    dtype="object",
)


@parameterize_batch_for_data_sources(data_source_configs=NON_SQL_DATA_SOURCES, data=DATA)
def test_success_complete_non_sql(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnPairValuesToBeEqual(
        column_A=EQUAL_STRINGS_A,
        column_B=EQUAL_STRINGS_B,
    )
    result = batch_for_datasource.validate(expectation, result_format=ResultFormat.COMPLETE)
    assert result.success


@parameterize_batch_for_data_sources(data_source_configs=SQL_DATA_SOURCES, data=DATA)
def test_success_complete_sql(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnPairValuesToBeEqual(
        column_A=EQUAL_STRINGS_A,
        column_B=EQUAL_STRINGS_B,
    )
    result = batch_for_datasource.validate(expectation, result_format=ResultFormat.COMPLETE)
    assert result.success
    assert result.to_json_dict()["result"] == {
        "element_count": 3,
        "unexpected_count": 0,
        "unexpected_percent": 0.0,
        "partial_unexpected_list": [],
        "missing_count": 0,
        "missing_percent": 0.0,
        "unexpected_percent_total": 0.0,
        "unexpected_percent_nonmissing": 0.0,
        "partial_unexpected_counts": [],
        "unexpected_list": [],
        "unexpected_index_query": ANY,
    }


@pytest.mark.parametrize(
    "expectation",
    [
        pytest.param(
            gxe.ExpectColumnPairValuesToBeEqual(
                column_A=EQUAL_STRINGS_A,
                column_B=EQUAL_STRINGS_B,
            ),
            id="equal_strings",
        ),
        pytest.param(
            gxe.ExpectColumnPairValuesToBeEqual(
                column_A=EQUAL_DATES_A,
                column_B=EQUAL_DATES_B,
            ),
            id="equal_dates",
        ),
        pytest.param(
            gxe.ExpectColumnPairValuesToBeEqual(
                column_A=ALL_EQUAL_NUMS_A,
                column_B=ALL_EQUAL_NUMS_B,
            ),
            id="all_equal_numbers",
        ),
        pytest.param(
            gxe.ExpectColumnPairValuesToBeEqual(
                column_A=ALL_EQUAL_NUMS_A,
                column_B=SOME_EQUAL_NUMS,
                mostly=0.6,
            ),
            id="some_equal_numbers",
        ),
        pytest.param(
            gxe.ExpectColumnPairValuesToBeEqual(
                column_A=NULLS_A,
                column_B=NULLS_B,
                ignore_row_if="either_value_is_missing",
            ),
            id="ignore_nulls",
        ),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_success(
    batch_for_datasource: Batch,
    expectation: gxe.ExpectColumnPairValuesToBeEqual,
) -> None:
    result = batch_for_datasource.validate(expectation)
    assert result.success


@pytest.mark.parametrize(
    "expectation",
    [
        pytest.param(
            gxe.ExpectColumnPairValuesToBeEqual(
                column_A=EQUAL_STRINGS_A,
                column_B=UNEQUAL_STRINGS,
            ),
            id="unequal_strings",
        ),
        pytest.param(
            gxe.ExpectColumnPairValuesToBeEqual(
                column_A=EQUAL_DATES_A,
                column_B=UNEQUAL_DATES,
            ),
            id="unequal_dates",
        ),
        pytest.param(
            gxe.ExpectColumnPairValuesToBeEqual(
                column_A=ALL_EQUAL_NUMS_A,
                column_B=SOME_EQUAL_NUMS,
            ),
            id="partially_equal_numbers",
        ),
        pytest.param(
            gxe.ExpectColumnPairValuesToBeEqual(
                column_A=NULLS_A,
                column_B=NULLS_A,
                ignore_row_if="neither",
            ),
            id="matching_nulls",
        ),
        pytest.param(
            gxe.ExpectColumnPairValuesToBeEqual(
                column_A=NULLS_A,
                column_B=NULLS_B,
            ),
            id="neither",
        ),
        pytest.param(
            gxe.ExpectColumnPairValuesToBeEqual(
                column_A=ALL_EQUAL_NUMS_A,
                column_B=SOME_EQUAL_NUMS,
                mostly=0.7,
            ),
            id="some_equal_numbers",
        ),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_failure(
    batch_for_datasource: Batch,
    expectation: gxe.ExpectColumnPairValuesToBeEqual,
) -> None:
    result = batch_for_datasource.validate(expectation)
    assert not result.success


@pytest.mark.parametrize(
    "suite_param_value,expected_result",
    [
        pytest.param("both_values_are_missing", True, id="success"),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_success_with_suite_param_ignore_row_if_(
    batch_for_datasource: Batch, suite_param_value: str, expected_result: bool
) -> None:
    suite_param_key = "test_expect_column_pair_values_to_be_equal"
    expectation = gxe.ExpectColumnPairValuesToBeEqual(
        column_A=EQUAL_STRINGS_A,
        column_B=EQUAL_STRINGS_B,
        ignore_row_if={"$PARAMETER": suite_param_key},
        result_format=ResultFormat.SUMMARY,
    )
    result = batch_for_datasource.validate(
        expectation, expectation_parameters={suite_param_key: suite_param_value}
    )
    assert result.success == expected_result


@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_include_unexpected_rows(batch_for_datasource: Batch) -> None:
    """Test that include_unexpected_rows works correctly for ExpectColumnPairValuesToBeEqual."""
    expectation = gxe.ExpectColumnPairValuesToBeEqual(
        column_A=EQUAL_STRINGS_A, column_B=UNEQUAL_STRINGS
    )
    result = batch_for_datasource.validate(
        expectation, result_format={"result_format": "BASIC", "include_unexpected_rows": True}
    )

    assert not result.success
    result_dict = result["result"]

    # Verify that unexpected_rows is present and contains the expected data
    assert "unexpected_rows" in result_dict
    assert result_dict["unexpected_rows"] is not None

    # Convert to DataFrame for easier comparison
    unexpected_rows_data = result_dict["unexpected_rows"]
    assert isinstance(unexpected_rows_data, pd.DataFrame)
    unexpected_rows_df = unexpected_rows_data

    # Should contain 1 row where column_A != column_B
    assert len(unexpected_rows_df) == 1

    # The unexpected row should have different values in column_A and column_B
    # The row is at index 2 (where "baz" != "wat")
    assert list(unexpected_rows_df.index) == [2]
    assert unexpected_rows_df.loc[2, EQUAL_STRINGS_A] == "baz"
    assert unexpected_rows_df.loc[2, UNEQUAL_STRINGS] == "wat"
    assert unexpected_rows_df.loc[2, EQUAL_STRINGS_A] != unexpected_rows_df.loc[2, UNEQUAL_STRINGS]


@parameterize_batch_for_data_sources(
    data_source_configs=[PostgreSQLDatasourceTestConfig()], data=DATA
)
def test_include_unexpected_rows_sql(batch_for_datasource: Batch) -> None:
    """Test include_unexpected_rows for ExpectColumnPairValuesToBeEqual with SQL data sources."""
    expectation = gxe.ExpectColumnPairValuesToBeEqual(
        column_A=EQUAL_STRINGS_A, column_B=UNEQUAL_STRINGS
    )
    result = batch_for_datasource.validate(
        expectation, result_format={"result_format": "BASIC", "include_unexpected_rows": True}
    )

    assert not result.success
    result_dict = result["result"]

    # Verify that unexpected_rows is present and contains the expected data
    assert "unexpected_rows" in result_dict
    assert result_dict["unexpected_rows"] is not None

    # For SQL data sources, unexpected_rows should be a list
    unexpected_rows_data = result_dict["unexpected_rows"]
    assert isinstance(unexpected_rows_data, list)

    # Should contain 1 row where column_A != column_B
    assert len(unexpected_rows_data) == 1

    # Verify that the unexpected row is a dictionary
    unexpected_row = unexpected_rows_data[0]
    assert isinstance(unexpected_row, dict)

    # Verify that the unexpected row contains the expected columns and values
    assert unexpected_row[EQUAL_STRINGS_A] == "baz"
    assert unexpected_row[UNEQUAL_STRINGS] == "wat"
