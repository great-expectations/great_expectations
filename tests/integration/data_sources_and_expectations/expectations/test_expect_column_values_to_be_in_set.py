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

NUMBERS_COLUMN = "numbers"
STRINGS_COLUMN = "strings"
DATES_COLUMN = "dates"
NULLS_COLUMN = "nulls"

DATA = pd.DataFrame(
    {
        NUMBERS_COLUMN: [1, 2, 3],
        STRINGS_COLUMN: ["a", "b", "c"],
        DATES_COLUMN: [
            datetime(2024, 1, 1).date(),  # noqa: DTZ001 # FIXME CoP
            datetime(2024, 2, 1).date(),  # noqa: DTZ001 # FIXME CoP
            datetime(2024, 3, 1).date(),  # noqa: DTZ001 # FIXME CoP
        ],
        NULLS_COLUMN: [1, None, 3],
    },
    dtype="object",
)


@parameterize_batch_for_data_sources(data_source_configs=NON_SQL_DATA_SOURCES, data=DATA)
def test_success_complete_non_sql(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnValuesToBeInSet(column=NUMBERS_COLUMN, value_set=[1, 2, 3])
    result = batch_for_datasource.validate(expectation, result_format=ResultFormat.COMPLETE)
    assert result.success


@parameterize_batch_for_data_sources(data_source_configs=SQL_DATA_SOURCES, data=DATA)
def test_success_complete_sql(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnValuesToBeInSet(column=NUMBERS_COLUMN, value_set=[1, 2, 3])
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
            gxe.ExpectColumnValuesToBeInSet(column=NUMBERS_COLUMN, value_set=[1, 2, 3]),
            id="basic_number_set",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToBeInSet(column=STRINGS_COLUMN, value_set=["a", "b", "c"]),
            id="string_set",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToBeInSet(
                column=DATES_COLUMN,
                value_set=[
                    datetime(2024, 1, 1).date(),  # noqa: DTZ001 # FIXME CoP
                    datetime(2024, 2, 1).date(),  # noqa: DTZ001 # FIXME CoP
                    datetime(2024, 3, 1).date(),  # noqa: DTZ001 # FIXME CoP
                ],
            ),
            id="date_set",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToBeInSet(column=NUMBERS_COLUMN, value_set=[1, 2, 3, 4]),
            id="superset",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToBeInSet(column=NUMBERS_COLUMN, value_set=[1, 2], mostly=0.66),
            id="mostly_in_set",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToBeInSet(column=NULLS_COLUMN, value_set=[1, 3]),
            id="ignore_nulls",
        ),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_success(
    batch_for_datasource: Batch,
    expectation: gxe.ExpectColumnValuesToBeInSet,
) -> None:
    result = batch_for_datasource.validate(expectation)
    assert result.success


@pytest.mark.parametrize(
    "expectation",
    [
        pytest.param(
            gxe.ExpectColumnValuesToBeInSet(column=NUMBERS_COLUMN, value_set=[1, 2]),
            id="incomplete_number_set",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToBeInSet(column=NUMBERS_COLUMN, value_set=[1, 2], mostly=0.7),
            id="mostly_threshold_not_met",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToBeInSet(column=NUMBERS_COLUMN, value_set=[]),
            id="empty_set",
        ),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_failure(
    batch_for_datasource: Batch,
    expectation: gxe.ExpectColumnValuesToBeInSet,
) -> None:
    result = batch_for_datasource.validate(expectation)
    assert not result.success


@parameterize_batch_for_data_sources(
    data_source_configs=JUST_PANDAS_DATA_SOURCES, data=pd.DataFrame({NUMBERS_COLUMN: []})
)
def test_empty_data_empty_set(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnValuesToBeInSet(column=NUMBERS_COLUMN, value_set=[])
    result = batch_for_datasource.validate(expectation)
    assert result.success


@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_include_unexpected_rows_pandas(batch_for_datasource: Batch) -> None:
    """Test include_unexpected_rows for ExpectColumnValuesToBeInSet with pandas data sources."""
    expectation = gxe.ExpectColumnValuesToBeInSet(column=NUMBERS_COLUMN, value_set=[1, 2])
    result = batch_for_datasource.validate(
        expectation, result_format={"result_format": "BASIC", "include_unexpected_rows": True}
    )

    assert not result.success
    result_dict = result["result"]

    # Verify that unexpected_rows is present and contains the expected data
    assert "unexpected_rows" in result_dict
    assert result_dict["unexpected_rows"] is not None

    # For pandas data sources, unexpected_rows should be directly usable
    unexpected_rows_data = result_dict["unexpected_rows"]
    assert isinstance(unexpected_rows_data, pd.DataFrame)

    # Convert directly to DataFrame for pandas data sources
    unexpected_rows_df = unexpected_rows_data

    # Should contain 1 row where NUMBERS_COLUMN has value 3 (not in set [1, 2])
    assert len(unexpected_rows_df) == 1
    assert list(unexpected_rows_df.index) == [2]

    # The unexpected row should have value 3 in NUMBERS_COLUMN
    assert unexpected_rows_df.loc[2, NUMBERS_COLUMN] == 3

    # Other columns should have their original values from row with index 2
    assert unexpected_rows_df.loc[2, STRINGS_COLUMN] == "c"


@parameterize_batch_for_data_sources(
    data_source_configs=[PostgreSQLDatasourceTestConfig()], data=DATA
)
def test_include_unexpected_rows_sql(batch_for_datasource: Batch) -> None:
    """Test include_unexpected_rows for ExpectColumnValuesToBeInSet with SQL data sources."""
    expectation = gxe.ExpectColumnValuesToBeInSet(column=NUMBERS_COLUMN, value_set=[1, 2])
    result = batch_for_datasource.validate(
        expectation, result_format={"result_format": "BASIC", "include_unexpected_rows": True}
    )

    assert not result.success
    result_dict = result["result"]

    # Verify that unexpected_rows is present and contains the expected data
    assert "unexpected_rows" in result_dict
    assert result_dict["unexpected_rows"] is not None

    unexpected_rows_data = result_dict["unexpected_rows"]
    assert isinstance(unexpected_rows_data, list)

    # Should contain 1 row where NUMBERS_COLUMN has value 3 (not in set [1, 2])
    assert len(unexpected_rows_data) == 1

    unexpected_row = unexpected_rows_data[0]
    assert isinstance(unexpected_row, dict)
    assert unexpected_row[NUMBERS_COLUMN] == 3
    assert unexpected_row[STRINGS_COLUMN] == "c"
