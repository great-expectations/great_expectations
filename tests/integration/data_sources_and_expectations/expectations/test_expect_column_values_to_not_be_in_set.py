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

DATA = pd.DataFrame(
    {
        NUMBERS_COLUMN: [1, 2, 3, None],
        STRINGS_COLUMN: ["a", "b", "c", None],
        DATES_COLUMN: [
            datetime(2024, 1, 1).date(),  # noqa: DTZ001 # FIXME CoP
            datetime(2024, 2, 1).date(),  # noqa: DTZ001 # FIXME CoP
            datetime(2024, 3, 1).date(),  # noqa: DTZ001 # FIXME CoP
            None,
        ],
    },
    dtype="object",
)


@parameterize_batch_for_data_sources(data_source_configs=NON_SQL_DATA_SOURCES, data=DATA)
def test_success_complete_non_sql(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnValuesToNotBeInSet(column=NUMBERS_COLUMN, value_set=[4, 5])
    result = batch_for_datasource.validate(expectation, result_format=ResultFormat.COMPLETE)
    assert result.success


@parameterize_batch_for_data_sources(data_source_configs=SQL_DATA_SOURCES, data=DATA)
def test_success_complete_sql(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnValuesToNotBeInSet(column=NUMBERS_COLUMN, value_set=[4, 5])
    result = batch_for_datasource.validate(expectation, result_format=ResultFormat.COMPLETE)
    assert result.success
    assert result.to_json_dict()["result"] == {
        "element_count": 4,
        "unexpected_count": 0,
        "unexpected_percent": 0.0,
        "partial_unexpected_list": [],
        "missing_count": 1,
        "missing_percent": 25.0,
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
            gxe.ExpectColumnValuesToNotBeInSet(column=NUMBERS_COLUMN, value_set=[4, 5]),
            id="no_overlap",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToNotBeInSet(column=STRINGS_COLUMN, value_set=["d", "e"]),
            id="strings",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToNotBeInSet(
                column=DATES_COLUMN,
                value_set=[
                    datetime(2024, 4, 1).date(),  # noqa: DTZ001 # FIXME CoP
                    datetime(2024, 5, 1).date(),  # noqa: DTZ001 # FIXME CoP
                ],
            ),
            id="dates",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToNotBeInSet(
                column=NUMBERS_COLUMN, value_set=[3, 4, 5], mostly=0.6
            ),
            id="mostly",
        ),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_success(
    batch_for_datasource: Batch,
    expectation: gxe.ExpectColumnValuesToNotBeInSet,
) -> None:
    result = batch_for_datasource.validate(expectation)
    assert result.success


@parameterize_batch_for_data_sources(
    data_source_configs=JUST_PANDAS_DATA_SOURCES, data=pd.DataFrame({NUMBERS_COLUMN: []})
)
def test_empty_data(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnValuesToNotBeInSet(column=NUMBERS_COLUMN, value_set=[])
    result = batch_for_datasource.validate(expectation)
    assert result.success


@pytest.mark.parametrize(
    "expectation",
    [
        pytest.param(
            gxe.ExpectColumnValuesToNotBeInSet(column=NUMBERS_COLUMN, value_set=[1, 2, 3]),
            id="exact_match",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToNotBeInSet(column=STRINGS_COLUMN, value_set=["a", "b"]),
            id="strings",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToNotBeInSet(
                column=DATES_COLUMN,
                value_set=[
                    datetime(2024, 1, 1).date(),  # noqa: DTZ001 # FIXME CoP
                    datetime(2024, 2, 1).date(),  # noqa: DTZ001 # FIXME CoP
                ],
            ),
            id="dates",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToNotBeInSet(column=NUMBERS_COLUMN, value_set=[3, 4, 5]),
            id="some_overlap",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToNotBeInSet(
                column=NUMBERS_COLUMN, value_set=[3, 4, 5], mostly=0.7
            ),
            id="mostly_not_met",
        ),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_failure(
    batch_for_datasource: Batch,
    expectation: gxe.ExpectColumnValuesToNotBeInSet,
) -> None:
    result = batch_for_datasource.validate(expectation)
    assert not result.success


@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_include_unexpected_rows_pandas(batch_for_datasource: Batch) -> None:
    """Test include_unexpected_rows for ExpectColumnValuesToNotBeInSet with pandas data sources."""
    expectation = gxe.ExpectColumnValuesToNotBeInSet(column=NUMBERS_COLUMN, value_set=[3, 4, 5])
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

    # Should contain 1 row where NUMBERS_COLUMN has value 3 (in the forbidden set)
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
    """Test include_unexpected_rows for ExpectColumnValuesToNotBeInSet with SQL data sources."""
    expectation = gxe.ExpectColumnValuesToNotBeInSet(column=NUMBERS_COLUMN, value_set=[3, 4, 5])
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

    # Should contain 1 row where NUMBERS_COLUMN has value 3 (in the forbidden set)
    assert len(unexpected_rows_data) == 1

    # Check that the forbidden value 3 and corresponding string "c" appear
    unexpected_rows_str = str(unexpected_rows_data)
    assert "3" in unexpected_rows_str
    assert "c" in unexpected_rows_str
