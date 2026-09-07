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

DIFFERENT_COL = "some_are_different"
SAME_COL = "all_the_same"

DATA = pd.DataFrame(
    {
        SAME_COL: ["FOO", "BAR", "BAZ", None],
        DIFFERENT_COL: ["FOOD", "BAR", "BAZ", None],
    }
)


@parameterize_batch_for_data_sources(data_source_configs=SQL_DATA_SOURCES, data=DATA)
def test_success_complete__sql(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnValueLengthsToEqual(column=SAME_COL, value=3)
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


@parameterize_batch_for_data_sources(data_source_configs=NON_SQL_DATA_SOURCES, data=DATA)
def test_success_complete__non_sql(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnValueLengthsToEqual(column=SAME_COL, value=3)
    result = batch_for_datasource.validate(expectation, result_format=ResultFormat.COMPLETE)
    assert result.success


@pytest.mark.parametrize(
    "expectation",
    [
        pytest.param(
            gxe.ExpectColumnValueLengthsToEqual(column=SAME_COL, value=3),
            id="exact_match",
        ),
        pytest.param(
            gxe.ExpectColumnValueLengthsToEqual(column=SAME_COL, value=3, mostly=0.75),
            id="with_mostly",
        ),
        pytest.param(
            gxe.ExpectColumnValueLengthsToEqual(column=DIFFERENT_COL, value=3, mostly=0.1),
            id="different_lengths_with_mostly",
        ),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_success(
    batch_for_datasource: Batch, expectation: gxe.ExpectColumnValueLengthsToEqual
) -> None:
    result = batch_for_datasource.validate(expectation)
    assert result.success


@pytest.mark.parametrize(
    "expectation",
    [
        pytest.param(
            gxe.ExpectColumnValueLengthsToEqual(column=DIFFERENT_COL, value=3),
            id="wrong_length",
        ),
        pytest.param(
            gxe.ExpectColumnValueLengthsToEqual(column=DIFFERENT_COL, value=3, mostly=0.9),
            id="mostly_too_high",
        ),
        pytest.param(
            gxe.ExpectColumnValueLengthsToEqual(column=SAME_COL, value=4, mostly=0.1),
            id="no_matches",
        ),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_failure(
    batch_for_datasource: Batch, expectation: gxe.ExpectColumnValueLengthsToEqual
) -> None:
    result = batch_for_datasource.validate(expectation)
    assert not result.success


@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_include_unexpected_rows_pandas(batch_for_datasource: Batch) -> None:
    """Test include_unexpected_rows for ExpectColumnValueLengthsToEqual with pandas data sources."""
    expectation = gxe.ExpectColumnValueLengthsToEqual(column=DIFFERENT_COL, value=3)
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

    # Should contain 1 row where DIFFERENT_COL doesn't have length 3 ("FOOD" has length 4)
    assert len(unexpected_rows_df) == 1
    assert list(unexpected_rows_df.index) == [0]

    # The unexpected row should have value "FOOD" in DIFFERENT_COL
    assert unexpected_rows_df.loc[0, DIFFERENT_COL] == "FOOD"

    # Other columns should have their original values from row with index 0
    assert unexpected_rows_df.loc[0, SAME_COL] == "FOO"


@parameterize_batch_for_data_sources(
    data_source_configs=[PostgreSQLDatasourceTestConfig()], data=DATA
)
def test_include_unexpected_rows_sql(batch_for_datasource: Batch) -> None:
    """Test include_unexpected_rows for ExpectColumnValueLengthsToEqual with SQL data sources."""
    expectation = gxe.ExpectColumnValueLengthsToEqual(column=DIFFERENT_COL, value=3)
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

    # Should contain 1 row where DIFFERENT_COL doesn't have length 3 ("FOOD" has length 4)
    assert len(unexpected_rows_data) == 1

    # Check that "FOOD" and "FOO" appear in the unexpected rows data
    unexpected_rows_str = str(unexpected_rows_data)
    assert "FOOD" in unexpected_rows_str
    assert "FOO" in unexpected_rows_str
