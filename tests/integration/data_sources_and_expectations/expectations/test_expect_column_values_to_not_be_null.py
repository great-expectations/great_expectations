from unittest.mock import ANY

import pandas as pd
import pytest

import great_expectations.expectations as gxe
from great_expectations.core.result_format import ResultFormat
from great_expectations.datasource.fluent.interfaces import Batch
from tests.integration.conftest import parameterize_batch_for_data_sources
from tests.integration.data_sources_and_expectations.data_source_lists import (
    JUST_PANDAS_DATA_SOURCES,
)
from tests.integration.test_utils.data_source_config import (
    SQL_DATA_SOURCES,
    PandasDataFrameDatasourceTestConfig,
    PandasFilesystemCsvDatasourceTestConfig,
    PostgreSQLDatasourceTestConfig,
    SparkFilesystemCsvDatasourceTestConfig,
)

NON_NULL_COLUMN = "none_nulls"
ALL_NULL_COLUMN = "oops_all_nulls"
MOSTLY_NULL_COLUMN = "mostly_nulls"

DATA = pd.DataFrame(
    {
        NON_NULL_COLUMN: [1, 2, 3, 4, 5],
        MOSTLY_NULL_COLUMN: pd.Series([1, None, None, None, None], dtype="object"),
        ALL_NULL_COLUMN: pd.Series([None, None, None, None, None], dtype="object"),
    },
)

IDX_DATA = pd.DataFrame(
    {
        "customer_id": [1, 2, 3, 4, 5],
        "email": ["a@x.com", "b@x.com", "c@x.com", "d@x.com", None],
    }
)

try:
    from great_expectations.compatibility.pyspark import types as PYSPARK_TYPES

    SPARK_COLUMN_TYPES = {
        NON_NULL_COLUMN: PYSPARK_TYPES.IntegerType,
        MOSTLY_NULL_COLUMN: PYSPARK_TYPES.IntegerType,
        ALL_NULL_COLUMN: PYSPARK_TYPES.IntegerType,
    }
except ModuleNotFoundError:
    SPARK_COLUMN_TYPES = {}


@parameterize_batch_for_data_sources(
    data_source_configs=[PandasDataFrameDatasourceTestConfig()], data=DATA
)
def test_failure_pandas_dataframe(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnValuesToNotBeNull(column=MOSTLY_NULL_COLUMN)
    result = batch_for_datasource.validate(expectation, result_format=ResultFormat.COMPLETE)
    assert not result.success
    assert result.to_json_dict()["result"] == {
        "element_count": 5,
        "unexpected_count": 4,
        "unexpected_percent": 80.0,
        "partial_unexpected_list": [None, None, None, None],
        "partial_unexpected_index_list": [1, 2, 3, 4],
        "partial_unexpected_counts": [
            {"count": 4, "value": None},
        ],
        "unexpected_list": [None, None, None, None],
        "unexpected_index_list": [1, 2, 3, 4],
        "unexpected_index_query": "df.filter(items=[1, 2, 3, 4], axis=0)",
    }


@parameterize_batch_for_data_sources(
    data_source_configs=[PandasFilesystemCsvDatasourceTestConfig()], data=DATA
)
def test_failure_pandas_csv(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnValuesToNotBeNull(column=MOSTLY_NULL_COLUMN)
    result = batch_for_datasource.validate(expectation, result_format=ResultFormat.COMPLETE)
    assert not result.success
    assert result.to_json_dict()["result"] == {
        "element_count": 5,
        "unexpected_count": 4,
        "unexpected_percent": 80.0,
        "partial_unexpected_list": [None, None, None, None],
        "partial_unexpected_index_list": [1, 2, 3, 4],
        "partial_unexpected_counts": [
            # TODO: NOT THIS
            {"count": 1, "value": None},
            {"count": 1, "value": None},
            {"count": 1, "value": None},
            {"count": 1, "value": None},
        ],
        "unexpected_list": [None, None, None, None],
        "unexpected_index_list": [1, 2, 3, 4],
        "unexpected_index_query": "df.filter(items=[1, 2, 3, 4], axis=0)",
    }


@parameterize_batch_for_data_sources(
    data_source_configs=[
        SparkFilesystemCsvDatasourceTestConfig(
            column_types=SPARK_COLUMN_TYPES,
        )
    ],
    data=DATA,
)
def test_failure_spark(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnValuesToNotBeNull(column=MOSTLY_NULL_COLUMN)
    result = batch_for_datasource.validate(expectation, result_format=ResultFormat.COMPLETE)
    assert not result.success
    assert result.to_json_dict()["result"] == {
        "element_count": 5,
        "unexpected_count": 4,
        "unexpected_percent": 80.0,
        "partial_unexpected_list": [None, None, None, None],
        "partial_unexpected_counts": [
            {"count": 4, "value": None},
        ],
        "unexpected_list": [None, None, None, None],
        "unexpected_index_query": ANY,
    }


@parameterize_batch_for_data_sources(data_source_configs=SQL_DATA_SOURCES, data=DATA)
def test_failure_complete_sql(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnValuesToNotBeNull(column=MOSTLY_NULL_COLUMN)
    result = batch_for_datasource.validate(expectation, result_format=ResultFormat.COMPLETE)
    assert not result.success
    assert result.to_json_dict()["result"] == {
        "element_count": 5,
        "unexpected_count": 4,
        "unexpected_percent": 80.0,
        "partial_unexpected_list": [None, None, None, None],
        "partial_unexpected_counts": [
            {"count": 4, "value": None},
        ],
        "unexpected_list": [None, None, None, None],
        "unexpected_index_query": ANY,
    }


@pytest.mark.parametrize(
    "expectation",
    [
        pytest.param(
            gxe.ExpectColumnValuesToNotBeNull(column=NON_NULL_COLUMN),
            id="no_nulls",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToNotBeNull(column=MOSTLY_NULL_COLUMN, mostly=0.2),
            id="mostly",
        ),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_success(
    batch_for_datasource: Batch,
    expectation: gxe.ExpectColumnValuesToNotBeNull,
) -> None:
    result = batch_for_datasource.validate(expectation)
    assert result.success


@pytest.mark.parametrize(
    "expectation",
    [
        pytest.param(
            gxe.ExpectColumnValuesToNotBeNull(column=ALL_NULL_COLUMN),
            id="no_nulls",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToNotBeNull(column=MOSTLY_NULL_COLUMN, mostly=0.3),
            id="mostly_not_met",
        ),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_failure(
    batch_for_datasource: Batch,
    expectation: gxe.ExpectColumnValuesToNotBeNull,
) -> None:
    result = batch_for_datasource.validate(expectation)
    assert not result.success


@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_include_unexpected_rows_pandas(batch_for_datasource: Batch) -> None:
    """Test that include_unexpected_rows works correctly for ExpectColumnValuesToNotBeNull."""
    expectation = gxe.ExpectColumnValuesToNotBeNull(column=MOSTLY_NULL_COLUMN)
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

    # Should contain 4 rows where MOSTLY_NULL_COLUMN is null
    assert len(unexpected_rows_df) == 4

    # All values in the MOSTLY_NULL_COLUMN should be null in the unexpected rows
    assert unexpected_rows_df[MOSTLY_NULL_COLUMN].isnull().all()

    # Other columns should have their original values (rows with indices 1,2,3,4)
    # In the unexpected_rows result, these get re-indexed starting from 0
    assert list(unexpected_rows_df[NON_NULL_COLUMN]) == [2, 3, 4, 5]
    assert list(unexpected_rows_df[ALL_NULL_COLUMN].isnull()) == [True, True, True, True]


@parameterize_batch_for_data_sources(
    data_source_configs=[PostgreSQLDatasourceTestConfig()], data=DATA
)
def test_include_unexpected_rows_sql(batch_for_datasource: Batch) -> None:
    """Test include_unexpected_rows for ExpectColumnValuesToNotBeNull with SQL."""
    expectation = gxe.ExpectColumnValuesToNotBeNull(column=MOSTLY_NULL_COLUMN)
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

    # Should contain 4 rows where MOSTLY_NULL_COLUMN is null
    assert len(unexpected_rows_data) == 4

    # Check that null values appear in the unexpected rows data (represented as None)
    unexpected_rows_str = str(unexpected_rows_data)
    assert "None" in unexpected_rows_str or "null" in unexpected_rows_str.lower()


@parameterize_batch_for_data_sources(
    data_source_configs=JUST_PANDAS_DATA_SOURCES,
    data=IDX_DATA,
)
def test_unexpected_index_column_names(batch_for_datasource: Batch) -> None:
    """
    Verify that when `unexpected_index_column_names` is requested via result_format,
    ExpectColumnValuesToNotBeNull includes it in the result payload.
    """
    expectation = gxe.ExpectColumnValuesToNotBeNull(column="email")

    result = batch_for_datasource.validate(
        expectation,
        result_format={
            "result_format": "COMPLETE",
            "unexpected_index_column_names": ["customer_id"],
        },
    )

    assert not result.success
    result_dict = result["result"]
    assert "unexpected_index_column_names" in result_dict
    assert result_dict["unexpected_index_column_names"] == ["customer_id"]
