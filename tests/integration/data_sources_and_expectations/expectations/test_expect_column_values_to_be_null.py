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
    PostgreSQLDatasourceTestConfig,
    SparkFilesystemCsvDatasourceTestConfig,
)

ALL_NULL_COLUMN = "all_nulls"
MOSTLY_NULL_COLUMN = "mostly_nulls"

DATA = pd.DataFrame(
    {
        MOSTLY_NULL_COLUMN: [1, None, None, None, None],  # 80% null
        ALL_NULL_COLUMN: [None, None, None, None, None],
    },
    dtype="object",
)

try:
    from great_expectations.compatibility.pyspark import types as PYSPARK_TYPES

    SPARK_COLUMN_TYPES = {
        MOSTLY_NULL_COLUMN: PYSPARK_TYPES.IntegerType,
        ALL_NULL_COLUMN: PYSPARK_TYPES.IntegerType,
    }
except ModuleNotFoundError:
    SPARK_COLUMN_TYPES = {}


@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_success_complete_pandas(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnValuesToBeNull(column=ALL_NULL_COLUMN)
    result = batch_for_datasource.validate(expectation, result_format=ResultFormat.COMPLETE)
    assert result.success


@parameterize_batch_for_data_sources(
    data_source_configs=[
        SparkFilesystemCsvDatasourceTestConfig(column_types=SPARK_COLUMN_TYPES),
    ],
    data=DATA,
)
def test_success_complete_spark(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnValuesToBeNull(column=ALL_NULL_COLUMN)
    result = batch_for_datasource.validate(expectation, result_format=ResultFormat.COMPLETE)
    assert result.success


@parameterize_batch_for_data_sources(data_source_configs=SQL_DATA_SOURCES, data=DATA)
def test_success_complete_sql(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnValuesToBeNull(column=ALL_NULL_COLUMN)
    result = batch_for_datasource.validate(expectation, result_format=ResultFormat.COMPLETE)
    assert result.success
    assert result.to_json_dict()["result"] == {
        "element_count": 5,
        "unexpected_count": 0,
        "unexpected_percent": 0.0,
        "partial_unexpected_list": [],
        "unexpected_index_query": ANY,
        "partial_unexpected_counts": [],
        "unexpected_list": [],
    }


@pytest.mark.parametrize(
    "expectation",
    [
        pytest.param(
            gxe.ExpectColumnValuesToBeNull(column=ALL_NULL_COLUMN),
            id="all_nulls",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToBeNull(column=MOSTLY_NULL_COLUMN, mostly=0.8),
            id="mostly_nulls_success",
        ),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_success(
    batch_for_datasource: Batch,
    expectation: gxe.ExpectColumnValuesToBeNull,
) -> None:
    result = batch_for_datasource.validate(expectation)
    assert result.success


@pytest.mark.parametrize(
    "expectation",
    [
        pytest.param(
            gxe.ExpectColumnValuesToBeNull(column=MOSTLY_NULL_COLUMN),
            id="not_all_nulls",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToBeNull(column=MOSTLY_NULL_COLUMN, mostly=0.9),
            id="mostly_threshold_not_met",
        ),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_failure(
    batch_for_datasource: Batch,
    expectation: gxe.ExpectColumnValuesToBeNull,
) -> None:
    result = batch_for_datasource.validate(expectation)
    assert not result.success


@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_include_unexpected_rows_pandas(batch_for_datasource: Batch) -> None:
    """Test that include_unexpected_rows works correctly for ExpectColumnValuesToBeNull."""
    expectation = gxe.ExpectColumnValuesToBeNull(column=MOSTLY_NULL_COLUMN)
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

    # Should contain 1 row where MOSTLY_NULL_COLUMN is not null (index 0 with value 1)
    assert len(unexpected_rows_df) == 1
    assert list(unexpected_rows_df.index) == [0]

    # The unexpected row should have value 1 in MOSTLY_NULL_COLUMN
    assert unexpected_rows_df.loc[0, MOSTLY_NULL_COLUMN] == 1

    # ALL_NULL_COLUMN should be null in the unexpected row
    assert pd.isna(unexpected_rows_df.loc[0, ALL_NULL_COLUMN])


@parameterize_batch_for_data_sources(
    data_source_configs=[PostgreSQLDatasourceTestConfig()], data=DATA
)
def test_include_unexpected_rows_sql(batch_for_datasource: Batch) -> None:
    """Test include_unexpected_rows for ExpectColumnValuesToBeNull with SQL."""
    expectation = gxe.ExpectColumnValuesToBeNull(column=MOSTLY_NULL_COLUMN)
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

    # Should contain 1 row where MOSTLY_NULL_COLUMN is not null (value 1)
    assert len(unexpected_rows_data) == 1

    # Check that the non-null value "1" appears in the unexpected rows data
    unexpected_rows_str = str(unexpected_rows_data)
    assert "1" in unexpected_rows_str
