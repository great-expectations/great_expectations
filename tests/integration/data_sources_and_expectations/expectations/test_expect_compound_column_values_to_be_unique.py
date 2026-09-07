from datetime import datetime

import pandas as pd
import pytest

import great_expectations.expectations as gxe
from great_expectations.compatibility import pydantic
from great_expectations.datasource.fluent.interfaces import Batch
from tests.integration.conftest import parameterize_batch_for_data_sources
from tests.integration.data_sources_and_expectations.data_source_lists import (
    JUST_PANDAS_DATA_SOURCES,
)
from tests.integration.test_utils.data_source_config import (
    ALL_DATA_SOURCES,
    PostgreSQLDatasourceTestConfig,
    SparkFilesystemCsvDatasourceTestConfig,
)

STRING_COL = "string_col"
INT_COL = "int_col"
INT_COL_2 = "int_col_2"
DUPLICATES = "duplicates"
EVENT_TS = "event_ts"
CATEGORY = "category"

try:
    from great_expectations.compatibility.pyspark import types as PYSPARK_TYPES

    SPARK_COLUMN_TYPES = {
        STRING_COL: PYSPARK_TYPES.StringType,
        INT_COL: PYSPARK_TYPES.IntegerType,
        INT_COL_2: PYSPARK_TYPES.IntegerType,
        DUPLICATES: PYSPARK_TYPES.IntegerType,
    }
    SPARK_TIMESTAMP_COLUMN_TYPES = {
        EVENT_TS: PYSPARK_TYPES.TimestampType,
        CATEGORY: PYSPARK_TYPES.StringType,
    }
except ModuleNotFoundError:
    SPARK_COLUMN_TYPES = {}
    SPARK_TIMESTAMP_COLUMN_TYPES = {}


DATA = pd.DataFrame(
    {
        STRING_COL: ["foo", "bar", "foo", "baz", None, None],
        INT_COL: [1, 2, 1, 3, None, None],
        INT_COL_2: [1, 2, 3, 4, None, None],
        DUPLICATES: [100, 100, 100, 100, 99, 99],
    }
)


@parameterize_batch_for_data_sources(data_source_configs=ALL_DATA_SOURCES, data=DATA)
def test_golden_path(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectCompoundColumnsToBeUnique(
        column_list=[STRING_COL, INT_COL, INT_COL_2],
        ignore_row_if="any_value_is_missing",
    )
    result = batch_for_datasource.validate(expectation)
    assert result.success


@pytest.mark.parametrize(
    "expectation",
    [
        pytest.param(
            gxe.ExpectCompoundColumnsToBeUnique(column_list=[INT_COL, INT_COL_2]),
            id="two_cols",
        ),
        pytest.param(
            gxe.ExpectCompoundColumnsToBeUnique(column_list=[STRING_COL, INT_COL, INT_COL_2]),
            id="three_cols",
        ),
        pytest.param(
            gxe.ExpectCompoundColumnsToBeUnique(column_list=[INT_COL, DUPLICATES], mostly=0.3),
            id="mostly",
        ),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_success(
    batch_for_datasource: Batch, expectation: gxe.ExpectCompoundColumnsToBeUnique
) -> None:
    result = batch_for_datasource.validate(expectation)
    assert result.success


@pytest.mark.parametrize(
    "expectation",
    [
        pytest.param(
            gxe.ExpectCompoundColumnsToBeUnique(column_list=[INT_COL, DUPLICATES]),
        ),
        pytest.param(
            gxe.ExpectCompoundColumnsToBeUnique(column_list=[INT_COL, DUPLICATES], mostly=0.4),
            id="mostly_threshold_not_met",
        ),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_failure(
    batch_for_datasource: Batch, expectation: gxe.ExpectCompoundColumnsToBeUnique
) -> None:
    result = batch_for_datasource.validate(expectation)
    assert not result.success


@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_include_unexpected_rows_pandas(batch_for_datasource: Batch) -> None:
    """Test that include_unexpected_rows works correctly for ExpectCompoundColumnsToBeUnique."""
    expectation = gxe.ExpectCompoundColumnsToBeUnique(column_list=[INT_COL, DUPLICATES])
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

    # Should contain duplicate compound values
    # (rows where the combination of INT_COL and DUPLICATES is duplicated)
    assert len(unexpected_rows_df) > 0

    # Verify that the unexpected rows contain the duplicated compound values
    # DUPLICATES column has [100, 100, 100, 100, 99, 99] and INT_COL has [1, 2, 1, 3, None, None]
    # So we should see duplicates for combinations that repeat
    assert len(unexpected_rows_df) >= 4  # At least the rows with duplicated combinations


@parameterize_batch_for_data_sources(
    data_source_configs=[PostgreSQLDatasourceTestConfig()], data=DATA
)
def test_include_unexpected_rows_sql(batch_for_datasource: Batch) -> None:
    """Test include_unexpected_rows for ExpectCompoundColumnsToBeUnique with SQL data sources."""
    expectation = gxe.ExpectCompoundColumnsToBeUnique(column_list=[INT_COL, DUPLICATES])
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

    # Should contain duplicate compound values
    assert len(unexpected_rows_data) > 0

    # Verify that the unexpected rows contain the duplicated compound values
    # there are 4 unexpected rows, but our implementation only returns each duplicate once
    assert len(unexpected_rows_data) >= 2

    unexpected_rows_str = str(unexpected_rows_data)
    # Should contain the duplicate values from the DUPLICATES column (100 and 99)
    assert "100" in unexpected_rows_str


@pytest.mark.parametrize(
    "result_format",
    [
        pytest.param(
            {"result_format": "BOOLEAN_ONLY", "return_unexpected_index_query": True},
            id="boolean_only_with_flag",
        ),
        pytest.param(
            {"result_format": "BASIC", "return_unexpected_index_query": True},
            id="basic_with_flag",
        ),
        pytest.param(
            {"result_format": "SUMMARY", "return_unexpected_index_query": True},
            id="summary_with_flag",
        ),
        pytest.param(
            {"result_format": "COMPLETE", "return_unexpected_index_query": True},
            id="complete_with_flag",
        ),
        pytest.param(
            {"result_format": "COMPLETE"},
            id="complete_default",
        ),
    ],
)
@parameterize_batch_for_data_sources(
    data_source_configs=[PostgreSQLDatasourceTestConfig()], data=DATA
)
def test_unexpected_index_query_sql(batch_for_datasource: Batch, result_format: dict) -> None:
    """Test that unexpected_index_query is returned for ExpectCompoundColumnsToBeUnique with SQL."""
    expectation = gxe.ExpectCompoundColumnsToBeUnique(column_list=[INT_COL, DUPLICATES])
    result = batch_for_datasource.validate(expectation, result_format=result_format)

    assert not result.success
    result_dict = result["result"]

    # Verify that unexpected_index_query is present
    assert "unexpected_index_query" in result_dict
    assert result_dict["unexpected_index_query"] is not None
    assert isinstance(result_dict["unexpected_index_query"], str)
    assert len(result_dict["unexpected_index_query"]) > 0

    # Verify the query includes the compound columns
    query = result_dict["unexpected_index_query"]
    assert INT_COL in query or INT_COL.lower() in query.lower()
    assert DUPLICATES in query or DUPLICATES.lower() in query.lower()


@parameterize_batch_for_data_sources(
    data_source_configs=[PostgreSQLDatasourceTestConfig()], data=DATA
)
def test_unexpected_index_query_sql_suppressed(batch_for_datasource: Batch) -> None:
    """Test that unexpected_index_query can be suppressed with return_unexpected_index_query=False."""  # noqa: E501
    expectation = gxe.ExpectCompoundColumnsToBeUnique(column_list=[INT_COL, DUPLICATES])
    result = batch_for_datasource.validate(
        expectation,
        result_format={"result_format": "COMPLETE", "return_unexpected_index_query": False},
    )

    assert not result.success
    result_dict = result["result"]

    # Verify that unexpected_index_query is NOT present when explicitly suppressed
    assert "unexpected_index_query" not in result_dict


@pytest.mark.unit
@pytest.mark.parametrize(
    "column_list",
    [
        pytest.param([], id="no_cols"),
        pytest.param([INT_COL_2], id="one_col"),
    ],
)
def test_invalid_config(column_list: list[str]) -> None:
    with pytest.raises(pydantic.ValidationError):
        gxe.ExpectCompoundColumnsToBeUnique(column_list=column_list)


# Naive datetimes are intentional: Spark's CSV roundtrip and timestamp parsing
# behave deterministically when input/output stay tz-naive, avoiding session/local
# timezone interactions that would otherwise make the comparison flaky.
DATA_WITH_TIMESTAMP_DUPLICATES = pd.DataFrame(
    {
        EVENT_TS: [
            datetime(2024, 1, 1),  # noqa: DTZ001
            datetime(2024, 1, 1),  # noqa: DTZ001
            datetime(2024, 1, 2),  # noqa: DTZ001
        ],
        CATEGORY: ["A", "A", "B"],
    }
)


@parameterize_batch_for_data_sources(
    data_source_configs=[
        SparkFilesystemCsvDatasourceTestConfig(column_types=SPARK_TIMESTAMP_COLUMN_TYPES),
    ],
    data=DATA_WITH_TIMESTAMP_DUPLICATES,
)
def test_partial_unexpected_list_spark_with_timestamp_columns(batch_for_datasource: Batch) -> None:
    # Regression test for https://github.com/great-expectations/great_expectations/issues/11633:
    # the Spark codepath used .toPandas() to materialize unexpected rows, which fails on
    # PySpark 3.1.x + Pandas 2.x because the corrected pandas type for TimestampType was
    # bare ``np.datetime64`` (no precision) — Pandas 2.x rejects that with
    # "Passing in 'datetime64' dtype with no precision is not allowed".
    result = batch_for_datasource.validate(
        gxe.ExpectCompoundColumnsToBeUnique(column_list=[EVENT_TS, CATEGORY]),
        result_format={"result_format": "COMPLETE"},
    )
    assert not result.success
    assert result.result["unexpected_count"] == 2

    rows = result.result["partial_unexpected_list"]
    assert len(rows) == 2
    expected_ts = datetime(2024, 1, 1)  # noqa: DTZ001
    for row in rows:
        # pd.Timestamp and datetime.datetime compare equal for the same moment, so this
        # assertion is independent of which Python type the metric returns.
        assert row[EVENT_TS] == expected_ts
        assert row[CATEGORY] == "A"
