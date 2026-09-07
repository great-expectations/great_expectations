import pandas as pd
import pytest
from sqlalchemy import types as sqlatypes

import great_expectations.expectations as gxe
from great_expectations.core.result_format import ResultFormat
from great_expectations.datasource.fluent.interfaces import Batch
from great_expectations.expectations.core.expect_column_quantile_values_to_be_between import (
    QuantileRange,
)
from tests.integration.conftest import parameterize_batch_for_data_sources
from tests.integration.data_sources_and_expectations.data_source_lists import (
    JUST_PANDAS_DATA_SOURCES,
)
from tests.integration.test_utils.data_source_config import (
    ALL_DATA_SOURCES,
    GenericSQLDatasourceTestConfig,
    RedshiftDatasourceTestConfig,
)
from tests.integration.test_utils.data_source_config.big_query import BigQueryDatasourceTestConfig
from tests.integration.test_utils.data_source_config.databricks import (
    DatabricksDatasourceTestConfig,
)
from tests.integration.test_utils.data_source_config.mysql import MySQLDatasourceTestConfig
from tests.integration.test_utils.data_source_config.pandas_data_frame import (
    PandasDataFrameDatasourceTestConfig,
)
from tests.integration.test_utils.data_source_config.pandas_filesystem_csv import (
    PandasFilesystemCsvDatasourceTestConfig,
)
from tests.integration.test_utils.data_source_config.postgres import PostgreSQLDatasourceTestConfig
from tests.integration.test_utils.data_source_config.snowflake import SnowflakeDatasourceTestConfig
from tests.integration.test_utils.data_source_config.spark_filesystem_csv import (
    SparkFilesystemCsvDatasourceTestConfig,
)
from tests.integration.test_utils.data_source_config.sql_server import SQLServerDatasourceTestConfig
from tests.integration.test_utils.data_source_config.sqlite import SqliteDatasourceTestConfig

COL_NAME = "my_col"

DATA = pd.DataFrame({COL_NAME: [1, 2, 2, 3, 3, 3, 4]})

# Same distribution as DATA, plus nulls. Excluding the nulls from the computation must yield the
# same quantiles DATA produces. The dtype keeps the column an integer one on the SQL backends, so
# that the nulls are the only thing that differs from DATA.
DATA_WITH_NULLS = pd.DataFrame({COL_NAME: [1, 2, 2, 3, 3, 3, 4, None, None]}, dtype="object")

# No duplicates, and quantiles chosen so that quantile * row_count is not a whole number, which is
# where the selected rank is easiest to get wrong.
DISTINCT_DATA = pd.DataFrame({COL_NAME: [10, 20, 30, 40]})

# 0.56 is not representable in binary, so 0.56 * 25 is 14.000000000000002 and a rank computed in
# floating point rounds up to 15. The correct rank is 14, because 14/25 is exactly 0.56.
TWENTY_FIVE_ROWS = pd.DataFrame({COL_NAME: list(range(1, 26))})

ALL_NULLS = pd.DataFrame({COL_NAME: [None, None, None]}, dtype="object")

# An all-null column carries no type of its own, so each backend is told what to make it.
ALL_NULLS_COLUMN_TYPES = {COL_NAME: sqlatypes.INTEGER}
try:
    from great_expectations.compatibility.pyspark import types as PYSPARK_TYPES

    ALL_NULLS_SPARK_COLUMN_TYPES = {COL_NAME: PYSPARK_TYPES.IntegerType}
except ModuleNotFoundError:
    ALL_NULLS_SPARK_COLUMN_TYPES = {}

ALL_NULLS_DATA_SOURCES = [
    BigQueryDatasourceTestConfig(column_types=ALL_NULLS_COLUMN_TYPES),
    DatabricksDatasourceTestConfig(column_types=ALL_NULLS_COLUMN_TYPES),
    SQLServerDatasourceTestConfig(column_types=ALL_NULLS_COLUMN_TYPES),
    MySQLDatasourceTestConfig(column_types=ALL_NULLS_COLUMN_TYPES),
    PandasDataFrameDatasourceTestConfig(),
    PandasFilesystemCsvDatasourceTestConfig(),
    PostgreSQLDatasourceTestConfig(column_types=ALL_NULLS_COLUMN_TYPES),
    RedshiftDatasourceTestConfig(column_types=ALL_NULLS_COLUMN_TYPES),
    GenericSQLDatasourceTestConfig(column_types=ALL_NULLS_COLUMN_TYPES),
    SnowflakeDatasourceTestConfig(column_types=ALL_NULLS_COLUMN_TYPES),
    SparkFilesystemCsvDatasourceTestConfig(column_types=ALL_NULLS_SPARK_COLUMN_TYPES),
    SqliteDatasourceTestConfig(column_types=ALL_NULLS_COLUMN_TYPES),
]

ALL_DATA_SOURCES_EXCEPT_BIGQUERY = [
    ds for ds in ALL_DATA_SOURCES if not isinstance(ds, BigQueryDatasourceTestConfig)
]

# TODO: Consider more test cases before removing expect_column_quantile_values_to_be_between.json


@parameterize_batch_for_data_sources(
    data_source_configs=ALL_DATA_SOURCES_EXCEPT_BIGQUERY, data=DATA
)
def test_success_complete_results(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnQuantileValuesToBeBetween(
        column=COL_NAME,
        quantile_ranges=QuantileRange(
            quantiles=[0, 0.333, 0.667, 1],
            value_ranges=[[0, 1], [2, 3], [3, 4], [4, 5]],
        ),
    )
    result = batch_for_datasource.validate(expectation, result_format=ResultFormat.COMPLETE)
    assert result.success
    assert result.to_json_dict()["result"] == {
        "observed_value": {
            "quantiles": [0.0, 0.333, 0.667, 1.0],
            "values": [1, 2, 3, 4],
        },
        "details": {
            "success_details": [True, True, True, True],
        },
    }


# Every backend, including BigQuery: this asserts only that no quantile was observed, so unlike
# the cases above it does not depend on the type or shape of a returned value.
@parameterize_batch_for_data_sources(data_source_configs=ALL_NULLS_DATA_SOURCES, data=ALL_NULLS)
def test_all_null_column_reports_unmet_expectation(batch_for_datasource: Batch) -> None:
    """A column with no quantiles fails the expectation instead of raising.

    The backends report the absent quantile differently -- SQL engines return NULL, pandas
    returns NaN, and Spark returns no values at all -- and every one of them must report it as
    an unmet expectation.
    """
    expectation = gxe.ExpectColumnQuantileValuesToBeBetween(
        column=COL_NAME,
        quantile_ranges=QuantileRange(quantiles=[0.25, 0.5], value_ranges=[[0, 99], [0, 99]]),
    )
    result = batch_for_datasource.validate(expectation, result_format=ResultFormat.COMPLETE)
    assert not result.success
    assert result.to_json_dict()["result"] == {
        "observed_value": {
            "quantiles": [0.25, 0.5],
            "values": [None, None],
        },
        "details": {
            "success_details": [False, False],
        },
    }


@parameterize_batch_for_data_sources(
    data_source_configs=ALL_DATA_SOURCES_EXCEPT_BIGQUERY, data=DATA_WITH_NULLS
)
def test_nulls_are_excluded_from_quantiles(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnQuantileValuesToBeBetween(
        column=COL_NAME,
        quantile_ranges=QuantileRange(
            quantiles=[0, 0.333, 0.667, 1],
            value_ranges=[[0, 1], [2, 3], [3, 4], [4, 5]],
        ),
    )
    result = batch_for_datasource.validate(expectation, result_format=ResultFormat.COMPLETE)
    assert result.success
    assert result.to_json_dict()["result"] == {
        "observed_value": {
            "quantiles": [0.0, 0.333, 0.667, 1.0],
            "values": [1, 2, 3, 4],
        },
        "details": {
            "success_details": [True, True, True, True],
        },
    }


# Pandas and SQLite only. The MySQL implementation resolves quantiles from percent_rank, which
# picks the largest rank at or below the quantile rather than the first one to reach it, so it
# reports [10, 20] for this data. That predates this change and is left alone here.
@parameterize_batch_for_data_sources(
    data_source_configs=[*JUST_PANDAS_DATA_SOURCES, SqliteDatasourceTestConfig()],
    data=DISTINCT_DATA,
)
def test_quantiles_when_quantile_times_count_is_not_whole(
    batch_for_datasource: Batch,
) -> None:
    expectation = gxe.ExpectColumnQuantileValuesToBeBetween(
        column=COL_NAME,
        quantile_ranges=QuantileRange(
            quantiles=[0.3, 0.6],
            value_ranges=[[20, 20], [30, 30]],
        ),
    )
    result = batch_for_datasource.validate(expectation, result_format=ResultFormat.COMPLETE)
    assert result.success
    assert result.to_json_dict()["result"] == {
        "observed_value": {
            "quantiles": [0.3, 0.6],
            "values": [20, 30],
        },
        "details": {
            "success_details": [True, True],
        },
    }


# Pandas and SQLite only, and deliberately so: do not extend this to the other SQL backends. They
# implement "percentile_disc" in double precision, where 0.56 * 25 exceeds 14, so PostgreSQL and
# its peers report 15 for this data. SQLite ranks on the quantile as written and reports 14.
#
# This case already passes on develop, where truncating the offset happens to land on 14. It is
# here to pin the exact-decimal arithmetic, not to reproduce a defect.
@parameterize_batch_for_data_sources(
    data_source_configs=[*JUST_PANDAS_DATA_SOURCES, SqliteDatasourceTestConfig()],
    data=TWENTY_FIVE_ROWS,
)
def test_quantile_times_count_inexact_in_binary_float(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnQuantileValuesToBeBetween(
        column=COL_NAME,
        quantile_ranges=QuantileRange(quantiles=[0.56], value_ranges=[[14, 14]]),
    )
    result = batch_for_datasource.validate(expectation, result_format=ResultFormat.COMPLETE)
    assert result.success
    assert result.to_json_dict()["result"] == {
        "observed_value": {
            "quantiles": [0.56],
            "values": [14],
        },
        "details": {
            "success_details": [True],
        },
    }


@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_allows_unspecified_extremes(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnQuantileValuesToBeBetween(
        column=COL_NAME,
        quantile_ranges=QuantileRange(
            quantiles=[0, 0.333, 0.667, 1],
            value_ranges=[[None, 1], [2, 3], [3, 4], [4, None]],
        ),
    )
    result = batch_for_datasource.validate(expectation, result_format=ResultFormat.COMPLETE)
    assert result.success


@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_failure(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnQuantileValuesToBeBetween(
        column=COL_NAME,
        quantile_ranges=QuantileRange(
            quantiles=[0, 0.333, 0.667, 1],
            value_ranges=[[0, 1], [1, 2], [1, 2], [2, 3]],
        ),
    )
    result = batch_for_datasource.validate(expectation, result_format=ResultFormat.COMPLETE)
    assert not result.success


@pytest.mark.parametrize(
    "suite_param_value,expected_result",
    [
        pytest.param(
            {
                "quantiles": [0, 0.333, 0.667, 1],
                "value_ranges": [[0, 1], [2, 3], [3, 4], [4, 5]],
            },
            True,
            id="success",
        ),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_success_with_suite_param_quantile_ranges_(
    batch_for_datasource: Batch, suite_param_value: dict, expected_result: bool
) -> None:
    suite_param_key = "test_expect_column_quantile_values_to_be_between"
    expectation = gxe.ExpectColumnQuantileValuesToBeBetween(
        column=COL_NAME,
        quantile_ranges={"$PARAMETER": suite_param_key},
        result_format=ResultFormat.SUMMARY,
    )
    result = batch_for_datasource.validate(
        expectation, expectation_parameters={suite_param_key: suite_param_value}
    )
    assert result.success == expected_result


@pytest.mark.parametrize(
    "suite_param_value,expected_result",
    [
        pytest.param(False, True, id="success"),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_success_with_suite_param_allow_relative_error_(
    batch_for_datasource: Batch, suite_param_value: bool, expected_result: bool
) -> None:
    suite_param_key = "test_expect_column_quantile_values_to_be_between"
    expectation = gxe.ExpectColumnQuantileValuesToBeBetween(
        column=COL_NAME,
        quantile_ranges=QuantileRange(
            quantiles=[0, 0.333, 0.667, 1],
            value_ranges=[[0, 1], [2, 3], [3, 4], [4, 5]],
        ),
        allow_relative_error={"$PARAMETER": suite_param_key},
        result_format=ResultFormat.SUMMARY,
    )
    result = batch_for_datasource.validate(
        expectation, expectation_parameters={suite_param_key: suite_param_value}
    )
    assert result.success == expected_result
