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
    BigQueryDatasourceTestConfig,
    DatabricksDatasourceTestConfig,
    GenericSQLDatasourceTestConfig,
    MySQLDatasourceTestConfig,
    PandasDataFrameDatasourceTestConfig,
    PandasFilesystemCsvDatasourceTestConfig,
    PostgreSQLDatasourceTestConfig,
    RedshiftDatasourceTestConfig,
    SnowflakeDatasourceTestConfig,
    SparkFilesystemCsvDatasourceTestConfig,
    SqliteDatasourceTestConfig,
    SQLServerDatasourceTestConfig,
)

INTEGER_COLUMN = "integers"
INTEGER_AND_NULL_COLUMN = "integers_and_nulls"
STRING_COLUMN = "strings"
NULL_COLUMN = "nulls"


DATA = pd.DataFrame(
    {
        INTEGER_COLUMN: [1, 2, 3, 4, 5],
        INTEGER_AND_NULL_COLUMN: [1, 2, 3, 4, None],
        STRING_COLUMN: ["a", "b", "c", "d", "e"],
        NULL_COLUMN: pd.Series([None, None, None, None, None]),
    },
    dtype="object",
)


try:
    from great_expectations.compatibility.pyspark import types as PYSPARK_TYPES

    SPARK_COLUMN_TYPES = {
        INTEGER_COLUMN: PYSPARK_TYPES.IntegerType,
        INTEGER_AND_NULL_COLUMN: PYSPARK_TYPES.IntegerType,
        STRING_COLUMN: PYSPARK_TYPES.StringType,
        NULL_COLUMN: PYSPARK_TYPES.IntegerType,
    }
except ModuleNotFoundError:
    SPARK_COLUMN_TYPES = {}


@parameterize_batch_for_data_sources(
    data_source_configs=[
        PandasDataFrameDatasourceTestConfig(),
        PandasFilesystemCsvDatasourceTestConfig(),
    ],
    data=DATA,
)
def test_success_for_type__int(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnValuesToBeOfType(column=INTEGER_COLUMN, type_="int")
    result = batch_for_datasource.validate(expectation)
    assert result.success


@parameterize_batch_for_data_sources(
    data_source_configs=[
        BigQueryDatasourceTestConfig(),
        SQLServerDatasourceTestConfig(),
        MySQLDatasourceTestConfig(),
        PostgreSQLDatasourceTestConfig(),
        RedshiftDatasourceTestConfig(),
        GenericSQLDatasourceTestConfig(),
        SqliteDatasourceTestConfig(),
    ],
    data=DATA,
)
def test_success_for_type__INTEGER(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnValuesToBeOfType(column=INTEGER_COLUMN, type_="INTEGER")
    result = batch_for_datasource.validate(expectation)
    assert result.success


@parameterize_batch_for_data_sources(
    data_source_configs=[DatabricksDatasourceTestConfig()],
    data=DATA,
)
def test_success_for_type__Integer(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnValuesToBeOfType(column=INTEGER_COLUMN, type_="INT")
    result = batch_for_datasource.validate(expectation)
    assert result.success


@parameterize_batch_for_data_sources(
    data_source_configs=[
        SparkFilesystemCsvDatasourceTestConfig(
            column_types=SPARK_COLUMN_TYPES,
        )
    ],
    data=DATA,
)
def test_success_for_type__IntegerType(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnValuesToBeOfType(column=INTEGER_COLUMN, type_="IntegerType")
    result = batch_for_datasource.validate(expectation)
    assert result.success


@parameterize_batch_for_data_sources(
    data_source_configs=[SnowflakeDatasourceTestConfig()],
    data=DATA,
)
def test_success_for_type__Number(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnValuesToBeOfType(column=INTEGER_COLUMN, type_="DECIMAL(38, 0)")
    result = batch_for_datasource.validate(expectation)
    assert result.success


@pytest.mark.parametrize(
    "expectation",
    [
        gxe.ExpectColumnValuesToBeOfType(column=INTEGER_COLUMN, type_="int"),
        gxe.ExpectColumnValuesToBeOfType(column=INTEGER_AND_NULL_COLUMN, type_="int"),
        gxe.ExpectColumnValuesToBeOfType(column=STRING_COLUMN, type_="str"),
        gxe.ExpectColumnValuesToBeOfType(column=NULL_COLUMN, type_="float64"),
    ],
)
@parameterize_batch_for_data_sources(
    data_source_configs=JUST_PANDAS_DATA_SOURCES,
    data=DATA,
)
def test_success_types(
    batch_for_datasource: Batch, expectation: gxe.ExpectColumnValuesToBeOfType
) -> None:
    result = batch_for_datasource.validate(expectation)
    assert result.success


@parameterize_batch_for_data_sources(
    data_source_configs=JUST_PANDAS_DATA_SOURCES,
    data=DATA,
)
def test_failure(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnValuesToBeOfType(column=INTEGER_COLUMN, type_="NUMBER")
    result = batch_for_datasource.validate(expectation)
    assert not result.success


# Group datasources with case-insensitive type handling
@parameterize_batch_for_data_sources(
    data_source_configs=[
        DatabricksDatasourceTestConfig(),
        PostgreSQLDatasourceTestConfig(),
        SnowflakeDatasourceTestConfig(),
        SQLServerDatasourceTestConfig(),
    ],
    data=DATA,
)
def test_case_insensitive_dialects(batch_for_datasource: Batch) -> None:
    dialect_name = batch_for_datasource.data.execution_engine.engine.dialect.name.lower()

    expected_dialects = ["snowflake", "databricks", "postgresql", "mssql"]
    assert dialect_name in expected_dialects, f"Unexpected dialect: {dialect_name}"

    if dialect_name == "snowflake":
        base_type = "DECIMAL(38, 0)"
    elif dialect_name == "databricks":
        base_type = "INT"
    elif dialect_name in {"postgresql", "mssql"}:
        base_type = "INTEGER"
    else:
        raise AssertionError(f"Unexpected dialect: {dialect_name}")

    for type_str in [base_type.lower(), base_type.upper(), base_type.capitalize()]:
        expectation = gxe.ExpectColumnValuesToBeOfType(column=INTEGER_COLUMN, type_=type_str)
        result = batch_for_datasource.validate(expectation)
        assert result.success, f"Expected success for type '{type_str}' on dialect '{dialect_name}'"


@pytest.mark.parametrize(
    "suite_param_value,expected_result",
    [
        pytest.param("int", True, id="success"),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_success_with_suite_param_type_(
    batch_for_datasource: Batch, suite_param_value: str, expected_result: bool
) -> None:
    suite_param_key = "test_expect_column_values_to_be_of_type"
    expectation = gxe.ExpectColumnValuesToBeOfType(
        column=INTEGER_COLUMN,
        type_={"$PARAMETER": suite_param_key},
        result_format=ResultFormat.SUMMARY,
    )
    result = batch_for_datasource.validate(
        expectation, expectation_parameters={suite_param_key: suite_param_value}
    )
    assert result.success == expected_result


@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_include_unexpected_rows_pandas(batch_for_datasource: Batch) -> None:
    """Test include_unexpected_rows for ExpectColumnValuesToBeOfType with pandas data sources."""
    expectation = gxe.ExpectColumnValuesToBeOfType(column=STRING_COLUMN, type_="int")
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

    # Use DataFrame directly for pandas data sources
    unexpected_rows_df = unexpected_rows_data

    # Should contain 5 rows where STRING_COLUMN is not of type int (all string values)
    assert len(unexpected_rows_df) == 5

    # The unexpected rows should contain all the string values
    unexpected_values = sorted(unexpected_rows_df[STRING_COLUMN].tolist())
    assert unexpected_values == ["a", "b", "c", "d", "e"]
