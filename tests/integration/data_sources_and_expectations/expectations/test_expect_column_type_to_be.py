import datetime

import pandas as pd
import pytest
from sqlalchemy import types as sqlatypes

import great_expectations.expectations as gxe
from great_expectations.core.result_format import ResultFormat
from great_expectations.datasource.fluent.interfaces import Batch
from great_expectations.execution_engine import (
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
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
STRING_COLUMN = "strings"
FLOAT_COLUMN = "floats"

DATA = pd.DataFrame(
    {
        INTEGER_COLUMN: [1, 2, 3, 4, 5],
        STRING_COLUMN: ["a", "b", "c", "d", "e"],
        FLOAT_COLUMN: [1.5, 2.5, 3.5, 4.5, 5.5],
    },
    dtype="object",
)

TYPED_DATA = pd.DataFrame(
    {
        INTEGER_COLUMN: pd.Series([1, 2, 3, 4, 5], dtype="int64"),
        STRING_COLUMN: pd.Series(["a", "b", "c", "d", "e"], dtype="str"),
    }
)

NULLABLE_INTEGER_COLUMN = "nullable_integers"
DATETIME_COLUMN = "datetimes"

NULLABLE_DATA = pd.DataFrame(
    {
        INTEGER_COLUMN: pd.Series([1, 2, 3], dtype="int64"),
        NULLABLE_INTEGER_COLUMN: pd.Series([1, None, 3], dtype="Int64"),
    }
)

DATETIME_DATA = pd.DataFrame(
    {DATETIME_COLUMN: pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"])}
)

BIGINT_COLUMN = "bigints"
DATE_COLUMN = "dates"
SQL_DATETIME_COLUMN = "sql_datetimes"
JSON_COLUMN = "json_values"

SQL_TYPED_DATA = pd.DataFrame(
    {
        BIGINT_COLUMN: [1, 2, 3],
        DATE_COLUMN: [datetime.date(2024, 1, d) for d in (1, 2, 3)],
        SQL_DATETIME_COLUMN: pd.to_datetime(
            ["2024-01-01 12:00", "2024-01-02 12:00", "2024-01-03 12:00"]
        ),
    }
)

JSON_DATA = pd.DataFrame({JSON_COLUMN: [1, 2]})

PANDAS_3 = int(pd.__version__.split(".")[0]) >= 3


try:
    from great_expectations.compatibility.pyspark import types as PYSPARK_TYPES

    SPARK_COLUMN_TYPES = {
        INTEGER_COLUMN: PYSPARK_TYPES.IntegerType,
        STRING_COLUMN: PYSPARK_TYPES.StringType,
        FLOAT_COLUMN: PYSPARK_TYPES.DoubleType,
    }
except ModuleNotFoundError:
    SPARK_COLUMN_TYPES = {}


@parameterize_batch_for_data_sources(
    data_source_configs=JUST_PANDAS_DATA_SOURCES,
    data=TYPED_DATA,
)
def test_success_pandas(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnTypeToBe(column=INTEGER_COLUMN, type_="int64")
    result = batch_for_datasource.validate(expectation)
    assert result.success
    assert set(result.result) == {"observed_value"}


@parameterize_batch_for_data_sources(
    data_source_configs=[
        PandasDataFrameDatasourceTestConfig(),
        PandasFilesystemCsvDatasourceTestConfig(),
    ],
    data=TYPED_DATA,
)
def test_success_for_type__int(batch_for_datasource: Batch) -> None:
    """Native python type name resolves via numpy dtype on pandas."""
    expectation = gxe.ExpectColumnTypeToBe(column=INTEGER_COLUMN, type_="int")
    result = batch_for_datasource.validate(expectation)
    assert result.success


@parameterize_batch_for_data_sources(
    data_source_configs=JUST_PANDAS_DATA_SOURCES,
    data=DATA,
)
def test_success_object_dtype(batch_for_datasource: Batch) -> None:
    """Schema-level check: object dtype matches only an object type request."""
    expectation = gxe.ExpectColumnTypeToBe(column=INTEGER_COLUMN, type_="object")
    result = batch_for_datasource.validate(expectation)
    assert result.success


@pytest.mark.parametrize(
    "column,type_,expected_success,expected_observed",
    [
        pytest.param(NULLABLE_INTEGER_COLUMN, "Int64", True, "Int64", id="Int64-matches-Int64"),
        pytest.param(NULLABLE_INTEGER_COLUMN, "int64", False, "Int64", id="Int64-not-int64"),
        pytest.param(INTEGER_COLUMN, "Int64", False, "int64", id="int64-not-Int64"),
    ],
)
@parameterize_batch_for_data_sources(
    data_source_configs=JUST_PANDAS_DATA_SOURCES,
    data=NULLABLE_DATA,
)
def test_nullable_and_numpy_integer_dtypes_are_distinct(
    batch_for_datasource: Batch,
    column: str,
    type_: str,
    expected_success: bool,
    expected_observed: str,
) -> None:
    result = batch_for_datasource.validate(gxe.ExpectColumnTypeToBe(column=column, type_=type_))
    assert result.success is expected_success
    assert result.result["observed_value"] == expected_observed
    assert result.exception_info["raised_exception"] is False


@parameterize_batch_for_data_sources(
    data_source_configs=JUST_PANDAS_DATA_SOURCES,
    data=DATETIME_DATA,
)
def test_unitless_datetime64_matches_any_resolution(batch_for_datasource: Batch) -> None:
    """The default datetime resolution differs across pandas versions; a unit-less name matches."""
    result = batch_for_datasource.validate(
        gxe.ExpectColumnTypeToBe(column=DATETIME_COLUMN, type_="datetime64")
    )
    assert result.success
    assert result.result["observed_value"].startswith("datetime64[")


@pytest.mark.skipif(not PANDAS_3, reason="string columns default to object dtype before pandas 3")
@parameterize_batch_for_data_sources(
    data_source_configs=JUST_PANDAS_DATA_SOURCES,
    data=TYPED_DATA,
)
def test_str_matches_default_string_column(batch_for_datasource: Batch) -> None:
    result = batch_for_datasource.validate(
        gxe.ExpectColumnTypeToBe(column=STRING_COLUMN, type_="str")
    )
    assert result.success
    assert result.result["observed_value"] == "str"


@parameterize_batch_for_data_sources(
    data_source_configs=JUST_PANDAS_DATA_SOURCES,
    data=DATA,
)
def test_str_does_not_match_object_dtype(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnTypeToBe(column=STRING_COLUMN, type_="str")
    result = batch_for_datasource.validate(expectation)
    assert not result.success
    assert result.result["observed_value"] == "object"


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
    expectation = gxe.ExpectColumnTypeToBe(column=INTEGER_COLUMN, type_="INTEGER")
    result = batch_for_datasource.validate(expectation)
    assert result.success


@parameterize_batch_for_data_sources(
    data_source_configs=[
        SqliteDatasourceTestConfig(),
    ],
    data=DATA,
)
def test_success_sql_integer(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnTypeToBe(column=INTEGER_COLUMN, type_="INTEGER")
    result = batch_for_datasource.validate(expectation)
    assert result.success
    assert result.result["observed_value"] == "INTEGER"


@parameterize_batch_for_data_sources(
    data_source_configs=[DatabricksDatasourceTestConfig()],
    data=DATA,
)
def test_success_for_type__INT(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnTypeToBe(column=INTEGER_COLUMN, type_="INT")
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
    expectation = gxe.ExpectColumnTypeToBe(column=INTEGER_COLUMN, type_="IntegerType")
    result = batch_for_datasource.validate(expectation)
    assert result.success
    assert result.result == {"observed_value": "IntegerType"}


@pytest.mark.parametrize(
    "type_,expected_success",
    [
        pytest.param("LongType", False, id="mismatch"),
        pytest.param("StringType", False, id="other-family-mismatch"),
        pytest.param("NumericType", True, id="abstract-family-match"),
        pytest.param("IntegralType", True, id="abstract-subfamily-match"),
    ],
)
@parameterize_batch_for_data_sources(
    data_source_configs=[
        SparkFilesystemCsvDatasourceTestConfig(
            column_types=SPARK_COLUMN_TYPES,
        )
    ],
    data=DATA,
)
def test_spark_known_type(batch_for_datasource: Batch, type_: str, expected_success: bool) -> None:
    result = batch_for_datasource.validate(
        gxe.ExpectColumnTypeToBe(column=INTEGER_COLUMN, type_=type_)
    )
    assert result.success is expected_success
    assert result.result == {"observed_value": "IntegerType"}
    assert result.exception_info["raised_exception"] is False


@pytest.mark.parametrize(
    "type_",
    [
        pytest.param("NotAType", id="unknown-name"),
        pytest.param("int", id="spark-sql-name"),
        pytest.param("Row", id="non-type-class"),
        pytest.param("Any", id="typing-alias"),
        pytest.param("datetime", id="imported-module"),
        pytest.param("cast", id="imported-function"),
    ],
)
@parameterize_batch_for_data_sources(
    data_source_configs=[
        SparkFilesystemCsvDatasourceTestConfig(
            column_types=SPARK_COLUMN_TYPES,
        )
    ],
    data=DATA,
)
def test_spark_unrecognized_type_raises(batch_for_datasource: Batch, type_: str) -> None:
    result = batch_for_datasource.validate(
        gxe.ExpectColumnTypeToBe(column=INTEGER_COLUMN, type_=type_)
    )
    assert not result.success
    assert result.exception_info["raised_exception"] is True
    assert f"Unrecognized spark type: {type_}" in result.exception_info["exception_message"]


@parameterize_batch_for_data_sources(
    data_source_configs=[SnowflakeDatasourceTestConfig()],
    data=DATA,
)
def test_success_for_type__DECIMAL_38_0(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnTypeToBe(column=INTEGER_COLUMN, type_="DECIMAL(38, 0)")
    result = batch_for_datasource.validate(expectation)
    assert result.success


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
        expectation = gxe.ExpectColumnTypeToBe(column=INTEGER_COLUMN, type_=type_str)
        result = batch_for_datasource.validate(expectation)
        assert result.success, f"Expected success for type '{type_str}' on dialect '{dialect_name}'"


@parameterize_batch_for_data_sources(
    data_source_configs=[PostgreSQLDatasourceTestConfig()],
    data=DATA,
)
def test_double_precision_matches_float_not_int(batch_for_datasource: Batch) -> None:
    """A multi-word type name matches the float column and is a plain mismatch on int."""
    float_result = batch_for_datasource.validate(
        gxe.ExpectColumnTypeToBe(column=FLOAT_COLUMN, type_="DOUBLE PRECISION")
    )
    assert float_result.success

    int_result = batch_for_datasource.validate(
        gxe.ExpectColumnTypeToBe(column=INTEGER_COLUMN, type_="DOUBLE PRECISION")
    )
    assert not int_result.success
    assert int_result.exception_info["raised_exception"] is False


@pytest.mark.parametrize(
    "column,type_",
    [
        pytest.param(BIGINT_COLUMN, "BIGINT", id="bigint"),
        pytest.param(DATE_COLUMN, "DATE", id="date"),
        pytest.param(SQL_DATETIME_COLUMN, "datetime", id="datetime-any-case"),
    ],
)
@parameterize_batch_for_data_sources(
    data_source_configs=[
        SqliteDatasourceTestConfig(
            column_types={
                BIGINT_COLUMN: sqlatypes.BIGINT,
                DATE_COLUMN: sqlatypes.DATE,
                SQL_DATETIME_COLUMN: sqlatypes.DATETIME,
            }
        ),
    ],
    data=SQL_TYPED_DATA,
)
def test_observed_type_name_matches_sqlite(
    batch_for_datasource: Batch, column: str, type_: str
) -> None:
    """The type name reported as observed_value always matches, even where the dialect module
    does not export it (BIGINT) or resolves it to a different class than the reflected type."""
    result = batch_for_datasource.validate(gxe.ExpectColumnTypeToBe(column=column, type_=type_))
    assert result.success
    assert result.result["observed_value"] == type_.upper()


@pytest.mark.parametrize(
    "type_",
    [
        pytest.param("int4", id="dialect-alias"),
        pytest.param("bool", id="dialect-alias-other-type"),
        pytest.param("NUMBER", id="other-dialect-name"),
        pytest.param("NOT_A_TYPE", id="unknown-name"),
    ],
)
@parameterize_batch_for_data_sources(
    data_source_configs=[PostgreSQLDatasourceTestConfig()],
    data=DATA,
)
def test_unresolved_type_name_is_a_plain_failure_postgresql(
    batch_for_datasource: Batch, type_: str
) -> None:
    """Aliases are not resolved and unknown names do not raise: the observed value names the
    type to use instead."""
    result = batch_for_datasource.validate(
        gxe.ExpectColumnTypeToBe(column=INTEGER_COLUMN, type_=type_)
    )
    assert not result.success
    assert result.exception_info["raised_exception"] is False
    assert result.result == {"observed_value": "INTEGER"}


@parameterize_batch_for_data_sources(
    data_source_configs=[
        MySQLDatasourceTestConfig(),
        PostgreSQLDatasourceTestConfig(),
        SqliteDatasourceTestConfig(),
    ],
    data=DATA,
)
def test_observed_type_name_must_match_exactly(batch_for_datasource: Batch) -> None:
    """A name that is only a prefix of the observed type name does not match it."""
    result = batch_for_datasource.validate(
        gxe.ExpectColumnTypeToBe(column=INTEGER_COLUMN, type_="INT")
    )
    assert not result.success
    assert result.exception_info["raised_exception"] is False
    assert result.result == {"observed_value": "INTEGER"}


@parameterize_batch_for_data_sources(
    data_source_configs=[
        MySQLDatasourceTestConfig(column_types={JSON_COLUMN: sqlatypes.JSON}),
        SqliteDatasourceTestConfig(column_types={JSON_COLUMN: sqlatypes.JSON}),
    ],
    data=JSON_DATA,
)
def test_lower_case_json_matches_json_column(batch_for_datasource: Batch) -> None:
    """The dialect modules export a `json` submodule, which must not be taken for the type."""
    result = batch_for_datasource.validate(
        gxe.ExpectColumnTypeToBe(column=JSON_COLUMN, type_="json")
    )
    assert result.success
    assert result.result == {"observed_value": "JSON"}


@parameterize_batch_for_data_sources(
    data_source_configs=[
        PandasDataFrameDatasourceTestConfig(),
        SqliteDatasourceTestConfig(),
        MySQLDatasourceTestConfig(),
        PostgreSQLDatasourceTestConfig(),
        SparkFilesystemCsvDatasourceTestConfig(column_types=SPARK_COLUMN_TYPES),
    ],
    data=DATA,
)
def test_column_name_is_resolved_case_insensitively(batch_for_datasource: Batch) -> None:
    """A column is found under a differently-cased name, as for other column expectations."""
    execution_engine = batch_for_datasource.data.execution_engine
    if isinstance(execution_engine, SqlAlchemyExecutionEngine):
        type_ = "INTEGER"
    elif isinstance(execution_engine, SparkDFExecutionEngine):
        type_ = "IntegerType"
    else:
        type_ = "object"

    result = batch_for_datasource.validate(
        gxe.ExpectColumnTypeToBe(column=INTEGER_COLUMN.upper(), type_=type_)
    )
    assert result.success
    assert result.exception_info["raised_exception"] is False


@parameterize_batch_for_data_sources(
    data_source_configs=[
        MySQLDatasourceTestConfig(),
        SqliteDatasourceTestConfig(),
    ],
    data=DATA,
)
def test_unknown_type_name_is_a_plain_failure(batch_for_datasource: Batch) -> None:
    result = batch_for_datasource.validate(
        gxe.ExpectColumnTypeToBe(column=INTEGER_COLUMN, type_="NOT_A_TYPE")
    )
    assert not result.success
    assert result.exception_info["raised_exception"] is False
    assert set(result.result) == {"observed_value"}
    assert result.result["observed_value"] is not None


@parameterize_batch_for_data_sources(
    data_source_configs=[
        SqliteDatasourceTestConfig(),
    ],
    data=DATA,
)
def test_known_type_mismatch_sqlite(batch_for_datasource: Batch) -> None:
    result = batch_for_datasource.validate(
        gxe.ExpectColumnTypeToBe(column=STRING_COLUMN, type_="INTEGER")
    )
    assert not result.success
    assert result.exception_info["raised_exception"] is False


@pytest.mark.parametrize(
    "suite_param_value,expected_result",
    [
        pytest.param("int64", True, id="success"),
        pytest.param("float64", False, id="failure"),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=TYPED_DATA)
def test_success_with_suite_param_type_(
    batch_for_datasource: Batch, suite_param_value: str, expected_result: bool
) -> None:
    suite_param_key = "test_expect_column_type_to_be"
    expectation = gxe.ExpectColumnTypeToBe(
        column=INTEGER_COLUMN,
        type_={"$PARAMETER": suite_param_key},
        result_format=ResultFormat.SUMMARY,
    )
    result = batch_for_datasource.validate(
        expectation, expectation_parameters={suite_param_key: suite_param_value}
    )
    assert result.success == expected_result


@parameterize_batch_for_data_sources(
    data_source_configs=JUST_PANDAS_DATA_SOURCES,
    data=TYPED_DATA,
)
def test_known_type_mismatch_returns_false(batch_for_datasource: Batch) -> None:
    """Known pandas type on a non-matching column is a plain failure, not an error."""
    result = batch_for_datasource.validate(
        gxe.ExpectColumnTypeToBe(column=INTEGER_COLUMN, type_="float64")
    )
    assert not result.success
    assert result.exception_info["raised_exception"] is False


@parameterize_batch_for_data_sources(
    data_source_configs=JUST_PANDAS_DATA_SOURCES,
    data=TYPED_DATA,
)
def test_unknown_type_raises_exception_info(batch_for_datasource: Batch) -> None:
    """Unknown type_ surfaces as exception_info, not a plain type mismatch."""
    expectation = gxe.ExpectColumnTypeToBe(column=INTEGER_COLUMN, type_="NUMBER")
    result = batch_for_datasource.validate(expectation)
    assert not result.success
    assert result.exception_info["raised_exception"] is True


@parameterize_batch_for_data_sources(
    data_source_configs=[
        PandasDataFrameDatasourceTestConfig(),
        SqliteDatasourceTestConfig(),
        PostgreSQLDatasourceTestConfig(),
        SparkFilesystemCsvDatasourceTestConfig(column_types=SPARK_COLUMN_TYPES),
    ],
    data=DATA,
)
def test_missing_column_raises(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnTypeToBe(column="non_existent_column", type_="INTEGER")
    result = batch_for_datasource.validate(expectation)
    assert not result.success
    assert result.exception_info["raised_exception"] is True
    assert (
        'The column "non_existent_column" in BatchData does not exist'
        in result.exception_info["exception_message"]
    )
