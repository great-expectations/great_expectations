import pandas as pd
import pytest
import sqlalchemy.types as sqltypes
from packaging import version

import great_expectations.expectations as gxe
from great_expectations.compatibility.aws import REDSHIFT_TYPES
from great_expectations.compatibility.databricks import DATABRICKS_TYPES
from great_expectations.compatibility.postgresql import POSTGRESQL_TYPES
from great_expectations.compatibility.snowflake import SNOWFLAKE_TYPES
from great_expectations.compatibility.sqlalchemy import (
    sqlalchemy as sa,
)
from great_expectations.core.result_format import ResultFormat
from great_expectations.datasource.fluent.interfaces import Batch
from tests.integration.conftest import parameterize_batch_for_data_sources
from tests.integration.data_sources_and_expectations.data_source_lists import (
    JUST_PANDAS_DATA_SOURCES,
)
from tests.integration.test_utils.data_source_config import (
    ALL_DATA_SOURCES,
    DatabricksDatasourceTestConfig,
    PandasDataFrameDatasourceTestConfig,
    PostgreSQLDatasourceTestConfig,
    RedshiftDatasourceTestConfig,
    SnowflakeDatasourceTestConfig,
)

INTEGER_COLUMN = "integers"
INTEGER_AND_NULL_COLUMN = "integers_and_nulls"
STRING_COLUMN = "strings"


DATA = pd.DataFrame(
    {
        INTEGER_COLUMN: [1, 2, 3, 4, 5],
        INTEGER_AND_NULL_COLUMN: [1, 2, 3, 4, None],
        STRING_COLUMN: ["a", "b", "c", "d", "e"],
    },
    dtype="object",
)

PASSING_DATA_SOURCES_EXCEPT_DATA_FRAMES = [
    ds
    for ds in ALL_DATA_SOURCES
    if not isinstance(
        ds,
        (
            PandasDataFrameDatasourceTestConfig,
            SnowflakeDatasourceTestConfig,
            DatabricksDatasourceTestConfig,
        ),
    )
]


@parameterize_batch_for_data_sources(
    data_source_configs=PASSING_DATA_SOURCES_EXCEPT_DATA_FRAMES, data=DATA
)
def test_success_complete(batch_for_datasource: Batch) -> None:
    type_list = [
        "INTEGER",
        "Integer",
        "int",
        "int64",
        "int32",
        "IntegerType",
        "_CUSTOM_DECIMAL",
    ]
    expectation = gxe.ExpectColumnValuesToBeInTypeList(column=INTEGER_COLUMN, type_list=type_list)
    result = batch_for_datasource.validate(expectation, result_format=ResultFormat.COMPLETE)
    result_dict = result.to_json_dict()["result"]

    assert result.success
    assert isinstance(result_dict, dict)
    assert result_dict["observed_value"] in type_list


@parameterize_batch_for_data_sources(
    data_source_configs=[PandasDataFrameDatasourceTestConfig()], data=DATA
)
def test_success_complete_pandas(batch_for_datasource: Batch) -> None:
    type_list = ["INTEGER", "int", "int64", "int32", "IntegerType"]
    expectation = gxe.ExpectColumnValuesToBeInTypeList(column=INTEGER_COLUMN, type_list=type_list)
    result = batch_for_datasource.validate(expectation, result_format=ResultFormat.COMPLETE)

    assert result.success


@pytest.mark.parametrize(
    "expectation",
    [
        pytest.param(
            gxe.ExpectColumnValuesToBeInTypeList(column=INTEGER_COLUMN, type_list=["int"]),
            id="integer_types",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToBeInTypeList(
                column=INTEGER_AND_NULL_COLUMN, type_list=["int", "float64"], mostly=0.8
            ),
            id="mostly",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToBeInTypeList(column=STRING_COLUMN, type_list=["str"]),
            id="string_types",
        ),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_success(
    batch_for_datasource: Batch,
    expectation: gxe.ExpectColumnValuesToBeInTypeList,
) -> None:
    result = batch_for_datasource.validate(expectation)
    assert result.success


@pytest.mark.parametrize(
    "expectation",
    [
        pytest.param(
            gxe.ExpectColumnValuesToBeInTypeList(column=INTEGER_COLUMN, type_list=["str"]),
            id="wrong_type",
        ),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_failure(
    batch_for_datasource: Batch,
    expectation: gxe.ExpectColumnValuesToBeInTypeList,
) -> None:
    result = batch_for_datasource.validate(expectation)
    assert not result.success


@pytest.mark.parametrize(
    "expectation",
    [
        pytest.param(
            gxe.ExpectColumnValuesToBeInTypeList(
                column="CHARACTER", type_list=["CHARACTER", "VARCHAR(1)"]
            ),
            id="CHARACTER",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToBeInTypeList(
                column="DEC", type_list=["DEC", "DECIMAL", "DECIMAL(38, 0)"]
            ),
            id="DEC",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToBeInTypeList(
                column="FIXED", type_list=["FIXED", "DECIMAL", "DECIMAL(38, 0)"]
            ),
            id="FIXED",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToBeInTypeList(column="GEOGRAPHY", type_list=["GEOGRAPHY"]),
            id="GEOGRAPHY",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToBeInTypeList(column="GEOMETRY", type_list=["GEOMETRY"]),
            id="GEOMETRY",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToBeInTypeList(
                column="NUMBER",
                type_list=[
                    "NUMBER",
                    "DECIMAL",
                    "NUMERIC",
                    "DECIMAL(38, 0)",  # 38, 0 is the default precision and scale
                ],
            ),
            id="NUMBER",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToBeInTypeList(
                column="STRING", type_list=["STRING", "VARCHAR", "VARCHAR(16777216)"]
            ),
            id="STRING",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToBeInTypeList(
                column="TEXT", type_list=["TEXT", "VARCHAR", "VARCHAR(16777216)"]
            ),
            id="TEXT",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToBeInTypeList(
                column="TIMESTAMP_LTZ", type_list=["TIMESTAMP_LTZ"]
            ),
            id="TIMESTAMP_LTZ",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToBeInTypeList(
                column="TIMESTAMP_NTZ", type_list=["TIMESTAMP_NTZ"]
            ),
            id="TIMESTAMP_NTZ",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToBeInTypeList(column="TIMESTAMP_TZ", type_list=["TIMESTAMP_TZ"]),
            id="TIMESTAMP_TZ",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToBeInTypeList(
                column="VARBINARY", type_list=["VARBINARY", "BINARY", "BINARY(8388608)"]
            ),
            id="VARBINARY",
        ),
        # INT , INTEGER , BIGINT , SMALLINT , TINYINT , BYTEINT are Synonymous with NUMBER,
        # except that precision and scale cannot be specified (i.e. always defaults to NUMBER(38, 0)
        pytest.param(
            gxe.ExpectColumnValuesToBeInTypeList(column="BYTEINT", type_list=["DECIMAL(38, 0)"]),
            id="BYTEINT",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToBeInTypeList(column="TINYINT", type_list=["DECIMAL(38, 0)"]),
            id="TINYINT",
        ),
        # Complex data types which are not hashable by testing framework currently
        # pytest.param(
        #     gxe.ExpectColumnValuesToBeInTypeList(
        #         column="VARIANT", type_list=["VARIANT"]
        #     ),
        #     id="VARIANT",
        # ),
        # pytest.param(
        #     gxe.ExpectColumnValuesToBeInTypeList(column="OBJECT", type_list=["OBJECT"]),
        #     id="OBJECT",
        # ),
        # pytest.param(
        #     gxe.ExpectColumnValuesToBeInTypeList(column="ARRAY", type_list=["ARRAY"]),
        #     id="ARRAY",
        # ),
        # These sqlachemy types map to _CUSTOM_* types in snowflake-sqlalchemy
        pytest.param(
            gxe.ExpectColumnValuesToBeInTypeList(column="_CUSTOM_Date", type_list=["DATE"]),
            id="_CUSTOM_Date",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToBeInTypeList(
                column="_CUSTOM_DateTime", type_list=["TIMESTAMP_NTZ"]
            ),
            id="_CUSTOM_DateTime",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToBeInTypeList(column="_CUSTOM_Time", type_list=["TIME"]),
            id="_CUSTOM_Time",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToBeInTypeList(column="_CUSTOM_Float", type_list=["FLOAT"]),
            id="_CUSTOM_Float",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToBeInTypeList(
                column="_CUSTOM_DECIMAL", type_list=["INTEGER", "DECIMAL(38, 0)"]
            ),
            id="_CUSTOM_DECIMAL",
        ),
    ],
)
@parameterize_batch_for_data_sources(
    data_source_configs=[
        SnowflakeDatasourceTestConfig(
            column_types={
                "ARRAY": SNOWFLAKE_TYPES.ARRAY,
                "BYTEINT": SNOWFLAKE_TYPES.BYTEINT,
                "CHARACTER": SNOWFLAKE_TYPES.CHARACTER,
                "DEC": SNOWFLAKE_TYPES.DEC,
                "FIXED": SNOWFLAKE_TYPES.FIXED,
                "GEOGRAPHY": SNOWFLAKE_TYPES.GEOGRAPHY,
                "GEOMETRY": SNOWFLAKE_TYPES.GEOMETRY,
                "NUMBER": SNOWFLAKE_TYPES.NUMBER,
                "OBJECT": SNOWFLAKE_TYPES.OBJECT,
                "STRING": SNOWFLAKE_TYPES.STRING,
                "TEXT": SNOWFLAKE_TYPES.TEXT,
                "TIMESTAMP_LTZ": SNOWFLAKE_TYPES.TIMESTAMP_LTZ,
                "TIMESTAMP_NTZ": SNOWFLAKE_TYPES.TIMESTAMP_NTZ,
                "TIMESTAMP_TZ": SNOWFLAKE_TYPES.TIMESTAMP_TZ,
                "TINYINT": SNOWFLAKE_TYPES.TINYINT,
                "VARBINARY": SNOWFLAKE_TYPES.VARBINARY,
                "VARIANT": SNOWFLAKE_TYPES.VARIANT,
                # These sqlachemy types map to _CUSTOM_* types in snowflake-sqlalchemy
                "_CUSTOM_Date": sqltypes.Date,
                "_CUSTOM_DateTime": sqltypes.DateTime,
                "_CUSTOM_Time": sqltypes.Time,
                "_CUSTOM_Float": sqltypes.Float,
                "_CUSTOM_DECIMAL": sqltypes.INTEGER,
            }
        )
    ],
    data=pd.DataFrame(
        {
            "BYTEINT": [1, 2, 3],
            "CHARACTER": ["a", "b", "c"],
            "DEC": [1.0, 2.0, 3.0],
            "FIXED": [1.0, 2.0, 3.0],
            "GEOGRAPHY": ["POINT(1 1)", "POINT(2 2)", "POINT(3 3)"],
            "GEOMETRY": ["POINT(1 1)", "POINT(2 2)", "POINT(3 3)"],
            "NUMBER": [1, 2, 3],
            "STRING": ["a", "b", "c"],
            "TEXT": ["a", "b", "c"],
            "TIMESTAMP_LTZ": [
                "2021-01-01 00:00:00",
                "2021-01-02 00:00:00",
                "2021-01-03 00:00:00",
            ],
            "TIMESTAMP_NTZ": [
                "2021-01-01 00:00:00",
                "2021-01-02 00:00:00",
                "2021-01-03 00:00:00",
            ],
            "TIMESTAMP_TZ": [
                "2021-01-01 00:00:00",
                "2021-01-02 00:00:00",
                "2021-01-03 00:00:00",
            ],
            "TINYINT": [1, 2, 3],
            "VARBINARY": [b"1", b"2", b"3"],
            # Complex data types which are not hashable by testing framework currently
            # "ARRAY": pd.Series([[1, 2, 3], [4, 5, 6], [7, 8, 9]], dtype="object"),
            # "OBJECT": [{"a": 1}, {"b": 2}, {"c": 3}],
            # "VARIANT": [{"a": 1}, {"b": 2}, {"c": 3}],
            # These sqlachemy types map to _CUSTOM_* types in snowflake-sqlalchemy
            "_CUSTOM_Date": [
                # Date in isoformat
                "2021-01-01",
                "2021-01-02",
                "2021-01-03",
            ],
            "_CUSTOM_DateTime": [
                # isoformat with microseconds
                "2021-01-01 00:00:00.000000",
                "2021-01-02 00:00:00.000000",
                "2021-01-03 00:00:00.000000",
            ],
            "_CUSTOM_Time": [
                "00:00:00.878281",
                "01:00:00.000000",
                "00:10:43.000000",
            ],
            "_CUSTOM_Float": [1.0, 2.0, 3.0],
            "_CUSTOM_DECIMAL": [1, 2, 3],
        },
        dtype="object",
    ),
)
def test_success_complete_snowflake(
    batch_for_datasource: Batch, expectation: gxe.ExpectColumnValuesToBeInTypeList
) -> None:
    result = batch_for_datasource.validate(expectation, result_format=ResultFormat.COMPLETE)
    result_dict = result.to_json_dict()["result"]

    assert result.success
    assert isinstance(result_dict, dict)
    assert isinstance(result_dict["observed_value"], str)
    assert isinstance(expectation.type_list, list)
    assert result_dict["observed_value"] in expectation.type_list


@pytest.mark.parametrize(
    "expectation",
    [
        pytest.param(
            gxe.ExpectColumnValuesToBeInTypeList(column="CHAR", type_list=["CHAR", "CHAR(1)"]),
            id="CHAR",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToBeInTypeList(column="TEXT", type_list=["TEXT"]),
            id="TEXT",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToBeInTypeList(column="INTEGER", type_list=["INTEGER"]),
            id="INTEGER",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToBeInTypeList(column="SMALLINT", type_list=["SMALLINT"]),
            id="SMALLINT",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToBeInTypeList(column="BIGINT", type_list=["BIGINT"]),
            id="BIGINT",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToBeInTypeList(
                column="TIMESTAMP", type_list=["TIMESTAMP", "TIMESTAMP WITHOUT TIME ZONE"]
            ),
            id="TIMESTAMP",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToBeInTypeList(column="DATE", type_list=["DATE"]),
            id="DATE",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToBeInTypeList(
                column="DOUBLE_PRECISION", type_list=["DOUBLE PRECISION"]
            ),
            id="DOUBLE_PRECISION",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToBeInTypeList(column="BOOLEAN", type_list=["BOOLEAN"]),
            id="BOOLEAN",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToBeInTypeList(column="NUMERIC", type_list=["NUMERIC"]),
            id="NUMERIC",
        ),
    ],
)
@parameterize_batch_for_data_sources(
    data_source_configs=[
        PostgreSQLDatasourceTestConfig(
            column_types={
                "CHAR": POSTGRESQL_TYPES.CHAR,
                "TEXT": POSTGRESQL_TYPES.TEXT,
                "INTEGER": POSTGRESQL_TYPES.INTEGER,
                "SMALLINT": POSTGRESQL_TYPES.SMALLINT,
                "BIGINT": POSTGRESQL_TYPES.BIGINT,
                "TIMESTAMP": POSTGRESQL_TYPES.TIMESTAMP,
                "DATE": POSTGRESQL_TYPES.DATE,
                "DOUBLE_PRECISION": POSTGRESQL_TYPES.DOUBLE_PRECISION,
                "BOOLEAN": POSTGRESQL_TYPES.BOOLEAN,
                "NUMERIC": POSTGRESQL_TYPES.NUMERIC,
            }
        ),
    ],
    data=pd.DataFrame(
        {
            "CHAR": ["a", "b", "c"],
            "TEXT": ["a", "b", "c"],
            "INTEGER": [1, 2, 3],
            "SMALLINT": [1, 2, 3],
            "BIGINT": [1, 2, 3],
            "TIMESTAMP": [
                "2021-01-01 00:00:00",
                "2021-01-02 00:00:00",
                "2021-01-03 00:00:00",
            ],
            "DATE": [
                # Date in isoformat
                "2021-01-01",
                "2021-01-02",
                "2021-01-03",
            ],
            "DOUBLE_PRECISION": [1.0, 2.0, 3.0],
            "BOOLEAN": [False, False, True],
            "NUMERIC": [1, 2, 3],
        },
        dtype="object",
    ),
)
def test_success_complete_postgres(
    batch_for_datasource: Batch, expectation: gxe.ExpectColumnValuesToBeInTypeList
) -> None:
    result = batch_for_datasource.validate(expectation, result_format=ResultFormat.COMPLETE)
    result_dict = result.to_json_dict()["result"]

    assert result.success
    assert isinstance(result_dict, dict)
    assert isinstance(result_dict["observed_value"], str)
    assert isinstance(expectation.type_list, list)
    assert result_dict["observed_value"] in expectation.type_list


@pytest.mark.parametrize(
    "expectation",
    [
        pytest.param(
            gxe.ExpectColumnValuesToBeInTypeList(column="STRING", type_list=["STRING"]),
            id="STRING",
        ),
        # SqlA Text gets converted to Databricks STRING
        pytest.param(
            gxe.ExpectColumnValuesToBeInTypeList(column="TEXT", type_list=["STRING"]),
            id="TEXT",
        ),
        # SqlA UNICODE gets converted to Databricks STRING
        pytest.param(
            gxe.ExpectColumnValuesToBeInTypeList(column="UNICODE", type_list=["STRING"]),
            id="UNICODE",
        ),
        # SqlA UNICODE_TEXT gets converted to Databricks STRING
        pytest.param(
            gxe.ExpectColumnValuesToBeInTypeList(column="UNICODE_TEXT", type_list=["STRING"]),
            id="UNICODE_TEXT",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToBeInTypeList(column="BOOLEAN", type_list=["BOOLEAN"]),
            id="BOOLEAN",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToBeInTypeList(
                column="DECIMAL", type_list=["DECIMAL", "DECIMAL(10, 0)"]
            ),
            id="DECIMAL",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToBeInTypeList(column="DATE", type_list=["DATE"]),
            id="DATE",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToBeInTypeList(column="TIMESTAMP", type_list=["TIMESTAMP"]),
            id="TIMESTAMP",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToBeInTypeList(
                column="TIMESTAMP_NTZ", type_list=["TIMESTAMP_NTZ"]
            ),
            id="TIMESTAMP_NTZ",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToBeInTypeList(column="FLOAT", type_list=["FLOAT"]),
            id="FLOAT",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToBeInTypeList(column="INT", type_list=["INT"]),
            id="INT",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToBeInTypeList(column="TINYINT", type_list=["TINYINT"]),
            id="TINYINT",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToBeInTypeList(
                column="DECIMAL", type_list=["DECIMAL", "DECIMAL(10, 0)"]
            ),
            id="DECIMAL",
        ),
        # SqlA Time gets converted to Databricks STRING,
        # but is not supported by our testing framework
        # pytest.param(
        #     gxe.ExpectColumnValuesToBeInTypeList(column="TIME", type_list=["STRING"]),
        #     id="TIME",
        # ),
        # SqlA UUID gets converted to Databricks STRING,
        # but is not supported by our testing framework.
        # pytest.param(
        #     gxe.ExpectColumnValuesToBeInTypeList(column="UUID", type_list=["STRING"]),
        #     id="UUID",
        # )
    ],
)
@parameterize_batch_for_data_sources(
    data_source_configs=[
        DatabricksDatasourceTestConfig(
            column_types={
                "STRING": DATABRICKS_TYPES.STRING,
                "TEXT": sqltypes.Text,
                "UNICODE": sqltypes.Unicode,
                "UNICODE_TEXT": sqltypes.UnicodeText,
                "BIGINT": sqltypes.BigInteger,
                "BOOLEAN": sqltypes.BOOLEAN,
                "DATE": sqltypes.DATE,
                "TIMESTAMP_NTZ": DATABRICKS_TYPES.TIMESTAMP_NTZ,
                "TIMESTAMP": DATABRICKS_TYPES.TIMESTAMP,
                "FLOAT": sqltypes.Float,
                "INT": sqltypes.Integer,
                "DECIMAL": sqltypes.Numeric,
                "SMALLINT": sqltypes.SmallInteger,
                "TINYINT": DATABRICKS_TYPES.TINYINT,
                # "TIME": sqltypes.Time,
                # "UUID": sqltypes.UUID,
            }
        )
    ],
    data=pd.DataFrame(
        {
            "STRING": ["a", "b", "c"],
            "TEXT": ["a", "b", "c"],
            "UNICODE": ["\u00e9", "\u00e9", "\u00e9"],
            "UNICODE_TEXT": ["a", "b", "c"],
            "BIGINT": [1111, 2222, 3333],
            "BOOLEAN": [True, True, False],
            "DATE": [
                "2021-01-01",
                "2021-01-02",
                "2021-01-03",
            ],
            "TIMESTAMP_NTZ": [
                "2021-01-01 00:00:00",
                "2021-01-02 00:00:00",
                "2021-01-03 00:00:00",
            ],
            "TIMESTAMP": [
                "2021-01-01 00:00:00",
                "2021-01-02 00:00:00",
                "2021-01-03 00:00:00",
            ],
            "DOUBLE": [1.0, 2.0, 3.0],
            "FLOAT": [1.0, 2.0, 3.0],
            "INT": [1, 2, 3],
            "DECIMAL": [1.1, 2.2, 3.3],
            "SMALLINT": [1, 2, 3],
            # "TIME": [
            #     sa.Time("22:17:33.123456"),
            #     sa.Time("22:17:33.123456"),
            #     sa.Time("22:17:33.123456"),
            # ],
            # "UUID": [
            #      uuid.UUID("905993ea-f50e-4284-bea0-5be3f0ed7031"),
            #      uuid.UUID("9406b631-fa2f-41cf-b666-f9a2ac3118c1"),
            #      uuid.UUID("47538f05-32e3-4594-80e2-0b3b33257ae7")
            #  ],
        },
        dtype="object",
    ),
)
def test_success_complete_databricks(
    batch_for_datasource: Batch, expectation: gxe.ExpectColumnValuesToBeInTypeList
) -> None:
    result = batch_for_datasource.validate(expectation, result_format=ResultFormat.COMPLETE)
    result_dict = result.to_json_dict()["result"]

    assert result.success
    assert isinstance(result_dict, dict)
    assert isinstance(result_dict["observed_value"], str)
    assert isinstance(expectation.type_list, list)
    assert result_dict["observed_value"] in expectation.type_list


if version.parse(sa.__version__) >= version.parse("2.0.0"):
    # Note: why not use pytest.skip?
    # the import of `sqltypes.Double` is only possible in sqlalchemy >= 2.0.0
    # the import is done as part of the instantiation of the test, which includes
    # processing the pytest.skip() statement. This way, we skip the instantiation
    # of the test entirely.
    @pytest.mark.parametrize(
        "expectation",
        [
            pytest.param(
                gxe.ExpectColumnValuesToBeInTypeList(
                    column="DOUBLE", type_list=["DOUBLE", "FLOAT"]
                ),
                id="DOUBLE",
            )
        ],
    )
    @parameterize_batch_for_data_sources(
        data_source_configs=[
            DatabricksDatasourceTestConfig(
                column_types={
                    "DOUBLE": sqltypes.Double,
                }
            )
        ],
        data=pd.DataFrame(
            {
                "DOUBLE": [1.0, 2.0, 3.0],
            },
            dtype="object",
        ),
    )
    def test_success_complete_databricks_double_type_only(
        batch_for_datasource: Batch, expectation: gxe.ExpectColumnValuesToBeInTypeList
    ) -> None:
        """What does this test and why?

        Databricks mostly uses SqlA types directly, but the double type is
        only available after sqlalchemy 2.0. We therefore split up the test
        into 2 parts, with this test being skipped if the SA version is too low.
        """
        result = batch_for_datasource.validate(expectation, result_format=ResultFormat.COMPLETE)
        result_dict = result.to_json_dict()["result"]

        assert result.success
        assert isinstance(result_dict, dict)
        assert isinstance(result_dict["observed_value"], str)
        assert isinstance(expectation.type_list, list)
        assert result_dict["observed_value"] in expectation.type_list


# Redshift will lowercase column names so we make the column names here all lowercase
# These means we aren't case insensitive in this expectation.
@pytest.mark.parametrize(
    "expectation",
    [
        pytest.param(
            gxe.ExpectColumnValuesToBeInTypeList(column="char", type_list=["CHAR", "CHAR(1)"]),
            id="CHAR",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToBeInTypeList(column="text", type_list=["VARCHAR"]),
            id="TEXT",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToBeInTypeList(column="integer", type_list=["INTEGER"]),
            id="INTEGER",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToBeInTypeList(column="smallint", type_list=["SMALLINT"]),
            id="SMALLINT",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToBeInTypeList(column="bigint", type_list=["BIGINT"]),
            id="BIGINT",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToBeInTypeList(column="timestamp", type_list=["TIMESTAMP"]),
            id="TIMESTAMP",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToBeInTypeList(column="date", type_list=["DATE"]),
            id="DATE",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToBeInTypeList(
                column="double_precision", type_list=["DOUBLE_PRECISION"]
            ),
            id="DOUBLE_PRECISION",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToBeInTypeList(column="boolean", type_list=["BOOLEAN"]),
            id="BOOLEAN",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToBeInTypeList(column="numeric", type_list=["DECIMAL"]),
            id="NUMERIC",
        ),
    ],
)
@parameterize_batch_for_data_sources(
    data_source_configs=[
        RedshiftDatasourceTestConfig(
            column_types={
                "char": REDSHIFT_TYPES.CHAR,
                "text": REDSHIFT_TYPES.VARCHAR,
                "integer": REDSHIFT_TYPES.INTEGER,
                "smallint": REDSHIFT_TYPES.SMALLINT,
                "bigint": REDSHIFT_TYPES.BIGINT,
                "timestamp": REDSHIFT_TYPES.TIMESTAMP,
                "date": REDSHIFT_TYPES.DATE,
                "double_precision": REDSHIFT_TYPES.DOUBLE_PRECISION,
                "boolean": REDSHIFT_TYPES.BOOLEAN,
                "numeric": REDSHIFT_TYPES.DECIMAL,
            }
        ),
    ],
    data=pd.DataFrame(
        {
            "char": ["a", "b", "c"],
            "text": ["a", "b", "c"],
            "integer": [1, 2, 3],
            "smallint": [1, 2, 3],
            "bigint": [1, 2, 3],
            "timestamp": [
                "2021-01-01 00:00:00",
                "2021-01-02 00:00:00",
                "2021-01-03 00:00:00",
            ],
            "date": [
                # Date in isoformat
                "2021-01-01",
                "2021-01-02",
                "2021-01-03",
            ],
            "double_precision": [1.0, 2.0, 3.0],
            "boolean": [False, False, True],
            "numeric": [1, 2, 3],
        },
        dtype="object",
    ),
)
def test_success_complete_redshift(
    batch_for_datasource: Batch, expectation: gxe.ExpectColumnValuesToBeInTypeList
) -> None:
    result = batch_for_datasource.validate(expectation, result_format=ResultFormat.COMPLETE)
    result_dict = result.to_json_dict()["result"]

    assert result.success
    assert isinstance(result_dict, dict)
    assert isinstance(result_dict["observed_value"], str)
    assert isinstance(expectation.type_list, list)
    # We require the user to use DECIMAL, the redshift official type:
    # https://docs.aws.amazon.com/redshift/latest/dg/c_Supported_data_types.html
    # However, we see the string numeric in the sqlalchemy type output.
    if result_dict["observed_value"].lower() != "numeric":
        assert result_dict["observed_value"] in expectation.type_list
    else:
        assert "DECIMAL" in expectation.type_list


@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_include_unexpected_rows_pandas(batch_for_datasource: Batch) -> None:
    """Test include_unexpected_rows for ExpectColumnValuesToBeInTypeList with pandas."""
    expectation = gxe.ExpectColumnValuesToBeInTypeList(column=STRING_COLUMN, type_list=["int"])
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

    # Should contain 5 rows where STRING_COLUMN is not of type int (all string values)
    assert len(unexpected_rows_df) == 5

    # The unexpected rows should contain all the string values
    unexpected_values = sorted(unexpected_rows_df[STRING_COLUMN].tolist())
    assert unexpected_values == ["a", "b", "c", "d", "e"]
