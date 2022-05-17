import datetime
import os
from typing import List

import pandas as pd
import pytest
from dateutil.parser import parse

from great_expectations.core.batch_spec import SqlAlchemyDatasourceBatchSpec
from great_expectations.core.id_dict import BatchSpec
from great_expectations.data_context.util import file_relative_path
from great_expectations.execution_engine import SqlAlchemyExecutionEngine
from great_expectations.execution_engine.split_and_sample.sqlalchemy_data_sampler import (
    SqlAlchemyDataSampler,
)
from great_expectations.execution_engine.sqlalchemy_batch_data import (
    SqlAlchemyBatchData,
)
from great_expectations.execution_engine.sqlalchemy_dialect import GESqlDialect
from great_expectations.self_check.util import build_sa_engine
from great_expectations.util import import_library_module


@pytest.mark.parametrize(
    "underscore_prefix",
    [
        pytest.param("_", id="underscore prefix"),
        pytest.param("", id="no underscore prefix"),
    ],
)
@pytest.mark.parametrize(
    "sampler_method_name",
    [
        pytest.param(sampler_method_name, id=sampler_method_name)
        for sampler_method_name in [
            "sample_using_limit",
            "sample_using_random",
            "sample_using_mod",
            "sample_using_a_list",
            "sample_using_md5",
        ]
    ],
)
def test_get_sampler_method(underscore_prefix: str, sampler_method_name: str):
    """What does this test and why?

    This test is to ensure that the sampler methods are accessible with and without underscores.
    When new sampling methods are added, the parameter list should be updated.
    """
    data_splitter: SqlAlchemyDataSampler = SqlAlchemyDataSampler()

    sampler_method_name_with_prefix = f"{underscore_prefix}{sampler_method_name}"

    assert data_splitter.get_sampler_method(sampler_method_name_with_prefix) == getattr(
        data_splitter, sampler_method_name
    )


def clean_query_for_comparison(query_string: str) -> str:
    """Remove whitespace and case from query for easy comparison.

    Args:
        query_string: query string to convert.

    Returns:
        String with removed whitespace and converted to lowercase.
    """
    """Remove """
    return query_string.replace("\n", "").replace("\t", "").replace(" ", "").lower()


@pytest.fixture
def dialect_name_to_sql_statement():
    def _dialect_name_to_sql_statement(dialect_name: GESqlDialect) -> str:
        DIALECT_NAME_TO_SQL_STATEMENT: dict = {
            GESqlDialect.POSTGRESQL: "SELECT * FROM TEST_SCHEMA_NAME.TEST_TABLE WHERE TRUE LIMIT 10",
            GESqlDialect.MYSQL: "SELECT * FROM TEST_SCHEMA_NAME.TEST_TABLE WHERE TRUE = 1 LIMIT 10",
            GESqlDialect.ORACLE: "SELECT * FROM test_schema_name.test_table WHERE 1 = 1 AND ROWNUM <= 10",
            GESqlDialect.MSSQL: "SELECT TOP 10 * FROM TEST_SCHEMA_NAME.TEST_TABLE WHERE 1 = 1",
            GESqlDialect.SQLITE: "SELECT * FROM TEST_SCHEMA_NAME.TEST_TABLE WHERE 1 = 1 LIMIT 10 OFFSET 0",
            GESqlDialect.BIGQUERY: "SELECT * FROM `TEST_SCHEMA_NAME`.`TEST_TABLE` WHERE TRUE LIMIT 10",
            GESqlDialect.SNOWFLAKE: "SELECT * FROM TEST_SCHEMA_NAME.TEST_TABLE WHERE TRUE LIMIT 10",
            GESqlDialect.REDSHIFT: "SELECT * FROM TEST_SCHEMA_NAME.TEST_TABLE WHERE TRUE LIMIT 10",
            GESqlDialect.AWSATHENA: 'SELECT * FROM "TEST_SCHEMA_NAME"."TEST_TABLE" WHERE TRUE LIMIT 10',
            GESqlDialect.DREMIO: 'SELECT * FROM "TEST_SCHEMA_NAME"."TEST_TABLE" WHERE 1 = 1 LIMIT 10',
            GESqlDialect.TERADATASQL: "SELECT TOP 10 * FROM TEST_SCHEMA_NAME.TEST_TABLE WHERE 1 = 1",
            GESqlDialect.TRINO: "SELECT * FROM TEST_SCHEMA_NAME.TEST_TABLE WHERE TRUE LIMIT 10",
        }
        return DIALECT_NAME_TO_SQL_STATEMENT[dialect_name]

    return _dialect_name_to_sql_statement


@pytest.mark.parametrize(
    "dialect_name",
    [
        pytest.param(
            dialect_name, id=dialect_name.value, marks=pytest.mark.external_sqldialect
        )
        for dialect_name in GESqlDialect.get_all_dialects()
    ],
)
def test_sample_using_limit_builds_correct_query_where_clause_none(
    dialect_name: GESqlDialect, dialect_name_to_sql_statement, sa
):
    """What does this test and why?

    split_on_limit should build the appropriate query based on input parameters.
    This tests dialects that differ from the standard dialect, not each dialect exhaustively.
    """

    # 1. Setup
    class MockSqlAlchemyExecutionEngine:
        def __init__(self, dialect_name: GESqlDialect):
            self._dialect_name = dialect_name
            self._connection_string = self.dialect_name_to_connection_string(
                dialect_name
            )

        DIALECT_TO_CONNECTION_STRING_STUB: dict = {
            GESqlDialect.POSTGRESQL: "postgresql://",
            GESqlDialect.MYSQL: "mysql+pymysql://",
            GESqlDialect.ORACLE: "oracle+cx_oracle://",
            GESqlDialect.MSSQL: "mssql+pyodbc://",
            GESqlDialect.SQLITE: "sqlite:///",
            GESqlDialect.BIGQUERY: "bigquery://",
            GESqlDialect.SNOWFLAKE: "snowflake://",
            GESqlDialect.REDSHIFT: "redshift+psycopg2://",
            GESqlDialect.AWSATHENA: f"awsathena+rest://@athena.us-east-1.amazonaws.com/some_test_db?s3_staging_dir=s3://some-s3-path/",
            GESqlDialect.DREMIO: "dremio://",
            GESqlDialect.TERADATASQL: "teradatasql://",
            GESqlDialect.TRINO: "trino://",
        }

        @property
        def dialect_name(self) -> str:
            return self._dialect_name.value

        def dialect_name_to_connection_string(self, dialect_name: GESqlDialect) -> str:
            return self.DIALECT_TO_CONNECTION_STRING_STUB.get(dialect_name)

        _BIGQUERY_MODULE_NAME = "sqlalchemy_bigquery"

        @property
        def dialect(self) -> sa.engine.Dialect:
            # TODO: AJB 20220512 move this dialect retrieval to a separate class from the SqlAlchemyExecutionEngine
            #  and then use it here.
            dialect_name: GESqlDialect = self._dialect_name
            if dialect_name == GESqlDialect.ORACLE:
                return import_library_module(
                    module_name="sqlalchemy.dialects.oracle"
                ).dialect()
            elif dialect_name == GESqlDialect.SNOWFLAKE:
                return import_library_module(
                    module_name="snowflake.sqlalchemy.snowdialect"
                ).dialect()
            elif dialect_name == GESqlDialect.DREMIO:
                # WARNING: Dremio Support is experimental, functionality is not fully under test
                return import_library_module(
                    module_name="sqlalchemy_dremio.pyodbc"
                ).dialect()
            # NOTE: AJB 20220512 Redshift dialect is not yet fully supported.
            # The below throws an `AttributeError: type object 'RedshiftDialect_psycopg2' has no attribute 'positional'`
            # elif dialect_name == "redshift":
            #     return import_library_module(
            #         module_name="sqlalchemy_redshift.dialect"
            #     ).RedshiftDialect
            elif dialect_name == GESqlDialect.BIGQUERY:
                return import_library_module(
                    module_name=self._BIGQUERY_MODULE_NAME
                ).dialect()
            elif dialect_name == GESqlDialect.TERADATASQL:
                # WARNING: Teradata Support is experimental, functionality is not fully under test
                return import_library_module(
                    module_name="teradatasqlalchemy.dialect"
                ).dialect()
            else:
                return sa.create_engine(self._connection_string).dialect

    mock_execution_engine: MockSqlAlchemyExecutionEngine = (
        MockSqlAlchemyExecutionEngine(dialect_name=dialect_name)
    )

    data_sampler: SqlAlchemyDataSampler = SqlAlchemyDataSampler()

    # 2. Create query using sampler
    table_name: str = "test_table"
    batch_spec: BatchSpec = BatchSpec(
        table_name=table_name,
        schema_name="test_schema_name",
        sampling_method="sample_using_limit",
        sampling_kwargs={"n": 10},
    )
    query = data_sampler.sample_using_limit(
        execution_engine=mock_execution_engine, batch_spec=batch_spec, where_clause=None
    )

    if not isinstance(query, str):
        query_str: str = clean_query_for_comparison(
            str(
                query.compile(
                    dialect=mock_execution_engine.dialect,
                    compile_kwargs={"literal_binds": True},
                )
            )
        )
    else:
        query_str: str = clean_query_for_comparison(query)

    expected: str = clean_query_for_comparison(
        dialect_name_to_sql_statement(dialect_name)
    )

    assert query_str == expected


@pytest.mark.integration
def test_sqlite_sample_using_limit(sa):

    csv_path: str = file_relative_path(
        os.path.dirname(os.path.dirname(__file__)),
        os.path.join(
            "test_sets",
            "taxi_yellow_tripdata_samples",
            "ten_trips_from_each_month",
            "yellow_tripdata_sample_10_trips_from_each_month.csv",
        ),
    )
    df: pd.DataFrame = pd.read_csv(csv_path)
    engine: SqlAlchemyExecutionEngine = build_sa_engine(df, sa)

    n: int = 10
    batch_spec: SqlAlchemyDatasourceBatchSpec = SqlAlchemyDatasourceBatchSpec(
        table_name="test",
        schema_name="main",
        sampling_method="sample_using_limit",
        sampling_kwargs={"n": n},
    )
    batch_data: SqlAlchemyBatchData = engine.get_batch_data(batch_spec=batch_spec)

    # Right number of rows?
    num_rows: int = batch_data.execution_engine.engine.execute(
        sa.select([sa.func.count()]).select_from(batch_data.selectable)
    ).scalar()
    assert num_rows == n

    # Right rows?
    rows: sa.Row = batch_data.execution_engine.engine.execute(
        sa.select([sa.text("*")]).select_from(batch_data.selectable)
    ).fetchall()

    row_dates: List[datetime.datetime] = [parse(row["pickup_datetime"]) for row in rows]
    for row_date in row_dates:
        assert row_date.month == 1
        assert row_date.year == 2018
