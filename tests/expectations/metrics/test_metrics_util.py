from typing import List

import pytest
from _pytest import monkeypatch

from great_expectations.data_context.util import file_relative_path

try:
    import sqlalchemy as sa
    from sqlalchemy import Column, Integer, String, select
    from sqlalchemy.orm import declarative_base
except ImportError:
    sa = None
    Column = None
    Integer = None
    String = None
    select = None
    declarative_base = None


from great_expectations.execution_engine import SqlAlchemyExecutionEngine
from great_expectations.expectations.metrics.util import (
    sql_statement_with_post_compile_to_string,
)
from tests.test_utils import (
    get_awsathena_connection_url,
    get_bigquery_connection_url,
    get_default_mssql_url,
    get_default_mysql_url,
    get_default_postgres_url,
    get_default_trino_url,
    get_redshift_connection_url,
    get_snowflake_connection_url,
)

# The following class allows for declarative instantiation of base class for SqlAlchemy. Adopted from
# https://docs.sqlalchemy.org/en/14/faq/sqlexpressions.html#rendering-postcompile-parameters-as-bound-parameters

Base = declarative_base()


class A(Base):
    __tablename__ = "a"
    id = Column(Integer, primary_key=True)
    data = Column(String)


def select_with_post_compile_statements() -> "sqlalchemy.sql.Select":
    test_id: str = "00000000"
    return select(A).where(A.data == test_id)


def _compare_select_statement_with_converted_string(engine) -> None:
    """
    Helper method used to do the call to sql_statement_with_post_compile_to_string() and compare with expected val
    Args:
        engine (ExecutionEngine): SqlAlchemyExecutionEngine with connection to backend under test
    """
    select_statement: "sqlalchemy.sql.Select" = select_with_post_compile_statements()
    returned_string = sql_statement_with_post_compile_to_string(
        engine=engine, select_statement=select_statement
    )
    assert returned_string == (
        "SELECT a.id, a.data \n" "FROM a \n" "WHERE a.data = '00000000'"
    )


@pytest.mark.unit
@pytest.mark.parametrize(
    "backend_name,connection_string",
    [
        (
            "sqlite",
            f"sqlite:///{file_relative_path(__file__, '../../test_sets/metrics_test.db')}",
        ),
        ("postgresql", get_default_postgres_url()),
        ("mysql", get_default_mysql_url()),
        ("mssql", get_default_mssql_url()),
        ("trino", get_default_trino_url()),
        ("redshift", get_redshift_connection_url()),
        ("snowflake", get_snowflake_connection_url()),
    ],
)
def test_sql_statement_conversion_to_string_for_backends(
    backend_name: str, connection_string: str, test_backends: List[str]
):

    if backend_name in test_backends:
        engine = SqlAlchemyExecutionEngine(connection_string=connection_string)
        _compare_select_statement_with_converted_string(engine=engine)
    else:
        pytest.skip(f"skipping sql statement conversion test for : {backend_name}")


@pytest.mark.unit
def test_sql_statement_conversion_to_string_awsathena(test_backends):
    if "awsathena" in test_backends:
        monkeypatch.setenv("ATHENA_STAGING_S3", "s3://test-staging/")
        monkeypatch.setenv("ATHENA_DB_NAME", "test_db_name")
        monkeypatch.setenv("ATHENA_TEN_TRIPS_DB_NAME", "test_ten_trips_db_name")
        connection_string = get_awsathena_connection_url()
        engine = SqlAlchemyExecutionEngine(connection_string=connection_string)
        _compare_select_statement_with_converted_string(engine=engine)
    else:
        pytest.skip(f"skipping sql statement conversion test for : awsathena")


@pytest.mark.unit
def test_sql_statement_conversion_to_string_bigquery(test_backends):
    """
    Bigquery backend returns a slightly different query
    """
    if "bigquery" in test_backends:
        monkeypatch.setenv("GE_TEST_GCP_PROJECT", "ge-oss")
        connection_string = get_bigquery_connection_url()
        engine = SqlAlchemyExecutionEngine(connection_string=connection_string)
        select_statement: "sqlalchemy.sql.Select" = (
            select_with_post_compile_statements()
        )
        returned_string = sql_statement_with_post_compile_to_string(
            engine=engine, select_statement=select_statement
        )
        assert returned_string == (
            "SELECT `a`.`id`, `a`.`data` \n"
            "FROM `a` \n"
            "WHERE `a`.`data` = '00000000'"
        )
    else:
        pytest.skip(f"skipping sql statement conversion test for : bigquery")
