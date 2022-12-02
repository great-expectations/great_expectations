import os

import pytest
from sqlalchemy import Column, Integer, String, select
from sqlalchemy.orm import declarative_base

from great_expectations.data_context.util import file_relative_path
from great_expectations.execution_engine import SqlAlchemyExecutionEngine
from great_expectations.expectations.metrics.util import (
    sql_statement_with_post_compile_to_string,
)

# https://docs.sqlalchemy.org/en/14/faq/sqlexpressions.html#rendering-bound-parameters-inline
from tests.test_utils import (
    get_awsathena_connection_url,
    get_bigquery_connection_url,
    get_redshift_connection_url,
    get_snowflake_connection_url,
)

# allows for declarative instantiation of base
Base = declarative_base()

# https://docs.sqlalchemy.org/en/14/faq/sqlexpressions.html#rendering-postcompile-parameters-as-bound-parameters
class A(Base):
    __tablename__ = "a"
    id = Column(Integer, primary_key=True)
    data = Column(String)


@pytest.fixture
def select_with_post_compile_statements(sa):
    id: str = "00000000"
    stmt: select = select(A).where(A.data == id)
    return stmt


def _compare_select_statement_with_converted_string(engine, select_statement):

    returned_string = sql_statement_with_post_compile_to_string(
        engine=engine, select_statement=select_statement
    )
    assert returned_string == (
        "SELECT a.id, a.data \n" "FROM a \n" "WHERE a.data = '00000000'"
    )


def test_sqlite(sa, test_backends, select_with_post_compile_statements):
    if "sqlite" in test_backends:
        sqlite_path = file_relative_path(__file__, "../../test_sets/metrics_test.db")
        connection_string = f"sqlite:///{sqlite_path}"
        engine = SqlAlchemyExecutionEngine(connection_string=connection_string)
        _compare_select_statement_with_converted_string(
            engine=engine, select_statement=select_with_post_compile_statements
        )
    else:
        assert False


def test_postgresql(sa, test_backends, select_with_post_compile_statements):
    if "postgresql" in test_backends:
        connection_string = "postgresql+psycopg2://postgres:@localhost/test_ci"
        engine = SqlAlchemyExecutionEngine(connection_string=connection_string)
        _compare_select_statement_with_converted_string(
            engine=engine, select_statement=select_with_post_compile_statements
        )
    else:
        assert False


def test_mysql(sa, test_backends, select_with_post_compile_statements):
    if "mysql" in test_backends:
        connection_string = "mysql+pymysql://root@localhost/test_ci"
        engine = SqlAlchemyExecutionEngine(connection_string=connection_string)
        _compare_select_statement_with_converted_string(
            engine=engine, select_statement=select_with_post_compile_statements
        )
    else:
        assert False


def test_mssql(sa, test_backends, select_with_post_compile_statements):
    if "mssql" in test_backends:
        db_hostname = os.getenv("GE_TEST_LOCAL_DB_HOSTNAME", "localhost")
        connection_string = f"mssql+pyodbc://sa:ReallyStrongPwd1234%^&*@{db_hostname}:1433/test_ci?driver=ODBC Driver 17 for SQL Server&charset=utf8&autocommit=true"
        engine = SqlAlchemyExecutionEngine(connection_string=connection_string)
        _compare_select_statement_with_converted_string(
            engine=engine, select_statement=select_with_post_compile_statements
        )
    else:
        assert False


def test_trino(sa, test_backends, select_with_post_compile_statements):
    if "trino" in test_backends:
        connection_string = "trino://test@localhost:8088/memory/schema"
        engine = SqlAlchemyExecutionEngine(connection_string=connection_string)
        _compare_select_statement_with_converted_string(
            engine=engine, select_statement=select_with_post_compile_statements
        )
    else:
        assert False


def test_redshift(sa, test_backends, select_with_post_compile_statements):
    if "redshift" in test_backends:
        connection_string = get_redshift_connection_url()
        engine = SqlAlchemyExecutionEngine(connection_string=connection_string)
        _compare_select_statement_with_converted_string(
            engine=engine, select_statement=select_with_post_compile_statements
        )
    else:
        pytest.skip("skipping redshift")


def test_snowflake(sa, test_backends, select_with_post_compile_statements):
    if "snowflake" in test_backends:
        connection_string = get_snowflake_connection_url()
        engine = SqlAlchemyExecutionEngine(connection_string=connection_string)
        _compare_select_statement_with_converted_string(
            engine=engine, select_statement=select_with_post_compile_statements
        )
    else:
        pytest.skip("skipping snowflake")


def test_awsathena(sa, test_backends, select_with_post_compile_statements):
    if "awsathena" in test_backends:
        athena_db_name_env_var: str = ("ATHENA_DB_NAME",)
        connection_string: str = get_awsathena_connection_url(athena_db_name_env_var)
        engine = SqlAlchemyExecutionEngine(connection_string=connection_string)
        _compare_select_statement_with_converted_string(
            engine=engine, select_statement=select_with_post_compile_statements
        )
    else:
        assert False


def test_bigquery(sa, test_backends, select_with_post_compile_statements):
    if "bigquery" in test_backends:
        connection_string = get_bigquery_connection_url()
        engine = SqlAlchemyExecutionEngine(connection_string=connection_string)
        _compare_select_statement_with_converted_string(
            engine=engine, select_statement=select_with_post_compile_statements
        )
    else:
        assert False
