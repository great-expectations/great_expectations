import os

import pytest

from great_expectations.datasource import (
    PandasDatasource,
    SparkDFDatasource,
    SqlAlchemyDatasource,
)
from great_expectations.execution_engine import SqlAlchemyExecutionEngine


@pytest.fixture(scope="module")
def basic_pandas_datasource():
    return PandasDatasource("basic_pandas_datasource")


@pytest.fixture
def postgresql_sqlalchemy_datasource(postgresql_engine):
    return SqlAlchemyDatasource(
        "postgresql_sqlalchemy_datasource", engine=postgresql_engine
    )


@pytest.fixture(scope="module")
def basic_sparkdf_datasource(test_backends):
    if "SparkDFDataset" not in test_backends:
        pytest.skip("Spark has not been enabled, so this test must be skipped.")
    return SparkDFDatasource("basic_sparkdf_datasource")



@pytest.fixture
def postgres_schema_engine(test_backends):
    # create a small
    if "postgresql" in test_backends:
        try:
            import sqlalchemy as sa

            url = "postgresql://{}:{}@{}:{}/{}"
            credentials = {
                "drivername": "postgresql",
                "username": "postgres",
                "password": "",
                "host": os.getenv("GE_TEST_LOCAL_DB_HOSTNAME", "localhost"),
                "port": "5432",
                "database": "test_ci",
            }
            postgres_engine = sa.create_engine(
                url.format(
                    credentials["username"],
                    credentials["password"],
                    credentials["host"],
                    credentials["port"],
                    credentials["database"],
                )
            )
            postgres_engine.execute("""CREATE SCHEMA IF NOT EXISTS testing;""")
            postgres_engine.execute(
                """CREATE TABLE IF NOT EXISTS
                    testing.prices(id VARCHAR(64),
                    category VARCHAR(54),
                    name VARCHAR(64),
                    price NUMERIC);"""
            )
            postgres_engine.execute(
                """INSERT INTO 
                testing.prices(id, category, name, price)
                VALUES
                ('abc', 'cheese', 'cheddar', 10),
                ('jkl', 'cheese', 'gouda', 15),
                ('mno', 'cereal', 'cheerios', 3),
                ('pqr', 'cereal', 'froot loops', 2),
                ('stu', 'cereal', 'frosted flakes', 4);"""
            )
            return postgres_engine
        except ImportError:
            sa = None
    else:
        pytest.skip("SqlAlchemy tests disabled; not testing views")


@pytest.fixture
def test_cases_for_sql_data_connector_postgres_execution_engine(
    sa, test_backends, postgres_schema_engine
):
    if sa is None:
        raise ValueError("SQL Database tests require sqlalchemy to be installed.")

    if "postgresql" in test_backends:
        my_execution_engine = SqlAlchemyExecutionEngine(
            name="test_sql_execution_engine",
            credentials={
                "drivername": "postgresql",
                "username": "postgres",
                "password": "",
                "host": os.getenv("GE_TEST_LOCAL_DB_HOSTNAME", "localhost"),
                "port": "5432",
                "database": "test_ci",
            },
        )
        return my_execution_engine
