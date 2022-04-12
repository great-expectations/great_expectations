import os

import pytest

from great_expectations.data_context.util import file_relative_path
from great_expectations.datasource import (
    PandasDatasource,
    SparkDFDatasource,
    SqlAlchemyDatasource,
)
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyExecutionEngine,
)
from great_expectations.self_check.util import get_sqlite_connection_url


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
def test_cases_for_sql_data_connector_sqlite_execution_engine(sa):
    if sa is None:
        raise ValueError("SQL Database tests require sqlalchemy to be installed.")

    db_file_path: str = file_relative_path(
        __file__,
        os.path.join("..", "test_sets", "test_cases_for_sql_data_connector.db"),
    )

    engine: sa.engine.Engine = sa.create_engine(get_sqlite_connection_url(db_file_path))
    conn: sa.engine.Connection = engine.connect()

    # Build a SqlAlchemyDataset using that database
    return SqlAlchemyExecutionEngine(
        name="test_sql_execution_engine",
        engine=conn,
    )
