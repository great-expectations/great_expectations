import pytest

from great_expectations.datasource import (
    PandasDatasource,
    SparkDFDatasource,
    SqlAlchemyDatasource,
)


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
