import pytest

from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.execution_environment import ExecutionEnvironment

# @pytest.fixture(scope="module")
# def execution_environment():


@pytest.fixture(scope="module")
def basic_pandas_execution_environment():

    execution_engine = {
        "class_name": "PandasExecutionEngine",
        "module_name": "great_expectations.execution_engine.pandas_execution_engine",
    }
    execution_environment = ExecutionEnvironment(
        name="foo", execution_engine=execution_engine
    )
    return execution_environment


# TODO : add equivalent fixtures for postgresql and sparkdf execution engine
# @pytest.fixture(scope="module")
# def basic_pandas_datasource():
#    return PandasDatasource("basic_pandas_datasource")


# @pytest.fixture
# def postgresql_sqlalchemy_datasource(postgresql_engine):
#    return SqlAlchemyDatasource(
#        "postgresql_sqlalchemy_datasource", engine=postgresql_engine
#    )


# @pytest.fixture(scope="module")
# def basic_sparkdf_datasource(test_backends):
#    if "SparkDFDataset" not in test_backends:
#        pytest.skip("Spark has not been enabled, so this test must be skipped.")
#    return SparkDFDatasource("basic_sparkdf_datasource")
