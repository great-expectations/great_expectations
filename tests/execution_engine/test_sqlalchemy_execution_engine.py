import pytest
import os

from great_expectations.execution_engine.sqlalchemy_execution_engine import SqlAlchemyExecutionEngine
from great_expectations.exceptions.exceptions import InvalidConfigError
from great_expectations.data_context.util import (
    file_relative_path
)
from great_expectations.core.batch import (
    BatchSpec,
)

def test_instantiation_via_connection_string(sa, test_db_connection_string):
    my_execution_engine = SqlAlchemyExecutionEngine(
        connection_string=test_db_connection_string
    )
    assert my_execution_engine.connection_string ==  test_db_connection_string
    assert my_execution_engine.credentials == None
    assert my_execution_engine.url == None

    my_execution_engine.get_batch_data_and_markers(BatchSpec(
        table_name="main.table_1",
        sampling_method="_sample_using_limit",
        sampling_kwargs={
            "n": 5
        }
    ))


def test_instantiation_via_url(sa):
    db_file = file_relative_path(
        __file__, os.path.join("..", "test_sets", "test_cases_for_sql_data_connector.db"),
    )
    my_execution_engine = SqlAlchemyExecutionEngine(
        url="sqlite:///"+db_file
    )
    assert my_execution_engine.connection_string == None
    assert my_execution_engine.credentials == None
    assert my_execution_engine.url[-36:] == 'test_cases_for_sql_data_connector.db'

    my_execution_engine.get_batch_data_and_markers(BatchSpec(
        table_name="table_partitioned_by_date_column__A",
        sampling_method="_sample_using_limit",
        sampling_kwargs={
            "n": 5
        }
    ))

def test_instantiation_via_credentials(sa, test_backends):
    if "postgresql" not in test_backends:
        pytest.skip("test_database_store_backend_get_url_for_key requires postgresql")
        
    my_execution_engine = SqlAlchemyExecutionEngine(
        credentials={
            "drivername": "postgresql",
            "username": "postgres",
            "password": "",
            "host": "localhost",
            "port": "5432",
            "database": "test_ci",
        }
    )
    assert my_execution_engine.connection_string == None
    assert my_execution_engine.credentials == {
        "username": "postgres",
        "password": "",
        "host": "localhost",
        "port": "5432",
        "database": "test_ci",
    }
    assert my_execution_engine.url == None

    # Note Abe 20201116: Let's add an actual test of get_batch_data_and_markers, which will require setting up test fixtures
    # my_execution_engine.get_batch_data_and_markers(BatchSpec(
    #     table_name="main.table_1",
    #     sampling_method="_sample_using_limit",
    #     sampling_kwargs={
    #         "n": 5
    #     }
    # ))


def test_instantiation_error_states(sa, test_db_connection_string):
    with pytest.raises(InvalidConfigError):
        SqlAlchemyExecutionEngine()
