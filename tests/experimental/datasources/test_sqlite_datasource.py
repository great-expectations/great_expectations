import pathlib

import pytest
from pydantic import ValidationError

from great_expectations.data_context.util import file_relative_path
from great_expectations.experimental.datasources import SqliteDatasource


@pytest.mark.unit
def test_connection_string_starts_with_sqlite():
    # The actual file doesn't matter only it's existence since SqlAlchemy does a check
    # when it creates the database engine.
    db_file = file_relative_path(
        __file__,
        pathlib.Path(
            "../../test_sets/taxi_yellow_tripdata_samples/sqlite/yellow_tripdata.db"
        ),
    )
    name = "sqlite_datasource"
    connection_string = f"sqlite:///{db_file}"
    datasource = SqliteDatasource(
        name=name,
        connection_string=connection_string,
    )
    assert datasource.name == name
    assert datasource.connection_string == connection_string


@pytest.mark.unit
def test_connection_string_that_does_not_start_with_sqlite():
    name = "sqlite_datasource"
    connection_string = "stuff+sqlite:///path/to/database/file.db"
    with pytest.raises(ValidationError):
        SqliteDatasource(
            name=name,
            connection_string=connection_string,
        )
