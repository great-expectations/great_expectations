import pathlib

import pytest
from pydantic import ValidationError

from great_expectations.experimental.datasources import SqliteDatasource
from great_expectations.experimental.datasources.sqlite_datasource import SqliteDsn


@pytest.mark.unit
def test_connection_string_starts_with_sqlite():
    # The actual file doesn't matter only it's existence since SqlAlchemy does a check
    # when it creates the database engine.
    relative_path = pathlib.Path(
        "..",
        "..",
        "test_sets",
        "taxi_yellow_tripdata_samples",
        "sqlite",
        "yellow_tripdata.db",
    )
    db_file = pathlib.Path(__file__).parent.joinpath(relative_path).resolve(strict=True)
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
    with pytest.raises(ValidationError) as e:
        SqliteDatasource(
            name=name,
            connection_string=connection_string,
        )
    assert str(e.value) == (
        "1 validation error for SqliteDatasource\n"
        "connection_string\n"
        "  URL scheme not permitted (type=value_error.url.scheme; "
        f"allowed_schemes={SqliteDsn.allowed_schemes})"
    )
