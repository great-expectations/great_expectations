from __future__ import annotations

import pathlib
from contextlib import _GeneratorContextManager, contextmanager
from typing import TYPE_CHECKING, Any, Callable, Generator, Optional

import pytest
from pydantic import ValidationError

from great_expectations.datasource.fluent import SqliteDatasource
from tests.datasource.fluent.conftest import sqlachemy_execution_engine_mock_cls

if TYPE_CHECKING:
    from great_expectations.data_context import AbstractDataContext


@pytest.fixture
def sqlite_datasource_name() -> str:
    return "sqlite_datasource"


@pytest.fixture
def sqlite_database_path() -> pathlib.Path:
    relative_path = pathlib.Path(
        "..",
        "..",
        "test_sets",
        "taxi_yellow_tripdata_samples",
        "sqlite",
        "yellow_tripdata.db",
    )
    return pathlib.Path(__file__).parent.joinpath(relative_path).resolve(strict=True)


@pytest.fixture
def sqlite_datasource(
    empty_data_context, sqlite_database_path, sqlite_datasource_name
) -> SqliteDatasource:
    connection_string = f"sqlite:///{sqlite_database_path}"
    return SqliteDatasource(
        name=sqlite_datasource_name,
        connection_string=connection_string,  # type: ignore[arg-type]  # pydantic will coerce
    )


@pytest.mark.unit
def test_connection_string_starts_with_sqlite(
    sqlite_datasource, sqlite_database_path, sqlite_datasource_name
):
    # The actual file doesn't matter only it's existence since SqlAlchemy does a check
    # when it creates the database engine.
    assert sqlite_datasource.name == sqlite_datasource_name
    assert sqlite_datasource.connection_string == f"sqlite:///{sqlite_database_path}"


@pytest.mark.unit
def test_connection_string_that_does_not_start_with_sqlite():
    name = "sqlite_datasource"
    connection_string = "stuff+sqlite:///path/to/database/file.db"
    with pytest.raises(ValidationError) as e:
        SqliteDatasource(
            name=name,
            connection_string=connection_string,
        )
    # the first error is due to missing a config template string
    assert e.value.errors()[1]["msg"] == "URL scheme not permitted"
    assert e.value.errors()[1].get("ctx") == {
        "allowed_schemes": {
            "sqlite",
            "sqlite+aiosqlite",
            "sqlite+pysqlcipher",
            "sqlite+pysqlite",
        }
    }


@pytest.mark.unit
def test_non_select_query_asset(sqlite_datasource):
    with pytest.raises(ValueError):
        sqlite_datasource.add_query_asset(name="query_asset", query="* from table")


# Test double used to return canned responses for splitter queries.
@contextmanager
def _create_sqlite_source(
    data_context: Optional[AbstractDataContext] = None,
    splitter_query_response: Optional[list[tuple[str]]] = None,
    create_temp_table: bool = True,
) -> Generator[Any, Any, Any]:
    execution_eng_cls = sqlachemy_execution_engine_mock_cls(
        validate_batch_spec=lambda _: None,
        dialect="sqlite",
        splitter_query_response=splitter_query_response,
    )
    # These type ignores when dealing with the execution_engine_override are because
    # it is a generic. We don't care about the exact type since we swap it out with our
    # mock for the purpose of this test and then replace it with the original.
    original_override = SqliteDatasource.execution_engine_override  # type: ignore[misc]
    try:
        SqliteDatasource.execution_engine_override = execution_eng_cls  # type: ignore[misc]
        sqlite_datasource = SqliteDatasource(
            name="sqlite_datasource",
            connection_string="sqlite://",  # type: ignore[arg-type]  # pydantic will coerce
            create_temp_table=create_temp_table,
        )
        if data_context:
            sqlite_datasource._data_context = data_context
        yield sqlite_datasource
    finally:
        SqliteDatasource.execution_engine_override = original_override  # type: ignore[misc]


@pytest.fixture
def create_sqlite_source() -> (
    Callable[
        [Optional[AbstractDataContext], list[tuple[str]]], _GeneratorContextManager[Any]
    ]
):
    return _create_sqlite_source


@pytest.mark.unit
@pytest.mark.parametrize(
    [
        "add_splitter_method_name",
        "splitter_kwargs",
        "splitter_query_responses",
        "sorter_args",
        "all_batches_cnt",
        "specified_batch_request",
        "specified_batch_cnt",
        "last_specified_batch_metadata",
    ],
    [
        pytest.param(
            "add_splitter_hashed_column",
            {"column_name": "passenger_count", "hash_digits": 3},
            [("abc",), ("bcd",), ("xyz",)],
            ["hash"],
            3,
            {"hash": "abc"},
            1,
            {"hash": "abc"},
            id="hash",
        ),
        pytest.param(
            "add_splitter_converted_datetime",
            {"column_name": "pickup_datetime", "date_format_string": "%Y-%m-%d"},
            [("2019-02-01",), ("2019-02-23",)],
            ["datetime"],
            2,
            {"datetime": "2019-02-23"},
            1,
            {"datetime": "2019-02-23"},
            id="converted_datetime",
        ),
    ],
)
def test_sqlite_specific_splitter(
    empty_data_context,
    create_sqlite_source,
    add_splitter_method_name,
    splitter_kwargs,
    splitter_query_responses,
    sorter_args,
    all_batches_cnt,
    specified_batch_request,
    specified_batch_cnt,
    last_specified_batch_metadata,
):
    with create_sqlite_source(
        data_context=empty_data_context,
        splitter_query_response=[response for response in splitter_query_responses],
    ) as source:
        asset = source.add_query_asset(name="query_asset", query="SELECT * from table")
        getattr(asset, add_splitter_method_name)(**splitter_kwargs)
        asset.add_sorters(sorter_args)
        # Test getting all batches
        all_batches = asset.get_batch_list_from_batch_request(
            asset.build_batch_request()
        )
        assert len(all_batches) == all_batches_cnt
        # Test getting specified batches
        specified_batches = asset.get_batch_list_from_batch_request(
            asset.build_batch_request(specified_batch_request)
        )
        assert len(specified_batches) == specified_batch_cnt
        assert specified_batches[-1].metadata == last_specified_batch_metadata


@pytest.mark.unit
def test_create_temp_table(empty_data_context, create_sqlite_source):
    with create_sqlite_source(
        data_context=empty_data_context, create_temp_table=False
    ) as source:
        assert source.create_temp_table is False
        asset = source.add_query_asset(name="query_asset", query="SELECT * from table")
        _ = asset.get_batch_list_from_batch_request(asset.build_batch_request())
        assert source._execution_engine._create_temp_table is False
