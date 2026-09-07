from datetime import date, datetime, timezone
from importlib import import_module
from typing import Any

import pytest
import sqlalchemy.sql

from great_expectations.core.batch_definition import BatchDefinition
from great_expectations.core.partitioners import (
    ColumnPartitionerDaily,
    ColumnPartitionerMonthly,
    ColumnPartitionerYearly,
)
from great_expectations.datasource.fluent import SQLDatasource
from great_expectations.datasource.fluent.sql_datasource import (
    QueryAsset,
    SqlAddBatchDefinitionError,
    TableAsset,
    _SQLAsset,
)
from tests.datasource.fluent.conftest import CreateSourceFixture


@pytest.fixture
def postgres_asset(empty_data_context, create_source: CreateSourceFixture, monkeypatch):
    my_config_variables = {"pipeline_filename": __file__}
    empty_data_context.config_variables.update(my_config_variables)

    monkeypatch.setattr(TableAsset, "test_connection", lambda _: None)
    years = [2021, 2022]
    asset_specified_metadata = {
        "pipeline_name": "my_pipeline",
        "no_curly_pipeline_filename": "$pipeline_filename",
        "curly_pipeline_filename": "${pipeline_filename}",
    }

    with create_source(
        validate_batch_spec=lambda _: None,
        dialect="postgresql",
        data_context=empty_data_context,
        partitioner_query_response=[{"year": year} for year in years],
    ) as source:
        asset = source.add_table_asset(
            name="query_asset",
            table_name="my_table",
            batch_metadata=asset_specified_metadata,
        )
        assert asset.batch_metadata == asset_specified_metadata

        yield asset


@pytest.mark.postgresql
def test_get_batch_identifiers_list__sort_ascending(postgres_asset):
    years = [2021, 2022]
    batches = postgres_asset.get_batch_identifiers_list(
        postgres_asset.build_batch_request(
            partitioner=ColumnPartitionerYearly(column_name="year", sort_ascending=True)
        )
    )

    assert len(batches) == len(years)
    for i, year in enumerate([2021, 2022]):
        batches[i]["year"] = year


@pytest.mark.postgresql
def test_get_batch_identifiers_list__sort_descending(postgres_asset):
    years = [2021, 2022]
    batches = postgres_asset.get_batch_identifiers_list(
        postgres_asset.build_batch_request(
            partitioner=ColumnPartitionerYearly(column_name="year", sort_ascending=False)
        )
    )

    assert len(batches) == len(years)
    for i, year in enumerate([2022, 2021]):
        batches[i]["year"] = year


class FakeResult:
    def __init__(self, queried_row: Any):
        self._queried_row = queried_row

    def first(self):
        # Sqlalchemy will return None if there are no rows when first is called.
        # We can mock that behavior by setting the queried value to None.
        queried_row = self._queried_row

        class Result:
            def __getattr__(self, attr: str):
                if queried_row:
                    return queried_row[0]
                return None

        return Result()


class FakeConnection:
    def __init__(self, queried_row: Any):
        self._queried_row = queried_row

    def execute(self, *args, **kwargs):
        return FakeResult(self._queried_row)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb): ...


class FakeEngineError(Exception):
    def __init__(self):
        super().__init__("We expect at most 1 element in the queried row when using FakeError")


class FakeEngine:
    def __init__(self, queried_row: Any):
        if queried_row and len(queried_row) > 1:
            raise FakeEngineError()
        self._queried_row = queried_row

    def connect(self):
        return FakeConnection(self._queried_row)


@pytest.fixture
def datasource(mocker) -> SQLDatasource:
    return mocker.Mock(spec=SQLDatasource)


@pytest.fixture
def datasource_with_mocked_get_engine(datasource) -> SQLDatasource:
    """Returns a mocked sqlalchemy engine from datasource.get_engine()

    We validate that adding batch definition uses a datetime column by querying
    the database. This patches the datasource so validating the batch definition
    will succeed.
    """
    datasource.get_engine = lambda: FakeEngine([date(2024, 10, 3)])
    return datasource


@pytest.fixture
def asset(datasource, monkeypatch) -> _SQLAsset:
    asset = _SQLAsset[SQLDatasource](name="test_asset", type="_sql_asset")
    asset._datasource = datasource  # same pattern Datasource uses to init Asset

    # _SQLAsset is abstract so we patch in some no-op implementations
    monkeypatch.setattr(
        _SQLAsset[SQLDatasource], "as_selectable", lambda _: sqlalchemy.sql.table("table")
    )

    return asset


@pytest.mark.unit
def test_add_batch_definition_fluent_sql__add_batch_definition_whole_table(
    datasource_with_mocked_get_engine, asset
):
    # arrange
    name = "batch_def_name"
    expected_batch_definition = BatchDefinition(name=name, partitioner=None, batching_regex=None)
    datasource = datasource_with_mocked_get_engine
    datasource.add_batch_definition.return_value = expected_batch_definition

    # act
    batch_definition = asset.add_batch_definition_whole_table(name=name)

    # assert
    assert batch_definition == expected_batch_definition
    datasource.add_batch_definition.assert_called_once_with(expected_batch_definition)


@pytest.mark.unit
@pytest.mark.parametrize("sort_ascending", (True, False))
def test_add_batch_definition_fluent_sql__add_batch_definition_yearly(
    datasource_with_mocked_get_engine, asset, sort_ascending
):
    # arrange
    name = "batch_def_name"
    column = "test_column"
    expected_batch_definition = BatchDefinition(
        name=name,
        partitioner=ColumnPartitionerYearly(column_name=column, sort_ascending=sort_ascending),
        batching_regex=None,
    )
    datasource = datasource_with_mocked_get_engine
    datasource.add_batch_definition.return_value = expected_batch_definition

    # act
    batch_definition = asset.add_batch_definition_yearly(
        name=name, column=column, sort_ascending=sort_ascending
    )

    # assert
    assert batch_definition == expected_batch_definition
    datasource.add_batch_definition.assert_called_once_with(expected_batch_definition)


@pytest.mark.unit
@pytest.mark.parametrize("sort_ascending", (True, False))
def test_add_batch_definition_fluent_sql__add_batch_definition_monthly(
    datasource_with_mocked_get_engine, asset, sort_ascending
):
    # arrange
    name = "batch_def_name"
    column = "test_column"
    expected_batch_definition = BatchDefinition(
        name=name,
        partitioner=ColumnPartitionerMonthly(column_name=column, sort_ascending=sort_ascending),
        batching_regex=None,
    )
    datasource = datasource_with_mocked_get_engine
    datasource.add_batch_definition.return_value = expected_batch_definition

    # act
    batch_definition = asset.add_batch_definition_monthly(
        name=name, column=column, sort_ascending=sort_ascending
    )

    # assert
    assert batch_definition == expected_batch_definition
    datasource.add_batch_definition.assert_called_once_with(expected_batch_definition)


@pytest.mark.unit
@pytest.mark.parametrize("sort_ascending", (True, False))
def test_add_batch_definition_fluent_sql__add_batch_definition_daily(
    datasource_with_mocked_get_engine, asset, sort_ascending
):
    # arrange
    name = "batch_def_name"
    column = "test_column"
    expected_batch_definition = BatchDefinition(
        name=name,
        partitioner=ColumnPartitionerDaily(column_name=column, sort_ascending=sort_ascending),
        batching_regex=None,
    )
    datasource = datasource_with_mocked_get_engine
    datasource.add_batch_definition.return_value = expected_batch_definition

    # act
    batch_definition = asset.add_batch_definition_daily(
        name=name, column=column, sort_ascending=sort_ascending
    )

    # assert
    assert batch_definition == expected_batch_definition
    datasource.add_batch_definition.assert_called_once_with(expected_batch_definition)


def add_table_asset(datasource: SQLDatasource):
    return datasource.add_table_asset(
        name="my_table_asset",
        table_name="my_table",
    )


def add_query_asset(datasource: SQLDatasource):
    return datasource.add_query_asset(name="my_query_asset", query="select * from my_table")


@pytest.mark.unit
@pytest.mark.parametrize(
    "queried_row",
    [
        pytest.param(
            ["not a datetime"],
            id="invalid type",
        ),
        pytest.param([None], id="None row returned"),
        pytest.param(None, id="No rows returned"),
        pytest.param([], id="Empty row returned"),
    ],
)
@pytest.mark.parametrize(
    "add_sql_asset",
    [
        pytest.param(add_table_asset, id="table asset"),
        pytest.param(add_query_asset, id="query asset"),
    ],
)
def test_validate_batch_definition_with_error(
    sql_datasource_table_asset_test_connection_noop: SQLDatasource,
    queried_row: Any,
    add_sql_asset,
    monkeypatch,
):
    # Setup
    # Monkeypatch SQLDatasource so we return a fake engine which returns a fixed db result.
    monkeypatch.setattr(
        SQLDatasource,
        "get_engine",
        lambda _: FakeEngine(queried_row),
    )
    datasource = sql_datasource_table_asset_test_connection_noop

    # Add our asset to the datasource
    asset = add_sql_asset(datasource)

    # Act and assert: Add a batch definition and assert error
    with pytest.raises(SqlAddBatchDefinitionError):
        asset.validate_batch_definition(ColumnPartitionerDaily(column_name="column_name"))


@pytest.mark.unit
@pytest.mark.parametrize(
    "queried_row",
    [
        pytest.param(
            [datetime(2024, 10, 28, 0, 0, 0, tzinfo=timezone.utc)],
            id="datetime",
        ),
        pytest.param([date(2024, 10, 28)], id="date"),
    ],
)
@pytest.mark.parametrize(
    "add_sql_asset",
    [
        pytest.param(add_table_asset, id="table asset"),
        pytest.param(add_query_asset, id="query asset"),
    ],
)
def test_validate_batch_definition(
    sql_datasource_table_asset_test_connection_noop: SQLDatasource,
    queried_row: Any,
    add_sql_asset,
    monkeypatch,
):
    # Setup
    # Monkeypatch SQLDatasource so we return a fake engine which returns a fixed db result.
    monkeypatch.setattr(
        SQLDatasource,
        "get_engine",
        lambda _: FakeEngine(queried_row),
    )
    datasource = sql_datasource_table_asset_test_connection_noop

    # Add our asset to the datasource
    asset = add_sql_asset(datasource)

    # Act
    NoneOnSuccess = asset.validate_batch_definition(
        ColumnPartitionerDaily(column_name="column_name")
    )

    # Assert
    assert NoneOnSuccess is None


# Tests I considered adding for test_validate_batch_definition but have not.
# 1. Engine dies on connect
# 2. Connection dies on execute


# Every dialect this repo renders SQL for in unit tests, keyed by the alias suffix its
# compiler emits before a subquery alias. Oracle omits the "AS"; the rest keep it.
QUERY_ASSET_SUBQUERY_ALIAS_PREFIX_BY_DIALECT = {
    "sqlite": "AS ",
    "postgresql": "AS ",
    "mssql": "AS ",
    "mysql": "AS ",
    "oracle": "",
}


@pytest.mark.unit
@pytest.mark.parametrize(
    "dialect_name", sorted(QUERY_ASSET_SUBQUERY_ALIAS_PREFIX_BY_DIALECT), ids=str
)
def test_query_asset_as_selectable_wraps_the_whole_statement(dialect_name: str) -> None:
    """A query asset's selectable must wrap the user's statement unchanged.

    The statement has to reach the compiler whole. Rebuilding it as a select over everything
    after its "SELECT" produces a select that owns no FROM clause, and the Oracle dialect
    completes such a select by appending "FROM DUAL" -- so the wrapped query went out with two
    FROM clauses and no Oracle version accepted it. This is the selectable a batch definition
    is validated against, so the failure landed on adding one, not on reading a batch.

    Only the Oracle case fails without the fix this pins; the other four are here to pin that
    the change leaves what they select untouched.

    The newline before the closing parenthesis is deliberate: it keeps that parenthesis and
    the alias out of reach of a line comment the statement may end in.
    """
    query = "SELECT id, created_at FROM my_table"
    asset = QueryAsset(name="query_asset", query=query)

    selectable = asset.as_selectable()

    assert isinstance(selectable, sqlalchemy.Subquery)
    dialect = import_module(f"sqlalchemy.dialects.{dialect_name}").dialect()
    rendered = str(sqlalchemy.select("*").select_from(selectable).compile(dialect=dialect))
    alias_prefix = QUERY_ASSET_SUBQUERY_ALIAS_PREFIX_BY_DIALECT[dialect_name]
    assert rendered == f"SELECT * \nFROM ({query}\n) {alias_prefix}anon_1"
