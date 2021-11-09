import pytest

from great_expectations.datasource import SqlAlchemyDatasource
from great_expectations.datasource.batch_kwargs_generator import (
    TableBatchKwargsGenerator,
)
from great_expectations.datasource.types import SqlAlchemyDatasourceTableBatchKwargs
from great_expectations.exceptions import BatchKwargsError

try:
    sqlalchemy = pytest.importorskip("sqlalchemy")
except ImportError:
    sqlalchemy = None


def test_basic_operation(basic_sqlalchemy_datasource):
    table_generator = TableBatchKwargsGenerator(
        datasource=basic_sqlalchemy_datasource,
        assets={
            "my_asset": {
                "table": "my_table",
                "schema": "$schema",  # Note the use of python $-template to allow substitution
            },
            "my_no_schema_asset": {"table": "important_data"},
            "dangerous.named_asset": {  # Usually, a period in the name will be interpreted as implying the schema
                "table": "named_asset",
                "schema": "$schema",
            },
        },
    )

    batch_kwargs = table_generator.yield_batch_kwargs(
        "my_asset", query_parameters={"schema": "foo"}
    )
    assert isinstance(batch_kwargs, SqlAlchemyDatasourceTableBatchKwargs)
    assert batch_kwargs.schema == "foo"
    assert batch_kwargs.table == "my_table"

    # Note that schema is ignored in this case -- it's not part of the defined asset
    batch_kwargs = table_generator.yield_batch_kwargs(
        "my_no_schema_asset", query_parameters={"schema": "foo"}
    )
    assert isinstance(batch_kwargs, SqlAlchemyDatasourceTableBatchKwargs)
    assert batch_kwargs.schema is None
    assert batch_kwargs.table == "important_data"

    # Here, it's just a classic acceptable case
    batch_kwargs = table_generator.yield_batch_kwargs("my_no_schema_asset")
    assert isinstance(batch_kwargs, SqlAlchemyDatasourceTableBatchKwargs)
    assert batch_kwargs.schema is None
    assert batch_kwargs.table == "important_data"

    # Note that in this case, we have a confusingly named asset, since it "could" be a schema + table name
    # Since it's not available to be found via introspection, however, and it *is* a valid name, this works fine
    batch_kwargs = table_generator.yield_batch_kwargs(
        "dangerous.named_asset", query_parameters={"schema": "bar"}
    )
    assert isinstance(batch_kwargs, SqlAlchemyDatasourceTableBatchKwargs)
    assert batch_kwargs.schema == "bar"
    assert batch_kwargs.table == "named_asset"

    # When a data asset is configured to require a template but it is not available, we should
    # fail with an informative message
    with pytest.raises(BatchKwargsError) as exc:
        table_generator.yield_batch_kwargs("my_asset")
    assert "missing template key" in exc.value.message


def test_db_introspection(postgresql_sqlalchemy_datasource, caplog):
    table_generator = TableBatchKwargsGenerator(
        datasource=postgresql_sqlalchemy_datasource
    )

    # Get a list of tables visible inside the defined database
    assets = table_generator.get_available_data_asset_names()
    assert len(assets["names"]) > 0
    table_name = assets["names"].pop()[0]

    # We should be able to get kwargs without having them specifically configured based on discovery
    batch_kwargs = table_generator.build_batch_kwargs(table_name)
    assert isinstance(batch_kwargs, SqlAlchemyDatasourceTableBatchKwargs)
    assert batch_kwargs.table == table_name
    assert batch_kwargs.schema == "public"

    # ... and that should work with and without explicit inclusion of the schema
    batch_kwargs = table_generator.yield_batch_kwargs("public." + table_name)
    assert isinstance(batch_kwargs, SqlAlchemyDatasourceTableBatchKwargs)
    assert batch_kwargs.table == table_name
    assert batch_kwargs.schema == "public"

    # We should be able to pass a limit; but calling yield again with different kwargs should yield a warning
    caplog.clear()
    batch_kwargs = table_generator.yield_batch_kwargs("public." + table_name, limit=10)
    assert isinstance(batch_kwargs, SqlAlchemyDatasourceTableBatchKwargs)
    assert batch_kwargs.table == table_name
    assert batch_kwargs.schema == "public"
    assert batch_kwargs.limit == 10
    assert [rec.message for rec in caplog.records] == [
        "Asked to yield batch_kwargs using different supplemental kwargs. Resetting iterator to "
        "use new supplemental kwargs."
    ]


def test_query_generator_view(sqlite_view_engine):
    datasource = SqlAlchemyDatasource(
        engine=sqlite_view_engine,
        batch_kwargs_generators={"table": {"class_name": "TableBatchKwargsGenerator"}},
    )  # Build a datasource with a queries generator to introspect our database with a view
    names = set(datasource.get_available_data_asset_names()["table"]["names"])

    # We should see both the table *and* the primary view, but *not* the temp view
    assert names == {("main.test_table", "table"), ("main.test_view", "view")}
