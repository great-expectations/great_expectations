import pytest

from great_expectations.core.batch import BatchSpec
from great_expectations.core.batch_spec import SqlAlchemyDatasourceBatchSpec
from great_expectations.execution_engine import SqlAlchemyExecutionEngine

try:
    sqlalchemy = pytest.importorskip("sqlalchemy")
except ImportError:
    sqlalchemy = None

from great_expectations.execution_engine.sqlalchemy_batch_data import (
    SqlAlchemyBatchData,
)

from ..test_utils import get_sqlite_temp_table_names


def test_instantiation_with_table_name(sqlite_view_engine):
    execution_engine: SqlAlchemyExecutionEngine = SqlAlchemyExecutionEngine(
        engine=sqlite_view_engine
    )
    batch_data: SqlAlchemyBatchData = SqlAlchemyBatchData(
        execution_engine=execution_engine,
        table_name="test_table",
    )

    # This is a very hacky type check.
    # A better way would be to figure out the proper parent class for dialects within SQLAlchemy
    assert (
        str(type(batch_data.sql_engine_dialect))[:28] == "<class 'sqlalchemy.dialects."
    )

    assert isinstance(batch_data.selectable, sqlalchemy.Table)

    assert type(batch_data.record_set_name) == str
    assert batch_data.record_set_name == "great_expectations_sub_selection"

    assert batch_data.use_quoted_name is False


def test_instantiation_with_query(sqlite_view_engine, test_df):
    test_df.to_sql("test_table_0", con=sqlite_view_engine)

    query: str = "SELECT * FROM test_table_0"
    # If create_temp_table=False, a new temp table should NOT be created
    # noinspection PyUnusedLocal
    batch_data: SqlAlchemyBatchData = SqlAlchemyBatchData(
        execution_engine=sqlite_view_engine,
        query=query,
        create_temp_table=False,
    )
    assert len(get_sqlite_temp_table_names(sqlite_view_engine)) == 1


# REMOVING PENDING READ OF table.head metric
# def test_head(sqlite_view_engine):
#     # Create a larger table so that we can downsample meaningfully
#     df = pd.DataFrame({"a": range(100)})
#     df.to_sql(name="test_table_2", con=sqlite_view_engine, index=False)
#
#     engine = SqlAlchemyExecutionEngine(engine=sqlite_view_engine)
#     batch_data = SqlAlchemyBatchData(
#         execution_engine=engine,
#         table_name="test_table_2",
#     )
#     engine.load_batch_data("__", batch_data)
#     validator = Validator(execution_engine=engine)
#     df = validator.head()
#     assert df.shape == (5, 2)
#
#     assert validator.head(fetch_all=True).shape == (100, 2)
#     assert validator.head(n_rows=20).shape == (20, 2)
#     assert validator.head(n_rows=20, fetch_all=True).shape == (100, 2)


def test_instantiation_with_and_without_temp_table(sqlite_view_engine, sa):
    print(get_sqlite_temp_table_names(sqlite_view_engine))
    assert len(get_sqlite_temp_table_names(sqlite_view_engine)) == 1
    assert get_sqlite_temp_table_names(sqlite_view_engine) == {"test_temp_view"}

    execution_engine: SqlAlchemyExecutionEngine = SqlAlchemyExecutionEngine(
        engine=sqlite_view_engine
    )
    # When the SqlAlchemyBatchData object is based on a table, a new temp table is NOT created, even if create_temp_table=True
    SqlAlchemyBatchData(
        execution_engine=execution_engine,
        table_name="test_table",
        create_temp_table=True,
    )
    assert len(get_sqlite_temp_table_names(sqlite_view_engine)) == 1

    selectable = sa.select("*").select_from(sa.text("main.test_table"))

    # If create_temp_table=False, a new temp table should NOT be created
    SqlAlchemyBatchData(
        execution_engine=execution_engine,
        selectable=selectable,
        create_temp_table=False,
    )
    assert len(get_sqlite_temp_table_names(sqlite_view_engine)) == 1

    # If create_temp_table=True, a new temp table should be created
    SqlAlchemyBatchData(
        execution_engine=execution_engine,
        selectable=selectable,
        create_temp_table=True,
    )
    assert len(get_sqlite_temp_table_names(sqlite_view_engine)) == 2

    # If create_temp_table=True, a new temp table should be created
    SqlAlchemyBatchData(
        execution_engine=execution_engine,
        selectable=selectable,
        # create_temp_table defaults to True
    )
    assert len(get_sqlite_temp_table_names(sqlite_view_engine)) == 3

    # testing whether schema is supported
    selectable = sa.select("*").select_from(sa.table(name="test_table", schema="main"))
    SqlAlchemyBatchData(
        execution_engine=execution_engine,
        selectable=selectable,
        # create_temp_table defaults to True
    )
    assert len(get_sqlite_temp_table_names(sqlite_view_engine)) == 4

    # test schema with execution engine
    # TODO : Will20210222 Add tests for specifying schema with non-sqlite backend that actually supports new schema creation
    my_batch_spec = SqlAlchemyDatasourceBatchSpec(
        **{
            "table_name": "test_table",
            "batch_identifiers": {},
            "schema_name": "main",
        }
    )
    res = execution_engine.get_batch_data_and_markers(batch_spec=my_batch_spec)
    assert len(res) == 2
