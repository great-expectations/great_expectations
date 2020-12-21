import pandas as pd
import pytest

try:
    import sqlalchemy
except ImportError:
    sqlalchemy = None


from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyBatchData,
)


def test_instantiation_with_table_name(sqlite_view_engine):
    batch_data = SqlAlchemyBatchData(
        engine=sqlite_view_engine,
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

    assert batch_data.use_quoted_name == False


def test_instantiation_with_query():
    # Note Abe 20111119: Fill this in
    pass


def test_instantiation_with_selectable():
    # Note Abe 20111119: Fill this in
    pass


def test_instantiation_errors(sqlite_view_engine):

    with pytest.raises(TypeError):
        SqlAlchemyBatchData()

    with pytest.raises(ValueError):
        SqlAlchemyBatchData(engine=sqlite_view_engine)

    # Note Abe 20111119: Let's add tests for more error states


def test_temp_table_mechanics():
    pass


def test_head(sqlite_view_engine):
    # Create a larger table so that we can downsample meaningfully
    df = pd.DataFrame({"a": range(100)})
    df.to_sql("test_table_2", con=sqlite_view_engine)

    batch_data = SqlAlchemyBatchData(
        engine=sqlite_view_engine,
        table_name="test_table_2",
    )
    df = batch_data.head()
    assert df.shape == (5, 2)

    assert batch_data.head(fetch_all=True).shape == (100, 2)
    assert batch_data.head(n=20).shape == (20, 2)
    assert batch_data.head(n=20, fetch_all=True).shape == (100, 2)


def test_row_count(sqlite_view_engine):
    # Create a larger table so that we can downsample meaningfully
    df = pd.DataFrame({"a": range(100)})
    df.to_sql("test_table_2", con=sqlite_view_engine)

    batch_data = SqlAlchemyBatchData(
        engine=sqlite_view_engine,
        table_name="test_table",
    )
    assert batch_data.row_count() == 5

    batch_data = SqlAlchemyBatchData(
        engine=sqlite_view_engine,
        table_name="test_table_2",
    )
    assert batch_data.row_count() == 100
