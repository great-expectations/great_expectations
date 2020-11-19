import pytest

import pandas as pd

from great_expectations.execution_engine.sqlalchemy_execution_engine import SqlAlchemyBatchData

def test_instantiation_with_table_name(sqlite_view_engine):
    batch_data = SqlAlchemyBatchData(
        engine=sqlite_view_engine,
        table_name="test_table",
    )

    # Note Abe 20111119: Fill this in:
    # assert batch_data.sql_engine_dialect == "something"
    # assert batch_data.record_set_name == "something"
    # assert batch_data.selectable == "something"
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
        SqlAlchemyBatchData(
            engine=sqlite_view_engine
        )

    # Note Abe 20111119: Let's add tests for more error states


def test_temp_table_mechanics():
    pass

def test_head(sqlite_view_engine):
    #Create a larger table so that we can downsample meaningfully
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
