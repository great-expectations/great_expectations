import pytest

from great_expectations.data_context.util import file_relative_path
from great_expectations.execution_engine import SqlAlchemyExecutionEngine
from great_expectations.execution_engine.sqlalchemy_batch_data import (
    SqlAlchemyBatchData,
)
from great_expectations.util import get_sqlalchemy_inspector
from tests.expectations.test_util import get_table_columns_metric


@pytest.mark.sqlite
def test_table_column_introspection(sa, capfd):
    db_file = file_relative_path(
        __file__,
        "../../test_sets/test_cases_for_sql_data_connector.db",
    )
    eng = sa.create_engine(f"sqlite:///{db_file}", echo=True)
    engine = SqlAlchemyExecutionEngine(engine=eng)
    batch_data = SqlAlchemyBatchData(
        execution_engine=engine, table_name="table_partitioned_by_date_column__A"
    )
    engine.load_batch_data("__", batch_data)
    assert isinstance(batch_data.selectable, sa.Table)
    assert batch_data.selectable.name == "table_partitioned_by_date_column__A"
    assert batch_data.selectable.schema is None

    insp = get_sqlalchemy_inspector(eng)
    columns = insp.get_columns(
        batch_data.selectable.name, schema=batch_data.selectable.schema
    )
    assert [x["name"] for x in columns] == [
        "index",
        "id",
        "date",
        "event_type",
        "favorite_color",
    ]
    std, err = capfd.readouterr()

    assert std
    assert not err

    # Assert that using the same inspector again does not cause a query
    columns = insp.get_columns(
        batch_data.selectable.name, schema=batch_data.selectable.schema
    )

    std, err = capfd.readouterr()

    assert not std
    assert not err


@pytest.mark.sqlite
def test_table_column_type(sa, capfd):
    db_file = file_relative_path(
        __file__,
        "../../test_sets/test_cases_for_sql_data_connector.db",
    )
    eng = sa.create_engine(f"sqlite:///{db_file}", echo=True)
    engine = SqlAlchemyExecutionEngine(engine=eng)
    batch_data = SqlAlchemyBatchData(
        execution_engine=engine, table_name="table_partitioned_by_date_column__A"
    )
    engine.load_batch_data("__", batch_data)
    assert isinstance(batch_data.selectable, sa.Table)
    assert batch_data.selectable.name == "table_partitioned_by_date_column__A"
    assert batch_data.selectable.schema is None

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    std, err = capfd.readouterr()

    assert std
    assert not err

    assert results[("table.columns", (), ())] == [
        "index",
        "id",
        "date",
        "event_type",
        "favorite_color",
    ]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    std, err = capfd.readouterr()
    # Should not have to re-inspect
    assert not std
    assert not err

    assert results[("table.columns", (), ())] == [
        "index",
        "id",
        "date",
        "event_type",
        "favorite_color",
    ]
