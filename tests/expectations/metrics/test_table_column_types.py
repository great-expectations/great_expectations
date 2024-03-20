from unittest import mock

import pytest

from great_expectations.data_context.util import file_relative_path
from great_expectations.exceptions.exceptions import MetricResolutionError
from great_expectations.execution_engine import SqlAlchemyExecutionEngine
from great_expectations.execution_engine.sqlalchemy_batch_data import (
    SqlAlchemyBatchData,
)
from tests.expectations.test_util import get_table_columns_metric


@pytest.mark.sqlite
def test_table_column_introspection(sa, capsys):
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

    insp = engine.get_inspector()
    columns = insp.get_columns(batch_data.selectable.name, schema=batch_data.selectable.schema)
    assert [x["name"] for x in columns] == [
        "index",
        "id",
        "date",
        "event_type",
        "favorite_color",
    ]
    assert capsys.readouterr().out

    # Assert that using the same inspector again does not cause a query
    columns = insp.get_columns(batch_data.selectable.name, schema=batch_data.selectable.schema)
    assert [x["name"] for x in columns] == [
        "index",
        "id",
        "date",
        "event_type",
        "favorite_color",
    ]
    assert not capsys.readouterr().out

    # Add another batch
    batch_data = SqlAlchemyBatchData(
        execution_engine=engine,
        table_name="table_that_should_be_partitioned_by_random_hash__H",
    )
    engine.load_batch_data("__1", batch_data)
    assert isinstance(batch_data.selectable, sa.Table)
    assert batch_data.selectable.name == "table_that_should_be_partitioned_by_random_hash__H"
    assert batch_data.selectable.schema is None

    columns = insp.get_columns(batch_data.selectable.name, schema=batch_data.selectable.schema)
    assert [x["name"] for x in columns] == [
        "index",
        "id",
        "event_type",
        "favorite_color",
    ]

    # Should have to re-inspect due to new batch
    assert capsys.readouterr().out


@pytest.mark.sqlite
def test_table_column_type__sqlalchemy_happy_path(sa, capsys):
    db_file = file_relative_path(
        __file__,
        "../../test_sets/test_cases_for_sql_data_connector.db",
    )
    eng = sa.create_engine(f"sqlite:///{db_file}", echo=True)
    engine = SqlAlchemyExecutionEngine(engine=eng)
    with mock.patch(
        "great_expectations.execution_engine.sqlalchemy_execution_engine.sa.inspect",
        wraps=sa.inspect,
    ) as mock_inspect:
        batch_data = SqlAlchemyBatchData(
            execution_engine=engine, table_name="table_partitioned_by_date_column__A"
        )
        engine.load_batch_data("__", batch_data)
        assert isinstance(batch_data.selectable, sa.Table)
        assert batch_data.selectable.name == "table_partitioned_by_date_column__A"
        assert batch_data.selectable.schema is None

        _table_columns_metric, results = get_table_columns_metric(execution_engine=engine)

        assert results[("table.columns", (), ())] == [
            "index",
            "id",
            "date",
            "event_type",
            "favorite_color",
        ]

        assert mock_inspect.call_count == 1

        _table_columns_metric, results = get_table_columns_metric(execution_engine=engine)

        assert results[("table.columns", (), ())] == [
            "index",
            "id",
            "date",
            "event_type",
            "favorite_color",
        ]

        # Should not have to re-inspect
        assert mock_inspect.call_count == 1

        # Add another batch
        batch_data = SqlAlchemyBatchData(
            execution_engine=engine,
            table_name="table_that_should_be_partitioned_by_random_hash__H",
        )
        engine.load_batch_data("__1", batch_data)
        assert isinstance(batch_data.selectable, sa.Table)
        assert batch_data.selectable.name == "table_that_should_be_partitioned_by_random_hash__H"
        assert batch_data.selectable.schema is None

        _table_columns_metric, results = get_table_columns_metric(execution_engine=engine)

        assert results[("table.columns", (), ())] == [
            "index",
            "id",
            "event_type",
            "favorite_color",
        ]

        # Should have to re-inspect due to new batch
        # But should use same inspector
        assert mock_inspect.call_count == 1


@pytest.mark.sqlite
def test_table_column_types__sqlalchemy_table_not_found(sa):
    db_file = file_relative_path(
        __file__,
        "../../test_sets/test_cases_for_sql_data_connector.db",
    )
    eng = sa.create_engine(f"sqlite:///{db_file}", echo=True)
    engine = SqlAlchemyExecutionEngine(engine=eng)

    # Table `table_partitioned_by_date_column__B` does not exist in database
    batch_data = SqlAlchemyBatchData(
        execution_engine=engine, table_name="table_partitioned_by_date_column__B"
    )
    engine.load_batch_data("__1", batch_data)
    assert isinstance(batch_data.selectable, sa.Table)
    assert batch_data.selectable.name == "table_partitioned_by_date_column__B"
    assert batch_data.selectable.schema is None

    with pytest.raises(MetricResolutionError) as exc:
        _table_columns_metric, _results = get_table_columns_metric(execution_engine=engine)
    assert "(sqlite3.OperationalError) no such table: table_partitioned_by_date_column__B" in str(
        exc.value
    )
