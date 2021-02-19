from great_expectations.data_context.util import file_relative_path
from great_expectations.execution_engine import SqlAlchemyExecutionEngine
from great_expectations.execution_engine.sqlalchemy_batch_data import (
    SqlAlchemyBatchData,
)
from great_expectations.expectations.metrics.import_manager import reflection


def test_table_column_introspection(sa):
    db_file = file_relative_path(
        __file__,
        "../../test_sets/test_cases_for_sql_data_connector.db",
    )
    eng = sa.create_engine(f"sqlite:///{db_file}")
    engine = SqlAlchemyExecutionEngine(engine=eng)
    batch_data = SqlAlchemyBatchData(
        execution_engine=engine, table_name="table_partitioned_by_date_column__A"
    )
    engine.load_batch_data("__", batch_data)
    assert isinstance(batch_data.selectable, sa.Table)
    assert batch_data.selectable.name == "table_partitioned_by_date_column__A"
    assert batch_data.selectable.schema is None

    insp = reflection.Inspector.from_engine(eng)
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
