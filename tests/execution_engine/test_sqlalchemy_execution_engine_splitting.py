import pytest

from great_expectations.execution_engine import SqlAlchemyExecutionEngine
from great_expectations.execution_engine.sqlalchemy_execution_engine import DatePart

try:
    sqlalchemy = pytest.importorskip("sqlalchemy")
except ImportError:
    sqlalchemy = None


def test_split_on_year():
    # E.g. test that split_on_date_parts() was called with the appropriate params.
    pass


def test_split_on_year_and_month():
    pass


def test_split_on_year_and_month_and_day():
    pass


def test_split_on_date_parts(sqlite_view_engine):
    execution_engine: SqlAlchemyExecutionEngine = SqlAlchemyExecutionEngine(
        engine=sqlite_view_engine
    )
    result = execution_engine.split_on_date_parts(
        table_name="test_table",
        column_name="a",
        batch_identifiers={"a": {"year": 2018, "month": 10}},
        date_parts=[DatePart.YEAR, DatePart.MONTH],
    )
    # TODO: AJB 20220418 - need to use mocking library ExpressionMatcher to show equality?
    assert result == sqlalchemy.and_(
        sqlalchemy.extract("year", "a") == 2018,
        sqlalchemy.extract("month", "a") == 10,
    )


def test_get_data_for_batch_identifiers_year():
    # E.g. test that get_data_for_batch_identifiers_for_split_on_date_parts()
    # was called with the appropriate params.
    pass


def test_get_data_for_batch_identifiers_year_and_month():
    pass


def test_get_data_for_batch_identifiers_year_and_month_and_day():
    pass


def test_get_data_for_batch_identifiers_for_split_on_date_parts():
    pass
