from mock_alchemy.comparison import ExpressionMatcher

from great_expectations.execution_engine import SqlAlchemyExecutionEngine
from great_expectations.execution_engine.sqlalchemy_data_splitter import (
    SqlAlchemyDataSplitter,
)
from great_expectations.execution_engine.sqlalchemy_execution_engine import DatePart


def test_split_on_year():
    # E.g. test that split_on_date_parts() was called with the appropriate params.
    pass


def test_split_on_year_and_month():
    pass


def test_split_on_year_and_month_and_day():
    pass


def test_split_on_date_parts(sa):

    sqlalchemy_data_splitter: SqlAlchemyDataSplitter = SqlAlchemyDataSplitter()

    # result = execution_engine.split_on_date_parts(
    result = sqlalchemy_data_splitter.split_on_date_parts(
        column_name="a",
        batch_identifiers={"a": {"year": 2018, "month": 10}},
        date_parts=[DatePart.YEAR, DatePart.MONTH],
    )

    # using mock-alchemy
    assert ExpressionMatcher(result) == ExpressionMatcher(
        sa.and_(
            sa.extract("year", sa.column("a")) == 2018,
            sa.extract("month", sa.column("a")) == 10,
        )
    )

    # using values
    assert isinstance(result, sa.sql.elements.BooleanClauseList)

    assert isinstance(result.clauses[0].comparator.type, sa.Boolean)
    assert isinstance(result.clauses[0].left, sa.sql.elements.Extract)
    assert result.clauses[0].left.field == "year"
    assert result.clauses[0].left.expr.name == "a"
    assert result.clauses[0].right.effective_value == 2018

    assert isinstance(result.clauses[1].comparator.type, sa.Boolean)
    assert isinstance(result.clauses[1].left, sa.sql.elements.Extract)
    assert result.clauses[1].left.field == "month"
    assert result.clauses[1].left.expr.name == "a"
    assert result.clauses[1].right.effective_value == 10


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
