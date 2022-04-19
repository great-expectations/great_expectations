import datetime
from unittest import mock

import pytest
from mock_alchemy.comparison import ExpressionMatcher

from great_expectations.execution_engine.sqlalchemy_data_splitter import (
    DatePart,
    SqlAlchemyDataSplitter,
)


@mock.patch(
    "great_expectations.execution_engine.sqlalchemy_data_splitter.SqlAlchemyDataSplitter.split_on_date_parts"
)
def test_split_on_year(mock_split_on_date_parts):
    """Test that a partially pre-filled version of split_on_date_parts() was called with the appropriate params."""
    sqlalchemy_data_splitter: SqlAlchemyDataSplitter = SqlAlchemyDataSplitter()
    column_name: str = "column_name"
    batch_identifiers: dict = {column_name: {"year": 2018, "month": 10}}

    sqlalchemy_data_splitter.split_on_year(
        column_name=column_name,
        batch_identifiers=batch_identifiers,
    )

    mock_split_on_date_parts.assert_called_with(
        column_name=column_name,
        batch_identifiers=batch_identifiers,
        date_parts=[DatePart.YEAR],
    )


@mock.patch(
    "great_expectations.execution_engine.sqlalchemy_data_splitter.SqlAlchemyDataSplitter.split_on_date_parts"
)
def test_split_on_year_and_month(mock_split_on_date_parts):
    """Test that a partially pre-filled version of split_on_date_parts() was called with the appropriate params."""
    sqlalchemy_data_splitter: SqlAlchemyDataSplitter = SqlAlchemyDataSplitter()
    column_name: str = "column_name"
    batch_identifiers: dict = {column_name: {"year": 2018, "month": 10}}

    sqlalchemy_data_splitter.split_on_year_and_month(
        column_name=column_name,
        batch_identifiers=batch_identifiers,
    )

    mock_split_on_date_parts.assert_called_with(
        column_name=column_name,
        batch_identifiers=batch_identifiers,
        date_parts=[DatePart.YEAR, DatePart.MONTH],
    )


@mock.patch(
    "great_expectations.execution_engine.sqlalchemy_data_splitter.SqlAlchemyDataSplitter.split_on_date_parts"
)
def test_split_on_year_and_month_and_day(mock_split_on_date_parts):
    """Test that a partially pre-filled version of split_on_date_parts() was called with the appropriate params."""
    sqlalchemy_data_splitter: SqlAlchemyDataSplitter = SqlAlchemyDataSplitter()
    column_name: str = "column_name"
    batch_identifiers: dict = {column_name: {"year": 2018, "month": 10, "day": 31}}

    sqlalchemy_data_splitter.split_on_year_and_month_and_day(
        column_name=column_name,
        batch_identifiers=batch_identifiers,
    )

    mock_split_on_date_parts.assert_called_with(
        column_name=column_name,
        batch_identifiers=batch_identifiers,
        date_parts=[DatePart.YEAR, DatePart.MONTH, DatePart.DAY],
    )


@pytest.mark.parametrize(
    "batch_identifiers_for_column",
    [
        pytest.param({"month": 10}, id="month_dict"),
        pytest.param("10-31-2018", id="dateutil parseable date string"),
        pytest.param(
            datetime.datetime(2018, 10, 31, 0, 0, 0),
            id="datetime",
        ),
        pytest.param(
            {"month": 11},
            marks=pytest.mark.xfail(strict=True),
            id="incorrect month_dict should fail",
        ),
        pytest.param(
            "not a real date",
            marks=pytest.mark.xfail(strict=True),
            id="non dateutil parseable date string",
        ),
        pytest.param(
            datetime.datetime(2018, 11, 30, 0, 0, 0),
            marks=pytest.mark.xfail(strict=True),
            id="incorrect datetime should fail",
        ),
    ],
)
@pytest.mark.parametrize(
    "date_parts",
    [
        pytest.param(
            [DatePart.MONTH],
            id="month_with_DatePart",
        ),
        pytest.param(
            ["month"],
            id="month_with_string_DatePart",
        ),
        pytest.param(
            ["Month"],
            id="month_with_string_mixed_case_DatePart",
        ),
        pytest.param(None, marks=pytest.mark.xfail(strict=True), id="date_parts=None"),
        pytest.param([], marks=pytest.mark.xfail(strict=True), id="date_parts=[]"),
        pytest.param(
            ["invalid"], marks=pytest.mark.xfail(strict=True), id="invalid date_parts"
        ),
        pytest.param(
            "invalid",
            marks=pytest.mark.xfail(strict=True),
            id="invalid date_parts (not a list)",
        ),
        pytest.param(
            "month",
            marks=pytest.mark.xfail(strict=True),
            id="invalid date_parts (not a list but valid str)",
        ),
        pytest.param(
            DatePart.MONTH,
            marks=pytest.mark.xfail(strict=True),
            id="invalid date_parts (not a list but valid DatePart)",
        ),
    ],
)
def test_split_on_date_parts_single_date_parts(
    batch_identifiers_for_column, date_parts, sa
):
    """What does this test and why?

    split_on_date_parts should still build the correct query when passed a single element list
     date_parts that is a string, DatePart enum objects, mixed case string.
     To match our interface it should accept a dateutil parseable string as the batch identifier
     or a datetime and also fail when parameters are invalid.
    """

    sqlalchemy_data_splitter: SqlAlchemyDataSplitter = SqlAlchemyDataSplitter()
    column_name: str = "column_name"

    result: sa.sql.elements.BooleanClauseList = (
        sqlalchemy_data_splitter.split_on_date_parts(
            column_name=column_name,
            batch_identifiers={column_name: batch_identifiers_for_column},
            date_parts=date_parts,
        )
    )

    # using mock-alchemy
    assert ExpressionMatcher(result) == ExpressionMatcher(
        sa.and_(
            sa.extract("month", sa.column(column_name)) == 10,
        )
    )

    # using values
    assert isinstance(result, sa.sql.elements.BinaryExpression)

    assert isinstance(result.comparator.type, sa.Boolean)
    assert isinstance(result.left, sa.sql.elements.Extract)
    assert result.left.field == "month"
    assert result.left.expr.name == column_name
    assert result.right.effective_value == 10


@pytest.mark.parametrize(
    "batch_identifiers_for_column",
    [
        pytest.param({"year": 2018, "month": 10}, id="year_and_month_dict"),
        pytest.param("10-31-2018", id="dateutil parseable date string"),
        pytest.param(
            datetime.datetime(2018, 10, 30, 0, 0, 0),
            id="datetime",
        ),
        pytest.param(
            {"year": 2019, "month": 10},
            marks=pytest.mark.xfail(strict=True),
            id="incorrect year_and_month_dict should fail",
        ),
        pytest.param(
            {"year": 2018, "month": 11},
            marks=pytest.mark.xfail(strict=True),
            id="incorrect year_and_month_dict should fail",
        ),
        pytest.param(
            "not a real date",
            marks=pytest.mark.xfail(strict=True),
            id="non dateutil parseable date string",
        ),
        pytest.param(
            datetime.datetime(2018, 11, 30, 0, 0, 0),
            marks=pytest.mark.xfail(strict=True),
            id="incorrect datetime should fail",
        ),
    ],
)
@pytest.mark.parametrize(
    "date_parts",
    [
        pytest.param(
            [DatePart.YEAR, DatePart.MONTH],
            id="year_month_with_DatePart",
        ),
        pytest.param(
            [DatePart.YEAR, "month"],
            id="year_month_with_mixed_DatePart",
        ),
        pytest.param(
            ["year", "month"],
            id="year_month_with_string_DatePart",
        ),
        pytest.param(
            ["YEAR", "Month"],
            id="year_month_with_string_mixed_case_DatePart",
        ),
        pytest.param(None, marks=pytest.mark.xfail(strict=True), id="date_parts=None"),
        pytest.param([], marks=pytest.mark.xfail(strict=True), id="date_parts=[]"),
        pytest.param(
            ["invalid", "date", "parts"],
            marks=pytest.mark.xfail(strict=True),
            id="invalid date_parts",
        ),
    ],
)
def test_split_on_date_parts_multiple_date_parts(
    batch_identifiers_for_column, date_parts, sa
):
    """What does this test and why?

    split_on_date_parts should still build the correct query when passed
     date parts that are strings, DatePart enum objects, a mixture and mixed case.
     To match our interface it should accept a dateutil parseable string as the batch identifier
     or a datetime and also fail when parameters are invalid.
    """

    sqlalchemy_data_splitter: SqlAlchemyDataSplitter = SqlAlchemyDataSplitter()
    column_name: str = "column_name"

    result: sa.sql.elements.BooleanClauseList = (
        sqlalchemy_data_splitter.split_on_date_parts(
            column_name=column_name,
            batch_identifiers={column_name: batch_identifiers_for_column},
            date_parts=date_parts,
        )
    )

    # using mock-alchemy
    assert ExpressionMatcher(result) == ExpressionMatcher(
        sa.and_(
            sa.extract("year", sa.column(column_name)) == 2018,
            sa.extract("month", sa.column(column_name)) == 10,
        )
    )

    # using values
    assert isinstance(result, sa.sql.elements.BooleanClauseList)

    assert isinstance(result.clauses[0].comparator.type, sa.Boolean)
    assert isinstance(result.clauses[0].left, sa.sql.elements.Extract)
    assert result.clauses[0].left.field == "year"
    assert result.clauses[0].left.expr.name == column_name
    assert result.clauses[0].right.effective_value == 2018

    assert isinstance(result.clauses[1].comparator.type, sa.Boolean)
    assert isinstance(result.clauses[1].left, sa.sql.elements.Extract)
    assert result.clauses[1].left.field == "month"
    assert result.clauses[1].left.expr.name == column_name
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
