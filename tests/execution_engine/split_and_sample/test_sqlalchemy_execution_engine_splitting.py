import datetime
import os
from typing import List
from unittest import mock

import pandas as pd
import pytest
from dateutil.parser import parse
from mock_alchemy.comparison import ExpressionMatcher

from great_expectations.core.batch_spec import SqlAlchemyDatasourceBatchSpec
from great_expectations.data_context.util import file_relative_path
from great_expectations.execution_engine import SqlAlchemyExecutionEngine
from great_expectations.execution_engine.split_and_sample.sqlalchemy_data_splitter import (
    DatePart,
    SqlAlchemyDataSplitter,
)
from great_expectations.execution_engine.sqlalchemy_batch_data import (
    SqlAlchemyBatchData,
)
from great_expectations.self_check.util import build_sa_engine
from tests.execution_engine.split_and_sample.split_and_sample_test_cases import (
    MULTIPLE_DATE_PART_BATCH_IDENTIFIERS,
    MULTIPLE_DATE_PART_DATE_PARTS,
    SINGLE_DATE_PART_BATCH_IDENTIFIERS,
    SINGLE_DATE_PART_DATE_PARTS,
)

# Here we add SqlAlchemyDataSplitter specific test cases to the generic test cases:
from tests.integration.fixtures.split_and_sample_data.splitter_test_cases_and_fixtures import (
    TaxiSplittingTestCases,
    TaxiTestData,
)

SINGLE_DATE_PART_DATE_PARTS += [
    pytest.param(
        [SqlAlchemyDataSplitter.date_part.MONTH],
        id="month getting date parts from SqlAlchemyDataSplitter.date_part",
    )
]
MULTIPLE_DATE_PART_DATE_PARTS += [
    pytest.param(
        [SqlAlchemyDataSplitter.date_part.YEAR, SqlAlchemyDataSplitter.date_part.MONTH],
        id="year_month getting date parts from SqlAlchemyDataSplitter.date_part",
    )
]


@mock.patch(
    "great_expectations.execution_engine.split_and_sample.sqlalchemy_data_splitter.SqlAlchemyDataSplitter.split_on_date_parts"
)
@pytest.mark.parametrize(
    "splitter_method_name,called_with_date_parts",
    [
        ("split_on_year", [DatePart.YEAR]),
        ("split_on_year_and_month", [DatePart.YEAR, DatePart.MONTH]),
        (
            "split_on_year_and_month_and_day",
            [DatePart.YEAR, DatePart.MONTH, DatePart.DAY],
        ),
    ],
)
def test_named_date_part_methods(
    mock_split_on_date_parts: mock.MagicMock,
    splitter_method_name: str,
    called_with_date_parts: List[DatePart],
):
    """Test that a partially pre-filled version of split_on_date_parts() was called with the appropriate params.
    For example, split_on_year.
    """
    data_splitter: SqlAlchemyDataSplitter = SqlAlchemyDataSplitter()
    column_name: str = "column_name"
    batch_identifiers: dict = {column_name: {"year": 2018, "month": 10, "day": 31}}

    getattr(data_splitter, splitter_method_name)(
        column_name=column_name,
        batch_identifiers=batch_identifiers,
    )

    mock_split_on_date_parts.assert_called_with(
        column_name=column_name,
        batch_identifiers=batch_identifiers,
        date_parts=called_with_date_parts,
    )


@pytest.mark.parametrize(
    "batch_identifiers_for_column",
    SINGLE_DATE_PART_BATCH_IDENTIFIERS,
)
@pytest.mark.parametrize(
    "date_parts",
    SINGLE_DATE_PART_DATE_PARTS,
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

    data_splitter: SqlAlchemyDataSplitter = SqlAlchemyDataSplitter()
    column_name: str = "column_name"

    result: sa.sql.elements.BooleanClauseList = data_splitter.split_on_date_parts(
        column_name=column_name,
        batch_identifiers={column_name: batch_identifiers_for_column},
        date_parts=date_parts,
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
    MULTIPLE_DATE_PART_BATCH_IDENTIFIERS,
)
@pytest.mark.parametrize(
    "date_parts",
    MULTIPLE_DATE_PART_DATE_PARTS,
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

    data_splitter: SqlAlchemyDataSplitter = SqlAlchemyDataSplitter()
    column_name: str = "column_name"

    result: sa.sql.elements.BooleanClauseList = data_splitter.split_on_date_parts(
        column_name=column_name,
        batch_identifiers={column_name: batch_identifiers_for_column},
        date_parts=date_parts,
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


@mock.patch(
    "great_expectations.execution_engine.split_and_sample.sqlalchemy_data_splitter.SqlAlchemyDataSplitter.get_data_for_batch_identifiers_for_split_on_date_parts"
)
@mock.patch("great_expectations.execution_engine.execution_engine.ExecutionEngine")
def test_get_data_for_batch_identifiers_year(
    mock_execution_engine: mock.MagicMock,
    mock_get_data_for_batch_identifiers_for_split_on_date_parts: mock.MagicMock,
):
    """test that get_data_for_batch_identifiers_for_split_on_date_parts() was called with the appropriate params."""
    data_splitter: SqlAlchemyDataSplitter = SqlAlchemyDataSplitter()
    table_name: str = "table_name"
    column_name: str = "column_name"

    data_splitter.get_data_for_batch_identifiers_year(
        execution_engine=mock_execution_engine,
        table_name=table_name,
        column_name=column_name,
    )

    mock_get_data_for_batch_identifiers_for_split_on_date_parts.assert_called_with(
        execution_engine=mock_execution_engine,
        table_name=table_name,
        column_name=column_name,
        date_parts=[DatePart.YEAR],
    )


@mock.patch(
    "great_expectations.execution_engine.split_and_sample.sqlalchemy_data_splitter.SqlAlchemyDataSplitter.get_data_for_batch_identifiers_for_split_on_date_parts"
)
@mock.patch("great_expectations.execution_engine.execution_engine.ExecutionEngine")
def test_get_data_for_batch_identifiers_year_and_month(
    mock_execution_engine: mock.MagicMock,
    mock_get_data_for_batch_identifiers_for_split_on_date_parts: mock.MagicMock,
):
    """test that get_data_for_batch_identifiers_for_split_on_date_parts() was called with the appropriate params."""
    data_splitter: SqlAlchemyDataSplitter = SqlAlchemyDataSplitter()
    table_name: str = "table_name"
    column_name: str = "column_name"

    data_splitter.get_data_for_batch_identifiers_year_and_month(
        execution_engine=mock_execution_engine,
        table_name=table_name,
        column_name=column_name,
    )

    mock_get_data_for_batch_identifiers_for_split_on_date_parts.assert_called_with(
        execution_engine=mock_execution_engine,
        table_name=table_name,
        column_name=column_name,
        date_parts=[DatePart.YEAR, DatePart.MONTH],
    )


@mock.patch(
    "great_expectations.execution_engine.split_and_sample.sqlalchemy_data_splitter.SqlAlchemyDataSplitter.get_data_for_batch_identifiers_for_split_on_date_parts"
)
@mock.patch("great_expectations.execution_engine.execution_engine.ExecutionEngine")
def test_get_data_for_batch_identifiers_year_and_month_and_day(
    mock_execution_engine: mock.MagicMock,
    mock_get_data_for_batch_identifiers_for_split_on_date_parts: mock.MagicMock,
):
    """test that get_data_for_batch_identifiers_for_split_on_date_parts() was called with the appropriate params."""
    data_splitter: SqlAlchemyDataSplitter = SqlAlchemyDataSplitter()
    table_name: str = "table_name"
    column_name: str = "column_name"

    data_splitter.get_data_for_batch_identifiers_year_and_month_and_day(
        execution_engine=mock_execution_engine,
        table_name=table_name,
        column_name=column_name,
    )

    mock_get_data_for_batch_identifiers_for_split_on_date_parts.assert_called_with(
        execution_engine=mock_execution_engine,
        table_name=table_name,
        column_name=column_name,
        date_parts=[DatePart.YEAR, DatePart.MONTH, DatePart.DAY],
    )


@pytest.mark.parametrize(
    "date_parts",
    SINGLE_DATE_PART_DATE_PARTS,
)
def test_get_split_query_for_data_for_batch_identifiers_for_split_on_date_parts_single_date_parts(
    date_parts, sa
):
    """What does this test and why?

    get_split_query_for_data_for_batch_identifiers_for_split_on_date_parts should still build the correct
    query when passed a single element list of date_parts that is a string, DatePart enum object, or mixed case string.
    """

    data_splitter: SqlAlchemyDataSplitter = SqlAlchemyDataSplitter()
    table_name: str = "table_name"
    column_name: str = "column_name"

    result: sa.sql.elements.BooleanClauseList = data_splitter.get_split_query_for_data_for_batch_identifiers_for_split_on_date_parts(
        table_name=table_name,
        column_name=column_name,
        date_parts=date_parts,
    )

    assert isinstance(result, sa.sql.Select)

    query_str: str = (
        str(result.compile(compile_kwargs={"literal_binds": True}))
        .replace("\n", "")
        .replace(" ", "")
        .lower()
    )
    assert (
        query_str
        == (
            "SELECT distinct(EXTRACT(month FROM column_name)) AS concat_distinct_values, "
            "CAST(EXTRACT(month FROM column_name) AS INTEGER) AS month FROM table_name"
        )
        .replace("\n", "")
        .replace(" ", "")
        .lower()
    )


@pytest.mark.parametrize(
    "date_parts",
    MULTIPLE_DATE_PART_DATE_PARTS,
)
def test_get_split_query_for_data_for_batch_identifiers_for_split_on_date_parts_multiple_date_parts(
    date_parts, sa
):
    """What does this test and why?
    get_split_query_for_data_for_batch_identifiers_for_split_on_date_parts should
    return the correct query when passed any valid set of parameters including multiple date parts.
    """
    data_splitter: SqlAlchemyDataSplitter = SqlAlchemyDataSplitter()
    table_name: str = "table_name"
    column_name: str = "column_name"

    result: sa.sql.elements.BooleanClauseList = data_splitter.get_split_query_for_data_for_batch_identifiers_for_split_on_date_parts(
        table_name=table_name,
        column_name=column_name,
        date_parts=date_parts,
    )

    assert isinstance(result, sa.sql.Select)

    query_str: str = (
        str(result.compile(compile_kwargs={"literal_binds": True}))
        .replace("\n", "")
        .replace(" ", "")
        .lower()
    )
    assert (
        query_str
        == (
            "SELECT distinct(concat("
            "CAST(EXTRACT(year FROM column_name) AS VARCHAR), CAST(EXTRACT(month FROM column_name) AS VARCHAR)"
            ")) AS concat_distinct_values,"
            "CAST(EXTRACT(year FROM column_name) AS INTEGER) AS year,"
            "CAST(EXTRACT(month FROM column_name) AS INTEGER) AS month"
            "FROM table_name"
        )
        .replace("\n", "")
        .replace(" ", "")
        .lower()
    )


@pytest.mark.parametrize(
    "underscore_prefix",
    [
        pytest.param("_", id="underscore prefix"),
        pytest.param("", id="no underscore prefix"),
    ],
)
@pytest.mark.parametrize(
    "splitter_method_name",
    [
        pytest.param(splitter_method_name, id=splitter_method_name)
        for splitter_method_name in [
            "split_on_year",
            "split_on_year_and_month",
            "split_on_year_and_month_and_day",
            "split_on_date_parts",
            "split_on_whole_table",
            "split_on_column_value",
            "split_on_converted_datetime",
            "split_on_divided_integer",
            "split_on_mod_integer",
            "split_on_multi_column_values",
            "split_on_hashed_column",
        ]
    ],
)
def test_get_splitter_method(underscore_prefix: str, splitter_method_name: str):
    data_splitter: SqlAlchemyDataSplitter = SqlAlchemyDataSplitter()

    splitter_method_name_with_prefix = f"{underscore_prefix}{splitter_method_name}"

    assert data_splitter.get_splitter_method(
        splitter_method_name_with_prefix
    ) == getattr(data_splitter, splitter_method_name)


def ten_trips_per_month_df() -> pd.DataFrame:
    csv_path: str = file_relative_path(
        os.path.dirname(os.path.dirname(__file__)),
        os.path.join(
            "test_sets",
            "taxi_yellow_tripdata_samples",
            "ten_trips_from_each_month",
            "yellow_tripdata_sample_10_trips_from_each_month.csv",
        ),
    )
    # Convert pickup_datetime to a datetime type column
    df: pd.DataFrame = pd.read_csv(csv_path)
    column_names_to_convert: List[str] = ["pickup_datetime", "dropoff_datetime"]
    for column_name_to_convert in column_names_to_convert:
        df[column_name_to_convert] = pd.to_datetime(df[column_name_to_convert])
    return df


@pytest.fixture
def in_memory_sqlite_taxi_ten_trips_per_month_execution_engine(sa):
    engine: SqlAlchemyExecutionEngine = build_sa_engine(ten_trips_per_month_df(), sa)
    return engine


TAXI_SPLITTING_TEST_CASES: TaxiSplittingTestCases = TaxiSplittingTestCases(
    taxi_test_data=TaxiTestData(
        test_df=ten_trips_per_month_df(), test_column_name="pickup_datetime"
    )
)


@pytest.mark.integration
@pytest.mark.parametrize(
    "test_case",
    [
        pytest.param(test_case, id=test_case.splitter_method_name)
        for test_case in TAXI_SPLITTING_TEST_CASES.test_cases()
    ],
)
def test_sqlite_split(
    test_case, sa, in_memory_sqlite_taxi_ten_trips_per_month_execution_engine
):
    """What does this test and why?
    splitters should work with sqlite.
    """

    engine: SqlAlchemyExecutionEngine = (
        in_memory_sqlite_taxi_ten_trips_per_month_execution_engine
    )

    batch_spec: SqlAlchemyDatasourceBatchSpec = SqlAlchemyDatasourceBatchSpec(
        table_name="test",
        schema_name="main",
        splitter_method=test_case.splitter_method_name,
        splitter_kwargs=test_case.splitter_kwargs,
        batch_identifiers={"pickup_datetime": test_case.expected_pickup_datetimes[0]},
    )
    batch_data: SqlAlchemyBatchData = engine.get_batch_data(batch_spec=batch_spec)

    # Right number of rows?
    num_rows: int = batch_data.execution_engine.engine.execute(
        sa.select([sa.func.count()]).select_from(batch_data.selectable)
    ).scalar()
    assert num_rows == test_case.num_expected_rows_in_first_batch_definition


@pytest.mark.integration
def test_sqlite_split_on_year(
    sa, in_memory_sqlite_taxi_ten_trips_per_month_execution_engine
):
    """What does this test and why?
    splitters should work with sqlite and return the correct rows.
    """

    engine: SqlAlchemyExecutionEngine = (
        in_memory_sqlite_taxi_ten_trips_per_month_execution_engine
    )

    n: int = 120
    batch_spec: SqlAlchemyDatasourceBatchSpec = SqlAlchemyDatasourceBatchSpec(
        table_name="test",
        schema_name="main",
        splitter_method="split_on_year",
        splitter_kwargs={"column_name": "pickup_datetime"},
        batch_identifiers={"pickup_datetime": "2018"},
    )
    batch_data: SqlAlchemyBatchData = engine.get_batch_data(batch_spec=batch_spec)

    # Right number of rows?
    num_rows: int = batch_data.execution_engine.engine.execute(
        sa.select([sa.func.count()]).select_from(batch_data.selectable)
    ).scalar()
    assert num_rows == n

    # Right rows?
    rows: sa.Row = batch_data.execution_engine.engine.execute(
        sa.select([sa.text("*")]).select_from(batch_data.selectable)
    ).fetchall()

    row_dates: List[datetime.datetime] = [parse(row["pickup_datetime"]) for row in rows]
    for row_date in row_dates:
        assert row_date.month >= 1
        assert row_date.month <= 12
        assert row_date.year == 2018


@pytest.mark.integration
def test_sqlite_split_and_sample_using_limit(
    sa, in_memory_sqlite_taxi_ten_trips_per_month_execution_engine
):
    """What does this test and why?
    splitters and samplers should work together in sqlite.
    """

    engine: SqlAlchemyExecutionEngine = (
        in_memory_sqlite_taxi_ten_trips_per_month_execution_engine
    )

    n: int = 3
    batch_spec: SqlAlchemyDatasourceBatchSpec = SqlAlchemyDatasourceBatchSpec(
        table_name="test",
        schema_name="main",
        sampling_method="sample_using_limit",
        sampling_kwargs={"n": n},
        splitter_method="split_on_year",
        splitter_kwargs={"column_name": "pickup_datetime"},
        batch_identifiers={"pickup_datetime": "2018"},
    )
    batch_data: SqlAlchemyBatchData = engine.get_batch_data(batch_spec=batch_spec)

    # Right number of rows?
    num_rows: int = batch_data.execution_engine.engine.execute(
        sa.select([sa.func.count()]).select_from(batch_data.selectable)
    ).scalar()
    assert num_rows == n

    # Right rows?
    rows: sa.Row = batch_data.execution_engine.engine.execute(
        sa.select([sa.text("*")]).select_from(batch_data.selectable)
    ).fetchall()

    row_dates: List[datetime.datetime] = [parse(row["pickup_datetime"]) for row in rows]
    for row_date in row_dates:
        assert row_date.month == 1
        assert row_date.year == 2018
