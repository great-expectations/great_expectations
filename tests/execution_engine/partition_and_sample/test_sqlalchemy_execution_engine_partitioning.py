from __future__ import annotations

import datetime
import os
from typing import List
from unittest import mock

import pandas as pd
import pytest
from dateutil.parser import parse

from great_expectations.core.batch_spec import SqlAlchemyDatasourceBatchSpec
from great_expectations.data_context.util import file_relative_path
from great_expectations.execution_engine import SqlAlchemyExecutionEngine
from great_expectations.execution_engine.partition_and_sample.sqlalchemy_data_partitioner import (
    DatePart,
    SqlAlchemyDataPartitioner,
)
from great_expectations.execution_engine.sqlalchemy_batch_data import (
    SqlAlchemyBatchData,
)
from great_expectations.self_check.util import build_sa_execution_engine
from tests.execution_engine.partition_and_sample.partition_and_sample_test_cases import (
    MULTIPLE_DATE_PART_BATCH_IDENTIFIERS,
    MULTIPLE_DATE_PART_DATE_PARTS,
    SINGLE_DATE_PART_BATCH_IDENTIFIERS,
    SINGLE_DATE_PART_DATE_PARTS,
)

# Here we add SqlAlchemyDataPartitioner specific test cases to the generic test cases:
from tests.integration.fixtures.partition_and_sample_data.partitioner_test_cases_and_fixtures import (  # noqa: E501
    TaxiPartitioningTestCase,
    TaxiPartitioningTestCasesBase,
    TaxiPartitioningTestCasesDateTime,
    TaxiPartitioningTestCasesWholeTable,
    TaxiTestData,
)
from tests.test_utils import convert_string_columns_to_datetime

SINGLE_DATE_PART_DATE_PARTS += [
    pytest.param(
        [SqlAlchemyDataPartitioner.date_part.MONTH],
        id="month getting date parts from SqlAlchemyDataPartitioner.date_part",
    )
]
MULTIPLE_DATE_PART_DATE_PARTS += [
    pytest.param(
        [
            SqlAlchemyDataPartitioner.date_part.YEAR,
            SqlAlchemyDataPartitioner.date_part.MONTH,
        ],
        id="year_month getting date parts from SqlAlchemyDataPartitioner.date_part",
    )
]

pytestmark = [
    pytest.mark.sqlalchemy_version_compatibility,
    pytest.mark.external_sqldialect,
]


@mock.patch(
    "great_expectations.execution_engine.partition_and_sample.sqlalchemy_data_partitioner.SqlAlchemyDataPartitioner.partition_on_date_parts"
)
@pytest.mark.parametrize(
    "partitioner_method_name,called_with_date_parts",
    [
        ("partition_on_year", [DatePart.YEAR]),
        ("partition_on_year_and_month", [DatePart.YEAR, DatePart.MONTH]),
        (
            "partition_on_year_and_month_and_day",
            [DatePart.YEAR, DatePart.MONTH, DatePart.DAY],
        ),
    ],
)
@pytest.mark.sqlite
def test_named_date_part_methods(
    mock_partition_on_date_parts: mock.MagicMock,  # noqa: TID251
    partitioner_method_name: str,
    called_with_date_parts: List[DatePart],
):
    """Test that a partially pre-filled version of partition_on_date_parts() was called with the appropriate params.
    For example, partition_on_year.
    """  # noqa: E501
    data_partitioner: SqlAlchemyDataPartitioner = SqlAlchemyDataPartitioner(dialect="sqlite")
    column_name: str = "column_name"
    batch_identifiers: dict = {column_name: {"year": 2018, "month": 10, "day": 31}}

    getattr(data_partitioner, partitioner_method_name)(
        column_name=column_name,
        batch_identifiers=batch_identifiers,
    )

    mock_partition_on_date_parts.assert_called_with(
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
@pytest.mark.sqlite
def test_partition_on_date_parts_single_date_parts(batch_identifiers_for_column, date_parts, sa):
    """What does this test and why?

    partition_on_date_parts should still build the correct query when passed a single element list
     date_parts that is a string, DatePart enum objects, mixed case string.
     To match our interface it should accept a dateutil parseable string as the batch identifier
     or a datetime and also fail when parameters are invalid.
    """

    data_partitioner: SqlAlchemyDataPartitioner = SqlAlchemyDataPartitioner(dialect="sqlite")
    column_name: str = "column_name"

    result: sa.sql.elements.BooleanClauseList = data_partitioner.partition_on_date_parts(
        column_name=column_name,
        batch_identifiers={column_name: batch_identifiers_for_column},
        date_parts=date_parts,
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
@pytest.mark.sqlite
def test_partition_on_date_parts_multiple_date_parts(batch_identifiers_for_column, date_parts, sa):
    """What does this test and why?

    partition_on_date_parts should still build the correct query when passed
     date parts that are strings, DatePart enum objects, a mixture and mixed case.
     To match our interface it should accept a dateutil parseable string as the batch identifier
     or a datetime and also fail when parameters are invalid.
    """

    data_partitioner: SqlAlchemyDataPartitioner = SqlAlchemyDataPartitioner(dialect="sqlite")
    column_name: str = "column_name"

    result: sa.sql.elements.BooleanClauseList = data_partitioner.partition_on_date_parts(
        column_name=column_name,
        batch_identifiers={column_name: batch_identifiers_for_column},
        date_parts=date_parts,
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
    "great_expectations.execution_engine.partition_and_sample.sqlalchemy_data_partitioner.SqlAlchemyDataPartitioner.get_data_for_batch_identifiers_for_partition_on_date_parts"
)
@mock.patch("great_expectations.execution_engine.execution_engine.ExecutionEngine")
@pytest.mark.sqlite
def test_get_data_for_batch_identifiers_year(
    mock_execution_engine: mock.MagicMock,  # noqa: TID251
    mock_get_data_for_batch_identifiers_for_partition_on_date_parts: mock.MagicMock,  # noqa: TID251
):
    """test that get_data_for_batch_identifiers_for_partition_on_date_parts() was called with the appropriate params."""  # noqa: E501
    data_partitioner: SqlAlchemyDataPartitioner = SqlAlchemyDataPartitioner(dialect="sqlite")
    # selectable should be a sa.Selectable object but since we are mocking out
    # get_data_for_batch_identifiers_for_partition_on_date_parts
    # and just verifying its getting passed through, we ignore the type here.
    selectable: str = "mock_selectable"
    column_name: str = "column_name"

    data_partitioner.get_data_for_batch_identifiers_year(
        execution_engine=mock_execution_engine,
        selectable=selectable,
        column_name=column_name,
    )

    mock_get_data_for_batch_identifiers_for_partition_on_date_parts.assert_called_with(
        execution_engine=mock_execution_engine,
        selectable=selectable,
        column_name=column_name,
        date_parts=[DatePart.YEAR],
    )


@mock.patch(
    "great_expectations.execution_engine.partition_and_sample.sqlalchemy_data_partitioner.SqlAlchemyDataPartitioner.get_data_for_batch_identifiers_for_partition_on_date_parts"
)
@mock.patch("great_expectations.execution_engine.execution_engine.ExecutionEngine")
@pytest.mark.sqlite
def test_get_data_for_batch_identifiers_year_and_month(
    mock_execution_engine: mock.MagicMock,  # noqa: TID251
    mock_get_data_for_batch_identifiers_for_partition_on_date_parts: mock.MagicMock,  # noqa: TID251
):
    """test that get_data_for_batch_identifiers_for_partition_on_date_parts() was called with the appropriate params."""  # noqa: E501
    data_partitioner: SqlAlchemyDataPartitioner = SqlAlchemyDataPartitioner(dialect="sqlite")
    selectable: str = "mock_selectable"
    column_name: str = "column_name"

    data_partitioner.get_data_for_batch_identifiers_year_and_month(
        execution_engine=mock_execution_engine,
        selectable=selectable,
        column_name=column_name,
    )

    mock_get_data_for_batch_identifiers_for_partition_on_date_parts.assert_called_with(
        execution_engine=mock_execution_engine,
        selectable=selectable,
        column_name=column_name,
        date_parts=[DatePart.YEAR, DatePart.MONTH],
    )


@mock.patch(
    "great_expectations.execution_engine.partition_and_sample.sqlalchemy_data_partitioner.SqlAlchemyDataPartitioner.get_data_for_batch_identifiers_for_partition_on_date_parts"
)
@mock.patch("great_expectations.execution_engine.execution_engine.ExecutionEngine")
@pytest.mark.sqlite
def test_get_data_for_batch_identifiers_year_and_month_and_day(
    mock_execution_engine: mock.MagicMock,  # noqa: TID251
    mock_get_data_for_batch_identifiers_for_partition_on_date_parts: mock.MagicMock,  # noqa: TID251
):
    """test that get_data_for_batch_identifiers_for_partition_on_date_parts() was called with the appropriate params."""  # noqa: E501
    data_partitioner: SqlAlchemyDataPartitioner = SqlAlchemyDataPartitioner(dialect="sqlite")
    selectable: str = "mock_selectable"
    column_name: str = "column_name"

    data_partitioner.get_data_for_batch_identifiers_year_and_month_and_day(
        execution_engine=mock_execution_engine,
        selectable=selectable,
        column_name=column_name,
    )

    mock_get_data_for_batch_identifiers_for_partition_on_date_parts.assert_called_with(
        execution_engine=mock_execution_engine,
        selectable=selectable,
        column_name=column_name,
        date_parts=[DatePart.YEAR, DatePart.MONTH, DatePart.DAY],
    )


@pytest.mark.parametrize(
    "date_parts",
    SINGLE_DATE_PART_DATE_PARTS,
)
@pytest.mark.sqlite
def test_get_partition_query_for_data_for_batch_identifiers_for_partition_on_date_parts_single_date_parts(  # noqa: E501
    date_parts, sa
):
    """What does this test and why?

    get_partition_query_for_data_for_batch_identifiers_for_partition_on_date_parts should still build the correct
    query when passed a single element list of date_parts that is a string, DatePart enum object, or mixed case string.
    """  # noqa: E501

    data_partitioner: SqlAlchemyDataPartitioner = SqlAlchemyDataPartitioner(dialect="sqlite")
    selectable: sa.sql.Selectable = sa.text("table_name")
    column_name: str = "column_name"

    result: sa.sql.elements.BooleanClauseList = data_partitioner.get_partition_query_for_data_for_batch_identifiers_for_partition_on_date_parts(  # noqa: E501
        selectable=selectable,
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
            f"CAST(EXTRACT(month FROM column_name) AS INTEGER) AS month FROM {selectable}"
        )
        .replace("\n", "")
        .replace(" ", "")
        .lower()
    )


@pytest.mark.parametrize(
    "date_parts",
    MULTIPLE_DATE_PART_DATE_PARTS,
)
@pytest.mark.parametrize(
    "dialect,expected_query_str",
    [
        pytest.param(
            "sqlite",
            "SELECT DISTINCT(CAST(EXTRACT(year FROM column_name) AS VARCHAR) || CAST (EXTRACT(month FROM column_name) AS VARCHAR)) AS concat_distinct_values, CAST(EXTRACT(year FROM column_name) AS INTEGER) AS year, CAST(EXTRACT(month FROM column_name) AS INTEGER) AS month FROM table_name",  # noqa: E501
            marks=pytest.mark.sqlite,
            id="sqlite",
        ),
        pytest.param(
            "postgres",
            "SELECT DISTINCT(CONCAT(CONCAT('', CAST(EXTRACT(year FROM column_name) AS VARCHAR)), CAST(EXTRACT(month FROM column_name) AS VARCHAR))) AS concat_distinct_values, CAST(EXTRACT(year FROM column_name) AS INTEGER) AS year, CAST(EXTRACT(month FROM column_name) AS INTEGER) AS month FROM table_name",  # noqa: E501
            marks=pytest.mark.postgresql,
            id="postgres",
        ),
    ],
)
def test_get_partition_query_for_data_for_batch_identifiers_for_partition_on_date_parts_multiple_date_parts(  # noqa: E501
    date_parts, dialect, expected_query_str, sa
):
    """What does this test and why?
    get_partition_query_for_data_for_batch_identifiers_for_partition_on_date_parts should
    return the correct query when passed any valid set of parameters including multiple date parts.
    """
    data_partitioner: SqlAlchemyDataPartitioner = SqlAlchemyDataPartitioner(dialect=dialect)
    selectable: sa.sql.Selectable = sa.text("table_name")
    column_name: str = "column_name"

    result: sa.sql.elements.BooleanClauseList = data_partitioner.get_partition_query_for_data_for_batch_identifiers_for_partition_on_date_parts(  # noqa: E501
        selectable=selectable,
        column_name=column_name,
        date_parts=date_parts,
    )

    assert isinstance(result, sa.sql.Select)

    actual_query_str: str = (
        str(result.compile(compile_kwargs={"literal_binds": True}))
        .replace("\n", "")
        .replace(" ", "")
        .lower()
    )
    assert actual_query_str == expected_query_str.replace("\n", "").replace(" ", "").lower()


@pytest.mark.parametrize(
    "underscore_prefix",
    [
        pytest.param("_", id="underscore prefix"),
        pytest.param("", id="no underscore prefix"),
    ],
)
@pytest.mark.parametrize(
    "partitioner_method_name",
    [
        pytest.param(partitioner_method_name, id=partitioner_method_name)
        for partitioner_method_name in [
            "partition_on_year",
            "partition_on_year_and_month",
            "partition_on_year_and_month_and_day",
            "partition_on_date_parts",
            "partition_on_whole_table",
            "partition_on_column_value",
            "partition_on_converted_datetime",
            "partition_on_divided_integer",
            "partition_on_mod_integer",
            "partition_on_multi_column_values",
            "partition_on_hashed_column",
        ]
    ],
)
@pytest.mark.sqlite
def test_get_partitioner_method(underscore_prefix: str, partitioner_method_name: str):
    data_partitioner: SqlAlchemyDataPartitioner = SqlAlchemyDataPartitioner(dialect="sqlite")

    partitioner_method_name_with_prefix = f"{underscore_prefix}{partitioner_method_name}"

    assert data_partitioner.get_partitioner_method(partitioner_method_name_with_prefix) == getattr(
        data_partitioner, partitioner_method_name
    )


def ten_trips_per_month_df() -> pd.DataFrame:
    csv_path: str = file_relative_path(
        os.path.dirname(os.path.dirname(__file__)),  # noqa: PTH120
        os.path.join(  # noqa: PTH118
            "test_sets",
            "taxi_yellow_tripdata_samples",
            "ten_trips_from_each_month",
            "yellow_tripdata_sample_10_trips_from_each_month.csv",
        ),
    )
    df: pd.DataFrame = pd.read_csv(csv_path)
    return df


@pytest.fixture
def in_memory_sqlite_taxi_ten_trips_per_month_execution_engine(sa):
    df: pd.DataFrame = ten_trips_per_month_df()
    convert_string_columns_to_datetime(
        df=df, column_names_to_convert=["pickup_datetime", "dropoff_datetime"]
    )
    engine: SqlAlchemyExecutionEngine = build_sa_execution_engine(df, sa)
    return engine


@pytest.mark.parametrize(
    "taxi_test_cases",
    [
        TaxiPartitioningTestCasesWholeTable(
            taxi_test_data=TaxiTestData(
                test_df=ten_trips_per_month_df(),
                test_column_name=None,
                test_column_names=None,
                column_names_to_convert=["pickup_datetime", "dropoff_datetime"],
            )
        ),
        TaxiPartitioningTestCasesDateTime(
            taxi_test_data=TaxiTestData(
                test_df=ten_trips_per_month_df(),
                test_column_name="pickup_datetime",
                test_column_names=None,
                column_names_to_convert=["pickup_datetime", "dropoff_datetime"],
            )
        ),
    ],
)
@pytest.mark.xfail(reason="To be implemented in V1-305", strict=True)
@pytest.mark.sqlite
def test_sqlite_partition(
    taxi_test_cases: TaxiPartitioningTestCasesBase,
    sa,
):
    """What does this test and why?
    partitioners should work with sqlite.
    """
    engine: SqlAlchemyExecutionEngine = build_sa_execution_engine(taxi_test_cases.test_df, sa)

    test_cases: List[TaxiPartitioningTestCase] = taxi_test_cases.test_cases()
    test_case: TaxiPartitioningTestCase
    batch_spec: SqlAlchemyDatasourceBatchSpec
    for test_case in test_cases:
        if test_case.table_domain_test_case:
            batch_spec = SqlAlchemyDatasourceBatchSpec(
                table_name="test",
                schema_name="main",
                partitioner_method=test_case.add_batch_definition_method_name,
                partitioner_kwargs=test_case.add_batch_definition_kwargs,
                batch_identifiers={},
            )
        else:  # noqa: PLR5501
            if taxi_test_cases.test_column_name:
                assert test_case.expected_column_values is not None
                batch_spec = SqlAlchemyDatasourceBatchSpec(
                    table_name="test",
                    schema_name="main",
                    partitioner_method=test_case.add_batch_definition_method_name,
                    partitioner_kwargs=test_case.add_batch_definition_kwargs,
                    batch_identifiers={
                        taxi_test_cases.test_column_name: test_case.expected_column_values[0]
                    },
                )
            elif taxi_test_cases.test_column_names:
                assert test_case.expected_column_values is not None
                column_name: str
                batch_spec = SqlAlchemyDatasourceBatchSpec(
                    table_name="test",
                    schema_name="main",
                    partitioner_method=test_case.add_batch_definition_method_name,
                    partitioner_kwargs=test_case.add_batch_definition_kwargs,
                    batch_identifiers={
                        column_name: test_case.expected_column_values[0][column_name]
                        for column_name in taxi_test_cases.test_column_names
                    },
                )
            else:
                raise ValueError("Missing test_column_names or test_column_names attribute.")

        batch_data: SqlAlchemyBatchData = engine.get_batch_data(batch_spec=batch_spec)

        # Right number of rows?
        num_rows: int = batch_data.execution_engine.execute_query(
            sa.select(sa.func.count()).select_from(batch_data.selectable)
        ).scalar()
        # noinspection PyUnresolvedReferences
        assert num_rows == test_case.num_expected_rows_in_first_batch_definition


@pytest.mark.sqlite
def test_sqlite_partition_on_year(sa, in_memory_sqlite_taxi_ten_trips_per_month_execution_engine):
    """What does this test and why?
    partitioners should work with sqlite and return the correct rows.
    """

    engine: SqlAlchemyExecutionEngine = in_memory_sqlite_taxi_ten_trips_per_month_execution_engine

    n: int = 120
    batch_spec: SqlAlchemyDatasourceBatchSpec = SqlAlchemyDatasourceBatchSpec(
        table_name="test",
        schema_name="main",
        partitioner_method="partition_on_year",
        partitioner_kwargs={"column_name": "pickup_datetime"},
        batch_identifiers={"pickup_datetime": "2018"},
    )
    batch_data: SqlAlchemyBatchData = engine.get_batch_data(batch_spec=batch_spec)

    # Right number of rows?
    num_rows: int = batch_data.execution_engine.execute_query(
        sa.select(sa.func.count()).select_from(batch_data.selectable)
    ).scalar()
    assert num_rows == n

    # Right rows?
    rows: list[sa.RowMapping] = (
        batch_data.execution_engine.execute_query(
            sa.select(sa.text("*")).select_from(batch_data.selectable)
        )
        .mappings()
        .fetchall()
    )

    row_dates: List[datetime.datetime] = [parse(row["pickup_datetime"]) for row in rows]
    for row_date in row_dates:
        assert row_date.month >= 1
        assert row_date.month <= 12
        assert row_date.year == 2018


@pytest.mark.sqlite
def test_sqlite_partition_and_sample_using_limit(
    sa, in_memory_sqlite_taxi_ten_trips_per_month_execution_engine
):
    """What does this test and why?
    partitioners and samplers should work together in sqlite.
    """

    engine: SqlAlchemyExecutionEngine = in_memory_sqlite_taxi_ten_trips_per_month_execution_engine

    n: int = 3
    batch_spec: SqlAlchemyDatasourceBatchSpec = SqlAlchemyDatasourceBatchSpec(
        table_name="test",
        schema_name="main",
        sampling_method="sample_using_limit",
        sampling_kwargs={"n": n},
        partitioner_method="partition_on_year",
        partitioner_kwargs={"column_name": "pickup_datetime"},
        batch_identifiers={"pickup_datetime": "2018"},
    )
    batch_data: SqlAlchemyBatchData = engine.get_batch_data(batch_spec=batch_spec)

    # Right number of rows?
    num_rows: int = batch_data.execution_engine.execute_query(
        sa.select(sa.func.count()).select_from(batch_data.selectable)
    ).scalar()
    assert num_rows == n

    # Right rows?
    rows: list[sa.RowMapping] = (
        batch_data.execution_engine.execute_query(
            sa.select(sa.text("*")).select_from(batch_data.selectable)
        )
        .mappings()
        .fetchall()
    )

    row_dates: List[datetime.datetime] = [parse(row["pickup_datetime"]) for row in rows]
    for row_date in row_dates:
        assert row_date.month == 1
        assert row_date.year == 2018
