from __future__ import annotations

import datetime
import hashlib
import os
from typing import TYPE_CHECKING, List
from unittest import mock

import pandas as pd
import pytest
import sqlalchemy
import sqlalchemy.dialects.mssql as mssql_dialect
import sqlalchemy.dialects.mysql as mysql_dialect
import sqlalchemy.dialects.oracle as oracle_dialect
import sqlalchemy.dialects.postgresql as postgresql_dialect
import sqlalchemy.dialects.sqlite as sqlite_dialect
from dateutil.parser import parse

from great_expectations.core.batch_spec import SqlAlchemyDatasourceBatchSpec
from great_expectations.data_context.util import file_relative_path
from great_expectations.execution_engine import SqlAlchemyExecutionEngine
from great_expectations.execution_engine.partition_and_sample.sqlalchemy_data_partitioner import (
    DatePart,
    SqlAlchemyDataPartitioner,
)
from great_expectations.self_check.util import build_sa_execution_engine
from tests.execution_engine.partition_and_sample.partition_and_sample_test_cases import (
    MULTIPLE_DATE_PART_BATCH_IDENTIFIERS,
    MULTIPLE_DATE_PART_DATE_PARTS,
    SINGLE_DATE_PART_BATCH_IDENTIFIERS,
    SINGLE_DATE_PART_DATE_PARTS,
)

# Here we add SqlAlchemyDataPartitioner specific test cases to the generic test cases:
from tests.integration.fixtures.partition_and_sample_data.partitioner_test_cases_and_fixtures import (  # noqa: E501 # FIXME CoP
    TaxiPartitioningTestCase,
    TaxiPartitioningTestCasesBase,
    TaxiPartitioningTestCasesDateTime,
    TaxiPartitioningTestCasesWholeTable,
    TaxiTestData,
)
from tests.test_utils import convert_string_columns_to_datetime

if TYPE_CHECKING:
    from great_expectations.execution_engine.sqlalchemy_batch_data import (
        SqlAlchemyBatchData,
    )

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
    mock_partition_on_date_parts: mock.MagicMock,  # noqa: TID251 # FIXME CoP
    partitioner_method_name: str,
    called_with_date_parts: List[DatePart],
):
    """Test that a partially pre-filled version of partition_on_date_parts() was called with the appropriate params.
    For example, partition_on_year.
    """  # noqa: E501 # FIXME CoP
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
    mock_execution_engine: mock.MagicMock,  # noqa: TID251 # FIXME CoP
    mock_get_data_for_batch_identifiers_for_partition_on_date_parts: mock.MagicMock,  # noqa: TID251 # FIXME CoP
):
    """test that get_data_for_batch_identifiers_for_partition_on_date_parts() was called with the appropriate params."""  # noqa: E501 # FIXME CoP
    data_partitioner: SqlAlchemyDataPartitioner = SqlAlchemyDataPartitioner(dialect="sqlite")
    # get_data_for_batch_identifiers_for_partition_on_date_parts is mocked out, so this is only
    # ever passed through -- but it is passed as the Selectable the signature declares.
    selectable: sqlalchemy.TableClause = sqlalchemy.table("mock_selectable")
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
    mock_execution_engine: mock.MagicMock,  # noqa: TID251 # FIXME CoP
    mock_get_data_for_batch_identifiers_for_partition_on_date_parts: mock.MagicMock,  # noqa: TID251 # FIXME CoP
):
    """test that get_data_for_batch_identifiers_for_partition_on_date_parts() was called with the appropriate params."""  # noqa: E501 # FIXME CoP
    data_partitioner: SqlAlchemyDataPartitioner = SqlAlchemyDataPartitioner(dialect="sqlite")
    selectable: sqlalchemy.TableClause = sqlalchemy.table("mock_selectable")
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
    mock_execution_engine: mock.MagicMock,  # noqa: TID251 # FIXME CoP
    mock_get_data_for_batch_identifiers_for_partition_on_date_parts: mock.MagicMock,  # noqa: TID251 # FIXME CoP
):
    """test that get_data_for_batch_identifiers_for_partition_on_date_parts() was called with the appropriate params."""  # noqa: E501 # FIXME CoP
    data_partitioner: SqlAlchemyDataPartitioner = SqlAlchemyDataPartitioner(dialect="sqlite")
    selectable: sqlalchemy.TableClause = sqlalchemy.table("mock_selectable")
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
def test_get_partition_query_for_data_for_batch_identifiers_for_partition_on_date_parts_single_date_parts(  # noqa: E501 # FIXME CoP
    date_parts, sa
):
    """What does this test and why?

    get_partition_query_for_data_for_batch_identifiers_for_partition_on_date_parts should still build the correct
    query when passed a single element list of date_parts that is a string, DatePart enum object, or mixed case string.
    """  # noqa: E501 # FIXME CoP

    data_partitioner: SqlAlchemyDataPartitioner = SqlAlchemyDataPartitioner(dialect="sqlite")
    selectable: sa.sql.Selectable = sa.text("table_name")
    column_name: str = "column_name"

    result: sa.sql.elements.BooleanClauseList = data_partitioner.get_partition_query_for_data_for_batch_identifiers_for_partition_on_date_parts(  # noqa: E501 # FIXME CoP
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
            "SELECT DISTINCT(CAST(EXTRACT(year FROM column_name) AS VARCHAR) || CAST (EXTRACT(month FROM column_name) AS VARCHAR)) AS concat_distinct_values, CAST(EXTRACT(year FROM column_name) AS INTEGER) AS year, CAST(EXTRACT(month FROM column_name) AS INTEGER) AS month FROM table_name",  # noqa: E501 # FIXME CoP
            marks=pytest.mark.sqlite,
            id="sqlite",
        ),
        pytest.param(
            "postgres",
            "SELECT DISTINCT(CONCAT(CONCAT('', CAST(EXTRACT(year FROM column_name) AS VARCHAR)), CAST(EXTRACT(month FROM column_name) AS VARCHAR))) AS concat_distinct_values, CAST(EXTRACT(year FROM column_name) AS INTEGER) AS year, CAST(EXTRACT(month FROM column_name) AS INTEGER) AS month FROM table_name",  # noqa: E501 # FIXME CoP
            marks=pytest.mark.postgresql,
            id="postgres",
        ),
    ],
)
def test_get_partition_query_for_data_for_batch_identifiers_for_partition_on_date_parts_multiple_date_parts(  # noqa: E501 # FIXME CoP
    date_parts, dialect, expected_query_str, sa
):
    """What does this test and why?
    get_partition_query_for_data_for_batch_identifiers_for_partition_on_date_parts should
    return the correct query when passed any valid set of parameters including multiple date parts.
    """
    data_partitioner: SqlAlchemyDataPartitioner = SqlAlchemyDataPartitioner(dialect=dialect)
    selectable: sa.sql.Selectable = sa.text("table_name")
    column_name: str = "column_name"

    result: sa.sql.elements.BooleanClauseList = data_partitioner.get_partition_query_for_data_for_batch_identifiers_for_partition_on_date_parts(  # noqa: E501 # FIXME CoP
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


# The partitioner's own `dialect` argument only changes which branch it builds (e.g. the
# SQLite-only concatenation form); the SQLAlchemy compile-time dialect below is what actually
# renders each dialect's SQL text, including its cast target types. Both are set to the same
# target per case so each rendering below reflects how that dialect is exercised at runtime.
PARTITION_QUERY_DIALECT_MODULES_BY_NAME = {
    "sqlite": sqlite_dialect,
    "postgresql": postgresql_dialect,
    "mssql": mssql_dialect,
    "mysql": mysql_dialect,
    "oracle": oracle_dialect,
}

PARTITION_QUERY_DATE_PARTS_BY_COUNT = {
    1: [DatePart.YEAR],
    2: [DatePart.YEAR, DatePart.MONTH],
    3: [DatePart.YEAR, DatePart.MONTH, DatePart.DAY],
}


def _render_partition_query_for_date_parts(dialect_name: str, date_parts: List[DatePart]) -> str:
    """Compile the multi-date-part partition query for one dialect, database-free.

    Compiling (rather than executing) against a bare SQLAlchemy dialect object requires neither
    a driver nor a live service, so this is safe to run in any environment.
    """
    data_partitioner = SqlAlchemyDataPartitioner(dialect=dialect_name)
    selectable = sqlalchemy.table("table_name")
    query = data_partitioner.get_partition_query_for_data_for_batch_identifiers_for_partition_on_date_parts(  # noqa: E501 # FIXME CoP
        selectable=selectable,
        column_name="column_name",
        date_parts=date_parts,
    )
    compiled = query.compile(
        dialect=PARTITION_QUERY_DIALECT_MODULES_BY_NAME[dialect_name].dialect(),
        compile_kwargs={"literal_binds": True},
    )
    rendered = str(compiled).replace("\n", "").replace(" ", "").lower()
    # SQLAlchemy's mssql dialect renders an empty string literal as N'' under SQLAlchemy 1.4
    # and as '' under SQLAlchemy 2.0 (the national-string prefix on a string literal) when
    # compiling the CONCAT construct here. That construct is not something this change touches,
    # so the two forms are normalized to compare equal rather than pinning whichever one the
    # installed SQLAlchemy version happens to render.
    return rendered.replace("concat(n''", "concat(''")


# Pinned exact renderings, one per (dialect, date-part count). Any change to a cast site, an
# extraction call, or the concatenation shape shows up here as a text mismatch, not merely a
# type check.
EXPECTED_PARTITION_QUERY_SQL_BY_DIALECT_AND_DATE_PART_COUNT = {
    ("sqlite", 1): (
        "selectdistinct(cast(strftime('%y',column_name)asinteger))asconcat_distinct_values,"
        "cast(cast(strftime('%y',column_name)asinteger)asinteger)asyearfromtable_name"
    ),
    ("postgresql", 1): (
        "selectdistinct(extract(yearfromcolumn_name))asconcat_distinct_values,"
        "cast(extract(yearfromcolumn_name)asinteger)asyearfromtable_name"
    ),
    ("mssql", 1): (
        "selectdistinct(datepart(year,column_name))asconcat_distinct_values,"
        "cast(datepart(year,column_name)asinteger)asyearfromtable_name"
    ),
    ("mysql", 1): (
        "selectdistinct(extract(yearfromcolumn_name))asconcat_distinct_values,"
        "cast(extract(yearfromcolumn_name)assignedinteger)asyearfromtable_name"
    ),
    ("oracle", 1): (
        "selectdistinct(extract(yearfromcolumn_name))asconcat_distinct_values,"
        "cast(extract(yearfromcolumn_name)asinteger)asyearfromtable_name"
    ),
    ("sqlite", 2): (
        "selectdistinct(cast(cast(strftime('%y',column_name)asinteger)asvarchar)"
        "||cast(cast(strftime('%m',column_name)asinteger)asvarchar))asconcat_distinct_values,"
        "cast(cast(strftime('%y',column_name)asinteger)asinteger)asyear,"
        "cast(cast(strftime('%m',column_name)asinteger)asinteger)asmonthfromtable_name"
    ),
    ("postgresql", 2): (
        "selectdistinct(concat(concat('',cast(extract(yearfromcolumn_name)asvarchar)),"
        "cast(extract(monthfromcolumn_name)asvarchar)))asconcat_distinct_values,"
        "cast(extract(yearfromcolumn_name)asinteger)asyear,"
        "cast(extract(monthfromcolumn_name)asinteger)asmonthfromtable_name"
    ),
    ("mssql", 2): (
        "selectdistinct(concat(concat('',cast(datepart(year,column_name)asvarchar(max))),"
        "cast(datepart(month,column_name)asvarchar(max))))asconcat_distinct_values,"
        "cast(datepart(year,column_name)asinteger)asyear,"
        "cast(datepart(month,column_name)asinteger)asmonthfromtable_name"
    ),
    ("mysql", 2): (
        "selectdistinct(concat(concat('',cast(extract(yearfromcolumn_name)aschar)),"
        "cast(extract(monthfromcolumn_name)aschar)))asconcat_distinct_values,"
        "cast(extract(yearfromcolumn_name)assignedinteger)asyear,"
        "cast(extract(monthfromcolumn_name)assignedinteger)asmonthfromtable_name"
    ),
    ("oracle", 2): (
        "selectdistinct(concat(concat('',cast(extract(yearfromcolumn_name)asvarchar2(4000char))),"
        "cast(extract(monthfromcolumn_name)asvarchar2(4000char))))asconcat_distinct_values,"
        "cast(extract(yearfromcolumn_name)asinteger)asyear,"
        "cast(extract(monthfromcolumn_name)asinteger)asmonthfromtable_name"
    ),
    ("sqlite", 3): (
        "selectdistinct(cast(cast(strftime('%y',column_name)asinteger)asvarchar)"
        "||cast(cast(strftime('%m',column_name)asinteger)asvarchar)"
        "||cast(cast(strftime('%d',column_name)asinteger)asvarchar))asconcat_distinct_values,"
        "cast(cast(strftime('%y',column_name)asinteger)asinteger)asyear,"
        "cast(cast(strftime('%m',column_name)asinteger)asinteger)asmonth,"
        "cast(cast(strftime('%d',column_name)asinteger)asinteger)asdayfromtable_name"
    ),
    ("postgresql", 3): (
        "selectdistinct(concat(concat(concat('',cast(extract(yearfromcolumn_name)asvarchar)),"
        "cast(extract(monthfromcolumn_name)asvarchar)),cast(extract(dayfromcolumn_name)asvarchar)"
        "))asconcat_distinct_values,"
        "cast(extract(yearfromcolumn_name)asinteger)asyear,"
        "cast(extract(monthfromcolumn_name)asinteger)asmonth,"
        "cast(extract(dayfromcolumn_name)asinteger)asdayfromtable_name"
    ),
    ("mssql", 3): (
        "selectdistinct(concat(concat(concat('',cast(datepart(year,column_name)asvarchar(max))),"
        "cast(datepart(month,column_name)asvarchar(max))),"
        "cast(datepart(day,column_name)asvarchar(max))))asconcat_distinct_values,"
        "cast(datepart(year,column_name)asinteger)asyear,"
        "cast(datepart(month,column_name)asinteger)asmonth,"
        "cast(datepart(day,column_name)asinteger)asdayfromtable_name"
    ),
    ("mysql", 3): (
        "selectdistinct(concat(concat(concat('',cast(extract(yearfromcolumn_name)aschar)),"
        "cast(extract(monthfromcolumn_name)aschar)),cast(extract(dayfromcolumn_name)aschar)"
        "))asconcat_distinct_values,"
        "cast(extract(yearfromcolumn_name)assignedinteger)asyear,"
        "cast(extract(monthfromcolumn_name)assignedinteger)asmonth,"
        "cast(extract(dayfromcolumn_name)assignedinteger)asdayfromtable_name"
    ),
    ("oracle", 3): (
        "selectdistinct(concat(concat(concat('',cast(extract(yearfromcolumn_name)asvarchar2(4000char))),"
        "cast(extract(monthfromcolumn_name)asvarchar2(4000char))),"
        "cast(extract(dayfromcolumn_name)asvarchar2(4000char))))asconcat_distinct_values,"
        "cast(extract(yearfromcolumn_name)asinteger)asyear,"
        "cast(extract(monthfromcolumn_name)asinteger)asmonth,"
        "cast(extract(dayfromcolumn_name)asinteger)asdayfromtable_name"
    ),
}


@pytest.mark.unit
@pytest.mark.parametrize(
    "dialect_name,date_part_count",
    sorted(EXPECTED_PARTITION_QUERY_SQL_BY_DIALECT_AND_DATE_PART_COUNT),
)
def test_partition_query_rendered_sql_is_pinned_across_dialects_and_date_part_counts(
    dialect_name: str, date_part_count: int
):
    """What does this test and why?

    get_partition_query_for_data_for_batch_identifiers_for_partition_on_date_parts renders
    dialect-specific SQL text that is otherwise only exercised end to end against a live
    database. Pinning its exact compiled text for SQLite, Postgres, SQL Server, MySQL, and
    Oracle, at one, two, and three date parts, is what lets a later change to any dialect
    branch, cast site, or concatenation shape be caught here rather than only downstream
    against a real service.
    """
    actual_query_str = _render_partition_query_for_date_parts(
        dialect_name, PARTITION_QUERY_DATE_PARTS_BY_COUNT[date_part_count]
    )
    expected_query_str = EXPECTED_PARTITION_QUERY_SQL_BY_DIALECT_AND_DATE_PART_COUNT[
        (dialect_name, date_part_count)
    ]
    assert actual_query_str == expected_query_str


@pytest.mark.unit
@pytest.mark.parametrize("dialect_name", sorted(PARTITION_QUERY_DIALECT_MODULES_BY_NAME))
def test_partition_query_single_date_part_never_carries_a_string_cast(dialect_name: str):
    """What does this test and why?

    The single-date-part path builds no concatenation, so it casts nothing to a string type at
    all -- only the extracted date part is cast, and that cast is to an integer. This is what
    makes the single-part path's exemption from the multi-part string-cast defect below an
    observation about the rendered SQL, rather than an assumption about the code that produced
    it.
    """
    rendered = _render_partition_query_for_date_parts(
        dialect_name, PARTITION_QUERY_DATE_PARTS_BY_COUNT[1]
    )
    assert "asvarchar" not in rendered
    assert "aschar" not in rendered


@pytest.mark.unit
@pytest.mark.parametrize("date_part_count", [2, 3])
def test_partition_query_oracle_multi_date_part_string_cast_carries_a_length(
    date_part_count: int,
):
    """What does this test and why?

    Engineering fact this assertion pins: SQLAlchemy's Oracle dialect compiles a bare
    ``sa.String`` type to ``VARCHAR2`` with no length, and a ``CAST(... AS VARCHAR2)`` with no
    length is rejected by Oracle's grammar once the query actually executes. Every string-cast
    site in the multi-date-part path now supplies an explicit length, so every one of them
    renders a lengthed ``VARCHAR2`` and none renders the length-less form. The assertion is
    positive and exhaustive by count: a partial migration would leave some sites length-less
    and change this count without necessarily changing whether the length-less form is present
    at all.
    """
    rendered = _render_partition_query_for_date_parts(
        "oracle", PARTITION_QUERY_DATE_PARTS_BY_COUNT[date_part_count]
    )
    assert rendered.count("asvarchar2(4000char)") == date_part_count
    assert "asvarchar2)" not in rendered


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
        os.path.dirname(os.path.dirname(__file__)),  # noqa: PTH120 # FIXME CoP
        os.path.join(  # noqa: PTH118 # FIXME CoP
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
        else:  # noqa: PLR5501 # FIXME CoP
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


def _sqlite_md5_udf_reference(value: object, hash_digits: int) -> str:
    """Reference implementation of the md5 UDF SqlAlchemyExecutionEngine registers on sqlite.

    Recomputed here in Python so the tests below pin the UDF's actual output rather than
    comparing the database against itself.
    """
    return hashlib.md5(str(value).encode("utf-8"), usedforsecurity=False).hexdigest()[
        -1 * hash_digits :
    ]


@pytest.mark.sqlite
def test_sqlite_partition_on_hashed_column(sa):
    """What does this test and why?
    partition_on_hashed_column has no native sqlite implementation; it depends on the md5
    UDF that SqlAlchemyExecutionEngine registers on every sqlite connection. Drive that
    path end to end so a regression in the UDF fails here.
    """
    hash_digits: int = 1
    column_name: str = "id"
    df: pd.DataFrame = pd.DataFrame({column_name: list(range(20))})
    engine: SqlAlchemyExecutionEngine = build_sa_execution_engine(df, sa)

    hash_value: str = _sqlite_md5_udf_reference(df[column_name][0], hash_digits)
    expected_ids: List[int] = [
        value
        for value in df[column_name]
        if _sqlite_md5_udf_reference(value, hash_digits) == hash_value
    ]
    # Guard against a vacuous assertion: the partition must be a non-empty, proper subset.
    assert 0 < len(expected_ids) < len(df)

    batch_spec: SqlAlchemyDatasourceBatchSpec = SqlAlchemyDatasourceBatchSpec(
        table_name="test",
        schema_name="main",
        partitioner_method="partition_on_hashed_column",
        partitioner_kwargs={"column_name": column_name, "hash_digits": hash_digits},
        batch_identifiers={column_name: hash_value},
    )
    batch_data: SqlAlchemyBatchData = engine.get_batch_data(batch_spec=batch_spec)

    rows: list[sa.RowMapping] = (
        engine.execute_query(sa.select(sa.text("*")).select_from(batch_data.selectable))
        .mappings()
        .fetchall()
    )

    assert sorted(row[column_name] for row in rows) == sorted(expected_ids)


@pytest.mark.sqlite
def test_sqlite_get_data_for_batch_identifiers_on_hashed_column(sa):
    """What does this test and why?
    Introspecting the batch identifiers for a hashed-column partitioner runs the same sqlite
    md5 UDF, through a different query. The identifiers it yields must be the real digests.
    """
    hash_digits: int = 2
    column_name: str = "id"
    df: pd.DataFrame = pd.DataFrame({column_name: list(range(20))})
    engine: SqlAlchemyExecutionEngine = build_sa_execution_engine(df, sa)

    data_partitioner: SqlAlchemyDataPartitioner = SqlAlchemyDataPartitioner(dialect="sqlite")
    batch_identifiers_list: List[dict] = data_partitioner.get_data_for_batch_identifiers(
        execution_engine=engine,
        selectable=sa.table("test", schema="main"),
        partitioner_method_name="partition_on_hashed_column",
        partitioner_kwargs={"column_name": column_name, "hash_digits": hash_digits},
    )

    expected_hash_values: set[str] = {
        _sqlite_md5_udf_reference(value, hash_digits) for value in df[column_name]
    }
    assert {
        batch_identifiers[column_name] for batch_identifiers in batch_identifiers_list
    } == expected_hash_values


# A query asset carrying a daily batch definition: the batch spec has both a `query` and a
# `partitioner_method`, which is the combination that skips the unwrapped-query shortcut in
# `_build_selectable_from_batch_spec` and wraps the statement as a subquery instead.
PARTITIONED_QUERY_ASSET_QUERY = "SELECT id, created_at FROM my_table"
PARTITIONED_QUERY_ASSET_BATCH_SPEC = SqlAlchemyDatasourceBatchSpec(
    data_asset_name="query_asset",
    query=PARTITIONED_QUERY_ASSET_QUERY,
    partitioner_method="partition_on_year_and_month_and_day",
    partitioner_kwargs={"column_name": "created_at"},
    batch_identifiers={"created_at": {"year": 2024, "month": 1, "day": 15}},
)

# Pinned exact renderings, one per dialect. Only the Oracle case fails without the fix this
# pins -- it is the dialect that completes a FROM-less SELECT by appending "FROM DUAL", which
# left the wrapped statement carrying two FROM clauses. The other four are here to pin that
# wrapping the whole statement rather than rebuilding it from its column list leaves their
# rendering untouched, so they pass before and after that change by design.
EXPECTED_PARTITIONED_QUERY_ASSET_SQL_BY_DIALECT = {
    "sqlite": (
        "select*from(selectid,created_atfrommy_table)asanon_1"
        "wherecast(strftime('%y',created_at)asinteger)=2024"
        "andcast(strftime('%m',created_at)asinteger)=1"
        "andcast(strftime('%d',created_at)asinteger)=15"
    ),
    "postgresql": (
        "select*from(selectid,created_atfrommy_table)asanon_1"
        "whereextract(yearfromcreated_at)=2024"
        "andextract(monthfromcreated_at)=1"
        "andextract(dayfromcreated_at)=15"
    ),
    "mssql": (
        "select*from(selectid,created_atfrommy_table)asanon_1"
        "wheredatepart(year,created_at)=2024"
        "anddatepart(month,created_at)=1"
        "anddatepart(day,created_at)=15"
    ),
    "mysql": (
        "select*from(selectid,created_atfrommy_table)asanon_1"
        "whereextract(yearfromcreated_at)=2024"
        "andextract(monthfromcreated_at)=1"
        "andextract(dayfromcreated_at)=15"
    ),
    # No "as" before the alias: this dialect's compiler omits it, and that is the form it
    # accepts. The alias is not what made this construct invalid here.
    "oracle": (
        "select*from(selectid,created_atfrommy_table)anon_1"
        "whereextract(yearfromcreated_at)=2024"
        "andextract(monthfromcreated_at)=1"
        "andextract(dayfromcreated_at)=15"
    ),
}


@pytest.mark.unit
@pytest.mark.parametrize(
    "dialect_name", sorted(EXPECTED_PARTITIONED_QUERY_ASSET_SQL_BY_DIALECT), ids=str
)
def test_partitioned_query_asset_selectable_wraps_the_whole_statement(dialect_name: str) -> None:
    """A partitioned query asset's selectable must wrap the user's statement unchanged.

    The statement has to reach the compiler whole. Rebuilding it as a select over everything
    after its "SELECT" produces a select that owns no FROM clause, and the Oracle dialect
    completes such a select by appending "FROM DUAL" -- so the wrapped query went out with two
    FROM clauses and no Oracle version accepted it.

    The engine is built on SQLite because a selectable is dialect-agnostic until it is
    compiled; compiling against a bare dialect object needs neither a driver nor a live
    service, so this runs anywhere.
    """
    execution_engine = SqlAlchemyExecutionEngine(engine=sqlalchemy.create_engine("sqlite://"))

    selectable = execution_engine._build_selectable_from_batch_spec(
        PARTITIONED_QUERY_ASSET_BATCH_SPEC
    )

    compiled = selectable.compile(
        dialect=PARTITION_QUERY_DIALECT_MODULES_BY_NAME[dialect_name].dialect(),
        compile_kwargs={"literal_binds": True},
    )
    rendered = str(compiled).replace("\n", "").replace(" ", "").lower()
    assert rendered == EXPECTED_PARTITIONED_QUERY_ASSET_SQL_BY_DIALECT[dialect_name]
