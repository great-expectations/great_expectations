from typing import Any, ClassVar, Optional
from unittest import mock
from unittest.mock import create_autospec

import pytest
from sqlalchemy.dialects import mysql, oracle  # noqa: F401 # registers sa.dialects.oracle

from great_expectations.compatibility.sqlalchemy import (
    sqlalchemy as sa,
)
from great_expectations.expectations.metrics.query_metric_provider import (
    QueryMetricProvider,
    QueryParameters,
    find_last_top_level_order_by,
    has_top_level_token,
    render_derived_table_alias,
    strip_top_level_order_by,
)
from great_expectations.expectations.metrics.query_metrics import (
    QueryColumn,
    QueryColumnPair,
    QueryMultipleColumns,
    QueryRowCount,
    QueryTable,
    QueryTemplateValues,
)
from tests.expectations.metrics.conftest import MockSqlAlchemyExecutionEngine


def _call_sqlalchemy_metric(metric_cls: Any, **kwargs: Any) -> Any:
    """Invoke a query metric provider's ``_sqlalchemy`` value function directly.

    ``_sqlalchemy`` names its first parameter ``cls`` but is a plain function, not a
    classmethod, so mypy types it (and the ``cls`` argument) as an instance. At runtime
    the metric machinery passes the class, which is what these tests reproduce.
    """
    return metric_cls._sqlalchemy(cls=metric_cls, **kwargs)


@pytest.mark.unit
def test_query_template_get_query_function_with_int():
    """Simple test to ensure that the `get_query()` method for QueryTemplateValue can handle integer value"""  # noqa: E501 # FIXME CoP
    query: str = """
            SELECT {column_to_check}
            FROM {batch}
            WHERE {condition}
            GROUP BY {column_to_check}
            """
    selectable = sa.Table("gx_temp_aaa", sa.MetaData(), schema=None)
    template_dict: dict = {"column_to_check": 1, "condition": "is_open"}
    metric_ob: QueryTemplateValues = QueryTemplateValues()
    formatted_query: str = metric_ob.get_query(query, template_dict, selectable)
    assert (
        formatted_query
        == """
            SELECT 1
            FROM gx_temp_aaa
            WHERE is_open
            GROUP BY 1
            """
    )


@pytest.mark.unit
def test_query_template_get_query_function_with_float():
    """Simple test to ensure that the `get_query()` method for QueryTemplateValue can handle float value"""  # noqa: E501 # FIXME CoP
    query: str = """
            SELECT {column_to_check}
            FROM {batch}
            WHERE {condition}
            GROUP BY {column_to_check}
            """
    selectable = sa.Table("gx_temp_aaa", sa.MetaData(), schema=None)
    template_dict: dict = {"column_to_check": 1.0, "condition": "is_open"}
    metric_ob: QueryTemplateValues = QueryTemplateValues()
    formatted_query: str = metric_ob.get_query(query, template_dict, selectable)
    assert (
        formatted_query
        == """
            SELECT 1.0
            FROM gx_temp_aaa
            WHERE is_open
            GROUP BY 1.0
            """
    )


class MyQueryColumn(QueryColumn):
    metric_name = "my_query.column"
    # The Query* base classes narrow value_keys to a fixed-length tuple by unannotated
    # assignment, though MetricProvider declares it Tuple[str, ...]; these test metrics
    # only need one key.
    value_keys = ("my_query",)  # type: ignore[assignment]

    query_param_name: ClassVar[str] = "my_query"


class MyQueryColumnPair(QueryColumnPair):
    metric_name = "my_query.column_pair"
    value_keys = ("my_query",)  # type: ignore[assignment]

    query_param_name: ClassVar[str] = "my_query"


class MyQueryMultipleColumns(QueryMultipleColumns):
    metric_name = "my_query.multiple_columns"
    value_keys = ("my_query",)  # type: ignore[assignment]

    query_param_name: ClassVar[str] = "my_query"


class MyQueryTable(QueryTable):
    metric_name = "my_query.table"
    value_keys = ("my_query",)

    query_param_name: ClassVar[str] = "my_query"


@pytest.mark.unit
@mock.patch.object(sa, "text")
@mock.patch.object(
    QueryMetricProvider, "_get_substituted_batch_subquery_from_query_and_batch_selectable"
)
@mock.patch.object(QueryMetricProvider, "_get_sqlalchemy_records_from_substituted_batch_subquery")
@pytest.mark.parametrize(
    "metric_class, class_metric_value_kwargs, query_parameters",
    [
        (
            MyQueryColumn,
            {"column": "my_column"},
            QueryParameters(column="my_column"),
        ),
        (
            MyQueryColumnPair,
            {"column_A": "my_column_A", "column_B": "my_column_B"},
            QueryParameters(column_A="my_column_A", column_B="my_column_B"),
        ),
        (
            MyQueryMultipleColumns,
            {"columns": ["my_column_1", "my_column_2", "my_column_3"]},
            QueryParameters(columns=["my_column_1", "my_column_2", "my_column_3"]),
        ),
        (
            MyQueryTable,
            {},
            None,
        ),
    ],
)
def test_sqlalchemy_query_metrics_that_return_records(
    mock_get_sqlalchemy_records_from_substituted_batch_subquery,
    mock_get_substituted_batch_subquery_from_query_and_batch_selectable,
    mock_sqlalchemy_text,
    mock_sqlalchemy_execution_engine: MockSqlAlchemyExecutionEngine,
    metric_class: type[QueryMetricProvider],
    class_metric_value_kwargs: dict,
    query_parameters: Optional[QueryParameters],
    batch_selectable: sa.Table,
):
    metric_value_kwargs = {
        "query_param": "my_query",
        "my_query": "SELECT * FROM {batch} WHERE passenger_count > 7",
    }
    metric_value_kwargs.update(class_metric_value_kwargs)

    mock_substituted_batch_subquery = "SELECT * FROM (my_table) WHERE passenger_count > 7"
    mock_get_substituted_batch_subquery_from_query_and_batch_selectable.return_value = (
        mock_substituted_batch_subquery
    )
    mock_sqlalchemy_text.return_value = "*"
    with mock.patch.object(mock_sqlalchemy_execution_engine, "execute_query"):
        _call_sqlalchemy_metric(
            metric_class,
            execution_engine=mock_sqlalchemy_execution_engine,
            metric_domain_kwargs={},
            metric_value_kwargs=metric_value_kwargs,
            metrics={},
            runtime_configuration={},
        )
    if query_parameters:
        mock_get_substituted_batch_subquery_from_query_and_batch_selectable.assert_called_once_with(
            query=metric_value_kwargs["my_query"],
            batch_selectable=batch_selectable,
            execution_engine=mock_sqlalchemy_execution_engine,
            query_parameters=query_parameters,
        )
    else:
        mock_get_substituted_batch_subquery_from_query_and_batch_selectable.assert_called_once_with(
            query=metric_value_kwargs["my_query"],
            batch_selectable=batch_selectable,
            execution_engine=mock_sqlalchemy_execution_engine,
        )
    expected_kwargs = {
        "substituted_batch_subquery": mock_substituted_batch_subquery,
        "execution_engine": mock_sqlalchemy_execution_engine,
    }
    if issubclass(metric_class, QueryTable):
        expected_kwargs["fetch_all"] = False
    mock_get_sqlalchemy_records_from_substituted_batch_subquery.assert_called_once_with(
        **expected_kwargs,
    )


class MyQueryRowCount(QueryRowCount):
    metric_name = "my_query.row_count"
    value_keys = ("my_query",)

    query_param_name: ClassVar[str] = "my_query"


@pytest.mark.unit
@mock.patch.object(sa, "text")
@mock.patch.object(
    QueryMetricProvider, "_get_substituted_batch_subquery_from_query_and_batch_selectable"
)
def test_sqlalchemy_query_row_count(
    mock_get_substituted_batch_subquery_from_query_and_batch_selectable,
    mock_sqlalchemy_text,
    mock_sqlalchemy_execution_engine: MockSqlAlchemyExecutionEngine,
    batch_selectable: sa.Table,
):
    metric_value_kwargs = {
        "query_param": "my_query",
        "my_query": "SELECT * FROM {batch} WHERE passenger_count > 7",
    }

    mock_substituted_batch_subquery = "SELECT * FROM (my_table) WHERE passenger_count > 7"
    mock_get_substituted_batch_subquery_from_query_and_batch_selectable.return_value = (
        mock_substituted_batch_subquery
    )
    mock_sqlalchemy_text.return_value = "*"
    with mock.patch.object(mock_sqlalchemy_execution_engine, "execute_query"):
        _call_sqlalchemy_metric(
            MyQueryRowCount,
            execution_engine=mock_sqlalchemy_execution_engine,
            metric_domain_kwargs={},
            metric_value_kwargs=metric_value_kwargs,
            metrics={},
            runtime_configuration={},
        )
    mock_get_substituted_batch_subquery_from_query_and_batch_selectable.assert_called_once_with(
        query=metric_value_kwargs["my_query"],
        batch_selectable=batch_selectable,
        execution_engine=mock_sqlalchemy_execution_engine,
    )


@pytest.mark.unit
def test_get_substituted_batch_subquery_uses_dialect_for_compilation(
    mock_sqlalchemy_execution_engine: MockSqlAlchemyExecutionEngine,
    monkeypatch: pytest.MonkeyPatch,
):
    """Test that batch selectable compilation uses the execution engine's dialect.

    This test verifies the fix for the Databricks identifier quoting issue where
    column names were being quoted with double quotes (") instead of backticks (`),
    causing Databricks to interpret them as string literals rather than column identifiers.
    """
    # Create a Select statement with a column that will be compiled
    metadata = sa.MetaData()
    test_table = sa.Table("test_table", metadata, sa.Column("ReportingDate", sa.TIMESTAMP))
    batch_selectable = sa.select(test_table).where(
        sa.extract("year", test_table.c.ReportingDate) == 2025
    )

    query = "SELECT * FROM {batch}"

    # Use MySQL dialect to simulate Databricks (both use backticks for identifiers)
    mysql_dialect = mysql.dialect()
    monkeypatch.setattr(mock_sqlalchemy_execution_engine.engine, "dialect", mysql_dialect)

    # Call the method
    result = QueryMetricProvider._get_substituted_batch_subquery_from_query_and_batch_selectable(
        query=query,
        batch_selectable=batch_selectable,
        execution_engine=mock_sqlalchemy_execution_engine,
    )

    # Verify the result is a string containing the compiled SQL with proper table/column references
    assert isinstance(result, str)
    assert "SELECT" in result.upper()
    # Verify that the batch selectable was actually compiled
    # (should contain table and column references)
    assert "test_table" in result.lower() or "reportingdate" in result.lower()


class TestHasTopLevelToken:
    @pytest.mark.unit
    @pytest.mark.parametrize(
        "query, token, expected",
        [
            pytest.param("SELECT * FROM t ORDER BY x", "ORDER BY", True, id="simple_match"),
            pytest.param("SELECT * FROM t", "ORDER BY", False, id="no_match"),
            pytest.param("select * from t order by x", "ORDER BY", True, id="case_insensitive"),
            pytest.param(
                "SELECT * FROM (SELECT * FROM t ORDER BY x) sub",
                "ORDER BY",
                False,
                id="nested_not_matched",
            ),
            pytest.param(
                "SELECT * FROM t ORDER BY x OFFSET 5 ROWS",
                "OFFSET",
                True,
                id="offset_at_top_level",
            ),
            pytest.param(
                "SELECT * FROM (SELECT * FROM t ORDER BY x OFFSET 0 ROWS) sub",
                "OFFSET",
                False,
                id="offset_nested_not_matched",
            ),
            pytest.param("", "ORDER BY", False, id="empty_string"),
            pytest.param(
                "SELECT *, ROW_NUMBER() OVER (ORDER BY x) FROM t",
                "ORDER BY",
                False,
                id="window_function_not_matched",
            ),
        ],
    )
    def test_has_top_level_token(self, query: str, token: str, expected: bool) -> None:
        assert has_top_level_token(query, token) is expected


class TestFindLastTopLevelOrderBy:
    @pytest.mark.unit
    @pytest.mark.parametrize(
        "query, expected_pos",
        [
            pytest.param("SELECT * FROM t ORDER BY x", 16, id="simple"),
            pytest.param("SELECT * FROM t", -1, id="none"),
            pytest.param("", -1, id="empty"),
            pytest.param(
                "SELECT * FROM (SELECT * FROM t ORDER BY x) sub ORDER BY y",
                47,
                id="inner_and_outer_returns_outer",
            ),
            pytest.param(
                "SELECT *, ROW_NUMBER() OVER (ORDER BY x) FROM t",
                -1,
                id="window_function_ignored",
            ),
            pytest.param(
                "SELECT * FROM t1 ORDER BY a UNION ALL SELECT * FROM t2 ORDER BY b",
                55,
                id="multiple_returns_last",
            ),
        ],
    )
    def test_find_last_top_level_order_by(self, query: str, expected_pos: int) -> None:
        assert find_last_top_level_order_by(query) == expected_pos


class TestStripTopLevelOrderBy:
    @pytest.mark.unit
    @pytest.mark.parametrize(
        "query, expected",
        [
            # --- basic stripping ---
            pytest.param(
                "SELECT * FROM t WHERE x > 1 ORDER BY x DESC",
                "SELECT * FROM t WHERE x > 1",
                id="where_then_order_by",
            ),
            pytest.param(
                "SELECT * FROM t ORDER BY x",
                "SELECT * FROM t",
                id="simple_order_by",
            ),
            pytest.param(
                "SELECT * FROM t ORDER BY x ASC, y DESC",
                "SELECT * FROM t",
                id="multi_column_order_by",
            ),
            pytest.param(
                "SELECT * FROM t GROUP BY color HAVING SUM(x) > 3 ORDER BY color",
                "SELECT * FROM t GROUP BY color HAVING SUM(x) > 3",
                id="group_having_order_by",
            ),
            # --- no-op (no ORDER BY) ---
            pytest.param(
                "SELECT * FROM t WHERE x > 1",
                "SELECT * FROM t WHERE x > 1",
                id="no_order_by",
            ),
            pytest.param("SELECT * FROM t", "SELECT * FROM t", id="plain_select"),
            pytest.param("", "", id="empty_string"),
            # --- case insensitivity ---
            pytest.param(
                "select * from t order by x",
                "select * from t",
                id="all_lowercase",
            ),
            pytest.param(
                "SELECT * FROM t Order By x",
                "SELECT * FROM t",
                id="mixed_case",
            ),
            pytest.param(
                "SELECT * FROM t ORDER  BY x",
                "SELECT * FROM t ORDER  BY x",
                id="extra_space_not_matched",
            ),
            # --- window functions preserved ---
            pytest.param(
                "SELECT *, ROW_NUMBER() OVER (ORDER BY x) FROM t",
                "SELECT *, ROW_NUMBER() OVER (ORDER BY x) FROM t",
                id="window_function_preserved",
            ),
            pytest.param(
                "SELECT *, ROW_NUMBER() OVER (PARTITION BY a ORDER BY x) as rn FROM t",
                "SELECT *, ROW_NUMBER() OVER (PARTITION BY a ORDER BY x) as rn FROM t",
                id="window_partition_preserved",
            ),
            pytest.param(
                "SELECT *, ROW_NUMBER() OVER (ORDER BY x) FROM t ORDER BY rn",
                "SELECT *, ROW_NUMBER() OVER (ORDER BY x) FROM t",
                id="window_preserved_outer_stripped",
            ),
            # --- subqueries ---
            pytest.param(
                "SELECT * FROM (SELECT TOP 5 * FROM t ORDER BY x) sub",
                "SELECT * FROM (SELECT TOP 5 * FROM t ORDER BY x) sub",
                id="inner_only_preserved",
            ),
            pytest.param(
                "SELECT * FROM (SELECT TOP 5 * FROM t ORDER BY x) sub ORDER BY y",
                "SELECT * FROM (SELECT TOP 5 * FROM t ORDER BY x) sub",
                id="inner_preserved_outer_stripped",
            ),
            pytest.param(
                "SELECT * FROM (SELECT * FROM (SELECT TOP 3 * FROM t ORDER BY x) a) b ORDER BY z",
                "SELECT * FROM (SELECT * FROM (SELECT TOP 3 * FROM t ORDER BY x) a) b",
                id="deeply_nested",
            ),
            # --- OFFSET guard (returns unchanged) ---
            pytest.param(
                "SELECT * FROM t ORDER BY x OFFSET 5 ROWS FETCH NEXT 10 ROWS ONLY",
                "SELECT * FROM t ORDER BY x OFFSET 5 ROWS FETCH NEXT 10 ROWS ONLY",
                id="offset_fetch_preserved",
            ),
            pytest.param(
                "SELECT * FROM t ORDER BY x OFFSET 0 ROWS",
                "SELECT * FROM t ORDER BY x OFFSET 0 ROWS",
                id="offset_only_preserved",
            ),
            pytest.param(
                "SELECT * FROM (SELECT * FROM t ORDER BY x OFFSET 0 ROWS) sub ORDER BY y",
                "SELECT * FROM (SELECT * FROM t ORDER BY x OFFSET 0 ROWS) sub",
                id="nested_offset_does_not_prevent_outer_strip",
            ),
            # --- TOP does not prevent stripping ---
            pytest.param(
                "SELECT TOP 10 * FROM t ORDER BY x DESC",
                "SELECT TOP 10 * FROM t",
                id="top_without_offset_still_strips",
            ),
            pytest.param(
                "SELECT * FROM (SELECT TOP 5 * FROM t ORDER BY x) sub ORDER BY y",
                "SELECT * FROM (SELECT TOP 5 * FROM t ORDER BY x) sub",
                id="nested_top_does_not_prevent_outer_strip",
            ),
            # --- complex patterns ---
            pytest.param(
                "SELECT * FROM t1 UNION ALL SELECT * FROM t2 ORDER BY x",
                "SELECT * FROM t1 UNION ALL SELECT * FROM t2",
                id="union_trailing_stripped",
            ),
            pytest.param(
                "SELECT t1.* FROM t1 JOIN t2 ON t1.id = t2.id ORDER BY t1.x",
                "SELECT t1.* FROM t1 JOIN t2 ON t1.id = t2.id",
                id="join_stripped",
            ),
            pytest.param(
                "SELECT * FROM t ORDER BY CASE WHEN x > 1 THEN 0 ELSE 1 END",
                "SELECT * FROM t",
                id="case_expression_stripped",
            ),
            pytest.param(
                "SELECT * FROM t ORDER BY COALESCE(x, 0)",
                "SELECT * FROM t",
                id="function_call_stripped",
            ),
            # --- multi-line ---
            pytest.param(
                "SELECT *\nFROM t\nWHERE x > 1\nORDER BY x DESC",
                "SELECT *\nFROM t\nWHERE x > 1",
                id="multiline",
            ),
            pytest.param(
                "SELECT *\r\nFROM t\r\nORDER BY x",
                "SELECT *\r\nFROM t",
                id="crlf_line_endings",
            ),
            # --- trailing content ---
            pytest.param(
                "SELECT * FROM t ORDER BY x   ",
                "SELECT * FROM t",
                id="trailing_whitespace",
            ),
            pytest.param(
                "SELECT * FROM t ORDER BY x;",
                "SELECT * FROM t",
                id="trailing_semicolon",
            ),
            # --- edge cases ---
            pytest.param("ORDER BY x", "", id="just_order_by"),
            pytest.param(
                "SELECT * FROM t1 ORDER BY a UNION ALL SELECT * FROM t2 ORDER BY b",
                "SELECT * FROM t1 ORDER BY a UNION ALL SELECT * FROM t2",
                id="multiple_top_level_strips_last",
            ),
        ],
    )
    def test_strip_top_level_order_by(self, query: str, expected: str) -> None:
        assert strip_top_level_order_by(query) == expected

    @pytest.mark.unit
    def test_large_query_does_not_hang(self) -> None:
        base = "SELECT col FROM table_name WHERE " + " AND ".join(
            f"col_{i} > {i}" for i in range(500)
        )
        query = base + " ORDER BY col"
        assert strip_top_level_order_by(query) == base

    @pytest.mark.unit
    def test_sql_comment_containing_order_by_is_known_limitation(self) -> None:
        """Naive parser treats comments as real SQL — known limitation."""
        query = "SELECT * FROM t -- ORDER BY x"
        assert strip_top_level_order_by(query) != query

    @pytest.mark.unit
    def test_string_literal_containing_order_by_is_known_limitation(self) -> None:
        """Naive parser treats string literals as real SQL — known limitation."""
        query = "SELECT * FROM t WHERE name = 'ORDER BY'"
        assert strip_top_level_order_by(query) != query


class MockSQLServerSqlAlchemyExecutionEngine(MockSqlAlchemyExecutionEngine):
    """Mock engine that reports dialect_name as 'mssql' (SQL Server)."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        from tests.expectations.metrics.conftest import MockSaEngine

        self.engine = MockSaEngine(dialect=sa.dialects.mssql.dialect())  # type: ignore[assignment]


@pytest.fixture
def mock_sql_server_execution_engine() -> MockSQLServerSqlAlchemyExecutionEngine:
    from tests.expectations.metrics.conftest import MockBatchManager

    engine = MockSQLServerSqlAlchemyExecutionEngine()
    engine._batch_manager = MockBatchManager()  # type: ignore[assignment]
    return engine


class MockOracleSqlAlchemyExecutionEngine(MockSqlAlchemyExecutionEngine):
    """Mock engine that reports dialect_name as 'oracle'."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        from tests.expectations.metrics.conftest import MockSaEngine

        self.engine = MockSaEngine(dialect=sa.dialects.oracle.dialect())  # type: ignore[assignment]


@pytest.fixture
def mock_oracle_execution_engine() -> MockOracleSqlAlchemyExecutionEngine:
    from tests.expectations.metrics.conftest import MockBatchManager

    engine = MockOracleSqlAlchemyExecutionEngine()
    engine._batch_manager = MockBatchManager()  # type: ignore[assignment]
    return engine


class TestQueryRowCountSQLServerOrderByStripping:
    @pytest.mark.unit
    @mock.patch.object(sa, "text")
    @mock.patch.object(
        QueryMetricProvider, "_get_substituted_batch_subquery_from_query_and_batch_selectable"
    )
    @pytest.mark.parametrize(
        "substituted_query, assert_order_by_present, assert_fragments",
        [
            pytest.param(
                "SELECT * FROM (my_table) AS subselect WHERE x > 1 ORDER BY x DESC",
                False,
                ["COUNT(*)"],
                id="plain_order_by_stripped",
            ),
            pytest.param(
                "SELECT TOP 10 * FROM (my_table) AS subselect WHERE x > 1 ORDER BY x DESC",
                False,
                ["TOP 10"],
                id="top_present_order_by_still_stripped",
            ),
            pytest.param(
                "SELECT * FROM (my_table) AS subselect "
                "ORDER BY x OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY",
                True,
                ["OFFSET"],
                id="offset_present_order_by_preserved",
            ),
            pytest.param(
                "SELECT * FROM (my_table) AS subselect WHERE x > 1",
                False,
                [],
                id="no_order_by_passes_through",
            ),
            pytest.param(
                "SELECT * FROM (SELECT TOP 5 * FROM my_table ORDER BY x) sub ORDER BY y",
                False,
                ["TOP 5", "ORDER BY x"],
                id="nested_top_outer_order_by_stripped",
            ),
        ],
    )
    def test_sql_server_query_row_count(
        self,
        mock_get_sub,
        mock_text,
        mock_sql_server_execution_engine: MockSQLServerSqlAlchemyExecutionEngine,
        batch_selectable: sa.Table,
        substituted_query: str,
        assert_order_by_present: bool,
        assert_fragments: list[str],
    ) -> None:
        mock_get_sub.return_value = substituted_query
        mock_text.return_value = "*"
        mock_result = create_autospec(sa.engine.CursorResult)
        mock_result.fetchone.return_value = (42,)

        with mock.patch.object(
            mock_sql_server_execution_engine, "execute_query", return_value=mock_result
        ):
            _call_sqlalchemy_metric(
                MyQueryRowCount,
                execution_engine=mock_sql_server_execution_engine,
                metric_domain_kwargs={},
                metric_value_kwargs={
                    "query_param": "my_query",
                    "my_query": "SELECT * FROM {batch}",
                },
                metrics={},
                runtime_configuration={},
            )

        actual_sql = mock_text.call_args[0][0]
        if assert_order_by_present:
            assert "ORDER BY" in actual_sql
        for fragment in assert_fragments:
            assert fragment in actual_sql

    @pytest.mark.unit
    @mock.patch.object(sa, "text")
    @mock.patch.object(
        QueryMetricProvider, "_get_substituted_batch_subquery_from_query_and_batch_selectable"
    )
    def test_non_sql_server_preserves_order_by(
        self,
        mock_get_sub,
        mock_text,
        mock_sqlalchemy_execution_engine: MockSqlAlchemyExecutionEngine,
        batch_selectable: sa.Table,
    ) -> None:
        mock_get_sub.return_value = (
            "SELECT * FROM (my_table) AS subselect WHERE x > 1 ORDER BY x DESC"
        )
        mock_text.return_value = "*"
        mock_result = create_autospec(sa.engine.CursorResult)
        mock_result.fetchone.return_value = (3,)

        with mock.patch.object(
            mock_sqlalchemy_execution_engine, "execute_query", return_value=mock_result
        ):
            _call_sqlalchemy_metric(
                MyQueryRowCount,
                execution_engine=mock_sqlalchemy_execution_engine,
                metric_domain_kwargs={},
                metric_value_kwargs={
                    "query_param": "my_query",
                    "my_query": "SELECT * FROM {batch} WHERE x > 1 ORDER BY x DESC",
                },
                metrics={},
                runtime_configuration={},
            )

        actual_sql = mock_text.call_args[0][0]
        assert "ORDER BY" in actual_sql


def _compile_batch_selectable(
    batch_selectable: sa.Table, dialect: sa.engine.interfaces.Dialect
) -> str:
    """Compile the same selectable, in the same way, that the code under test compiles it.

    ``sa.select(batch_selectable)`` with no columns pulled out renders differently across
    SQLAlchemy major versions (e.g. an extra space after ``SELECT``), so that fragment is
    derived here at runtime rather than hardcoded, leaving only this work's contribution --
    the alias wrapping asserted alongside this helper's return value -- pinned as a literal.
    """
    return str(
        sa.select(batch_selectable).compile(dialect=dialect, compile_kwargs={"literal_binds": True})
    )


class TestDerivedTableAliasDialectInvariance:
    """Characterization tests pinning the derived-table alias text each dialect receives.

    Three sites build a derived-table alias, all of them through
    ``render_derived_table_alias``: the single-nested form here (a query-metric re-embedding a
    compiled ``Select`` bound to ``subselect``) and the doubly-nested form in
    ``TestRowCountDerivedTableAliasDialectInvariance`` below (the row-count statement's
    ``substituted_batch_subquery`` wrapping around the same inner form). The keyword ``AS``
    precedes the alias on every dialect whose grammar admits it there, and is omitted on the
    one whose grammar does not. They
    are asserted separately because the two nesting depths surface as two distinct database
    errors on Oracle, from two different metrics of the same expectation.
    """

    @pytest.mark.unit
    def test_single_nested_alias_default_dialect(
        self,
        mock_sqlalchemy_execution_engine: MockSqlAlchemyExecutionEngine,
        batch_selectable: sa.Table,
    ) -> None:
        result = (
            QueryMetricProvider._get_substituted_batch_subquery_from_query_and_batch_selectable(
                query="SELECT * FROM {batch} WHERE passenger_count > 7",
                batch_selectable=sa.select(batch_selectable),
                execution_engine=mock_sqlalchemy_execution_engine,
            )
        )
        compiled = _compile_batch_selectable(
            batch_selectable, mock_sqlalchemy_execution_engine.engine.dialect
        )
        assert result == (f"SELECT * FROM ({compiled}) AS subselect WHERE passenger_count > 7")

    @pytest.mark.unit
    def test_alias_keyword_is_emitted_when_the_dialect_is_unknown(self) -> None:
        """The renderer emits the keyword when the dialect name is unknown.

        ``render_derived_table_alias`` omits ``AS`` only for the one grammar that rejects it
        before a table alias. A ``None`` dialect name means the caller could not say which
        grammar applies, so the form every other dialect receives is the safe answer -- an
        unknown dialect must not inherit the exception.
        """
        assert (
            render_derived_table_alias(subquery="SELECT 1", alias="subselect", dialect_name=None)
            == "(SELECT 1) AS subselect"
        )

    @pytest.mark.unit
    def test_single_nested_alias_sql_server(
        self,
        mock_sql_server_execution_engine: MockSQLServerSqlAlchemyExecutionEngine,
        batch_selectable: sa.Table,
    ) -> None:
        result = (
            QueryMetricProvider._get_substituted_batch_subquery_from_query_and_batch_selectable(
                query="SELECT * FROM {batch} WHERE passenger_count > 7",
                batch_selectable=sa.select(batch_selectable),
                execution_engine=mock_sql_server_execution_engine,
            )
        )
        compiled = _compile_batch_selectable(
            batch_selectable, mock_sql_server_execution_engine.engine.dialect
        )
        assert result == (f"SELECT * FROM ({compiled}) AS subselect WHERE passenger_count > 7")

    @pytest.mark.unit
    def test_single_nested_alias_oracle(
        self,
        mock_oracle_execution_engine: MockOracleSqlAlchemyExecutionEngine,
        batch_selectable: sa.Table,
    ) -> None:
        """Oracle's grammar admits ``AS`` before a column alias but not before a table alias, so
        the derived-table alias renderer omits the keyword here. Every other assertion in this
        module must stay byte-identical alongside this inversion.
        """
        result = (
            QueryMetricProvider._get_substituted_batch_subquery_from_query_and_batch_selectable(
                query="SELECT * FROM {batch} WHERE passenger_count > 7",
                batch_selectable=sa.select(batch_selectable),
                execution_engine=mock_oracle_execution_engine,
            )
        )
        compiled = _compile_batch_selectable(
            batch_selectable, mock_oracle_execution_engine.engine.dialect
        )
        assert result == (f"SELECT * FROM ({compiled}) subselect WHERE passenger_count > 7")

    @pytest.mark.unit
    def test_join_query_receives_no_alias_default_dialect(
        self,
        mock_sqlalchemy_execution_engine: MockSqlAlchemyExecutionEngine,
        batch_selectable: sa.Table,
    ) -> None:
        """The sibling branch immediately above the aliased one: a JOIN query gets no alias at
        all, on any dialect. This branch carries no dialect condition, so a dialect-specific
        renderer applied to the aliased branch must not disturb it.
        """
        result = (
            QueryMetricProvider._get_substituted_batch_subquery_from_query_and_batch_selectable(
                query="SELECT * FROM {batch} JOIN other_table ON 1=1",
                batch_selectable=sa.select(batch_selectable),
                execution_engine=mock_sqlalchemy_execution_engine,
            )
        )
        compiled = _compile_batch_selectable(
            batch_selectable, mock_sqlalchemy_execution_engine.engine.dialect
        )
        assert result == f"SELECT * FROM ({compiled}) JOIN other_table ON 1=1"

    @pytest.mark.unit
    def test_join_query_receives_no_alias_oracle(
        self,
        mock_oracle_execution_engine: MockOracleSqlAlchemyExecutionEngine,
        batch_selectable: sa.Table,
    ) -> None:
        """The unaliased JOIN branch is already identical on Oracle today; it is not part of the
        defect and is not expected to change.
        """
        result = (
            QueryMetricProvider._get_substituted_batch_subquery_from_query_and_batch_selectable(
                query="SELECT * FROM {batch} JOIN other_table ON 1=1",
                batch_selectable=sa.select(batch_selectable),
                execution_engine=mock_oracle_execution_engine,
            )
        )
        compiled = _compile_batch_selectable(
            batch_selectable, mock_oracle_execution_engine.engine.dialect
        )
        assert result == f"SELECT * FROM ({compiled}) JOIN other_table ON 1=1"


class TestRowCountDerivedTableAliasDialectInvariance:
    """Characterization tests pinning the row-count statement's doubly-nested derived-table
    alias, and the column alias that sits alongside it in the same statement.

    The inner form is supplied pre-compiled here (mirroring the existing
    ``TestQueryRowCountSQLServerOrderByStripping`` pattern in this module) so each test isolates
    the outer wrapping this statement adds: the table alias ``substituted_batch_subquery``,
    whose keyword is dialect-dependent, and the column alias ``AS unexpected_row_count``, which
    every dialect here accepts and which no dialect-specific rendering touches.

    Each test asserts the full statement text and then, separately, a standalone assertion
    isolating the column alias (``SELECT COUNT(*) as unexpected_row_count FROM``). The two are
    kept independent on purpose: a change to the table alias rewrites this class's Oracle-side
    full-statement literal, and the standalone assertion is what keeps a dropped or renamed
    column alias from riding through that rewrite unnoticed.
    """

    @pytest.mark.unit
    @mock.patch.object(sa, "text")
    @mock.patch.object(
        QueryMetricProvider, "_get_substituted_batch_subquery_from_query_and_batch_selectable"
    )
    def test_double_nested_alias_default_dialect(
        self,
        mock_get_sub,
        mock_text,
        mock_sqlalchemy_execution_engine: MockSqlAlchemyExecutionEngine,
        batch_selectable: sa.Table,
    ) -> None:
        mock_get_sub.return_value = (
            "SELECT * FROM (my_table) AS subselect WHERE passenger_count > 7"
        )
        mock_text.return_value = "*"
        mock_result = create_autospec(sa.engine.CursorResult)
        mock_result.fetchone.return_value = (42,)

        with mock.patch.object(
            mock_sqlalchemy_execution_engine, "execute_query", return_value=mock_result
        ):
            _call_sqlalchemy_metric(
                MyQueryRowCount,
                execution_engine=mock_sqlalchemy_execution_engine,
                metric_domain_kwargs={},
                metric_value_kwargs={
                    "query_param": "my_query",
                    "my_query": "SELECT * FROM {batch}",
                },
                metrics={},
                runtime_configuration={},
            )

        actual_sql = mock_text.call_args[0][0]
        assert actual_sql == (
            "SELECT COUNT(*) as unexpected_row_count FROM "
            "(SELECT * FROM (my_table) AS subselect WHERE passenger_count > 7) "
            "AS substituted_batch_subquery"
        )
        # Standalone: the column alias, pinned independently of the table alias above, so a
        # future rewrite of this test's Oracle-side full-statement literal cannot silently drop
        # or rename it.
        assert "SELECT COUNT(*) as unexpected_row_count FROM" in actual_sql

    @pytest.mark.unit
    @mock.patch.object(sa, "text")
    @mock.patch.object(
        QueryMetricProvider, "_get_substituted_batch_subquery_from_query_and_batch_selectable"
    )
    def test_double_nested_alias_sql_server(
        self,
        mock_get_sub,
        mock_text,
        mock_sql_server_execution_engine: MockSQLServerSqlAlchemyExecutionEngine,
        batch_selectable: sa.Table,
    ) -> None:
        mock_get_sub.return_value = (
            "SELECT * FROM (my_table) AS subselect WHERE passenger_count > 7"
        )
        mock_text.return_value = "*"
        mock_result = create_autospec(sa.engine.CursorResult)
        mock_result.fetchone.return_value = (42,)

        with mock.patch.object(
            mock_sql_server_execution_engine, "execute_query", return_value=mock_result
        ):
            _call_sqlalchemy_metric(
                MyQueryRowCount,
                execution_engine=mock_sql_server_execution_engine,
                metric_domain_kwargs={},
                metric_value_kwargs={
                    "query_param": "my_query",
                    "my_query": "SELECT * FROM {batch}",
                },
                metrics={},
                runtime_configuration={},
            )

        actual_sql = mock_text.call_args[0][0]
        assert actual_sql == (
            "SELECT COUNT(*) as unexpected_row_count FROM "
            "(SELECT * FROM (my_table) AS subselect WHERE passenger_count > 7) "
            "AS substituted_batch_subquery"
        )
        # Standalone: the column alias, pinned independently of the table alias above, so a
        # future rewrite of this test's Oracle-side full-statement literal cannot silently drop
        # or rename it.
        assert "SELECT COUNT(*) as unexpected_row_count FROM" in actual_sql

    @pytest.mark.unit
    @mock.patch.object(sa, "text")
    @mock.patch.object(
        QueryMetricProvider, "_get_substituted_batch_subquery_from_query_and_batch_selectable"
    )
    def test_double_nested_alias_oracle(
        self,
        mock_get_sub,
        mock_text,
        mock_oracle_execution_engine: MockOracleSqlAlchemyExecutionEngine,
        batch_selectable: sa.Table,
    ) -> None:
        """Pins the derived-table alias renderer's effect at the second nesting depth: Oracle's
        outer table alias omits ``AS``, while the inner alias supplied via the mocked
        ``_get_substituted_batch_subquery_from_query_and_batch_selectable`` return value is
        unaffected -- that call is mocked here, so this test exercises only the outer wrapping
        this statement adds. This is the doubly-nested counterpart to
        ``test_single_nested_alias_oracle`` above -- both invert together, because the two
        nesting depths surface as two distinct database errors from two different metrics. The
        column alias ``as unexpected_row_count`` in this same statement is a separate, accepted
        construct; it is pinned independently below via a standalone assertion, not only inside
        this method's full-statement literal, so a rewrite of that literal cannot silently carry
        the column alias along with it.
        """
        mock_get_sub.return_value = (
            "SELECT * FROM (my_table) AS subselect WHERE passenger_count > 7"
        )
        mock_text.return_value = "*"
        mock_result = create_autospec(sa.engine.CursorResult)
        mock_result.fetchone.return_value = (42,)

        with mock.patch.object(
            mock_oracle_execution_engine, "execute_query", return_value=mock_result
        ):
            _call_sqlalchemy_metric(
                MyQueryRowCount,
                execution_engine=mock_oracle_execution_engine,
                metric_domain_kwargs={},
                metric_value_kwargs={
                    "query_param": "my_query",
                    "my_query": "SELECT * FROM {batch}",
                },
                metrics={},
                runtime_configuration={},
            )

        actual_sql = mock_text.call_args[0][0]
        assert actual_sql == (
            "SELECT COUNT(*) as unexpected_row_count FROM "
            "(SELECT * FROM (my_table) AS subselect WHERE passenger_count > 7) "
            "substituted_batch_subquery"
        )
        # Standalone: the column alias, pinned independently of the table alias above, so a
        # future rewrite of this test's Oracle-side full-statement literal cannot silently drop
        # or rename it.
        assert "SELECT COUNT(*) as unexpected_row_count FROM" in actual_sql


class TestQueryTemplateValuesDerivedTableAliasDialectInvariance:
    """Characterization tests pinning the third derived-table alias construction site, in
    ``QueryTemplateValues._sqlalchemy``.

    The stock mock execution engine's ``get_compute_domain`` returns a ``sa.Table``, which
    takes the unaliased ``sa.Table`` branch in ``query_template_values.py`` rather than the
    ``sa.sql.Select`` branch that binds the compiled selectable to ``subselect``. Each test below
    patches ``get_compute_domain`` on the engine instance to return a compiled ``Select``
    instead, so the aliased branch is actually exercised, and patches ``execute_query`` so no
    real database call is attempted. The exact text passed to ``sa.text`` is asserted.
    """

    @pytest.mark.unit
    def test_single_nested_alias_default_dialect(
        self,
        mock_sqlalchemy_execution_engine: MockSqlAlchemyExecutionEngine,
        batch_selectable: sa.Table,
    ) -> None:
        mock_result = create_autospec(sa.engine.CursorResult)
        mock_result.fetchall.return_value = []

        with (
            mock.patch.object(
                mock_sqlalchemy_execution_engine,
                "get_compute_domain",
                return_value=(sa.select(batch_selectable), {}, {}),
            ),
            mock.patch.object(
                mock_sqlalchemy_execution_engine, "execute_query", return_value=mock_result
            ) as mock_execute_query,
            mock.patch.object(sa, "text", side_effect=lambda x: x),
        ):
            _call_sqlalchemy_metric(
                QueryTemplateValues,
                execution_engine=mock_sqlalchemy_execution_engine,
                metric_domain_kwargs={},
                metric_value_kwargs={
                    "template_dict": {},
                    "query": "SELECT * FROM {batch} WHERE passenger_count > 7",
                },
                metrics={},
                runtime_configuration={},
            )
            actual_sql = mock_execute_query.call_args[0][0]
        compiled = _compile_batch_selectable(
            batch_selectable, mock_sqlalchemy_execution_engine.engine.dialect
        )
        assert actual_sql == (f"SELECT * FROM ({compiled}) AS subselect WHERE passenger_count > 7")

    @pytest.mark.unit
    def test_single_nested_alias_sql_server(
        self,
        mock_sql_server_execution_engine: MockSQLServerSqlAlchemyExecutionEngine,
        batch_selectable: sa.Table,
    ) -> None:
        mock_result = create_autospec(sa.engine.CursorResult)
        mock_result.fetchall.return_value = []

        with (
            mock.patch.object(
                mock_sql_server_execution_engine,
                "get_compute_domain",
                return_value=(sa.select(batch_selectable), {}, {}),
            ),
            mock.patch.object(
                mock_sql_server_execution_engine, "execute_query", return_value=mock_result
            ) as mock_execute_query,
            mock.patch.object(sa, "text", side_effect=lambda x: x),
        ):
            _call_sqlalchemy_metric(
                QueryTemplateValues,
                execution_engine=mock_sql_server_execution_engine,
                metric_domain_kwargs={},
                metric_value_kwargs={
                    "template_dict": {},
                    "query": "SELECT * FROM {batch} WHERE passenger_count > 7",
                },
                metrics={},
                runtime_configuration={},
            )
            actual_sql = mock_execute_query.call_args[0][0]
        compiled = _compile_batch_selectable(
            batch_selectable, mock_sql_server_execution_engine.engine.dialect
        )
        assert actual_sql == (f"SELECT * FROM ({compiled}) AS subselect WHERE passenger_count > 7")

    @pytest.mark.unit
    def test_single_nested_alias_oracle(
        self,
        mock_oracle_execution_engine: MockOracleSqlAlchemyExecutionEngine,
        batch_selectable: sa.Table,
    ) -> None:
        """Oracle's grammar admits ``AS`` before a column alias but not before a table alias, so
        the derived-table alias renderer omits the keyword at this third construction site too.
        This is one of the assertions in this module that inverts together with its siblings in
        the other two alias-construction test classes.
        """
        mock_result = create_autospec(sa.engine.CursorResult)
        mock_result.fetchall.return_value = []

        with (
            mock.patch.object(
                mock_oracle_execution_engine,
                "get_compute_domain",
                return_value=(sa.select(batch_selectable), {}, {}),
            ),
            mock.patch.object(
                mock_oracle_execution_engine, "execute_query", return_value=mock_result
            ) as mock_execute_query,
            mock.patch.object(sa, "text", side_effect=lambda x: x),
        ):
            _call_sqlalchemy_metric(
                QueryTemplateValues,
                execution_engine=mock_oracle_execution_engine,
                metric_domain_kwargs={},
                metric_value_kwargs={
                    "template_dict": {},
                    "query": "SELECT * FROM {batch} WHERE passenger_count > 7",
                },
                metrics={},
                runtime_configuration={},
            )
            actual_sql = mock_execute_query.call_args[0][0]
        compiled = _compile_batch_selectable(
            batch_selectable, mock_oracle_execution_engine.engine.dialect
        )
        assert actual_sql == (f"SELECT * FROM ({compiled}) subselect WHERE passenger_count > 7")


class TestLiteralBooleanReplacementDialectInvariance:
    """Characterization tests pinning the literal-boolean replacement behavior.

    ``query_metric_provider.py`` replaces the literal ``WHERE true`` with ``WHERE 1=1`` when the
    execution engine reports the ``mssql`` or ``oracle`` dialect: SQL Server has no boolean
    literal, and Oracle rejects a bare ``true`` as a ``WHERE``-clause predicate with ORA-00920
    before it gained a SQL boolean type in 23ai. These tests pin that behavior on SQL Server and
    on Oracle, and pin its absence on a default dialect.
    """

    @pytest.mark.unit
    def test_sql_server_replaces_literal_where_true(
        self,
        mock_sql_server_execution_engine: MockSQLServerSqlAlchemyExecutionEngine,
        batch_selectable: sa.Table,
    ) -> None:
        result = (
            QueryMetricProvider._get_substituted_batch_subquery_from_query_and_batch_selectable(
                query="SELECT * FROM {batch} WHERE true",
                batch_selectable=batch_selectable,
                execution_engine=mock_sql_server_execution_engine,
            )
        )
        assert result == "SELECT * FROM my_table WHERE 1=1"

    @pytest.mark.unit
    def test_oracle_replaces_literal_where_true(
        self,
        mock_oracle_execution_engine: MockOracleSqlAlchemyExecutionEngine,
        batch_selectable: sa.Table,
    ) -> None:
        result = (
            QueryMetricProvider._get_substituted_batch_subquery_from_query_and_batch_selectable(
                query="SELECT * FROM {batch} WHERE true",
                batch_selectable=batch_selectable,
                execution_engine=mock_oracle_execution_engine,
            )
        )
        assert result == "SELECT * FROM my_table WHERE 1=1"

    @pytest.mark.unit
    def test_default_dialect_does_not_replace_literal_where_true(
        self,
        mock_sqlalchemy_execution_engine: MockSqlAlchemyExecutionEngine,
        batch_selectable: sa.Table,
    ) -> None:
        result = (
            QueryMetricProvider._get_substituted_batch_subquery_from_query_and_batch_selectable(
                query="SELECT * FROM {batch} WHERE true",
                batch_selectable=batch_selectable,
                execution_engine=mock_sqlalchemy_execution_engine,
            )
        )
        assert result == "SELECT * FROM my_table WHERE true"
