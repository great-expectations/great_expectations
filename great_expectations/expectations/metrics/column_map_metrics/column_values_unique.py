from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, NamedTuple, Optional, Sequence, Union

import great_expectations.exceptions as gx_exceptions
from great_expectations.compatibility import pyspark, sqlalchemy
from great_expectations.compatibility.pyspark import functions as F
from great_expectations.compatibility.sqlalchemy import (
    Select,
)
from great_expectations.compatibility.sqlalchemy import (
    sqlalchemy as sa,
)
from great_expectations.compatibility.typing_extensions import override
from great_expectations.constants import MAX_RESULT_RECORDS
from great_expectations.core.metric_function_types import (
    MetricPartialFunctionTypes,
    MetricPartialFunctionTypeSuffixes,
)
from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.execution_engine.sqlalchemy_dialect import GXSqlDialect
from great_expectations.expectations.metrics.map_metric_provider import (
    ColumnMapMetricProvider,
    column_condition_partial,
    column_function_partial,
)
from great_expectations.expectations.metrics.map_metric_provider.map_condition_auxilliary_methods import (  # noqa: E501 # long module path
    _get_sqlalchemy_customized_unexpected_index_list,
)
from great_expectations.expectations.metrics.util import sqlalchemy_select_to_sql_string
from great_expectations.util import get_sqlalchemy_selectable
from great_expectations.validator.validation_graph import MetricConfiguration

if TYPE_CHECKING:
    from great_expectations.expectations.expectation_configuration import (
        ExpectationConfiguration,
    )


_DUP_KEY_COUNT_LABEL = "_num_rows"
_DUP_KEY_SUBQUERY_ALIAS = "column_values_count_per_value_subquery"
_SOURCE_SUBQUERY_ALIAS = "column_values_unique_source"
_DUP_KEYS_SUBQUERY_ALIAS = "column_values_unique_dup_keys"

# A value is a duplicate once it occurs at least this many times.
_DUPLICATE_THRESHOLD = 2

# MySQL and its wire-compatible forks refuse to reference the same temporary table more
# than once in a single statement ("Can't reopen table"), and a batch built from a query
# is materialized into one when the datasource is configured with create_temp_table.
# Row retrieval must therefore read the batch exactly once on these engines.
_SINGLE_REFERENCE_DIALECTS = frozenset({GXSqlDialect.MYSQL.value, GXSqlDialect.SINGLESTOREDB.value})


def _count_label(table_columns: Sequence[Any]) -> str:
    """Return a label for the window count that no source column can shadow.

    Nothing stops a table from containing a column called "_num_rows". Reusing that name
    for the count would put two columns of the same name in one projection, and column
    access by name would then silently resolve to the source data rather than to the
    count -- producing wrong results with no error.
    """
    taken = {str(column_name) for column_name in table_columns}
    label = _DUP_KEY_COUNT_LABEL
    while label in taken:
        label = f"_{label}"
    return label


def _column_by_name(selectable: Any, column_name: str) -> Any:
    """Return the column named "column_name" from a projection built on "table.columns".

    On the dialects GX treats as case-insensitive, the "table.columns" metric reports
    names as "CaseInsensitiveString", which hashes by its case-folded value. A plain
    "str" holding the same name is then an equal key that hashes differently, so a
    projection built from "table.columns" cannot be indexed by a raw domain column name
    unless that name is already lower case -- ".c" lookup raises "KeyError" instead.

    Try the hash lookup first so the common path stays O(1), and fall back to matching
    by equality, which both spellings agree on. Two columns that differ only by case are
    physically distinct (e.g. via quoted identifiers), so an exact-spelling match is
    preferred over a merely case-folded one -- the loop keeps scanning past a folded
    match in case a later entry matches exactly, and only settles for the folded match
    if no exact one turns up. A name that matches nothing still raises the original
    "KeyError".
    """
    try:
        return selectable.c[column_name]
    except KeyError:
        case_insensitive_match = None
        for key, column in selectable.c.items():
            if key == column_name:
                if str(key) == column_name:
                    return column
                if case_insensitive_match is None:
                    case_insensitive_match = column
        if case_insensitive_match is not None:
            return case_insensitive_match
        raise


class _DuplicateRowsSource(NamedTuple):
    """The pieces of a query that returns the source rows whose target value repeats.

    "columns" exposes the source columns via its ".c" accessor, "from_clause" is what
    the query selects FROM, and "whereclause" is an extra filter to apply (or None).
    """

    columns: Any
    from_clause: Any
    whereclause: Any

    def column(self, column_name: str) -> Any:
        """Return the source column named "column_name"."""
        return _column_by_name(self.columns, column_name)


def _build_duplicate_rows_source(
    execution_engine: SqlAlchemyExecutionEngine,
    metric_domain_kwargs: Dict[str, Any],
    column_name: str,
    table_columns: Sequence[Any],
) -> _DuplicateRowsSource:
    """Build the FROM clause shared by the row-retrieval metrics.

    The default shape joins a narrow GROUP BY/HAVING aggregate back to the source. The
    aggregate reads only the target column, so no engine has to carry the full row width
    through a sort, but it does read the source twice. Engines that cannot tolerate the
    second read get a single-pass window instead; carrying every column through the
    window is acceptable there because they are row stores, which is also why the wide
    window is what "compound_columns.unique" already uses on them.

    "SqlAlchemyBatchData" exposes the source table as a metadata-less "sa.Table" shell
    (no reflected columns), so its ".c" accessor is empty. Likewise, when a row_condition
    is present "get_domain_records" returns a "SELECT * FROM ... WHERE ..." Select whose
    ".c" collection carries no named columns. Both shapes are wrapped in an explicit
    projection so that ".c" is populated and source-side columns can be referenced
    unambiguously.
    """
    selectable = get_sqlalchemy_selectable(
        execution_engine.get_domain_records(domain_kwargs=metric_domain_kwargs)  # type: ignore[arg-type] # FIXME CoP
    )

    if execution_engine.dialect_name in _SINGLE_REFERENCE_DIALECTS:
        count_label = _count_label(table_columns)
        source = (
            sa.select(
                *[sa.column(c) for c in table_columns],
                sa.func.count().over(partition_by=sa.column(column_name)).label(count_label),
            )
            .select_from(selectable)  # type: ignore[arg-type] # FIXME CoP
            # Excluded here rather than downstream so that null rows never join a
            # partition and inflate its count.
            .where(sa.column(column_name).is_not(None))
            .subquery(_SOURCE_SUBQUERY_ALIAS)
        )
        return _DuplicateRowsSource(
            columns=source,
            from_clause=source,
            whereclause=source.c[count_label] >= _DUPLICATE_THRESHOLD,
        )

    source = (
        sa.select(*[sa.column(c) for c in table_columns])
        .select_from(selectable)  # type: ignore[arg-type] # FIXME CoP
        .subquery(_SOURCE_SUBQUERY_ALIAS)
    )
    dup_keys = (
        sa.select(sa.column(column_name))
        .select_from(selectable)  # type: ignore[arg-type] # FIXME CoP
        .where(sa.column(column_name).is_not(None))
        .group_by(sa.column(column_name))
        .having(sa.func.count() >= _DUPLICATE_THRESHOLD)
        .subquery(_DUP_KEYS_SUBQUERY_ALIAS)
    )
    return _DuplicateRowsSource(
        columns=source,
        from_clause=source.join(
            dup_keys,
            _column_by_name(source, column_name) == dup_keys.c[column_name],
        ),
        whereclause=None,
    )


def _sqlalchemy_unique_unexpected_rows(
    cls,
    execution_engine: SqlAlchemyExecutionEngine,
    metric_domain_kwargs: Dict[str, Any],
    metric_value_kwargs: Dict[str, Any],
    metrics: Dict[str, Any],
    **kwargs,
) -> Sequence[Any]:
    """Return full source rows for values that appear more than once.

    On most engines the source is read twice (cheap narrow hash-aggregate + hash join
    back), but only when the caller requests "unexpected_rows" (typically COMPLETE
    result_format). The dominant "unexpected_count" path stays single-scan.
    """
    column_name: str = metric_domain_kwargs["column"]
    table_columns: List[str] = metrics["table.columns"]
    duplicates = _build_duplicate_rows_source(
        execution_engine=execution_engine,
        metric_domain_kwargs=metric_domain_kwargs,
        column_name=column_name,
        table_columns=table_columns,
    )
    column_selector = [duplicates.column(c) for c in table_columns]
    query = sa.select(*column_selector).select_from(duplicates.from_clause)
    if duplicates.whereclause is not None:
        query = query.where(duplicates.whereclause)
    result_format = metric_value_kwargs["result_format"]
    if result_format["result_format"] != "COMPLETE":
        limit = min(result_format["partial_unexpected_count"], MAX_RESULT_RECORDS)
        query = query.limit(limit)
    try:
        return [
            row._asdict()
            for row in execution_engine.execute_query(query).fetchmany(MAX_RESULT_RECORDS)
        ]
    except sqlalchemy.OperationalError as oe:
        raise gx_exceptions.InvalidMetricAccessorDomainKwargsKeyError(
            message=f"An SQL execution Exception occurred: {oe!s}."
        )


def _sqlalchemy_unique_unexpected_index_list(
    cls,
    execution_engine: SqlAlchemyExecutionEngine,
    metric_domain_kwargs: Dict[str, Any],
    metric_value_kwargs: Dict[str, Any],
    metrics: Dict[str, Any],
    **kwargs,
) -> Union[List[Dict[str, Any]], None]:
    """Return specified index columns + target column for duplicate rows."""
    result_format = metric_value_kwargs["result_format"]
    unexpected_index_column_names = result_format.get("unexpected_index_column_names")
    if not unexpected_index_column_names:
        return None

    column_name: str = metric_domain_kwargs["column"]
    all_table_columns: List[str] = metrics.get("table.columns", [])
    for idx_col in unexpected_index_column_names:
        if idx_col not in all_table_columns:
            raise gx_exceptions.InvalidMetricAccessorDomainKwargsKeyError(
                message=(
                    f'Error: The unexpected_index_column: "{idx_col}" does not exist in '
                    "SQL Table. Please check your configuration and try again."
                )
            )

    duplicates = _build_duplicate_rows_source(
        execution_engine=execution_engine,
        metric_domain_kwargs=metric_domain_kwargs,
        column_name=column_name,
        table_columns=all_table_columns,
    )
    column_selector = [duplicates.column(c) for c in unexpected_index_column_names]
    column_selector.append(duplicates.column(column_name))
    query = sa.select(*column_selector).select_from(duplicates.from_clause)
    if duplicates.whereclause is not None:
        query = query.where(duplicates.whereclause)
    query = query.limit(result_format["partial_unexpected_count"])
    exclude_unexpected_values: bool = result_format.get("exclude_unexpected_values", False)
    try:
        query_result: List[sqlalchemy.Row] = execution_engine.execute_query(query).fetchall()  # type: ignore[assignment] # FIXME CoP
    except sqlalchemy.OperationalError as oe:
        raise gx_exceptions.InvalidMetricAccessorDomainKwargsKeyError(
            message=f"An SQL execution Exception occurred: {oe!s}."
        )

    return _get_sqlalchemy_customized_unexpected_index_list(
        exclude_unexpected_values=exclude_unexpected_values,
        unexpected_index_column_names=unexpected_index_column_names,
        query_result=query_result,
        domain_column_name_list=[column_name],
    )


def _sqlalchemy_unique_unexpected_index_query(
    cls,
    execution_engine: SqlAlchemyExecutionEngine,
    metric_domain_kwargs: Dict[str, Any],
    metric_value_kwargs: Dict[str, Any],
    metrics: Dict[str, Any],
    **kwargs,
) -> Optional[str]:
    """Return an executable SQL string selecting the duplicate rows.

    The default "_sqlalchemy_map_condition_query" renders the map condition
    against the raw source table, but our condition references the narrow
    count-per-value subquery, which is absent from that FROM clause. Build the
    query string from the same join-back pattern used by the other
    row-retrieval paths instead, so the string surfaced in validation results
    and Data Docs runs against the source database as-is.
    """
    result_format = metric_value_kwargs["result_format"]
    if result_format.get("return_unexpected_index_query") is False:
        return None

    column_name: str = metric_domain_kwargs["column"]
    all_table_columns: List[str] = metrics.get("table.columns", [])
    unexpected_index_column_names: List[str] = (
        result_format.get("unexpected_index_column_names") or []
    )
    for idx_col in unexpected_index_column_names:
        if idx_col not in all_table_columns:
            raise gx_exceptions.InvalidMetricAccessorDomainKwargsKeyError(
                message=(
                    f'Error: The unexpected_index_column: "{idx_col}" does not exist in '
                    "SQL Table. Please check your configuration and try again."
                )
            )

    duplicates = _build_duplicate_rows_source(
        execution_engine=execution_engine,
        metric_domain_kwargs=metric_domain_kwargs,
        column_name=column_name,
        table_columns=all_table_columns,
    )
    column_selector = [duplicates.column(c) for c in unexpected_index_column_names]
    column_selector.append(duplicates.column(column_name))
    query = sa.select(*column_selector).select_from(duplicates.from_clause)
    if duplicates.whereclause is not None:
        query = query.where(duplicates.whereclause)
    return sqlalchemy_select_to_sql_string(engine=execution_engine, select_statement=query)


class ColumnValuesUnique(ColumnMapMetricProvider):
    """Detects duplicate values in a column.

    The "SqlAlchemyExecutionEngine" implementation materializes a *narrow* windowed
    subquery that exposes only the target column and a "_num_rows" count per value.
    Because the source table is scanned exactly once and the window operator carries
    only one column through the sort/partition phase, this avoids both:

    * the "col NOT IN (dup_subquery)" double-scan pattern (original failure mode),
    * the "SELECT *table_columns, count() OVER ... FROM source" wide-row window that
      forced Redshift to materialize every column (including JSON/SUPER fields)
      through the sort, occasionally tripping the WLM "low_timeout" rule on
      column-store backends even after the double-scan was removed.

    Auxiliary metrics that need the full source row ("unexpected_rows") or specific
    "unexpected_index_column_names" are served by a separate join-back path that
    re-reads only the necessary columns from the source table, keeping the common
    "BASIC" result_format (only "unexpected_count" requested) on the single-scan
    fast path.
    """

    function_metric_name = "column_values.count_per_value"
    condition_metric_name = "column_values.unique"

    # The narrow windowed subquery (below) carries only the target column. The
    # default map-condition row-retrieval providers assume the selectable
    # carries every table column (compound_columns.unique pattern), which
    # would re-introduce the wide-row window on Redshift. Override the three
    # SqlAlchemy row-retrieval hooks with narrow dup-keys subqueries joined
    # back to source.
    sqlalchemy_unexpected_rows_provider = staticmethod(_sqlalchemy_unique_unexpected_rows)
    sqlalchemy_unexpected_index_list_provider = staticmethod(
        _sqlalchemy_unique_unexpected_index_list
    )
    sqlalchemy_unexpected_index_query_provider = staticmethod(
        _sqlalchemy_unique_unexpected_index_query
    )

    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        return ~column.duplicated(keep=False)

    @column_function_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy_function(cls, column, _table, **kwargs):
        # Narrow projection: only the target column and the window count per value.
        # Auxiliary methods that consume this selectable (unexpected_count,
        # unexpected_values, unexpected_value_counts) only ever read these two
        # columns. Paths that need additional source columns are overridden via
        # the sqlalchemy_*_provider class attributes to join back to source.
        count_label = _count_label(kwargs["_metrics"]["table.columns"])
        from_clause = _table.subquery() if isinstance(_table, Select) else _table
        return (
            sa.select(
                sa.column(column.name),
                sa.func.count().over(partition_by=sa.column(column.name)).label(count_label),
            )
            .select_from(from_clause)
            .alias(_DUP_KEY_SUBQUERY_ALIAS)
        )

    @column_condition_partial(
        engine=SqlAlchemyExecutionEngine,
        partial_fn_type=MetricPartialFunctionTypes.WINDOW_CONDITION_FN,
    )
    def _sqlalchemy_condition(cls, column, **kwargs):
        metrics = kwargs["_metrics"]
        count_per_value_query, _, _ = metrics[
            f"column_values.count_per_value.{MetricPartialFunctionTypeSuffixes.MAP.value}"
        ]
        # Derived the same way as in "_sqlalchemy_function" so that both agree on which
        # column of the projection holds the count.
        count_label = _count_label(metrics["table.columns"])
        return count_per_value_query.c[count_label] < _DUPLICATE_THRESHOLD

    @column_condition_partial(
        engine=SparkDFExecutionEngine,
        partial_fn_type=MetricPartialFunctionTypes.WINDOW_CONDITION_FN,
    )
    def _spark(cls, column, **kwargs):
        return F.count(F.lit(1)).over(pyspark.Window.partitionBy(column)) <= 1

    @classmethod
    @override
    def _get_evaluation_dependencies(
        cls,
        metric: MetricConfiguration,
        configuration: Optional[ExpectationConfiguration] = None,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[dict] = None,
    ):
        dependencies: dict = super()._get_evaluation_dependencies(
            metric=metric,
            configuration=configuration,
            execution_engine=execution_engine,
            runtime_configuration=runtime_configuration,
        )

        if isinstance(execution_engine, SqlAlchemyExecutionEngine) and (
            metric.metric_name
            == f"column_values.unique.{MetricPartialFunctionTypeSuffixes.CONDITION.value}"
        ):
            dependencies[
                f"column_values.count_per_value.{MetricPartialFunctionTypeSuffixes.MAP.value}"
            ] = MetricConfiguration(
                metric_name=f"column_values.count_per_value.{MetricPartialFunctionTypeSuffixes.MAP.value}",
                metric_domain_kwargs=metric.metric_domain_kwargs,
                metric_value_kwargs=None,
            )

        return dependencies
