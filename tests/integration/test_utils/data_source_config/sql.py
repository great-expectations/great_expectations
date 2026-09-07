from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date, datetime
from functools import cached_property
from typing import TYPE_CHECKING, Any, Callable, Generic, Mapping, Optional, Sequence, Union, cast

import numpy as np
import pandas as pd
from typing_extensions import override

from great_expectations.compatibility.sqlalchemy import (
    Column,
    MetaData,
    Table,
    TextClause,
    TypeEngine,
    create_engine,
    insert,
    sqltypes,
)
from great_expectations.datasource.fluent.sql_datasource import TableAsset
from tests.integration.sql_session_manager import (
    ConnectionDetails,
    SessionSQLEngineManager,
)
from tests.integration.test_utils.data_source_config.backend_spec import TransactionMode
from tests.integration.test_utils.data_source_config.base import BatchTestSetup
from tests.integration.test_utils.data_source_config.sql_config import _SqlConfigT

if TYPE_CHECKING:
    import sqlalchemy as sa

    from great_expectations.data_context import AbstractDataContext
    from great_expectations.datasource.fluent.interfaces import Batch
    from tests.integration.test_utils.data_source_config.backend_spec import SqlBackendSpec

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _TableData:
    name: str
    df: pd.DataFrame
    table: Table


InferrableTypesLookup = dict[type[Any], Union[type[TypeEngine], TypeEngine]]

InferredColumnTypes = dict[str, Union[type[TypeEngine], TypeEngine]]


_SHARED_DEFAULTS: InferrableTypesLookup = {
    str: sqltypes.VARCHAR,
    int: sqltypes.INTEGER,
    # Not a bare DECIMAL: an unqualified decimal is read by several dialects as scale zero,
    # which rounds every fractional value in a fixture frame to an integer on write, so an
    # assertion about a value ends up asserting about a different one. Nor `Double`, which one
    # dialect renders as a type its server does not have.
    #
    # A precision-carrying float is the 64-bit type on every dialect here but four, and each of
    # those four declares its own override: Oracle raises rather than compiling it, over binary
    # vs. decimal precision; Databricks discards the precision and reads the bare `FLOAT` left
    # over as its 4-byte type; ClickHouse does the same, that spelling being its `Float32`;
    # SingleStore keeps the precision and ignores it, creating a 4-byte column anyway. Removing
    # any of those four surfaces this line's limit, so they and it belong together -- see
    # `DOUBLE_PRECISION_FLOAT_OVERRIDE` below, which two of them share. The dialects that discard
    # the precision without an override (SQLite, Snowflake) already mean 64 bits by a bare
    # `FLOAT`.
    float: sqltypes.Float(precision=53),
    bool: sqltypes.BOOLEAN,
    date: sqltypes.DATE,
    # `DateTime`, not DATETIME or TIMESTAMP. Those two are emitted verbatim, and each is wrong
    # somewhere: PostgreSQL has no DATETIME and rejects the DDL, while in T-SQL TIMESTAMP is a
    # row-version column that refuses an explicit value entirely.
    datetime: sqltypes.DateTime(),
    pd.Timestamp: sqltypes.DateTime(),
}
"""The Python-type-to-SQL-type map every SQL backend starts from.

These name types every dialect in use here has, and carry a precision wherever leaving it off
would let the server pick one. An unparameterised type compiles on every dialect, so a mismatch is
not a DDL error the harness would notice -- it is whatever that server decided the type meant,
applied silently to the fixture data.

Shared instances rather than per-call ones, which is safe because a SQLAlchemy type instance
carries no binding to the table it is used on -- the same sharing a backend's declared
`column_type_overrides` mapping already relies on. Schema *items* are the constructs that bind on
attachment, and those are declared as a factory for that reason (see `SqlBackendSpec`).
"""


DOUBLE_PRECISION_FLOAT_OVERRIDE: InferrableTypesLookup = (
    {float: sqltypes.Double()} if hasattr(sqltypes, "Double") else {}
)
"""A `float` override for the two backends that need the 8-byte type named and can take it plain.

They get there by different routes, which is why neither is fixable in the default. The Databricks
dialect discards the precision and emits a bare `FLOAT`, and that spelling is its server's
single-precision type. SingleStore is handed `FLOAT(53)` intact and, unlike MySQL, does not promote
it -- `SHOW CREATE TABLE` reports a plain `float`. Either way a Python float, which is a double, is
stored at half the width the fixture declared, with valid DDL and no error: verified against a live
SingleStore, which stores 16777217.0 as 16777200.0 under the default and exactly under this.
`Double` names the width in the type name, leaving the server nothing to pick.

SQLite and Snowflake also emit a bare `FLOAT` and need nothing: both mean 64 bits by it.
ClickHouse does not, and is the third backend narrowed by the default -- but its types all
carry a `Nullable(...)` wrapper, so it states its own `Float64` rather than sharing this.

Empty on SQLAlchemy 1.4, where `Double` does not exist. This module is imported there -- the
minimum-version lane constrains to that floor and collects every test module -- so naming the type
unconditionally would break collection in a lane that runs none of these backends. An environment
that did run one under 1.4 would get the narrowing back, which is why the round-trip suite asserts
the stored value rather than trusting the declaration.
"""


def inferrable_types_for(backend_spec: SqlBackendSpec) -> InferrableTypesLookup:
    """The Python-type-to-SQL-type map one backend builds its fixture columns from.

    The backend's declared `column_type_overrides` are merged over `_SHARED_DEFAULTS`, so a
    backend that needs a different type for a given Python type (e.g. a length-carrying string
    type) states that fact once, in its spec, rather than by overriding a property.

    A function of the declaration rather than of a live setup, because what a backend's fixture
    columns become is decided entirely by what that backend declares. Resolving it needs no
    server, no credentials and no engine, which is what lets a test record the answer for every
    registered backend at once rather than only for the backends a given lane can connect to.
    """
    return {**_SHARED_DEFAULTS, **backend_spec.column_type_overrides}


class SQLBatchTestSetup(BatchTestSetup[_SqlConfigT, TableAsset], ABC, Generic[_SqlConfigT]):
    SCHEMA_PREFIX = "gx_ci_test_"

    @property
    def backend_spec(self) -> SqlBackendSpec:
        return self.config.backend_spec

    @abstractmethod
    def build_connection_string(self, schema: str | None = None) -> str:
        """Connection string used to connect to SQL backend.

        When called without a schema, returns the base connection string for
        setup/teardown.  When called with a schema, returns a connection string
        that targets the given schema (for the GX datasource).
        """

    @property
    def use_schema(self) -> bool:
        """Whether to use a schema when connecting to SQL backend.

        If `True`, a schema will be automatically created.
        """
        return self.backend_spec.uses_schema

    @property
    def inferrable_types_lookup(self) -> InferrableTypesLookup:
        """Dict of Python type keys mapped to SQL dialect-specific SqlAlchemy types."""
        return inferrable_types_for(self.backend_spec)

    def __init__(
        self,
        config: _SqlConfigT,
        data: pd.DataFrame,
        extra_data: Mapping[str, pd.DataFrame],
        context: AbstractDataContext,
        table_name: Optional[str] = None,  # Overrides random table name generation
        engine_manager: Optional[SessionSQLEngineManager] = None,
    ) -> None:
        self.engine_manager = engine_manager
        self.extra_data = extra_data
        self.metadata = MetaData()
        self._user_specified_table_name = table_name
        super().__init__(config, data, context=context)

    @override
    def make_batch(self) -> Batch:
        return (
            self.make_asset()
            .add_batch_definition_whole_table(name=self._random_resource_name())
            .get_batch()
        )

    @cached_property
    def table_name(self) -> str:
        return self.main_table_data.name

    @cached_property
    def main_table_data(self) -> _TableData:
        name = self._user_specified_table_name or self._create_table_name()
        return self._create_table_data(
            name=name,
            df=self.data,
            column_types=self.config.column_types or {},
        )

    @cached_property
    def extra_table_data(self) -> Mapping[str, _TableData]:
        return {
            label: self._create_table_data(
                name=self._create_table_name(label),
                df=df,
                column_types=self.config.extra_column_types.get(label, {}),
            )
            for label, df in self.extra_data.items()
        }

    @cached_property
    def tables(self) -> Sequence[Table]:
        extra_tables = [td.table for td in self.extra_table_data.values()]
        return [self.main_table_data.table, *extra_tables]

    @cached_property
    def schema(self) -> Union[str, None]:
        if self.use_schema:
            schema_name = self.config.schema_name or self._random_resource_name()
            return f"{self.SCHEMA_PREFIX}{schema_name}"
        elif self.config.schema_name:
            raise ValueError(
                "Schema name provided but use_schema is False for this datasource type."
            )
        else:
            return None

    def _get_engine(self) -> tuple[sa.engine.Engine, Callable[[], None]]:
        if self.engine_manager:
            connection_details = ConnectionDetails(
                connection_string=self.build_connection_string(),
            )
            engine = self.engine_manager.get_engine(connection_details)
            return engine, lambda: None
        else:
            engine = create_engine(url=self.build_connection_string())
            return engine, engine.dispose

    def _safe_commit(self, conn: sa.Connection) -> None:
        """Safely commit a connection, skipping backends that declare they auto-commit.

        Some databases like Databricks auto-commit and don't support explicit transactions.
        For a backend whose declaration states that, this method skips the commit call
        entirely, trusting the declaration rather than inspecting the connection's dialect.

        Args:
            conn: SQLAlchemy connection to commit
        """
        if self.backend_spec.transaction_mode is TransactionMode.EXPLICIT_COMMIT:
            conn.commit()

    @staticmethod
    def _sanitize_null_values(values: list[dict]) -> list[dict]:
        """Replace pandas/numpy null sentinels with Python None.

        ``df.where(pd.notna(df), None)`` does **not** work for numeric columns
        because ``None`` is silently cast back to ``np.nan`` in float dtypes.
        Post-processing the dict representation guarantees every null-like
        scalar (``np.nan``, ``pd.NA``, ``pd.NaT``) becomes a real ``None``
        that SQL drivers can bind as NULL.
        """
        return [{k: None if pd.isna(v) else v for k, v in row.items()} for row in values]

    @staticmethod
    def _safe_bulk_insert(
        conn: sa.Connection,
        table: Table,
        values: Sequence[dict[Any, Any] | tuple[Any, ...]],
        max_params: int | None = None,
    ) -> None:
        """
        Allows insertion of multiple values paying attention to parameter limits

        :param conn: An SQLAlchemy connection
        :param table: An SQLAlchemy table
        :param values: List of tuples to insert
        :param max_params: Maximum number of parameters to allow, or None if unlimited
        :return: None
        """
        if not values:
            return

        if not max_params:
            # Pass `values` as `execute()`'s parameters rather than via `.values(...)`.
            # `insert(table).values(values)` is SQLAlchemy's "multi-values" Core idiom: it
            # compiles ONE INSERT statement with inline VALUES tuples, flattening the row
            # dicts into synthetic per-position parameter names (`col_m0`, `col_m1`, ...).
            # That's harmless for dialects whose DBAPI just binds one flat parameter dict to
            # one literal statement, but ClickHouse's SQLAlchemy dialect
            # (`clickhouse_sqlalchemy`) unconditionally forces the executemany path for any
            # non-empty INSERT and its compiler strips the inline VALUES text, expecting real
            # per-row dicts keyed by actual column names instead -- so the driver raises a
            # `KeyError` on the first column it looks up. `execute(insert(table), values)` is
            # SQLAlchemy's documented dialect-transparent executemany calling convention: it
            # lets each dialect compile and bind the statement the way it needs to.
            #
            # `execute()`'s parameters overload only accepts mapping rows (not tuples), unlike
            # `.values()`. The sole caller (`setup()`, below) always passes rows produced by
            # `_sanitize_null_values`, which are always dicts, so this narrows the declared
            # `dict | tuple` union to the shape actually passed at runtime.
            conn.execute(insert(table), cast("Sequence[Mapping[str, Any]]", values))
        else:
            num_columns = len(values[0])
            max_rows = max_params // num_columns

            for i in range(0, len(values), max_rows):
                chunk = values[i : i + max_rows]
                conn.execute(insert(table).values(chunk))

    @override
    def setup(self) -> None:
        engine, cleanup = self._get_engine()

        with engine.connect() as conn:
            # create schema if needed

            if self.schema:
                logger.info(f"CREATING SCHEMA {self.schema}")
                conn.execute(TextClause(f"CREATE SCHEMA {self.schema}"))

            # create tables
            all_table_data = self._ensure_all_table_data_created()
            self.metadata.create_all(conn)

            # insert data
            for table_data in all_table_data:
                # pd.DataFrame(...).to_dict("index") returns a dictionary where the keys are the row
                # index and the values are a dict of column names mapped to column values.
                # Then we pass that list of dicts in as parameters to our insert statement.
                #   INSERT INTO test_table (my_int_column, my_str_column) VALUES (?, ?)
                #   [...] [('1', 'foo'), ('2', 'bar')]
                # Convert to dicts, then sanitize: replace all pandas/numpy null
                # sentinels (NaN, NA, NaT) with Python None so SQL drivers can
                # handle them.  df.where() cannot do this for float columns
                # because None is cast back to np.nan in numeric dtypes.
                values = list(table_data.df.to_dict("index").values())
                values = self._sanitize_null_values(values)
                self._safe_bulk_insert(
                    conn, table_data.table, values, self.backend_spec.insert_parameter_limit
                )

            # Commit transaction (safe for databases without transaction support)
            self._safe_commit(conn)
        cleanup()

    @override
    def teardown(self) -> None:
        engine, cleanup = self._get_engine()
        with engine.connect() as conn:
            for table in self.tables:
                table.drop(conn)
            if self.schema:
                logger.info(f"DROPPING SCHEMA {self.schema}")
                conn.execute(TextClause(f"DROP SCHEMA {self.schema}"))
            # Commit transaction (safe for databases without transaction support)
            self._safe_commit(conn)
        cleanup()

    def _create_table_name(self, label: Optional[str] = None) -> str:
        parts = ["expectation_test_table", label, self._random_resource_name()]
        return "_".join([part for part in parts if part])

    def _ensure_all_table_data_created(self) -> Sequence[_TableData]:
        return [self.main_table_data, *self.extra_table_data.values()]

    def _create_table_data(
        self, name: str, df: pd.DataFrame, column_types: Mapping[str, type[TypeEngine]]
    ) -> _TableData:
        columns = self._get_column_types(df=df, column_types=column_types)
        table = self._create_table(name, columns=columns)
        return _TableData(
            name=name,
            df=df,
            table=table,
        )

    def _create_table(self, name: str, columns: InferredColumnTypes) -> Table:
        column_list = [Column(col_name, col_type) for col_name, col_type in columns.items()]
        # Called once per table: a dialect storage-engine construct binds to the first table it
        # is attached to, so each table needs freshly constructed items rather than one instance
        # shared across every table this setup creates.
        table_schema_items = self.backend_spec.table_schema_items
        items = table_schema_items() if table_schema_items is not None else ()
        return Table(name, self.metadata, *column_list, *items, schema=self.schema)

    def _get_column_types(
        self,
        df: pd.DataFrame,
        column_types: Mapping[str, type[TypeEngine]],
    ) -> InferredColumnTypes:
        all_column_types = self._infer_column_types(df)
        # prefer explicit types if they're provided
        all_column_types.update(column_types)
        untyped_columns = set(df.columns) - set(all_column_types.keys())
        if untyped_columns:
            config_class_name = self.config.__class__.__name__
            message = (
                f"Unable to infer types for the following column(s): "
                f"{', '.join(untyped_columns)}. \n"
                f"Please provide the missing types as the `column_types` "
                f"parameter when \ninstantiating {config_class_name}."
            )
            raise RuntimeError(message)
        return all_column_types

    def _normalize_python_type(self, value_type: type) -> type:
        """Normalize numpy types to their Python equivalents for type inference."""
        if issubclass(value_type, np.integer):
            return int
        if issubclass(value_type, np.floating):
            return float
        if issubclass(value_type, np.bool_):
            return bool
        return value_type

    def _infer_column_types(self, data: pd.DataFrame) -> InferredColumnTypes:
        inferred_column_types: InferredColumnTypes = {}
        for column, value_list in data.to_dict("list").items():
            non_null_value_list = [val for val in value_list if not (val is None or pd.isna(val))]
            if not non_null_value_list:
                # if we have an all null column, just arbitrarily use INTEGER
                inferred_column_types[str(column)] = sqltypes.INTEGER
            else:
                # Normalize the first value's type (e.g., numpy.int64 -> int)
                first_value_type = type(non_null_value_list[0])
                normalized_type = self._normalize_python_type(first_value_type)

                # Check if all values match the normalized type
                if not all(
                    self._normalize_python_type(type(val)) == normalized_type
                    for val in non_null_value_list
                ):
                    raise RuntimeError(
                        f"Cannot infer type of column {column}. "
                        "Please provide an explicit column type in the test config."
                    )
                # Get inferred type from lookup using normalized type
                # (normalized_type handles numpy types -> Python types conversion)
                inferred_type = self.inferrable_types_lookup.get(normalized_type)
                if inferred_type:
                    inferred_column_types[str(column)] = inferred_type
        return inferred_column_types
