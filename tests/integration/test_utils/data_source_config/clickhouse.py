from datetime import date, datetime
from typing import TYPE_CHECKING, Mapping, Optional, Sequence

import pandas as pd
import pytest

from great_expectations.compatibility.sqlalchemy import TextClause
from great_expectations.compatibility.typing_extensions import override
from great_expectations.data_context import AbstractDataContext
from great_expectations.datasource.fluent.sql_datasource import TableAsset
from tests.integration.sql_session_manager import SessionSQLEngineManager
from tests.integration.test_utils.data_source_config.backend_spec import (
    SqlBackendSpec,
    TableSchemaItemFactory,
    TransactionMode,
)
from tests.integration.test_utils.data_source_config.base import BatchTestSetup
from tests.integration.test_utils.data_source_config.data_source_spec import (
    CiLaneRef,
    DataSourceProvisioning,
    ExecutionEngineKind,
    SupportTier,
)
from tests.integration.test_utils.data_source_config.registry import register_sql_config
from tests.integration.test_utils.data_source_config.sql import SQLBatchTestSetup
from tests.integration.test_utils.data_source_config.sql_config import SqlDatasourceTestConfig

if TYPE_CHECKING:
    import sqlalchemy as sa  # type-only, exactly as `sql.py` and `backend_spec.py` do it

try:
    from clickhouse_sqlalchemy import types as clickhouse_types
except ImportError:
    clickhouse_types = None


def _clickhouse_table_engines() -> Sequence["sa.sql.schema.SchemaItem"]:
    from clickhouse_sqlalchemy import engines

    return (engines.MergeTree(order_by=TextClause("tuple()")),)


# Alias-conformance binding: this is the value the record declares, and its annotation is the
# framework's alias rather than a restatement of the signature.
_CLICKHOUSE_TABLE_SCHEMA_ITEMS: TableSchemaItemFactory = _clickhouse_table_engines

# Every ClickHouse type is wrapped in `Nullable(...)` because this dialect carries nullability
# as a property of the column type rather than as a column attribute: the dialect's DDL compiler
# forces `nullable=True` on every column regardless, so an unwrapped type would still compile but
# reject nulls at insert. The harness's insert path converts pandas/numpy null sentinels to real
# `None` before insert, so this matters for round-tripping nulls correctly.
#
# `int -> Int64` replaces the shared map's `INTEGER`, ClickHouse's 32-bit alias. `float ->
# Float64` replaces its `Float(precision=53)`: this dialect discards the precision, and the bare
# `FLOAT` left over is this server's *single*-precision `Float32` -- verified against a live
# server, which stores 16777217.0 as 16777216.0 under it and exactly under `Float64`. That is the
# same narrowing Databricks needs its own override for, reached the same way (see
# `DOUBLE_PRECISION_FLOAT_OVERRIDE` in `sql.py`); here the `Nullable(...)` wrapper means the entry
# has to be declared regardless, so it names the 64-bit type rather than deferring to a shared
# default that would not survive the trip. `date -> Date32` and `datetime -> DateTime64()` replace
# the shared map's narrower `DATE` (starts at 1970) and `DateTime()`, which renders a
# second-resolution `DateTime` on this dialect; `pd.Timestamp` is overridden alongside `datetime`
# because it is a separate key in the shared inferred-type map. No length is declared for `str`:
# a length on this dialect's generic `String` type renders `FixedString(n)`, a padded fixed-width
# type, not what's wanted here -- unlike the length-carrying `str` override other backends
# declare.
#
# The map is built eagerly at module scope, behind an import guard, because -- unlike the
# table-schema-item factory above, which is deferred since its engine object binds to a table --
# this map is immutable, shareable, inert data with no per-table freshness need. This resolves to
# an empty mapping when the dialect package is absent, so importing this module never requires
# the dialect package to be installed.
_COLUMN_TYPE_OVERRIDES = (
    {}
    if clickhouse_types is None
    else {
        str: clickhouse_types.Nullable(clickhouse_types.String),
        int: clickhouse_types.Nullable(clickhouse_types.Int64),
        float: clickhouse_types.Nullable(clickhouse_types.Float64),
        bool: clickhouse_types.Nullable(clickhouse_types.Boolean),
        date: clickhouse_types.Nullable(clickhouse_types.Date32),
        datetime: clickhouse_types.Nullable(clickhouse_types.DateTime64()),
        pd.Timestamp: clickhouse_types.Nullable(clickhouse_types.DateTime64()),
    }
)


@register_sql_config
class ClickHouseDatasourceTestConfig(SqlDatasourceTestConfig):
    DATA_SOURCE_SPEC = SqlBackendSpec(
        label="clickhouse",
        public_name="ClickHouse",
        marker="clickhouse",
        provisioning=DataSourceProvisioning.LOCAL_CONTAINER,
        execution_engine=ExecutionEngineKind.SQL,
        fluent_types=frozenset({"sql"}),
        ci_lane=CiLaneRef(workflow_job="marker-tests", marker_token="clickhouse"),
        # ClickHouse has no `CREATE SCHEMA`; the database is carried in the connection string,
        # as MySQL's is.
        uses_schema=False,
        # ClickHouse has no standard transactions; its rollback is a no-op and its DBAPI commit
        # is a no-op.
        transaction_mode=TransactionMode.AUTOCOMMIT,
        table_schema_items=_CLICKHOUSE_TABLE_SCHEMA_ITEMS,
        column_type_overrides=_COLUMN_TYPE_OVERRIDES,
        dev_requirements_file="reqs/requirements-dev-clickhouse.txt",
        task_runner_marker="clickhouse",
        container_service="clickhouse",
        tiers=frozenset({SupportTier.CURATED_SQL, SupportTier.FLUENT_API}),
        tier_case_exclusions={
            # Same root cause already recorded on this backend's scoped module's
            # `test_match_regex`/`test_not_match_regex`: the dialect's regex-matching path calls
            # a SQL function this dialect does not have. ClickHouse has no `regexp_like` function;
            # the server rejects the query with
            # `DB::Exception: Function with name 'regexp_like' does not exist`. An issue still
            # needs to be filed for this defect; this reason will be updated with its link once
            # one exists.
            "regex_match": (
                "ClickHouse has no `regexp_like` SQL function; the dialect's regex-matching path "
                "calls it and the server rejects the query with `DB::Exception: Function with "
                "name 'regexp_like' does not exist`. An issue still needs to be filed for this "
                "defect."
            ),
            # A driver defect, not a dialect gap: this dialect's SQLAlchemy layer inserts rows
            # through an executemany path that keys each row by the sanitized bind-parameter
            # name (for example `user_name`) rather than the real column name (`user name`) for
            # any identifier that needs quoting, so the insert raises a `KeyError` at insert time
            # and the table this case needs ends up with zero rows. An issue still needs to be
            # filed for this defect; this reason will be updated with its link once one exists.
            "quoted_identifiers": (
                "This dialect's SQLAlchemy/driver insert path keys each row by the sanitized "
                "bind-parameter name instead of the real column name for identifiers requiring "
                "quoting, raising a `KeyError` at insert time and leaving the table empty. An "
                "issue still needs to be filed for this defect."
            ),
        },
    )

    @override
    def create_batch_setup(
        self,
        request: pytest.FixtureRequest,
        data: pd.DataFrame,
        extra_data: Mapping[str, pd.DataFrame],
        context: AbstractDataContext,
        engine_manager: Optional[SessionSQLEngineManager] = None,
    ) -> BatchTestSetup:
        return ClickHouseBatchTestSetup(
            data=data,
            config=self,
            extra_data=extra_data,
            table_name=self.table_name,
            context=context,
            engine_manager=engine_manager,
        )


class ClickHouseBatchTestSetup(SQLBatchTestSetup[ClickHouseDatasourceTestConfig]):
    # Native driver on port 9000, carrying the container's test database. A bare `clickhouse://`
    # scheme resolves to the HTTP driver, which serialises through tab-separated text rather than
    # typed binary values, so the scheme is always written out explicitly.
    _BASE_CONNECTION_STRING = "clickhouse+native://localhost:9000/test_ci"

    @override
    def build_connection_string(self, schema: str | None = None) -> str:
        # This backend declares no schema support, so `schema` is unused; the signature is the
        # shared abstract one.
        return self._BASE_CONNECTION_STRING

    @override
    def make_asset(self) -> TableAsset:
        # No ClickHouse-specific fluent datasource type exists, so this reaches its datasource
        # through the dialect-agnostic SQL datasource instead.
        return self.context.data_sources.add_sql(
            name=self._random_resource_name(),
            connection_string=self.build_connection_string(),
        ).add_table_asset(
            name=self._random_resource_name(),
            table_name=self.table_name,
        )
