from datetime import datetime
from typing import Mapping, Optional

import pandas as pd
import pytest

from great_expectations.compatibility.sqlalchemy import sqltypes
from great_expectations.compatibility.typing_extensions import override
from great_expectations.data_context import AbstractDataContext
from great_expectations.datasource.fluent.sql_datasource import TableAsset
from tests.integration.sql_session_manager import SessionSQLEngineManager
from tests.integration.test_utils.data_source_config.backend_spec import SqlBackendSpec
from tests.integration.test_utils.data_source_config.base import BatchTestSetup
from tests.integration.test_utils.data_source_config.data_source_spec import (
    CiLaneRef,
    DataSourceProvisioning,
    ExecutionEngineKind,
    SupportTier,
)
from tests.integration.test_utils.data_source_config.registry import register_sql_config
from tests.integration.test_utils.data_source_config.sql import (
    InferrableTypesLookup,
    SQLBatchTestSetup,
)
from tests.integration.test_utils.data_source_config.sql_config import SqlDatasourceTestConfig

try:
    # No stub and no package to import from in most environments -- this dialect is installed
    # only in the lane that runs against Exasol, and the type-check environment installs no
    # backend driver at all. That absence is the condition this block exists to detect, so the
    # error it raises there is the expected state rather than a missing dependency. The ignore
    # must come first on the line: mypy reads `# type: ignore` only as the leading comment.
    import sqlalchemy_exasol  # type: ignore[import-not-found] # noqa: F401
except ImportError:
    _EXASOL_DIALECT_INSTALLED = False
else:
    _EXASOL_DIALECT_INSTALLED = True

_BINARY_FLOAT_INT_OVERRIDE: InferrableTypesLookup = (
    {int: sqltypes.DOUBLE_PRECISION} if _EXASOL_DIALECT_INSTALLED else {}
)
"""The `int` override, named only where the dialect that needs it is installed.

`DOUBLE_PRECISION` is a SQLAlchemy 2.0 addition. This module is imported under 1.4 all the same:
the `py310-min-install` and `py311-min-install` lanes pin `sqlalchemy<2.0.0`, and collection
imports `tests/integration/conftest.py`, which imports this config package. Naming the type
unconditionally therefore kills collection outright in two lanes that never connect to Exasol.

The guard keys on the dialect package, not on the attribute, and that axis is the point. Keyed
here, the empty branch is unreachable by construction wherever Exasol runs: `sqlalchemy-exasol`
declares `sqlalchemy>=2.0.0,<3` in its own distribution metadata, so pip cannot resolve an
environment that holds the dialect and lacks `DOUBLE_PRECISION`. The same absence that empties this
mapping also makes the `exa` dialect unloadable, so a lane without the override is a lane with no
Exasol to talk to, and the row recording this rendering skips rather than asserting the wrong type.
`clickhouse.py` guards its own map on that same axis. Keyed to the attribute, as this was,
unreachability rested instead on that version pin being argued in prose -- a constraint nothing in
this repository reads, which a dependency bump could relax with no line here moving.

The dialect-present/attribute-absent combination now fails loudly: it raises `AttributeError` at
import rather than degrading to the shared `INTEGER` default. pip cannot produce that combination,
so the difference is theoretical -- but a silent fallback there would reintroduce exactly the
failure the comment on `int` below describes, and reintroduce it without a line here moving.

`DOUBLE_PRECISION_FLOAT_OVERRIDE` in `sql.py` shares the shape -- a 2.0 type named only behind a
guard -- but not the axis, and it keeps its `hasattr` form for a reason that does not apply here:
its degraded branch is caught, because it overrides `float`, which the renderings table records for
both Databricks and SingleStore, so losing it turns those rows red. This override had no such row
until `int` was recorded for Exasol alongside it.
"""


@register_sql_config
class ExasolDatasourceTestConfig(SqlDatasourceTestConfig):
    DATA_SOURCE_SPEC = SqlBackendSpec(
        label="exasol",
        public_name="Exasol",
        marker="exasol",
        provisioning=DataSourceProvisioning.LOCAL_CONTAINER,
        execution_engine=ExecutionEngineKind.SQL,
        fluent_types=frozenset({"sql"}),
        ci_lane=CiLaneRef(workflow_job="marker-tests", marker_token="exasol"),
        # Unlike Oracle (where a schema is a user), SingleStore, and ClickHouse, Exasol has
        # first-class schemas: `CREATE SCHEMA` and `DROP SCHEMA` are plain supported DDL, so the
        # shared setup's schema isolation works here as it does for PostgreSQL. Teardown drops
        # every table before the schema, which is what keeps the bare `DROP SCHEMA` (no CASCADE)
        # valid. Exasol folds unquoted identifiers to upper case, and the harness's generated
        # schema and table names are all lower case with legal characters, so SQLAlchemy renders
        # them unquoted and both the DDL and the later queries fold to the same upper-case name.
        uses_schema=True,
        # No transaction_mode override: Exasol has real transactions and the driver commits
        # explicitly, so the shared default (explicit commit) already matches its behavior.
        column_type_overrides={
            # Exasol's VARCHAR requires a length -- it is declared with a `max length` create
            # parameter, and a bare one is rejected at parse time with
            # `syntax error, unexpected ')', expecting '('`. MySQL, SingleStore, and Oracle
            # declare the same override for the same reason.
            str: sqltypes.VARCHAR(255),
            # Exasol has no DATETIME type at all; it is absent from the server's own type list,
            # and naming it fails with `syntax error, unexpected IDENTIFIER_PART_`. TIMESTAMP is
            # the type this dialect actually has.
            datetime: sqltypes.TIMESTAMP,
            pd.Timestamp: sqltypes.TIMESTAMP,
            # `int` is mapped to DOUBLE PRECISION, this dialect's native binary float, and this
            # is a deliberate trade rather than a dialect requirement -- INTEGER itself works
            # fine for storage. The problem is on the way back out: this driver returns an
            # exact-numeric (DECIMAL) result as a Python `str`, and `SUM()` over any
            # exact-numeric column widens to a DECIMAL wide enough to trigger that. So a summed
            # INTEGER column arrives as `'90'`, and core's `_validate_metric_value_between`
            # compares it against a float and raises `TypeError: '>=' not supported between
            # instances of 'str' and 'float'`, failing every aggregate case in the curated suite.
            #
            # Nothing else reaches it. Every exact-numeric type this dialect has behaves the
            # same way under SUM (INTEGER, SMALLINT, BIGINT, DECIMAL(9,0) and DECIMAL(18,0) all
            # verified returning `str`); only TINYINT, capped at 999 and so unusable for
            # arbitrary test data, and the binary floats come back numeric. Naming a DECIMAL
            # with explicit precision and scale -- the remedy Oracle declares for its own
            # numeric trouble -- is closed off here for two independent reasons: a precision of
            # 38 exceeds this dialect's maximum of 36 and is rejected outright with `illegal
            # precision value: 38`, and this driver returns every DECIMAL with a non-zero scale
            # as a Python `str` at any precision (verified at DECIMAL(36,10), (18,10) and (15,2)
            # alike). pyexasol ships an opt-in `fetch_mapper` that would convert these properly,
            # but the dialect exposes only ENCRYPTION, SSLCertificate, AUTOCOMMIT and FINGERPRINT
            # as URL parameters, so a connection string cannot request it -- and even
            # engine-level `connect_args` would not help, because the metrics run on the engine
            # GX builds internally from this connection string, not on the harness's own engine.
            #
            # The cost is that integer columns are stored as 64-bit binary floats, exact only
            # below 2^53. That is accepted here because this declaration's only consumer is
            # type inference for harness-created test tables, whose values are small. The
            # honest fix is upstream -- either the driver/dialect coercing exact numerics, or
            # core tolerating a numeric string -- and this override should be removed once
            # either lands.
            **_BINARY_FLOAT_INT_OVERRIDE,
        },
        dev_requirements_file="reqs/requirements-dev-exasol.txt",
        task_runner_marker="exasol",
        container_service="exasol",
        tiers=frozenset({SupportTier.CURATED_SQL, SupportTier.FLUENT_API}),
        tier_case_exclusions={
            # A driver defect, not a dialect gap -- the same category as ClickHouse's
            # `quoted_identifiers` entry. sqlalchemy-exasol 7.1.3's `EXACompiler.extract_map`
            # maps `year`, `month` and `day` to the strftime tokens `%Y`, `%m` and `%d`, so
            # SQLAlchemy's generic `visit_extract` renders the date partitioner's
            # `sa.func.extract("year", col)` as `EXTRACT(%Y FROM col)`, which the server rejects
            # with `syntax error, unexpected invalid token`. The server itself accepts
            # `EXTRACT(YEAR FROM col)` (verified live), so the fix belongs in the driver's
            # compiler, not in this harness and not in core. An issue still needs to be filed
            # for this defect; this reason will be updated with its link once one exists.
            "batch_definition": (
                "sqlalchemy-exasol 7.1.3 compiles every EXTRACT as `EXTRACT(%Y FROM col)` -- its "
                "compiler's extract_map holds strftime tokens -- so the date partitioner's query "
                "is rejected with `syntax error, unexpected invalid token`. The server accepts "
                "`EXTRACT(YEAR FROM col)`, so this is a driver defect. An issue still needs to be "
                "filed for this defect."
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
        return ExasolBatchTestSetup(
            data=data,
            config=self,
            extra_data=extra_data,
            table_name=self.table_name,
            context=context,
            engine_manager=engine_manager,
        )


class ExasolBatchTestSetup(SQLBatchTestSetup[ExasolDatasourceTestConfig]):
    # `exa+websocket` names the pure-Python WebSocket driver chain (sqlalchemy-exasol ->
    # pyexasol -> websocket-client), which is what this lane's requirements file installs; there
    # is no ODBC path. Port 8563 is Exasol's default and the port the compose file publishes.
    # `sys`/`exasol` are the image's built-in credentials, so no CI secret is needed.
    _BASE_CONNECTION_STRING = "exa+websocket://sys:exasol@127.0.0.1:8563"

    # Exasol requires TLS, and the container generates a self-signed certificate at start. Its
    # fingerprint is therefore not knowable when this constant is written, which rules out the
    # `FINGERPRINT` route a fixed connection string would otherwise use; disabling verification
    # is what a throwaway local container can express instead.
    _SSL_QUERY = "SSLCertificate=SSL_VERIFY_NONE"

    @override
    def build_connection_string(self, schema: str | None = None) -> str:
        # This dialect takes the schema to open as the URL's path segment, so a schema-targeting
        # string is the base string plus that path -- the shape this repo's earlier Exasol
        # connection helper also used.
        path = f"/{schema}" if schema else ""
        return f"{self._BASE_CONNECTION_STRING}{path}?{self._SSL_QUERY}"

    @override
    def make_asset(self) -> TableAsset:
        # No Exasol-specific fluent datasource exists, so this reaches its datasource through the
        # dialect-agnostic SQL datasource instead.
        return self.context.data_sources.add_sql(
            name=self._random_resource_name(),
            connection_string=self.build_connection_string(schema=self.schema),
        ).add_table_asset(
            name=self._random_resource_name(),
            table_name=self.table_name,
        )
