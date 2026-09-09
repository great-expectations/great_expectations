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

_BINARY_FLOAT_INT_OVERRIDE: InferrableTypesLookup = (
    {int: sqltypes.DOUBLE_PRECISION} if hasattr(sqltypes, "DOUBLE_PRECISION") else {}
)
"""The `int` override, named only where the type it names exists.

`DOUBLE_PRECISION` is a SQLAlchemy 2.0 addition. This module is imported under 1.4 all the same:
the `py310-min-install` and `py311-min-install` lanes pin `sqlalchemy<2.0.0`, and collection
imports `tests/integration/conftest.py`, which imports this config package. Naming the type
unconditionally therefore kills collection outright in two lanes that never connect to Exasol.
Same shape and same reason as `DOUBLE_PRECISION_FLOAT_OVERRIDE` in `sql.py`, which Databricks and
SingleStore share.

The empty branch is unreachable wherever Exasol actually runs: `sqlalchemy-exasol` requires
`sqlalchemy>=2.0.0,<3`, so no lane can both install this dialect and be on 1.4. A lane on 1.4 has
no Exasol to talk to, and every lane that has one gets the override.
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
