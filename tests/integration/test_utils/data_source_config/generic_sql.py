from __future__ import annotations

import dataclasses
import os
from dataclasses import dataclass
from typing import TYPE_CHECKING, Mapping, Optional

from great_expectations.compatibility.sqlalchemy import sqltypes
from great_expectations.compatibility.typing_extensions import override
from tests.integration.test_utils.data_source_config.backend_spec import (
    SqlBackendSpec,
    TransactionMode,
)
from tests.integration.test_utils.data_source_config.data_source_spec import (
    CiLaneRef,
    DataSourceProvisioning,
    ExecutionEngineKind,
)
from tests.integration.test_utils.data_source_config.sql import SQLBatchTestSetup
from tests.integration.test_utils.data_source_config.sql_config import SqlDatasourceTestConfig

if TYPE_CHECKING:
    import pandas as pd
    import pytest

    from great_expectations.data_context import AbstractDataContext
    from great_expectations.datasource.fluent.sql_datasource import TableAsset
    from tests.integration.sql_session_manager import SessionSQLEngineManager
    from tests.integration.test_utils.data_source_config.base import BatchTestSetup


@dataclass(frozen=True, eq=False)
class GenericSQLDatasourceTestConfig(SqlDatasourceTestConfig):
    """Config for testing against any SQL backend via a caller-provided connection string.

    Unlike the dialect-specific configs (e.g. PostgreSQLDatasourceTestConfig), the connection
    string is not baked in — it must be supplied at construction time. This makes the config
    reusable across any SQLAlchemy-compatible database, but it also means this config has no
    fixed identity to enrol in the SQL backend registry: it is deliberately never decorated with
    `@register_sql_config`, and must never appear in the set that gates CI.

    `eq=False` is required here for the same reason `sql_config.py`'s class docstring gives for
    the base class itself: this class adds fields, which requires re-decorating with
    `@dataclass`, and a bare `@dataclass(frozen=True)` would silently regenerate `__eq__` and
    `__hash__` — discarding `DataSourceTestConfig`'s hand-written `__hash__`, which reduces
    `extra_column_types` to a hashable tuple before hashing it, in favor of one that hashes the
    raw `dict` value and raises on every instance.
    """

    DATA_SOURCE_SPEC = SqlBackendSpec(
        label="generic_sql",
        public_name="Generic SQL",
        marker="generic_sql",
        provisioning=DataSourceProvisioning.EXTERNAL_CREDENTIALS,
        execution_engine=ExecutionEngineKind.SQL,
        fluent_types=frozenset({"sql"}),
        ci_lane=CiLaneRef(workflow_job="marker-tests", marker_token="generic_sql"),
        uses_schema=False,
        # The caller-supplied connection string may point at a dialect that requires a length
        # for VARCHAR, and this backend cannot know in advance which dialect that will be.
        column_type_overrides={str: sqltypes.VARCHAR(255)},
    )

    connection_string: str = ""

    autocommit: bool = False
    """Per-instance transaction mode, since this config has no fixed identity for `DATA_SOURCE_SPEC`
    to state it once. `__post_init__` below derives a `backend_spec_override` from it, using the
    per-instance seam `SqlDatasourceTestConfig` defines for exactly this: a declaration that
    varies per call rather than per class. Leave unset for the default explicit-commit mode.
    """

    def __post_init__(self) -> None:
        if self.autocommit and self.backend_spec_override is None:
            # An override must also vary the label: two instances that differ only in
            # `autocommit` would otherwise declare the same label, and the session-scoped
            # batch-setup cache is keyed on config equality (which compares label, not this
            # field) - so they would collide and silently share one setup, with the second
            # instance inheriting the first instance's transaction behavior.
            object.__setattr__(
                self,
                "backend_spec_override",
                dataclasses.replace(
                    self.DATA_SOURCE_SPEC,
                    transaction_mode=TransactionMode.AUTOCOMMIT,
                    label=f"{self.DATA_SOURCE_SPEC.label}_autocommit",
                ),
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
        return GenericSQLBatchTestSetup(
            data=data,
            config=self,
            extra_data=extra_data,
            table_name=self.table_name,
            context=context,
            engine_manager=engine_manager,
        )


class GenericSQLBatchTestSetup(SQLBatchTestSetup[GenericSQLDatasourceTestConfig]):
    """Batch setup that works with any SQLAlchemy connection string.

    Uses ``context.data_sources.add_sql`` — the dialect-agnostic datasource —
    so callers only need to provide a valid connection string.

    If no connection_string is provided in the config, reads from the
    GX_TEST_GENERIC_SQL_CONNECTION_STRING environment variable.
    """

    def __init__(
        self,
        config: GenericSQLDatasourceTestConfig,
        data: pd.DataFrame,
        extra_data: Mapping[str, pd.DataFrame],
        context: AbstractDataContext,
        table_name: Optional[str] = None,
        engine_manager: Optional[SessionSQLEngineManager] = None,
    ) -> None:
        # Read from environment variable if connection_string is empty
        self._connection_string = config.connection_string
        if not self._connection_string:
            self._connection_string = os.environ.get("GX_TEST_GENERIC_SQL_CONNECTION_STRING", "")
        if not self._connection_string:
            raise ValueError(
                "GenericSQLBatchTestSetup requires a connection string. "
                "Either pass connection_string to GenericSQLDatasourceTestConfig "
                "or set GX_TEST_GENERIC_SQL_CONNECTION_STRING environment variable."
            )
        # `config.autocommit` is the in-code declaration; a non-empty environment variable
        # is an additional, out-of-code way to request the same behavior, read here (batch-setup
        # construction time) rather than at import, alongside the connection-string read above.
        # Unlike `autocommit`, this variable is never folded into a label: it is process-global,
        # so every escape-hatch setup in a run resolves it identically and no two cache entries
        # can disagree - unlike the per-instance field, where two instances in one session can
        # differ and would otherwise collide.
        self._autocommit = config.autocommit or bool(
            os.environ.get("GX_TEST_GENERIC_SQL_AUTOCOMMIT", "")
        )
        super().__init__(
            config=config,
            data=data,
            extra_data=extra_data,
            table_name=table_name,
            engine_manager=engine_manager,
            context=context,
        )

    @property
    @override
    def backend_spec(self) -> SqlBackendSpec:
        spec = self.config.backend_spec
        if self._autocommit and spec.transaction_mode is not TransactionMode.AUTOCOMMIT:
            return dataclasses.replace(spec, transaction_mode=TransactionMode.AUTOCOMMIT)
        return spec

    @override
    def build_connection_string(self, schema: str | None = None) -> str:
        return self._connection_string

    @override
    def make_asset(self) -> TableAsset:
        return self.context.data_sources.add_sql(
            name=self._random_resource_name(),
            connection_string=self.build_connection_string(),
        ).add_table_asset(
            name=self._random_resource_name(),
            table_name=self.table_name,
        )
