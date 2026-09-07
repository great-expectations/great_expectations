"""What each registered SQL backend's fixture columns actually become, written down.

A shared default is a per-dialect decision even though it is written once, and a backend's own
override is the other half of that decision. What a table ends up holding is the two together, so
that is what this records: one row per registered SQL backend, resolved from its declaration and
compiled against its dialect.

Recording the shared defaults alone would leave the half a reader most needs to see. An override
can change, or be dropped, and move nothing here -- which is how a backend whose datetime column
had been pinned to a sub-second type could come to be created as a second-resolution one with no
line disagreeing.

This does not know which renderings are right; no local check can, since a type name means whatever
the server says it means, and only a run against one settles that. What it does is make the
renderings visible: editing a shared default or a backend override changes this table in the same
diff, so the per-backend consequences are read at review time rather than discovered a
continuous-integration run later.

A module of its own, rather than a class inside `test_sql_batch_test_setup.py`, because of how each
case has to be marked. A backend's row is only worth checking where that backend's dialect is
installed, which means the case must carry that backend's own pytest marker -- and every test must
carry exactly one required marker (`tests/conftest.py`, `--verify-marker-coverage-and-exit`). A
module-level marker would be a second one on every case here.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, List, Mapping, Type, Union

import pandas as pd
import pytest
import sqlalchemy as sa
from sqlalchemy.dialects import registry as sa_dialect_registry

from tests.integration.test_utils.data_source_config import iter_data_source_specs
from tests.integration.test_utils.data_source_config import sql as sql_module
from tests.integration.test_utils.data_source_config.backend_spec import SqlBackendSpec

if TYPE_CHECKING:
    from _pytest.mark.structures import ParameterSet

    from great_expectations.compatibility.sqlalchemy import TypeEngine


def _rendered(sql_type: Union[Type[TypeEngine], TypeEngine], dialect: sa.engine.Dialect) -> str:
    """The DDL fragment one dialect emits for one declared type."""
    instance = sql_type() if isinstance(sql_type, type) else sql_type
    return str(instance.compile(dialect=dialect))


def _sql_backend_specs() -> List[SqlBackendSpec]:
    """Every registered SQL backend's declaration, in label order.

    Read from the registry rather than named here, so a backend added later is one this module
    accounts for without an edit to this module -- and is not silently skipped by it.
    """
    return [spec for spec in iter_data_source_specs() if isinstance(spec, SqlBackendSpec)]


def _backend_params() -> List[ParameterSet]:
    """One parameter per registered SQL backend, carrying that backend's own pytest marker.

    The marker is what puts a backend's case in the lane that installs its dialect. Seven of these
    backends have a third-party dialect that only their own lane installs, so without it their
    recorded rows would be checked in no lane at all.

    Exactly one marker per case, never a second: a test carrying two required markers fails the
    repository's marker-coverage check as surely as one carrying none. A backend declaring no
    marker of its own has no lane to be routed to, so its case is marked as what it then is -- a
    check on a property of this project, run wherever that lane runs.
    """
    return [
        pytest.param(spec, marks=getattr(pytest.mark, spec.marker or "project"), id=spec.label)
        for spec in _sql_backend_specs()
    ]


_RESOLVED: Mapping[str, Mapping[str, str]] = {
    "big-query": {"dialect": "bigquery", "float": "FLOAT64", "datetime": "DATETIME"},
    "clickhouse": {
        "dialect": "clickhouse",
        "float": "Nullable(Float64)",
        "datetime": "Nullable(DateTime64(3))",
    },
    "databricks": {"dialect": "databricks", "float": "DOUBLE", "datetime": "TIMESTAMP_NTZ"},
    "mssql": {"dialect": "mssql", "float": "FLOAT(53)", "datetime": "DATETIME"},
    "mysql": {"dialect": "mysql", "float": "FLOAT(53)", "datetime": "DATETIME"},
    "oracle": {"dialect": "oracle", "float": "DECIMAL(38, 10)", "datetime": "TIMESTAMP"},
    "postgresql": {
        "dialect": "postgresql",
        "float": "FLOAT(53)",
        "datetime": "TIMESTAMP WITHOUT TIME ZONE",
    },
    "redshift": {
        "dialect": "redshift",
        "float": "FLOAT(53)",
        "datetime": "TIMESTAMP WITHOUT TIME ZONE",
    },
    "singlestore": {"dialect": "singlestoredb", "float": "DOUBLE", "datetime": "DATETIME"},
    # Lowercase as this dialect emits it; the server reads type names case-insensitively.
    "snowflake": {"dialect": "snowflake", "float": "FLOAT", "datetime": "datetime"},
    "sqlite": {"dialect": "sqlite", "float": "FLOAT", "datetime": "DATETIME"},
    "trino": {"dialect": "trino", "float": "DOUBLE", "datetime": "TIMESTAMP"},
}
"""Keyed by the backend's own declared label; `dialect` names the SQLAlchemy dialect it connects
through, which is spelled differently from the label for two of them.

`float` and `datetime` only, because those two are where a dialect's reading of a type name has
actually diverged from what the harness meant. `pd.Timestamp` carries no column of its own: it is
asserted to render as `datetime` does, since a backend overriding one and not the other is a defect
rather than a fact worth recording.
"""


class TestEveryBackendResolvesTheColumnTypesRecordedHere:
    @pytest.mark.project
    def test_every_registered_sql_backend_has_a_row(self) -> None:
        """A backend with no row is one whose columns nobody wrote down."""
        assert {spec.label for spec in _sql_backend_specs()} == set(_RESOLVED), (
            "a SQL backend is registered that this table does not account for (or the reverse); "
            "add its row, resolved from its declaration, in the change that adds the backend"
        )

    @pytest.mark.parametrize("spec", _backend_params())
    def test_the_recorded_types_are_what_this_backend_resolves(self, spec: SqlBackendSpec) -> None:
        recorded = _RESOLVED.get(spec.label)
        assert recorded is not None, (
            f"{spec.label} has no row here; test_every_registered_sql_backend_has_a_row says why"
        )

        try:
            dialect = sa_dialect_registry.load(recorded["dialect"])()
        except sa.exc.NoSuchModuleError:
            # Only this: a dialect absent from the lane is the expected case, and skipping says so
            # out loud. Any other failure means an installed dialect is broken, which must not read
            # here as though the backend simply were not present.
            pytest.skip(f"the {recorded['dialect']} dialect is not installed in this lane")

        resolved = sql_module.inferrable_types_for(spec)
        for name, python_type in (("float", float), ("datetime", datetime)):
            assert _rendered(resolved[python_type], dialect) == recorded[name], (
                f"{spec.label} resolves `{name}` to a different type than recorded here; confirm "
                "the new rendering is a type that server has, and that it holds a declared value "
                "without narrowing it, then update this table in the same change"
            )

        assert _rendered(resolved[pd.Timestamp], dialect) == recorded["datetime"], (
            f"{spec.label} resolves a pandas timestamp to a different type than a plain datetime; "
            "the two describe the same fixture column and a backend overriding one without the "
            "other stores the same value two ways"
        )
