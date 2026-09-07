"""The SQL config base: identity derived from a declared `SqlBackendSpec`.

Every dialect-specific config used to hand-write `label` and `pytest_mark` as properties. Once a
backend's identity is captured declaratively in a `SqlBackendSpec` (see `backend_spec.py`), the
hand-written properties become redundant with the declaration and a source of drift between the
two. That redundancy is gone: the declaration a config states once lives on the shared config
base, which derives `label` and `pytest_mark` from it for every data source alike. What is left
here is what is genuinely SQL - the per-instance override seam, and the narrowing of the
declaration to the SQL sub-record for the dialect facts only a SQL consumer reads - so a concrete
backend config states its identity in exactly one place.

This module imports from `base.py`, `backend_spec.py`, and `registry.py` only. It must not import
`sql.py` or any backend module: those sit to this module's right in the dependency direction
(`base` -> `backend_spec` -> `registry` -> `sql_config` -> `sql` -> backend modules -> `tiers`),
and a module is only ever allowed to import from modules to its own left.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional, TypeVar, cast

from great_expectations.compatibility.typing_extensions import override
from tests.integration.test_utils.data_source_config.base import DataSourceTestConfig

if TYPE_CHECKING:
    from tests.integration.test_utils.data_source_config.backend_spec import SqlBackendSpec
    from tests.integration.test_utils.data_source_config.data_source_spec import DataSourceSpec


@dataclass(frozen=True, eq=False)
class SqlDatasourceTestConfig(DataSourceTestConfig):
    """Base for SQL backend configs, narrowing the declared record to its SQL sub-record.

    A concrete subclass declares its identity once, as a class variable:

        class PostgreSQLDatasourceTestConfig(SqlDatasourceTestConfig):
            DATA_SOURCE_SPEC = SqlBackendSpec(
                label="postgresql", public_name="PostgreSQL", marker="postgresql", ...
            )

    `label` and `pytest_mark` are then derived from that declaration by the shared config base
    rather than hand-written, so the declaration is the single place a backend's identity is
    stated. This class adds no identity property of its own: deriving them again here off the
    narrowed accessor would restate what the base already computes from the same record.

    One consumer, however, cannot know its declaration until construction time: the generic-SQL
    escape hatch is a single registered-or-not config class used against a caller-supplied
    connection string, and its identity (in particular its dialect-derived table schema items and
    type overrides) varies per call, not per class. A class variable alone cannot express that —
    it is shared by every instance of the class. `backend_spec_override` is the seam that closes
    that gap: an optional per-instance field that, when set, takes precedence over the class-level
    `DATA_SOURCE_SPEC`. The `data_source_spec` property below is the single place that resolves the
    two, and `backend_spec` narrows what it returns, so every other property and every consumer
    reads identity through one name regardless of which of the two supplied it.

    Adding `backend_spec_override` as a dataclass field, rather than a plain attribute, requires
    decorating this class with `@dataclass` itself. `eq=False` is deliberate: `@dataclass` only
    ever leaves `__eq__`/`__hash__` untouched when a class either defines its own or opts out of
    generation entirely. `DataSourceTestConfig.__eq__`/`__hash__` are hand-written to reduce
    `extra_column_types` to a hashable tuple before hashing it; the naive generated `__hash__`
    that `@dataclass(frozen=True)` would otherwise install here hashes that field's raw `dict`
    value and raises `TypeError` on every instance. `eq=False` keeps this class, and every
    subclass that is not itself re-decorated, on the inherited, dict-safe implementation.

    That protection does not survive re-decoration. A subclass adding a field of its own must
    decorate itself with `@dataclass`, and a bare `@dataclass(frozen=True)` regenerates both
    dunders — reintroducing the raw-`dict` hash without redeclaring anything by hand, and so
    without any obvious signal that it has done so. Any subclass that re-decorates must therefore
    pass `eq=False` as well.

    **That rule is scoped to a config adding no field whose value should be compared, and it is
    not a general one.** It holds for a SQL config because every field one adds is either
    identity the declaration already carries or a per-instance seam that must not widen equality.
    It does not hold for a config carrying option mappings the harness has to tell apart — read
    and write options, say. Opting such a config out of generated equality would leave it
    comparing only label and mark, so two instances configured differently would compare equal,
    and the session-scoped batch-setup cache keyed on config equality would hand the second one
    the first one's setup, reading its data with the first one's options. That is silently wrong
    data rather than an error, so the opt-out belongs only where the added fields carry nothing
    equality has to see.

    The existing field set inherited from `DataSourceTestConfig` (`name`, `table_name`,
    `schema_name`, `column_types`, `extra_column_types`) is untouched, so equality, hashing, and
    `test_id` — all of which are defined in terms of `label`, `pytest_mark`, and those fields —
    keep their existing meaning. `create_batch_setup` remains abstract; only the narrowing
    accessor and the per-instance override seam are added here.
    """

    backend_spec_override: Optional[SqlBackendSpec] = None
    """Per-instance declaration override. `None` (the default) means "use `DATA_SOURCE_SPEC`"; every
    concrete backend except the generic-SQL escape hatch leaves this unset. See the class
    docstring for why this seam exists and why it is per-instance rather than per-class.

    **An override must also vary the label.** This field takes no part in equality: two instances
    of one class whose overrides differ but whose labels agree compare equal. The session-scoped
    batch-setup cache is keyed on a wrapper whose own equality defers to config equality, so such
    instances share a single `BatchTestSetup` — the second one silently reusing the first one's
    tables, built from the first one's declaration. That surfaces as wrong data rather than as an
    error, so a caller varying the declaration per instance must vary the label with it.
    """

    @property
    @override
    def data_source_spec(self) -> DataSourceSpec:
        """The declaration that governs this instance: `backend_spec_override` when set, the
        class-level `DATA_SOURCE_SPEC` otherwise.

        Overriding resolution here, rather than adding the override field to the shared base, is
        what keeps the seam local to the one config that needs it: the base knows only that
        resolution is a property, not that any subclass varies it.
        """
        if self.backend_spec_override is not None:
            return self.backend_spec_override
        return self.DATA_SOURCE_SPEC

    @property
    def backend_spec(self) -> SqlBackendSpec:
        """This config's declaration, narrowed to the SQL sub-record.

        Every SQL consumer reads dialect facts - schema usage, transaction mode, table schema
        items, column-type overrides, insert parameter limit - through this one accessor, so this
        is the single place the shared declaration is narrowed.

        The narrowing is sound rather than an unchecked cast: registration rejects a SQL config
        whose declared record is not a SQL sub-record, with an error naming the class and the
        record type it declared, so no registered SQL config can reach this property with a
        record that carries no dialect facts. The class variable itself is deliberately not
        re-annotated with the narrower type on this class: a second annotation would put the
        declaration in two places, and the type checker does not reject one, so nothing but a
        deliberate check would catch it.
        """
        return cast("SqlBackendSpec", self.data_source_spec)


_SqlConfigT = TypeVar("_SqlConfigT", bound=SqlDatasourceTestConfig)  # noqa: PYI018
"""Bound to `SqlDatasourceTestConfig` rather than the wider `_ConfigT` (`base.py`). Unused within
this module by design: `sql.py`'s `SQLBatchTestSetup` imports it to re-bind its own generic
parameter, so every concrete `SQLBatchTestSetup` subclass is statically tied to a config that
carries a declared `SqlBackendSpec` - not merely any `DataSourceTestConfig` - and can therefore
read `backend_spec` off `self.config` without a cast."""
