"""Declarative facts a SQL backend states about itself, on top of the universal ones.

This module holds only data: the dialect facts that are true of a SQL backend and meaningless for
any other kind of data source, expressed as a sub-record of the universal one. Keeping them here
rather than on the universal record means a non-SQL declaration cannot express them and a reader
of a non-SQL declaration is never shown a field that has no meaning for it.

Its one intra-package import is the module holding the universal record and the vocabulary every
data source shares, which is itself dependency-free, so a throwaway record can be constructed here
without importing a backend and this module stays to the left of everything that consumes it in
the dependency graph.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Callable, Mapping, Optional, Sequence, Union

from tests.integration.test_utils.data_source_config.data_source_spec import DataSourceSpec

if TYPE_CHECKING:
    import sqlalchemy as sa

    from great_expectations.compatibility.sqlalchemy import TypeEngine


class TransactionMode(Enum):
    """How a backend handles transaction boundaries during setup and teardown."""

    EXPLICIT_COMMIT = "explicit_commit"
    """The default: the harness calls ``conn.commit()`` after setup and before teardown."""

    AUTOCOMMIT = "autocommit"
    """The backend has no standard transactions; the harness skips explicit commits."""


# Produces the dialect-required positional schema items for ONE table. Called once per table so
# that each table gets its own freshly constructed items; a schema item that binds to a table on
# attachment cannot be reused across tables. `sa` is imported under TYPE_CHECKING only, the
# pattern `sql.py` already uses, so this module needs no dialect and no runtime SQLAlchemy symbol
# it does not already have.
TableSchemaItemFactory = Callable[[], Sequence["sa.sql.schema.SchemaItem"]]


@dataclass(frozen=True, kw_only=True)
class SqlBackendSpec(DataSourceSpec):
    """Every dialect fact about a SQL backend that is data rather than behavior.

    Keyword-only and frozen, matching the record it extends. Keyword-only is what permits the
    required ``uses_schema`` field below to follow the base record's defaulted fields at all;
    without it, dataclass inheritance would reject the declaration outright.
    """

    uses_schema: bool
    """Whether the harness creates a per-test schema for this backend and qualifies its tables
    with it.

    Required rather than defaulted, deliberately. A backend that forgot to declare it would fall
    through to whichever value a default named: if that default were ``False``, the backend would
    silently get schema-less table creation and its tests would exercise a shape nobody chose,
    which is a wrong-data failure rather than an error. Requiring the field turns the omission
    into a construction error at the declaration, where a reader can see what is missing.
    """

    transaction_mode: TransactionMode = TransactionMode.EXPLICIT_COMMIT

    table_schema_items: Optional[TableSchemaItemFactory] = None
    """A zero-argument callable returning a sequence of positional SQLAlchemy schema items,
    or ``None`` (the default) when the backend contributes nothing.

    This is a factory, not a stored instance, for two independent reasons, plus a third that
    governs where it can be declared:

    - *Positional, not keyword.* SQLAlchemy accepts dialect-specific ``Table`` keyword arguments
      only when the dialect registers them in ``construct_arguments``; a dialect storage-engine
      construct is not one of those, so it cannot be passed as a ``Table`` keyword argument at
      all — it must arrive positionally, alongside the generated columns.
    - *A factory, not a stored instance.* Such a construct binds to the first ``Table`` it is
      attached to, so reusing one instance across tables corrupts the second table's schema.
      Each table therefore needs freshly constructed items, which only a callable invoked once
      per table can provide.
    - *Deferred construction.* Building the items requires the dialect package, and this
      declaration module is imported in lanes where that package is not installed. A
      zero-argument callable defers construction to the point where the backend has been
      selected by marker and its dialect is installed; a stored instance could not be built
      where the declaration lives.
    """

    column_type_overrides: Mapping[type, Union[type[TypeEngine], TypeEngine]] = field(
        default_factory=dict
    )
    """Python-type-to-SQLAlchemy-type overrides merged over the shared default inference
    mapping, defaulting to empty.

    This is a mapping, not a callable, because there is no per-table freshness requirement:
    the values are inert data with no construction cost, so no callable indirection is
    warranted. The consequence is that the field cannot be made lazy the way
    ``table_schema_items`` is — assigning the result of a function call still evaluates at
    declaration time, because it is the value that is stored, not the callable. A backend whose
    override values come from its dialect package therefore builds the mapping at module scope
    behind an import guard that yields an empty mapping when the package is absent. That is the
    supported shape: the declared record is populated where the dialect is installed and empty
    where it is not.
    """

    insert_parameter_limit: Optional[int] = None
