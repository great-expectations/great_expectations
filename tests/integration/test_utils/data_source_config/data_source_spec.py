"""Declarative facts every data source states about itself, SQL or not.

This module holds only data: the universal record a data source declares to describe itself,
plus the vocabularies that record is written in. It imports nothing from this package's other
modules - standard library and ``pytest`` only - so it sits to the left of everything that reads
it in the dependency direction, and a test can construct a throwaway record without importing a
data source module and without any dialect driver or Spark distribution installed.

The record is declared keyword-only and frozen.

*Keyword-only*, because a SQL sub-record extends this one with a required dialect field, and
dataclass inheritance would otherwise forbid a required field on the subclass from following a
defaulted field on the base. Keyword-only construction removes that ordering constraint entirely,
so the core can carry defaults and the sub-record can still require what it must. Every
construction site in the tree already passes keyword arguments, so nothing has to change to
satisfy it.

*Frozen*, because a record describes what a data source is rather than the state of a run.
Freezing it makes a record hashable and safely shareable across the session-scoped machinery that
reads it, and turns an accidental write into an error at the point it happens.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import FrozenSet, Mapping, Optional

import pytest


class DataSourceProvisioning(Enum):
    """Where a test run gets an instance of this data source."""

    LOCAL_CONTAINER = "local_container"
    """Started from a compose file for the duration of the test run."""

    LOCAL_FILE = "local_file"
    """No server at all; the data source is a local file (e.g. SQLite)."""

    EXTERNAL_CREDENTIALS = "external_credentials"
    """Hosted; reached with credentials read from the environment."""

    IN_PROCESS = "in_process"
    """No backend service of any kind; the data source runs inside the test process."""


class SupportTier(Enum):
    """Named suites a data source can participate in."""

    CANONICAL_EXPECTATIONS = "canonical_expectations"
    """The shared canonical expectation parameterization.

    Membership means this data source runs the expectation modules' shared parameterization. It
    is a statement about a suite, not about an engine: pandas, Spark and SQL data sources all
    declare it. It was named for a SQL engine before, which described the same suite while
    implying that only SQL backends could join it.
    """

    CURATED_SQL = "curated_sql"
    """The smaller curated SQL backend suite.

    Keeps saying SQL, because the suite it gates exists to prove dialect behavior and so has no
    meaning for a data source that speaks no dialect.
    """

    FLUENT_API = "fluent_api"
    """Every fluent datasource type this data source is reached through satisfies the standard
    create, update and create-or-update contract, including configuration persistence.

    This is a claim about the data source management API, not about reachability: the suite that
    earns this tier runs with connection testing neutralized. A data source can satisfy it and be
    unreachable.
    """


class MarkerScope(Enum):
    """Whether a declared marker names this data source alone or a class of them."""

    DEDICATED = "dedicated"
    """The marker names this data source and nothing else."""

    SHARED = "shared"
    """The marker names a dependency class that more than one data source belongs to."""


class ExecutionEngineKind(Enum):
    """The engine that executes a data source's tests."""

    PANDAS = "pandas"
    SPARK = "spark"
    SQL = "sql"


@dataclass(frozen=True)
class CiLaneRef:
    """Where a data source's tests run in the CI workflow."""

    workflow_job: str
    """A key under ``jobs:`` in the CI workflow file."""

    marker_token: str
    """The marker token that job selects tests on."""


@dataclass(frozen=True, kw_only=True)
class DataSourceSpec:
    """The universal record: what a data source is, and how its tests are selected.

    Constructing one has no side effect and performs no validation, so a test can build a
    throwaway without affecting the set of data sources the harness treats as registered.
    Validation belongs to registration, which is the deliberate act of joining that set.
    """

    # identity
    label: str
    """The harness identity: appears in the parameterized test id and orders the registry.
    Unique across all records."""

    public_name: str
    """The user-facing name of the data source, the one a generated document prints.

    This is deliberately *not* derived from ``label`` and is *not* unique: two records describing
    variants of one data source carry the same public name, because they describe the same thing
    to a user even though the harness exercises them separately.

    Where the shipped supported-data-source vocabulary has a member for this data source, this
    field carries that member's exact value. Inventing a second spelling here would create a
    second name vocabulary alongside the shipped one, and two vocabularies naming the same data
    source differently is precisely the drift this mechanism exists to remove.
    """

    # what it is
    provisioning: DataSourceProvisioning
    """Where a test run obtains an instance of this data source."""

    execution_engine: Optional[ExecutionEngineKind] = None
    """The engine a config drives, where one engine owns the record.

    Optional, because a record can name a storage target rather than an engine. An object store is
    read by more than one engine, so naming a single one for such a record would state something
    false; leaving it unset says "this record does not identify an engine" rather than picking one
    arbitrarily.
    """

    fluent_types: FrozenSet[str] = frozenset()
    """The fluent datasource ``type`` literals this record corresponds to, so a suite
    parameterized over the fluent type registry can map its results back onto tier declarations
    without a second hand-written mapping. The correspondence is many-to-many in both
    directions."""

    provisioning_note: Optional[str] = None
    """Free text recording what reaching a real instance actually takes, where the provisioning
    member alone does not say enough."""

    # how its tests are selected
    marker: Optional[str] = None
    """The pytest marker name selecting this data source's tests; may differ from ``label``.
    ``None`` when no marker selects this data source."""

    marker_scope: Optional[MarkerScope] = None
    """Whether ``marker`` names this data source alone or a class of data sources."""

    # what it claims
    tiers: FrozenSet[SupportTier] = frozenset()
    """The named test tiers this data source participates in, e.g.
    ``tiers=frozenset({SupportTier.CURATED_SQL})``.

    Always write a ``frozenset(...)`` literal, never a bare set literal: a bare
    ``{SupportTier.CURATED_SQL}`` is a ``set``, which mypy rejects against this ``FrozenSet``
    field, and ``tests/`` is inside mypy's ``files``, so that is a hard failure rather than a
    lint note. Membership in no tier is a valid declaration meaning "this data source ships, but
    no tier's suite proves it".
    """

    tier_case_exclusions: Mapping[str, str] = field(default_factory=dict)
    """Case key to reason string, defaulting to empty. Lets a tier member sit out one named case
    within that tier's suite, with a required reason recorded next to the declaration."""

    # wiring coordinates
    ci_lane: Optional[CiLaneRef] = None
    """Where this data source's tests run in the CI workflow."""

    dev_requirements_file: Optional[str] = None
    """Repo-relative path, e.g. ``"reqs/requirements-dev-mysql.txt"``."""

    task_runner_marker: Optional[str] = None
    """Dependency-map key; ``None`` means no entry is needed."""

    container_service: Optional[str] = None
    """Compose directory name under ``assets/docker/``."""

    @property
    def pytest_mark(self) -> pytest.MarkDecorator:
        """Resolve the declared marker name to a pytest mark decorator.

        Compares equal to the literal ``pytest.mark.<name>``.

        Raises ``ValueError`` when no marker is declared. A record with no marker has no tests to
        select, and returning a placeholder mark instead would let such a record be parameterized
        into a suite, where the placeholder would select nothing and the run would report as
        passing - advertising coverage that never executed.
        """
        if self.marker is None:
            raise ValueError(
                f"Data source {self.label!r} declares no pytest marker, so it has no mark to "
                f"resolve. Declare a marker before parameterizing this data source into a suite."
            )
        return getattr(pytest.mark, self.marker)
