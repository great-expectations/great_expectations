"""Process-global registry of data source declarations.

A record (see `data_source_spec.py`, and its SQL sub-record in `backend_spec.py`) is free to
construct; nothing about building one has any side effect. Registration is the separate,
deliberate act that enrols a record into the set the harness treats as "the data sources that
exist" — the set completeness checks, derived suite membership, and CI wiring checks all walk.
That split is what lets a test build a throwaway record, or even a throwaway registered class,
without polluting the set that gates CI.

A registered entry pairs a record with an *optional* config class. Requiring a config would mean
the only data sources that can be described are the ones this repository happens to exercise,
which is exactly what makes "what data sources exist" unanswerable from code; a data source with
no harness config registers through `register_data_source`, while one with a config registers
through a class decorator - `register_data_source_config` for any config, or
`register_sql_config`, which is that decorator plus the one rule that is a property of being a
SQL config. The accessors that hand back config classes skip the entries that have none,
so a consumer parameterizing over configs is never handed a record it cannot instantiate.

Validation is split by what it is a property of. Some rules are well-formedness of one record on
its own terms and hold for every record. Others are *scaled to what the record claims*: a record
claiming no tier is saying "this data source ships, but no tier's suite proves it", which is an
honest declaration that needs no marker and no lane, while a tier claim asserts that a suite runs
somewhere and so makes the elements that locate that suite required. Applying the strict rules to
every record would make the honest declaration unexpressible; applying none of them would let a
support table advertise coverage that never runs.

**Limitation carried forward.** A record's per-case exclusion keys carry no tier attribution: the
mapping names cases, not the tier whose suite publishes them, and the ceiling below counts the
whole mapping. That is exact only while a single tier publishes case keys, which is the situation
today. As soon as a second tier publishes its own key namespace, a record could sit out two cases
in each tier and still pass a ceiling meant to bound how much of one suite it skips — so the count
must become per-tier before a second publishing tier arrives, not after.

This module imports only the two declaration modules, `data_source_spec` and `backend_spec`. It
does not import the SQL config base, `sql.py`, or any backend module — those sit to this module's
right in the dependency direction (`data_source_spec` -> `backend_spec` -> `registry` ->
`sql_config` -> `sql` -> backend modules -> `tiers`), and a module is only ever allowed to import
from modules to its own left.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    ClassVar,
    Dict,
    Final,
    Iterator,
    Mapping,
    Optional,
    Protocol,
    Tuple,
    Type,
    TypeVar,
)

from tests.integration.test_utils.data_source_config.backend_spec import SqlBackendSpec
from tests.integration.test_utils.data_source_config.data_source_spec import (
    DataSourceProvisioning,
    ExecutionEngineKind,
    MarkerScope,
    SupportTier,
)

if TYPE_CHECKING:
    from tests.integration.test_utils.data_source_config.data_source_spec import DataSourceSpec

# The ceiling on tier_case_exclusions per backend. See _validate_tier_case_exclusion_ceiling: a
# reason makes one exclusion answerable, but only a count makes the whole set answerable, so the
# count is enforced here rather than left to per-exclusion review.
_MAX_TIER_CASE_EXCLUSIONS = 2

# The criterion every config-bound record with a declared execution engine has to declare: the
# shared canonical expectation parameterization, the suite the ~46 expectation modules run.
_SHARED_PARAMETERIZATION_TIER: Final = SupportTier.CANONICAL_EXPECTATIONS

# The deliberate non-participants, each with the reason it sits out. This is an explicit literal
# rather than a derived set on purpose: participation in the shared parameterization is the
# default, and opting out is a decision someone has to write down. Three SQL backends came to be
# missing from that suite precisely because opting out took no writing anything down.
#
# Two liveness checks in the guard suite keep this from becoming a place exemptions accumulate
# after their reason expires: one fails if an entry names a label no registered record has, and one
# fails if an entry names a record that does declare the criterion.
#
# `GenericSQLDatasourceTestConfig`, the generic-SQL escape hatch, is deliberately absent from this
# literal and never needs an entry, because it is never registered — it carries no registration
# decorator at all, so no registration of it ever reaches the rule below. It is nonetheless a SQL
# config that the harness drives, and it was carried by the shared data-source lists back when
# those were hand-written, so a reader who knows only that will read its absence here as an
# oversight; it is not one.
_OUTSIDE_SHARED_PARAMETERIZATION: Final[Mapping[str, str]] = {
    "clickhouse": (
        "curated tier: its dialect behavior is proven by the curated backend suite, and the "
        "shared parameterization deliberately omits it"
    ),
    "oracle": (
        "curated tier: its dialect behavior is proven by the curated backend suite, and the "
        "shared parameterization deliberately omits it"
    ),
    "singlestore": (
        "curated tier: its dialect behavior is proven by the curated backend suite, and the "
        "shared parameterization deliberately omits it"
    ),
    "trino": (
        "curated tier: its dialect behavior is proven by the curated backend suite, and the "
        "shared parameterization deliberately omits it"
    ),
}


class _DeclaresDataSourceSpec(Protocol):
    """Structural shape a config class must have to be enrolled.

    Registration only needs one class attribute. Describing that as a `Protocol` here, rather than
    importing the concrete config base, is what lets this module stay to the left of that base in
    the dependency direction while still type-checking the decorator's argument.

    The slot is typed at *core* width, because a config class need not be a SQL one: this is the
    width the registry stores, the width every accessor hands back, and the width every read in
    this module and its consumers is written against.
    """

    DATA_SOURCE_SPEC: ClassVar[DataSourceSpec]


class _DeclaresSqlBackendSpec(_DeclaresDataSourceSpec, Protocol):
    """The same shape, narrowed to a SQL sub-record, for the SQL-specific entry point.

    This exists for a type-checker reason, not a design one, and it is worth stating plainly because
    the obvious simplification does not work. A concrete config assigns its declaration as
    `DATA_SOURCE_SPEC = SqlBackendSpec(...)`, and the checker infers *that* class's own symbol at
    the narrow type even though the shared config base declares the slot at core width. A Protocol
    **variable** member is invariant, so a class whose symbol is inferred narrow does not satisfy a
    protocol demanding the wider type — every SQL config would stop satisfying a single core-width
    protocol, at the registration site, for a reason that has nothing to do with the registration.

    Declaring the narrow protocol as a *subclass* of the core-width one resolves that without a
    cast and without re-annotating anything in any config module: a class satisfying the narrow
    protocol is nominally a subtype of the core-width one, so `register_sql_config` can accept
    what it accepts today, keep full dialect type information on what it reads, and still hand the
    class to storage typed at core width.
    """

    DATA_SOURCE_SPEC: ClassVar[SqlBackendSpec]


_C = TypeVar("_C", bound=_DeclaresSqlBackendSpec)
_ConfigT = TypeVar("_ConfigT", bound=_DeclaresDataSourceSpec)


@dataclass(frozen=True)
class RegisteredDataSource:
    """One entry in the registry: a record, and the config class that declared it if there is one.

    Holding both in a single entry is what lets the config-returning accessors be *derived* from
    the record set rather than maintained beside it. Two parallel stores would let the same data
    source be present in one and absent from the other, which is the drift this registry exists
    to remove.
    """

    spec: DataSourceSpec
    """What the data source declares about itself."""

    config_class: Optional[Type[_DeclaresDataSourceSpec]] = None
    """The config class the harness drives, or ``None`` for a declaration this repository does not
    exercise."""


_by_label: Dict[str, RegisteredDataSource] = {}
_by_marker: Dict[str, RegisteredDataSource] = {}


def _name_for(config_class: Optional[type], spec: DataSourceSpec) -> str:
    """Name a declaration in an error message.

    A config-bound declaration is named by its class, which is the name a maintainer sees at the
    decoration site. A declaration with no config has no such name, so its label and public name
    are used instead — the error still has to identify which declaration it is about. Both are
    given because either one alone can be the duplicated value in a collision message, so naming
    only that one would print two halves that read identically. Two records sharing a label *and*
    a public name still read alike; the remaining signal in that case is that the collision is a
    duplicated declaration rather than two declarations that disagree.
    """
    if config_class is not None:
        return config_class.__name__
    return f"the record labelled {spec.label!r} ({spec.public_name!r})"


def _registrant_name(registered: RegisteredDataSource) -> str:
    """Name an already-registered entry in an error message."""
    return _name_for(registered.config_class, registered.spec)


def _validate_identity(name: str, spec: DataSourceSpec) -> None:
    """Reject a record that is malformed on its own terms.

    Every rule here is a property of one record read in isolation, so every one of them holds for
    every record regardless of what it claims. Each optional field is checked only where it is
    stated: these rules reject a field that names nothing, never the absence of a field.
    """
    if not spec.label:
        raise ValueError(
            f"{name} declares an empty data source label; it must be a "
            f"non-empty string, since it appears in the parameterized test id used to select "
            f"this data source's cases"
        )
    if not spec.public_name:
        raise ValueError(
            f"{name} declares an empty public data source name; it must be a non-empty string, "
            f"since it is the name a generated document prints for this data source. There is no "
            f"fallback: deriving it from the label would invent a second spelling of a name the "
            f"shipped vocabulary already fixes"
        )
    if spec.marker is not None and not spec.marker:
        raise ValueError(
            f"{name} declares an empty data source marker; a declared marker must be a "
            f"non-empty string naming the pytest mark used to select this data source's tests. "
            f"Declaring no marker at all is how a record says that no marker selects it"
        )
    if spec.marker_scope is not None and spec.marker is None:
        raise ValueError(
            f"{name} declares a marker scope ({spec.marker_scope.value!r}) but no marker; a "
            f"scope states whether a marker names this data source alone or a class of data "
            f"sources, and there is no marker here for it to describe"
        )
    lane = spec.ci_lane
    # A lane is optional on the record, so these checks apply to a stated lane only: they reject a
    # lane that names nothing, not the absence of one. A record claiming a tier is separately
    # required to state a lane at all; see _validate_tier_claims.
    if lane is not None:
        if not lane.workflow_job:
            raise ValueError(
                f"{name} declares an empty CI lane workflow job; it must be a "
                f"non-empty string naming the workflow job that runs this data source's lane. A "
                f"lane naming no job cannot be located in the workflow file, so its wiring cannot "
                f"be checked at all"
            )
        if not lane.marker_token:
            raise ValueError(
                f"{name} declares an empty CI lane marker token; it must be a "
                f"non-empty string identifying which CI lane runs this data source's "
                f"marker-selected tests"
            )


def _validate_tier_claims(name: str, spec: DataSourceSpec) -> None:
    """Reject a tier claim that nothing attests to.

    These obligations are scaled to the claim rather than imposed on every record. Membership in
    no tier is a valid declaration meaning "this data source ships, but no tier's suite proves
    it", and such a record owes no marker, no lane and no container service. A tier claim says the
    opposite — that a suite runs somewhere — so at the moment it is made, the elements that make
    that suite locatable become required. A tier claim no lane attests to is how a support table
    starts advertising coverage that never runs.
    """
    if not spec.tiers:
        return
    claimed = ", ".join(sorted(tier.value for tier in spec.tiers))
    if spec.marker is None:
        raise ValueError(
            f"{name} claims tier membership ({claimed}) but declares no data source marker; a "
            f"tier claim asserts that a suite runs somewhere, and with no marker no suite can "
            f"select this data source's tests. Declare a marker, or claim no tier"
        )
    if spec.ci_lane is None:
        raise ValueError(
            f"{name} claims tier membership ({claimed}) but declares no CI lane; a tier claim "
            f"asserts that a suite runs somewhere, and a claim no lane attests to is how a "
            f"support table starts advertising coverage that never runs. Declare the lane that "
            f"runs it, or claim no tier"
        )
    if (
        spec.provisioning is DataSourceProvisioning.LOCAL_CONTAINER
        and spec.container_service is None
    ):
        raise ValueError(
            f"{name} claims tier membership ({claimed}) with LOCAL_CONTAINER provisioning but "
            f"names no container_service; a tier claim asserts that a suite runs somewhere, and "
            f"the suite for a locally containerized data source cannot run unless the record "
            f"names the compose service that starts it"
        )


def _validate_insert_parameter_limit(name: str, spec: SqlBackendSpec) -> None:
    if spec.insert_parameter_limit is not None and spec.insert_parameter_limit <= 0:
        raise ValueError(
            f"{name} declares a non-positive insert_parameter_limit "
            f"({spec.insert_parameter_limit!r}); it must be a positive integer, or omitted "
            f"entirely when the backend has no chunking limit"
        )


def _validate_container_provisioning(name: str, spec: DataSourceSpec) -> None:
    """Reject a container service that nothing would ever start.

    This was once a biconditional, and it is now deliberately relaxed in one direction only.
    Naming a service without local-container provisioning stays an error: nothing in the harness
    would start that service, so the declaration describes something that never runs.

    The other direction — local-container provisioning naming no service — is legal for a record
    claiming no tier, and that is the honest declaration for a data source distributed as a
    container image this repository has no compose file for: running it locally is how you would
    reach it, and this repository does not. A record claiming a tier keeps the original
    obligation, enforced in _validate_tier_claims, because there the suite has to actually run.
    """
    if (
        spec.container_service is not None
        and spec.provisioning is not DataSourceProvisioning.LOCAL_CONTAINER
    ):
        raise ValueError(
            f"{name} declares a container_service "
            f"({spec.container_service!r}) without LOCAL_CONTAINER provisioning; a container "
            f"service is only meaningful for a data source the harness starts locally"
        )


def _validate_table_schema_items(name: str, spec: SqlBackendSpec) -> None:
    # Checked for callability only, never invoked: calling it would require the backend's dialect
    # package, and registration must never assume that package is installed (this module is
    # imported in lanes that install no SQL dialect at all).
    if spec.table_schema_items is not None and not callable(spec.table_schema_items):
        raise ValueError(
            f"{name} declares table_schema_items "
            f"({spec.table_schema_items!r}) that is not callable; it must be a zero-argument "
            f"factory, or omitted entirely"
        )


def _validate_tier_case_exclusion_reasons(name: str, spec: DataSourceSpec) -> None:
    for key, reason in spec.tier_case_exclusions.items():
        if not key:
            raise ValueError(
                f"{name} declares a tier case exclusion with an empty case key; "
                f"every excluded case must be named"
            )
        if not reason or not reason.strip():
            raise ValueError(
                f"{name} declares a tier case exclusion for case {key!r} with no "
                f"reason (or a whitespace-only one); an unexplained exclusion is exactly the "
                f"silent narrowing this mechanism exists to prevent, so every exclusion must "
                f"record why it exists"
            )


def _validate_tier_case_exclusion_ceiling(name: str, spec: DataSourceSpec) -> None:
    count = len(spec.tier_case_exclusions)
    if count <= _MAX_TIER_CASE_EXCLUSIONS:
        return
    keys = sorted(spec.tier_case_exclusions)
    raise ValueError(
        f"{name} declares {count} tier case exclusions {keys!r}, exceeding the "
        f"ceiling of {_MAX_TIER_CASE_EXCLUSIONS}. The count is taken over the whole mapping "
        f"regardless of what each individual reason records — an exclusion for observed "
        f"non-determinism subtracts exactly as much coverage as one for a dialect gap. A reason "
        f"makes one exclusion answerable; only a count makes the set of exclusions answerable, "
        f"which is why this check exists on top of the per-exclusion reason requirement. The "
        f"remedy is to escalate this backend's tier participation, not to raise the ceiling"
    )


def _validate_tier_case_exclusions(name: str, spec: DataSourceSpec) -> None:
    _validate_tier_case_exclusion_reasons(name, spec)
    _validate_tier_case_exclusion_ceiling(name, spec)


def _validate_shared_parameterization_declaration(
    name: str, spec: DataSourceSpec, config_class: Optional[Type[_DeclaresDataSourceSpec]]
) -> None:
    """Reject a record that silently opts out of the shared canonical expectation parameterization.

    The rule is keyed on *having a config class and declaring an execution engine*, because those
    two together are what make a record something the shared parameterization could run. A
    declaration-only record has no config to instantiate and runs in no suite, so dragging it into
    the shared parameterization would be inventing coverage; a config that names no execution
    engine is a record the derived engine lists cannot place either, and is left to the
    well-formedness rules above rather than given a rule of its own here.

    Opting out stays possible, but only in writing: the record's label has to appear in
    `_OUTSIDE_SHARED_PARAMETERIZATION` with the reason it sits out. That is the whole point — three
    SQL backends came to be missing from the shared suite because opting out required nothing.
    """
    if config_class is None or spec.execution_engine is None:
        return
    if _SHARED_PARAMETERIZATION_TIER in spec.tiers:
        return
    if spec.label in _OUTSIDE_SHARED_PARAMETERIZATION:
        return
    raise ValueError(
        f"{name} declares a config class and execution engine "
        f"({spec.execution_engine.value!r}) but does not declare the shared canonical expectation "
        f"parameterization criterion ({_SHARED_PARAMETERIZATION_TIER.value!r}), and its label "
        f"({spec.label!r}) is not among the deliberate non-participants "
        f"({sorted(_OUTSIDE_SHARED_PARAMETERIZATION)!r}). A config the harness drives against a "
        f"named engine runs that suite unless someone decided otherwise, and that decision has to "
        f"be written down with its reason — silent omission is how three SQL backends came to be "
        f"missing from the suite. Declare the criterion, or add this label to the "
        f"non-participants with the reason it sits out"
    )


def _validate_spec(name: str, spec: DataSourceSpec) -> None:
    """Reject a malformed declaration.

    The dialect checks fire for a SQL sub-record and for nothing else. Testing the record's type
    rather than probing for attributes is what keeps the two groups legible: a future third
    sub-record adds one branch here rather than a scattering of attribute probes.
    """
    _validate_identity(name, spec)
    _validate_container_provisioning(name, spec)
    _validate_tier_claims(name, spec)
    _validate_tier_case_exclusions(name, spec)
    if isinstance(spec, SqlBackendSpec):
        _validate_insert_parameter_limit(name, spec)
        _validate_table_schema_items(name, spec)


def _dedicated_marker(spec: DataSourceSpec) -> Optional[str]:
    """The marker this record claims as its own, or ``None`` if it claims none.

    A record that declares its marker *shared* is asserting that the marker names a dependency
    class rather than this data source alone, and a dependency class can legitimately contain more
    than one data source — so a shared marker is neither checked for collision nor indexed, and
    cannot collide with anything later.

    An *undeclared* scope counts as dedicated. A marker names one data source unless a record says
    otherwise, so the relaxation is keyed on an explicit shared declaration; reading an undeclared
    scope as shared would silently drop the collision check for every record that declares no
    scope, which today is all of them.
    """
    if spec.marker is None or spec.marker_scope is MarkerScope.SHARED:
        return None
    return spec.marker


def _register(spec: DataSourceSpec, config_class: Optional[Type[_DeclaresDataSourceSpec]]) -> None:
    """Validate one declaration and enrol it, or raise naming the declaration and the value.

    Both entry points route through here, so a record registered without a config class is held to
    exactly the same rules as one registered with one, and is indexed the same way. A registration
    path that validated less would make the rules a property of how a record was registered rather
    than of the record.
    """
    name = _name_for(config_class, spec)
    _validate_spec(name, spec)
    _validate_shared_parameterization_declaration(name, spec, config_class)

    duplicate_label = _by_label.get(spec.label)
    if duplicate_label is not None:
        raise ValueError(
            f"duplicate data source label {spec.label!r} declared by "
            f"{_registrant_name(duplicate_label)} and {name}"
        )
    marker = _dedicated_marker(spec)
    if marker is not None:
        duplicate_marker = _by_marker.get(marker)
        if duplicate_marker is not None:
            raise ValueError(
                f"duplicate dedicated data source marker {marker!r} declared by "
                f"{_registrant_name(duplicate_marker)} and {name}; a record declaring its marker "
                f"dedicated asserts the marker names it and nothing else. Where a marker really "
                f"does name a class of data sources, declare it shared on both records"
            )

    registered = RegisteredDataSource(spec=spec, config_class=config_class)
    _by_label[spec.label] = registered
    if marker is not None:
        _by_marker[marker] = registered


def register_sql_config(config_class: type[_C]) -> type[_C]:
    """Enrol a SQL config class. Raises `ValueError` at decoration time on a duplicate label, a
    colliding dedicated marker, or any invariant violation in its declared record.
    """
    spec = config_class.DATA_SOURCE_SPEC
    if not isinstance(spec, SqlBackendSpec):
        # A ValueError rather than a TypeError, deliberately: this is a malformed declaration like
        # every other rejection here, not a caller passing the wrong argument to a function. One
        # exception type for every way a registration can be rejected is what lets a caller catch
        # registration failure without enumerating the ways a declaration can be wrong.
        raise ValueError(  # noqa: TRY004
            f"{config_class.__name__} declares a record of type {type(spec).__name__}, which is "
            f"not a SQL sub-record; every SQL consumer reads dialect facts off this declaration, "
            f"and a record that carries none cannot answer them. Register a data source with no "
            f"dialect facts through the plain record registration instead"
        )
    return register_data_source_config(config_class)


def register_data_source_config(config_class: type[_ConfigT]) -> type[_ConfigT]:
    """Enrol a config class and the record it declares. Raises `ValueError` at decoration time on a
    duplicate label, a colliding dedicated marker, or any invariant violation in that record.

    This is the entry point for any config the harness drives, SQL or not. `register_sql_config`
    is this function plus the one rule that is a property of being a SQL config — that the declared
    record carries dialect facts — so both paths share every other rule, and the set of rules a
    config is held to is a property of the config rather than of which decorator enrolled it.
    """
    spec = config_class.DATA_SOURCE_SPEC
    if spec.marker is None:
        raise ValueError(
            f"{config_class.__name__} declares a record with no data source marker; a config is "
            f"parameterized into a suite by its mark, so its marker has to resolve. Declare a "
            f"marker, or register the record on its own without a config class"
        )
    _register(spec, config_class)
    return config_class


def register_data_source(spec: DataSourceSpec) -> DataSourceSpec:
    """Enrol a record that has no config class, and return it.

    This is the entry point for a data source this repository declares but does not exercise. It
    returns the record so a declaration module can bind the registered object to a name in one
    statement, exactly as the class decorator returns the class it enrolled.

    Raises `ValueError` on the same terms as the class decorator, minus the two rules that are
    properties of having a config at all.
    """
    _register(spec, None)
    return spec


def iter_data_sources() -> Tuple[RegisteredDataSource, ...]:
    """Every registered entry, ordered by record label."""
    return tuple(_by_label[label] for label in sorted(_by_label))


def iter_data_source_specs() -> Tuple[DataSourceSpec, ...]:
    """Every registered record, ordered by label."""
    return tuple(registered.spec for registered in iter_data_sources())


def iter_data_source_configs() -> Tuple[Type[_DeclaresDataSourceSpec], ...]:
    """Registered config classes, ordered by spec label.

    Entries registered without a config class are skipped rather than represented by a
    placeholder: this accessor's callers instantiate what it returns.
    """
    return tuple(
        registered.config_class
        for registered in iter_data_sources()
        if registered.config_class is not None
    )


def data_source_configs_for_tier(tier: SupportTier) -> Tuple[Type[_DeclaresDataSourceSpec], ...]:
    """Registered config classes declaring membership in `tier`, ordered by spec label."""
    return tuple(
        config_class
        for config_class in iter_data_source_configs()
        if tier in config_class.DATA_SOURCE_SPEC.tiers
    )


def data_source_configs_for_engine(
    engine: ExecutionEngineKind,
) -> Tuple[Type[_DeclaresDataSourceSpec], ...]:
    """Registered config classes declaring `engine`, ordered by spec label.

    A record that names no engine is returned for no engine at all. Guessing one would state
    something the declaration deliberately withholds — a record naming a storage target rather
    than an engine is read by more than one of them.
    """
    return tuple(
        config_class
        for config_class in iter_data_source_configs()
        if config_class.DATA_SOURCE_SPEC.execution_engine is engine
    )


@contextmanager
def isolated_registry() -> Iterator[None]:
    """Snapshot the registry, clear it, yield an isolated empty registry, then restore the
    snapshot verbatim regardless of what happens inside.

    The registry is process-global module state, so a test that registers a throwaway record —
    including every duplicate-rejection test, which needs one successful registration before the
    conflicting one — must not leave that record behind for later tests or for the real registry
    consumers (the wiring drift check and the registered-set pin) to trip over. Clearing before
    yielding, rather than only restoring after, is what makes the empty registry inside the seam
    genuinely isolated rather than merely a live view onto whatever the real registry happens to
    hold at the time — which in turn is what lets a test assert whole-registry equality exactly,
    without that assertion depending on how many real records happen to be registered elsewhere.

    Both stores are covered, records and marker index alike: a seam that emptied one and left the
    other live would let a throwaway registration inside it collide with a marker declared
    outside, which is a failure that has nothing to do with the test that hit it.
    """
    saved_by_label = dict(_by_label)
    saved_by_marker = dict(_by_marker)
    _by_label.clear()
    _by_marker.clear()
    try:
        yield
    finally:
        _by_label.clear()
        _by_label.update(saved_by_label)
        _by_marker.clear()
        _by_marker.update(saved_by_marker)
