"""Dispatcher for typed validation result schemas.

Public API:
    as_typed(result_dict, *, expectation_type, result_format=None,
             configured_result_format=None, engine_hint=None) -> Result
    family_for(expectation_type: str) -> str
    infer_result_format(result_dict, *, family) -> Optional[ResultFormat]
    Result  (Union alias)
    ParseError  (exception)
    UnknownExpectationTypeError  (exception)

All are re-exported from ``validation_result_schemas/__init__.py``.

Import rules (enforced by ruff banned-api):
- Pydantic symbols come exclusively from ``great_expectations.compatibility.pydantic``.
- No PEP 604 unions (``X | Y``); use ``Optional[X]`` or ``Union[X, Y]``.
- No direct ``import pydantic``.
"""

from __future__ import annotations

from itertools import pairwise
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    FrozenSet,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Union,
    cast,
)

from great_expectations.compatibility import pydantic
from great_expectations.core.result_format import ResultFormat
from great_expectations.core.validation_result_schemas.schemas.aggregate_result import (
    AggregateBasicResult,
    AggregateBooleanOnlyResult,
    AggregateCompleteResult,
    AggregateSummaryResult,
)
from great_expectations.core.validation_result_schemas.schemas.map_result import (
    MapBasicResult,
    MapBooleanOnlyResult,
    MapCompleteResult,
    MapSummaryResult,
)
from great_expectations.core.validation_result_schemas.schemas.per_expectation_overrides import (
    TypeExpectationObservedValueResult,
)

if TYPE_CHECKING:
    from great_expectations.core.result_format import ResultFormatUnion
    from great_expectations.core.validation_result_schemas.format_config import (
        ResultFormatConfig,
    )

# ---------------------------------------------------------------------------
# Public type alias
# ---------------------------------------------------------------------------

Result = Union[
    MapBooleanOnlyResult,
    MapBasicResult,
    MapSummaryResult,
    MapCompleteResult,
    AggregateBooleanOnlyResult,
    AggregateBasicResult,
    AggregateSummaryResult,
    AggregateCompleteResult,
    TypeExpectationObservedValueResult,
]

# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class ParseError(Exception):
    """Raised when as_typed cannot match result_dict to a registered schema variant.

    Wraps pydantic.ValidationError; message names the unmatched fields and the
    candidate variant(s) that were tried.

    Attributes:
        pydantic_errors: the wrapped ``pydantic.ValidationError``'s ``.errors()`` sequence, or
            ``None`` when this ParseError was not raised from a pydantic failure (an
            unregistered expectation type, for example). Lets a caller identify what
            specifically failed -- e.g. an "extra fields not permitted" entry and the field
            it names -- without parsing the exception message text.
    """

    def __init__(
        self,
        message: str,
        *,
        pydantic_errors: Optional[Sequence[Mapping[str, Any]]] = None,
    ) -> None:
        super().__init__(message)
        self.pydantic_errors = pydantic_errors


class UnknownExpectationTypeError(LookupError):
    """Raised by :func:`family_for` when an expectation type is not registered.

    Guessing a family for an unregistered type would silently mis-type every
    result for that expectation, so the caller is told instead.  ``as_typed``
    converts this into a :class:`ParseError` that names the type.
    """


# Module-level error message templates (TRY003: avoid long messages outside exception class).
def _override_parse_error_msg(
    expectation_type: str, eff_engine: Optional[str], cls_name: str, exc: object
) -> str:
    return f"Failed to parse {expectation_type!r} with engine={eff_engine!r} as {cls_name}: {exc}"


def _family_parse_error_msg(
    expectation_type: str, fmt_value: str, cls_name: str, exc: object
) -> str:
    return f"Failed to parse {expectation_type!r} ({fmt_value}) as {cls_name}: {exc}"


def _unknown_type_msg(expectation_type: str) -> str:
    return (
        f"Cannot determine a result schema family for {expectation_type!r}: no such "
        f"expectation is registered.  Register the expectation, or pass a result dict "
        f"for a registered type."
    )


# ---------------------------------------------------------------------------
# Family names
# ---------------------------------------------------------------------------

FAMILY_MAP = "map"
FAMILY_AGGREGATE = "aggregate"

# Families are derived from the expectation class hierarchy (see family_for), so
# this cache is the only per-type state; a hand-maintained table drifts silently
# as expectations are added, renamed, or re-parented.
_FAMILY_CACHE: Dict[str, str] = {}

# ---------------------------------------------------------------------------
# _SHAPE_ONLY_OVERRIDES — per-expectation overrides keyed on payload shape
# ---------------------------------------------------------------------------

# Both expectations below bypass the map family's format-driven output on every
# engine -- pandas, SQL, and Spark alike -- whenever they take their non-map
# validation path, and instead emit a bare {"observed_value": ...} (or empty, at
# BOOLEAN_ONLY) result dict.  Only pandas' object-dtype map path produces the full
# map field set instead.  The override therefore cannot be selected by engine_hint:
# the same engine emits both shapes depending on the data, and every engine emits
# the narrow shape.  See _OVERRIDE_SHAPE below for the predicate that picks it.
_SHAPE_ONLY_OVERRIDES: Dict[str, Any] = {
    "expect_column_values_to_be_of_type": TypeExpectationObservedValueResult,
    "expect_column_values_to_be_in_type_list": TypeExpectationObservedValueResult,
}

# The result dict's key set must be a subset of this to select the override above:
# either empty ({}, the BOOLEAN_ONLY case) or exactly {"observed_value"}.  Anything
# wider -- e.g. the full map field set -- is not this narrow shape and falls
# through to family dispatch instead.
_OVERRIDE_SHAPE: FrozenSet[str] = frozenset({"observed_value"})

# ---------------------------------------------------------------------------
# Format dispatch tables
# ---------------------------------------------------------------------------

_FORMAT_MAP: Dict[str, Dict[ResultFormat, Any]] = {
    FAMILY_MAP: {
        ResultFormat.BOOLEAN_ONLY: MapBooleanOnlyResult,
        ResultFormat.BASIC: MapBasicResult,
        ResultFormat.SUMMARY: MapSummaryResult,
        ResultFormat.COMPLETE: MapCompleteResult,
    },
    FAMILY_AGGREGATE: {
        ResultFormat.BOOLEAN_ONLY: AggregateBooleanOnlyResult,
        ResultFormat.BASIC: AggregateBasicResult,
        ResultFormat.SUMMARY: AggregateSummaryResult,
        ResultFormat.COMPLETE: AggregateCompleteResult,
    },
}

# Least to most permissive.  Each variant's field set is a superset of the one
# before it (the schema classes form an inheritance chain); the format inference
# below depends on that, and ``test_format_order_is_a_widening_chain`` pins it.
_FORMAT_ORDER: Tuple[ResultFormat, ...] = (
    ResultFormat.BOOLEAN_ONLY,
    ResultFormat.BASIC,
    ResultFormat.SUMMARY,
    ResultFormat.COMPLETE,
)

# Field names accepted by each (family, format) schema, read off the classes
# themselves so the inference below cannot drift from the schemas it dispatches to.
_SCHEMA_FIELDS: Dict[str, Dict[ResultFormat, FrozenSet[str]]] = {
    family: {fmt: frozenset(cls.__fields__) for fmt, cls in by_format.items()}
    for family, by_format in _FORMAT_MAP.items()
}

# Fields that first appear at each format, most specific format first.  A result
# dict containing any of them was rendered at that format or higher, which is what
# makes a key set enough to recover the format.  Aggregate SUMMARY adds no fields
# over BASIC, so its entry is empty and never discriminates — that is the honest
# answer for a family whose BASIC and SUMMARY payloads are identical.
_DISCRIMINATING_FIELDS: Dict[str, Tuple[Tuple[ResultFormat, FrozenSet[str]], ...]] = {
    family: tuple(
        reversed(
            [
                (fmt, by_format[fmt] - by_format[previous])
                for previous, fmt in pairwise(_FORMAT_ORDER)
            ]
        )
    )
    for family, by_format in _SCHEMA_FIELDS.items()
}

# Used when the key set does not discriminate and no format was supplied.  The
# most permissive variant of a family accepts a well-formed result rendered at any
# format, so guessing it can only ever be over-wide; guessing a narrower variant
# would reject valid input outright.
_AMBIGUOUS_SHAPE_FORMAT: ResultFormat = _FORMAT_ORDER[-1]


# ---------------------------------------------------------------------------
# Public functions
# ---------------------------------------------------------------------------


def family_for(expectation_type: str) -> str:
    """Return ``'map'`` or ``'aggregate'`` for the given expectation type.

    The family is derived from the registered implementation's base classes: an
    expectation is map-family iff it extends ``ColumnMapExpectation``,
    ``ColumnPairMapExpectation``, or ``MulticolumnMapExpectation``.  Everything
    else — column aggregates, batch/table expectations — is aggregate-family.

    Results are memoized per expectation type.

    Raises:
        UnknownExpectationTypeError: the type is not in the expectation registry.
    """
    cached = _FAMILY_CACHE.get(expectation_type)
    if cached is not None:
        return cached

    # Imported inside the function on purpose: great_expectations.expectations.expectation
    # imports expectation_validation_result at module scope and
    # ExpectationValidationResult.as_typed lazy-imports this module.  Module-level
    # imports here would close that cycle at import time.
    from great_expectations.exceptions import ExpectationNotFoundError
    from great_expectations.expectations.expectation import (
        ColumnMapExpectation,
        ColumnPairMapExpectation,
        MulticolumnMapExpectation,
    )
    from great_expectations.expectations.registry import get_expectation_impl

    try:
        impl = get_expectation_impl(expectation_type)
    except ExpectationNotFoundError as exc:
        raise UnknownExpectationTypeError(_unknown_type_msg(expectation_type)) from exc

    # The map bases are themselves BatchExpectation subclasses, so they must be
    # tested first — testing for BatchExpectation would swallow every map type.
    family = (
        FAMILY_MAP
        if issubclass(
            impl, (ColumnMapExpectation, ColumnPairMapExpectation, MulticolumnMapExpectation)
        )
        else FAMILY_AGGREGATE
    )
    _FAMILY_CACHE[expectation_type] = family
    return family


def infer_result_format(result_dict: Dict[str, Any], *, family: str) -> Optional[ResultFormat]:
    """Recover the ResultFormat a result dict was rendered at from its key set.

    ``result_format`` passed to ``Batch.validate(...)`` is not persisted into the
    expectation's kwargs, so the result dict itself is the only reliable record of
    the format it was rendered at.

    Returns ``None`` when the key set does not discriminate — an aggregate result
    whose BASIC, SUMMARY, and COMPLETE payloads are all ``{"observed_value": ...}``,
    for example.  Callers decide what to do with an undetermined format.

    An empty dict is one such undetermined case, not a special one: BOOLEAN_ONLY
    renders as an empty result dict, but that is an implication in one direction
    only.  Some expectations (``expect_column_to_exist``, for one) report only a
    success verdict and render an empty dict at every format, so an empty dict does
    not, on its own, imply the result was rendered at BOOLEAN_ONLY.
    """
    keys = set(result_dict)
    for fmt, discriminating in _DISCRIMINATING_FIELDS[family]:
        if keys & discriminating:
            return fmt

    return None


def as_typed(
    result_dict: Dict[str, Any],
    *,
    expectation_type: str,
    result_format: Optional[ResultFormatUnion] = None,
    configured_result_format: Optional[ResultFormatUnion] = None,
    engine_hint: Optional[str] = None,
) -> Result:
    """Dispatch ``result_dict`` to the matching schema variant and return the parsed model.

    Resolution order:
      1. Per-expectation override table, matched by the *shape* of ``result_dict``
         (e.g. a bare ``{"observed_value": ...}`` payload for
         ``expect_column_values_to_be_of_type`` and
         ``expect_column_values_to_be_in_type_list``), regardless of
         ``engine_hint``.
      2. Family from :func:`family_for`.
      3. Format from ``result_format`` if given, else inferred from the result
         dict's key set, else ``configured_result_format``, else the most
         permissive variant of the family.
      4. ``_FORMAT_MAP[family][format]``.

    Args:
        result_dict: the ``result`` payload of an ExpectationValidationResult.
        expectation_type: registered expectation name, used to pick the family.
        result_format: the format the caller asked the engine for.  Authoritative
            when supplied: a disagreement between it and the shape of
            ``result_dict`` is a finding the caller wants to see, not something to
            paper over by re-inferring.
        configured_result_format: a weaker signal recovered from expectation
            configuration.  Consulted only when the key set does not discriminate,
            because configuration is not a record of what the engine actually
            rendered.
        engine_hint: ``'pandas'`` | ``'spark'`` | ``'sql'``, or ``None`` for
            unknown.  Never guessed from the result dict: the fields that look
            SQL-specific are not (pandas emits ``unexpected_index_query`` too, as
            a ``df.filter(...)`` expression), and a wrong guess turns on
            engine-conditional validation that then fails on correct input.

    Raises:
        ParseError: when the expectation type is unregistered, or when pydantic
            construction fails; the message names the candidate class.
    """
    # 1. Per-expectation override, selected by the shape of result_dict alone.
    # engine_hint plays no part: the expectations in the table emit this same
    # narrow shape from every engine, so keying on engine_hint would type the
    # identical dict differently depending on which engine happened to produce it.
    override_cls = _SHAPE_ONLY_OVERRIDES.get(expectation_type)
    if override_cls is not None and set(result_dict) <= _OVERRIDE_SHAPE:
        try:
            return override_cls(**result_dict)
        except pydantic.ValidationError as exc:
            raise ParseError(
                _override_parse_error_msg(
                    expectation_type, engine_hint, override_cls.__name__, exc
                ),
                pydantic_errors=exc.errors(),
            ) from exc

    # 2. Family-based dispatch.
    try:
        family = family_for(expectation_type)
    except UnknownExpectationTypeError as exc:
        raise ParseError(_unknown_type_msg(expectation_type)) from exc

    # 3. Format resolution.
    resolved_format = _resolve_result_format(
        result_dict,
        family=family,
        requested=result_format,
        configured=configured_result_format,
    )
    schema_cls = _FORMAT_MAP[family][resolved_format]

    # Pass engine_hint into the model only for map-family schemas.  Map schemas
    # declare ``engine_hint`` as a field (MapResultBase); aggregate schemas do
    # not, and use ``extra = Extra.forbid``, so injecting it there would raise
    # a ValidationError.
    data = dict(result_dict)
    if engine_hint is not None and family == FAMILY_MAP:
        data["engine_hint"] = engine_hint

    try:
        return schema_cls(**data)
    except pydantic.ValidationError as exc:
        raise ParseError(
            _family_parse_error_msg(
                expectation_type, resolved_format.value, schema_cls.__name__, exc
            ),
            pydantic_errors=exc.errors(),
        ) from exc


# ---------------------------------------------------------------------------
# Format resolution helpers
# ---------------------------------------------------------------------------


def _normalize_result_format(value: Optional[ResultFormatUnion]) -> Optional[ResultFormat]:
    """Normalise the several shapes a result_format may arrive in to a ResultFormat.

    Accepts a :class:`ResultFormat`, its string value, or a parsed result-format
    config dict.  Returns ``None`` for ``None`` and for a config dict with no
    ``result_format`` key: ``parse_result_format`` injects its other defaults into
    a dict that does not name a format (``parse_result_format({})`` is a live call
    site), so absence there means "unspecified", not "malformed".
    """
    if value is None:
        return None
    if isinstance(value, ResultFormat):
        return value
    if isinstance(value, dict):
        config = cast("ResultFormatConfig", value)
        declared = config.get("result_format")
        return ResultFormat(declared) if declared is not None else None
    return ResultFormat(value)


def _resolve_result_format(
    result_dict: Dict[str, Any],
    *,
    family: str,
    requested: Optional[ResultFormatUnion],
    configured: Optional[ResultFormatUnion],
) -> ResultFormat:
    """Pick the ResultFormat to dispatch on. See :func:`as_typed` for the ordering."""
    declared = _normalize_result_format(requested)
    if declared is not None:
        return declared

    inferred = infer_result_format(result_dict, family=family)
    if inferred is not None:
        return inferred

    from_config = _normalize_result_format(configured)
    if from_config is not None:
        return from_config

    return _AMBIGUOUS_SHAPE_FORMAT
