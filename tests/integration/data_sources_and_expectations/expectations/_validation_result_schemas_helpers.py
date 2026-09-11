"""Matrix runner helpers for validation result schema tests.

Underscore-prefixed so pytest does not collect this module.

These helpers are imported by the matrix runner and by its unit tests. They are intentionally
free of test-framework dependencies, so the same functions the runner uses are the ones the unit
tests exercise directly rather than a re-implementation of them.
"""

from __future__ import annotations

import math
from typing import TYPE_CHECKING, Any, Dict, FrozenSet, List, Mapping, Optional, TypeVar

from great_expectations.core.validation_result_schemas.field_validators import (
    classify_runtime_type,
)
from tests.integration.data_sources_and_expectations.expectations._validation_result_schemas_cases import (  # noqa: E501
    SELF_DATA_SOURCE_SENTINEL,
    SELF_TABLE_SENTINEL,
)

if TYPE_CHECKING:
    from great_expectations.datasource.fluent.interfaces import Batch
    from great_expectations.expectations.expectation import Expectation

_ExpectationT = TypeVar("_ExpectationT", bound="Expectation")

# ---------------------------------------------------------------------------
# Field-set coverage
# ---------------------------------------------------------------------------

ENGINE_NATIVE_FIELDS: FrozenSet[str] = frozenset({"unexpected_rows"})
"""Result-dict keys whose value is an engine-native object rather than data.

``unexpected_rows`` is a pandas ``DataFrame`` on the pandas engine, a Spark ``DataFrame`` on
Spark, and a list of row mappings on SQL. Two data frames holding identical data do not compare
equal -- ``==`` on either library's frame returns an elementwise frame, not a bool -- so these
keys are checked for *identity* instead: the schema must hand back the very object it was given,
which is the strongest statement available and the one that actually matters, since any
conversion at all would be the schema layer changing a value it was only asked to type.

The set is deliberately small and stated by name. Widening it by inferring "looks exotic" from
the runtime type would exempt exactly the silent coercions this check exists to catch.
"""


def _values_equal(parsed_value: Any, raw_value: Any) -> bool:
    """Whether two result-dict values are equal, tolerating NaN and non-boolean comparisons.

    ``NaN != NaN`` by IEEE rule, so a NaN that survived the schema untouched would otherwise read
    as a changed value. A comparison that refuses to reduce to a bool (a numpy array, say) falls
    back to identity, which is the only thing that can be asserted about such a value here.
    """
    if (
        isinstance(parsed_value, float)
        and isinstance(raw_value, float)
        and math.isnan(parsed_value)
        and math.isnan(raw_value)
    ):
        return True
    try:
        return bool(parsed_value == raw_value)
    except (TypeError, ValueError):
        return parsed_value is raw_value


def assert_field_set_covered(raw_result_dict: dict, parsed_model: Any) -> None:
    """Assert the parsed model reproduces every key of ``raw_result_dict`` unchanged.

    Three things are checked, in order:

    1. Every raw key is a field of the parsed model. The model may carry extra fields the raw
       dict does not (the optional fields of a wider format variant); the reverse is information
       loss.
    2. Every raw value is reproduced *equal* on the model.
    3. Every raw value is reproduced with the *same runtime type*.

    Checking key presence alone would assert nothing: these schemas set ``extra = Extra.forbid``,
    so a raw key the model does not declare has already failed parsing before this function is
    reached, and a key the model does declare is in ``.dict()`` whether or not the value survived.
    The value and type checks are what make this a real assertion -- a field typed ``float`` that
    silently widened an integer ``5`` to ``5.0``, or one typed ``str`` that stringified it, passes
    a key-set comparison and fails here.

    Keys in :data:`ENGINE_NATIVE_FIELDS` are checked by identity instead; see that constant.

    Raises:
        AssertionError: naming every offending key and what happened to it.
    """
    model_dict = parsed_model.dict()
    missing = [key for key in raw_result_dict if key not in model_dict]
    assert not missing, f"Fields in raw_result_dict not covered by parsed model: {missing}"

    altered: List[str] = []
    for key, raw_value in raw_result_dict.items():
        parsed_value = getattr(parsed_model, key)
        if key in ENGINE_NATIVE_FIELDS:
            if parsed_value is not raw_value:
                altered.append(
                    f"{key}: engine-native value was replaced rather than passed through "
                    f"({type(raw_value).__name__} in, {type(parsed_value).__name__} out)"
                )
            continue
        if type(parsed_value) is not type(raw_value):
            altered.append(
                f"{key}: type changed from {type(raw_value).__name__} to "
                f"{type(parsed_value).__name__}"
            )
        elif not _values_equal(parsed_value, raw_value):
            altered.append(f"{key}: value changed from {raw_value!r} to {parsed_value!r}")

    assert not altered, (
        "Parsed model did not reproduce the raw result dict unchanged: " + "; ".join(altered)
    )


# ---------------------------------------------------------------------------
# Findings metadata
# ---------------------------------------------------------------------------


def summarize_raw_dict(raw: dict) -> dict:
    """Extract structure (field names and types) from a result dict, never values.

    Returns a dict with keys:
    - raw_field_set: sorted list of field names
    - raw_field_types: {field_name: RuntimeTypeName.value}
    """
    return {
        "raw_field_set": sorted(raw.keys()),
        "raw_field_types": {k: classify_runtime_type(v).value for k, v in raw.items()},
    }


def summarize_raised_exception(exception_info: Any) -> Optional[str]:
    """One-line summary of the first exception recorded in ``exception_info``, or ``None``.

    ``ExpectationValidationResult.exception_info`` carries one of two shapes: a single record
    (``{"raised_exception": ..., "exception_message": ..., ...}``) or a mapping of metric
    identifier to such a record. Both are handled, because which one appears depends on how far
    validation got, and a runner that understood only one shape would read the other as "nothing
    was raised" -- turning the very cells this check exists to catch back into silent passes.
    """
    if not isinstance(exception_info, dict):
        return None
    records: List[Mapping[str, Any]] = []
    if "raised_exception" in exception_info:
        records.append(exception_info)
    else:
        records.extend(value for value in exception_info.values() if isinstance(value, dict))
    for record in records:
        if record.get("raised_exception"):
            message = record.get("exception_message")
            return str(message) if message else "exception recorded with no message"
    return None


# ---------------------------------------------------------------------------
# Self-reference resolution
# ---------------------------------------------------------------------------


def resolve_self_references(
    expectation: _ExpectationT,
    *,
    table_name: Optional[str],
    data_source_name: Optional[str],
) -> _ExpectationT:
    """Substitute the case table's self-reference sentinels with this batch's real names.

    A case that names the batch's own table or data source cannot hold the real value: both are
    generated with a random suffix when the batch is set up. It holds a sentinel instead, and this
    function replaces it in every string-valued field immediately before validation, returning a
    copy so the shared, module-level case instance is never mutated (it is reused by every other
    cell in the matrix).

    Substitution is driven by the sentinel appearing in a value, not by a hand-listed set of field
    names: a list of field names would silently stop substituting the moment a case put a sentinel
    somewhere new, leaving the literal sentinel text in a query.

    Raises:
        ValueError: when a sentinel is present but the corresponding name is unavailable, rather
            than passing the literal sentinel through to the engine as a table or source name.
    """
    replacements: Dict[str, Optional[str]] = {
        SELF_TABLE_SENTINEL: table_name,
        SELF_DATA_SOURCE_SENTINEL: data_source_name,
    }
    updates: Dict[str, str] = {}
    for field_name in expectation.__fields__:
        value = getattr(expectation, field_name, None)
        if not isinstance(value, str):
            continue
        substituted = value
        for sentinel, actual in replacements.items():
            if sentinel not in substituted:
                continue
            if not actual:
                raise ValueError(
                    f"Field {field_name!r} references {sentinel!r}, but this batch setup exposes "
                    "no such name. A case using this sentinel must be restricted to the engines "
                    "whose batch setup provides it."
                )
            substituted = substituted.replace(sentinel, actual)
        if substituted != value:
            updates[field_name] = substituted
    if not updates:
        return expectation
    return expectation.copy(update=updates)


def release_sql_connections(batch: Batch) -> None:
    """Dispose the pooled connections a SQL batch's datasource holds, once a cell is done with it.

    A SQL datasource holds two distinct SQLAlchemy engines, each with its own connection pool: the
    execution engine's own engine, built from the connection string when the execution engine is
    constructed, and the datasource's separate `get_engine()` engine. Both need disposing -- closing
    one leaves the other's pooled connections open.

    The harness builds a fresh datasource for every test that requests a batch, and the session
    context keeps each one alive until teardown, so without this, every SQL test would leave both
    engines' pooled connections idle on the server for the rest of the session. A module with a
    handful of tests never notices; at this matrix's cell count per data source, that was enough --
    on top of the sibling modules -- to exhaust the CI PostgreSQL container's connection limit for
    every test that ran after it. Disposing both engines here returns the cell's connections; the
    next cell gets its own datasource anyway, so nothing here needs to be kept alive for reuse.
    """
    datasource = batch.datasource
    if not hasattr(datasource, "get_engine"):
        return
    datasource.get_execution_engine().engine.dispose()
    datasource.get_engine().dispose()
