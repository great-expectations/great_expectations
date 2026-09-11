"""Reusable pydantic v1 field validators for validation result schemas.

All validators are pure functions intended to be bound to schema classes via
``pydantic.validator``.  They are defined once here and imported by every
schema family so that per-format classes stay thin.

Import rules (enforced by ruff banned-api):
- Pydantic symbols come exclusively from ``great_expectations.compatibility.pydantic``.
- ``RuntimeTypeName`` comes from ``validation_result_schemas.types``.
- No direct ``import pydantic``, no PEP 604 unions.
"""

from __future__ import annotations

from typing import Any, Optional

from great_expectations.core.validation_result_schemas.types import RuntimeTypeName

# ---------------------------------------------------------------------------
# Runtime-type classifier
# ---------------------------------------------------------------------------

# Module-level type map used by classify_runtime_type.
# bool is intentionally excluded — it must be checked before int (bool is a
# subclass of int), so it gets its own explicit branch in the function.
_RUNTIME_TYPE_MAP: dict = {
    int: RuntimeTypeName.INT,
    float: RuntimeTypeName.FLOAT,
    str: RuntimeTypeName.STR,
    list: RuntimeTypeName.LIST,
    dict: RuntimeTypeName.DICT,
}


def classify_runtime_type(value: Any) -> RuntimeTypeName:
    """Classify the runtime type of a heterogeneous field (e.g., unexpected_rows).

    Returns a stable ``RuntimeTypeName`` enum value used in findings metadata.
    Never raises — all branches end in a known enum member.

    Handles pyspark and pandas DataFrames by inspecting ``type(v).__module__``
    and ``type(v).__name__`` so that neither library needs to be imported at
    module load time.
    """
    if value is None:
        return RuntimeTypeName.NONE

    # Check bool before int — bool is a subclass of int in Python
    if isinstance(value, bool):
        return RuntimeTypeName.BOOL

    for t, name in _RUNTIME_TYPE_MAP.items():
        if isinstance(value, t):
            return name

    # DataFrame detection without importing the package
    type_name = type(value).__name__
    module = type(value).__module__
    # Both the class name and the module prefix have to agree: a DataFrame from
    # another library (polars, say) is not a pandas frame, and a pyspark Row or
    # Column is not a Spark DataFrame. Either alone would misreport the runtime
    # type the findings exist to record.
    if type_name == "DataFrame" and module.startswith("pandas"):
        return RuntimeTypeName.DATAFRAME_PANDAS
    if type_name == "DataFrame" and module.startswith("pyspark"):
        return RuntimeTypeName.DATAFRAME_SPARK

    return RuntimeTypeName.OTHER


# ---------------------------------------------------------------------------
# Field validators (pydantic v1 style — bound by callers via validator())
# ---------------------------------------------------------------------------


def validate_unexpected_rows_passthrough(cls: Any, v: Any) -> Any:
    """v1 validator for ``unexpected_rows``.

    Accepts any runtime type; the matrix runner records the actual type via
    ``classify_runtime_type`` for findings.  Does **not** raise on type mismatch
    — the schema accepts ``Any`` for this field because the runtime type differs
    across execution engines (pandas DataFrame, list[dict] on SQL, Spark frame).
    """
    return v


def validate_partial_unexpected_counts_fallback(cls: Any, v: Optional[list]) -> Optional[list]:
    """v1 validator for ``partial_unexpected_counts``.

    Accepts the two documented shapes:
    - Canonical: ``[{"value": x, "count": n}, ...]``
    - Error fallback: ``[{"error": "partial_exception_counts requires a hashable type"}]``
    - ``None``

    Both shapes are returned unchanged — the validator is a passthrough that
    exists so the schema explicitly acknowledges the fallback rather than
    inadvertently forbidding it.
    """
    return v
