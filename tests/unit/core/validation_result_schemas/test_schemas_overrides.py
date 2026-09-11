"""Unit tests for per-expectation schema overrides.

Covers:
- expect_column_values_to_be_of_type's map-shaped payload (pandas' object-dtype
  path) matches MapBasicResult, NOT TypeExpectationObservedValueResult.
- expect_column_values_to_be_of_type's bare observed_value payload -- which every
  engine emits outside that map path -- matches the override.
- Extra fields on the override raise pydantic.ValidationError (extra=forbid).

All tests are marked @pytest.mark.unit and run via:
    pytest tests/unit/core/validation_result_schemas/test_schemas_overrides.py -m unit
"""

from __future__ import annotations

import pytest

from great_expectations.compatibility import pydantic
from great_expectations.core.validation_result_schemas.schemas.map_result import (
    MapBasicResult,
)
from great_expectations.core.validation_result_schemas.schemas.per_expectation_overrides import (
    TypeExpectationObservedValueResult,
)

# ---------------------------------------------------------------------------
# Pandas-path: expect_column_values_to_be_of_type emits a map-shaped result
# ---------------------------------------------------------------------------

_PANDAS_RESULT = {
    "element_count": 10,
    "unexpected_count": 0,
    "unexpected_percent": 0.0,
    "partial_unexpected_list": [],
}


@pytest.mark.unit
def test_pandas_path_parses_as_map_basic_result() -> None:
    """The pandas result for expect_column_values_to_be_of_type is map-shaped.

    It must parse as MapBasicResult, confirming it belongs to the Map family.
    """
    m = MapBasicResult.parse_obj(_PANDAS_RESULT)
    assert m.element_count == 10
    assert m.unexpected_count == 0
    assert m.unexpected_percent == 0.0
    assert m.partial_unexpected_list == []


# ---------------------------------------------------------------------------
# SQL/Spark-path: expect_column_values_to_be_of_type emits {observed_value: ...}
# ---------------------------------------------------------------------------

_SQL_SPARK_RESULT = {"observed_value": "str"}


@pytest.mark.unit
def test_sql_spark_path_parses_as_override() -> None:
    """The SQL/Spark result for expect_column_values_to_be_of_type matches the override.

    SQL/Spark bypasses _format_map_output and emits only {observed_value: <type-name>}.
    """
    r = TypeExpectationObservedValueResult(**_SQL_SPARK_RESULT)
    assert r.observed_value == "str"


@pytest.mark.unit
def test_sql_spark_path_observed_value_preserved() -> None:
    """observed_value carries the type name string verbatim."""
    r = TypeExpectationObservedValueResult(observed_value="INTEGER")
    assert r.observed_value == "INTEGER"


@pytest.mark.unit
def test_sql_spark_path_observed_value_is_not_stringified() -> None:
    """Case-insensitive SQL dialects hand back the column type object itself.

    The override reports whatever it was given; a ``str`` annotation would turn a
    type object into its repr and the typed view would stop matching the EVR.
    """

    class _ColumnType:
        """Stands in for a SQLAlchemy type object, which is not a str."""

    value = _ColumnType()
    r = TypeExpectationObservedValueResult(observed_value=value)
    assert r.observed_value is value


# ---------------------------------------------------------------------------
# extra=forbid: unknown fields on the override must raise
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_override_extra_field_raises() -> None:
    """TypeExpectationObservedValueResult rejects unknown extra fields."""
    with pytest.raises(pydantic.ValidationError):
        TypeExpectationObservedValueResult.parse_obj(
            {
                "observed_value": "int",
                "unexpected_extra": "x",
            }
        )
