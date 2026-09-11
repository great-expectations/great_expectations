"""Unit tests for field_validators.py.

Covers:
- classify_runtime_type for every declared RuntimeTypeName enum value
- validate_partial_unexpected_counts_fallback for both valid shapes

All tests are marked @pytest.mark.unit and run via:
    pytest tests/unit/core/validation_result_schemas/test_field_validators.py -m unit
"""

from __future__ import annotations

from typing import Any, List, Optional

import pytest

from great_expectations.compatibility import pydantic
from great_expectations.core.validation_result_schemas.field_validators import (
    classify_runtime_type,
    validate_partial_unexpected_counts_fallback,
    validate_unexpected_rows_passthrough,
)
from great_expectations.core.validation_result_schemas.types import RuntimeTypeName

# ---------------------------------------------------------------------------
# Helpers — minimal Pydantic v1 model for exercising validators
# ---------------------------------------------------------------------------


class _PartialCountsModel(pydantic.BaseModel):
    """Minimal model to exercise validate_partial_unexpected_counts_fallback."""

    partial_unexpected_counts: Optional[List[Any]] = None

    _validate_counts = pydantic.validator("partial_unexpected_counts", pre=True, allow_reuse=True)(
        validate_partial_unexpected_counts_fallback
    )


class _PassthroughModel(pydantic.BaseModel):
    """Minimal model to exercise validate_unexpected_rows_passthrough."""

    unexpected_rows: Any = None

    _validate_rows = pydantic.validator("unexpected_rows", pre=True, allow_reuse=True)(
        validate_unexpected_rows_passthrough
    )


# ---------------------------------------------------------------------------
# classify_runtime_type
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_classify_none() -> None:
    assert classify_runtime_type(None) == RuntimeTypeName.NONE


@pytest.mark.unit
def test_classify_bool() -> None:
    # bool must be checked before int since bool is a subclass of int
    assert classify_runtime_type(True) == RuntimeTypeName.BOOL
    assert classify_runtime_type(False) == RuntimeTypeName.BOOL


@pytest.mark.unit
def test_classify_int() -> None:
    assert classify_runtime_type(0) == RuntimeTypeName.INT
    assert classify_runtime_type(42) == RuntimeTypeName.INT
    assert classify_runtime_type(-1) == RuntimeTypeName.INT


@pytest.mark.unit
def test_classify_float() -> None:
    assert classify_runtime_type(3.14) == RuntimeTypeName.FLOAT
    assert classify_runtime_type(0.0) == RuntimeTypeName.FLOAT


@pytest.mark.unit
def test_classify_str() -> None:
    assert classify_runtime_type("hello") == RuntimeTypeName.STR
    assert classify_runtime_type("") == RuntimeTypeName.STR


@pytest.mark.unit
def test_classify_list() -> None:
    assert classify_runtime_type([]) == RuntimeTypeName.LIST
    assert classify_runtime_type([1, 2, 3]) == RuntimeTypeName.LIST


@pytest.mark.unit
def test_classify_dict() -> None:
    assert classify_runtime_type({}) == RuntimeTypeName.DICT
    assert classify_runtime_type({"key": "value"}) == RuntimeTypeName.DICT


@pytest.mark.unit
def test_classify_pandas_dataframe() -> None:
    """pandas DataFrame should return DATAFRAME_PANDAS without requiring pandas at import time."""
    pd = pytest.importorskip("pandas")
    df = pd.DataFrame({"a": [1, 2, 3]})
    assert classify_runtime_type(df) == RuntimeTypeName.DATAFRAME_PANDAS


@pytest.mark.unit
def test_classify_spark_dataframe_other_when_pyspark_unavailable() -> None:
    """When pyspark is unavailable, a mock object named DataFrame from pyspark should
    be classified as DATAFRAME_SPARK if it looks like pyspark, or OTHER otherwise."""

    # Without actual pyspark, we simulate the check using a mock
    # The classifier should detect pyspark via module path inspection
    class _FakeSparkDataFrame:
        pass

    # Give it a pyspark-like module path
    _FakeSparkDataFrame.__module__ = "pyspark.sql.dataframe"
    _FakeSparkDataFrame.__name__ = "DataFrame"

    fake_spark_df = _FakeSparkDataFrame()
    result = classify_runtime_type(fake_spark_df)
    assert result == RuntimeTypeName.DATAFRAME_SPARK


def _fake_instance(module: str, name: str) -> object:
    cls = type(name, (), {})
    cls.__module__ = module
    return cls()


@pytest.mark.unit
@pytest.mark.parametrize(
    ("module", "name", "expected"),
    [
        ("polars.dataframe.frame", "DataFrame", RuntimeTypeName.OTHER),
        ("pyspark.sql.types", "Row", RuntimeTypeName.OTHER),
        ("pyspark.sql.column", "Column", RuntimeTypeName.OTHER),
        ("pyspark.sql.connect.dataframe", "DataFrame", RuntimeTypeName.DATAFRAME_SPARK),
        ("pandas.core.frame", "DataFrame", RuntimeTypeName.DATAFRAME_PANDAS),
    ],
)
def test_classify_dataframe_requires_both_name_and_module(
    module: str, name: str, expected: RuntimeTypeName
) -> None:
    """A class named DataFrame from another library is not a pandas frame, and a pyspark
    object that is not a DataFrame is not a Spark frame; both facts must agree."""
    assert classify_runtime_type(_fake_instance(module, name)) == expected


@pytest.mark.unit
def test_classify_other_for_unknown_type() -> None:
    class _CustomObject:
        pass

    assert classify_runtime_type(_CustomObject()) == RuntimeTypeName.OTHER
    assert classify_runtime_type(object()) == RuntimeTypeName.OTHER


@pytest.mark.unit
def test_classify_never_raises() -> None:
    """classify_runtime_type must never raise regardless of input."""

    # Includes edge cases: class instances, iterators, generators
    class _WeirdObject:
        def __class_getitem__(cls, item: Any) -> Any:
            raise RuntimeError("should never be called")

    for value in [
        _WeirdObject(),
        (1, 2, 3),  # tuple -> OTHER
        {1, 2, 3},  # set -> OTHER
        lambda: None,  # callable -> OTHER
    ]:
        result = classify_runtime_type(value)
        assert isinstance(result, RuntimeTypeName), f"Expected RuntimeTypeName for {value!r}"


# ---------------------------------------------------------------------------
# validate_unexpected_rows_passthrough
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_passthrough_accepts_none() -> None:
    m = _PassthroughModel(unexpected_rows=None)
    assert m.unexpected_rows is None


@pytest.mark.unit
def test_passthrough_accepts_list() -> None:
    rows = [{"a": 1}, {"a": 2}]
    m = _PassthroughModel(unexpected_rows=rows)
    assert m.unexpected_rows == rows


@pytest.mark.unit
def test_passthrough_accepts_dict() -> None:
    m = _PassthroughModel(unexpected_rows={"a": 1})
    assert m.unexpected_rows == {"a": 1}


@pytest.mark.unit
def test_passthrough_returns_value_unchanged() -> None:
    sentinel = object()
    # Can't pass an arbitrary object through pydantic's JSON serialization, but
    # we can verify the validator function directly
    result = validate_unexpected_rows_passthrough(None, sentinel)
    assert result is sentinel


# ---------------------------------------------------------------------------
# validate_partial_unexpected_counts_fallback
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_partial_counts_accepts_none() -> None:
    m = _PartialCountsModel(partial_unexpected_counts=None)
    assert m.partial_unexpected_counts is None


@pytest.mark.unit
def test_partial_counts_accepts_canonical_shape() -> None:
    """Canonical shape: [{value: x, count: n}, ...]"""
    counts = [{"value": "foo", "count": 3}, {"value": "bar", "count": 1}]
    m = _PartialCountsModel(partial_unexpected_counts=counts)
    assert m.partial_unexpected_counts == counts


@pytest.mark.unit
def test_partial_counts_accepts_error_fallback_shape() -> None:
    """Error fallback shape: [{"error": "partial_exception_counts requires a hashable type"}]"""
    fallback = [{"error": "partial_exception_counts requires a hashable type"}]
    m = _PartialCountsModel(partial_unexpected_counts=fallback)
    assert m.partial_unexpected_counts == fallback


@pytest.mark.unit
def test_partial_counts_accepts_empty_list() -> None:
    m = _PartialCountsModel(partial_unexpected_counts=[])
    assert m.partial_unexpected_counts == []


@pytest.mark.unit
def test_partial_counts_returns_value_unchanged() -> None:
    counts = [{"value": "x", "count": 5}]
    result = validate_partial_unexpected_counts_fallback(None, counts)
    assert result == counts


@pytest.mark.unit
def test_partial_counts_none_returned_as_none() -> None:
    result = validate_partial_unexpected_counts_fallback(None, None)
    assert result is None
