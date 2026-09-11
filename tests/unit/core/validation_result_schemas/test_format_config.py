"""Unit tests for the ResultFormatConfig TypedDict.

Round-trips parse_result_format() output under each ResultFormat value, asserting
that the keys it always injects are present, that ``result_format`` is optional
because the dict branch can return without it, and that the dispatcher reads a
config of this shape when it has to recover a format from configuration.
"""

from __future__ import annotations

from typing import Mapping

import pytest

from great_expectations.core.result_format import ResultFormat
from great_expectations.core.validation_result_schemas.dispatcher import (
    _normalize_result_format,
)
from great_expectations.core.validation_result_schemas.format_config import (
    ResultFormatConfig,
    ResultFormatConfigRequired,
)
from great_expectations.expectations.expectation_configuration import parse_result_format

# Keys parse_result_format injects into every config it returns.
ALWAYS_INJECTED_KEYS = frozenset(
    {
        "partial_unexpected_count",
        "include_unexpected_rows",
        "map_expectation_unexpected_rows_as_dict",
    }
)
OPTIONAL_KEYS = frozenset(
    {
        "exclude_unexpected_values",
        "return_unexpected_index_query",
        "unexpected_index_column_names",
    }
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _assert_required_keys_present(config: Mapping[str, object]) -> None:
    """Assert every always-injected key is present in the config dict."""
    missing = ALWAYS_INJECTED_KEYS - config.keys()
    assert not missing, f"Missing required keys: {missing}"


def _assert_optional_keys_absent(config: Mapping[str, object]) -> None:
    """Assert optional keys are NOT present (string-only parse_result_format input)."""
    present = OPTIONAL_KEYS & config.keys()
    assert not present, f"Optional keys should be absent but found: {present}"


# ---------------------------------------------------------------------------
# Tests: string-form parse_result_format produces only required keys
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_boolean_only_required_keys_present() -> None:
    raw = parse_result_format(ResultFormat.BOOLEAN_ONLY.value)
    config: ResultFormatConfig = raw  # type: ignore[assignment]
    _assert_required_keys_present(config)
    _assert_optional_keys_absent(config)
    assert config["result_format"] == ResultFormat.BOOLEAN_ONLY.value


@pytest.mark.unit
def test_basic_required_keys_present() -> None:
    raw = parse_result_format(ResultFormat.BASIC.value)
    config: ResultFormatConfig = raw  # type: ignore[assignment]
    _assert_required_keys_present(config)
    _assert_optional_keys_absent(config)
    assert config["result_format"] == ResultFormat.BASIC.value


@pytest.mark.unit
def test_summary_required_keys_present() -> None:
    raw = parse_result_format(ResultFormat.SUMMARY.value)
    config: ResultFormatConfig = raw  # type: ignore[assignment]
    _assert_required_keys_present(config)
    _assert_optional_keys_absent(config)
    assert config["result_format"] == ResultFormat.SUMMARY.value


@pytest.mark.unit
def test_complete_required_keys_present() -> None:
    raw = parse_result_format(ResultFormat.COMPLETE.value)
    config: ResultFormatConfig = raw  # type: ignore[assignment]
    _assert_required_keys_present(config)
    _assert_optional_keys_absent(config)
    assert config["result_format"] == ResultFormat.COMPLETE.value


# ---------------------------------------------------------------------------
# Tests: dict-form parse_result_format with optional keys present
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_optional_exclude_unexpected_values_present_when_supplied() -> None:
    raw = parse_result_format(
        {
            "result_format": ResultFormat.COMPLETE.value,
            "exclude_unexpected_values": True,
        }
    )
    config: ResultFormatConfig = raw  # type: ignore[assignment]
    _assert_required_keys_present(config)
    assert "exclude_unexpected_values" in config
    assert config["exclude_unexpected_values"] is True


@pytest.mark.unit
def test_optional_return_unexpected_index_query_present_when_supplied() -> None:
    raw = parse_result_format(
        {
            "result_format": ResultFormat.COMPLETE.value,
            "return_unexpected_index_query": False,
        }
    )
    config: ResultFormatConfig = raw  # type: ignore[assignment]
    _assert_required_keys_present(config)
    assert "return_unexpected_index_query" in config
    assert config["return_unexpected_index_query"] is False


@pytest.mark.unit
def test_both_optional_keys_present_when_supplied() -> None:
    raw = parse_result_format(
        {
            "result_format": ResultFormat.SUMMARY.value,
            "exclude_unexpected_values": False,
            "return_unexpected_index_query": True,
        }
    )
    config: ResultFormatConfig = raw  # type: ignore[assignment]
    _assert_required_keys_present(config)
    assert "exclude_unexpected_values" in config
    assert "return_unexpected_index_query" in config


# ---------------------------------------------------------------------------
# Tests: result_format itself is optional
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_dict_input_without_a_format_returns_a_config_without_one() -> None:
    """parse_result_format({}) is a live call site and names no format.

    This is why result_format is an optional key: a reader who assumed
    ``config["result_format"]`` was always safe would be wrong here.
    """
    raw = parse_result_format({})
    _assert_required_keys_present(raw)
    assert "result_format" not in raw


@pytest.mark.unit
def test_result_format_is_not_a_required_key() -> None:
    assert "result_format" not in ResultFormatConfigRequired.__required_keys__
    assert "result_format" not in ResultFormatConfig.__required_keys__
    assert "result_format" in ResultFormatConfig.__optional_keys__


# ---------------------------------------------------------------------------
# Tests: the dispatcher reads a config of this shape
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.parametrize("result_format", list(ResultFormat))
def test_dispatcher_recovers_the_format_from_a_parsed_config(
    result_format: ResultFormat,
) -> None:
    config = parse_result_format(result_format.value)
    assert _normalize_result_format(config) == result_format


@pytest.mark.unit
def test_dispatcher_treats_a_config_without_a_format_as_unspecified() -> None:
    """An absent result_format means "unspecified", not "malformed"."""
    assert _normalize_result_format(parse_result_format({})) is None


# ---------------------------------------------------------------------------
# Tests: partial_unexpected_count default
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_partial_unexpected_count_defaults_to_20() -> None:
    raw = parse_result_format(ResultFormat.BASIC.value)
    assert raw["partial_unexpected_count"] == 20


@pytest.mark.unit
def test_partial_unexpected_count_preserved_when_supplied() -> None:
    raw = parse_result_format(
        {
            "result_format": ResultFormat.BASIC.value,
            "partial_unexpected_count": 5,
        }
    )
    assert raw["partial_unexpected_count"] == 5


# ---------------------------------------------------------------------------
# Tests: TypedDict structural constraints
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_result_format_config_required_is_typeddict() -> None:
    """Confirm ResultFormatConfigRequired is a TypedDict (not a runtime check, but importable)."""
    # Verify the class exists and has the expected annotations
    annotations = ResultFormatConfigRequired.__annotations__
    assert "partial_unexpected_count" in annotations
    assert "include_unexpected_rows" in annotations
    assert "map_expectation_unexpected_rows_as_dict" in annotations


@pytest.mark.unit
def test_result_format_config_extends_required() -> None:
    """Confirm ResultFormatConfig inherits required keys from ResultFormatConfigRequired."""
    # TypedDict merges required keys from bases into __required_keys__; works on 3.10+.
    assert ResultFormatConfigRequired.__required_keys__ <= ResultFormatConfig.__required_keys__
    assert ResultFormatConfig.__required_keys__ >= ALWAYS_INJECTED_KEYS


@pytest.mark.unit
def test_result_format_config_has_optional_keys_in_annotations() -> None:
    """Confirm ResultFormatConfig declares optional keys."""
    # ResultFormatConfig (total=False subclass) owns the optional fields
    own_annotations = ResultFormatConfig.__annotations__
    assert "exclude_unexpected_values" in own_annotations
    assert "return_unexpected_index_query" in own_annotations
    assert "result_format" in own_annotations
