# ruff: noqa: TRY003

from __future__ import annotations

import math
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, time
from pathlib import Path
from typing import TYPE_CHECKING, Any, BinaryIO, TextIO, cast

from ruamel.yaml.error import YAMLError

from great_expectations._docs_decorators import public_api
from great_expectations.contracts.errors import ODCSContractError, UnsupportedODCSRuleError
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.exceptions import ExpectationNotFoundError
from great_expectations.expectations import (
    ExpectColumnValueLengthsToBeBetween,
    ExpectColumnValuesToBeBetween,
    ExpectColumnValuesToBeInSet,
    ExpectColumnValuesToBeOfType,
    ExpectColumnValuesToBeUnique,
    ExpectColumnValuesToMatchRegex,
    ExpectColumnValuesToNotBeInSet,
    ExpectColumnValuesToNotBeNull,
    ExpectCompoundColumnsToBeUnique,
    ExpectTableColumnsToMatchSet,
    ExpectTableRowCountToBeBetween,
)
from great_expectations.expectations.registry import get_expectation_impl

if TYPE_CHECKING:
    from great_expectations.expectations.expectation import Expectation

ODCSSource = str | Path | Mapping[str, Any] | TextIO | BinaryIO

_API_VERSION_PREFIX = "v3.1"
_CONTRACT_KIND = "DataContract"
_PERCENT_MAX = 100
_BETWEEN_ARITY = 2
_LOGICAL_TYPES = frozenset(
    {
        "array",
        "boolean",
        "date",
        "integer",
        "number",
        "object",
        "string",
        "time",
        "timestamp",
    }
)
_OPERATORS = (
    "mustBe",
    "mustNotBe",
    "mustBeGreaterThan",
    "mustBeGreaterOrEqualTo",
    "mustBeLessThan",
    "mustBeLessOrEqualTo",
    "mustBeBetween",
    "mustNotBeBetween",
)
_WRONG_OPERATORS = {
    "mustBeGreaterThanOrEqualTo": "mustBeGreaterOrEqualTo",
    "mustBeLessThanOrEqualTo": "mustBeLessOrEqualTo",
}
_EMAIL_PATTERN = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
_UUID_PATTERN = (
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)
_FORMAT_PATTERNS = {"email": _EMAIL_PATTERN, "uuid": _UUID_PATTERN}
_CAMEL_BOUNDARY = re.compile(r"(?<!^)(?=[A-Z])")


def _freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return tuple(sorted((str(key), _freeze(item)) for key, item in value.items()))
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(item) for item in value)
    if isinstance(value, set):
        return tuple(sorted(_freeze(item) for item in value))
    return value


def _merge_expectation_meta(
    existing_meta: Mapping[str, Any] | None,
    duplicate_meta: Mapping[str, Any] | None,
) -> dict[str, Any]:
    merged = dict(existing_meta or {})
    duplicate = dict(duplicate_meta or {})
    old_odcs = merged.get("odcs")
    new_odcs = duplicate.get("odcs")
    if isinstance(old_odcs, Mapping) and isinstance(new_odcs, Mapping):
        odcs = dict(old_odcs)
        sources = odcs.get("sources")
        if not isinstance(sources, list):
            sources = [{key: value for key, value in odcs.items() if key != "sources"}]
        source = dict(new_odcs)
        if source not in sources:
            sources.append(source)
        odcs["sources"] = sources
        merged["odcs"] = odcs
    elif isinstance(new_odcs, Mapping):
        merged["odcs"] = dict(new_odcs)
    for key, value in duplicate.items():
        if key != "odcs":
            merged.setdefault(key, value)
    return merged


class _ExpectationBuilder:
    """Collect expectations while preventing exact structural duplicates."""

    def __init__(self) -> None:
        self.expectations: list[Expectation] = []
        self._by_marker: dict[tuple[Any, ...], Expectation] = {}

    def add(self, expectation: Expectation, *key: Any) -> None:
        marker = (expectation.expectation_type, *(_freeze(part) for part in key))
        existing = self._by_marker.get(marker)
        if existing is not None:
            existing.meta = _merge_expectation_meta(existing.meta, expectation.meta)
            return
        self._by_marker[marker] = expectation
        self.expectations.append(expectation)


@dataclass
class _ImportContext:
    contract_id: str
    schema_index: int
    property_names: set[str]
    type_map: Mapping[str, str] | None
    builder: _ExpectationBuilder
    ignored: list[dict[str, str]]


@dataclass(frozen=True)
class _LibraryRule:
    rule: Mapping[str, Any]
    path: str
    metric: str
    operator: str
    value: Any
    unit: str
    meta: dict[str, Any]


@dataclass(frozen=True)
class _PropertySpec:
    column: str
    logical_type: str | None
    options: Mapping[str, Any]
    path: str
    meta: dict[str, Any]


def _load_yaml(text: str) -> Any:
    try:
        return YAMLHandler().load(text)
    except YAMLError as error:
        raise ODCSContractError(f"Invalid YAML: {error}") from error


def _require_mapping(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ODCSContractError(f"{label} must be a mapping, got {type(value).__name__}")
    return cast("Mapping[str, Any]", value)


def _read_yaml_file(path: Path) -> Mapping[str, Any]:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as error:
        raise ODCSContractError(f"Could not read ODCS contract {path}: {error}") from error
    return _require_mapping(_load_yaml(text), f"contract {path}")


def _is_file(path: Path) -> bool:
    try:
        return path.is_file()
    except OSError:
        return False


def _load_string_source(source: str) -> Mapping[str, Any]:
    if "\n" not in source and "\r" not in source and _is_file(Path(source)):
        return _read_yaml_file(Path(source))

    parsed = _load_yaml(source)
    if isinstance(parsed, str):
        path = Path(parsed)
        if not _is_file(path):
            raise ODCSContractError(f"ODCS source {parsed!r} is not an existing file")
        return _read_yaml_file(path)
    return _require_mapping(parsed, "contract")


def _load_stream_source(source: TextIO | BinaryIO) -> Mapping[str, Any]:
    text = source.read()
    if isinstance(text, bytes):
        try:
            text = text.decode("utf-8")
        except UnicodeDecodeError as error:
            raise ODCSContractError("ODCS source stream must be UTF-8 encoded") from error
    if not isinstance(text, str):
        raise ODCSContractError("ODCS source stream must contain text")
    return _require_mapping(_load_yaml(text), "contract")


def _load_contract(source: ODCSSource) -> Mapping[str, Any]:
    if isinstance(source, Mapping):
        return source
    if isinstance(source, Path):
        return _read_yaml_file(source)
    if isinstance(source, str):
        return _load_string_source(source)
    if callable(getattr(source, "read", None)):
        return _load_stream_source(source)
    raise ODCSContractError(
        "source must be a path, a YAML string, a mapping, or a readable text/binary stream"
    )


def _validate_contract_header(contract: Mapping[str, Any]) -> None:
    legacy = sorted(key for key in ("dataset", "columns", "quality") if key in contract)
    if legacy:
        joined = ", ".join(legacy)
        raise ODCSContractError(
            f"Invalid ODCS v3 contract: root key(s) {joined} are not allowed. "
            "Define columns under schema[].properties and quality rules under "
            "schema[].quality or schema[].properties[].quality."
        )

    required = ("version", "apiVersion", "kind", "id", "status")
    missing = [field for field in required if field not in contract]
    if missing:
        raise ODCSContractError("Missing required ODCS root field(s): " + ", ".join(missing))
    for field in required:
        value = contract[field]
        if not isinstance(value, str) or not value.strip():
            raise ODCSContractError(f"ODCS root field {field!r} must be a non-empty string")

    if contract["kind"] != _CONTRACT_KIND:
        raise ODCSContractError(
            f"ODCS root field 'kind' must be {_CONTRACT_KIND!r}, got {contract['kind']!r}"
        )
    api_version = contract["apiVersion"]
    if api_version != _API_VERSION_PREFIX and not api_version.startswith(f"{_API_VERSION_PREFIX}."):
        raise ODCSContractError(
            f"Only ODCS v3.1 contracts are supported; got apiVersion {api_version!r}"
        )

    schemas = contract.get("schema")
    if not isinstance(schemas, list) or not schemas:
        raise ODCSContractError(
            "ODCS root field 'schema' must be a non-empty list of schema objects"
        )


def _collect_schemas(
    contract: Mapping[str, Any],
) -> list[tuple[int, Mapping[str, Any]]]:
    named: list[tuple[int, Mapping[str, Any]]] = []
    seen: set[str] = set()
    for index, raw_schema in enumerate(contract["schema"]):
        schema_object = _require_mapping(raw_schema, f"schema[{index}]")
        name = schema_object.get("name")
        if not isinstance(name, str) or not name:
            raise ODCSContractError(f"schema[{index}].name must be a non-empty string")
        if name in seen:
            raise ODCSContractError(f"Duplicate schema object name {name!r}")
        seen.add(name)
        named.append((index, schema_object))
    return named


def _select_schema(
    contract: Mapping[str, Any], schema_name: str | None
) -> tuple[int, Mapping[str, Any]]:
    named = _collect_schemas(contract)
    if schema_name is None:
        if len(named) != 1:
            available = ", ".join(schema_object["name"] for _, schema_object in named)
            raise ODCSContractError(
                "Contract defines multiple schema objects "
                f"({available}); pass schema= to select one"
            )
        return named[0]

    if not isinstance(schema_name, str) or not schema_name:
        raise ODCSContractError("schema must be a non-empty string")
    for index, schema_object in named:
        if schema_object["name"] == schema_name:
            return index, schema_object
    available = ", ".join(schema_object["name"] for _, schema_object in named)
    raise ODCSContractError(
        f"Schema {schema_name!r} not found; available schema objects: {available}"
    )


def _validate_quality_container(container: Mapping[str, Any], path: str) -> None:
    quality = container.get("quality")
    if quality is None:
        return
    if not isinstance(quality, list):
        raise ODCSContractError(f"{path}.quality must be a list")
    for index, rule in enumerate(quality):
        _require_mapping(rule, f"{path}.quality[{index}]")


def _validate_relationships(container: Mapping[str, Any], path: str) -> None:
    relationships = container.get("relationships")
    if relationships is None:
        return
    if not isinstance(relationships, list):
        raise ODCSContractError(f"{path}.relationships must be a list")
    for index, relationship in enumerate(relationships):
        _require_mapping(relationship, f"{path}.relationships[{index}]")


def _validate_property_name(
    prop: Mapping[str, Any],
    path: str,
    *,
    require_name: bool,
    names: set[str] | None,
) -> None:
    name = prop.get("name")
    if name is None:
        if require_name:
            raise ODCSContractError(f"{path}.name must be a non-empty string")
        return
    if not isinstance(name, str) or not name:
        raise ODCSContractError(f"{path}.name must be a non-empty string")
    if names is not None:
        if name in names:
            raise ODCSContractError(f"{path} contains duplicate property {name!r}")
        names.add(name)


def _validate_nested_properties(prop: Mapping[str, Any], path: str) -> None:
    nested_properties = prop.get("properties")
    if nested_properties is not None:
        if not isinstance(nested_properties, list):
            raise ODCSContractError(f"{path}.properties must be a list")
        nested_names: set[str] = set()
        for index, raw_property in enumerate(nested_properties):
            child = _require_mapping(raw_property, f"{path}.properties[{index}]")
            _validate_property(
                child,
                f"{path}.properties[{index}]",
                require_name=True,
                names=nested_names,
            )

    items = prop.get("items")
    if items is not None:
        item = _require_mapping(items, f"{path}.items")
        _validate_property(item, f"{path}.items", require_name=False, names=None)


def _validate_property(
    prop: Mapping[str, Any],
    path: str,
    *,
    require_name: bool,
    names: set[str] | None,
) -> None:
    _validate_property_name(prop, path, require_name=require_name, names=names)

    logical_type = prop.get("logicalType")
    if logical_type is not None and logical_type not in _LOGICAL_TYPES:
        expected = ", ".join(sorted(_LOGICAL_TYPES))
        raise ODCSContractError(
            f"{path}.logicalType must be one of {expected}; got {logical_type!r}"
        )

    options = prop.get("logicalTypeOptions")
    if options is not None and not isinstance(options, Mapping):
        raise ODCSContractError(f"{path}.logicalTypeOptions must be a mapping")

    _validate_quality_container(prop, path)
    _validate_relationships(prop, path)
    _validate_nested_properties(prop, path)


def _validate_schema(
    schema_object: Mapping[str, Any], schema_index: int
) -> list[Mapping[str, Any]]:
    path = f"schema[{schema_index}]"
    logical_type = schema_object.get("logicalType")
    if logical_type is not None and logical_type != "object":
        raise ODCSContractError(f"{path}.logicalType must be 'object', got {logical_type!r}")

    properties = schema_object.get("properties")
    if not isinstance(properties, list) or not properties:
        raise ODCSContractError(f"{path}.properties must be a non-empty list")

    _validate_relationships(schema_object, path)
    names: set[str] = set()
    for index, raw_property in enumerate(properties):
        property_path = f"{path}.properties[{index}]"
        prop = _require_mapping(raw_property, property_path)
        _validate_property(prop, property_path, require_name=True, names=names)

    _validate_quality_container(schema_object, path)
    return cast("list[Mapping[str, Any]]", properties)


def _meta(contract_id: str, path: str) -> dict[str, Any]:
    return {"odcs": {"contract_id": contract_id, "path": path}}


def _rule_meta(
    contract_id: str, path: str, rule: Mapping[str, Any], metric: str | None = None
) -> dict[str, Any]:
    odcs_meta: dict[str, Any] = {"contract_id": contract_id, "path": path}
    rule_id = rule.get("id")
    if rule_id is not None:
        odcs_meta["rule_id"] = str(rule_id)
    if metric is None:
        raw_metric = rule.get("metric", rule.get("rule"))
        if isinstance(raw_metric, str) and raw_metric:
            metric = raw_metric
    if metric is not None:
        odcs_meta["metric"] = metric
    return {"odcs": odcs_meta}


def _optional_bool(container: Mapping[str, Any], key: str, path: str) -> bool:
    value = container.get(key)
    if value is None:
        return False
    if not isinstance(value, bool):
        raise ODCSContractError(f"{path}.{key} must be a boolean")
    return value


def _optional_int(container: Mapping[str, Any], key: str, path: str) -> int | None:
    value = container.get(key)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise ODCSContractError(f"{path}.{key} must be an integer")
    return value


def _as_int(value: Any, path: str, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ODCSContractError(f"{path}: {label} must be an integer")
    if value < 0:
        raise ODCSContractError(f"{path}: {label} must be non-negative")
    return value


def _as_number(value: Any, path: str, label: str) -> float | int:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ODCSContractError(f"{path}: {label} must be a number")
    return value


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _is_zero_bound(operator: str, value: Any) -> bool:
    if operator == "mustBeBetween":
        return (
            isinstance(value, (list, tuple))
            and len(value) == _BETWEEN_ARITY
            and all(_is_number(item) and item == 0 for item in value)
        )
    return operator in ("mustBe", "mustBeLessOrEqualTo") and _is_number(value) and value == 0


def _mostly_for_max(operator: str, value: Any, path: str) -> float:
    if operator not in ("mustBeLessThan", "mustBeLessOrEqualTo"):
        raise UnsupportedODCSRuleError(
            f"{path}: operator {operator!r} is not supported for a percentage "
            "threshold; use mustBeLessThan or mustBeLessOrEqualTo, or mustBe: 0 "
            "for an exact absence check"
        )
    percent = _as_number(value, path, "percentage threshold")
    if operator == "mustBeLessThan" and percent <= 0:
        raise UnsupportedODCSRuleError(
            f"{path}: mustBeLessThan: {percent} is not a meaningful percentage "
            "threshold; use mustBe: 0 for an exact absence check"
        )
    if not 0 <= percent < _PERCENT_MAX:
        raise UnsupportedODCSRuleError(
            f"{path}: percentage threshold must be between 0 and 100 (exclusive), got {percent}"
        )
    mostly = 1 - percent / _PERCENT_MAX
    if operator == "mustBeLessThan":
        # Expectation.mostly is inclusive, so nudge the bound to keep the strict comparison.
        return math.nextafter(mostly, 1.0)
    return mostly


def _parse_operator(rule: Mapping[str, Any], path: str) -> tuple[str, Any]:
    for wrong, correct in _WRONG_OPERATORS.items():
        if wrong in rule:
            raise ODCSContractError(f"{path}: {wrong!r} is not an ODCS operator; use {correct!r}")

    present = [operator for operator in _OPERATORS if operator in rule]
    if len(present) != 1:
        found = ", ".join(present) if present else "none"
        raise ODCSContractError(
            f"{path}: a data quality rule must define exactly one operator; found {found}"
        )

    operator = present[0]
    value = rule[operator]
    if operator in ("mustBeBetween", "mustNotBeBetween"):
        if not isinstance(value, (list, tuple)) or len(value) != _BETWEEN_ARITY:
            raise ODCSContractError(f"{path}.{operator} must be a two-item list")
        value = list(value)
    return operator, value


def _parse_unit(rule: Mapping[str, Any], path: str) -> str:
    raw_unit = rule.get("unit")
    if raw_unit is None:
        return "rows"
    if not isinstance(raw_unit, str):
        raise ODCSContractError(f"{path}.unit must be a string")
    unit = raw_unit.lower()
    if unit not in ("rows", "percent"):
        raise UnsupportedODCSRuleError(
            f"{path}: unit {raw_unit!r} is not supported; use 'rows' or 'percent'"
        )
    return unit


def _validate_bound(value: Any, logical_type: str, path: str) -> None:
    if logical_type in ("integer", "number"):
        if not _is_number(value):
            raise ODCSContractError(f"{path}: numeric bounds must be numbers, got {value!r}")
    elif logical_type in ("date", "timestamp", "time"):
        if not isinstance(value, (str, date, datetime, time)):
            raise ODCSContractError(
                f"{path}: date/time bounds must be strings or date/time values, got {value!r}"
            )


def _lower_bound(minimum: Any, exclusive: Any, path: str) -> tuple[Any, bool]:
    if exclusive is None:
        return minimum, False
    if minimum is None:
        return exclusive, True
    try:
        if exclusive >= minimum:
            return exclusive, True
    except TypeError as error:
        raise ODCSContractError(
            f"{path}: minimum and exclusiveMinimum are not comparable"
        ) from error
    return minimum, False


def _upper_bound(maximum: Any, exclusive: Any, path: str) -> tuple[Any, bool]:
    if exclusive is None:
        return maximum, False
    if maximum is None:
        return exclusive, True
    try:
        if exclusive <= maximum:
            return exclusive, True
    except TypeError as error:
        raise ODCSContractError(
            f"{path}: maximum and exclusiveMaximum are not comparable"
        ) from error
    return maximum, False


def _validate_range(
    minimum: Any, maximum: Any, strict_min: bool, strict_max: bool, path: str
) -> None:
    if minimum is None or maximum is None:
        return
    try:
        if minimum > maximum:
            raise ODCSContractError(
                f"{path}: minimum {minimum!r} is greater than maximum {maximum!r}"
            )
        if minimum == maximum and (strict_min or strict_max):
            raise ODCSContractError(f"{path}: the exclusive bounds produce an empty range")
    except TypeError as error:
        raise ODCSContractError(f"{path}: minimum and maximum are not comparable") from error


def _range_bounds(
    options: Mapping[str, Any], path: str, logical_type: str
) -> tuple[Any, Any, bool, bool] | None:
    minimum = options.get("minimum")
    maximum = options.get("maximum")
    exclusive_minimum = options.get("exclusiveMinimum")
    exclusive_maximum = options.get("exclusiveMaximum")
    for value in (minimum, maximum, exclusive_minimum, exclusive_maximum):
        if value is not None:
            _validate_bound(value, logical_type, path)

    minimum, strict_min = _lower_bound(minimum, exclusive_minimum, path)
    maximum, strict_max = _upper_bound(maximum, exclusive_maximum, path)
    _validate_range(minimum, maximum, strict_min, strict_max, path)
    if minimum is None and maximum is None:
        return None
    return minimum, maximum, strict_min, strict_max


def _arguments(rule: Mapping[str, Any], path: str) -> Mapping[str, Any]:
    arguments = rule.get("arguments")
    if not isinstance(arguments, Mapping):
        raise ODCSContractError(f"{path}.arguments must be a mapping")
    return arguments


def _add_unique(
    column: str,
    meta: dict[str, Any],
    builder: _ExpectationBuilder,
    *,
    mostly: float | None = None,
) -> None:
    if mostly is None:
        expectation = ExpectColumnValuesToBeUnique(column=column, meta=meta)
    else:
        expectation = ExpectColumnValuesToBeUnique(column=column, mostly=mostly, meta=meta)
    builder.add(expectation, "unique", column, mostly)


def _add_compound(
    columns: list[str],
    meta: dict[str, Any],
    builder: _ExpectationBuilder,
    *,
    mostly: float | None = None,
) -> None:
    if mostly is None:
        expectation = ExpectCompoundColumnsToBeUnique(column_list=columns, meta=meta)
    else:
        expectation = ExpectCompoundColumnsToBeUnique(column_list=columns, mostly=mostly, meta=meta)
    builder.add(expectation, "compound", columns, mostly)


def _add_null_values(
    rule_info: _LibraryRule,
    property_name: str,
    context: _ImportContext,
) -> None:
    if _is_zero_bound(rule_info.operator, rule_info.value):
        context.builder.add(
            ExpectColumnValuesToNotBeNull(column=property_name, meta=rule_info.meta),
            "not_null",
            property_name,
            None,
        )
        return
    if rule_info.unit == "percent":
        mostly = _mostly_for_max(rule_info.operator, rule_info.value, rule_info.path)
        context.builder.add(
            ExpectColumnValuesToNotBeNull(column=property_name, mostly=mostly, meta=rule_info.meta),
            "not_null",
            property_name,
            mostly,
        )
        return
    raise UnsupportedODCSRuleError(
        f"{rule_info.path}: nullValues with unit 'rows' only supports an exact "
        "zero threshold; use a percent threshold for non-zero limits"
    )


def _add_missing_values(
    rule_info: _LibraryRule,
    property_name: str,
    context: _ImportContext,
) -> None:
    if not _is_zero_bound(rule_info.operator, rule_info.value):
        raise UnsupportedODCSRuleError(
            f"{rule_info.path}: missingValues only supports an exact zero threshold"
        )
    arguments = _arguments(rule_info.rule, rule_info.path)
    missing = arguments.get("missingValues")
    if not isinstance(missing, list) or not missing:
        raise ODCSContractError(
            f"{rule_info.path}.arguments.missingValues must be a non-empty list"
        )

    if any(item is None for item in missing):
        context.builder.add(
            ExpectColumnValuesToNotBeNull(column=property_name, meta=rule_info.meta),
            "not_null",
            property_name,
            None,
        )
    sentinels = [item for item in missing if item is not None]
    if sentinels:
        context.builder.add(
            ExpectColumnValuesToNotBeInSet(
                column=property_name, value_set=sentinels, meta=rule_info.meta
            ),
            "not_in_set",
            property_name,
            sentinels,
        )


def _invalid_target(arguments: Mapping[str, Any], path: str) -> tuple[str, Any]:
    pattern = arguments.get("pattern")
    valid_values = arguments.get("validValues")
    if (pattern is None) == (valid_values is None):
        raise ODCSContractError(
            f"{path}.arguments must define exactly one of pattern or validValues"
        )
    if pattern is not None:
        if not isinstance(pattern, str) or not pattern:
            raise ODCSContractError(f"{path}.arguments.pattern must be a string")
        return "pattern", pattern
    if not isinstance(valid_values, list) or not valid_values:
        raise ODCSContractError(f"{path}.arguments.validValues must be a non-empty list")
    return "valid_values", valid_values


def _threshold_mostly(rule_info: _LibraryRule) -> float | None:
    if _is_zero_bound(rule_info.operator, rule_info.value):
        return None
    if rule_info.unit != "percent":
        raise UnsupportedODCSRuleError(
            f"{rule_info.path}: invalidValues with unit 'rows' only supports an "
            "exact zero threshold; use a percent threshold for non-zero limits"
        )
    return _mostly_for_max(rule_info.operator, rule_info.value, rule_info.path)


def _add_invalid_values(
    rule_info: _LibraryRule,
    property_name: str,
    context: _ImportContext,
) -> None:
    arguments = _arguments(rule_info.rule, rule_info.path)
    target, target_value = _invalid_target(arguments, rule_info.path)
    mostly = _threshold_mostly(rule_info)
    expectation: Expectation

    if target == "pattern":
        if mostly is None:
            expectation = ExpectColumnValuesToMatchRegex(
                column=property_name, regex=target_value, meta=rule_info.meta
            )
        else:
            expectation = ExpectColumnValuesToMatchRegex(
                column=property_name,
                regex=target_value,
                mostly=mostly,
                meta=rule_info.meta,
            )
        context.builder.add(expectation, "regex", property_name, target_value, mostly)
        return

    if mostly is None:
        expectation = ExpectColumnValuesToBeInSet(
            column=property_name, value_set=target_value, meta=rule_info.meta
        )
    else:
        expectation = ExpectColumnValuesToBeInSet(
            column=property_name,
            value_set=target_value,
            mostly=mostly,
            meta=rule_info.meta,
        )
    context.builder.add(expectation, "in_set", property_name, target_value, mostly)


def _duplicate_columns(rule_info: _LibraryRule, context: _ImportContext) -> list[str]:
    arguments = _arguments(rule_info.rule, rule_info.path)
    columns = arguments.get("properties")
    if not isinstance(columns, list) or not columns:
        raise ODCSContractError(f"{rule_info.path}.arguments.properties must be a non-empty list")
    if any(not isinstance(column, str) or not column for column in columns):
        raise ODCSContractError(
            f"{rule_info.path}.arguments.properties must contain non-empty strings"
        )
    if len(set(columns)) != len(columns):
        raise ODCSContractError(
            f"{rule_info.path}.arguments.properties must not contain duplicates"
        )
    unknown = sorted(set(columns) - context.property_names)
    if unknown:
        raise ODCSContractError(
            f"{rule_info.path}.arguments.properties references unknown column(s): "
            + ", ".join(unknown)
        )
    return columns


def _add_duplicate_values(
    rule_info: _LibraryRule,
    level: str,
    property_name: str | None,
    context: _ImportContext,
) -> None:
    mostly: float | None = None
    if not _is_zero_bound(rule_info.operator, rule_info.value):
        if rule_info.unit != "percent":
            raise UnsupportedODCSRuleError(
                f"{rule_info.path}: duplicateValues with unit 'rows' only supports an "
                "exact zero threshold; use a percent threshold for non-zero limits"
            )
        mostly = _mostly_for_max(rule_info.operator, rule_info.value, rule_info.path)

    if level == "property":
        if property_name is None:
            raise ODCSContractError(
                f"{rule_info.path}: property-level duplicateValues has no column"
            )
        _add_unique(property_name, rule_info.meta, context.builder, mostly=mostly)
        return

    columns = _duplicate_columns(rule_info, context)
    if len(columns) == 1:
        _add_unique(columns[0], rule_info.meta, context.builder, mostly=mostly)
        return
    _add_compound(columns, rule_info.meta, context.builder, mostly=mostly)


def _row_count_bounds(
    operator: str, value: Any, path: str
) -> tuple[int | None, int | None, bool, bool]:
    if operator == "mustBe":
        count = _as_int(value, path, "rowCount mustBe")
        return count, count, False, False
    if operator == "mustBeBetween":
        low = _as_int(value[0], path, "rowCount mustBeBetween")
        high = _as_int(value[1], path, "rowCount mustBeBetween")
        if low > high:
            raise ODCSContractError(
                f"{path}: rowCount mustBeBetween lower bound exceeds upper bound"
            )
        return low, high, False, False
    if operator == "mustBeGreaterThan":
        return _as_int(value, path, "rowCount mustBeGreaterThan"), None, True, False
    if operator == "mustBeGreaterOrEqualTo":
        return _as_int(value, path, "rowCount mustBeGreaterOrEqualTo"), None, False, False
    if operator == "mustBeLessThan":
        return None, _as_int(value, path, "rowCount mustBeLessThan"), False, True
    if operator == "mustBeLessOrEqualTo":
        return None, _as_int(value, path, "rowCount mustBeLessOrEqualTo"), False, False
    raise UnsupportedODCSRuleError(f"{path}: operator {operator!r} is not supported for rowCount")


def _add_row_count(rule_info: _LibraryRule, context: _ImportContext) -> None:
    if rule_info.unit != "rows":
        raise UnsupportedODCSRuleError(f"{rule_info.path}: rowCount only supports unit 'rows'")
    minimum, maximum, strict_min, strict_max = _row_count_bounds(
        rule_info.operator, rule_info.value, rule_info.path
    )
    context.builder.add(
        ExpectTableRowCountToBeBetween(
            min_value=minimum,
            max_value=maximum,
            strict_min=strict_min,
            strict_max=strict_max,
            meta=rule_info.meta,
        ),
        "row_count",
        minimum,
        maximum,
        strict_min,
        strict_max,
    )


def _custom_engine(rule: Mapping[str, Any], path: str) -> str | None:
    engine = rule.get("engine")
    if not isinstance(engine, str) or not engine:
        raise ODCSContractError(f"{path}: custom rules require a non-empty 'engine'")
    normalized = engine.lower().replace("-", "").replace("_", "").replace(" ", "")
    if normalized not in ("greatexpectations", "gx"):
        return None
    return engine


def _custom_implementation(rule: Mapping[str, Any], path: str) -> Mapping[str, Any]:
    implementation = rule.get("implementation")
    if isinstance(implementation, str):
        parsed = _load_yaml(implementation)
        if isinstance(parsed, str):
            parsed = {"type": parsed}
        implementation = parsed
    if not isinstance(implementation, Mapping):
        raise ODCSContractError(
            f"{path}.implementation must be a mapping, a YAML mapping string, "
            "or an Expectation type name"
        )
    return implementation


def _custom_type_and_kwargs(
    implementation: Mapping[str, Any], path: str
) -> tuple[str, Mapping[str, Any]]:
    type_name = implementation.get("type")
    if not isinstance(type_name, str) or not type_name:
        raise ODCSContractError(f"{path}.implementation.type must be a non-empty string")
    kwargs = implementation.get("kwargs", {})
    if not isinstance(kwargs, Mapping):
        raise ODCSContractError(f"{path}.implementation.kwargs must be a mapping")
    return type_name, kwargs


def _custom_expectation_class(type_name: str, path: str) -> type[Expectation]:
    try:
        return get_expectation_impl(type_name)
    except ExpectationNotFoundError as error:
        raise ODCSContractError(
            f"{path}: custom expectation {type_name!r} is not registered"
        ) from error


def _normalize_custom_kwargs(
    expectation_class: type[Expectation],
    kwargs: Mapping[str, Any],
    path: str,
) -> dict[str, Any]:
    fields = set(expectation_class.__fields__)
    normalized: dict[str, Any] = {}
    for key, value in kwargs.items():
        if not isinstance(key, str):
            raise ODCSContractError(f"{path}.implementation.kwargs keys must be strings")
        target = key if key in fields else _CAMEL_BOUNDARY.sub("_", key).lower()
        normalized[target if target in fields else key] = value
    return normalized


def _merge_custom_meta(
    kwargs: dict[str, Any],
    rule: Mapping[str, Any],
    path: str,
    contract_id: str,
) -> None:
    user_meta = kwargs.get("meta")
    if user_meta is not None and not isinstance(user_meta, Mapping):
        raise ODCSContractError(f"{path}.implementation.kwargs.meta must be a mapping")
    merged_meta = dict(user_meta or {})
    merged_meta.update(_rule_meta(contract_id, path, rule))
    kwargs["meta"] = merged_meta


def _construct_custom_expectation(
    expectation_class: type[Expectation],
    kwargs: dict[str, Any],
    type_name: str,
    path: str,
) -> Expectation:
    try:
        return expectation_class(**kwargs)
    except (TypeError, ValueError) as error:
        raise ODCSContractError(
            f"{path}: could not build custom expectation {type_name!r}: {error}"
        ) from error


def _add_custom_rule(
    rule: Mapping[str, Any],
    path: str,
    context: _ImportContext,
    property_name: str | None = None,
) -> None:
    engine = _custom_engine(rule, path)
    if engine is None:
        context.ignored.append(
            {
                "path": path,
                "reason": f"custom rule targets engine {rule.get('engine')!r}",
            }
        )
        return
    implementation = _custom_implementation(rule, path)
    type_name, kwargs = _custom_type_and_kwargs(implementation, path)
    expectation_class = _custom_expectation_class(type_name, path)
    normalized_kwargs = _normalize_custom_kwargs(expectation_class, kwargs, path)
    if (
        property_name is not None
        and "column" in expectation_class.__fields__
        and "column" not in normalized_kwargs
    ):
        normalized_kwargs["column"] = property_name
    _merge_custom_meta(normalized_kwargs, rule, path, context.contract_id)
    expectation = _construct_custom_expectation(
        expectation_class, normalized_kwargs, type_name, path
    )
    context.builder.add(expectation, "custom", path)


def _dispatch_property_metric(
    rule_info: _LibraryRule,
    property_name: str,
    context: _ImportContext,
) -> None:
    if rule_info.metric == "nullValues":
        _add_null_values(rule_info, property_name, context)
        return
    if rule_info.metric == "missingValues":
        _add_missing_values(rule_info, property_name, context)
        return
    _add_invalid_values(rule_info, property_name, context)


def _dispatch_library_rule(
    rule_info: _LibraryRule,
    level: str,
    property_name: str | None,
    context: _ImportContext,
) -> None:
    if rule_info.metric == "rowCount":
        if level != "schema":
            raise ODCSContractError(f"{rule_info.path}: rowCount is only valid at the schema level")
        _add_row_count(rule_info, context)
        return
    if rule_info.metric == "duplicateValues":
        _add_duplicate_values(rule_info, level, property_name, context)
        return
    if rule_info.metric in ("nullValues", "missingValues", "invalidValues"):
        if level != "property" or property_name is None:
            raise ODCSContractError(
                f"{rule_info.path}: {rule_info.metric} is only valid at the property level"
            )
        _dispatch_property_metric(rule_info, property_name, context)
        return
    raise ODCSContractError(
        f"{rule_info.path}: unknown library metric {rule_info.metric!r}; expected "
        "one of nullValues, missingValues, invalidValues, duplicateValues, rowCount"
    )


def _add_quality_rule(
    rule: Mapping[str, Any],
    level: str,
    path: str,
    property_name: str | None,
    context: _ImportContext,
) -> None:
    raw_type = rule.get("type", "library")
    if not isinstance(raw_type, str):
        raise ODCSContractError(f"{path}.type must be a string")
    rule_type = raw_type.lower()

    if rule_type == "text":
        context.ignored.append(
            {"path": path, "reason": "text rules are descriptive and not executable"}
        )
        return
    if rule_type == "sql":
        raise UnsupportedODCSRuleError(
            f"{path}: SQL data quality rules cannot be imported into an ExpectationSuite"
        )
    if rule_type == "custom":
        _add_custom_rule(rule, path, context, property_name)
        return
    if rule_type != "library":
        raise ODCSContractError(f"{path}: unknown quality rule type {raw_type!r}")

    metric = rule.get("metric")
    if not isinstance(metric, str) or not metric:
        metric = rule.get("rule")
    if not isinstance(metric, str) or not metric:
        raise ODCSContractError(f"{path}: library rules require a 'metric'")
    operator, value = _parse_operator(rule, path)
    unit = _parse_unit(rule, path)
    rule_info = _LibraryRule(
        rule=rule,
        path=path,
        metric=metric,
        operator=operator,
        value=value,
        unit=unit,
        meta=_rule_meta(context.contract_id, path, rule, metric),
    )
    _dispatch_library_rule(rule_info, level, property_name, context)


def _add_schema_quality(schema_object: Mapping[str, Any], context: _ImportContext) -> None:
    for rule_index, rule in enumerate(schema_object.get("quality") or []):
        path = f"schema[{context.schema_index}].quality[{rule_index}]"
        _add_quality_rule(
            cast("Mapping[str, Any]", rule),
            "schema",
            path,
            None,
            context,
        )


def _add_property_rules(properties: list[Mapping[str, Any]], context: _ImportContext) -> None:
    for property_index, prop in enumerate(properties):
        property_path = f"schema[{context.schema_index}].properties[{property_index}]"
        for rule_index, rule in enumerate(prop.get("quality") or []):
            path = f"{property_path}.quality[{rule_index}]"
            _add_quality_rule(
                cast("Mapping[str, Any]", rule),
                "property",
                path,
                cast("str", prop["name"]),
                context,
            )
        _add_property_structure(prop, property_path, context)


def _reject_non_string_constraints(
    spec: _PropertySpec,
    ignored: list[dict[str, str]],
) -> None:
    if any(key in spec.options for key in ("minLength", "maxLength", "pattern")):
        raise ODCSContractError(
            f"{spec.path}: string constraints are only supported for logicalType 'string'"
        )
    format_name = spec.options.get("format")
    if format_name is not None:
        ignored.append(
            {
                "path": spec.path,
                "reason": (
                    f"format {format_name!r} is not mapped for logicalType {spec.logical_type!r}"
                ),
            }
        )


def _add_pattern_constraint(spec: _PropertySpec, builder: _ExpectationBuilder) -> None:
    pattern = spec.options.get("pattern")
    if pattern is None:
        return
    if not isinstance(pattern, str) or not pattern:
        raise ODCSContractError(f"{spec.path}.logicalTypeOptions.pattern must be a string")
    builder.add(
        ExpectColumnValuesToMatchRegex(column=spec.column, regex=pattern, meta=spec.meta),
        "regex",
        spec.column,
        pattern,
        None,
    )


def _add_format_constraint(
    spec: _PropertySpec,
    builder: _ExpectationBuilder,
    ignored: list[dict[str, str]],
) -> None:
    format_name = spec.options.get("format")
    if format_name is None:
        return
    if not isinstance(format_name, str):
        raise ODCSContractError(f"{spec.path}.logicalTypeOptions.format must be a string")
    regex = _FORMAT_PATTERNS.get(format_name.lower())
    if regex is None:
        ignored.append({"path": spec.path, "reason": f"format {format_name!r} is not mapped"})
        return
    builder.add(
        ExpectColumnValuesToMatchRegex(column=spec.column, regex=regex, meta=spec.meta),
        "regex",
        spec.column,
        regex,
        None,
    )


def _add_length_constraint(spec: _PropertySpec, builder: _ExpectationBuilder) -> None:
    min_length = _optional_int(spec.options, "minLength", spec.path)
    max_length = _optional_int(spec.options, "maxLength", spec.path)
    if min_length is not None and min_length < 0:
        raise ODCSContractError(f"{spec.path}.logicalTypeOptions.minLength must be non-negative")
    if max_length is not None and max_length < 0:
        raise ODCSContractError(f"{spec.path}.logicalTypeOptions.maxLength must be non-negative")
    if min_length is not None and max_length is not None and min_length > max_length:
        raise ODCSContractError(
            f"{spec.path}: minLength {min_length} is greater than maxLength {max_length}"
        )
    if min_length is not None or max_length is not None:
        builder.add(
            ExpectColumnValueLengthsToBeBetween(
                column=spec.column,
                min_value=min_length,
                max_value=max_length,
                meta=spec.meta,
            ),
            "lengths",
            spec.column,
            min_length,
            max_length,
        )


def _add_string_constraints(spec: _PropertySpec, context: _ImportContext) -> None:
    if spec.logical_type is None:
        return
    if spec.logical_type != "string":
        _reject_non_string_constraints(spec, context.ignored)
        return
    _add_pattern_constraint(spec, context.builder)
    _add_format_constraint(spec, context.builder, context.ignored)
    _add_length_constraint(spec, context.builder)


def _add_numeric_constraints(spec: _PropertySpec, context: _ImportContext) -> None:
    range_keys = ("minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum")
    if spec.logical_type is None:
        return
    if spec.logical_type not in ("integer", "number", "date", "timestamp", "time"):
        if any(key in spec.options for key in range_keys):
            raise ODCSContractError(
                f"{spec.path}: range constraints are not supported for logicalType "
                f"{spec.logical_type!r}"
            )
        return

    bounds = _range_bounds(spec.options, spec.path, spec.logical_type)
    if bounds is not None:
        minimum, maximum, strict_min, strict_max = bounds
        context.builder.add(
            ExpectColumnValuesToBeBetween(
                column=spec.column,
                min_value=minimum,
                max_value=maximum,
                strict_min=strict_min,
                strict_max=strict_max,
                meta=spec.meta,
            ),
            "between",
            spec.column,
            minimum,
            maximum,
            strict_min,
            strict_max,
        )

    if spec.options.get("multipleOf") is not None:
        context.ignored.append(
            {"path": spec.path, "reason": "multipleOf constraints are not mapped"}
        )


def _add_type_constraint(
    prop: Mapping[str, Any],
    spec: _PropertySpec,
    context: _ImportContext,
) -> None:
    backend_type = prop.get("physicalType")
    if backend_type is not None and (not isinstance(backend_type, str) or not backend_type):
        raise ODCSContractError(f"{spec.path}.physicalType must be a non-empty string")
    if (
        context.type_map is not None
        and spec.logical_type is not None
        and spec.logical_type in context.type_map
    ):
        backend_type = context.type_map[spec.logical_type]
    if backend_type is None:
        return
    context.builder.add(
        ExpectColumnValuesToBeOfType(column=spec.column, type_=backend_type, meta=spec.meta),
        "type",
        spec.column,
        backend_type,
    )


def _record_nested_ignored(
    container: Mapping[str, Any],
    path: str,
    ignored: list[dict[str, str]],
) -> None:
    ignored.append({"path": path, "reason": "nested property is not mapped"})
    quality = container.get("quality")
    if isinstance(quality, list):
        for index, _ in enumerate(quality):
            ignored.append(
                {
                    "path": f"{path}.quality[{index}]",
                    "reason": "nested quality rules are not mapped",
                }
            )
    if container.get("relationships") is not None:
        ignored.append({"path": path, "reason": "property relationships are not mapped"})

    nested_properties = container.get("properties")
    if isinstance(nested_properties, list):
        for index, child in enumerate(nested_properties):
            if isinstance(child, Mapping):
                _record_nested_ignored(child, f"{path}.properties[{index}]", ignored)
    items = container.get("items")
    if isinstance(items, Mapping):
        _record_nested_ignored(items, f"{path}.items", ignored)


def _ignored_options_reason(logical_type: str, options: Mapping[str, Any]) -> str | None:
    if logical_type == "object" and any(
        options.get(key) is not None for key in ("maxProperties", "minProperties", "required")
    ):
        return "object constraints are not mapped"
    if logical_type == "array" and any(
        options.get(key) is not None for key in ("minItems", "maxItems", "uniqueItems")
    ):
        return "array constraints are not mapped"
    if logical_type in ("timestamp", "time") and any(
        options.get(key) is not None for key in ("timezone", "defaultTimezone")
    ):
        return "timezone constraints are not mapped"
    return None


def _record_ignored_options(spec: _PropertySpec, ignored: list[dict[str, str]]) -> None:
    if spec.logical_type is None:
        if spec.options:
            ignored.append(
                {
                    "path": spec.path,
                    "reason": "logicalTypeOptions are not mapped without logicalType",
                }
            )
        return
    reason = _ignored_options_reason(spec.logical_type, spec.options)
    if reason is not None:
        ignored.append({"path": spec.path, "reason": reason})


def _record_nested_structure(
    prop: Mapping[str, Any],
    path: str,
    ignored: list[dict[str, str]],
) -> None:
    if prop.get("relationships") is not None:
        ignored.append({"path": path, "reason": "property relationships are not mapped"})

    nested_properties = prop.get("properties")
    if nested_properties is not None:
        ignored.append({"path": path, "reason": "nested object properties are not mapped"})
        if isinstance(nested_properties, list):
            for index, child in enumerate(nested_properties):
                if isinstance(child, Mapping):
                    _record_nested_ignored(child, f"{path}.properties[{index}]", ignored)

    items = prop.get("items")
    if items is not None:
        ignored.append({"path": path, "reason": "array items are not mapped"})
        if isinstance(items, Mapping):
            _record_nested_ignored(items, f"{path}.items", ignored)


def _record_ignored_structure(
    prop: Mapping[str, Any],
    spec: _PropertySpec,
    ignored: list[dict[str, str]],
) -> None:
    _record_nested_structure(prop, spec.path, ignored)
    _record_ignored_options(spec, ignored)


def _add_property_structure(
    prop: Mapping[str, Any],
    path: str,
    context: _ImportContext,
) -> None:
    raw_options = prop.get("logicalTypeOptions")
    options = cast("Mapping[str, Any]", raw_options) if isinstance(raw_options, Mapping) else {}
    raw_logical_type = prop.get("logicalType")
    spec = _PropertySpec(
        column=cast("str", prop["name"]),
        logical_type=cast("str | None", raw_logical_type),
        options=options,
        path=path,
        meta=_meta(context.contract_id, path),
    )

    if _optional_bool(prop, "required", path) or _optional_bool(prop, "primaryKey", path):
        context.builder.add(
            ExpectColumnValuesToNotBeNull(column=spec.column, meta=spec.meta),
            "not_null",
            spec.column,
            None,
        )
    if _optional_bool(prop, "unique", path):
        _add_unique(spec.column, spec.meta, context.builder)

    _add_string_constraints(spec, context)
    _add_numeric_constraints(spec, context)
    _add_type_constraint(prop, spec, context)
    _record_ignored_structure(prop, spec, context.ignored)


def _primary_key_columns(
    properties: list[Mapping[str, Any]], schema_index: int
) -> list[tuple[int, int, str]]:
    primary_keys: list[tuple[int | None, int, str]] = []
    for index, prop in enumerate(properties):
        path = f"schema[{schema_index}].properties[{index}]"
        if not _optional_bool(prop, "primaryKey", path):
            continue
        position = prop.get("primaryKeyPosition")
        if position is None or position == -1:
            position = None
        elif isinstance(position, bool) or not isinstance(position, int) or position < 1:
            raise ODCSContractError(f"{path}.primaryKeyPosition must be a positive integer or -1")
        primary_keys.append((position, index, cast("str", prop["name"])))

    explicit_positions = [position for position, _, _ in primary_keys if position is not None]
    if len(set(explicit_positions)) != len(explicit_positions):
        raise ODCSContractError(f"schema[{schema_index}]: primaryKeyPosition values must be unique")

    next_position = max(explicit_positions, default=0) + 1
    assigned: list[tuple[int, int, str]] = []
    for position_or_none, index, name in primary_keys:
        position = position_or_none
        if position is None:
            position = next_position
            next_position += 1
        assigned.append((position, index, name))
    assigned.sort()
    return assigned


def _add_primary_key(properties: list[Mapping[str, Any]], context: _ImportContext) -> None:
    primary_keys = _primary_key_columns(properties, context.schema_index)
    if not primary_keys:
        return
    meta = _meta(context.contract_id, f"schema[{context.schema_index}]")
    for _, _, name in primary_keys:
        context.builder.add(
            ExpectColumnValuesToNotBeNull(column=name, meta=meta),
            "not_null",
            name,
            None,
        )
    if len(primary_keys) == 1:
        _add_unique(primary_keys[0][2], meta, context.builder)
        return
    _add_compound([name for _, _, name in primary_keys], meta, context.builder)


def _suite_metadata(
    contract: Mapping[str, Any],
    schema_object: Mapping[str, Any],
    ignored: list[dict[str, str]],
) -> dict[str, Any]:
    schema_name = cast("str", schema_object["name"])
    contract_name = contract.get("name")
    if not isinstance(contract_name, str) or not contract_name:
        contract_name = schema_name
    suite_meta: dict[str, Any] = {
        "odcs": {
            "contract_id": cast("str", contract["id"]),
            "contract_name": contract_name,
            "contract_version": contract["version"],
            "api_version": contract["apiVersion"],
            "schema": schema_name,
            "ignored": ignored,
        }
    }
    return suite_meta


def _validate_import_options(suite_name: str | None, type_map: Mapping[str, str] | None) -> None:
    if suite_name is not None and (not isinstance(suite_name, str) or not suite_name):
        raise ODCSContractError("suite_name must be a non-empty string")
    if type_map is None:
        return
    if not isinstance(type_map, Mapping):
        raise ODCSContractError("type_map must be a mapping from logical types to type names")
    for logical_type, type_name in type_map.items():
        if not isinstance(logical_type, str) or logical_type not in _LOGICAL_TYPES:
            raise ODCSContractError(f"type_map key {logical_type!r} is not an ODCS logicalType")
        if not isinstance(type_name, str) or not type_name:
            raise ODCSContractError(f"type_map[{logical_type!r}] must be a non-empty string")


@public_api
def from_odcs(
    source: ODCSSource,
    *,
    schema: str | None = None,
    suite_name: str | None = None,
    type_map: Mapping[str, str] | None = None,
) -> ExpectationSuite:
    """Build an ExpectationSuite from an ODCS v3.1 data contract.

    The importer accepts the ODCS v3.1 schema shape: a contract with a
    ``schema`` list, each schema object containing ``properties`` and optional
    ``quality`` rules. It translates the supported subset of ODCS metrics and
    schema constraints into existing Great Expectations Expectations. Rules
    that cannot be represented are either recorded in the suite metadata
    (descriptive or foreign rules) or raise an explicit error (SQL rules and
    unsupported operators/thresholds); they are never silently ignored.

    Args:
        source: A path to a YAML file, a YAML string, a mapping, or a text or
            binary stream containing the contract.
        schema: Name of the schema object to import when the contract defines
            more than one. Optional when the contract defines exactly one.
        suite_name: Name for the resulting suite. Defaults to the selected
            schema object's name.
        type_map: Optional mapping from ODCS ``logicalType`` values to
            backend-specific type names for ``ExpectColumnValuesToBeOfType``.
            A property's ``physicalType`` is used when no mapping is supplied.

    Returns:
        An ExpectationSuite containing the imported Expectations. The caller
        is responsible for adding it to a context.

    Raises:
        ODCSContractError: If the contract is malformed or a rule is invalid.
        UnsupportedODCSRuleError: If a valid ODCS rule cannot be represented by
            an existing Expectation.
    """
    contract = _load_contract(source)
    _validate_contract_header(contract)
    _validate_import_options(suite_name, type_map)
    schema_index, schema_object = _select_schema(contract, schema)
    properties = _validate_schema(schema_object, schema_index)

    context = _ImportContext(
        contract_id=cast("str", contract["id"]),
        schema_index=schema_index,
        property_names={cast("str", prop["name"]) for prop in properties},
        type_map=type_map,
        builder=_ExpectationBuilder(),
        ignored=[],
    )
    context.builder.add(
        ExpectTableColumnsToMatchSet(
            column_set=[cast("str", prop["name"]) for prop in properties],
            meta=_meta(context.contract_id, f"schema[{schema_index}]"),
        ),
        "schema_columns",
    )

    _add_schema_quality(schema_object, context)
    _add_property_rules(properties, context)
    _add_primary_key(properties, context)

    if schema_object.get("relationships"):
        context.ignored.append(
            {
                "path": f"schema[{schema_index}]",
                "reason": "relationships are not mapped",
            }
        )

    schema_name = cast("str", schema_object["name"])
    return ExpectationSuite(
        name=suite_name or schema_name,
        expectations=context.builder.expectations,
        meta=_suite_metadata(contract, schema_object, context.ignored),
    )


__all__ = ["ODCSSource", "from_odcs"]
