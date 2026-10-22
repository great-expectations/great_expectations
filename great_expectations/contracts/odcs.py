from __future__ import annotations

import io
import re
from pathlib import Path
from typing import TYPE_CHECKING, Any, TextIO

from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.expectations.core.expect_column_max_to_be_between import (
    ExpectColumnMaxToBeBetween,
)
from great_expectations.expectations.core.expect_column_values_to_be_of_type import (
    ExpectColumnValuesToBeOfType,
)
from great_expectations.expectations.core.expect_column_values_to_be_unique import (
    ExpectColumnValuesToBeUnique,
)
from great_expectations.expectations.core.expect_column_values_to_match_regex import (
    ExpectColumnValuesToMatchRegex,
)
from great_expectations.expectations.core.expect_column_values_to_not_be_null import (
    ExpectColumnValuesToNotBeNull,
)
from great_expectations.expectations.core.expect_compound_columns_to_be_unique import (
    ExpectCompoundColumnsToBeUnique,
)
from great_expectations.expectations.core.expect_table_columns_to_match_set import (
    ExpectTableColumnsToMatchSet,
)
from great_expectations.expectations.core.expect_table_row_count_to_be_between import (
    ExpectTableRowCountToBeBetween,
)

if TYPE_CHECKING:
    from great_expectations.data_context.data_context.abstract_data_context import (
        AbstractDataContext,
    )
    from great_expectations.expectations.expectation import Expectation

# Standard regex for email verification in data quality assertions
EMAIL_REGEX = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
BOUND_PAIR_LENGTH = 2


def _parse_yaml_source(source: str | Path | dict[str, Any] | TextIO) -> dict[str, Any]:
    """Parse YAML from a file path, string, stream, or existing dictionary."""
    if isinstance(source, dict):
        return source

    yaml_handler = YAMLHandler()

    if isinstance(source, Path):
        with open(source, encoding="utf-8") as f:
            data = yaml_handler.load(f)
    elif isinstance(source, str):
        if "\n" not in source and Path(source).is_file():
            with open(source, encoding="utf-8") as f:
                data = yaml_handler.load(f)
        else:
            data = yaml_handler.load(source)
    elif isinstance(source, io.TextIOWrapper):
        data = yaml_handler.load(source)
    elif hasattr(source, "read"):
        data = yaml_handler.load(source.read())
    else:
        raise TypeError(  # noqa: TRY003 # FIXME CoP
            f"Unsupported source type: {type(source).__name__}."
        )

    if not isinstance(data, dict):
        raise ValueError(  # noqa: TRY003, TRY004 # FIXME CoP
            f"Parsed contract YAML must be a mapping/dict, got {type(data).__name__}"
        )

    return data


def _extract_dataset_name(data: dict[str, Any]) -> str | None:  # noqa: C901 # FIXME CoP
    """Extract dataset name from ODCS v3 data contract structure."""
    dataset = data.get("dataset")
    if isinstance(dataset, dict) and dataset.get("name"):
        return str(dataset["name"])
    if isinstance(dataset, str) and dataset.strip():
        return dataset.strip()
    if isinstance(dataset, list) and dataset:
        first = dataset[0]
        if isinstance(first, dict) and first.get("name"):
            return str(first["name"])

    for key in ("name", "id"):
        if data.get(key):
            return str(data[key])

    schema = data.get("schema")
    if isinstance(schema, list) and schema and isinstance(schema[0], dict):
        if schema[0].get("name"):
            return str(schema[0]["name"])

    return None


def _normalize_raw_column_collection(cols: Any) -> list[dict[str, Any]]:
    """Normalize a list or dictionary of columns into a list of dicts."""
    results: list[dict[str, Any]] = []
    if isinstance(cols, list):
        results.extend(c for c in cols if isinstance(c, dict))
    elif isinstance(cols, dict):
        for col_name, col_def in cols.items():
            if isinstance(col_def, dict):
                results.append({"name": col_name, **col_def})
            elif isinstance(col_def, str):
                results.append({"name": col_name, "type": col_def})
    return results


def _extract_columns_from_schema_item(item: dict[str, Any]) -> list[dict[str, Any]]:
    """Extract column definitions from an item in the schema list."""
    results: list[dict[str, Any]] = []
    for key in ("columns", "properties", "fields"):
        if key in item:
            results.extend(_normalize_raw_column_collection(item[key]))
    has_type = "type" in item or "logicalType" in item or "physicalType" in item
    if "name" in item and has_type:
        results.append(item)
    return results


def _extract_columns_and_types(  # noqa: C901, PLR0912 # FIXME CoP
    data: dict[str, Any],
) -> list[dict[str, Any]]:
    """Extract column definitions from ODCS v3 structure."""
    raw_columns: list[dict[str, Any]] = []

    # 1. dataset.columns / dataset.properties / dataset.fields
    dataset = data.get("dataset")
    if isinstance(dataset, dict):
        for key in ("columns", "properties", "fields"):
            if key in dataset:
                raw_columns.extend(_normalize_raw_column_collection(dataset[key]))

    # 2. schema block
    schema = data.get("schema")
    if isinstance(schema, list):
        for item in schema:
            if isinstance(item, dict):
                raw_columns.extend(_extract_columns_from_schema_item(item))
    elif isinstance(schema, dict):
        for key in ("columns", "properties", "fields"):
            if key in schema:
                raw_columns.extend(_normalize_raw_column_collection(schema[key]))

    # 3. top-level columns / properties / fields
    for key in ("columns", "properties", "fields"):
        if key in data:
            raw_columns.extend(_normalize_raw_column_collection(data[key]))

    # Deduplicate and normalize by column name
    normalized: dict[str, dict[str, Any]] = {}
    for col in raw_columns:
        if "name" in col:
            col_name = str(col["name"])
            normalized.setdefault(col_name, {}).update(col)

    return list(normalized.values())


def _process_quality_block(  # noqa: C901 # FIXME CoP
    q: Any, default_col: str | None = None
) -> list[dict[str, Any]]:
    """Convert a quality block (list or dict) into a list of rule dicts."""
    rules: list[dict[str, Any]] = []
    if isinstance(q, list):
        for rule in q:
            if isinstance(rule, dict):
                r = dict(rule)
                if default_col and "column" not in r and "property" not in r:
                    r["column"] = default_col
                rules.append(r)
    elif isinstance(q, dict):
        for key, val in q.items():
            if isinstance(val, dict):
                r = dict(val)
                r.setdefault("type", key)
                if default_col and "column" not in r and "property" not in r:
                    r["column"] = default_col
                rules.append(r)
            elif isinstance(val, list):
                rules.append({"type": key, "columns": val})
            else:
                rules.append({"type": key, "value": val})
    return rules


def _extract_quality_rules(data: dict[str, Any]) -> list[dict[str, Any]]:
    """Gather all quality rules defined across root, dataset, and schema."""
    rules: list[dict[str, Any]] = []

    if "quality" in data:
        rules.extend(_process_quality_block(data["quality"]))

    dataset = data.get("dataset")
    if isinstance(dataset, dict) and "quality" in dataset:
        rules.extend(_process_quality_block(dataset["quality"]))

    schema = data.get("schema")
    if isinstance(schema, list):
        for item in schema:
            if isinstance(item, dict) and "quality" in item:
                rules.extend(_process_quality_block(item["quality"]))

    return rules


def _parse_numeric_bound(val: Any) -> tuple[float | int | None, float | int | None]:
    """Parse numeric bounds from various representations."""
    if isinstance(val, (int, float)):
        return val, val
    if isinstance(val, (list, tuple)) and len(val) == BOUND_PAIR_LENGTH:
        return val[0], val[1]
    if isinstance(val, str):
        val = val.strip()
        between_match = re.search(
            r"between\s+(\d+(?:\.\d+)?)\s+and\s+(\d+(?:\.\d+)?)", val, re.IGNORECASE
        )
        if between_match:
            return float(between_match.group(1)), float(between_match.group(2))
        gte = re.search(r">=\s*(\d+(?:\.\d+)?)", val)
        lte = re.search(r"<=\s*(\d+(?:\.\d+)?)", val)
        gt = re.search(r">\s*(\d+(?:\.\d+)?)", val)
        lt = re.search(r"<\s*(\d+(?:\.\d+)?)", val)

        min_val = float(gte.group(1)) if gte else (float(gt.group(1)) + 1 if gt else None)
        max_val = float(lte.group(1)) if lte else (float(lt.group(1)) - 1 if lt else None)
        if min_val is not None or max_val is not None:
            return min_val, max_val
        try:
            num = float(val)
            return (int(num) if num.is_integer() else num), (int(num) if num.is_integer() else num)
        except ValueError:
            pass
    return None, None


def _build_column_quality_expectations(  # noqa: C901 # FIXME CoP
    col: dict[str, Any], col_name: str
) -> list[Expectation]:
    """Build expectations from column-level quality definitions."""
    expectations: list[Expectation] = []
    q_list = col["quality"] if isinstance(col["quality"], list) else [col["quality"]]
    for q in q_list:
        if not isinstance(q, dict):
            continue
        rule_type = (q.get("type") or q.get("rule") or q.get("metric") or "").lower()
        if rule_type in ("missing", "missing_values", "nullvalues", "null_values"):
            expectations.append(ExpectColumnValuesToNotBeNull(column=col_name))
        elif rule_type in ("invalid", "invalid_values", "regex", "pattern"):
            pat = q.get("pattern") or q.get("regex")
            if not pat and (q.get("format") == "email" or "email" in col_name.lower()):
                pat = EMAIL_REGEX
            if pat:
                expectations.append(ExpectColumnValuesToMatchRegex(column=col_name, regex=str(pat)))
        elif rule_type in ("duplicate", "duplicates", "unique"):
            expectations.append(ExpectColumnValuesToBeUnique(column=col_name))
        elif rule_type in ("freshness", "recency", "timeliness"):
            min_val = (
                q.get("min_value")
                or q.get("min")
                or q.get("mustBeGreaterThan")
                or q.get("mustBeGreaterThanOrEqualTo")
            )
            max_val = (
                q.get("max_value")
                or q.get("max")
                or q.get("mustBeLessThan")
                or q.get("mustBeLessThanOrEqualTo")
            )
            if (
                "mustBeBetween" in q
                and isinstance(q["mustBeBetween"], (list, tuple))
                and len(q["mustBeBetween"]) == BOUND_PAIR_LENGTH
            ):
                min_val, max_val = q["mustBeBetween"][0], q["mustBeBetween"][1]
            bound = q.get("bound") or q.get("literal")
            if bound and min_val is None and max_val is None:
                min_val = bound
            expectations.append(
                ExpectColumnMaxToBeBetween(column=col_name, min_value=min_val, max_value=max_val)
            )
    return expectations


def _build_column_expectations(columns: list[dict[str, Any]]) -> list[Expectation]:  # noqa: C901 # FIXME CoP
    """Build expectations defined at the column level."""
    expectations: list[Expectation] = []
    column_names = [col["name"] for col in columns if "name" in col]
    if column_names:
        expectations.append(ExpectTableColumnsToMatchSet(column_set=column_names))

    for col in columns:
        col_name = col["name"]
        col_type = (
            col.get("type")
            or col.get("logicalType")
            or col.get("physicalType")
            or col.get("dataType")
        )
        if col_type:
            expectations.append(ExpectColumnValuesToBeOfType(column=col_name, type_=str(col_type)))

        if col.get("required") is True or col.get("nullable") is False:
            expectations.append(ExpectColumnValuesToNotBeNull(column=col_name))

        if col.get("unique") is True:
            expectations.append(ExpectColumnValuesToBeUnique(column=col_name))

        fmt = col.get("format")
        pattern = col.get("pattern") or col.get("regex")
        has_quality_pattern = False
        if "quality" in col:
            q_list = col["quality"] if isinstance(col["quality"], list) else [col["quality"]]
            has_quality_pattern = any(
                isinstance(q, dict)
                and (q.get("type") or q.get("rule") or q.get("metric") or "").lower()
                in ("invalid", "invalid_values", "regex", "pattern")
                for q in q_list
            )

        if pattern:
            expectations.append(ExpectColumnValuesToMatchRegex(column=col_name, regex=str(pattern)))
        elif not has_quality_pattern and (
            fmt == "email" or (isinstance(fmt, str) and "email" in fmt.lower())
        ):
            expectations.append(ExpectColumnValuesToMatchRegex(column=col_name, regex=EMAIL_REGEX))

        if "quality" in col:
            expectations.extend(_build_column_quality_expectations(col, col_name))

    return expectations


def _build_row_count_expectation(rule: dict[str, Any]) -> Expectation | None:
    """Build ExpectTableRowCountToBeBetween from a row_count quality rule."""
    min_val = (
        rule.get("min_value")
        or rule.get("min")
        or rule.get("mustBeGreaterThanOrEqualTo")
        or rule.get("mustBeGreaterThan")
    )
    max_val = (
        rule.get("max_value")
        or rule.get("max")
        or rule.get("mustBeLessThanOrEqualTo")
        or rule.get("mustBeLessThan")
    )
    if (
        "mustBeBetween" in rule
        and isinstance(rule["mustBeBetween"], (list, tuple))
        and len(rule["mustBeBetween"]) == BOUND_PAIR_LENGTH
    ):
        min_val, max_val = rule["mustBeBetween"][0], rule["mustBeBetween"][1]
    elif "mustBe" in rule:
        p_min, p_max = _parse_numeric_bound(rule["mustBe"])
        min_val = min_val if min_val is not None else p_min
        max_val = max_val if max_val is not None else p_max
    elif "value" in rule:
        p_min, p_max = _parse_numeric_bound(rule["value"])
        min_val = min_val if min_val is not None else p_min
        max_val = max_val if max_val is not None else p_max

    if min_val is not None and isinstance(min_val, (int, float)):
        min_val = int(min_val) if float(min_val).is_integer() else min_val
    if max_val is not None and isinstance(max_val, (int, float)):
        max_val = int(max_val) if float(max_val).is_integer() else max_val

    return ExpectTableRowCountToBeBetween(min_value=min_val, max_value=max_val)


def _build_missing_expectations(rule: dict[str, Any]) -> list[Expectation]:
    """Build ExpectColumnValuesToNotBeNull expectations from missing rule."""
    cols: list[str] = []
    for key in ("column", "property", "field"):
        if key in rule:
            cols.append(str(rule[key]))
    if "columns" in rule and isinstance(rule["columns"], (list, tuple)):
        cols.extend(str(c) for c in rule["columns"])
    elif "value" in rule:
        if isinstance(rule["value"], (list, tuple)):
            cols.extend(str(c) for c in rule["value"])
        elif isinstance(rule["value"], str):
            cols.append(rule["value"])
    return [ExpectColumnValuesToNotBeNull(column=c) for c in cols]


def _build_invalid_expectations(rule: dict[str, Any]) -> list[Expectation]:
    """Build ExpectColumnValuesToMatchRegex from invalid quality rule."""
    target_col = rule.get("column") or rule.get("property") or rule.get("field")
    pat = rule.get("pattern") or rule.get("regex") or rule.get("format")
    if (
        not pat
        and (rule.get("format") == "email" or (target_col and "email" in str(target_col).lower()))
    ) or pat == "email":
        pat = EMAIL_REGEX

    if target_col and pat:
        return [ExpectColumnValuesToMatchRegex(column=str(target_col), regex=str(pat))]
    return []


def _build_duplicate_expectations(  # noqa: C901 # FIXME CoP
    rule: dict[str, Any],
) -> list[Expectation]:
    """Build uniqueness expectations from duplicate quality rule."""
    target_cols: list[str] = []
    for key in ("columns", "column_list", "properties"):
        if key in rule and isinstance(rule[key], (list, tuple)):
            target_cols.extend(str(c) for c in rule[key])
    if not target_cols:
        for key in ("column", "property", "field"):
            if key in rule:
                target_cols.append(str(rule[key]))
    if not target_cols and "value" in rule:
        if isinstance(rule["value"], (list, tuple)):
            target_cols.extend(str(c) for c in rule["value"])
        elif isinstance(rule["value"], str):
            target_cols.append(rule["value"])

    if len(target_cols) >= BOUND_PAIR_LENGTH:
        return [ExpectCompoundColumnsToBeUnique(column_list=target_cols)]
    if len(target_cols) == 1:
        return [ExpectColumnValuesToBeUnique(column=target_cols[0])]
    return []


def _build_freshness_expectations(rule: dict[str, Any]) -> list[Expectation]:
    """Build ExpectColumnMaxToBeBetween from freshness quality rule."""
    target_col = rule.get("column") or rule.get("property") or rule.get("field")
    min_val = (
        rule.get("min_value")
        or rule.get("min")
        or rule.get("mustBeGreaterThan")
        or rule.get("mustBeGreaterThanOrEqualTo")
    )
    max_val = (
        rule.get("max_value")
        or rule.get("max")
        or rule.get("mustBeLessThan")
        or rule.get("mustBeLessThanOrEqualTo")
    )
    if (
        "mustBeBetween" in rule
        and isinstance(rule["mustBeBetween"], (list, tuple))
        and len(rule["mustBeBetween"]) == BOUND_PAIR_LENGTH
    ):
        min_val, max_val = rule["mustBeBetween"][0], rule["mustBeBetween"][1]
    bound = rule.get("bound") or rule.get("literal")
    if bound and min_val is None and max_val is None:
        min_val = bound

    if target_col:
        return [
            ExpectColumnMaxToBeBetween(column=str(target_col), min_value=min_val, max_value=max_val)
        ]
    return []


def _build_rule_expectations(rule: dict[str, Any]) -> list[Expectation]:
    """Map a single ODCS quality rule to corresponding GX expectations."""
    rule_type = str(
        rule.get("type") or rule.get("rule") or rule.get("metric") or rule.get("name") or ""
    ).lower()

    if rule_type in ("row_count", "rowcount", "row_counts", "rowcounts"):
        exp = _build_row_count_expectation(rule)
        return [exp] if exp else []
    if rule_type in ("missing", "missing_values", "nullvalues", "null_values"):
        return _build_missing_expectations(rule)
    if rule_type in ("invalid", "invalid_values", "regex", "pattern"):
        return _build_invalid_expectations(rule)
    if rule_type in ("duplicate", "duplicates", "duplicate_count", "duplicatecount", "unique"):
        return _build_duplicate_expectations(rule)
    if rule_type in ("freshness", "recency", "timeliness"):
        return _build_freshness_expectations(rule)
    return []


def from_odcs(
    source: str | Path | dict[str, Any] | TextIO,
    suite_name: str | None = None,
    context: AbstractDataContext | None = None,
) -> ExpectationSuite:
    """Read an Open Data Contract Standard (ODCS v3) contract and build an ExpectationSuite.

    Maps ODCS v3 fields to Great Expectations expectations:
    - dataset.name -> Asset / Suite name
    - column name + type -> ExpectTableColumnsToMatchSet, ExpectColumnValuesToBeOfType
    - quality.row_count -> ExpectTableRowCountToBeBetween
    - quality.missing -> ExpectColumnValuesToNotBeNull
    - quality.invalid (e.g. email) -> ExpectColumnValuesToMatchRegex
    - quality.duplicate -> ExpectCompoundColumnsToBeUnique (or ExpectColumnValuesToBeUnique)
    - quality.freshness -> ExpectColumnMaxToBeBetween (with a literal bound)

    Args:
        source: ODCS contract as a YAML string, file path, Path object, or parsed dict.
        suite_name: Optional explicit name for the ExpectationSuite. Derived from dataset.name.
        context: Optional DataContext. If provided, the suite is added to context.suites.

    Returns:
        ExpectationSuite: Populated with GX expectations corresponding to the ODCS contract rules.
    """
    data = _parse_yaml_source(source)
    resolved_suite_name = suite_name or _extract_dataset_name(data) or "odcs_contract_suite"

    expectations: list[Expectation] = []

    columns = _extract_columns_and_types(data)
    expectations.extend(_build_column_expectations(columns))

    quality_rules = _extract_quality_rules(data)
    for rule in quality_rules:
        expectations.extend(_build_rule_expectations(rule))

    suite = ExpectationSuite(name=resolved_suite_name, expectations=expectations)

    if context is not None:
        suite = context.suites.add(suite)

    return suite
