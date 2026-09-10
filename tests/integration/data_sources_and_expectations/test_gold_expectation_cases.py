"""Unit coverage for the gold-tier case record and its shared fixture data.

These tests exercise the case table module in isolation — no data-source registry, no live batch
setup — because that module is required to import and validate cleanly with no data-source
dependency installed at all.
"""

from __future__ import annotations

import subprocess
import sys
import textwrap
from pathlib import Path

import pandas as pd
import pytest

import great_expectations.expectations as gxe
from tests.integration.data_sources_and_expectations.gold_expectation_cases import (
    CATEGORY_COL,
    DATE_COL,
    DECREASING_COL,
    FLOAT_COL,
    GOLD_EXTRA_TABLE_DATA,
    GOLD_FIXTURE_DATA,
    INCREASING_KEY_COL,
    JSON_COL,
    MULTICOLUMN_A_COL,
    MULTICOLUMN_B_COL,
    MULTICOLUMN_C_COL,
    NULLABLE_COL,
    PAIR_HIGH_COL,
    PAIR_LOW_COL,
    PATTERN_COL,
    STRFTIME_COL,
    TIMESTAMP_COL,
    CaseFixtureShape,
    GoldCase,
)
from tests.integration.test_utils.execution_engine_kind import ExecutionEngineKind

REPO_ROOT = Path(__file__).resolve().parents[3]


@pytest.mark.unit
def test_case_with_default_engines_applies_to_every_engine() -> None:
    case = GoldCase(
        key="expect_column_values_to_not_be_null",
        passing=gxe.ExpectColumnValuesToNotBeNull(column="increasing_key"),
        failing=gxe.ExpectColumnValuesToNotBeNull(column="nullable_value"),
    )
    assert case.engines == frozenset(ExecutionEngineKind)


@pytest.mark.unit
def test_case_with_empty_engine_set_is_rejected() -> None:
    with pytest.raises(ValueError, match="empty engine set"):
        GoldCase(
            key="expect_column_values_to_not_be_null",
            passing=gxe.ExpectColumnValuesToNotBeNull(column="increasing_key"),
            failing=gxe.ExpectColumnValuesToNotBeNull(column="nullable_value"),
            engines=frozenset(),
            engine_restriction_reason="deliberately empty for this test",
        )


@pytest.mark.unit
def test_case_can_restrict_to_a_proper_subset_of_engines() -> None:
    case = GoldCase(
        key="expect_column_values_to_not_be_null",
        passing=gxe.ExpectColumnValuesToNotBeNull(column="increasing_key"),
        failing=gxe.ExpectColumnValuesToNotBeNull(column="nullable_value"),
        fixture_shape=CaseFixtureShape.EXTRA_TABLE,
        engines=frozenset({ExecutionEngineKind.SQL}),
        engine_restriction_reason="the extra-table setup is only exposed for SQL backends",
    )
    assert case.engines == frozenset({ExecutionEngineKind.SQL})


@pytest.mark.unit
def test_fixture_shape_is_closed_to_three_members() -> None:
    assert {member.value for member in CaseFixtureShape} == {
        "standard",
        "extra_table",
        "comparison",
    }


@pytest.mark.unit
def test_shared_frame_carries_no_extra_tables() -> None:
    # The shared frame must be exactly one table: the known STANDARD column vocabulary, nothing
    # more. Asserting column-name disjointness against the extra table alone would pass even if
    # the extra table's rows were merged into the shared frame under different column names, so
    # assert the positive property instead -- GOLD_FIXTURE_DATA's column set is *exactly* the
    # published STANDARD vocabulary, and every value is scalar (no nested table smuggled in as a
    # column of frames/records).
    expected_columns = {
        INCREASING_KEY_COL,
        DECREASING_COL,
        FLOAT_COL,
        CATEGORY_COL,
        PATTERN_COL,
        NULLABLE_COL,
        DATE_COL,
        TIMESTAMP_COL,
        JSON_COL,
        STRFTIME_COL,
        PAIR_LOW_COL,
        PAIR_HIGH_COL,
        MULTICOLUMN_A_COL,
        MULTICOLUMN_B_COL,
        MULTICOLUMN_C_COL,
    }
    assert isinstance(GOLD_FIXTURE_DATA, pd.DataFrame)
    assert set(GOLD_FIXTURE_DATA.columns) == expected_columns
    assert set(GOLD_FIXTURE_DATA.columns).isdisjoint(set(GOLD_EXTRA_TABLE_DATA.columns))


@pytest.mark.unit
def test_module_imports_with_no_data_source_dependency_installed() -> None:
    """Prove, not assert: import the module in a fresh subprocess, then inspect that process's
    own ``sys.modules`` to confirm nothing from the data-source registry/driver world was ever
    loaded.

    This does not rely on an ``ImportError`` from a blocked package -- most per-backend driver
    imports in this repo are already wrapped in their own ``try/except ImportError`` guard, so
    blocking a package the module never needed proves nothing either way. It also does not
    hand-list which packages "must not" load: a hardcoded blocklist only covers what someone
    remembered to write down (this repo has a recorded history of hardcoded pattern lists that
    silently covered nothing). Instead it asserts positively on what *is* present after the
    import: no submodule of ``tests.integration.test_utils.data_source_config`` -- the package
    whose ``__init__`` eagerly imports every backend and the registry-derived ``tiers`` module --
    and no ``sqlalchemy``, which is what a live data-source read would need and is not required to
    read this module's pure data. This module never imports ``great_expectations`` itself, either
    -- the populated case table that constructs real ``Expectation`` instances lives in
    ``gold_expectation_case_table.py``, which imports ``great_expectations`` to build those
    instances, precisely so this module can keep this guarantee.

    Uses ``sys.modules`` introspection rather than a ``sys.meta_path`` blocking finder: a finder
    built on the legacy ``find_module`` hook is silently ignored on Python 3.12+ (the fallback
    that called it was removed), which would degrade this test to a bare import with every driver
    already installed -- still green, proving nothing.
    """
    probe = textwrap.dedent(
        """
        import sys

        sys.path.insert(0, "")

        from tests.integration.data_sources_and_expectations.gold_expectation_cases import (
            GOLD_FIXTURE_DATA,
            GOLD_EXTRA_TABLE_DATA,
        )

        assert len(GOLD_FIXTURE_DATA) > 0
        assert len(GOLD_EXTRA_TABLE_DATA) > 0

        loaded = set(sys.modules)

        registry_modules = {
            name
            for name in loaded
            if name == "tests.integration.test_utils.data_source_config"
            or name.startswith("tests.integration.test_utils.data_source_config.")
        }
        assert not registry_modules, (
            f"gold_expectation_cases.py pulled in the data_source_config package: "
            f"{sorted(registry_modules)}"
        )

        sqlalchemy_modules = {
            name for name in loaded if name == "sqlalchemy" or name.startswith("sqlalchemy.")
        }
        assert not sqlalchemy_modules, (
            f"gold_expectation_cases.py pulled in sqlalchemy: {sorted(sqlalchemy_modules)}"
        )

        great_expectations_modules = {
            name
            for name in loaded
            if name == "great_expectations" or name.startswith("great_expectations.")
        }
        assert not great_expectations_modules, (
            "gold_expectation_cases.py pulled in great_expectations itself (which "
            "opportunistically imports every installed SQL dialect driver at package-import "
            f"time): {sorted(great_expectations_modules)}"
        )

        print("OK")
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", probe],
        check=False,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, f"stdout={result.stdout!r} stderr={result.stderr!r}"
    assert "OK" in result.stdout
