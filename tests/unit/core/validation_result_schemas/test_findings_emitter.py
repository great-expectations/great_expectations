"""Unit tests for findings_emitter.py.

Covers:
- Round-trip: write N findings via context manager, read back JSON, assert structure
- Determinism: two runs over shuffled input produce byte-identical output
  (modulo timestamps), including for findings that tie on everything but
  datasource_test_id
- Env-var resolution: GX_VALIDATION_FINDINGS_DIR overrides default
- Default dir is anchored on the installed package, not the process CWD
- Atomic write: if Path.replace raises, the destination file is unchanged

All tests are marked @pytest.mark.unit and run via:
    pytest tests/unit/core/validation_result_schemas/test_findings_emitter.py -m unit
"""

from __future__ import annotations

import json
import random
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, List
from unittest.mock import patch

import pytest

from great_expectations.core.validation_result_schemas.findings_emitter import (
    _DEFAULT_DIR,
    _ENV_VAR,
    _GX_PACKAGE_DIR,
    SCHEMA_VERSION,
    FindingsWriter,
)

if TYPE_CHECKING:
    from great_expectations.core.validation_result_schemas.types import Finding


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_SAMPLE_FINDINGS: List[Finding] = [
    {
        "expectation_type": "expect_column_values_to_not_be_null",
        "result_format": "COMPLETE",
        "engine": "pandas",
        "datasource_test_id": "ds-001",
        "status": "parsed",
    },
    {
        "expectation_type": "expect_column_to_exist",
        "result_format": "BASIC",
        "engine": "spark",
        "datasource_test_id": "ds-002",
        "status": "parsed",
    },
    {
        "expectation_type": "expect_column_values_to_be_in_set",
        "result_format": "SUMMARY",
        "engine": "pandas",
        "datasource_test_id": "ds-003",
        "status": "failed",
        "error_summary": "schema mismatch",
    },
    # The next three differ from each other only by datasource_test_id.  Without
    # it in the sort key they tie, and a tie keeps execution order — which varies
    # between runs.
    {
        "expectation_type": "expect_column_values_to_be_in_set",
        "result_format": "SUMMARY",
        "engine": "pandas",
        "datasource_test_id": "ds-005",
        "status": "parsed",
    },
    {
        "expectation_type": "expect_column_values_to_be_in_set",
        "result_format": "SUMMARY",
        "engine": "pandas",
        "datasource_test_id": "ds-004",
        "status": "parsed",
    },
    {
        "expectation_type": "expect_column_values_to_be_in_set",
        "result_format": "SUMMARY",
        "engine": "pandas",
        "datasource_test_id": "ds-006",
        "status": "parsed",
    },
]

_FIXED_TS = "2026-05-07T14:23:11Z"


def _mock_now(*args, **kwargs):
    """Return a fixed datetime for deterministic timestamp tests."""
    return datetime(2026, 5, 7, 14, 23, 11, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# 1. Round-trip: write N findings, read back JSON, assert structure
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_round_trip_findings(tmp_path: Path) -> None:
    """Write findings via context manager and verify the JSON envelope."""
    run_id = "test-round-trip-run"
    with patch(
        "great_expectations.core.validation_result_schemas.findings_emitter.datetime"
    ) as mock_dt:
        mock_dt.now.return_value = _mock_now()
        mock_dt.now.side_effect = _mock_now

        with FindingsWriter(run_id, output_dir=tmp_path) as writer:
            for finding in _SAMPLE_FINDINGS:
                writer.write_finding(finding)

    output_file = tmp_path / f"{run_id}.json"
    assert output_file.exists(), "Output file should exist after close()"

    with output_file.open() as f:
        data = json.load(f)

    # Envelope fields
    assert data["schema_version"] == SCHEMA_VERSION
    assert data["run_id"] == run_id
    assert "started_at_utc" in data
    assert "completed_at_utc" in data
    assert "gx_version" in data
    assert isinstance(data["gx_version"], str)
    assert isinstance(data["findings"], list)
    assert len(data["findings"]) == len(_SAMPLE_FINDINGS)

    # Spot-check one finding field
    types_in_output = {f["expectation_type"] for f in data["findings"]}
    assert "expect_column_values_to_not_be_null" in types_in_output
    assert "expect_column_to_exist" in types_in_output


# ---------------------------------------------------------------------------
# 2. Determinism: two runs with same findings produce identical findings list
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_deterministic_output(tmp_path: Path) -> None:
    """Two runs over the same findings in different orders produce the same file.

    Feeding both runs the same list would only prove that sorted() is stable; the
    orders are shuffled so the ordering has to come from the sort key itself.
    """
    run_id = "deterministic-run"
    dirs = [tmp_path / "run1", tmp_path / "run2"]
    for d in dirs:
        d.mkdir()

    rng = random.Random(20260901)
    orderings = [list(_SAMPLE_FINDINGS), list(_SAMPLE_FINDINGS)]
    rng.shuffle(orderings[0])
    rng.shuffle(orderings[1])
    assert orderings[0] != orderings[1], "shuffled inputs must differ for this test to bite"

    for output_dir, findings in zip(dirs, orderings, strict=True):
        with patch(
            "great_expectations.core.validation_result_schemas.findings_emitter.datetime"
        ) as mock_dt:
            mock_dt.now.side_effect = _mock_now

            with FindingsWriter(run_id, output_dir=output_dir) as writer:
                for finding in findings:
                    writer.write_finding(finding)

    file1 = dirs[0] / f"{run_id}.json"
    file2 = dirs[1] / f"{run_id}.json"

    data1 = json.loads(file1.read_text())
    data2 = json.loads(file2.read_text())

    # Findings lists should be identical (same sort order)
    assert data1["findings"] == data2["findings"]

    # With mocked timestamps, full envelope should also be identical
    assert data1 == data2

    # And byte-identical on disk, not merely equal once parsed.
    assert file1.read_text() == file2.read_text()


@pytest.mark.unit
def test_findings_sorted_by_sort_key(tmp_path: Path) -> None:
    """Findings are sorted by (expectation_type, engine, result_format, datasource_test_id)."""
    run_id = "sorted-run"

    # Add findings in reverse alphabetical order to confirm sorting
    findings_reversed = list(reversed(_SAMPLE_FINDINGS))

    with FindingsWriter(run_id, output_dir=tmp_path) as writer:
        for finding in findings_reversed:
            writer.write_finding(finding)

    data = json.loads((tmp_path / f"{run_id}.json").read_text())
    sort_keys = [
        (
            f.get("expectation_type", ""),
            f.get("engine", ""),
            f.get("result_format", ""),
            f.get("datasource_test_id", ""),
        )
        for f in data["findings"]
    ]
    assert sort_keys == sorted(sort_keys)


@pytest.mark.unit
def test_datasource_test_id_breaks_ties(tmp_path: Path) -> None:
    """Findings identical apart from datasource_test_id are ordered by that id."""
    run_id = "tie-break-run"
    tie_group = {"ds-004", "ds-005", "ds-006"}
    tied = [f for f in _SAMPLE_FINDINGS if f["datasource_test_id"] in tie_group]
    assert len(tied) == 3, "fixture no longer contains the tie group this test needs"

    with FindingsWriter(run_id, output_dir=tmp_path) as writer:
        for finding in reversed(tied):
            writer.write_finding(finding)

    data = json.loads((tmp_path / f"{run_id}.json").read_text())
    assert [f["datasource_test_id"] for f in data["findings"]] == ["ds-004", "ds-005", "ds-006"]


# ---------------------------------------------------------------------------
# 3. Env-var resolution
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_env_var_resolution(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """GX_VALIDATION_FINDINGS_DIR env var is used when output_dir is None."""
    env_dir = tmp_path / "env_output"
    env_dir.mkdir()
    monkeypatch.setenv(_ENV_VAR, str(env_dir))

    run_id = "env-var-run"
    with FindingsWriter(run_id) as writer:
        writer.write_finding(_SAMPLE_FINDINGS[0])

    assert (env_dir / f"{run_id}.json").exists()


@pytest.mark.unit
def test_explicit_output_dir_overrides_env_var(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Explicit output_dir takes precedence over env var."""
    env_dir = tmp_path / "env_output"
    env_dir.mkdir()
    explicit_dir = tmp_path / "explicit_output"
    explicit_dir.mkdir()

    monkeypatch.setenv(_ENV_VAR, str(env_dir))

    run_id = "explicit-override-run"
    with FindingsWriter(run_id, output_dir=explicit_dir) as writer:
        writer.write_finding(_SAMPLE_FINDINGS[0])

    assert (explicit_dir / f"{run_id}.json").exists()
    assert not (env_dir / f"{run_id}.json").exists()


@pytest.mark.unit
def test_default_dir_used_when_no_env_var(monkeypatch: pytest.MonkeyPatch) -> None:
    """When neither arg nor env var is set, _DEFAULT_DIR is used."""
    monkeypatch.delenv(_ENV_VAR, raising=False)

    run_id = "default-dir-run"
    # Patch the mkdir the writer actually calls, so resolving the default does not
    # create a directory in the working tree as a side effect of the assertion.
    with patch.object(Path, "mkdir"):
        writer = FindingsWriter(run_id)
        assert writer._output_dir == Path(_DEFAULT_DIR)


# ---------------------------------------------------------------------------
# 4. Atomic write: if Path.replace raises, destination file is unchanged
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_atomic_write_no_partial_file_on_failure(tmp_path: Path) -> None:
    """If Path.replace raises, the destination file is not created/corrupted."""
    run_id = "atomic-fail-run"
    dest_file = tmp_path / f"{run_id}.json"
    assert not dest_file.exists()

    with patch(
        "great_expectations.core.validation_result_schemas.findings_emitter.Path.replace",
        side_effect=OSError("simulated replace failure"),
    ):
        writer = FindingsWriter(run_id, output_dir=tmp_path)
        writer.write_finding(_SAMPLE_FINDINGS[0])
        with pytest.raises(OSError, match="simulated replace failure"):
            writer.close()

    # Destination should not exist (atomic write failed before rename)
    assert not dest_file.exists(), "Destination file must not exist after failed atomic write"


@pytest.mark.unit
def test_atomic_write_preserves_existing_on_failure(tmp_path: Path) -> None:
    """If Path.replace raises when overwriting, old content is preserved."""
    run_id = "atomic-overwrite-run"
    dest_file = tmp_path / f"{run_id}.json"
    original_content = '{"old": "content"}'
    dest_file.write_text(original_content)

    with patch(
        "great_expectations.core.validation_result_schemas.findings_emitter.Path.replace",
        side_effect=OSError("simulated replace failure"),
    ):
        writer = FindingsWriter(run_id, output_dir=tmp_path)
        writer.write_finding(_SAMPLE_FINDINGS[0])
        with pytest.raises(OSError):
            writer.close()

    assert dest_file.read_text() == original_content, (
        "Existing file must be unchanged after failed atomic write"
    )


# ---------------------------------------------------------------------------
# 5. Context-manager protocol
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_context_manager_calls_close(tmp_path: Path) -> None:
    """Exiting context manager calls close() and produces output."""
    run_id = "ctx-manager-run"
    with FindingsWriter(run_id, output_dir=tmp_path) as writer:
        writer.write_finding(_SAMPLE_FINDINGS[0])

    assert (tmp_path / f"{run_id}.json").exists()


@pytest.mark.unit
def test_context_manager_propagates_exception(tmp_path: Path) -> None:
    """Exception inside context manager propagates after close()."""
    run_id = "ctx-exception-run"
    with pytest.raises(ValueError, match="test error"):
        with FindingsWriter(run_id, output_dir=tmp_path) as writer:
            writer.write_finding(_SAMPLE_FINDINGS[0])
            raise ValueError("test error")


# ---------------------------------------------------------------------------
# 6. Module constants
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_schema_version_is_int() -> None:
    assert isinstance(SCHEMA_VERSION, int)
    assert SCHEMA_VERSION == 1


@pytest.mark.unit
def test_default_dir_is_path() -> None:
    assert isinstance(_DEFAULT_DIR, Path)


@pytest.mark.unit
def test_default_dir_is_anchored_on_the_package_not_the_cwd() -> None:
    """A CWD-relative default writes findings wherever the run happened to start."""
    assert _DEFAULT_DIR.is_absolute()
    assert _GX_PACKAGE_DIR.name == "great_expectations"
    assert _DEFAULT_DIR.parents[3] == _GX_PACKAGE_DIR.parent
    assert _DEFAULT_DIR.relative_to(_GX_PACKAGE_DIR.parent) == Path(
        "tests/_artifacts/validation_result_schemas/findings"
    )


@pytest.mark.unit
def test_env_var_name() -> None:
    assert _ENV_VAR == "GX_VALIDATION_FINDINGS_DIR"
