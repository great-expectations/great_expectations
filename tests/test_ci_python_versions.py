"""Checks the single definition of the supported Python versions in the CI workflow.

The `python-versions` job in `.github/workflows/ci.yml` holds the supported interpreters as one
list and derives, in a shell step, every shape the other jobs read: the full list, the floor, the
latest, and the per-event reductions. Pull requests run the base branch's copy of the workflow, so
an edit to that job or to the jobs that read it gets no in-PR exercise of the workflow itself. This
module is the pre-merge signal instead: it reads the checked-out copy of `ci.yml`, executes the
derive step's shell body once per event name, and checks the wiring the workflow depends on,
including the exclusion register in `marker-tests` that holds a lane on an older interpreter.

The event policy is pinned against a synthetic version list written out by hand below, not
against the real one, so the expected values are independent of the step's own logic and do not
change when the supported range moves.
"""

from __future__ import annotations

import json
import os
import pathlib
import re
import shutil
import subprocess
from functools import cache
from typing import Any, Dict, Final, Iterator, List, Set

import pytest

from great_expectations.core.yaml_handler import YAMLHandler

pytestmark = pytest.mark.unit

PROJECT_ROOT: Final = pathlib.Path(__file__).parent.parent
CI_WORKFLOW: Final = PROJECT_ROOT / ".github" / "workflows" / "ci.yml"
CONSTRAINTS_DIR: Final = PROJECT_ROOT / "ci" / "constraints-test"

DEFINITIONS_JOB: Final = "python-versions"
DERIVE_STEP_ID: Final = "derive"

_OUTPUT_REFERENCE: Final = re.compile(r"needs\.python-versions\.outputs\.([A-Za-z0-9_]+)")


@cache
def _jobs() -> Dict[str, Any]:
    data = YAMLHandler().load(CI_WORKFLOW.read_text())
    jobs = data["jobs"]
    assert isinstance(jobs, dict), f"{CI_WORKFLOW} has a non-mapping 'jobs' section"
    return jobs


def _definitions_job() -> Dict[str, Any]:
    job = _jobs().get(DEFINITIONS_JOB)
    assert isinstance(job, dict), f"{CI_WORKFLOW} has no job {DEFINITIONS_JOB!r}"
    return job


def _derive_step() -> Dict[str, Any]:
    matches = [step for step in _definitions_job()["steps"] if step.get("id") == DERIVE_STEP_ID]
    assert len(matches) == 1, f"expected one step with id {DERIVE_STEP_ID!r} in {DEFINITIONS_JOB}"
    return matches[0]


def _python_versions() -> str:
    return str(_definitions_job()["env"]["PYTHON_VERSIONS"])


def _needs(job: Dict[str, Any]) -> List[str]:
    needs = job.get("needs", [])
    return [needs] if isinstance(needs, str) else list(needs)


def _strings(node: Any) -> Iterator[str]:
    if isinstance(node, str):
        yield node
    elif isinstance(node, dict):
        for key, value in node.items():
            yield from _strings(key)
            yield from _strings(value)
    elif isinstance(node, list):
        for item in node:
            yield from _strings(item)


def _outputs_read_by(job: Dict[str, Any]) -> Set[str]:
    return {match.group(1) for text in _strings(job) for match in _OUTPUT_REFERENCE.finditer(text)}


def _require_tool(name: str) -> None:
    # The derive step shells out to jq; CI images carry it, a developer machine may not.
    if shutil.which(name) is None:
        if os.environ.get("CI"):
            pytest.fail(f"{name} is not on PATH; the derive step cannot be executed")
        pytest.skip(f"{name} is not on PATH")


def _run_derive(
    tmp_path: pathlib.Path, python_versions: str, event: str
) -> subprocess.CompletedProcess[str]:
    _require_tool("bash")
    _require_tool("jq")
    script = tmp_path / "derive.sh"
    script.write_text(str(_derive_step()["run"]))
    output = tmp_path / "github_output"
    output.write_text("")
    # An otherwise empty environment, so nothing ambient can change the result. Actions runs a
    # `run:` step with no `shell:` key as `bash -e`; the step sets `-euo pipefail` itself.
    env = {
        "PATH": os.environ.get("PATH", ""),
        "HOME": os.environ.get("HOME", "/"),
        "PYTHON_VERSIONS": python_versions,
        "GITHUB_EVENT_NAME": event,
        "GITHUB_OUTPUT": str(output),
    }
    return subprocess.run(
        ["bash", "--noprofile", "--norc", "-e", str(script)],
        env=env,
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )


def _derived_outputs(tmp_path: pathlib.Path, python_versions: str, event: str) -> Dict[str, str]:
    result = _run_derive(tmp_path, python_versions, event)
    assert result.returncode == 0, (
        f"the derive step failed under {event!r} (exit {result.returncode}): {result.stderr}"
    )
    lines = (tmp_path / "github_output").read_text().splitlines()
    return dict(line.split("=", 1) for line in lines if line)


# Three versions, so the floor-and-latest pair differs from the full list, and 3.9 before 3.10, so
# a string sort instead of a numeric one would be caught.
SYNTHETIC_VERSIONS: Final = '["3.9", "3.10", "3.11"]'

_SYNTHETIC_ALL: Final = '["3.9", "3.10", "3.11"]'
_SYNTHETIC_LATEST_ONLY: Final = '["3.11"]'
_SYNTHETIC_FLOOR_AND_LATEST: Final = '["3.9", "3.11"]'
_SYNTHETIC_EVENT_INDEPENDENT: Final = {
    "all": _SYNTHETIC_ALL,
    "floor": "3.9",
    "latest": "3.11",
    "latest_only": _SYNTHETIC_LATEST_ONLY,
    "min_versions_matrix": (
        '{"include": [{"python-version": "3.9", "tag": "py39"}, '
        '{"python-version": "3.10", "tag": "py310"}, '
        '{"python-version": "3.11", "tag": "py311"}]}'
    ),
}


@pytest.mark.parametrize(
    ("event", "reduced", "reduced_with_floor"),
    [
        pytest.param("schedule", _SYNTHETIC_ALL, _SYNTHETIC_ALL, id="schedule"),
        pytest.param("push", _SYNTHETIC_ALL, _SYNTHETIC_ALL, id="push"),
        pytest.param("merge_group", _SYNTHETIC_LATEST_ONLY, _SYNTHETIC_ALL, id="merge_group"),
        pytest.param(
            "pull_request_target",
            _SYNTHETIC_LATEST_ONLY,
            _SYNTHETIC_FLOOR_AND_LATEST,
            id="pull_request_target",
        ),
        pytest.param(
            "workflow_dispatch",
            _SYNTHETIC_LATEST_ONLY,
            _SYNTHETIC_FLOOR_AND_LATEST,
            id="workflow_dispatch",
        ),
        pytest.param(
            "some_future_event",
            _SYNTHETIC_LATEST_ONLY,
            _SYNTHETIC_FLOOR_AND_LATEST,
            id="unrecognized-event",
        ),
    ],
)
def test_derive_step_event_policy(
    tmp_path: pathlib.Path, event: str, reduced: str, reduced_with_floor: str
) -> None:
    outputs = _derived_outputs(tmp_path, SYNTHETIC_VERSIONS, event)

    assert outputs == {
        **_SYNTHETIC_EVENT_INDEPENDENT,
        "reduced": reduced,
        "reduced_with_floor": reduced_with_floor,
    }


def test_derive_step_single_version_list(tmp_path: pathlib.Path) -> None:
    outputs = _derived_outputs(tmp_path, '["3.12"]', "pull_request_target")

    assert outputs["reduced_with_floor"] == '["3.12"]'
    assert outputs["floor"] == outputs["latest"] == "3.12"


@pytest.mark.parametrize(
    "python_versions",
    [
        pytest.param("", id="empty"),
        pytest.param("[]", id="empty-array"),
        pytest.param('"3.10"', id="not-an-array"),
        pytest.param('["3.10", 3.11]', id="non-string-entry"),
        pytest.param('["3.10", "3.11.2"]', id="patch-version"),
        pytest.param('["3.11", "3.10"]', id="descending"),
        pytest.param('["3.10", "3.10"]', id="duplicate"),
        pytest.param('["3.10", "3.11"', id="invalid-json"),
    ],
)
def test_derive_step_rejects_a_malformed_definition(
    tmp_path: pathlib.Path, python_versions: str
) -> None:
    result = _run_derive(tmp_path, python_versions, "schedule")

    assert result.returncode != 0
    assert "PYTHON_VERSIONS" in result.stderr
    assert (tmp_path / "github_output").read_text() == ""


def test_derive_step_accepts_the_committed_definition(tmp_path: pathlib.Path) -> None:
    outputs = _derived_outputs(tmp_path, _python_versions(), "schedule")

    assert json.loads(outputs["all"]) == json.loads(_python_versions())


def test_derive_step_runs_the_same_outside_actions() -> None:
    # The step body is executed above outside Actions. A workflow expression in it would be
    # expanded by Actions and not here, and a step-level shell or env would not be reproduced,
    # so the checks above would no longer be checking what Actions runs.
    step = _derive_step()

    assert "${{" not in str(step["run"])
    assert "${{" not in _python_versions()
    assert "shell" not in step
    assert "env" not in step


def test_every_supported_version_has_a_min_versions_constraints_file() -> None:
    versions = json.loads(_python_versions())
    missing = [
        version
        for version in versions
        if not (CONSTRAINTS_DIR / f"py{version.replace('.', '')}-min-install.txt").is_file()
    ]

    assert versions
    assert missing == [], (
        f"every entry of PYTHON_VERSIONS needs {CONSTRAINTS_DIR}/py<XY>-min-install.txt "
        f"for the min-versions job; missing for {missing}"
    )


# Which definition output each job reads. `marker-tests` and `marker-tests-snowflake` must read
# `reduced`, not `latest_only`: the two agree on pull requests and manual runs, and differ only on
# scheduled and release-tag runs, which a pre-merge run never exercises.
EXPECTED_OUTPUTS_READ: Final = {
    "unit-tests": {"all"},
    "marker-tests": {"reduced"},
    "marker-tests-snowflake": {"reduced"},
    "import_gx": {"reduced_with_floor"},
    "redshift": {"latest_only"},
    "pyspark4-marker-tests": {"latest"},
    "gallery": {"latest"},
    "min-versions": {"min_versions_matrix"},
}


def test_each_job_reads_the_expected_definition_output() -> None:
    actual = {
        job_id: outputs
        for job_id, job in _jobs().items()
        if job_id != DEFINITIONS_JOB and (outputs := _outputs_read_by(job))
    }

    assert actual == EXPECTED_OUTPUTS_READ


def test_every_job_reading_the_definition_needs_it() -> None:
    # Without the `needs` edge the output expression evaluates to an empty string.
    missing = sorted(
        job_id
        for job_id, job in _jobs().items()
        if _outputs_read_by(job) and DEFINITIONS_JOB not in _needs(job)
    )

    assert missing == []


def test_every_definition_output_is_declared() -> None:
    declared = set(_definitions_job()["outputs"])
    read = set().union(*(_outputs_read_by(job) for job in _jobs().values()))

    assert read <= declared, f"jobs read undeclared outputs: {sorted(read - declared)}"


@pytest.mark.parametrize(
    ("aggregator", "required"),
    [
        pytest.param("ci-required", {DEFINITIONS_JOB, "min-versions"}, id="ci-required"),
        pytest.param("build-n-publish", {"min-versions"}, id="build-n-publish"),
        pytest.param("notify_on_failure", {"min-versions"}, id="notify_on_failure"),
    ],
)
def test_aggregators_depend_on_the_derived_jobs(aggregator: str, required: Set[str]) -> None:
    assert required <= set(_needs(_jobs()[aggregator]))


# The exclusion register in `marker-tests`: the lanes held on an older interpreter because their
# driver has no release for the latest one, and the interpreter each is held on. Lifting or adding
# a hold means editing this table along with the register.
MARKER_TESTS_JOB: Final = "marker-tests"
EXPECTED_HOLDS: Final = {
    "spark": "3.13",
    "spark_connect": "3.13",
}


def _marker_tests_matrix() -> Dict[str, Any]:
    return _jobs()[MARKER_TESTS_JOB]["strategy"]["matrix"]


def _cells(entries: Any) -> List[Dict[str, str]]:
    return [dict(entry) for entry in entries or []]


def test_the_register_holds_exactly_the_expected_lanes() -> None:
    # A hold is one exclude of the lane's cell on the latest interpreter plus one include of the
    # held cell. The exclude is written as a literal version, so it is compared against the
    # committed definition's latest: appending a newer interpreter fails this until the register
    # moves with it.
    versions = json.loads(_python_versions())
    matrix = _marker_tests_matrix()

    assert sorted(_cells(matrix.get("exclude")), key=lambda cell: cell["markers"]) == [
        {"markers": lane, "python-version": versions[-1]} for lane in sorted(EXPECTED_HOLDS)
    ]
    assert sorted(_cells(matrix.get("include")), key=lambda cell: cell["markers"]) == [
        {"markers": lane, "python-version": EXPECTED_HOLDS[lane]} for lane in sorted(EXPECTED_HOLDS)
    ]


def test_every_held_lane_and_interpreter_exists_in_the_matrix() -> None:
    # An include that names an unknown marker or interpreter does not fail the workflow: it adds a
    # new cell of its own, so a typo would run a lane that does not exist instead of the held one.
    versions = json.loads(_python_versions())
    markers = _marker_tests_matrix()["markers"]

    assert set(EXPECTED_HOLDS) <= set(markers)
    for lane, held in EXPECTED_HOLDS.items():
        assert held in versions[:-1], (
            f"{lane} is held on {held}, which is not an older entry of PYTHON_VERSIONS"
        )
