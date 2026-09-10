from __future__ import annotations

import logging
import pathlib
from pprint import pformat as pf
from typing import Final

import pytest
import tomli
from tasks import _SUITE_MARKERS, MARKER_DEPENDENCY_MAP, _marker_statement

pytestmark = pytest.mark.project

LOGGER: Final = logging.getLogger(__name__)
PROJECT_ROOT: Final = pathlib.Path(__file__).parent.parent
PYPROJECT_TOML: Final = PROJECT_ROOT / "pyproject.toml"
# Markers that are used to launch CI but map to a different marker for tests.
# eg, mssql should run the sql_server tests so, while a marker for CI,
# there should be no tests with this marker.
NO_TEST_MARKERS: Final = ["mssql"]


@pytest.fixture(scope="module")
def pyproject_toml_dict() -> dict:
    """Parse pyporject.toml and return as dict"""
    return tomli.loads(PYPROJECT_TOML.read_text())


@pytest.fixture(scope="module")
def pytest_markers(pyproject_toml_dict: dict) -> list[str]:
    """Return pytest markers"""
    LOGGER.debug(f"pytest config ->\n{pf(pyproject_toml_dict['tool']['pytest'], depth=2)}")
    marker_details = pyproject_toml_dict["tool"]["pytest"]["ini_options"]["markers"]
    LOGGER.debug(f"marker_details ->\n{pf(marker_details)}")
    return [m.split(":")[0] for m in marker_details]


def test_marker_mappings_are_registered(pytest_markers: list[str]):
    """
    Check that all pytest marker mappings are actually valid,
    and have been registered with pytest.
    """
    LOGGER.debug(f"pytest_markers:\n----------\n{pf(pytest_markers)}")

    for marker in MARKER_DEPENDENCY_MAP:
        if marker in NO_TEST_MARKERS:
            continue
        assert marker in pytest_markers


@pytest.mark.parametrize(
    ("marker", "expected_base"),
    [
        pytest.param("sqlite", "sqlite", id="plain-marker"),
        pytest.param("postgresql", "all_backends or postgresql", id="widened-marker"),
        pytest.param(
            "openpyxl or pyarrow or project or sqlite or aws_creds",
            "openpyxl or pyarrow or project or sqlite or aws_creds",
            id="five-way-disjunction",
        ),
    ],
)
def test_marker_statement_excludes_every_suite_marker_by_default(marker: str, expected_base: str):
    """With no opt-in, every shared lane's marker expression additionally excludes every
    suite marker, and the pre-existing expression is parenthesised so the exclusion cannot
    re-scope across a disjunction.
    """
    statement = _marker_statement(marker)

    exclusion = " and ".join(f"not {suite_marker}" for suite_marker in _SUITE_MARKERS)
    assert statement == f"'({expected_base}) and {exclusion}'"


def test_the_default_statement_names_the_suite_marker_literally():
    """Spelled out rather than derived, so that renaming the suite marker to something no
    test carries -- which would silently stop excluding anything -- fails here.
    """
    assert _marker_statement("sqlite") == "'(sqlite) and not gold'"


def test_every_suite_marker_is_a_declared_marker(pytest_markers: list[str]):
    """A suite marker that no test can carry excludes nothing, and pytest reports an
    unknown marker in an expression as simply not matching, so the lane would silently
    widen rather than fail.
    """
    for suite_marker in _SUITE_MARKERS:
        assert suite_marker in pytest_markers


def test_suite_markers_are_excluded_conjunctively(monkeypatch: pytest.MonkeyPatch):
    """Two suite markers must both be excluded. Joining the exclusions with `or` reads
    naturally and is a tautology -- anything not carrying one marker satisfies it -- so
    the lane would stop excluding the moment a second suite exists.
    """
    monkeypatch.setattr("tasks._SUITE_MARKERS", ("gold", "platinum"))

    assert _marker_statement("sqlite") == "'(sqlite) and not gold and not platinum'"


@pytest.mark.parametrize(
    ("marker", "expected_base"),
    [
        pytest.param("sqlite", "sqlite", id="plain-marker"),
        pytest.param("postgresql", "all_backends or postgresql", id="widened-marker"),
        pytest.param(
            "openpyxl or pyarrow or project or sqlite or aws_creds",
            "openpyxl or pyarrow or project or sqlite or aws_creds",
            id="five-way-disjunction",
        ),
    ],
)
def test_marker_statement_opts_into_one_suite(marker: str, expected_base: str):
    """With a suite opt-in, the expression additionally requires that suite's marker, so
    the lane selects only that suite for the given data source.
    """
    suite = _SUITE_MARKERS[0]

    statement = _marker_statement(marker, suite=suite)

    assert statement == f"'({expected_base}) and {suite}'"


def test_the_opt_in_statement_names_the_suite_marker_literally():
    """The opt-in half spelled out, for the same reason as the default half."""
    assert _marker_statement("sqlite", suite="gold") == "'(sqlite) and gold'"


if __name__ == "__main__":
    pytest.main([__file__, "-vv"])
