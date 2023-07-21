from __future__ import annotations

import logging
import pathlib
from pprint import pformat as pf
from typing import Final

import pytest
import tomli
from tasks import MARKER_DEPENDENDENCY_MAP

pytestmarks = pytest.mark.project

LOGGER: Final = logging.getLogger(__name__)
PROJECT_ROOT: Final = pathlib.Path(__file__).parent.parent
PYPROJECT_TOML: Final = PROJECT_ROOT / "pyproject.toml"


@pytest.fixture(scope="module")
def pyproject_toml_dict() -> dict:
    """Parse pyporject.toml and return as dict"""
    return tomli.loads(PYPROJECT_TOML.read_text())


@pytest.fixture(scope="module")
def pytest_markers(pyproject_toml_dict: dict) -> list[str]:
    """Return pytest markers"""
    LOGGER.debug(
        f"pytest config ->\n{pf(pyproject_toml_dict['tool']['pytest'], depth=2)}"
    )
    marker_details = pyproject_toml_dict["tool"]["pytest"]["ini_options"]["markers"]
    LOGGER.debug(f"marker_details ->\n{pf(marker_details)}")
    return [m.split(":")[0] for m in marker_details]


def test_marker_mappings_are_registered(pytest_markers: list[str]):
    """
    Check that all pytest marker mappings are acutallly valid,
    and have been registered with pytest.
    """
    LOGGER.debug(f"pytest_markers:\n----------\n{pf(pytest_markers)}")

    for marker in MARKER_DEPENDENDENCY_MAP:
        assert marker in pytest_markers


if __name__ == "__main__":
    pytest.main([__file__, "-vv"])
