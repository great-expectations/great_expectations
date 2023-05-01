from __future__ import annotations

import json
import logging
import sys

import requests
from packaging import version
from typing_extensions import TypedDict

logger = logging.getLogger(__name__)


class _PyPIPackageInfo(TypedDict):
    version: str


class _PyPIPackageData(TypedDict):
    info: _PyPIPackageInfo


# Should only run in prod and in specific tests
# This flag let's us conditionally turn on the feature
_ENABLE_VERSION_CHECK_IN_TESTS = False


class _VersionChecker:

    _BASE_PYPI_URL = "https://pypi.org/pypi"
    _PYPI_GX_ENDPOINT = f"{_BASE_PYPI_URL}/great_expectations/json"

    def __init__(self, user_version: str) -> None:
        self._user_version = version.Version(user_version)

    def check_if_using_latest_gx(self) -> bool:
        if self._running_non_version_check_tests():
            return True

        pypi_version = self._get_latest_version_from_pypi()
        if not pypi_version:
            logger.debug("Could not compare with latest PyPI version; skipping check.")
            return True

        if self._is_using_outdated_release(pypi_version):
            self._warn_user(pypi_version)
            return False
        return True

    def _running_non_version_check_tests(self) -> bool:
        # Exit early unless specifically running tests/data_context/cloud_data_context/test_version_checker.py
        return "pytest" in sys.modules and not _ENABLE_VERSION_CHECK_IN_TESTS

    def _get_latest_version_from_pypi(self) -> version.Version | None:
        response_json: _PyPIPackageData | None = None
        try:
            response = requests.get(self._PYPI_GX_ENDPOINT)
            response.raise_for_status()
            response_json = response.json()
        except json.JSONDecodeError as jsonError:
            logger.debug(f"Failed to parse PyPI API response into JSON: {jsonError}")
        except requests.HTTPError as http_err:
            logger.debug(
                f"An HTTP error occurred when trying to hit PyPI API: {http_err}"
            )
        except requests.Timeout as timeout_exc:
            logger.debug(
                f"Failed to hit the PyPI API due a timeout error: {timeout_exc}"
            )

        if not response_json:
            return None

        # Structure should be guaranteed but let's be defensive in case PyPI changes.
        info = response_json.get("info", {})
        pkg_version = info.get("version")
        if not pkg_version:
            logger.debug(
                "Successfully hit PyPI API but payload structure is not as expected."
            )
            return None

        return version.Version(pkg_version)

    def _is_using_outdated_release(self, pypi_version: version.Version) -> bool:
        return pypi_version > self._user_version

    def _warn_user(self, pypi_version: version.Version) -> None:
        logger.warning(
            f"You are using great_expectations version {self._user_version}; "
            f"however, version {pypi_version} is available.\nYou should consider "
            "upgrading via `pip install great_expectations --upgrade`\n."
        )
