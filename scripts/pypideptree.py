from __future__ import annotations

import datetime as dt
import json
import logging
import pprint
import subprocess
from collections import defaultdict
from shutil import which
from typing import Dict, List, NamedTuple, Optional, Set, Tuple, TypedDict, cast

import requests
from dateutil import parser

PYPI_BASE_URL = "https://pypi.org/pypi"
TIME_ELAPSED_THRESHOLD = 7


logger = logging.getLogger(__name__)


class PyPIPackage(TypedDict):
    key: str
    package_name: str
    installed_version: str
    required_version: str
    dependencies: List[PyPIPackage]

    # These are added by us when generating the report
    latest_release: Optional[str]
    latest_release_timestamp: Optional[str]


class PyPIPackageJSON(TypedDict):
    info: PyPIPackageInfo
    # Always length 2 due to bdist_wheel and sdist (both have the info we need)
    releases: Dict[str, Tuple[PyPIPackageRelease, PyPIPackageRelease]]
    # ... Object is far richer but only including necessary elements


class PyPIPackageRelease(TypedDict):
    upload_time: str
    # ... Object is far richer but only including necessary elements


class PyPIPackageInfo(TypedDict):
    version: str
    # ... Object is far richer but only including necessary elements


class LatestReleaseInfo(NamedTuple):
    version: Optional[str]
    timestamp: Optional[str]


def collect_pkgs() -> List[PyPIPackage]:
    if not which("pipdeptree"):
        raise ValueError("Could not find 'pipdeptree' dependency in PATH")
    out = subprocess.check_output(["pipdeptree", "--json-tree"]).decode()
    return json.loads(out)


def update_pkgs_with_latest_release_info(
    pkgs: List[PyPIPackage],
) -> None:
    seen: Dict[str, LatestReleaseInfo] = {}
    stack = [pkg for pkg in pkgs]
    while stack:
        pkg = stack.pop()
        name = pkg["key"]
        for dep in pkg["dependencies"]:
            stack.append(dep)

        latest_release_info = _evaluate_pkg(pkg=pkg, cache=seen)
        pkg["latest_release"] = latest_release_info.version
        pkg["latest_release_timestamp"] = latest_release_info.timestamp
        seen[name] = latest_release_info


def _request_pypi(pkg: PyPIPackage) -> PyPIPackageJSON:
    url = f"{PYPI_BASE_URL}/{pkg['key']}/json"
    resp = requests.get(url)
    return cast(PyPIPackageJSON, resp.json())


def _evaluate_pkg(
    pkg: PyPIPackage, cache: Dict[str, LatestReleaseInfo]
) -> LatestReleaseInfo:
    name = pkg["key"]
    if name in cache:
        return cache[name]

    try:
        pkg_json = _request_pypi(pkg)
        version = pkg_json["info"]["version"]
        timestamp = pkg_json["releases"][version][0]["upload_time"]
    except:
        version = None
        timestamp = None

    return LatestReleaseInfo(version=version, timestamp=timestamp)


def determine_time_elapsed_since_last_release(
    pkgs: List[PyPIPackage],
) -> Dict[int, Set[str]]:
    diagnostics = defaultdict(set)
    today = dt.datetime.today()
    stack = [pkg for pkg in pkgs]
    while stack:
        pkg = stack.pop()
        name = pkg["key"]
        for dep in pkg["dependencies"]:
            stack.append(dep)

        timestamp_str = pkg["latest_release_timestamp"]
        if not timestamp_str:
            continue

        timestamp = parser.parse(timestamp_str)
        days_elapsed = (today - timestamp).days
        diagnostics[days_elapsed].add(name)

    return diagnostics


def print_results(pkgs: List[PyPIPackage], time_elapsed: Dict[int, Set[str]]) -> None:
    print("===== Dependency Tree =====")
    pprint.pprint(pkgs)

    print("\n\n===== Packages of Interest =====")
    for i in range(TIME_ELAPSED_THRESHOLD + 1):
        deps = time_elapsed.get(i, [])
        for dep in deps:
            print(i, dep)


def main() -> None:
    pkgs = collect_pkgs()
    update_pkgs_with_latest_release_info(pkgs)
    time_elapsed = determine_time_elapsed_since_last_release(pkgs)
    print_results(pkgs=pkgs, time_elapsed=time_elapsed)


if __name__ == "__main__":
    main()
