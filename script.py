import datetime as dt
from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Tuple

import click
import pkg_resources
import requests
from dateutil.parser import parse
from pkg_resources import Requirement


@dataclass(frozen=True)
class PyPIDetails:
    home_page: str
    version: str
    upload_time: dt.datetime
    child_deps: List[Requirement]

    @classmethod
    def from_json(cls, json: dict) -> "PyPIDetails":
        home_page: str = json["info"]["home_page"]
        version: str = json["info"]["version"]
        latest_release: List[Dict[str, str]] = json["releases"][version]
        upload_time: dt.datetime = parse(latest_release[0]["upload_time"])

        raw_deps: List[str] = json["info"]["requires_dist"] or []
        child_deps: List[Requirement] = [
            req for req in pkg_resources.parse_requirements(raw_deps)
        ]
        child_deps = _filter_open_ended_requirements(child_deps)

        return cls(
            home_page=home_page,
            version=version,
            upload_time=upload_time,
            child_deps=child_deps,
        )

    def __repr__(self) -> str:
        return f"""  Version: {self.version}
  Time:    {str(self.upload_time)}
  URL:     {self.home_page}
"""


def collect_requirements(requirements_files: Tuple[str]) -> List[Requirement]:
    all_requirements: List[Requirement] = []
    for requirements_file in requirements_files:
        with open(requirements_file) as f:
            try:
                file_requirements: List[Requirement] = [
                    req for req in pkg_resources.parse_requirements(f)
                ]
                all_requirements.extend(file_requirements)
            except:
                continue

    all_requirements = _filter_open_ended_requirements(all_requirements)
    return all_requirements


def populate_dependency_graph(
    requirements: List[Requirement], depth: int = 1
) -> Dict[Requirement, PyPIDetails]:
    dependency_graph: Dict[Requirement, PyPIDetails] = {}

    stack: List[Tuple[Requirement, int]] = [
        (requirement, depth) for requirement in requirements
    ]
    while stack:
        node, d = stack.pop()
        if d == 0 or node in dependency_graph:
            continue

        json: dict = _make_request(node)

        try:
            details: PyPIDetails = PyPIDetails.from_json(json)
        except:
            continue

        for child in details.child_deps:
            stack.append((child, d - 1))

        dependency_graph[node] = details

    return dependency_graph


def traverse_dependency_graph(
    requirements: List[Requirement],
    dependency_graph: Dict[Requirement, PyPIDetails],
    timestamp: dt.datetime,
) -> Set[Requirement]:
    seen: Set[Requirement] = set()
    requirements_to_review: Set[Requirement] = set()

    print("[DEPENDENCY GRAPH]")
    stack: List[Tuple[Requirement, int]] = [
        (requirement, 0) for requirement in requirements
    ]
    while stack:
        requirement, d = stack.pop()
        if requirement in seen:
            continue

        seen.add(requirement)
        _print_requirement(requirement, d)

        details: Optional[PyPIDetails] = dependency_graph.get(requirement)
        if details:
            for child in details.child_deps:
                stack.append((child, d + 1))

            if details.upload_time > timestamp:
                requirements_to_review.add(requirement)

    return requirements_to_review


def determine_summary(
    requirements: Set[Requirement], pypi_graph: Dict[Requirement, PyPIDetails]
) -> None:
    print("\n\n[SUMMARY]")
    if not requirements:
        print("No requirements to review!")
        return

    for requirement in requirements:
        details: PyPIDetails = pypi_graph[requirement]
        _print_requirement(requirement)
        print(details)


def _make_request(requirement: Requirement) -> dict:
    url: str = f"https://pypi.org/pypi/{requirement.project_name}/json"
    json: dict = requests.get(url).json()
    return json


def _print_requirement(requirement: Requirement, spacing: int = 0) -> None:
    pin: str = str(requirement.specifier) or ""
    if pin:
        pin = f"({pin})"
    print(f"{'  ' * spacing}{requirement.project_name} {pin}")


def _filter_open_ended_requirements(
    requirements: List[Requirement],
) -> List[Requirement]:
    def _filter(req: Requirement) -> bool:
        specs: str = str(req.specifier)
        return not specs or specs.startswith(">")

    return list(filter(_filter, requirements))


@click.command()
@click.argument("requirements_files", type=click.Path(exists=True), nargs=-1)
@click.option("--n", "depth", type=int, default=1)
@click.option(
    "--date",
    "timestamp",
    type=click.DateTime(),
    default=dt.datetime.utcnow() - dt.timedelta(days=1),
)
def main(requirements_files: Tuple[str], depth: int, timestamp: dt.datetime) -> None:
    requirements: List[Requirement] = collect_requirements(requirements_files)
    pypi_graph: Dict[Requirement, PyPIDetails] = populate_dependency_graph(
        requirements, depth=depth
    )
    requirements_to_review: Set[Requirement] = traverse_dependency_graph(
        requirements, pypi_graph, timestamp
    )
    determine_summary(requirements_to_review, pypi_graph)


if __name__ == "__main__":
    main()
