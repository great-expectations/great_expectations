import importlib
import inspect
import json
import os
import sys
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any, List, Optional

from great_expectations.expectations.expectation import Expectation


@dataclass
class ExpectationCompletenessCheck:
    message: str
    passed: bool


@dataclass
class ExpectationCompletenessChecklist:
    experimental: List[ExpectationCompletenessCheck]
    beta: List[ExpectationCompletenessCheck]
    production: List[ExpectationCompletenessCheck]


@dataclass
class PackageCompletenessStatus:
    scaffolded: int
    experimental: int
    beta: int
    production: int
    total: int


@dataclass
class RenderedExpectationDetail:
    name: str
    tags: List[str]
    supported: List[str]
    status: ExpectationCompletenessChecklist


@dataclass
class Dependency:
    text: str
    link: str
    version: Optional[str]


@dataclass
class GitHubUser:
    username: str
    full_name: Optional[str]


@dataclass
class SocialLink:
    account_type: str
    identifier: str


@dataclass
class DomainExpert:
    full_name: str
    social_links: List[SocialLink]
    picture: str


class Maturity(Enum):
    CONCEPT_ONLY = "CONCEPT_ONLY"
    EXPERIMENTAL = "EXPERIMENTAL"
    BETA = "BETA"
    PRODUCTION = "PRODUCTION"


@dataclass
class GreatExpectationsContribPackage:
    # Core
    package_name: str
    icon: str
    description: str
    expectations: List[RenderedExpectationDetail]
    expectation_count: int
    dependencies: List[Dependency]
    maturity: Maturity
    status: PackageCompletenessStatus

    # Users
    owners: List[GitHubUser]
    contributors: List[GitHubUser]
    domain_experts: List[DomainExpert]

    # Metadata
    version: str

    def to_json_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_json_file(cls, file: str) -> "GreatExpectationsContribPackage":
        with open(file) as f:
            contents = f.read()

        data = json.loads(contents)
        return cls(**data)

    def determine_values(self) -> None:
        package = self._identify_user_package()
        expectations_module = self._import_expectations_module(package)
        expectations = self._retrieve_expectations_from_module(expectations_module)
        diagnostics = self._gather_diagnostics(expectations)

        self._determine_core_values()
        self._determine_user_values()
        self._determine_metadata()

    def _determine_core_values(self) -> None:
        pass

    def _determine_user_values(self) -> None:
        pass

    def _determine_metadata(self) -> None:
        pass

    def _identify_user_package(self):
        packages = [d for d in os.listdir() if os.path.isdir(d) and "expectations" in d]
        if len(packages) == 0:
            pass
        elif len(packages) > 1:
            pass

        package = packages[0]
        return package

    def _import_expectations_module(self, package: str) -> Any:
        try:
            cwd = os.getcwd()
            sys.path.append(cwd)
            expectations_module = importlib.import_module(f"{package}.expectations")
            return expectations_module
        except ModuleNotFoundError:
            print(f"Could not import user expectations")

    def _retrieve_expectations_from_module(
        self, expectations_module: Any
    ) -> List[Expectation]:
        expectations = []
        names = []
        for name, obj in inspect.getmembers(expectations_module):
            if inspect.isclass(obj) and name.endswith(
                "Expectation"
            ):  # Maybe use 'isinstance'?
                expectations.append(obj)
                names.append(name)

        print(f"Found {len(names)} expectation(s): {names}")
        return expectations

    def _gather_diagnostics(self, expectations: List[Expectation]) -> List[dict]:
        diagnostics_list = []
        for expectation in expectations:
            instance = expectation()
            diagnostics = instance.run_diagnostics()
            diagnostics_list.append(diagnostics)
        return diagnostics_list
