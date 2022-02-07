import importlib
import inspect
import logging
import os
import sys
from dataclasses import dataclass
from enum import Enum
from typing import Any, List, Optional, Type

import pkg_resources

from great_expectations.core.expectation_diagnostics.expectation_diagnostics import (
    ExpectationDiagnostics,
)
from great_expectations.expectations.expectation import Expectation
from great_expectations.types import SerializableDictDot

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@dataclass
class ExpectationCompletenessCheck(SerializableDictDot):
    message: str
    passed: bool


@dataclass
class ExpectationCompletenessChecklist(SerializableDictDot):
    experimental: List[ExpectationCompletenessCheck]
    beta: List[ExpectationCompletenessCheck]
    production: List[ExpectationCompletenessCheck]


@dataclass
class PackageCompletenessStatus(SerializableDictDot):
    concept_only: int
    experimental: int
    beta: int
    production: int
    total: int


@dataclass
class RenderedExpectation(SerializableDictDot):
    name: str
    tags: List[str]
    supported: List[str]
    status: ExpectationCompletenessChecklist


@dataclass
class Dependency(SerializableDictDot):
    text: str
    link: str
    version: Optional[str] = None


@dataclass
class GitHubUser(SerializableDictDot):
    username: str
    full_name: Optional[str] = None


class SocialLinkType(Enum):
    TWITTER = "TWITTER"
    INSTAGRAM = "INSTAGRAM"
    LINKEDIN = "LINKEDIN"
    MEDIUM = "MEDIUM"


@dataclass
class SocialLink(SerializableDictDot):
    account_type: SocialLinkType
    identifier: str


@dataclass
class DomainExpert(SerializableDictDot):
    full_name: str
    social_links: List[SocialLink]
    picture: str


class Maturity(Enum):
    CONCEPT_ONLY = "CONCEPT_ONLY"
    EXPERIMENTAL = "EXPERIMENTAL"
    BETA = "BETA"
    PRODUCTION = "PRODUCTION"


@dataclass
class GreatExpectationsContribPackageManifest(SerializableDictDot):
    # Core
    package_name: Optional[str] = None
    icon: Optional[str] = None
    description: Optional[str] = None
    expectations: Optional[List[RenderedExpectation]] = None
    expectation_count: Optional[int] = None
    dependencies: Optional[List[Dependency]] = None
    maturity: Optional[Maturity] = None
    status: Optional[PackageCompletenessStatus] = None

    # Users
    owners: Optional[List[GitHubUser]] = None
    contributors: Optional[List[GitHubUser]] = None
    domain_experts: Optional[List[DomainExpert]] = None

    # Metadata
    version: Optional[str] = None

    def update_package_state(self) -> None:
        """
        Parses diagnostic reports from package Expectations and uses them to update JSON state
        """
        diagnostics = self._retrieve_package_expectations_diagnostics()
        self._update_attrs_with_diagnostics(diagnostics)

    def _update_attrs_with_diagnostics(
        self, diagnostics: List[ExpectationDiagnostics]
    ) -> None:
        self.expectations = []
        self.expectation_count = len(diagnostics)
        self.dependencies = self._parse_dependencies_from_requirements_file(
            "requirements.txt"
        )
        self.contributors = []
        self.maturity = Maturity.CONCEPT_ONLY
        self.status = PackageCompletenessStatus(0, 0, 0, 0, 0)

        for diagnostic in diagnostics:
            for contributor in diagnostic.library_metadata.contributors:
                github_user = GitHubUser(contributor)
                if github_user not in self.contributors:
                    self.contributors.append(github_user)

            expectation = RenderedExpectation(
                name=diagnostic.description.snake_name,
                tags=diagnostic.library_metadata.tags,
                supported=[],
                status=diagnostic.maturity_checklist,  # Should be converted to the proper type
            )
            self.expectations.append(expectation)

            expectation_maturity = diagnostic.library_metadata.maturity
            self.status[expectation_maturity.lower()] += 1
            self.status.total += 1

        maturity = self.status.to_dict()
        self.maturity = max(maturity, key=maturity.get).upper()

    def _parse_dependencies_from_requirements_file(self, path: str) -> List[Dependency]:
        if not os.path.exists(path):
            raise FileNotFoundError("Could not find requirements file")

        with open(path) as f:
            requirements = [req for req in pkg_resources.parse_requirements(f)]

        def _convert_to_dependency(
            requirement: pkg_resources.Requirement,
        ) -> Dependency:
            name = requirement.project_name
            pypi_url = f"https://pypi.org/project/{name}"
            if requirement.specs:
                version = ", ".join(
                    "".join(symbol for symbol in pin) for pin in requirement.specs
                )
            else:
                version = None
            return Dependency(text=name, link=pypi_url, version=version)

        return list(map(_convert_to_dependency, requirements))

    def _retrieve_package_expectations_diagnostics(
        self,
    ) -> List[ExpectationDiagnostics]:
        try:
            package = self._identify_user_package()
            expectations_module = self._import_expectations_module(package)
            expectations = self._retrieve_expectations_from_module(expectations_module)
            diagnostics = self._gather_diagnostics(expectations)
            return diagnostics
        except Exception as e:
            # Exceptions should not break the CLI - this behavior should be working in the background
            # without the user being concerned about the underlying functionality
            logger.warning(
                f"Something went wrong when modifying the contributor package JSON object: {e}"
            )
            return []

    def _identify_user_package(self) -> str:
        # Guaranteed to have a dir named '<MY_PACKAGE>_expectations' through Cookiecutter validation
        packages = [
            d for d in os.listdir() if os.path.isdir(d) and d.endswith("_expectations")
        ]

        # A sanity check in case the user modifies the Cookiecutter template in unexpected ways
        if len(packages) == 0:
            raise FileNotFoundError("Could not find a user-defined package")
        elif len(packages) > 1:
            raise ValueError("Found more than one user-defined package")

        return packages[0]

    def _import_expectations_module(self, package: str) -> Any:
        # Need to add user's project to the PYTHONPATH
        cwd = os.getcwd()
        sys.path.append(cwd)
        try:
            expectations_module = importlib.import_module(f"{package}.expectations")
            return expectations_module
        except ModuleNotFoundError:
            raise

    def _retrieve_expectations_from_module(
        self, expectations_module: Any
    ) -> List[Type[Expectation]]:
        expectations: List[Type[Expectation]] = []
        names: List[str] = []
        for name, obj in inspect.getmembers(expectations_module):
            if inspect.isclass(obj) and issubclass(obj, Expectation):
                expectations.append(obj)
                names.append(name)

        logger.info(f"Found {len(names)} expectation(s): {names}")
        return expectations

    def _gather_diagnostics(
        self, expectations: List[Type[Expectation]]
    ) -> List[ExpectationDiagnostics]:
        diagnostics_list = []
        for expectation in expectations:
            instance = expectation()
            diagnostics = instance.run_diagnostics()
            diagnostics_list.append(diagnostics)
            logger.info(f"Successfully retrieved diagnostics from {expectation}")

        return diagnostics_list
