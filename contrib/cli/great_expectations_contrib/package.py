import importlib
import inspect
import logging
import os
import sys
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Type

from great_expectations.expectations.expectation import Expectation

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Type alias that will need to be updated to reflect the complex nature of the 'run_diagnostics' return object
Diagnostics = Dict[str, Any]


@dataclass(frozen=True)
class ExpectationCompletenessCheck:
    message: str
    passed: bool


@dataclass(frozen=True)
class ExpectationCompletenessChecklist:
    experimental: List[ExpectationCompletenessCheck]
    beta: List[ExpectationCompletenessCheck]
    production: List[ExpectationCompletenessCheck]


@dataclass(frozen=True)
class PackageCompletenessStatus:
    concept_only: int
    experimental: int
    beta: int
    production: int
    total: int


@dataclass(frozen=True)
class RenderedExpectation:
    name: str
    tags: List[str]
    supported: List[str]
    status: ExpectationCompletenessChecklist


@dataclass(frozen=True)
class Dependency:
    text: str
    link: str
    version: Optional[str]


@dataclass(frozen=True)
class GitHubUser:
    username: str
    full_name: Optional[str]


class SocialLinkType(Enum):
    TWITTER = "TWITTER"
    INSTAGRAM = "INSTAGRAM"
    LINKEDIN = "LINKEDIN"
    MEDIUM = "MEDIUM"


@dataclass(frozen=True)
class SocialLink:
    account_type: SocialLinkType
    identifier: str


@dataclass(frozen=True)
class DomainExpert:
    full_name: str
    social_links: List[SocialLink]
    picture: str


class Maturity(Enum):
    CONCEPT_ONLY = "CONCEPT_ONLY"
    EXPERIMENTAL = "EXPERIMENTAL"
    BETA = "BETA"
    PRODUCTION = "PRODUCTION"


@dataclass(frozen=True)
class GreatExpectationsContribPackageManifest:
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

    def _update_attrs_with_diagnostics(self, diagnostics: List[Diagnostics]) -> None:
        # TODO: Write logic to assign values to attrs
        # This is a black box for now
        # for diagnostic in diagnostics:
        #     pass
        raise NotImplementedError

    def _retrieve_package_expectations_diagnostics(self) -> List[Diagnostics]:
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
    ) -> List[Diagnostics]:
        diagnostics_list = []
        for expectation in expectations:
            instance = expectation()
            diagnostics = instance.run_diagnostics()
            diagnostics_list.append(diagnostics)
            logger.info(f"Successfully retrieved diagnostics from {expectation}")

        return diagnostics_list
