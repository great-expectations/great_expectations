import importlib
import inspect
import logging
import os
import sys
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Type

import pkg_resources
from ruamel.yaml import YAML

from great_expectations.core.expectation_diagnostics.expectation_diagnostics import (
    ExpectationDiagnostics,
)
from great_expectations.expectations.expectation import Expectation
from great_expectations.types import SerializableDictDot

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

yaml = YAML()


@dataclass
class PackageCompletenessStatus(SerializableDictDot):
    concept_only: int
    experimental: int
    beta: int
    production: int
    total: int


@dataclass
class Dependency(SerializableDictDot):
    text: str
    link: str
    version: Optional[str] = None


@dataclass
class GitHubUser(SerializableDictDot):
    username: str
    full_name: Optional[str] = None


class SocialLinkType(str, Enum):
    TWITTER = "TWITTER"
    GITHUB = "GITHUB"
    LINKEDIN = "LINKEDIN"
    MEDIUM = "MEDIUM"
    WEBSITE = "WEBSITE"


@dataclass
class SocialLink(SerializableDictDot):
    account_type: SocialLinkType
    identifier: str


@dataclass
class DomainExpert(SerializableDictDot):
    full_name: str
    social_links: Optional[List[SocialLink]] = None
    picture: Optional[str] = None
    title: Optional[str] = None
    bio: Optional[str] = None


class Maturity(str, Enum):
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
    expectations: Optional[Dict[str, ExpectationDiagnostics]] = None
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

    def to_json_dict(self) -> dict:
        # Chetan - 20220511 - this is a TEMPORARY patch to pop non-serializable values from the result dict
        json_dict = asdict(self)
        for value in json_dict["expectations"].values():
            for test in value["tests"]:
                test.pop("validation_result")
        return json_dict

    def update_package_state(self) -> None:
        """
        Parses diagnostic reports from package Expectations and uses them to update JSON state
        """
        diagnostics = (
            GreatExpectationsContribPackageManifest.retrieve_package_expectations_diagnostics()
        )
        self._update_attrs_with_diagnostics(diagnostics)

    def _update_attrs_with_diagnostics(self, diagnostics: List[ExpectationDiagnostics]) -> None:
        self._update_from_package_info("package_info.yml")
        self._update_expectations(diagnostics)
        self._update_dependencies("requirements.txt")
        self._update_contributors(diagnostics)

    def _update_from_package_info(self, path: str) -> None:  # noqa: C901 - too complex
        if not os.path.exists(path):  # noqa: PTH110
            logger.warning(f"Could not find package info file {path}")
            return

        with open(path) as f:
            data: dict = yaml.load(f.read())

        if not data:
            logger.warning(f"{path} is empty so exiting early")
            return

        # Assign general attrs
        general = data.get("general")
        if general:
            for attr in ("package_name", "icon", "description"):
                if attr == "icon":
                    # If the user has provided an icon, we need to check if it is a relative URL.
                    # If it is, we need to convert to the HTTPS path that will show up when merged into `develop`.
                    icon: Optional[str] = general.get(attr)
                    if icon and os.path.exists(icon):  # noqa: PTH110
                        package_name: str = os.path.basename(  # noqa: PTH119
                            os.getcwd()  # noqa: PTH109
                        )
                        url: str = os.path.join(  # noqa: PTH118
                            "https://raw.githubusercontent.com/great-expectations/great_expectations/develop/contrib",
                            package_name,
                            icon,
                        )
                        self["icon"] = url
                    else:
                        self["icon"] = icon
                else:
                    self[attr] = general.get(attr)

        # Assign code owners
        code_owners = data.get("code_owners")
        if code_owners:
            self.code_owners = []
            for owner in code_owners:
                code_owner = GitHubUser(**owner)
                self.code_owners.append(code_owner)

        # Assign domain experts
        domain_experts = data.get("domain_experts")
        if domain_experts:
            self.domain_experts = []
            for expert in domain_experts:
                # If the user has provided a picture, we need to check if it is a relative URL.
                # If it is, we need to convert to the HTTPS path that will show up when merged into `develop`.
                picture_path: Optional[str] = expert.get("picture")
                if picture_path and os.path.exists(picture_path):  # noqa: PTH110
                    package_name: str = os.path.basename(  # noqa: PTH119
                        os.getcwd()  # noqa: PTH109
                    )
                    url: str = os.path.join(  # noqa: PTH118
                        "https://raw.githubusercontent.com/great-expectations/great_expectations/develop/contrib",
                        package_name,
                        picture_path,
                    )
                    expert["picture"] = url

                domain_expert = DomainExpert(**expert)
                self.domain_experts.append(domain_expert)

    def _update_expectations(self, diagnostics: List[ExpectationDiagnostics]) -> None:
        expectations = {}
        status = {maturity.name: 0 for maturity in Maturity}

        for diagnostic in diagnostics:
            name = diagnostic.description.snake_name
            expectations[name] = diagnostic
            expectation_maturity = diagnostic.library_metadata.maturity
            status[expectation_maturity] += 1

        self.expectations = expectations
        self.expectation_count = len(expectations)

        # Enum is all caps but status attributes are lowercase
        lowercase_status = {k.lower(): v for k, v in status.items()}
        lowercase_status["total"] = sum(status.values())
        self.status = PackageCompletenessStatus(**lowercase_status)

        maturity = max(status, key=status.get)  # Get the key with the max value
        self.maturity = Maturity[maturity]

    def _update_dependencies(self, path: str) -> None:
        if not os.path.exists(path):  # noqa: PTH110
            logger.warning(f"Could not find requirements file {path}")
            return

        with open(path) as f:
            requirements = [req for req in pkg_resources.parse_requirements(f)]

        def _convert_to_dependency(
            requirement: pkg_resources.Requirement,
        ) -> Dependency:
            name = requirement.project_name
            pypi_url = f"https://pypi.org/project/{name}"
            if requirement.specs:
                # Stringify tuple of pins
                version = ", ".join(
                    "".join(symbol for symbol in pin) for pin in sorted(requirement.specs)
                )
            else:
                version = None
            return Dependency(text=name, link=pypi_url, version=version)

        dependencies = list(map(_convert_to_dependency, requirements))
        self.dependencies = dependencies

    def _update_contributors(self, diagnostics: List[ExpectationDiagnostics]) -> None:
        contributors = []
        for diagnostic in diagnostics:
            for contributor in diagnostic.library_metadata.contributors:
                github_user = GitHubUser(contributor)
                if github_user not in contributors:
                    contributors.append(github_user)

        self.contributors = contributors

    @staticmethod
    def retrieve_package_expectations_diagnostics() -> List[ExpectationDiagnostics]:
        try:
            package = GreatExpectationsContribPackageManifest._identify_user_package()
            expectations_module = (
                GreatExpectationsContribPackageManifest._import_expectations_module(package)
            )
            expectations = (
                GreatExpectationsContribPackageManifest._retrieve_expectations_from_module(
                    expectations_module
                )
            )
            diagnostics = GreatExpectationsContribPackageManifest._gather_diagnostics(expectations)
            return diagnostics
        except Exception as e:
            # Exceptions should not break the CLI - this behavior should be working in the background
            # without the user being concerned about the underlying functionality
            logger.warning(
                f"Something went wrong when modifying the contributor package JSON object: {e}"
            )
            return []

    @staticmethod
    def _identify_user_package() -> str:
        # Guaranteed to have a dir named '<MY_PACKAGE>_expectations' through Cookiecutter validation
        packages = [
            d
            for d in os.listdir()
            if os.path.isdir(d) and d.endswith("_expectations")  # noqa: PTH112
        ]

        # A sanity check in case the user modifies the Cookiecutter template in unexpected ways
        if len(packages) == 0:
            raise FileNotFoundError("Could not find a user-defined package")  # noqa: TRY003
        elif len(packages) > 1:
            raise ValueError("Found more than one user-defined package")  # noqa: TRY003

        return packages[0]

    @staticmethod
    def _import_expectations_module(package: str) -> Any:
        # Need to add user's project to the PYTHONPATH
        cwd = os.getcwd()  # noqa: PTH109
        sys.path.append(cwd)
        try:
            expectations_module = importlib.import_module(f"{package}.expectations")
            return expectations_module
        except ModuleNotFoundError:  # noqa: TRY302
            raise

    @staticmethod
    def _retrieve_expectations_from_module(
        expectations_module: Any,
    ) -> List[Type[Expectation]]:
        expectations: List[Type[Expectation]] = []
        names: List[str] = []
        for name, obj in inspect.getmembers(expectations_module):
            # ProfileNumericColumnsDiffExpectation from capitalone_dataprofiler_expectations
            # is a base class that the contrib Expectations in that package all inherit from
            if inspect.isclass(obj) and issubclass(obj, Expectation) and not obj.is_abstract():
                expectations.append(obj)
                names.append(name)

        logger.info(f"Found {len(names)} expectation(s): {names}")
        return expectations

    @staticmethod
    def _gather_diagnostics(
        expectations: List[Type[Expectation]],
    ) -> List[ExpectationDiagnostics]:
        diagnostics_list = []
        for expectation in expectations:
            instance = expectation()
            diagnostics = instance.run_diagnostics()
            diagnostics_list.append(diagnostics)
            logger.info(f"Successfully retrieved diagnostics from {expectation}")

        return diagnostics_list
