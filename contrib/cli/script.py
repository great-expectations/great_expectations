from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any, Dict, List, Optional


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
    SCAFFOLDED = "SCAFFOLDED"
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
    def from_json(cls, data: Dict[str, Any]) -> "GreatExpectationsContribPackage":
        return cls(**data)

    def determine_values(self) -> None:
        self._determine_core_values()
        self._determine_user_values()
        self._determine_metadata()

    def _determine_core_values(self) -> None:
        pass

    def _determine_user_values(self) -> None:
        pass

    def _determine_metadata(self) -> None:
        pass


# Just testing serialization/deserialization
pkg = GreatExpectationsContribPackage(
    package_name="my_package",
    icon="my_icon",
    description="my_description",
    expectations=[],
    expectation_count=0,
    dependencies=[],
    maturity=Maturity.SCAFFOLDED,
    status=PackageCompletenessStatus(0, 0, 0, 0, 0),
    owners=[],
    contributors=[],
    domain_experts=[],
    version="",
)

foo = pkg.to_json_dict()
print(GreatExpectationsContribPackage.from_json(foo))
