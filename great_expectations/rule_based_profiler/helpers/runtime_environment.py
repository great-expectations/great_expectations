import logging
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Union, cast

from great_expectations.core.util import convert_to_json_serializable
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.types import SerializableDictDot

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class RuntimeEnvironmentTableDomainTypeDirectivesKeys(Enum):  # isort:skip
    pass


class RuntimeEnvironmentColumnDomainTypeDirectivesKeys(Enum):  # isort:skip
    INCLUDE_COLUMN_NAMES = "include_column_names"
    EXCLUDE_COLUMN_NAMES = "exclude_column_names"
    INCLUDE_COLUMN_NAME_SUFFIXES = "include_column_name_suffixes"
    EXCLUDE_COLUMN_NAME_SUFFIXES = "exclude_column_name_suffixes"
    INCLUDE_SEMANTIC_TYPES = "include_semantic_types"
    EXCLUDE_SEMANTIC_TYPES = "exclude_semantic_types"
    CARDINALITY_LIMIT_MODE = "cardinality_limit_mode"


class RuntimeEnvironmentColumnPairDomainTypeDirectivesKeys(Enum):  # isort:skip
    pass


class RuntimeEnvironmentMulticolumnDomainTypeDirectivesKeys(Enum):  # isort:skip
    pass


RuntimeEnvironmentDomainTypeDirectivesKeys = Union[  # isort:skip
    RuntimeEnvironmentTableDomainTypeDirectivesKeys,
    RuntimeEnvironmentColumnDomainTypeDirectivesKeys,
    RuntimeEnvironmentColumnPairDomainTypeDirectivesKeys,
    RuntimeEnvironmentMulticolumnDomainTypeDirectivesKeys,
]


@dataclass
class RuntimeEnvironmentVariablesDirectives(SerializableDictDot):
    rule_name: str
    variables: Optional[Dict[str, Any]] = None

    def to_dict(self) -> dict:
        """
        Returns dictionary equivalent of this object.
        """
        return asdict(self)

    def to_json_dict(self) -> dict:
        """
        Returns JSON dictionary equivalent of this object.
        """
        return convert_to_json_serializable(data=self.to_dict())


def build_variables_directives(
    **kwargs: dict,
) -> List[RuntimeEnvironmentVariablesDirectives]:
    """
    This method makes best-effort attempt to identify directives, supplied in "kwargs", as "variables", referenced by
    components of "Rule" objects, identified by respective "rule_name" property as indicated, and return each of these
    directives as part of dedicated "RuntimeEnvironmentVariablesDirectives" typed object for every "rule_name" (string).
    """
    rule_name: str
    variables: Optional[Dict[str, Any]]
    return [
        RuntimeEnvironmentVariablesDirectives(
            rule_name=rule_name,
            variables=variables,
        )
        for rule_name, variables in kwargs.items()
    ]


@dataclass
class RuntimeEnvironmentDomainTypeDirectives(SerializableDictDot):
    domain_type: MetricDomainTypes
    directives: Dict[RuntimeEnvironmentDomainTypeDirectivesKeys, Any]

    def to_dict(self) -> dict:
        """
        Returns dictionary equivalent of this object.
        """
        return asdict(self)

    def to_json_dict(self) -> dict:
        """
        Returns JSON dictionary equivalent of this object.
        """
        return convert_to_json_serializable(data=self.to_dict())


def build_domain_type_directives(
    **kwargs: dict,
) -> List[RuntimeEnvironmentDomainTypeDirectives]:
    """
    This method makes best-effort attempt to identify directives, supplied in "kwargs", as supported properties,
    corresponnding to "DomainBuilder" classes, associated with every "MetricDomainTypes", and return each of these
    directives as part of dedicated "RuntimeEnvironmentDomainTypeDirectives" typed object for every "MetricDomainTypes".
    """
    domain_type_directives_list: List[RuntimeEnvironmentDomainTypeDirectives] = []

    column_domain_type_directives: RuntimeEnvironmentDomainTypeDirectives = (
        _build_specific_domain_type_directives(
            domain_type=MetricDomainTypes.COLUMN,
            domain_type_directives_keys=cast(
                RuntimeEnvironmentDomainTypeDirectivesKeys,
                RuntimeEnvironmentColumnDomainTypeDirectivesKeys,
            ),
            **kwargs,
        )
    )
    domain_type_directives_list.append(column_domain_type_directives)

    return domain_type_directives_list


def _build_specific_domain_type_directives(
    domain_type: MetricDomainTypes,
    domain_type_directives_keys: RuntimeEnvironmentDomainTypeDirectivesKeys,
    **kwargs: dict,
) -> RuntimeEnvironmentDomainTypeDirectives:
    domain_type_directives: RuntimeEnvironmentDomainTypeDirectives = (
        RuntimeEnvironmentDomainTypeDirectives(
            domain_type=domain_type,
            directives={},
        )
    )

    key: str
    value: Any
    for key, value in kwargs.items():
        try:
            # noinspection PyCallingNonCallable
            domain_type_directives_key: RuntimeEnvironmentDomainTypeDirectivesKeys = (
                domain_type_directives_keys(key)
            )
            domain_type_directives.directives[domain_type_directives_key] = value
        except ValueError:
            # Skip every directive that is not defined key in some "RuntimeEnvironmentDomainTypeDirectivesKeys" member.
            pass

    return domain_type_directives
