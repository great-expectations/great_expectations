import logging
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any, Dict, List, Union, cast

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.types import SerializableDictDot

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class RuntimeEnvironmentColumnDomainTypeDirectivesKeys(Enum):
    INCLUDE_COLUMN_NAMES = "include_column_names"
    EXCLUDE_COLUMN_NAMES = "exclude_column_names"
    INCLUDE_COLUMN_NAME_SUFFIXES = "include_column_name_suffixes"
    EXCLUDE_COLUMN_NAME_SUFFIXES = "exclude_column_name_suffixes"
    INCLUDE_SEMANTIC_TYPES = "include_semantic_types"
    EXCLUDE_SEMANTIC_TYPES = "exclude_semantic_types"


class RuntimeEnvironmentColumnPairDomainTypeDirectivesKeys(Enum):
    pass


class RuntimeEnvironmentMulticolumnDomainTypeDirectivesKeys(Enum):
    pass


class RuntimeEnvironmentTableDomainTypeDirectivesKeys(Enum):
    pass


RuntimeEnvironmentDomainTypeDirectivesKeys = Union[
    RuntimeEnvironmentColumnDomainTypeDirectivesKeys,
    RuntimeEnvironmentColumnPairDomainTypeDirectivesKeys,
    RuntimeEnvironmentMulticolumnDomainTypeDirectivesKeys,
    RuntimeEnvironmentTableDomainTypeDirectivesKeys,
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

    key: RuntimeEnvironmentDomainTypeDirectivesKeys
    value: Any
    for key, value in kwargs.items():
        try:
            # noinspection PyCallingNonCallable
            domain_type_directives_key: RuntimeEnvironmentDomainTypeDirectivesKeys = (
                domain_type_directives_keys(key)
            )
            domain_type_directives.directives[domain_type_directives_key] = value
        except ValueError as e:
            raise ge_exceptions.ProfilerExecutionError(
                message=f'Unknown property "{key}" in "{domain_type_directives_keys}"; {e} was raised.'
            )

    return domain_type_directives
