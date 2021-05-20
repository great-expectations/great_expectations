from enum import Enum
from typing import Any, Callable, Optional

from great_expectations.core.util import MetaClsEnumJoin


class DomainTypes(Enum):
    """
    This is a base class for the different specific DomainTypes classes, each of which enumerates the particular variety
    of domain types (e.g., "StructuredDomainTypes", "SemanticDomainTypes", "MetricDomainTypes", etc.).  Since the base
    "DomainTypes" extends "Enum", the JSON serialization, supported for the general "Enum" class, applies for all
    "DomainTypes" classes, too.
    """

    @classmethod
    def has_member_key(cls, key: Any) -> bool:
        key_exists: bool = key in cls.__members__ or str(key).upper() in cls.__members__
        if key_exists:
            hash_op: Optional[Callable] = getattr(key, "__hash__", None)
            if callable(hash_op):
                return True
            return False
        return False


class StructuredDomainTypes(DomainTypes):
    SCHEMA = "schema"
    TABLE = "table"
    COLUMN = "column"
    COLUMN_PAIR = "column_pair"
    MULTICOLUMN = "multicolumn"


class SemanticDomainTypes(DomainTypes):
    NUMERIC = "numeric"
    TEXT = "text"
    LOGIC = "logic"
    DATETIME = "datetime"
    BINARY = "binary"
    CURRENCY = "currency"
    IDENTITY = "identity"
    VALUE_SET = "value_set"
    MISCELLANEOUS = "miscellaneous"
    UNKNOWN = "unknown"


class MetricDomainTypes(
    Enum, metaclass=MetaClsEnumJoin, enums=(StructuredDomainTypes, SemanticDomainTypes)
):
    @classmethod
    def has_member_key(cls, key: Any) -> bool:
        klass: DomainTypes
        return any(
            [
                klass.has_member_key(key=key) for klass in cls.enums
            ]
        )
