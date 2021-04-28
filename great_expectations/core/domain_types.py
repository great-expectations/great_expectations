from enum import Enum

from great_expectations.core.util import MetaClsEnumJoin


class DomainTypes(Enum):
    """
    This is a base class for the different specific DomainTypes classes, each of which enumerates the particular variety
    of domain types (e.g., "StorageDomainTypes", "SemanticDomainTypes", "MetricDomainTypes", etc.).  Since the base
    "DomainTypes" extends "Enum", the JSON serialization, supported for the general "Enum" class, applies for all
    "DomainTypes" classes, too.
    """

    pass


class StorageDomainTypes(DomainTypes):
    SCHEMA = "schema"
    TABLE = "table"
    COLUMN = "column"
    COLUMN_PAIR = "column_pair"
    MULTICOLUMN = "multicolumn"


class SemanticDomainTypes(DomainTypes):
    IDENTITY = "identity"
    NUMERIC = "numeric"
    VALUE_SET = "value_set"
    DATETIME = "datetime"


class MetricDomainTypes(
    Enum, metaclass=MetaClsEnumJoin, enums=(StorageDomainTypes, SemanticDomainTypes)
):
    pass
