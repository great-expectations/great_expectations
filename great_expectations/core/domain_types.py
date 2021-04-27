from enum import Enum
from typing import Tuple, cast

from great_expectations.util import extend_enum


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
    NUMERIC = "numeric"
    DATETIME = "datetime"


@extend_enum(tuple([cast(Enum, StorageDomainTypes), cast(Enum, SemanticDomainTypes)]))
class MetricDomainTypes:
    pass
