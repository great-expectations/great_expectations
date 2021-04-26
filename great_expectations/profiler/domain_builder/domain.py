from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any, Dict, Optional, Union

from great_expectations.core import IDDict
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.types import SerializableDictDot


class DomainTypes(Enum):
    """
    This is a base class for the different specific DomainTypes classes, each of which enumerates the particular variety
    of domain types (e.g., "StorageDomainTypes", "SemanticDomainTypes", etc.).  Since the base "DomainTypes" extends
    "Enum", the JSON serialization, supported for the general "Enum" class, applies for all "DomainTypes" classes, too.
    """

    pass


# TODO: <Alex>ALEX -- We need to review these Storage Domain Types and finalize this Enum in terms of what is required.</Alex>
class StorageDomainTypes(DomainTypes):
    SCHEMA = "schema"
    TABLE = "table"
    COLUMN = "column"


# TODO: <Alex>ALEX -- We need to review these Semantic Domain Types and finalize this Enum in terms of what is required.</Alex>
class SemanticDomainTypes(DomainTypes):
    INTEGER = "integer"
    NUMERIC = "numeric"
    DATETIME = "datetime"


@dataclass
class Domain(SerializableDictDot):
    domain_kwargs: Optional[
        Dict[str, Union[str, MetricDomainTypes, Dict[str, Any]]]
    ] = None
    domain_type: Optional[DomainTypes] = None

    def to_json_dict(self) -> dict:
        return convert_to_json_serializable(data=asdict(self))

    @property
    def id(self) -> str:
        return IDDict(self.to_json_dict()).to_id()
