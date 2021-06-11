from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any, Dict, Optional, Union

from great_expectations.core.util import convert_to_json_serializable
from great_expectations.types import SerializableDictDot


class SemanticDomainTypes(Enum):
    NUMERIC = "numeric"
    TEXT = "text"
    LOGIC = "logic"
    DATETIME = "datetime"
    BINARY = "binary"
    CURRENCY = "currency"
    VALUE_SET = "value_set"
    MISCELLANEOUS = "miscellaneous"
    UNKNOWN = "unknown"


@dataclass
class InferredSemanticDomainType(SerializableDictDot):
    semantic_domain_type: Optional[Union[str, SemanticDomainTypes]] = None
    details: Optional[Dict[str, Any]] = None

    def to_json_dict(self) -> dict:
        return convert_to_json_serializable(data=asdict(self))
