from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any, Callable, Dict, Optional, Union

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

    @classmethod
    def has_member_key(cls, key: Any) -> bool:
        key_exists: bool = key in cls.__members__
        if key_exists:
            hash_op: Optional[Callable] = getattr(key, "__hash__", None)
            if callable(hash_op):
                return True
            return False
        return False


@dataclass
class InferredSemanticDomainType(SerializableDictDot):
    semantic_domain_type: Optional[Union[str, SemanticDomainTypes]] = None
    details: Optional[Dict[str, Any]] = None

    def to_json_dict(self) -> dict:
        return convert_to_json_serializable(data=asdict(self))
