from dataclasses import asdict, dataclass
from enum import Enum, EnumMeta
from typing import Any, Callable, Dict, Optional, Union

from great_expectations.core.util import convert_to_json_serializable
from great_expectations.types import SerializableDictDot


class SemanticDomainEnumMeta(EnumMeta):
    def __getitem__(self, item):
        name: Any = item
        if isinstance(item, str):
            name = str(item).upper()
        try:
            return super().__getitem__(name)
        except (TypeError, KeyError) as e:
            raise ValueError(
                f'SemanticDomainEnumMeta is unable to access SemanticDomainTypes (illegal key "{item}" was passed).'
            )


class SemanticDomainTypes(Enum, metaclass=SemanticDomainEnumMeta):
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
        key_exists: bool = key in cls.__members__ or str(key).upper() in cls.__members__
        if key_exists:
            hash_op: Optional[Callable] = getattr(key, "__hash__", None) or getattr(
                str(key).upper(), "__hash__", None
            )
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
