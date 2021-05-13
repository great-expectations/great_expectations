from dataclasses import asdict, dataclass
from typing import Any, Dict, Optional, Union

from great_expectations.core.domain_types import SemanticDomainTypes
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.types import SerializableDictDot


@dataclass
class InferredSemanticDomainType(SerializableDictDot):
    semantic_domain_type: Optional[Union[str, SemanticDomainTypes]] = None
    details: Optional[Dict[str, Any]] = None

    def to_json_dict(self) -> dict:
        return convert_to_json_serializable(data=asdict(self))
