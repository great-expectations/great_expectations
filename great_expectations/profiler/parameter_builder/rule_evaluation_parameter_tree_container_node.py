from dataclasses import asdict, dataclass
from typing import Any, Dict, Optional, Union

from great_expectations.core.util import convert_to_json_serializable
from great_expectations.types import SerializableDictDot


@dataclass
class ParameterTreeContainerNode(SerializableDictDot):
    parameters: Dict[str, Any] = None
    details: Optional[Dict[str, Union[str, dict]]] = None
    descendants: Optional[Dict[str, "ParameterTreeContainerNode"]] = None

    def to_json_dict(self) -> dict:
        return convert_to_json_serializable(data=asdict(self))
