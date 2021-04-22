from dataclasses import asdict, dataclass
from typing import Any, Dict, Optional, Union

from great_expectations.core.util import convert_to_json_serializable
from great_expectations.types import SerializableDictDot


@dataclass
class Parameter(SerializableDictDot):
    parameters: Dict[str, Any]
    details: Optional[Dict[str, Union[str, dict]]] = None

    def to_json_dict(self) -> dict:
        return convert_to_json_serializable(data=asdict(self))
