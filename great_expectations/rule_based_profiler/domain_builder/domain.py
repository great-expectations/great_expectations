from dataclasses import asdict, dataclass
from typing import Any, Dict, Optional, Union

from great_expectations.core import IDDict
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.types import SerializableDictDot


@dataclass
class Domain(SerializableDictDot):
    domain_kwargs: Optional[Dict[str, Union[str, Dict[str, Any]]]] = None

    def to_json_dict(self) -> dict:
        return convert_to_json_serializable(data=asdict(self))

    @property
    def id(self) -> str:
        return IDDict(self.to_json_dict()).to_id()
