from dataclasses import asdict, dataclass
from typing import Any, Dict, Optional, Union

from great_expectations.core import IDDict
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.types import SerializableDictDot


@dataclass
class Domain(SerializableDictDot):
    # TODO: <Alex>ALEX -- Need to discuss the implication to SemanticDomainTypeDomainBuilder</Alex>
    # domain_kwargs: Optional[
    #     Dict[str, Union[str, MetricDomainTypes, Dict[str, Any]]]
    # ] = None
    # domain_type: Optional[MetricDomainTypes] = None
    domain_kwargs: Optional[Dict[str, Union[str, Dict[str, Any]]]] = None

    def to_json_dict(self) -> dict:
        return convert_to_json_serializable(data=asdict(self))

    @property
    def id(self) -> str:
        return IDDict(self.to_json_dict()).to_id()
