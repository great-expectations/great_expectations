# TODO: <Alex>ALEX</Alex>
# from dataclasses import asdict, dataclass
from dataclasses import dataclass
from typing import Any, Dict, Optional, Union

from great_expectations.core import IDDict


@dataclass
class Parameter(IDDict):
    parameters: Dict[str, Any]
    details: Optional[Dict[str, Union[str, dict]]] = None

    # TODO: <Alex>ALEX</Alex>
    # def id(self) -> str:
    #     return IDDict(asdict(self)).to_id()
    def id(self) -> str:
        return self.to_id()
