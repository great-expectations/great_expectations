from dataclasses import dataclass, asdict
from typing import Dict, Any, Union, Optional

from great_expectations.core import IDDict


@dataclass
class Parameter:
    parameters: Dict[str, Any]
    details: Optional[Dict[str, Union[str, dict]]] = None

    def id(self) -> str:
        return IDDict(asdict(self)).to_id()