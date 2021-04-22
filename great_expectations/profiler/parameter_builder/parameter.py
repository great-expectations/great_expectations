from dataclasses import asdict, dataclass
from typing import Any, Dict, Optional, Union

from great_expectations.core import IDDict
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.types import SerializableDictDot


# TODO: <Alex>ALEX -- The inheritance of DictDot is temporary.  It will be replaced by a DataClass in "develop", and then SerializableDictDot will be a DataClass with the to_jsono_dict() interface.  The present class will be updated, once these changes to "develop" are have been made.</Alex>
@dataclass
class Parameter(IDDict, SerializableDictDot):
    parameters: Dict[str, Any]
    details: Optional[Dict[str, Union[str, dict]]] = None

    # TODO: <Alex>ALEX -- parameter_id does not seem to be useful (commenting out for now, marking for deletion).</Alex>
    # @property
    # def id(self) -> str:
    #     return self.to_id()

    def to_json_dict(self) -> dict:
        return convert_to_json_serializable(data=asdict(self))
