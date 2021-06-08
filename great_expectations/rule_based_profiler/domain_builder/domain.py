from typing import Any, Dict, Optional, Union

from great_expectations.core import IDDict
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.types.base import DotDict


# TODO: <Alex>ALEX -- multiple inheritance is to be used with caution (potentially fix to use single inheritance)</Alex>
class Domain(IDDict, DotDict):
    # Adding an explicit constructor to highlight the specific properties that will be used.
    def __init__(self, domain_kwargs: Optional[Dict[str, Any]] = None):
        domain_kwargs_dot_dict: DotDict = self._convert_dictionaries_to_dot_dicts(
            source=domain_kwargs
        )
        super().__init__(domain_kwargs=domain_kwargs_dot_dict)

    def to_json_dict(self) -> dict:
        return convert_to_json_serializable(data=self)

    @property
    # Adding this property for convenience (also, in the future, arguments may not be all set to their default values).
    def id(self) -> str:
        return self.to_id(id_keys=None, id_ignore_keys=None)

    def _convert_dictionaries_to_dot_dicts(
        self, source: Optional[Any] = None
    ) -> Optional[Union[Any, DotDict]]:
        if source is None:
            return None

        if isinstance(source, dict):
            if not isinstance(source, DotDict):
                source = DotDict(source)
            key: str
            value: Any
            for key, value in source.items():
                source[key] = self._convert_dictionaries_to_dot_dicts(source=value)

        return source
