import hashlib
import json
from typing import Any, Set, TypeVar, Union

from great_expectations.core.util import convert_to_json_serializable

T = TypeVar("T")


class IDDict(dict):
    _id_ignore_keys: Set[str] = set()

    def to_id(self, id_keys=None, id_ignore_keys=None):
        if id_keys is None:
            id_keys = self.keys()
        if id_ignore_keys is None:
            id_ignore_keys = self._id_ignore_keys
        id_keys = set(id_keys) - set(id_ignore_keys)
        if len(id_keys) == 0:
            return tuple()
        elif len(id_keys) == 1:
            key = list(id_keys)[0]
            return f"{key}={str(self[key])}"

        _id_dict = convert_to_json_serializable(data={k: self[k] for k in id_keys})
        return hashlib.md5(
            json.dumps(_id_dict, sort_keys=True).encode("utf-8")
        ).hexdigest()

    def __hash__(self) -> int:  # type: ignore[override]
        """Overrides the default implementation"""
        _result_hash: int = hash(self.to_id())
        return _result_hash


def convert_dictionary_to_id_dict(source: Union[T, dict]) -> Union[T, IDDict]:
    if not isinstance(source, dict):
        return source

    return _convert_dictionary_to_id_dict(source=IDDict(source))


def _convert_dictionary_to_id_dict(source: dict) -> IDDict:
    key: str
    value: Any
    for key, value in source.items():
        if isinstance(value, dict):
            source[key] = _convert_dictionary_to_id_dict(source=value)
        elif isinstance(source, (list, set, tuple)):
            data_type: type = type(source)

            element: Any
            source = data_type(
                [_convert_dictionary_to_id_dict(source=element) for element in source]
            )

    return IDDict(source)


class BatchKwargs(IDDict):
    pass


class BatchSpec(IDDict):
    pass


class MetricKwargs(IDDict):
    pass
