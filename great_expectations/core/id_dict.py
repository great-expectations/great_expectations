import hashlib
import json
from typing import Any, Optional, Set


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

        _id_dict = {k: self[k] for k in id_keys}
        return hashlib.md5(
            json.dumps(_id_dict, sort_keys=True).encode("utf-8")
        ).hexdigest()

    @staticmethod
    def convert_dictionary_to_id_dict(data: Optional[Any]):
        """
        This method converts any nested "data" argument of dictionary or iterable type to "IDDict" object (recursively).
        """
        if isinstance(data, dict):
            data = IDDict(data)

            key: str
            value: Any
            for key, value in data.items():
                data[key] = IDDict.convert_dictionary_to_id_dict(data=value)
        elif isinstance(data, (list, set, tuple)):
            data_type: type = type(data)

            value: Any  # type: ignore[no-redef]
            data = data_type(
                [IDDict.convert_dictionary_to_id_dict(data=value) for value in data]
            )

        return data

    def __hash__(self) -> int:  # type: ignore[override]
        """Overrides the default implementation"""
        _result_hash: int = hash(self.to_id())
        return _result_hash


class BatchKwargs(IDDict):
    pass


class BatchSpec(IDDict):
    pass


class MetricKwargs(IDDict):
    pass
