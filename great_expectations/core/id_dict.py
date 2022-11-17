import hashlib
import json
from typing import Set

from great_expectations.core.util import convert_to_json_serializable


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


class BatchKwargs(IDDict):
    pass


class BatchSpec(IDDict):
    pass


class MetricKwargs(IDDict):
    pass
