"""Contains general abstract or base classes used across configuration objects."""
from abc import ABC
from typing import Optional

from great_expectations.types import SerializableDictDot


class AbstractConfig(ABC, SerializableDictDot):
    """Abstract base class for Config objects. Sets the fields that must be included on a Config."""

    def __init__(self, id_: Optional[str] = None, name: Optional[str] = None) -> None:
        self.id_ = id_
        self.name = name
        super().__init__()

    def to_dict(self) -> dict:
        data = super().to_dict()
        for attr in ("id_", "name"):
            if data.get(attr) is None:
                data.pop(attr)
        return data

    def to_raw_dict(self) -> dict:
        data = super().to_raw_dict()
        for attr in ("id_", "name"):
            if data.get(attr) is None:
                data.pop(attr)
        return data

    def to_json_dict(self) -> dict:
        return self.to_dict()
