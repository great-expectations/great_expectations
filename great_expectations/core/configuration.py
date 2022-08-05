"""Contains general abstract or base classes used across configuration objects."""
import copy
from abc import ABC
from typing import Optional

from great_expectations.marshmallow__shade.decorators import post_dump
from great_expectations.marshmallow__shade.schema import Schema
from great_expectations.types import SerializableDictDot


class AbstractConfig(ABC, SerializableDictDot):
    """Abstract base class for Config objects. Sets the fields that must be included on a Config."""

    def __init__(self, id_: Optional[str] = None, name: Optional[str] = None) -> None:
        self.id_ = id_
        self.name = name
        super().__init__()


class AbstractConfigSchema(Schema):
    REMOVE_KEYS_IF_NONE = ["id", "name"]

    @post_dump
    def remove_keys_if_none(self, data: dict, **kwargs) -> dict:
        data = copy.deepcopy(data)
        for key in AbstractConfigSchema.REMOVE_KEYS_IF_NONE:
            if key in data and data[key] is None:
                data.pop(key)
        return data
