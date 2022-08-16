"""Contains general abstract or base classes used across configuration objects."""
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

    @classmethod
    def _dict_round_trip(cls, schema: Schema, target: dict) -> dict:
        """
        Round trip a dictionary with a schema so that validation and serialization logic is applied.

        Example: Loading a config with a `_id` field but serializing it as `id`.
        """
        _loaded = schema.load(target)
        _config = cls(**_loaded)
        return _config.to_json_dict()


class AbstractConfigSchema(Schema):
    REMOVE_KEYS_IF_NONE = ["id", "name"]

    @post_dump
    def filter_none(self, data: dict, **kwargs) -> dict:
        return {
            key: value
            for key, value in data.items()
            if key not in AbstractConfigSchema.REMOVE_KEYS_IF_NONE or value is not None
        }
