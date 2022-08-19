"""Contains general abstract or base classes used across configuration objects."""
import uuid
from abc import ABC
from typing import Mapping, Optional, Type, Union

from great_expectations.marshmallow__shade.decorators import post_dump
from great_expectations.marshmallow__shade.schema import Schema
from great_expectations.types import SerializableDictDot

_NOT_PROVIDED = uuid.uuid4()


class AbstractConfig(ABC, SerializableDictDot):
    """Abstract base class for Config objects. Sets the fields that must be included on a Config."""

    _INIT_KEY_MAPPINGS = {"id": "id_"}

    def __init__(self, id_: Optional[str] = None, name: Optional[str] = None) -> None:
        self.id_ = id_
        self.name = name
        super().__init__()

    @classmethod
    def parse_obj(
        cls: Type["AbstractConfig"], obj: Union[dict, Mapping]
    ) -> "AbstractConfig":
        """
        Instantiate the class from a dictionary-like object.
        Translates the keys as needed into `__init__` compatible keyword arguments.

        No additonal logic is applied.
        """
        if not isinstance(obj, dict):
            try:
                obj = dict(obj)
            except (TypeError, ValueError):
                raise TypeError(
                    f"{cls.__name__} expected dict not {obj.__class__.__name__}"
                )

        for target_key, replacement_key in cls._INIT_KEY_MAPPINGS.items():
            # using magic string to ensure that we preserve the original value even if it was None or fasly.
            value = obj.pop(target_key, _NOT_PROVIDED)
            if value == _NOT_PROVIDED:
                continue
            obj[replacement_key] = value
        return cls(**obj)

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
