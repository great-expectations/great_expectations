"""Contains general abstract or base classes used across configuration objects."""
from abc import ABC
from typing import Optional

from great_expectations.types import SerializableDictDot


class AbstractConfig(ABC, SerializableDictDot):
    """Abstract base class for Config objects. Sets the fields that must be included on a Config."""

    def __init__(self, id_: Optional[str] = None, name: Optional[str] = None) -> None:
        # Note: name and id are optional currently to avoid updating all documentation within
        # the scope of this work.
        if id_ is not None:
            self.id = id_
        if name is not None:
            self.name = name
        super().__init__()
