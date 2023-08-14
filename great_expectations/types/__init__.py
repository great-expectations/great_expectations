import copy
import logging
from enum import Enum
from typing import ClassVar, Dict, Optional, Set

import pandas as pd
import pydantic

from great_expectations.compatibility import pyspark

from ..alias_types import JSONValues
from ..core._docs_decorators import public_api
from .base import SerializableDotDict
from .colors import ColorPalettes, PrimaryColors, SecondaryColors, TintsAndShades
from .configurations import ClassConfig
from .fonts import FontFamily, FontFamilyURL

logger = logging.getLogger(__name__)


class DictDot:
    """A convenience class for migrating away from untyped dictionaries to stronger typed objects.

    Can be instantiated with arguments:

        my_A = MyClassA(
                foo="a string",
                bar=1,
            )

    Can be instantiated from a dictionary:

        my_A = MyClassA(
            **{
                "foo": "a string",
                "bar": 1,
            }
        )

    Can be accessed using both dictionary and dot notation

        my_A.foo == "a string"
        my_A.bar == 1

        my_A["foo"] == "a string"
        my_A["bar"] == 1

    Pairs nicely with @dataclass:

        @dataclass()
        class MyClassA(DictDot):
            foo: str
            bar: int

    Can be made immutable:

        @dataclass(frozen=True)
        class MyClassA(DictDot):
            foo: str
            bar: int

    For more examples of usage, please see `test_dataclass_serializable_dot_dict_pattern.py` in the tests folder.
    """

    include_field_names: ClassVar[Set[str]] = set()
    exclude_field_names: ClassVar[Set[str]] = set()

    def __getitem__(self, item):
        if isinstance(item, int):
            return list(self.__dict__.keys())[item]
        return getattr(self, item)

    def __setitem__(self, key, value) -> None:
        setattr(self, key, value)

    def __delitem__(self, key) -> None:
        delattr(self, key)

    def __contains__(self, key):
        return hasattr(self, key)

    def __len__(self):
        return len(self.__dict__)

    def keys(self):
        return self.__dict__.keys()

    def values(self):
        return self.to_raw_dict().values()

    def items(self):
        return self.to_raw_dict().items()

    def get(self, key, default_value=None):
        if self.__contains__(key=key):
            return self.__getitem__(item=key)
        return self.__dict__.get(key, default_value)

    def to_raw_dict(self) -> dict:
        """Convert this object into a standard dictionary, recursively.

        This is often convenient for serialization, and in cases where an untyped version of the object is required.
        """

        new_dict = safe_deep_copy(data=self.__dict__)

        # This is needed to play nice with pydantic.
        if "__initialised__" in new_dict:
            del new_dict["__initialised__"]

        # DictDot's to_raw_dict method works recursively, when a DictDot contains other DictDots.
        for key, value in new_dict.items():
            # Recursive conversion works on keys that are DictDots...
            if isinstance(value, DictDot):
                new_dict[key] = value.to_raw_dict()

            # ...and Enums...
            if isinstance(value, Enum):
                new_dict[key] = value.value

            # ...and when DictDots and Enums are nested one layer deeper in lists or tuples
            if isinstance(value, list) or isinstance(value, tuple):  # noqa: PLR1701
                new_dict[key] = [temp_element for temp_element in value]
                for i, element in enumerate(value):
                    if isinstance(element, DictDot):
                        new_dict[key][i] = element.to_raw_dict()

                    if isinstance(element, Enum):
                        new_dict[key][i] = element.value

            # Note: conversion will not work automatically if there are additional layers in between.

        return new_dict

    def to_dict(self) -> dict:
        new_dict = {
            key: self[key]
            for key in self.property_names(
                include_keys=self.include_field_names,
                exclude_keys=self.exclude_field_names,
            )
        }
        for key, value in new_dict.items():
            if isinstance(value, pydantic.BaseModel):
                new_dict[key] = value.dict()

            if isinstance(value, DictDot):
                new_dict[key] = value.to_dict()

            if isinstance(value, Enum):
                new_dict[key] = value.value

            if isinstance(value, list) or isinstance(value, tuple):  # noqa: PLR1701
                new_dict[key] = [temp_element for temp_element in value]
                for i, element in enumerate(value):
                    if isinstance(value, pydantic.BaseModel):
                        new_dict[key][i] = element.dict()

                    if isinstance(element, DictDot):
                        new_dict[key][i] = element.to_dict()

                    if isinstance(element, Enum):
                        new_dict[key][i] = element.value

        return new_dict

    def property_names(
        self,
        include_keys: Optional[Set[str]] = None,
        exclude_keys: Optional[Set[str]] = None,
    ) -> Set[str]:
        """
        Assuming that -- by convention -- names of private properties of an object are prefixed by "_" (a single
        underscore character), return these property names as public property names.  To support this convention, the
        extending classes must implement property accessors, corresponding to the property names, return by this method.

        :param include_keys: inclusion list ("include only these properties, while excluding all the rest")
        :param exclude_keys: exclusion list ("exclude only these properties, while include all the rest")
        :return: property names, subject to inclusion/exclusion filtering
        """
        if include_keys is None:
            include_keys = set()

        if exclude_keys is None:
            exclude_keys = set()

        if include_keys & exclude_keys:
            raise ValueError(
                "Common keys between sets of include_keys and exclude_keys filtering directives are illegal."
            )

        key: str

        # Gather private fields:
        # By Python convention, properties of non-trivial length, prefixed by underscore ("_") character, are private.
        private_fields: Set[str] = set(
            filter(
                lambda name: len(name) > 1,
                [key[1:] for key in self.keys() if key[0] == "_"],
            )
        )
        # Gather public fields.
        public_fields: Set[str] = {key for key in self.keys() if key[0] != "_"}
        # Combine private and public fields using the "Set Union" operation.
        property_names: Set[str] = public_fields | private_fields

        keys_for_exclusion: list = []

        def assert_valid_keys(keys: Set[str], purpose: str) -> None:
            name: str
            for name in keys:
                try:
                    _ = self[name]
                except AttributeError:
                    try:
                        _ = self[f"_{name}"]
                    except AttributeError:
                        raise ValueError(
                            f'Property "{name}", marked for {purpose} on object "{str(type(self))}", does not exist.'
                        )

        if include_keys:
            # Make sure that all properties, marked for inclusion, actually exist on the object.
            assert_valid_keys(keys=include_keys, purpose="inclusion")
            keys_for_exclusion.extend(
                [key for key in property_names if key not in include_keys]
            )

        if exclude_keys:
            # Make sure that all properties, marked for exclusion, actually exist on the object.
            assert_valid_keys(keys=exclude_keys, purpose="exclusion")
            keys_for_exclusion.extend(
                [key for key in property_names if key in exclude_keys]
            )

        keys_for_exclusion = list(set(keys_for_exclusion))

        return {key for key in property_names if key not in keys_for_exclusion}


class SerializableDictDot(DictDot):
    def to_json_dict(self) -> Dict[str, JSONValues]:
        """Returns a JSON-serializable dict representation of the SerializableDictDot.

        Subclasses must implement this abstract method.

        Returns:
            A JSON-serializable dict representation of the SerializableDictDot
        """

        # TODO: <Alex>2/4/2022</Alex>
        # A reference implementation can be provided, once circular import dependencies, caused by relative locations of
        # the "great_expectations/types/__init__.py" and "great_expectations/core/util.py" modules are resolved.
        raise NotImplementedError


def safe_deep_copy(data, memo=None):
    """
    This method makes a copy of a dictionary, applying deep copy to attribute values, except for non-pickleable objects.
    """
    if isinstance(data, (pd.Series, pd.DataFrame)) or (
        pyspark.pyspark and isinstance(data, pyspark.DataFrame)
    ):
        return data

    if isinstance(data, (list, tuple)):
        return [safe_deep_copy(data=element, memo=memo) for element in data]

    if isinstance(data, dict):
        return {
            key: safe_deep_copy(data=value, memo=memo) for key, value in data.items()
        }

    # noinspection PyArgumentList
    return copy.deepcopy(data, memo)
