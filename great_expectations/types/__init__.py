import copy
from enum import Enum

from .configurations import ClassConfig


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

    def __getitem__(self, item):
        if isinstance(item, int):
            return list(self.__dict__.keys())[item]
        return getattr(self, item)

    def __setitem__(self, key, value):
        setattr(self, key, value)

    def __delitem__(self, key):
        delattr(self, key)

    def __contains__(self, key):
        return hasattr(self, key)

    def __len__(self):
        return len(self.__dict__)

    def keys(self):
        return self.to_dict().keys()

    def values(self):
        return self.to_dict().values()

    def items(self):
        return self.to_dict().items()

    def get(self, key, default_value=None):
        if self.__contains__(key=key):
            return self.__getitem__(item=key)
        return self.__dict__.get(key, default_value)

    def to_dict(self):
        """Convert this object into a standard dictionary, recursively.

        This is often convenient for serialization, and in cases where an untyped version of the object is required.
        """

        new_dict = copy.deepcopy(self.__dict__)

        # This is needed to play nice with pydantic.
        if "__initialised__" in new_dict:
            del new_dict["__initialised__"]

        # DictDot's to_dict method works recursively, when a DictDot contains other DictDots.
        for key, value in new_dict.items():
            # Recursive conversion works on keys that are DictDots...
            if isinstance(value, DictDot):
                new_dict[key] = value.to_dict()

            # ...and Enums...
            elif isinstance(value, Enum):
                new_dict[key] = value.value

            # ...and when DictDots and Enums are nested one layer deeper in lists or tuples
            if isinstance(value, list) or isinstance(value, tuple):
                new_dict[key] = [temp_element for temp_element in value]
                for i, element in enumerate(value):
                    if isinstance(element, DictDot):
                        new_dict[key][i] = element.to_dict()
                    elif isinstance(element, Enum):
                        new_dict[key][i] = element.value

            # Note: conversion will not work automatically if there are additional layers in between.

        return new_dict


class SerializableDictDot(DictDot):
    def to_json_dict(self) -> dict:
        raise NotImplementedError
