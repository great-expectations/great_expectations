# TODO: <Alex>ALEX</Alex>
# import json
import copy
# import sys
from enum import Enum
from typing import Set, Optional, Any
# TODO: <Alex>ALEX</Alex>
# import datetime
# import uuid
# import decimal
# TODO: <Alex>ALEX</Alex>

# TODO: <Alex>ALEX</Alex>
# import numpy as np
# import pandas as pd
# TODO: <Alex>ALEX</Alex>

# TODO: <Alex>ALEX</Alex>
# import logging
# TODO: <Alex>ALEX</Alex>

from .base import SerializableDotDict
from .configurations import ClassConfig
# TODO: <Alex>ALEX</Alex>
# TODO: <Alex>ALEX_TEST -- circular import</Alex>
# from ..core import RunIdentifier
# TODO: <Alex>ALEX</Alex>
# from ..util import safe_deep_copy, deep_filter_properties_iterable
# TODO: <Alex>ALEX</Alex>

# TODO: <Alex>ALEX</Alex>
# logger = logging.getLogger(__name__)
# TODO: <Alex>ALEX</Alex>

# TODO: <Alex>ALEX</Alex>
#
# try:
#     import sqlalchemy
#     from sqlalchemy.engine.row import LegacyRow
# except ImportError:
#     sqlalchemy = None
#     LegacyRow = None
#     logger.debug("Unable to load SqlAlchemy or one of its subclasses.")
#
# try:
#     import pyspark
# except ImportError:
#     pyspark = None
#     logger.debug(
#         "Unable to load pyspark; install optional spark dependency if you will be working with Spark dataframes"
#     )
# TODO: <Alex>ALEX</Alex>


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

    include_field_names: Set[str] = set()
    exclude_field_names: Set[str] = set()

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
        return self.to_raw_dict().keys()

    def values(self):
        return self.to_raw_dict().values()

    def items(self):
        return self.to_raw_dict().items()

    def get(self, key, default_value=None):
        if self.__contains__(key=key):
            return self.__getitem__(item=key)
        return self.__dict__.get(key, default_value)

    def to_raw_dict(self):
        """Convert this object into a standard dictionary, recursively.

        This is often convenient for serialization, and in cases where an untyped version of the object is required.
        """

        # print(f'\n[ALEX_TEST] [DICT_DOT:TO_RAW_DICT()] SELF.__DICT__: {self.__dict__} ; TYPE: {str(type(self.__dict__))}')
        # TODO: <Alex>ALEX</Alex>
        # new_dict = copy.deepcopy(self.__dict__)
        # TODO: <Alex>ALEX</Alex>
        # TODO: <Alex>ALEX</Alex>
        # TODO: <Alex>ALEX</Alex>
        new_dict = _safe_deep_copy(data=self.__dict__)
        # TODO: <Alex>ALEX</Alex>
        # TODO: <Alex>ALEX</Alex>

        # This is needed to play nice with pydantic.
        if "__initialised__" in new_dict:
            del new_dict["__initialised__"]

        # DictDot's to_raw_dict method works recursively, when a DictDot contains other DictDots.
        for key, value in new_dict.items():
            # Recursive conversion works on keys that are DictDots...
            if isinstance(value, DictDot):
                new_dict[key] = value.to_raw_dict()

            # ...and Enums...
            elif isinstance(value, Enum):
                new_dict[key] = value.value

            # ...and when DictDots and Enums are nested one layer deeper in lists or tuples
            if isinstance(value, list) or isinstance(value, tuple):
                new_dict[key] = [temp_element for temp_element in value]
                for i, element in enumerate(value):
                    if isinstance(element, DictDot):
                        new_dict[key][i] = element.to_raw_dict()
                    elif isinstance(element, Enum):
                        new_dict[key][i] = element.value

            # Note: conversion will not work automatically if there are additional layers in between.

        return new_dict

    def to_dict(self) -> dict:
        new_dict = {
            key: self[key] for key in self.property_names(
                include_keys=self.include_field_names,
                exclude_keys=self.exclude_field_names,
            )
        }
        for key, value in new_dict.items():
            if isinstance(value, DictDot):
                new_dict[key] = value.to_dict()
            elif isinstance(value, Enum):
                new_dict[key] = value.value

            if isinstance(value, list) or isinstance(value, tuple):
                new_dict[key] = [temp_element for temp_element in value]
                for i, element in enumerate(value):
                    if isinstance(element, DictDot):
                        new_dict[key][i] = element.to_dict()
                    elif isinstance(element, Enum):
                        new_dict[key][i] = element.value

        print(f'\n[ALEX_TEST] [DICT_DOT:TO_DICT()] NEW_DICT: {new_dict} ; TYPE: {str(type(new_dict))}')
        return new_dict

    # def to_dict(self) -> dict:
    #     # TODO: <Alex>ALEX</Alex>
    #     # dict_obj: dict = {}
    #     # TODO: <Alex>ALEX</Alex>
    #
    #     name: str
    #     # TODO: <Alex>ALEX</Alex>
    #     # field_value: Any
    #     # for field_name in self.field_names:
    #     #     if hasattr(self, f"_{field_name}"):
    #     #         field_value = getattr(self, field_name)
    #     #         dict_obj[field_name] = field_value
    #     # TODO: <Alex>ALEX</Alex>
    #
    #     # TODO: <Alex>ALEX</Alex>
    #     dict_obj: dict = {name: self[name] for name in self.field_names()}
    #     # TODO: <Alex>ALEX</Alex>
    #     # return dict_obj
    #     # TODO: <Alex>ALEX</Alex>
    #     # TODO: <Alex>ALEX</Alex>
    #     deep_filter_properties_iterable(properties=dict_obj, inplace=True)
    #     return dict_obj
    #     # TODO: <Alex>ALEX</Alex>

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

        property_names: Set[str] = set(
            filter(
                lambda name: len(name) > 1,
                [
                    key[1:] for key in self.keys() if key[0] == "_"
                ],
            )
        )
        # print(f'\n[ALEX_TEST] [DICT_DOT:PROPERTY_NAMES()] SELF: TYPE: {str(type(self))}')
        # print(f'\n[ALEX_TEST] [DICT_DOT:PROPERTY_NAMES()] INCLUDE_KEYS: {include_keys} ; TYPE: {str(type(include_keys))}')
        # print(f'\n[ALEX_TEST] [DICT_DOT:PROPERTY_NAMES()] EXCLUDE_KEYS: {exclude_keys} ; TYPE: {str(type(exclude_keys))}')
        # print(f'\n[ALEX_TEST] [DICT_DOT:PROPERTY_NAMES()] PROPERTY_NAMES: {property_names} ; TYPE: {str(type(property_names))}')

        keys_for_exclusion: list = []

        if include_keys:
            # Make sure that all properties, marked for inclusion, actually exist on the object.
            for key in include_keys:
                try:
                    _ = self[key]
                except AttributeError:
                    raise ValueError(
                        f'Property "{key}", marked for inclusion on object "{str(type(self))}", does not exist.'
                    )

            keys_for_exclusion.extend(
                [key for key in property_names if key not in include_keys]
            )

        if exclude_keys:
            # Make sure that all properties, marked for exclusion, actually exist on the object.
            for key in exclude_keys:
                try:
                    _ = self[key]
                except AttributeError:
                    raise ValueError(
                        f'Property "{key}", marked for exclusion on object "{str(type(self))}", does not exist.'
                    )

            keys_for_exclusion.extend(
                [key for key in property_names if key in exclude_keys]
            )

        keys_for_exclusion = list(set(keys_for_exclusion))

        # TODO: <Alex>ALEX</Alex>
        a = {key for key in property_names if key not in keys_for_exclusion}
        # print(f'\n[ALEX_TEST] [DICT_DOT:PROPERTY_NAMES()] REMAINING_FILTERED_FIELDS: {a} ; TYPE: {str(type(a))}')
        # TODO: <Alex>ALEX</Alex>
        return {
            key for key in property_names if key not in keys_for_exclusion
        }

    # TODO: <Alex>ALEX_TEST -- COULD_BE_SUBCLASS</Alex>
    # def __deepcopy__(self, memo):
    #     cls = self.__class__
    #     result = cls.__new__(cls)
    #
    #     memo[id(self)] = result
    #     for key, value in self.to_raw_dict().items():
    #         value_copy = _safe_deep_copy(data=value, memo=memo)
    #         setattr(result, key, value_copy)
    #
    #     return result
    # TODO: <Alex>ALEX</Alex>


# TODO: <Alex>ALEX_TEST -- put a comment here how a reference implementation is provided, which can be overridden.</Alex>
class SerializableDictDot(DictDot):
    # TODO: <Alex>ALEX</Alex>
    def to_json_dict(self) -> dict:
        raise NotImplementedError
    # TODO: <Alex>ALEX</Alex>
    # TODO: <Alex>ALEX_TEST -- SUBCLASS (with note)</Alex>
    # def to_json_dict(self) -> dict:
    #     dict_obj: dict = self.to_dict()
    #     serializeable_dict: dict = SerializableDictDot._convert_to_json_serializable(data=dict_obj)
    #     return serializeable_dict
    #
    # TODO: <Alex>ALEX_TEST -- SUBCLASS (with note)</Alex>
    # def __repr__(self) -> str:
    #     json_dict: dict = self.to_json_dict()
    #     deep_filter_properties_iterable(
    #         properties=json_dict,
    #         inplace=True,
    #     )
    #     return json.dumps(json_dict, indent=2)
    #
    # def __str__(self) -> str:
    #     return self.__repr__()

    # TODO: <Alex>ALEX_TEST -- circular import -- maybe implement a lighter-weight version only focused on dataframes?</Alex>
    # TODO: <Alex>ALEX_TEST -- Or just abandon having a default implementation and push it down to subclasses (and make a comment that this could be done in parent class, but circular imports.</Alex>
    # TODO: <Alex>ALEX_TEST -- put a comment here that this is a temporary copy from great_expectations/core/util.py until circular dependencies are resolved.</Alex>
    # @staticmethod
    # def _convert_to_json_serializable(data):
    #     """
    #     Helper function to convert an object to one that is json serializable
    #     Args:
    #         data: an object to attempt to convert a corresponding json-serializable object
    #     Returns:
    #         (dict) A converted test_object
    #     Warning:
    #         test_obj may also be converted in place.
    #     """
    #
    #     # If it's one of our types, we use our own conversion; this can move to full schema
    #     # once nesting goes all the way down
    #     if isinstance(data, (SerializableDictDot, SerializableDotDict)):
    #         return data.to_json_dict()
    #
    #     # Handling "float(nan)" separately is required by Python-3.6 and Pandas-0.23 versions.
    #     if isinstance(data, float) and np.isnan(data):
    #         return None
    #
    #     if isinstance(data, (str, int, float, bool)):
    #         # No problem to encode json
    #         return data
    #
    #     if isinstance(data, dict):
    #         new_dict = {}
    #         for key in data:
    #             # A pandas index can be numeric, and a dict key can be numeric, but a json key must be a string
    #             new_dict[str(key)] = SerializableDictDot._convert_to_json_serializable(data[key])
    #
    #         return new_dict
    #
    #     if isinstance(data, (list, tuple, set)):
    #         new_list = []
    #         for val in data:
    #             new_list.append(SerializableDictDot._convert_to_json_serializable(val))
    #
    #         return new_list
    #
    #     if isinstance(data, (np.ndarray, pd.Index)):
    #         # test_obj[key] = test_obj[key].tolist()
    #         # If we have an array or index, convert it first to a list--causing coercion to float--and then round
    #         # to the number of digits for which the string representation will equal the float representation
    #         return [SerializableDictDot._convert_to_json_serializable(x) for x in data.tolist()]
    #
    #     if isinstance(data, np.int64):
    #         return int(data)
    #
    #     if isinstance(data, np.float64):
    #         return float(data)
    #
    #     if isinstance(data, (datetime.datetime, datetime.date)):
    #         return data.isoformat()
    #
    #     if isinstance(data, uuid.UUID):
    #         return str(data)
    #
    #     # Use built in base type from numpy, https://docs.scipy.org/doc/numpy-1.13.0/user/basics.types.html
    #     # https://github.com/numpy/numpy/pull/9505
    #     if np.issubdtype(type(data), np.bool_):
    #         return bool(data)
    #
    #     if np.issubdtype(type(data), np.integer) or np.issubdtype(type(data), np.uint):
    #         return int(data)
    #
    #     if np.issubdtype(type(data), np.floating):
    #         # Note: Use np.floating to avoid FutureWarning from numpy
    #         return float(round(data, sys.float_info.dig))
    #
    #     # Note: This clause has to come after checking for np.ndarray or we get:
    #     #      `ValueError: The truth value of an array with more than one element is ambiguous. Use a.any() or a.all()`
    #     if data is None:
    #         # No problem to encode json
    #         return data
    #
    #     try:
    #         if not isinstance(data, list) and pd.isna(data):
    #             # pd.isna is functionally vectorized, but we only want to apply this to single objects
    #             # Hence, why we test for `not isinstance(list)`
    #             return None
    #     except TypeError:
    #         pass
    #     except ValueError:
    #         pass
    #
    #     if isinstance(data, pd.Series):
    #         # Converting a series is tricky since the index may not be a string, but all json
    #         # keys must be strings. So, we use a very ugly serialization strategy
    #         index_name = data.index.name or "index"
    #         value_name = data.name or "value"
    #         return [
    #             {
    #                 index_name: SerializableDictDot._convert_to_json_serializable(idx),
    #                 value_name: SerializableDictDot._convert_to_json_serializable(val),
    #             }
    #             for idx, val in data.iteritems()
    #         ]
    #
    #     if isinstance(data, pd.DataFrame):
    #         return SerializableDictDot._convert_to_json_serializable(data.to_dict(orient="records"))
    #
    #     if pyspark and isinstance(data, pyspark.sql.DataFrame):
    #         # using StackOverflow suggestion for converting pyspark df into dictionary
    #         # https://stackoverflow.com/questions/43679880/pyspark-dataframe-to-dictionary-columns-as-keys-and-list-of-column-values-ad-di
    #         return SerializableDictDot._convert_to_json_serializable(
    #             dict(zip(data.schema.names, zip(*data.collect())))
    #         )
    #
    #     # SQLAlchemy serialization
    #     if LegacyRow and isinstance(data, LegacyRow):
    #         return dict(data)
    #
    #     if isinstance(data, decimal.Decimal):
    #         if SerializableDictDot._requires_lossy_conversion(data):
    #             logger.warning(
    #                 f"Using lossy conversion for decimal {data} to float object to support serialization."
    #             )
    #         return float(data)
    #
    #     # TODO: <Alex>ALEX_TEST -- circular import</Alex>
    #     # if isinstance(data, RunIdentifier):
    #     #     return data.to_json_dict()
    #     # TODO: <Alex>ALEX</Alex>
    #
    #     else:
    #         raise TypeError(
    #             "%s is of type %s which cannot be serialized."
    #             % (str(data), type(data).__name__)
    #         )
    #
    # # TODO: <Alex>ALEX_TEST -- put a comment here that this is a temporary copy from great_expectations/core/util.py until circular dependencies are resolved.</Alex>
    # @staticmethod
    # def _requires_lossy_conversion(d):
    #     return d - decimal.Context(prec=sys.float_info.dig).create_decimal(d) != 0


# TODO: <Alex>ALEX_TEST -- circular import (so keeping a copy)</Alex>
# TODO: <Alex>ALEX</Alex>
def _safe_deep_copy(data, memo=None):
    import pyspark
    import pandas as pd

    """
    This method makes a copy of a dictionary, applying deep copy to attribute values, except for non-pickleable objects.
    """
    if isinstance(data, (pd.Series, pd.DataFrame)) or (
        pyspark and isinstance(data, pyspark.sql.DataFrame)
    ):
        return data

    if isinstance(data, (list, tuple)):
        return [_safe_deep_copy(data=element, memo=memo) for element in data]

    if isinstance(data, dict):
        return {
            key: _safe_deep_copy(data=value, memo=memo) for key, value in data.items()
        }

    # noinspection PyArgumentList
    return copy.deepcopy(data, memo)
# TODO: <Alex>ALEX</Alex>

