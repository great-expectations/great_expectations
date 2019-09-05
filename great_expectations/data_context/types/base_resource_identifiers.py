import logging
logger = logging.getLogger(__name__)

from collections import Iterable
from six import string_types, class_types

from great_expectations.types import (
    RequiredKeysDotDict,
    AllowedKeysDotDict,
)

# TODO: Move this to great_expectations.types.base.py
class OrderedKeysDotDict(AllowedKeysDotDict):
    """extends AllowedKeysDotDict with a strict ordering of parameters.
    This make OrderedKeysDotDict behave somehwat like collections.namedtuples.

    OrderedKeysDotDicts...
        have an exactly-defined number of elements
        can parse from tuples
        coerce types by default
        raise an IndexError if args don't line up with keys

    See tests for examples.

    Lining up args with keys can be complex when key_types allow nesting.
    This logic is built into _zip_keys_and_args_to_dict.
    Again, see tests for examples.
    """

    _key_order = []

    def __init__(self, *args, **kwargs):
        logger.debug(self.__class__.__name__)
        assert set(self._key_order) == set(self._allowed_keys)
        assert set(self._key_order) == set(self._required_keys)

        coerce_types = kwargs.pop("coerce_types", True),
        if args == ():
            super(OrderedKeysDotDict, self).__init__(
                coerce_types=coerce_types,
                *args, **kwargs
            )

        else:
            new_kwargs = self._zip_keys_and_args_to_dict(args)
            super(OrderedKeysDotDict, self).__init__(
                coerce_types=coerce_types,
                **new_kwargs
            )

    @classmethod
    def _zip_keys_and_args_to_dict(cls, args):

        index = 0
        new_dict = {}
        for key in cls._key_order:
            try:
                if isinstance(args[index], dict):
                    increment = 1
                    new_dict[key] = args[index]

                else:
                    key_type = cls._key_types.get(key, None)
                    if isinstance(key_type, class_types) and issubclass(key_type, DataContextResourceIdentifier):
                        increment = key_type._recursively_get_key_length()
                        new_dict[key] = args[index:index + increment]
                    else:
                        increment = 1
                        new_dict[key] = args[index]

            except IndexError as e:
                raise IndexError("Not enough arguments in {0} to populate all fields in {1} : {2}".format(
                    args,
                    cls.__name__,
                    cls._key_order,
                ))

            index += increment

        if index < len(args):
            raise IndexError("Too many arguments in {0} to populate the fields in {1} : {2}".format(
                args,
                cls.__name__,
                cls._key_order,
            ))

        return new_dict

    @classmethod
    def _recursively_get_key_length(cls):
        key_length = 0

        for key in cls._key_order:
            key_type = cls._key_types.get(key, None)
            if isinstance(key_type, class_types) and issubclass(key_type, DataContextResourceIdentifier):
                key_length += key_type._recursively_get_key_length()
            else:
                key_length += 1

        return key_length


class DataContextResourceIdentifier(OrderedKeysDotDict):
    """is used to uniquely identify resources used by the DataContext.

    DataContextResourceIdentifier is based OrderedKeysDotDict.
    It extends the base class with a to_string method that converts
    the full, nested structure of the identifier into a string.

    For example:

        "DataAssetIdentifier.my_db.default_generator.my_table"
        "ValidationResultIdentifier.my_db.default_generator.my_table.default_expectations.warnings.prod.20190801"

    These strings are also used for hashing, so that DataContextResourceIdentifiers can be used in sets, etc.

    The parse_string_to_data_context_resource_identifier convenience method great_expectations.util
    can instantiate a valid identifier from any full identifier string.


    Notes on usage and convention:
    * DataContextResourceIdentifiers define the "namespace" of the DataContext.
    * In an import dependency sense, these DCRIs exist prior to the DataContext class: DataContext imports and makes use of many Ids.
    * That said, DCRIs exist primarily to make possible the work of the DataContext, so it's hard to separate the concepts cleanly.

    * DCRIs the internal typing system for DataContexts.
    * Within the DataContext (and related classes, like DataSources, Actions, Stores, etc.), concepts that can be typed as existing DCRIs should always be cast to DCRI format---never strings or dictionaries.
    * Methods that expect a DCRI should check types using IsInstance at the entry point when they receive the input.
    * In particular, most ReadWriteStores are usually bound tightly to DCRIs. (WriteOnlyStores
    * If you're reading something from a Store and it's not keyed on a DCRI, it probably should be.

    Note on typing in python in general:
    We sometimes joke that adding types is turning the Great Expectations codebase into Java.
    This may feel un-pythonic, but for the core classes and abstractions of the library, 
    we judge that the purpose of the code is closer to software engineering than data manipulation and analysis.
    In that context, typing is far more helpful than harmful.

    Note on transition plans:
    * The OrderedKeysDotDict class is a homegrown typing system that we regard as a transitionary state.
    * When we deprecate python 2 (and possibly sooner), this class will be replaced by a more 
    """

    def __init__(self, *args, **kwargs):
        # TODO : Pull out all of this logic into a `from_string` classmethod:
        from_string = kwargs.pop("from_string", None)

        if from_string == None:
            super(DataContextResourceIdentifier, self).__init__(
                *args, **kwargs
            )

        else:
            # /END TODO
            super(DataContextResourceIdentifier, self).__init__(
                *from_string.split(".")[1:],
                **kwargs
            )

    # TODO : Change this to __str__
    def to_string(self, include_class_prefix=True, separator="."):
        return separator.join(self._get_string_elements(include_class_prefix))

    # NOTE: This logic has been pulled into NamespacedReadWriteStore.
    # I'm not sure if we should keep a copy here.
    def _get_string_elements(self, include_class_prefix=True):
        string_elements = []

        if include_class_prefix:
            string_elements.append(self.__class__.__name__)

        for key in self._key_order:
            if isinstance(self[key], DataContextResourceIdentifier):
                string_elements += self[key]._get_string_elements(include_class_prefix=False)
            else:
                string_elements.append(str(self[key]))

        return string_elements

    # This is required to make DataContextResourceIdentifiers hashable
    def __hash__(self):
        return hash(self.to_string())

    def __eq__(self, other):
        return self.__hash__() == other.__hash__()
