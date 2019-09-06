import logging
logger = logging.getLogger(__name__)

from collections import Iterable
from six import string_types, class_types

from great_expectations.types import (
    RequiredKeysDotDict,
    AllowedKeysDotDict,
    OrderedKeysDotDict,
)

class DataContextKey(AllowedKeysDotDict):
    """is used to uniquely identify resources used by the DataContext.

    DataContextKey is based OrderedKeysDotDict.
    It extends the base class with a to_string method that converts
    the full, nested structure of the identifier into a string.

    For example:

        "DataAssetIdentifier.my_db.default_generator.my_table"
        "ValidationResultIdentifier.my_db.default_generator.my_table.default_expectations.warnings.prod.20190801"

    These strings are also used for hashing, so that DataContextKey can be used in sets, etc.

    The parse_string_to_data_context_resource_identifier convenience method great_expectations.util
    can instantiate a valid identifier from any full identifier string.


    Notes on usage and convention:
    DataContextKeys define the "namespace" of the DataContext.
    In an import dependency sense, these Keys exist prior to the DataContext class: DataContext imports and makes use of many Ids.
    That said, DataContextKeys exist primarily to make possible the work of the DataContext, so it's hard to separate the concepts cleanly.

    DataContextKeys are the internal typing system for DataContexts.
    Within the DataContext (and related classes, like DataSources, Actions, Stores, etc.), concepts that can be typed as existing DataContextKeys should always be cast to DataContextKeys format---never strings or dictionaries.
    Methods that expect a DataContextKeys should check types using isinstance at the entry point when they receive the input.
    In particular, most ReadWriteStores are usually bound tightly to DataContextKeys. (This is not necessarily true for WriteOnlyStores, which might be creating un-typed data, such as HTML.)
    If you're reading something from a Store and it's not keyed on a DataContextKeys, it probably should be.

    Note on typing in python in general:
    We sometimes joke that adding types is turning the Great Expectations codebase into Java.
    This may feel un-pythonic, but for the core classes and abstractions of the library, 
    we judge that the purpose of the code is closer to software engineering than data manipulation and analysis.
    In that context, typing is far more helpful than harmful.

    Note on transition plans:
    * The OrderedKeysDotDict class is a homegrown typing system that we regard as a transitionary state.
    * When we deprecate python 2 (and possibly sooner), this class will be replaced by a more standard approach to typing.
    """

    pass

class OrderedDataContextKey(DataContextKey, OrderedKeysDotDict):
    """
    """

    def __init__(self, *args, **kwargs):
        # TODO : Pull out all of this logic into a `from_string` classmethod:
        from_string = kwargs.pop("from_string", None)

        if from_string == None:
            super(DataContextKey, self).__init__(
                *args, **kwargs
            )

        else:
            # /END TODO
            super(DataContextKey, self).__init__(
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
            if isinstance(self[key], DataContextKey):
                string_elements += self[key]._get_string_elements(include_class_prefix=False)
            else:
                string_elements.append(str(self[key]))

        return string_elements

    # This is required to make OrderedDataContextKeys hashable
    def __hash__(self):
        return hash(self.to_string())

    def __eq__(self, other):
        return self.__hash__() == other.__hash__()

