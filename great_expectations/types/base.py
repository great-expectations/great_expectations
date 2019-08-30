import logging
logger = logging.getLogger(__name__)

from collections import Iterable
import inspect
import copy
from six import string_types

from ruamel.yaml import YAML, yaml_object
yaml = YAML()


class ListOf(object):
    def __init__(self, type_):
        self.type_ = type_

class DictOf(object):
    def __init__(self, type_):
        self.type_ = type_


@yaml_object(yaml)
class DotDict(dict):
    """This class provides dot.notation dot.notation access to dictionary attributes.

    It is also serializable by the ruamel.yaml library used in Great Expectations for managing
    configuration objects.
    """

    def __getattr__(self, item):
        return self.get(item)

    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__

    def __dir__(self):
        return self.keys()

    # Cargo-cultishly copied from: https://github.com/spindlelabs/pyes/commit/d2076b385c38d6d00cebfe0df7b0d1ba8df934bc
    def __deepcopy__(self, memo):
        return DotDict([(copy.deepcopy(k, memo), copy.deepcopy(v, memo)) for k, v in self.items()])

    # The following are required to support yaml serialization, since we do not raise
    # AttributeError from __getattr__ in DotDict. We *do* raise that AttributeError when it is possible to know
    # a given attribute is not allowed (because it's not in _allowed_keys)
    _yaml_merge = []

    @classmethod
    def yaml_anchor(cls):
        # This is required since our dotdict allows *any* access via dotNotation, blocking the normal
        # behavior of raising an AttributeError when trying to access a nonexistent function
        return None

    @classmethod
    def to_yaml(cls, representer, node):
        """Use dict representation for DotDict (and subtypes by default)"""
        return representer.represent_dict(node)


@yaml_object(yaml)
class RequiredKeysDotDict(DotDict):
    """RequiredKeysDotDict adds support for _required_keys and _key_types to DotDict, making it useful for defining
    serializable dictionary-like types for GE objects.

    _required_keys must be a *set* of key names that *must* be present. Any key is allowed.
    _key_types is a dictionary of key_name: key_type pairs that define required types for keys. Note that key_types
        *may* include keys that are not required.

    Consequently, this class is generally useless on its own.
    You need to subclass it like so:

    # This class should be yaml-serializable
    @yaml_object(yaml)
    class MyAllowedKeysDotDict(RequiredKeysDotDict):
        # Keys "x", "y", and "z" MUST be present
        _required_keys = { "x", "y", "z" }
        # The value for key "y" MUST be a RequiredKeysDotDict
        _key_types = {
            "x": RequiredKeysDotDict
        }
    """
    _required_keys = set()
    _key_types = {}

    def __init__(self, *args, **kwargs):
        """Build a new RequiredKeysDotDict.

        Args:
            *args: A dictionary or RequiredKeysDotDict from which to build this RequiredKeysDotDict
            coerce_types (boolean): whether or not to attempt to coerce objects in the constructor to the types
                required by the _key_types dictionary.
            **kwargs: Additional key-value pairs to be included in the RequiredKeysDotDict
        """
        # Support PY2 by leaving coerce_types out of explicit params list
        coerce_types = kwargs.pop("coerce_types", False)
        super(RequiredKeysDotDict, self).__init__(*args, **kwargs)
        for key in self._required_keys:
            if key not in self.keys():
                raise KeyError("key: {!r} is missing even though it's in the required keys: {!r}".format(
                    key,
                    self._required_keys
                ))

        for key, value in self.items():
            if key in self._key_types:
                if coerce_types:
                    value = self._coerce_complex_value_to_type(value, self._key_types[key])

                # Validate types
                self._validate_value_type(key, value, self._key_types[key])

            self[key] = value

    def __setitem__(self, key, val):
        if key in self._key_types:
            self._validate_value_type(key, val, self._key_types[key])

        dict.__setitem__(self, key, val)

    __setattr__ = __setitem__

    def __delitem__(self, key):
        if key in self._required_keys:
            raise KeyError("key: {!r} cannot be deleted because it's in the required keys: {!r}".format(
                    key,
                    self._required_keys
                ))
        else:
            dict.__delitem__(self, key)

    __delattr__ = __delitem__

    def _validate_value_type(self, key, value, type_):
        # TODO: Catch errors and raise more informative error messages here
        if isinstance(type_, ListOf):
            if not isinstance(value, Iterable):
                raise TypeError("key: {!r} must be an Iterable type, not {!r}".format(
                    key,
                    type(value),
                ))

            for v in value:
                if not isinstance(v, type_.type_):
                    raise TypeError("values in key: {!r} must be of type: {!r}, not {!r} {!r}".format(
                        key,
                        type_.type_,
                        v,
                        type(v),
                    ))

        elif isinstance(type_, DictOf):
            if not isinstance(value, dict):
                raise TypeError("key: {!r} must be a mapping, not {!r}".format(
                    key,
                    type(value),
                ))

            for k, v in value.items():
                if not isinstance(v, type_.type_):
                    raise TypeError("values in key: {!r} must be of type: {!r}, not {!r} {!r}".format(
                        key,
                        type_.type_,
                        v,
                        type(v),
                    ))

        else:
            if isinstance(type_, list):
                any_match = False
                for type_element in type_:
                    if type_element == None:
                        if value == None:
                            any_match = True
                    elif isinstance(value, type_element):
                        any_match = True

                if not any_match:
                    raise TypeError("key: {!r} must be of type {!r}, not {!r}".format(
                        key,
                        type_,
                        type(value),
                    ))

            else:
                if not isinstance(value, type_):
                    raise TypeError("key: {!r} must be of type {!r}, not {!r}".format(
                        key,
                        type_,
                        type(value),
                    ))
    
    def _coerce_complex_value_to_type(self, value, type_):
        # logger.debug("RequiredKeysDotDict._coerce_complex_value_to_type")

        # If the given type is an instance of RequiredKeysDotDict, apply coerce_types recursively
        try:
            if isinstance(type_, ListOf):
                if inspect.isclass(type_.type_) and issubclass(type_.type_,
                                                                                RequiredKeysDotDict):
                    value = [type_.type_(coerce_types=True, **v) for v in value]
                else:
                    value = [
                        self._coerce_simple_value_to_type(v, type_.type_)
                        for v in value
                    ]

            elif isinstance(type_, DictOf):
                if inspect.isclass(type_.type_) and issubclass(type_.type_,
                                                                                RequiredKeysDotDict):
                    value = dict([(k, type_.type_(coerce_types=True, **v)) for k, v in value.items()])
                else:
                    value = dict([
                        (k, self._coerce_simple_value_to_type(v, type_.type_))
                        for k, v in value.items()
                    ])

            else:
                if inspect.isclass(type_) and issubclass(type_, RequiredKeysDotDict):
                    if type(value) == tuple:
                        value = type_(coerce_types=True, *value)
                    else:
                        try:
                            value = type_(coerce_types=True, **value)
                        except TypeError as e:
                            raise TypeError(type_.__name__ + " cannot accept " + str(value) + " as input.")
                else:
                    value = self._coerce_simple_value_to_type(value, type_)

        except TypeError as e:
            raise TypeError("Unable to initialize " + self.__class__.__name__ + ".\n\tTypeError raised: " + str(e))

        return value

    def _coerce_simple_value_to_type(self, value, type_):
        """Convenience method to handle the case where type_ == string type, and any other similarly weird things in the future
        """
        # logger.debug("RequiredKeysDotDict._coerce_simple_value_to_type")

        if type_ == string_types:
            return str(value)

        else:
            return type_(value)


@yaml_object(yaml)
class AllowedKeysDotDict(RequiredKeysDotDict):
    """AllowedKeysDotDict adds support an _allowed_keys set to the RequiredKeysDotDict, limiting the total set
    of keys that may be in the dictionary. It should be used when the type requirements are stronger than
    RequiredKeysDotDict

    _allowed_keys must be a *set* of key names that *may* be present. No other key is allowed.
    _required_keys must be a *set* of key names that *must* be present.
    _key_types is a dictionary of key_name: key_type pairs that define required types for keys. Note that key_types
        *may* include keys that are not required.

    Consequently, this class is generally useless on its own.
    You need to subclass it like so:

    # This class should be yaml-serializable
    @yaml_object(yaml)
    class MyAllowedKeysDotDict(RequiredKeysDotDict):
        # ONLY keys "x", "y", and "z" are allowed
        _allowed_keys = {"x", "y", "z"}
        # key "x" MUST be present
        _required_keys = {"x"}
        # the value for key "x" MUST be an integer
        _key_types = {
            "x": int
        }
    """
    _allowed_keys = set()

    def __init__(self, *args, **kwargs):
        if not self._required_keys.issubset(self._allowed_keys):
            raise ValueError("_required_keys : {!r} must be a subset of _allowed_keys {!r}".format(
                self._required_keys,
                self._allowed_keys,
            ))
        super(AllowedKeysDotDict, self).__init__(*args, **kwargs)
        for key in self.keys():
            if key not in self._allowed_keys:
                raise KeyError("key: {!r} not in allowed keys: {!r}".format(
                    key,
                    self._allowed_keys
                ))

    def __getattr__(self, item):
        if item in self._allowed_keys:
            return self.get(item)
        else:
            # We raise AttributeError in the event that someone tries to access a nonexistent property
            # to be more consistent with usual type semantics without losing dictionary access patterns.
            # Note that a dictionary would usually raise KeyError
            raise AttributeError("key: {0} does not exist in this instance of {1}".format(
                item,
                self.__class__.__name__,
            ))

    def __setitem__(self, key, val):
        if key not in self._allowed_keys:
            raise KeyError("key: {!r} not in allowed keys: {!r}".format(
                key,
                self._allowed_keys
            ))

        super(AllowedKeysDotDict, self).__setattr__(key, val)

    __setattr__ = __setitem__
