from collections import Iterable
import inspect
import copy

from ruamel.yaml import YAML, yaml_object
yaml = YAML()

class ListOf(object):
    def __init__(self, type_):
        self.type_ = type_


@yaml_object(yaml)
class DotDict(dict):
    """dot.notation access to dictionary attributes"""

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
    _required_keys = set()
    _key_types = {}

    def __init__(self, *args, coerce_types=False, **kwargs):
        super(RequiredKeysDotDict, self).__init__(*args, **kwargs)
        for key in self._required_keys:
            if key not in self.keys():
                raise KeyError("key: {!r} is missing even though it's in the required keys: {!r}".format(
                    key,
                    self._required_keys
                ))

        for key, value in self.items():
            if key in self._key_types and coerce_types:
                # Update values if coerce_types==True
                try:
                    # If the given type is an instance of LooselyTypedDotDict, apply coerce_types recursively
                    if isinstance(self._key_types[key], ListOf):
                        if inspect.isclass(self._key_types[key].type_) and issubclass(self._key_types[key].type_,
                                                                                      RequiredKeysDotDict):
                            value = [self._key_types[key].type_(coerce_types=True, **v) for v in value]
                        else:
                            value = [self._key_types[key].type_(
                                v) for v in value]

                    else:
                        if inspect.isclass(self._key_types[key]) and issubclass(self._key_types[key],
                                                                                RequiredKeysDotDict):
                            value = self._key_types[key](coerce_types=True, **value)
                        else:
                            value = self._key_types[key](value)
                except TypeError as e:
                    raise ("Unable to initialize " + self.__class__.__name__ + ": could not convert type. TypeError "
                                                                               "raised: " + str(e))
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

    @classmethod
    def _validate_value_type(cls, key, value, type_):
        if type(value) != type_:
            if isinstance(type_, ListOf):
                if not isinstance(value, Iterable):
                    raise TypeError("key: {!r} must be an Iterable type, not {!r}".format(
                        key,
                        type_.type_,
                        value,
                        type(value),
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


@yaml_object(yaml)
class LooselyTypedDotDict(RequiredKeysDotDict):
    """dot.notation access to dictionary attributes, with limited keys
    

    Note: this class is pretty useless on its own.
    You need to subclass it like so:

    class MyLooselyTypedDotDict(LooselyTypedDotDict):
        _allowed_keys = set([
            "x", "y", "z"
        ])

    """
    _allowed_keys = set()

    def __init__(self, *args, coerce_types=False, **kwargs):
        if not self._required_keys.issubset(self._allowed_keys):
            raise ValueError("_required_keys : {!r} must be a subset of _allowed_keys {!r}".format(
                self._required_keys,
                self._allowed_keys,
            ))
        super(LooselyTypedDotDict, self).__init__(*args, coerce_types=coerce_types, **kwargs)
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
            raise AttributeError

    def __setitem__(self, key, val):
        if key not in self._allowed_keys:
            raise KeyError("key: {!r} not in allowed keys: {!r}".format(
                key,
                self._allowed_keys
            ))

        super(LooselyTypedDotDict, self).__setattr__(key, val)

    __setattr__ = __setitem__