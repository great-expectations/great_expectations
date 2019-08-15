from collections import Iterable
import inspect
import copy

class ListOf(object):
    def __init__(self, type_):
        self.type_ = type_

class DotDict(dict):
    """dot.notation access to dictionary attributes"""

    def __getattr__(self, attr):
        return self.get(attr)

    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__

    def __dir__(self):
        return self.keys()

    # Cargo-cultishly copied from: https://github.com/spindlelabs/pyes/commit/d2076b385c38d6d00cebfe0df7b0d1ba8df934bc
    def __deepcopy__(self, memo):
        return DotDict([(copy.deepcopy(k, memo), copy.deepcopy(v, memo)) for k, v in self.items()])


#Inspiration : https://codereview.stackexchange.com/questions/81794/dictionary-with-restricted-keys
class LooselyTypedDotDict(DotDict):
    """dot.notation access to dictionary attributes, with limited keys
    

    Note: this class is pretty useless on its own.
    You need to subclass it like so:

    class MyLooselyTypedDotDict(LooselyTypedDotDict):
        _allowed_keys = set([
            "x", "y", "z"
        ])

    """

    _allowed_keys = set()
    _required_keys = set()
    _key_types = {}

    def __init__(self, coerce_types=False, **kwargs):
        # print(kwargs)
        # print(self._allowed_keys)
        # print(self._required_keys)

        if not self._required_keys.issubset(self._allowed_keys):
            raise ValueError("_required_keys : {!r} must be a subset of _allowed_keys {!r}".format(
                self._required_keys,
                self._allowed_keys,
            ))

        for key, value in kwargs.items():
            if key not in self._allowed_keys:
                raise KeyError("key: {!r} not in allowed keys: {!r}".format(
                    key,
                    self._allowed_keys
                ))

            # if key in self._key_types and not isinstance(key, self._key_types[key]):
            if key in self._key_types:
                # print(value)

                #Update values if coerce_types==True
                if coerce_types:
                    #TODO: Catch errors and raise more informative error messages here

                    #If the given type is an instance of LooselyTypedDotDict, apply coerce_types recursively
                    if isinstance(self._key_types[key], ListOf):
                        if inspect.isclass(self._key_types[key].type_) and issubclass(self._key_types[key].type_, LooselyTypedDotDict):
                            value = [self._key_types[key].type_(coerce_types=True, **v) for v in value]
                        else:
                            value = [self._key_types[key].type_(v) for v in value]

                    else:
                        if inspect.isclass(self._key_types[key]) and issubclass(self._key_types[key], LooselyTypedDotDict):
                            value = self._key_types[key](coerce_types=True, **value)
                        else:
                            value = self._key_types[key](value)
                
                # print(value)
                
                #Validate types
                self._validate_value_type(key, value, self._key_types[key])

            self[key] = value

        for key in self._required_keys:
            if key not in kwargs:
                raise KeyError("key: {!r} is missing even though it's in the required keys: {!r}".format(
                    key,
                    self._required_keys
                ))

    def __setitem__(self, key, val):
        if key not in self._allowed_keys:
            raise KeyError("key: {!r} not in allowed keys: {!r}".format(
                key,
                self._allowed_keys
            ))
        
        if key in self._key_types:
            self._validate_value_type(key, val, self._key_types[key])

        dict.__setitem__(self, key, val)

    def __setattr__(self, key, val):
        if key not in self._allowed_keys:
            raise KeyError("key: {!r} not in allowed keys: {!r}".format(
                key,
                self._allowed_keys
            ))

        if key in self._key_types:
            self._validate_value_type(key, val, self._key_types[key])

        dict.__setitem__(self, key, val)

    def __delitem__(self, key):
        if key in self._required_keys:
            raise KeyError("key: {!r} is required and cannot be deleted".format(
                key,
            ))

        dict.__delitem__(self, key)

    def __delattr__(self, key):
        if key in self._required_keys:
            raise KeyError("key: {!r} is required and cannot be deleted".format(
                key,
            ))

        dict.__delitem__(self, key)
    
    def _validate_value_type(self, key, value, type_):
        if type(value) != type_:

            #TODO: Catch errors and raise more informative error messages here
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

            else:
                raise TypeError("key: {!r} must be of type {!r}, not {!r}".format(
                    key,
                    type_,
                    type(value),
                ))
