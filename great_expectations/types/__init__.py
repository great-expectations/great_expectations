from .configurations import ClassConfig
import copy

class DictDot:
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

    def keys(self) -> set:
        # This is needed to play nice with pydantic.
        # Yes, this means that this method returns a different object than an dict
        keys = set(self.__dict__.keys())
        if "__initialised__" in keys:
            keys.remove("__initialised__")

        return keys

    def items(self):
        return self.to_dict().items()

    def get(self, key, default_value=None):
        return self.__dict__.get(key, default_value)

    def to_dict(self):
        new_dict = copy.deepcopy(self.__dict__)

        # This is needed to play nice with pydantic.
        if "__initialised__" in new_dict:
            del new_dict["__initialised__"]

        return new_dict


class SerializableDictDot(DictDot):
    def to_json_dict(self) -> dict:
        raise NotImplementedError
