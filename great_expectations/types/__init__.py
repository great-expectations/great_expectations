from .configurations import ClassConfig


class DictDot:
    def __getitem__(self, item):
        if isinstance(item, int):
            return list(self.__dict__.keys())[item]
        return getattr(self, item)

    def __setitem__(self, key, value):
        setattr(self, key, value)

    def __delitem__(self, key):
        delattr(self, key)

    def __iter__(self):
        return iter(vars(self).keys())

    def items(self):
        return iter(vars(self).items())


class SerializableDictDot(DictDot):
    def to_json_dict(self) -> dict:
        raise NotImplementedError
