from typing import TypeVar, Hashable, Mapping, Union, Optional
from collections import UserDict

import pandas as pd

T = TypeVar("T", bound=Hashable)


class BiDict(
    UserDict,
    Mapping[T, T],
):
    def __init__(
        self,
        __dict: Optional[Mapping[Hashable, Hashable]] = None,
        /,
        **kwargs: Hashable,
    ):
        if isinstance(__dict, Mapping):
            kws: dict = {**__dict, **kwargs}
        else:
            kws = kwargs
        super().__init__(**kws)

    def __delitem__(self, key: Hashable):
        value = self.data.pop(key)
        super().pop(value, None)

    def __setitem__(self, key: Hashable, value: Hashable):
        if key in self:
            del self[self[key]]
        if value in self:
            del self[value]
        super().__setitem__(key, value)
        super().__setitem__(value, key)

    def __repr__(self):
        return f"{type(self).__name__}({super().__repr__()})"


if __name__ == "__main__":
    d1: BiDict = BiDict({"foo": "bar"})
    print(d1)
    d2: BiDict[str] = BiDict(foo="bar")
    print(d1["foo"])
    print(d1["bar"])
    td: BiDict[Union[str, type]] = BiDict(dict=dict, pandas=pd.DataFrame)
    print(td)
    print(repr(td))
    # print(td[pd.DataFrame])
