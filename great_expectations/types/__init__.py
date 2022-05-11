
import copy
import logging
from enum import Enum
from typing import Optional, Set
import pandas as pd
from .base import SerializableDotDict
from .color_palettes import ColorPalettes, Colors
from .configurations import ClassConfig
logger = logging.getLogger(__name__)
try:
    import pyspark
except ImportError:
    pyspark = None
    logger.debug('Unable to load pyspark; install optional spark dependency if you will be working with Spark dataframes')

class DictDot():
    'A convenience class for migrating away from untyped dictionaries to stronger typed objects.\n\n    Can be instantiated with arguments:\n\n        my_A = MyClassA(\n                foo="a string",\n                bar=1,\n            )\n\n    Can be instantiated from a dictionary:\n\n        my_A = MyClassA(\n            **{\n                "foo": "a string",\n                "bar": 1,\n            }\n        )\n\n    Can be accessed using both dictionary and dot notation\n\n        my_A.foo == "a string"\n        my_A.bar == 1\n\n        my_A["foo"] == "a string"\n        my_A["bar"] == 1\n\n    Pairs nicely with @dataclass:\n\n        @dataclass()\n        class MyClassA(DictDot):\n            foo: str\n            bar: int\n\n    Can be made immutable:\n\n        @dataclass(frozen=True)\n        class MyClassA(DictDot):\n            foo: str\n            bar: int\n\n    For more examples of usage, please see `test_dataclass_serializable_dot_dict_pattern.py` in the tests folder.\n    '
    include_field_names: Set[str] = set()
    exclude_field_names: Set[str] = set()

    def __getitem__(self, item):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        if isinstance(item, int):
            return list(self.__dict__.keys())[item]
        return getattr(self, item)

    def __setitem__(self, key, value) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        setattr(self, key, value)

    def __delitem__(self, key) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        delattr(self, key)

    def __contains__(self, key):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return hasattr(self, key)

    def __len__(self):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return len(self.__dict__)

    def keys(self):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return self.__dict__.keys()

    def values(self):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return self.to_raw_dict().values()

    def items(self):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return self.to_raw_dict().items()

    def get(self, key, default_value=None):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        if self.__contains__(key=key):
            return self.__getitem__(item=key)
        return self.__dict__.get(key, default_value)

    def to_raw_dict(self):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Convert this object into a standard dictionary, recursively.\n\n        This is often convenient for serialization, and in cases where an untyped version of the object is required.\n        '
        new_dict = safe_deep_copy(data=self.__dict__)
        if ('__initialised__' in new_dict):
            del new_dict['__initialised__']
        for (key, value) in new_dict.items():
            if isinstance(value, DictDot):
                new_dict[key] = value.to_raw_dict()
            if isinstance(value, Enum):
                new_dict[key] = value.value
            if (isinstance(value, list) or isinstance(value, tuple)):
                new_dict[key] = [temp_element for temp_element in value]
                for (i, element) in enumerate(value):
                    if isinstance(element, DictDot):
                        new_dict[key][i] = element.to_raw_dict()
                    if isinstance(element, Enum):
                        new_dict[key][i] = element.value
        return new_dict

    def to_dict(self) -> dict:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        new_dict = {key: self[key] for key in self.property_names(include_keys=self.include_field_names, exclude_keys=self.exclude_field_names)}
        for (key, value) in new_dict.items():
            if isinstance(value, DictDot):
                new_dict[key] = value.to_dict()
            if isinstance(value, Enum):
                new_dict[key] = value.value
            if (isinstance(value, list) or isinstance(value, tuple)):
                new_dict[key] = [temp_element for temp_element in value]
                for (i, element) in enumerate(value):
                    if isinstance(element, DictDot):
                        new_dict[key][i] = element.to_dict()
                    if isinstance(element, Enum):
                        new_dict[key][i] = element.value
        return new_dict

    def property_names(self, include_keys: Optional[Set[str]]=None, exclude_keys: Optional[Set[str]]=None) -> Set[str]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        Assuming that -- by convention -- names of private properties of an object are prefixed by "_" (a single\n        underscore character), return these property names as public property names.  To support this convention, the\n        extending classes must implement property accessors, corresponding to the property names, return by this method.\n\n        :param include_keys: inclusion list ("include only these properties, while excluding all the rest")\n        :param exclude_keys: exclusion list ("exclude only these properties, while include all the rest")\n        :return: property names, subject to inclusion/exclusion filtering\n        '
        if (include_keys is None):
            include_keys = set()
        if (exclude_keys is None):
            exclude_keys = set()
        if (include_keys & exclude_keys):
            raise ValueError('Common keys between sets of include_keys and exclude_keys filtering directives are illegal.')
        key: str
        private_fields: Set[str] = set(filter((lambda name: (len(name) > 1)), [key[1:] for key in self.keys() if (key[0] == '_')]))
        public_fields: Set[str] = {key for key in self.keys() if (key[0] != '_')}
        property_names: Set[str] = (public_fields | private_fields)
        keys_for_exclusion: list = []

        def assert_valid_keys(keys: Set[str], purpose: str) -> None:
            import inspect
            __frame = inspect.currentframe()
            __file = __frame.f_code.co_filename
            __func = __frame.f_code.co_name
            for (k, v) in __frame.f_locals.items():
                if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                    continue
                print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
            name: str
            for name in keys:
                try:
                    _ = self[name]
                except AttributeError:
                    try:
                        _ = self[f'_{name}']
                    except AttributeError:
                        raise ValueError(f'Property "{name}", marked for {purpose} on object "{str(type(self))}", does not exist.')
        if include_keys:
            assert_valid_keys(keys=include_keys, purpose='inclusion')
            keys_for_exclusion.extend([key for key in property_names if (key not in include_keys)])
        if exclude_keys:
            assert_valid_keys(keys=exclude_keys, purpose='exclusion')
            keys_for_exclusion.extend([key for key in property_names if (key in exclude_keys)])
        keys_for_exclusion = list(set(keys_for_exclusion))
        return {key for key in property_names if (key not in keys_for_exclusion)}

class SerializableDictDot(DictDot):

    def to_json_dict(self) -> dict:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        # TODO: <Alex>2/4/2022</Alex>\n        A reference implementation can be provided, once circular import dependencies, caused by relative locations of\n        the "great_expectations/types/__init__.py" and "great_expectations/core/util.py" modules are resolved.\n        '
        raise NotImplementedError

def safe_deep_copy(data, memo=None):
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    '\n    This method makes a copy of a dictionary, applying deep copy to attribute values, except for non-pickleable objects.\n    '
    if (isinstance(data, (pd.Series, pd.DataFrame)) or (pyspark and isinstance(data, pyspark.sql.DataFrame))):
        return data
    if isinstance(data, (list, tuple)):
        return [safe_deep_copy(data=element, memo=memo) for element in data]
    if isinstance(data, dict):
        return {key: safe_deep_copy(data=value, memo=memo) for (key, value) in data.items()}
    return copy.deepcopy(data, memo)
