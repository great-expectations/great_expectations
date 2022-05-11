
import copy
import logging
from ruamel.yaml import YAML, yaml_object
logger = logging.getLogger(__name__)
yaml = YAML()

@yaml_object(yaml)
class DotDict(dict):
    'This class provides dot.notation access to dictionary attributes.\n\n    It is also serializable by the ruamel.yaml library used in Great Expectations for managing\n    configuration objects.\n    '

    def __getattr__(self, item):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return self.get(item)
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__

    def __dir__(self):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return self.keys()

    def __deepcopy__(self, memo):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return DotDict([(copy.deepcopy(k, memo), copy.deepcopy(v, memo)) for (k, v) in self.items()])
    _yaml_merge = []

    @classmethod
    def yaml_anchor(cls):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return None

    @classmethod
    def to_yaml(cls, representer, node):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Use dict representation for DotDict (and subtypes by default)'
        return representer.represent_dict(node)

class SerializableDotDict(DotDict):
    '\n    Analogously to the way "SerializableDictDot" extends "DictDot" to provide JSON serialization, the present class,\n    "SerializableDotDict" extends "DotDict" to provide JSON-serializable version of the "DotDict" class as well.\n    Since "DotDict" is already YAML-serializable, "SerializableDotDict" is both YAML-serializable and JSON-serializable.\n    '

    def to_json_dict(self) -> dict:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        raise NotImplementedError
