
import hashlib
import json

class IDDict(dict):
    _id_ignore_keys = set()

    def to_id(self, id_keys=None, id_ignore_keys=None):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        if (id_keys is None):
            id_keys = self.keys()
        if (id_ignore_keys is None):
            id_ignore_keys = self._id_ignore_keys
        id_keys = (set(id_keys) - set(id_ignore_keys))
        if (len(id_keys) == 0):
            return tuple()
        elif (len(id_keys) == 1):
            key = list(id_keys)[0]
            return f'{key}={str(self[key])}'
        _id_dict = {k: self[k] for k in id_keys}
        return hashlib.md5(json.dumps(_id_dict, sort_keys=True).encode('utf-8')).hexdigest()

class BatchKwargs(IDDict):
    pass

class BatchSpec(IDDict):
    pass

class MetricKwargs(IDDict):
    pass
