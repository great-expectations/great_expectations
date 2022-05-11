
from hashlib import md5

def tuple_to_hash(tuple_):
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    return md5(str(tuple_).encode('utf-8')).hexdigest()

def kwargs_to_tuple(d):
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    'Convert expectation configuration kwargs to a canonical tuple.'
    if isinstance(d, list):
        return tuple((kwargs_to_tuple(v) for v in sorted(d)))
    elif isinstance(d, dict):
        return tuple(((k, kwargs_to_tuple(v)) for (k, v) in sorted(d.items()) if (k not in ['result_format', 'include_config', 'catch_exceptions', 'meta'])))
    return d
