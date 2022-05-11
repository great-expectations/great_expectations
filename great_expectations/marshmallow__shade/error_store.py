
'Utilities for storing collections of error messages.\n\n.. warning::\n\n    This module is treated as private API.\n    Users should not need to use this module directly.\n'
from great_expectations.marshmallow__shade.exceptions import SCHEMA

class ErrorStore():

    def __init__(self) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        self.errors = {}

    def store_error(self, messages, field_name=SCHEMA, index=None) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        if ((field_name != SCHEMA) or (not isinstance(messages, dict))):
            messages = {field_name: messages}
        if (index is not None):
            messages = {index: messages}
        self.errors = merge_errors(self.errors, messages)

def merge_errors(errors1, errors2):
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    'Deeply merge two error messages.\n\n    The format of ``errors1`` and ``errors2`` matches the ``message``\n    parameter of :exc:`marshmallow.exceptions.ValidationError`.\n    '
    if (not errors1):
        return errors2
    if (not errors2):
        return errors1
    if isinstance(errors1, list):
        if isinstance(errors2, list):
            return (errors1 + errors2)
        if isinstance(errors2, dict):
            return dict(errors2, **{SCHEMA: merge_errors(errors1, errors2.get(SCHEMA))})
        return (errors1 + [errors2])
    if isinstance(errors1, dict):
        if isinstance(errors2, list):
            return dict(errors1, **{SCHEMA: merge_errors(errors1.get(SCHEMA), errors2)})
        if isinstance(errors2, dict):
            errors = dict(errors1)
            for (key, val) in errors2.items():
                if (key in errors):
                    errors[key] = merge_errors(errors[key], val)
                else:
                    errors[key] = val
            return errors
        return dict(errors1, **{SCHEMA: merge_errors(errors1.get(SCHEMA), errors2)})
    if isinstance(errors2, list):
        return ([errors1] + errors2)
    if isinstance(errors2, dict):
        return dict(errors2, **{SCHEMA: merge_errors(errors1, errors2.get(SCHEMA))})
    return [errors1, errors2]
