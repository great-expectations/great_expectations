
import datetime
import decimal
import sys
from functools import wraps
import numpy as np
import pandas as pd
from great_expectations.exceptions import InvalidExpectationConfigurationError
from great_expectations.types import SerializableDictDot, SerializableDotDict

def parse_result_format(result_format):
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    'This is a simple helper utility that can be used to parse a string result_format into the dict format used\n    internally by great_expectations. It is not necessary but allows shorthand for result_format in cases where\n    there is no need to specify a custom partial_unexpected_count.'
    if isinstance(result_format, str):
        result_format = {'result_format': result_format, 'partial_unexpected_count': 20}
    elif ('partial_unexpected_count' not in result_format):
        result_format['partial_unexpected_count'] = 20
    return result_format
'Docstring inheriting descriptor. Note that this is not a docstring so that this is not added to @DocInherit-decorated functions\' hybrid docstrings.\n\nUsage::\n\n    class Foo(object):\n        def foo(self):\n            "Frobber"\n            pass\n\n    class Bar(Foo):\n        @DocInherit\n        def foo(self):\n            pass\n\n    Now, Bar.foo.__doc__ == Bar().foo.__doc__ == Foo.foo.__doc__ == "Frobber"\n\n    Original implementation cribbed from:\n    https://stackoverflow.com/questions/2025562/inherit-docstrings-in-python-class-inheritance,\n    following a discussion on comp.lang.python that resulted in:\n    http://code.activestate.com/recipes/576862/. Unfortunately, the\n    original authors did not anticipate deep inheritance hierarchies, and\n    we ran into a recursion issue when implementing custom subclasses of\n    PandasDataset:\n    https://github.com/great-expectations/great_expectations/issues/177.\n\n    Our new homegrown implementation directly searches the MRO, instead\n    of relying on super, and concatenates documentation together.\n'

class DocInherit():

    def __init__(self, mthd) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        self.mthd = mthd
        self.name = mthd.__name__
        self.mthd_doc = mthd.__doc__

    def __get__(self, obj, cls):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        doc = (self.mthd_doc if (self.mthd_doc is not None) else '')
        for parent in cls.mro():
            if (self.name not in parent.__dict__):
                continue
            if (parent.__dict__[self.name].__doc__ is not None):
                doc = f'''{doc}
{parent.__dict__[self.name].__doc__}'''

        @wraps(self.mthd, assigned=('__name__', '__module__'))
        def f(*args, **kwargs):
            import inspect
            __frame = inspect.currentframe()
            __file = __frame.f_code.co_filename
            __func = __frame.f_code.co_name
            for (k, v) in __frame.f_locals.items():
                if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                    continue
                print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
            return self.mthd(obj, *args, **kwargs)
        f.__doc__ = doc
        return f

def recursively_convert_to_json_serializable(test_obj):
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    '\n    Helper function to convert a dict object to one that is serializable\n\n    Args:\n        test_obj: an object to attempt to convert a corresponding json-serializable object\n\n    Returns:\n        (dict) A converted test_object\n\n    Warning:\n        test_obj may also be converted in place.\n\n    '
    if isinstance(test_obj, (SerializableDictDot, SerializableDotDict)):
        return test_obj
    try:
        if ((not isinstance(test_obj, list)) and np.isnan(test_obj)):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(test_obj, (str, int, float, bool)):
        return test_obj
    elif isinstance(test_obj, dict):
        new_dict = {}
        for key in test_obj:
            if ((key == 'row_condition') and (test_obj[key] is not None)):
                ensure_row_condition_is_correct(test_obj[key])
            new_dict[str(key)] = recursively_convert_to_json_serializable(test_obj[key])
        return new_dict
    elif isinstance(test_obj, (list, tuple, set)):
        new_list = []
        for val in test_obj:
            new_list.append(recursively_convert_to_json_serializable(val))
        return new_list
    elif isinstance(test_obj, (np.ndarray, pd.Index)):
        return [recursively_convert_to_json_serializable(x) for x in test_obj.tolist()]
    elif (test_obj is None):
        return test_obj
    elif isinstance(test_obj, (datetime.datetime, datetime.date)):
        return str(test_obj)
    elif np.issubdtype(type(test_obj), np.bool_):
        return bool(test_obj)
    elif (np.issubdtype(type(test_obj), np.integer) or np.issubdtype(type(test_obj), np.uint)):
        return int(test_obj)
    elif np.issubdtype(type(test_obj), np.floating):
        return float(round(test_obj, sys.float_info.dig))
    elif isinstance(test_obj, pd.Series):
        index_name = (test_obj.index.name or 'index')
        value_name = (test_obj.name or 'value')
        return [{index_name: recursively_convert_to_json_serializable(idx), value_name: recursively_convert_to_json_serializable(val)} for (idx, val) in test_obj.iteritems()]
    elif isinstance(test_obj, pd.DataFrame):
        return recursively_convert_to_json_serializable(test_obj.to_dict(orient='records'))
    elif isinstance(test_obj, decimal.Decimal):
        return float(test_obj)
    else:
        raise TypeError(('%s is of type %s which cannot be serialized.' % (str(test_obj), type(test_obj).__name__)))

def ensure_row_condition_is_correct(row_condition_string) -> None:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    'Ensure no quote nor \\\\n are introduced in row_condition string.\n\n    Otherwise it may cause an issue at the reload of the expectation.\n    An error is raised at the declaration of the expectations to ensure\n    the user is not doing a mistake. He can use double quotes for example.\n\n    Parameters\n    ----------\n    row_condition_string : str\n        the pandas query string\n    '
    if ("'" in row_condition_string):
        raise InvalidExpectationConfigurationError(f'{row_condition_string} cannot be serialized to json. Do not introduce simple quotes in configuration.Use double quotes instead.')
    if ('\n' in row_condition_string):
        raise InvalidExpectationConfigurationError(f'{repr(row_condition_string)} cannot be serialized to json. Do not introduce \n in configuration.')
