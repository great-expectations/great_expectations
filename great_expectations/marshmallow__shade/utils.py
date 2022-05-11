
'Utility methods for marshmallow.'
import collections
import datetime as dt
import functools
import inspect
import json
import re
import typing
import warnings
from collections.abc import Mapping
from email.utils import format_datetime, parsedate_to_datetime
from pprint import pprint as py_pprint
from great_expectations.marshmallow__shade.base import FieldABC
from great_expectations.marshmallow__shade.exceptions import FieldInstanceResolutionError
from great_expectations.marshmallow__shade.warnings import RemovedInMarshmallow4Warning
EXCLUDE = 'exclude'
INCLUDE = 'include'
RAISE = 'raise'

class _Missing():

    def __bool__(self):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return False

    def __copy__(self):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return self

    def __deepcopy__(self, _):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return self

    def __repr__(self):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return '<marshmallow.missing>'
missing = _Missing()

def is_generator(obj) -> bool:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    'Return True if ``obj`` is a generator'
    return (inspect.isgeneratorfunction(obj) or inspect.isgenerator(obj))

def is_iterable_but_not_string(obj) -> bool:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    "Return True if ``obj`` is an iterable object that isn't a string."
    return ((hasattr(obj, '__iter__') and (not hasattr(obj, 'strip'))) or is_generator(obj))

def is_collection(obj) -> bool:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    'Return True if ``obj`` is a collection type, e.g list, tuple, queryset.'
    return (is_iterable_but_not_string(obj) and (not isinstance(obj, Mapping)))

def is_instance_or_subclass(val, class_) -> bool:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    'Return True if ``val`` is either a subclass or instance of ``class_``.'
    try:
        return issubclass(val, class_)
    except TypeError:
        return isinstance(val, class_)

def is_keyed_tuple(obj) -> bool:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    "Return True if ``obj`` has keyed tuple behavior, such as\n    namedtuples or SQLAlchemy's KeyedTuples.\n    "
    return (isinstance(obj, tuple) and hasattr(obj, '_fields'))

def pprint(obj, *args, **kwargs) -> None:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    'Pretty-printing function that can pretty-print OrderedDicts\n    like regular dictionaries. Useful for printing the output of\n    :meth:`marshmallow.Schema.dump`.\n    '
    warnings.warn("marshmallow's pprint function is deprecated and will be removed in marshmallow 4.", RemovedInMarshmallow4Warning)
    if isinstance(obj, collections.OrderedDict):
        print(json.dumps(obj, *args, **kwargs))
    else:
        py_pprint(obj, *args, **kwargs)

def is_aware(datetime: dt.datetime) -> bool:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    return ((datetime.tzinfo is not None) and (datetime.tzinfo.utcoffset(datetime) is not None))

def from_rfc(datestring: str) -> dt.datetime:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    'Parse a RFC822-formatted datetime string and return a datetime object.\n\n    https://stackoverflow.com/questions/885015/how-to-parse-a-rfc-2822-date-time-into-a-python-datetime  # noqa: B950\n    '
    return parsedate_to_datetime(datestring)

def rfcformat(datetime: dt.datetime) -> str:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    'Return the RFC822-formatted representation of a datetime object.\n\n    :param datetime datetime: The datetime.\n    '
    return format_datetime(datetime)
_iso8601_datetime_re = re.compile('(?P<year>\\d{4})-(?P<month>\\d{1,2})-(?P<day>\\d{1,2})[T ](?P<hour>\\d{1,2}):(?P<minute>\\d{1,2})(?::(?P<second>\\d{1,2})(?:\\.(?P<microsecond>\\d{1,6})\\d{0,6})?)?(?P<tzinfo>Z|[+-]\\d{2}(?::?\\d{2})?)?$')
_iso8601_date_re = re.compile('(?P<year>\\d{4})-(?P<month>\\d{1,2})-(?P<day>\\d{1,2})$')
_iso8601_time_re = re.compile('(?P<hour>\\d{1,2}):(?P<minute>\\d{1,2})(?::(?P<second>\\d{1,2})(?:\\.(?P<microsecond>\\d{1,6})\\d{0,6})?)?')

def get_fixed_timezone(offset: typing.Union[(int, float, dt.timedelta)]) -> dt.timezone:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    'Return a tzinfo instance with a fixed offset from UTC.'
    if isinstance(offset, dt.timedelta):
        offset = (offset.total_seconds() // 60)
    sign = ('-' if (offset < 0) else '+')
    hhmm = ('%02d%02d' % divmod(abs(offset), 60))
    name = (sign + hhmm)
    return dt.timezone(dt.timedelta(minutes=offset), name)

def from_iso_datetime(value):
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    'Parse a string and return a datetime.datetime.\n\n    This function supports time zone offsets. When the input contains one,\n    the output uses a timezone with a fixed offset from UTC.\n    '
    match = _iso8601_datetime_re.match(value)
    if (not match):
        raise ValueError('Not a valid ISO8601-formatted datetime string')
    kw = match.groupdict()
    kw['microsecond'] = (kw['microsecond'] and kw['microsecond'].ljust(6, '0'))
    tzinfo = kw.pop('tzinfo')
    if (tzinfo == 'Z'):
        tzinfo = dt.timezone.utc
    elif (tzinfo is not None):
        offset_mins = (int(tzinfo[(- 2):]) if (len(tzinfo) > 3) else 0)
        offset = ((60 * int(tzinfo[1:3])) + offset_mins)
        if (tzinfo[0] == '-'):
            offset = (- offset)
        tzinfo = get_fixed_timezone(offset)
    kw = {k: int(v) for (k, v) in kw.items() if (v is not None)}
    kw['tzinfo'] = tzinfo
    return dt.datetime(**kw)

def from_iso_time(value):
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    "Parse a string and return a datetime.time.\n\n    This function doesn't support time zone offsets.\n    "
    match = _iso8601_time_re.match(value)
    if (not match):
        raise ValueError('Not a valid ISO8601-formatted time string')
    kw = match.groupdict()
    kw['microsecond'] = (kw['microsecond'] and kw['microsecond'].ljust(6, '0'))
    kw = {k: int(v) for (k, v) in kw.items() if (v is not None)}
    return dt.time(**kw)

def from_iso_date(value):
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    'Parse a string and return a datetime.date.'
    match = _iso8601_date_re.match(value)
    if (not match):
        raise ValueError('Not a valid ISO8601-formatted date string')
    kw = {k: int(v) for (k, v) in match.groupdict().items()}
    return dt.date(**kw)

def isoformat(datetime: dt.datetime) -> str:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    'Return the ISO8601-formatted representation of a datetime object.\n\n    :param datetime datetime: The datetime.\n    '
    return datetime.isoformat()

def to_iso_date(date: dt.date) -> str:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    return dt.date.isoformat(date)

def ensure_text_type(val: typing.Union[(str, bytes)]) -> str:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    if isinstance(val, bytes):
        val = val.decode('utf-8')
    return str(val)

def pluck(dictlist: typing.List[typing.Dict[(str, typing.Any)]], key: str):
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    "Extracts a list of dictionary values from a list of dictionaries.\n    ::\n\n        >>> dlist = [{'id': 1, 'name': 'foo'}, {'id': 2, 'name': 'bar'}]\n        >>> pluck(dlist, 'id')\n        [1, 2]\n    "
    return [d[key] for d in dictlist]

def get_value(obj, key: typing.Union[(int, str)], default=missing):
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    'Helper for pulling a keyed value off various types of objects. Fields use\n    this method by default to access attributes of the source object. For object `x`\n    and attribute `i`, this method first tries to access `x[i]`, and then falls back to\n    `x.i` if an exception is raised.\n\n    .. warning::\n        If an object `x` does not raise an exception when `x[i]` does not exist,\n        `get_value` will never check the value `x.i`. Consider overriding\n        `marshmallow.fields.Field.get_value` in this case.\n    '
    if ((not isinstance(key, int)) and ('.' in key)):
        return _get_value_for_keys(obj, key.split('.'), default)
    else:
        return _get_value_for_key(obj, key, default)

def _get_value_for_keys(obj, keys, default):
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    if (len(keys) == 1):
        return _get_value_for_key(obj, keys[0], default)
    else:
        return _get_value_for_keys(_get_value_for_key(obj, keys[0], default), keys[1:], default)

def _get_value_for_key(obj, key, default):
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    if (not hasattr(obj, '__getitem__')):
        return getattr(obj, key, default)
    try:
        return obj[key]
    except (KeyError, IndexError, TypeError, AttributeError):
        return getattr(obj, key, default)

def set_value(dct: typing.Dict[(str, typing.Any)], key: str, value: typing.Any) -> None:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    "Set a value in a dict. If `key` contains a '.', it is assumed\n    be a path (i.e. dot-delimited string) to the value's location.\n\n    ::\n\n        >>> d = {}\n        >>> set_value(d, 'foo.bar', 42)\n        >>> d\n        {'foo': {'bar': 42}}\n    "
    if ('.' in key):
        (head, rest) = key.split('.', 1)
        target = dct.setdefault(head, {})
        if (not isinstance(target, dict)):
            raise ValueError(f'Cannot set {key} in {head} due to existing value: {target}')
        set_value(target, rest, value)
    else:
        dct[key] = value

def callable_or_raise(obj):
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    'Check that an object is callable, else raise a :exc:`ValueError`.'
    if (not callable(obj)):
        raise ValueError(f'Object {obj!r} is not callable.')
    return obj

def _signature(func: typing.Callable) -> typing.List[str]:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    return list(inspect.signature(func).parameters.keys())

def get_func_args(func: typing.Callable) -> typing.List[str]:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    'Given a callable, return a list of argument names. Handles\n    `functools.partial` objects and class-based callables.\n\n    .. versionchanged:: 3.0.0a1\n        Do not return bound arguments, eg. ``self``.\n    '
    if (inspect.isfunction(func) or inspect.ismethod(func)):
        return _signature(func)
    if isinstance(func, functools.partial):
        return _signature(func.func)
    return _signature(func)

def resolve_field_instance(cls_or_instance):
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    'Return a Schema instance from a Schema class or instance.\n\n    :param type|Schema cls_or_instance: Marshmallow Schema class or instance.\n    '
    if isinstance(cls_or_instance, type):
        if (not issubclass(cls_or_instance, FieldABC)):
            raise FieldInstanceResolutionError
        return cls_or_instance()
    else:
        if (not isinstance(cls_or_instance, FieldABC)):
            raise FieldInstanceResolutionError
        return cls_or_instance
