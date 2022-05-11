
import logging
import warnings
logger = logging.getLogger(__name__)

class MetaMetricProvider(type):
    'MetaMetricProvider registers metrics as they are defined.'

    def __new__(cls, clsname, bases, attrs):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        newclass = super().__new__(cls, clsname, bases, attrs)
        newclass._register_metric_functions()
        return newclass

class DeprecatedMetaMetricProvider(MetaMetricProvider):
    '\n    Goals:\n        Instantiation of a deprecated class should raise a warning;\n        Subclassing of a deprecated class should raise a warning;\n        Support isinstance and issubclass checks.\n    '
    warnings.simplefilter('default', category=DeprecationWarning)
    logging.captureWarnings(False)

    def __new__(cls, name, bases, classdict, *args, **kwargs):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        alias = classdict.get('_DeprecatedMetaMetricProvider__alias')
        if (alias is not None):

            def new(cls, *args, **kwargs):
                import inspect
                __frame = inspect.currentframe()
                __file = __frame.f_code.co_filename
                __func = __frame.f_code.co_name
                for (k, v) in __frame.f_locals.items():
                    if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                        continue
                    print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
                alias = getattr(cls, '_DeprecatedMetaMetricProvider__alias')
                if (alias is not None):
                    warnings.warn(f'''{cls.__name__} has been renamed to {alias} -- the alias {cls.__name__} is deprecated as of v0.13.12 and will be removed in v0.16.
''', DeprecationWarning, stacklevel=2)
                return alias(*args, **kwargs)
            classdict['__new__'] = new
            classdict['_DeprecatedMetaMetricProvider__alias'] = alias
        fixed_bases = []
        for b in bases:
            alias = getattr(b, '_DeprecatedMetaMetricProvider__alias', None)
            if (alias is not None):
                warnings.warn(f'''{b.__name__} has been renamed to {alias.__name__} -- the alias {b.__name__} is deprecated as of v0.13.12 and will be removed in v0.16.
''', DeprecationWarning, stacklevel=2)
            b = (alias or b)
            if (b not in fixed_bases):
                fixed_bases.append(b)
        fixed_bases = tuple(fixed_bases)
        return super().__new__(cls, name, fixed_bases, classdict, *args, **kwargs)

    def __instancecheck__(cls, instance):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return any((cls.__subclasscheck__(c) for c in {type(instance), instance.__class__}))

    def __subclasscheck__(cls, subclass):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        if (subclass is cls):
            return True
        else:
            return issubclass(subclass, getattr(cls, '_DeprecatedMetaMetricProvider__alias'))
