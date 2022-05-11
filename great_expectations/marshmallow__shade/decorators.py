"Decorators for registering schema pre-processing and post-processing methods.\nThese should be imported from the top-level `marshmallow` module.\n\nMethods decorated with\n`pre_load <marshmallow.decorators.pre_load>`, `post_load <marshmallow.decorators.post_load>`,\n`pre_dump <marshmallow.decorators.pre_dump>`, `post_dump <marshmallow.decorators.post_dump>`,\nand `validates_schema <marshmallow.decorators.validates_schema>` receive\n``many`` as a keyword argument. In addition, `pre_load <marshmallow.decorators.pre_load>`,\n`post_load <marshmallow.decorators.post_load>`,\nand `validates_schema <marshmallow.decorators.validates_schema>` receive\n``partial``. If you don't need these arguments, add ``**kwargs`` to your method\nsignature.\n\n\nExample: ::\n\n    from great_expectations.marshmallow__shade import (\n        Schema, pre_load, pre_dump, post_load, validates_schema,\n        validates, fields, ValidationError\n    )\n\n    class UserSchema(Schema):\n\n        email = fields.Str(required=True)\n        age = fields.Integer(required=True)\n\n        @post_load\n        def lowerstrip_email(self, item, many, **kwargs):\n            item['email'] = item['email'].lower().strip()\n            return item\n\n        @pre_load(pass_many=True)\n        def remove_envelope(self, data, many, **kwargs):\n            namespace = 'results' if many else 'result'\n            return data[namespace]\n\n        @post_dump(pass_many=True)\n        def add_envelope(self, data, many, **kwargs):\n            namespace = 'results' if many else 'result'\n            return {namespace: data}\n\n        @validates_schema\n        def validate_email(self, data, **kwargs):\n            if len(data['email']) < 3:\n                raise ValidationError('Email must be more than 3 characters', 'email')\n\n        @validates('age')\n        def validate_age(self, data, **kwargs):\n            if data < 14:\n                raise ValidationError('Too young!')\n\n.. note::\n    These decorators only work with instance methods. Class and static\n    methods are not supported.\n\n.. warning::\n    The invocation order of decorated methods of the same type is not guaranteed.\n    If you need to guarantee order of different processing steps, you should put\n    them in the same processing method.\n"
import functools

PRE_DUMP = "pre_dump"
POST_DUMP = "post_dump"
PRE_LOAD = "pre_load"
POST_LOAD = "post_load"
VALIDATES = "validates"
VALIDATES_SCHEMA = "validates_schema"


def validates(field_name: str):
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
    "Register a field validator.\n\n    :param str field_name: Name of the field that the method validates.\n    "
    return set_hook(None, VALIDATES, field_name=field_name)


def validates_schema(
    fn=None, pass_many=False, pass_original=False, skip_on_field_errors=True
):
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
    "Register a schema-level validator.\n\n    By default it receives a single object at a time, transparently handling the ``many``\n    argument passed to the `Schema`'s :func:`~marshmallow.Schema.validate` call.\n    If ``pass_many=True``, the raw data (which may be a collection) is passed.\n\n    If ``pass_original=True``, the original data (before unmarshalling) will be passed as\n    an additional argument to the method.\n\n    If ``skip_on_field_errors=True``, this validation method will be skipped whenever\n    validation errors have been detected when validating fields.\n\n    .. versionchanged:: 3.0.0b1\n        ``skip_on_field_errors`` defaults to `True`.\n\n    .. versionchanged:: 3.0.0\n        ``partial`` and ``many`` are always passed as keyword arguments to\n        the decorated method.\n    "
    return set_hook(
        fn,
        (VALIDATES_SCHEMA, pass_many),
        pass_original=pass_original,
        skip_on_field_errors=skip_on_field_errors,
    )


def pre_dump(fn=None, pass_many=False):
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
    "Register a method to invoke before serializing an object. The method\n    receives the object to be serialized and returns the processed object.\n\n    By default it receives a single object at a time, transparently handling the ``many``\n    argument passed to the `Schema`'s :func:`~marshmallow.Schema.dump` call.\n    If ``pass_many=True``, the raw data (which may be a collection) is passed.\n\n    .. versionchanged:: 3.0.0\n        ``many`` is always passed as a keyword arguments to the decorated method.\n    "
    return set_hook(fn, (PRE_DUMP, pass_many))


def post_dump(fn=None, pass_many=False, pass_original=False):
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
    "Register a method to invoke after serializing an object. The method\n    receives the serialized object and returns the processed object.\n\n    By default it receives a single object at a time, transparently handling the ``many``\n    argument passed to the `Schema`'s :func:`~marshmallow.Schema.dump` call.\n    If ``pass_many=True``, the raw data (which may be a collection) is passed.\n\n    If ``pass_original=True``, the original data (before serializing) will be passed as\n    an additional argument to the method.\n\n    .. versionchanged:: 3.0.0\n        ``many`` is always passed as a keyword arguments to the decorated method.\n    "
    return set_hook(fn, (POST_DUMP, pass_many), pass_original=pass_original)


def pre_load(fn=None, pass_many=False):
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
    "Register a method to invoke before deserializing an object. The method\n    receives the data to be deserialized and returns the processed data.\n\n    By default it receives a single object at a time, transparently handling the ``many``\n    argument passed to the `Schema`'s :func:`~marshmallow.Schema.load` call.\n    If ``pass_many=True``, the raw data (which may be a collection) is passed.\n\n    .. versionchanged:: 3.0.0\n        ``partial`` and ``many`` are always passed as keyword arguments to\n        the decorated method.\n    "
    return set_hook(fn, (PRE_LOAD, pass_many))


def post_load(fn=None, pass_many=False, pass_original=False):
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
    "Register a method to invoke after deserializing an object. The method\n    receives the deserialized data and returns the processed data.\n\n    By default it receives a single object at a time, transparently handling the ``many``\n    argument passed to the `Schema`'s :func:`~marshmallow.Schema.load` call.\n    If ``pass_many=True``, the raw data (which may be a collection) is passed.\n\n    If ``pass_original=True``, the original data (before deserializing) will be passed as\n    an additional argument to the method.\n\n    .. versionchanged:: 3.0.0\n        ``partial`` and ``many`` are always passed as keyword arguments to\n        the decorated method.\n    "
    return set_hook(fn, (POST_LOAD, pass_many), pass_original=pass_original)


def set_hook(fn, key, **kwargs):
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
    "Mark decorated function as a hook to be picked up later.\n    You should not need to use this method directly.\n\n    .. note::\n        Currently only works with functions and instance methods. Class and\n        static methods are not supported.\n\n    :return: Decorated function if supplied, else this decorator with its args\n        bound.\n    "
    if fn is None:
        return functools.partial(set_hook, key=key, **kwargs)
    try:
        hook_config = fn.__marshmallow_hook__
    except AttributeError:
        fn.__marshmallow_hook__ = hook_config = {}
    hook_config[key] = kwargs
    return fn
