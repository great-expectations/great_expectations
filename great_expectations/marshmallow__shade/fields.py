
'Field classes for various types of data.'
import collections
import copy
import datetime as dt
import decimal
import math
import numbers
import typing
import uuid
import warnings
from collections.abc import Mapping as _Mapping
from great_expectations.marshmallow__shade import class_registry, types, utils, validate
from great_expectations.marshmallow__shade.base import FieldABC, SchemaABC
from great_expectations.marshmallow__shade.exceptions import FieldInstanceResolutionError, StringNotCollectionError, ValidationError
from great_expectations.marshmallow__shade.utils import is_aware, is_collection
from great_expectations.marshmallow__shade.utils import missing as missing_
from great_expectations.marshmallow__shade.utils import resolve_field_instance
from great_expectations.marshmallow__shade.validate import Length, Validator
from great_expectations.marshmallow__shade.warnings import RemovedInMarshmallow4Warning
__all__ = ['Field', 'Raw', 'Nested', 'Mapping', 'Dict', 'List', 'Tuple', 'String', 'UUID', 'Number', 'Integer', 'Decimal', 'Boolean', 'Float', 'DateTime', 'NaiveDateTime', 'AwareDateTime', 'Time', 'Date', 'TimeDelta', 'Url', 'URL', 'Email', 'Method', 'Function', 'Str', 'Bool', 'Int', 'Constant', 'Pluck']
_T = typing.TypeVar('_T')

class Field(FieldABC):
    'Basic field from which other fields should extend. It applies no\n    formatting by default, and should only be used in cases where\n    data does not need to be formatted before being serialized or deserialized.\n    On error, the name of the field will be returned.\n\n    :param default: If set, this value will be used during serialization if the input value\n        is missing. If not set, the field will be excluded from the serialized output if the\n        input value is missing. May be a value or a callable.\n    :param missing: Default deserialization value for the field if the field is not\n        found in the input data. May be a value or a callable.\n    :param data_key: The name of the dict key in the external representation, i.e.\n        the input of `load` and the output of `dump`.\n        If `None`, the key will match the name of the field.\n    :param attribute: The name of the attribute to get the value from when serializing.\n        If `None`, assumes the attribute has the same name as the field.\n        Note: This should only be used for very specific use cases such as\n        outputting multiple fields for a single attribute. In most cases,\n        you should use ``data_key`` instead.\n    :param validate: Validator or collection of validators that are called\n        during deserialization. Validator takes a field\'s input value as\n        its only parameter and returns a boolean.\n        If it returns `False`, an :exc:`ValidationError` is raised.\n    :param required: Raise a :exc:`ValidationError` if the field value\n        is not supplied during deserialization.\n    :param allow_none: Set this to `True` if `None` should be considered a valid value during\n        validation/deserialization. If ``missing=None`` and ``allow_none`` is unset,\n        will default to ``True``. Otherwise, the default is ``False``.\n    :param load_only: If `True` skip this field during serialization, otherwise\n        its value will be present in the serialized data.\n    :param dump_only: If `True` skip this field during deserialization, otherwise\n        its value will be present in the deserialized object. In the context of an\n        HTTP API, this effectively marks the field as "read-only".\n    :param dict error_messages: Overrides for `Field.default_error_messages`.\n    :param metadata: Extra arguments to be stored as metadata.\n\n    .. versionchanged:: 2.0.0\n        Removed `error` parameter. Use ``error_messages`` instead.\n\n    .. versionchanged:: 2.0.0\n        Added `allow_none` parameter, which makes validation/deserialization of `None`\n        consistent across fields.\n\n    .. versionchanged:: 2.0.0\n        Added `load_only` and `dump_only` parameters, which allow field skipping\n        during the (de)serialization process.\n\n    .. versionchanged:: 2.0.0\n        Added `missing` parameter, which indicates the value for a field if the field\n        is not found during deserialization.\n\n    .. versionchanged:: 2.0.0\n        ``default`` value is only used if explicitly set. Otherwise, missing values\n        inputs are excluded from serialized output.\n\n    .. versionchanged:: 3.0.0b8\n        Add ``data_key`` parameter for the specifying the key in the input and\n        output data. This parameter replaced both ``load_from`` and ``dump_to``.\n    '
    _CHECK_ATTRIBUTE = True
    _creation_index = 0
    default_error_messages = {'required': 'Missing data for required field.', 'null': 'Field may not be null.', 'validator_failed': 'Invalid value.'}

    def __init__(self, *, default: typing.Any=missing_, missing: typing.Any=missing_, data_key: str=None, attribute: str=None, validate: typing.Union[(typing.Callable[([typing.Any], typing.Any)], typing.Iterable[typing.Callable[([typing.Any], typing.Any)]])]=None, required: bool=False, allow_none: bool=None, load_only: bool=False, dump_only: bool=False, error_messages: typing.Dict[(str, str)]=None, **metadata) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        self.default = default
        self.attribute = attribute
        self.data_key = data_key
        self.validate = validate
        if (validate is None):
            self.validators = []
        elif callable(validate):
            self.validators = [validate]
        elif utils.is_iterable_but_not_string(validate):
            self.validators = list(validate)
        else:
            raise ValueError("The 'validate' parameter must be a callable or a collection of callables.")
        self.allow_none = ((missing is None) if (allow_none is None) else allow_none)
        self.load_only = load_only
        self.dump_only = dump_only
        if ((required is True) and (missing is not missing_)):
            raise ValueError("'missing' must not be set for required fields.")
        self.required = required
        self.missing = missing
        self.metadata = metadata
        self._creation_index = Field._creation_index
        Field._creation_index += 1
        messages = {}
        for cls in reversed(self.__class__.__mro__):
            messages.update(getattr(cls, 'default_error_messages', {}))
        messages.update((error_messages or {}))
        self.error_messages = messages

    def __repr__(self) -> str:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return '<fields.{ClassName}(default={self.default!r}, attribute={self.attribute!r}, validate={self.validate}, required={self.required}, load_only={self.load_only}, dump_only={self.dump_only}, missing={self.missing}, allow_none={self.allow_none}, error_messages={self.error_messages})>'.format(ClassName=self.__class__.__name__, self=self)

    def __deepcopy__(self, memo):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return copy.copy(self)

    def get_value(self, obj, attr, accessor=None, default=missing_):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Return the value for a given key from an object.\n\n        :param object obj: The object to get the value from.\n        :param str attr: The attribute/key in `obj` to get the value from.\n        :param callable accessor: A callable used to retrieve the value of `attr` from\n            the object `obj`. Defaults to `marshmallow.utils.get_value`.\n        '
        attribute = getattr(self, 'attribute', None)
        accessor_func = (accessor or utils.get_value)
        check_key = (attr if (attribute is None) else attribute)
        return accessor_func(obj, check_key, default)

    def _validate(self, value) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Perform validation on ``value``. Raise a :exc:`ValidationError` if validation\n        does not succeed.\n        '
        errors = []
        kwargs = {}
        for validator in self.validators:
            try:
                r = validator(value)
                if ((not isinstance(validator, Validator)) and (r is False)):
                    raise self.make_error('validator_failed')
            except ValidationError as err:
                kwargs.update(err.kwargs)
                if isinstance(err.messages, dict):
                    errors.append(err.messages)
                else:
                    errors.extend(err.messages)
        if errors:
            raise ValidationError(errors, **kwargs)

    def make_error(self, key: str, **kwargs) -> ValidationError:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Helper method to make a `ValidationError` with an error message\n        from ``self.error_messages``.\n        '
        try:
            msg = self.error_messages[key]
        except KeyError as error:
            class_name = self.__class__.__name__
            message = 'ValidationError raised by `{class_name}`, but error key `{key}` does not exist in the `error_messages` dictionary.'.format(class_name=class_name, key=key)
            raise AssertionError(message) from error
        if isinstance(msg, (str, bytes)):
            msg = msg.format(**kwargs)
        return ValidationError(msg)

    def fail(self, key: str, **kwargs) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Helper method that raises a `ValidationError` with an error message\n        from ``self.error_messages``.\n\n        .. deprecated:: 3.0.0\n            Use `make_error <marshmallow.fields.Field.make_error>` instead.\n        '
        warnings.warn('`Field.fail` is deprecated. Use `raise self.make_error("{}", ...)` instead.'.format(key), RemovedInMarshmallow4Warning)
        raise self.make_error(key=key, **kwargs)

    def _validate_missing(self, value) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Validate missing values. Raise a :exc:`ValidationError` if\n        `value` should be considered missing.\n        '
        if (value is missing_):
            if (hasattr(self, 'required') and self.required):
                raise self.make_error('required')
        if (value is None):
            if (hasattr(self, 'allow_none') and (self.allow_none is not True)):
                raise self.make_error('null')

    def serialize(self, attr: str, obj: typing.Any, accessor: typing.Callable[([typing.Any, str, typing.Any], typing.Any)]=None, **kwargs):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        "Pulls the value for the given key from the object, applies the\n        field's formatting and returns the result.\n\n        :param attr: The attribute/key to get from the object.\n        :param obj: The object to access the attribute/key from.\n        :param accessor: Function used to access values from ``obj``.\n        :param kwargs: Field-specific keyword arguments.\n        "
        if self._CHECK_ATTRIBUTE:
            value = self.get_value(obj, attr, accessor=accessor)
            if ((value is missing_) and hasattr(self, 'default')):
                default = self.default
                value = (default() if callable(default) else default)
            if (value is missing_):
                return value
        else:
            value = None
        return self._serialize(value, attr, obj, **kwargs)

    def deserialize(self, value: typing.Any, attr: str=None, data: typing.Mapping[(str, typing.Any)]=None, **kwargs):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Deserialize ``value``.\n\n        :param value: The value to deserialize.\n        :param attr: The attribute/key in `data` to deserialize.\n        :param data: The raw input data passed to `Schema.load`.\n        :param kwargs: Field-specific keyword arguments.\n        :raise ValidationError: If an invalid value is passed or if a required value\n            is missing.\n        '
        self._validate_missing(value)
        if (value is missing_):
            _miss = self.missing
            return (_miss() if callable(_miss) else _miss)
        if ((getattr(self, 'allow_none', False) is True) and (value is None)):
            return None
        output = self._deserialize(value, attr, data, **kwargs)
        self._validate(output)
        return output

    def _bind_to_schema(self, field_name, schema) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Update field with values from its parent schema. Called by\n        :meth:`Schema._bind_field <marshmallow.Schema._bind_field>`.\n\n        :param str field_name: Field name set in schema.\n        :param Schema schema: Parent schema.\n        '
        self.parent = (self.parent or schema)
        self.name = (self.name or field_name)

    def _serialize(self, value: typing.Any, attr: str, obj: typing.Any, **kwargs):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        "Serializes ``value`` to a basic Python datatype. Noop by default.\n        Concrete :class:`Field` classes should implement this method.\n\n        Example: ::\n\n            class TitleCase(Field):\n                def _serialize(self, value, attr, obj, **kwargs):\n                    if not value:\n                        return ''\n                    return str(value).title()\n\n        :param value: The value to be serialized.\n        :param str attr: The attribute or key on the object to be serialized.\n        :param object obj: The object the value was pulled from.\n        :param dict kwargs: Field-specific keyword arguments.\n        :return: The serialized value\n        "
        return value

    def _deserialize(self, value: typing.Any, attr: typing.Optional[str], data: typing.Optional[typing.Mapping[(str, typing.Any)]], **kwargs):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Deserialize value. Concrete :class:`Field` classes should implement this method.\n\n        :param value: The value to be deserialized.\n        :param attr: The attribute/key in `data` to be deserialized.\n        :param data: The raw input data passed to the `Schema.load`.\n        :param kwargs: Field-specific keyword arguments.\n        :raise ValidationError: In case of formatting or validation failure.\n        :return: The deserialized value.\n\n        .. versionchanged:: 2.0.0\n            Added ``attr`` and ``data`` parameters.\n\n        .. versionchanged:: 3.0.0\n            Added ``**kwargs`` to signature.\n        '
        return value

    @property
    def context(self):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'The context dictionary for the parent :class:`Schema`.'
        return self.parent.context

    @property
    def root(self):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Reference to the `Schema` that this field belongs to even if it is buried in a\n        container field (e.g. `List`).\n        Return `None` for unbound fields.\n        '
        ret = self
        while hasattr(ret, 'parent'):
            ret = ret.parent
        return (ret if isinstance(ret, SchemaABC) else None)

class Raw(Field):
    'Field that applies no formatting.'

class Nested(Field):
    'Allows you to nest a :class:`Schema <marshmallow.Schema>`\n    inside a field.\n\n    Examples: ::\n\n        class ChildSchema(Schema):\n            id = fields.Str()\n            name = fields.Str()\n            # Use lambda functions when you need two-way nesting or self-nesting\n            parent = fields.Nested(lambda: ParentSchema(only=("id",)), dump_only=True)\n            siblings = fields.List(fields.Nested(lambda: ChildSchema(only=("id", "name"))))\n\n        class ParentSchema(Schema):\n            id = fields.Str()\n            children = fields.List(\n                fields.Nested(ChildSchema(only=("id", "parent", "siblings")))\n            )\n            spouse = fields.Nested(lambda: ParentSchema(only=("id",)))\n\n    When passing a `Schema <marshmallow.Schema>` instance as the first argument,\n    the instance\'s ``exclude``, ``only``, and ``many`` attributes will be respected.\n\n    Therefore, when passing the ``exclude``, ``only``, or ``many`` arguments to `fields.Nested`,\n    you should pass a `Schema <marshmallow.Schema>` class (not an instance) as the first argument.\n\n    ::\n\n        # Yes\n        author = fields.Nested(UserSchema, only=(\'id\', \'name\'))\n\n        # No\n        author = fields.Nested(UserSchema(), only=(\'id\', \'name\'))\n\n    :param nested: `Schema` instance, class, class name (string), or callable that returns a `Schema` instance.\n    :param exclude: A list or tuple of fields to exclude.\n    :param only: A list or tuple of fields to marshal. If `None`, all fields are marshalled.\n        This parameter takes precedence over ``exclude``.\n    :param many: Whether the field is a collection of objects.\n    :param unknown: Whether to exclude, include, or raise an error for unknown\n        fields in the data. Use `EXCLUDE`, `INCLUDE` or `RAISE`.\n    :param kwargs: The same keyword arguments that :class:`Field` receives.\n    '
    default_error_messages = {'type': 'Invalid type.'}

    def __init__(self, nested: typing.Union[(SchemaABC, type, str, typing.Callable[([], SchemaABC)])], *, default: typing.Any=missing_, only: types.StrSequenceOrSet=None, exclude: types.StrSequenceOrSet=(), many: bool=False, unknown: str=None, **kwargs) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        if ((only is not None) and (not is_collection(only))):
            raise StringNotCollectionError('"only" should be a collection of strings.')
        if (not is_collection(exclude)):
            raise StringNotCollectionError('"exclude" should be a collection of strings.')
        if (nested == 'self'):
            warnings.warn("Passing 'self' to `Nested` is deprecated. Use `Nested(lambda: MySchema(...))` instead.", RemovedInMarshmallow4Warning)
        self.nested = nested
        self.only = only
        self.exclude = exclude
        self.many = many
        self.unknown = unknown
        self._schema = None
        super().__init__(default=default, **kwargs)

    @property
    def schema(self):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'The nested Schema object.\n\n        .. versionchanged:: 1.0.0\n            Renamed from `serializer` to `schema`.\n        '
        if (not self._schema):
            context = getattr(self.parent, 'context', {})
            if (callable(self.nested) and (not isinstance(self.nested, type))):
                nested = self.nested()
            else:
                nested = self.nested
            if isinstance(nested, SchemaABC):
                self._schema = copy.copy(nested)
                self._schema.context.update(context)
                set_class = self._schema.set_class
                if (self.only is not None):
                    if (self._schema.only is not None):
                        original = self._schema.only
                    else:
                        original = self._schema.fields.keys()
                    self._schema.only = (set_class(self.only) & set_class(original))
                if self.exclude:
                    original = self._schema.exclude
                    self._schema.exclude = (set_class(self.exclude) | set_class(original))
                self._schema._init_fields()
            else:
                if (isinstance(nested, type) and issubclass(nested, SchemaABC)):
                    schema_class = nested
                elif (not isinstance(nested, (str, bytes))):
                    raise ValueError('`Nested` fields must be passed a `Schema`, not {}.'.format(nested.__class__))
                elif (nested == 'self'):
                    schema_class = self.root.__class__
                else:
                    schema_class = class_registry.get_class(nested)
                self._schema = schema_class(many=self.many, only=self.only, exclude=self.exclude, context=context, load_only=self._nested_normalized_option('load_only'), dump_only=self._nested_normalized_option('dump_only'))
        return self._schema

    def _nested_normalized_option(self, option_name: str) -> typing.List[str]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        nested_field = f'{self.name}.'
        return [field.split(nested_field, 1)[1] for field in getattr(self.root, option_name, set()) if field.startswith(nested_field)]

    def _serialize(self, nested_obj, attr, obj, **kwargs):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        schema = self.schema
        if (nested_obj is None):
            return None
        many = (schema.many or self.many)
        return schema.dump(nested_obj, many=many)

    def _test_collection(self, value) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        many = (self.schema.many or self.many)
        if (many and (not utils.is_collection(value))):
            raise self.make_error('type', input=value, type=value.__class__.__name__)

    def _load(self, value, data, partial=None):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        try:
            valid_data = self.schema.load(value, unknown=self.unknown, partial=partial)
        except ValidationError as error:
            raise ValidationError(error.messages, valid_data=error.valid_data) from error
        return valid_data

    def _deserialize(self, value, attr, data, partial=None, **kwargs):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Same as :meth:`Field._deserialize` with additional ``partial`` argument.\n\n        :param bool|tuple partial: For nested schemas, the ``partial``\n            parameter passed to `Schema.load`.\n\n        .. versionchanged:: 3.0.0\n            Add ``partial`` parameter.\n        '
        self._test_collection(value)
        return self._load(value, data, partial=partial)

class Pluck(Nested):
    'Allows you to replace nested data with one of the data\'s fields.\n\n    Example: ::\n\n        from great_expectations.marshmallow__shade import Schema, fields\n\n        class ArtistSchema(Schema):\n            id = fields.Int()\n            name = fields.Str()\n\n        class AlbumSchema(Schema):\n            artist = fields.Pluck(ArtistSchema, \'id\')\n\n\n        in_data = {\'artist\': 42}\n        loaded = AlbumSchema().load(in_data) # => {\'artist\': {\'id\': 42}}\n        dumped = AlbumSchema().dump(loaded)  # => {\'artist\': 42}\n\n    :param Schema nested: The Schema class or class name (string)\n        to nest, or ``"self"`` to nest the :class:`Schema` within itself.\n    :param str field_name: The key to pluck a value from.\n    :param kwargs: The same keyword arguments that :class:`Nested` receives.\n    '

    def __init__(self, nested: typing.Union[(SchemaABC, type, str, typing.Callable[([], SchemaABC)])], field_name: str, **kwargs) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        super().__init__(nested, only=(field_name,), **kwargs)
        self.field_name = field_name

    @property
    def _field_data_key(self):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        only_field = self.schema.fields[self.field_name]
        return (only_field.data_key or self.field_name)

    def _serialize(self, nested_obj, attr, obj, **kwargs):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        ret = super()._serialize(nested_obj, attr, obj, **kwargs)
        if (ret is None):
            return None
        if self.many:
            return utils.pluck(ret, key=self._field_data_key)
        return ret[self._field_data_key]

    def _deserialize(self, value, attr, data, partial=None, **kwargs):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        self._test_collection(value)
        if self.many:
            value = [{self._field_data_key: v} for v in value]
        else:
            value = {self._field_data_key: value}
        return self._load(value, data, partial=partial)

class List(Field):
    'A list field, composed with another `Field` class or\n    instance.\n\n    Example: ::\n\n        numbers = fields.List(fields.Float())\n\n    :param cls_or_instance: A field class or instance.\n    :param kwargs: The same keyword arguments that :class:`Field` receives.\n\n    .. versionchanged:: 2.0.0\n        The ``allow_none`` parameter now applies to deserialization and\n        has the same semantics as the other fields.\n\n    .. versionchanged:: 3.0.0rc9\n        Does not serialize scalar values to single-item lists.\n    '
    default_error_messages = {'invalid': 'Not a valid list.'}

    def __init__(self, cls_or_instance: typing.Union[(Field, type)], **kwargs) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        super().__init__(**kwargs)
        try:
            self.inner = resolve_field_instance(cls_or_instance)
        except FieldInstanceResolutionError as error:
            raise ValueError('The list elements must be a subclass or instance of marshmallow.base.FieldABC.') from error
        if isinstance(self.inner, Nested):
            self.only = self.inner.only
            self.exclude = self.inner.exclude

    def _bind_to_schema(self, field_name, schema) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        super()._bind_to_schema(field_name, schema)
        self.inner = copy.deepcopy(self.inner)
        self.inner._bind_to_schema(field_name, self)
        if isinstance(self.inner, Nested):
            self.inner.only = self.only
            self.inner.exclude = self.exclude

    def _serialize(self, value, attr, obj, **kwargs) -> typing.Optional[typing.List[typing.Any]]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        if (value is None):
            return None
        return [self.inner._serialize(each, attr, obj, **kwargs) for each in value]

    def _deserialize(self, value, attr, data, **kwargs) -> typing.List[typing.Any]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        if (not utils.is_collection(value)):
            raise self.make_error('invalid')
        result = []
        errors = {}
        for (idx, each) in enumerate(value):
            try:
                result.append(self.inner.deserialize(each, **kwargs))
            except ValidationError as error:
                if (error.valid_data is not None):
                    result.append(error.valid_data)
                errors.update({idx: error.messages})
        if errors:
            raise ValidationError(errors, valid_data=result)
        return result

class Tuple(Field):
    'A tuple field, composed of a fixed number of other `Field` classes or\n    instances\n\n    Example: ::\n\n        row = Tuple((fields.String(), fields.Integer(), fields.Float()))\n\n    .. note::\n        Because of the structured nature of `collections.namedtuple` and\n        `typing.NamedTuple`, using a Schema within a Nested field for them is\n        more appropriate than using a `Tuple` field.\n\n    :param Iterable[Field] tuple_fields: An iterable of field classes or\n        instances.\n    :param kwargs: The same keyword arguments that :class:`Field` receives.\n\n    .. versionadded:: 3.0.0rc4\n    '
    default_error_messages = {'invalid': 'Not a valid tuple.'}

    def __init__(self, tuple_fields, *args, **kwargs) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        super().__init__(*args, **kwargs)
        if (not utils.is_collection(tuple_fields)):
            raise ValueError('tuple_fields must be an iterable of Field classes or instances.')
        try:
            self.tuple_fields = [resolve_field_instance(cls_or_instance) for cls_or_instance in tuple_fields]
        except FieldInstanceResolutionError as error:
            raise ValueError('Elements of "tuple_fields" must be subclasses or instances of marshmallow.base.FieldABC.') from error
        self.validate_length = Length(equal=len(self.tuple_fields))

    def _bind_to_schema(self, field_name, schema) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        super()._bind_to_schema(field_name, schema)
        new_tuple_fields = []
        for field in self.tuple_fields:
            field = copy.deepcopy(field)
            field._bind_to_schema(field_name, self)
            new_tuple_fields.append(field)
        self.tuple_fields = new_tuple_fields

    def _serialize(self, value, attr, obj, **kwargs) -> typing.Optional[typing.Tuple]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        if (value is None):
            return None
        return tuple((field._serialize(each, attr, obj, **kwargs) for (field, each) in zip(self.tuple_fields, value)))

    def _deserialize(self, value, attr, data, **kwargs) -> typing.Tuple:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        if (not utils.is_collection(value)):
            raise self.make_error('invalid')
        self.validate_length(value)
        result = []
        errors = {}
        for (idx, (field, each)) in enumerate(zip(self.tuple_fields, value)):
            try:
                result.append(field.deserialize(each, **kwargs))
            except ValidationError as error:
                if (error.valid_data is not None):
                    result.append(error.valid_data)
                errors.update({idx: error.messages})
        if errors:
            raise ValidationError(errors, valid_data=result)
        return tuple(result)

class String(Field):
    'A string field.\n\n    :param kwargs: The same keyword arguments that :class:`Field` receives.\n    '
    default_error_messages = {'invalid': 'Not a valid string.', 'invalid_utf8': 'Not a valid utf-8 string.'}

    def _serialize(self, value, attr, obj, **kwargs) -> typing.Optional[str]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        if (value is None):
            return None
        return utils.ensure_text_type(value)

    def _deserialize(self, value, attr, data, **kwargs) -> typing.Any:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        if (not isinstance(value, (str, bytes))):
            raise self.make_error('invalid')
        try:
            return utils.ensure_text_type(value)
        except UnicodeDecodeError as error:
            raise self.make_error('invalid_utf8') from error

class UUID(String):
    'A UUID field.'
    default_error_messages = {'invalid_uuid': 'Not a valid UUID.'}

    def _validated(self, value) -> typing.Optional[uuid.UUID]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Format the value or raise a :exc:`ValidationError` if an error occurs.'
        if (value is None):
            return None
        if isinstance(value, uuid.UUID):
            return value
        try:
            if (isinstance(value, bytes) and (len(value) == 16)):
                return uuid.UUID(bytes=value)
            else:
                return uuid.UUID(value)
        except (ValueError, AttributeError, TypeError) as error:
            raise self.make_error('invalid_uuid') from error

    def _deserialize(self, value, attr, data, **kwargs) -> typing.Optional[uuid.UUID]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return self._validated(value)

class Number(Field):
    'Base class for number fields.\n\n    :param bool as_string: If `True`, format the serialized value as a string.\n    :param kwargs: The same keyword arguments that :class:`Field` receives.\n    '
    num_type = float
    default_error_messages = {'invalid': 'Not a valid number.', 'too_large': 'Number too large.'}

    def __init__(self, *, as_string: bool=False, **kwargs) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        self.as_string = as_string
        super().__init__(**kwargs)

    def _format_num(self, value) -> typing.Any:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        "Return the number value for value, given this field's `num_type`."
        return self.num_type(value)

    def _validated(self, value) -> typing.Optional[_T]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Format the value or raise a :exc:`ValidationError` if an error occurs.'
        if (value is None):
            return None
        if ((value is True) or (value is False)):
            raise self.make_error('invalid', input=value)
        try:
            return self._format_num(value)
        except (TypeError, ValueError) as error:
            raise self.make_error('invalid', input=value) from error
        except OverflowError as error:
            raise self.make_error('too_large', input=value) from error

    def _to_string(self, value) -> str:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return str(value)

    def _serialize(self, value, attr, obj, **kwargs) -> typing.Optional[typing.Union[(str, _T)]]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        "Return a string if `self.as_string=True`, otherwise return this field's `num_type`."
        if (value is None):
            return None
        ret = self._format_num(value)
        return (self._to_string(ret) if self.as_string else ret)

    def _deserialize(self, value, attr, data, **kwargs) -> typing.Optional[_T]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return self._validated(value)

class Integer(Number):
    'An integer field.\n\n    :param strict: If `True`, only integer types are valid.\n        Otherwise, any value castable to `int` is valid.\n    :param kwargs: The same keyword arguments that :class:`Number` receives.\n    '
    num_type = int
    default_error_messages = {'invalid': 'Not a valid integer.'}

    def __init__(self, *, strict: bool=False, **kwargs) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        self.strict = strict
        super().__init__(**kwargs)

    def _validated(self, value):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        if self.strict:
            if (isinstance(value, numbers.Number) and isinstance(value, numbers.Integral)):
                return super()._validated(value)
            raise self.make_error('invalid', input=value)
        return super()._validated(value)

class Float(Number):
    'A double as an IEEE-754 double precision string.\n\n    :param bool allow_nan: If `True`, `NaN`, `Infinity` and `-Infinity` are allowed,\n        even though they are illegal according to the JSON specification.\n    :param bool as_string: If `True`, format the value as a string.\n    :param kwargs: The same keyword arguments that :class:`Number` receives.\n    '
    num_type = float
    default_error_messages = {'special': 'Special numeric values (nan or infinity) are not permitted.'}

    def __init__(self, *, allow_nan: bool=False, as_string: bool=False, **kwargs) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        self.allow_nan = allow_nan
        super().__init__(as_string=as_string, **kwargs)

    def _validated(self, value):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        num = super()._validated(value)
        if (self.allow_nan is False):
            if (math.isnan(num) or (num == float('inf')) or (num == float('-inf'))):
                raise self.make_error('special')
        return num

class Decimal(Number):
    "A field that (de)serializes to the Python ``decimal.Decimal`` type.\n    It's safe to use when dealing with money values, percentages, ratios\n    or other numbers where precision is critical.\n\n    .. warning::\n\n        This field serializes to a `decimal.Decimal` object by default. If you need\n        to render your data as JSON, keep in mind that the `json` module from the\n        standard library does not encode `decimal.Decimal`. Therefore, you must use\n        a JSON library that can handle decimals, such as `simplejson`, or serialize\n        to a string by passing ``as_string=True``.\n\n    .. warning::\n\n        If a JSON `float` value is passed to this field for deserialization it will\n        first be cast to its corresponding `string` value before being deserialized\n        to a `decimal.Decimal` object. The default `__str__` implementation of the\n        built-in Python `float` type may apply a destructive transformation upon\n        its input data and therefore cannot be relied upon to preserve precision.\n        To avoid this, you can instead pass a JSON `string` to be deserialized\n        directly.\n\n    :param places: How many decimal places to quantize the value. If `None`, does\n        not quantize the value.\n    :param rounding: How to round the value during quantize, for example\n        `decimal.ROUND_UP`. If `None`, uses the rounding value from\n        the current thread's context.\n    :param allow_nan: If `True`, `NaN`, `Infinity` and `-Infinity` are allowed,\n        even though they are illegal according to the JSON specification.\n    :param as_string: If `True`, serialize to a string instead of a Python\n        `decimal.Decimal` type.\n    :param kwargs: The same keyword arguments that :class:`Number` receives.\n\n    .. versionadded:: 1.2.0\n    "
    num_type = decimal.Decimal
    default_error_messages = {'special': 'Special numeric values (nan or infinity) are not permitted.'}

    def __init__(self, places: int=None, rounding: str=None, *, allow_nan: bool=False, as_string: bool=False, **kwargs) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        self.places = (decimal.Decimal((0, (1,), (- places))) if (places is not None) else None)
        self.rounding = rounding
        self.allow_nan = allow_nan
        super().__init__(as_string=as_string, **kwargs)

    def _format_num(self, value):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        num = decimal.Decimal(str(value))
        if self.allow_nan:
            if num.is_nan():
                return decimal.Decimal('NaN')
        if ((self.places is not None) and num.is_finite()):
            num = num.quantize(self.places, rounding=self.rounding)
        return num

    def _validated(self, value):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        try:
            num = super()._validated(value)
        except decimal.InvalidOperation as error:
            raise self.make_error('invalid') from error
        if ((not self.allow_nan) and (num.is_nan() or num.is_infinite())):
            raise self.make_error('special')
        return num

    def _to_string(self, value):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return format(value, 'f')

class Boolean(Field):
    'A boolean field.\n\n    :param truthy: Values that will (de)serialize to `True`. If an empty\n        set, any non-falsy value will deserialize to `True`. If `None`,\n        `marshmallow.fields.Boolean.truthy` will be used.\n    :param falsy: Values that will (de)serialize to `False`. If `None`,\n        `marshmallow.fields.Boolean.falsy` will be used.\n    :param kwargs: The same keyword arguments that :class:`Field` receives.\n    '
    truthy = {'t', 'T', 'true', 'True', 'TRUE', 'on', 'On', 'ON', 'y', 'Y', 'yes', 'Yes', 'YES', '1', 1, True}
    falsy = {'f', 'F', 'false', 'False', 'FALSE', 'off', 'Off', 'OFF', 'n', 'N', 'no', 'No', 'NO', '0', 0, 0.0, False}
    default_error_messages = {'invalid': 'Not a valid boolean.'}

    def __init__(self, *, truthy: typing.Set=None, falsy: typing.Set=None, **kwargs) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        super().__init__(**kwargs)
        if (truthy is not None):
            self.truthy = set(truthy)
        if (falsy is not None):
            self.falsy = set(falsy)

    def _serialize(self, value, attr, obj, **kwargs):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        if (value is None):
            return None
        try:
            if (value in self.truthy):
                return True
            elif (value in self.falsy):
                return False
        except TypeError:
            pass
        return bool(value)

    def _deserialize(self, value, attr, data, **kwargs):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        if (not self.truthy):
            return bool(value)
        else:
            try:
                if (value in self.truthy):
                    return True
                elif (value in self.falsy):
                    return False
            except TypeError as error:
                raise self.make_error('invalid', input=value) from error
        raise self.make_error('invalid', input=value)

class DateTime(Field):
    'A formatted datetime string.\n\n    Example: ``\'2014-12-22T03:12:58.019077+00:00\'``\n\n    :param format: Either ``"rfc"`` (for RFC822), ``"iso"`` (for ISO8601),\n        or a date format string. If `None`, defaults to "iso".\n    :param kwargs: The same keyword arguments that :class:`Field` receives.\n\n    .. versionchanged:: 3.0.0rc9\n        Does not modify timezone information on (de)serialization.\n    '
    SERIALIZATION_FUNCS = {'iso': utils.isoformat, 'iso8601': utils.isoformat, 'rfc': utils.rfcformat, 'rfc822': utils.rfcformat}
    DESERIALIZATION_FUNCS = {'iso': utils.from_iso_datetime, 'iso8601': utils.from_iso_datetime, 'rfc': utils.from_rfc, 'rfc822': utils.from_rfc}
    DEFAULT_FORMAT = 'iso'
    OBJ_TYPE = 'datetime'
    SCHEMA_OPTS_VAR_NAME = 'datetimeformat'
    default_error_messages = {'invalid': 'Not a valid {obj_type}.', 'invalid_awareness': 'Not a valid {awareness} {obj_type}.', 'format': '"{input}" cannot be formatted as a {obj_type}.'}

    def __init__(self, format: str=None, **kwargs) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        super().__init__(**kwargs)
        self.format = format

    def _bind_to_schema(self, field_name, schema) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        super()._bind_to_schema(field_name, schema)
        self.format = (self.format or getattr(self.root.opts, self.SCHEMA_OPTS_VAR_NAME) or self.DEFAULT_FORMAT)

    def _serialize(self, value, attr, obj, **kwargs):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        if (value is None):
            return None
        data_format = (self.format or self.DEFAULT_FORMAT)
        format_func = self.SERIALIZATION_FUNCS.get(data_format)
        if format_func:
            return format_func(value)
        else:
            return value.strftime(data_format)

    def _deserialize(self, value, attr, data, **kwargs):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        if (not value):
            raise self.make_error('invalid', input=value, obj_type=self.OBJ_TYPE)
        data_format = (self.format or self.DEFAULT_FORMAT)
        func = self.DESERIALIZATION_FUNCS.get(data_format)
        if func:
            try:
                return func(value)
            except (TypeError, AttributeError, ValueError) as error:
                raise self.make_error('invalid', input=value, obj_type=self.OBJ_TYPE) from error
        else:
            try:
                return self._make_object_from_format(value, data_format)
            except (TypeError, AttributeError, ValueError) as error:
                raise self.make_error('invalid', input=value, obj_type=self.OBJ_TYPE) from error

    @staticmethod
    def _make_object_from_format(value, data_format):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return dt.datetime.strptime(value, data_format)

class NaiveDateTime(DateTime):
    'A formatted naive datetime string.\n\n    :param format: See :class:`DateTime`.\n    :param timezone: Used on deserialization. If `None`,\n        aware datetimes are rejected. If not `None`, aware datetimes are\n        converted to this timezone before their timezone information is\n        removed.\n    :param kwargs: The same keyword arguments that :class:`Field` receives.\n\n    .. versionadded:: 3.0.0rc9\n    '
    AWARENESS = 'naive'

    def __init__(self, format: str=None, *, timezone: dt.timezone=None, **kwargs) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        super().__init__(format=format, **kwargs)
        self.timezone = timezone

    def _deserialize(self, value, attr, data, **kwargs):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        ret = super()._deserialize(value, attr, data, **kwargs)
        if is_aware(ret):
            if (self.timezone is None):
                raise self.make_error('invalid_awareness', awareness=self.AWARENESS, obj_type=self.OBJ_TYPE)
            ret = ret.astimezone(self.timezone).replace(tzinfo=None)
        return ret

class AwareDateTime(DateTime):
    'A formatted aware datetime string.\n\n    :param format: See :class:`DateTime`.\n    :param default_timezone: Used on deserialization. If `None`, naive\n        datetimes are rejected. If not `None`, naive datetimes are set this\n        timezone.\n    :param kwargs: The same keyword arguments that :class:`Field` receives.\n\n    .. versionadded:: 3.0.0rc9\n    '
    AWARENESS = 'aware'

    def __init__(self, format: str=None, *, default_timezone: dt.timezone=None, **kwargs) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        super().__init__(format=format, **kwargs)
        self.default_timezone = default_timezone

    def _deserialize(self, value, attr, data, **kwargs):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        ret = super()._deserialize(value, attr, data, **kwargs)
        if (not is_aware(ret)):
            if (self.default_timezone is None):
                raise self.make_error('invalid_awareness', awareness=self.AWARENESS, obj_type=self.OBJ_TYPE)
            ret = ret.replace(tzinfo=self.default_timezone)
        return ret

class Time(Field):
    'ISO8601-formatted time string.\n\n    :param kwargs: The same keyword arguments that :class:`Field` receives.\n    '
    default_error_messages = {'invalid': 'Not a valid time.', 'format': '"{input}" cannot be formatted as a time.'}

    def _serialize(self, value, attr, obj, **kwargs):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        if (value is None):
            return None
        ret = value.isoformat()
        if value.microsecond:
            return ret[:15]
        return ret

    def _deserialize(self, value, attr, data, **kwargs):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Deserialize an ISO8601-formatted time to a :class:`datetime.time` object.'
        if (not value):
            raise self.make_error('invalid')
        try:
            return utils.from_iso_time(value)
        except (AttributeError, TypeError, ValueError) as error:
            raise self.make_error('invalid') from error

class Date(DateTime):
    'ISO8601-formatted date string.\n\n    :param format: Either ``"iso"`` (for ISO8601) or a date format string.\n        If `None`, defaults to "iso".\n    :param kwargs: The same keyword arguments that :class:`Field` receives.\n    '
    default_error_messages = {'invalid': 'Not a valid date.', 'format': '"{input}" cannot be formatted as a date.'}
    SERIALIZATION_FUNCS = {'iso': utils.to_iso_date, 'iso8601': utils.to_iso_date}
    DESERIALIZATION_FUNCS = {'iso': utils.from_iso_date, 'iso8601': utils.from_iso_date}
    DEFAULT_FORMAT = 'iso'
    OBJ_TYPE = 'date'
    SCHEMA_OPTS_VAR_NAME = 'dateformat'

    @staticmethod
    def _make_object_from_format(value, data_format):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return dt.datetime.strptime(value, data_format).date()

class TimeDelta(Field):
    "A field that (de)serializes a :class:`datetime.timedelta` object to an\n    integer and vice versa. The integer can represent the number of days,\n    seconds or microseconds.\n\n    :param precision: Influences how the integer is interpreted during\n        (de)serialization. Must be 'days', 'seconds', 'microseconds',\n        'milliseconds', 'minutes', 'hours' or 'weeks'.\n    :param kwargs: The same keyword arguments that :class:`Field` receives.\n\n    .. versionchanged:: 2.0.0\n        Always serializes to an integer value to avoid rounding errors.\n        Add `precision` parameter.\n    "
    DAYS = 'days'
    SECONDS = 'seconds'
    MICROSECONDS = 'microseconds'
    MILLISECONDS = 'milliseconds'
    MINUTES = 'minutes'
    HOURS = 'hours'
    WEEKS = 'weeks'
    default_error_messages = {'invalid': 'Not a valid period of time.', 'format': '{input!r} cannot be formatted as a timedelta.'}

    def __init__(self, precision: str=SECONDS, **kwargs) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        precision = precision.lower()
        units = (self.DAYS, self.SECONDS, self.MICROSECONDS, self.MILLISECONDS, self.MINUTES, self.HOURS, self.WEEKS)
        if (precision not in units):
            msg = 'The precision must be {} or "{}".'.format(', '.join([f'"{each}"' for each in units[:(- 1)]]), units[(- 1)])
            raise ValueError(msg)
        self.precision = precision
        super().__init__(**kwargs)

    def _serialize(self, value, attr, obj, **kwargs):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        if (value is None):
            return None
        base_unit = dt.timedelta(**{self.precision: 1})
        return int((value.total_seconds() / base_unit.total_seconds()))

    def _deserialize(self, value, attr, data, **kwargs):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        try:
            value = int(value)
        except (TypeError, ValueError) as error:
            raise self.make_error('invalid') from error
        kwargs = {self.precision: value}
        try:
            return dt.timedelta(**kwargs)
        except OverflowError as error:
            raise self.make_error('invalid') from error

class Mapping(Field):
    'An abstract class for objects with key-value pairs.\n\n    :param keys: A field class or instance for dict keys.\n    :param values: A field class or instance for dict values.\n    :param kwargs: The same keyword arguments that :class:`Field` receives.\n\n    .. note::\n        When the structure of nested data is not known, you may omit the\n        `keys` and `values` arguments to prevent content validation.\n\n    .. versionadded:: 3.0.0rc4\n    '
    mapping_type = dict
    default_error_messages = {'invalid': 'Not a valid mapping type.'}

    def __init__(self, keys: typing.Union[(Field, type)]=None, values: typing.Union[(Field, type)]=None, **kwargs) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        super().__init__(**kwargs)
        if (keys is None):
            self.key_field = None
        else:
            try:
                self.key_field = resolve_field_instance(keys)
            except FieldInstanceResolutionError as error:
                raise ValueError('"keys" must be a subclass or instance of marshmallow.base.FieldABC.') from error
        if (values is None):
            self.value_field = None
        else:
            try:
                self.value_field = resolve_field_instance(values)
            except FieldInstanceResolutionError as error:
                raise ValueError('"values" must be a subclass or instance of marshmallow.base.FieldABC.') from error
            if isinstance(self.value_field, Nested):
                self.only = self.value_field.only
                self.exclude = self.value_field.exclude

    def _bind_to_schema(self, field_name, schema) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        super()._bind_to_schema(field_name, schema)
        if self.value_field:
            self.value_field = copy.deepcopy(self.value_field)
            self.value_field._bind_to_schema(field_name, self)
        if isinstance(self.value_field, Nested):
            self.value_field.only = self.only
            self.value_field.exclude = self.exclude
        if self.key_field:
            self.key_field = copy.deepcopy(self.key_field)
            self.key_field._bind_to_schema(field_name, self)

    def _serialize(self, value, attr, obj, **kwargs):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        if (value is None):
            return None
        if ((not self.value_field) and (not self.key_field)):
            return value
        if (self.key_field is None):
            keys = {k: k for k in value.keys()}
        else:
            keys = {k: self.key_field._serialize(k, None, None, **kwargs) for k in value.keys()}
        result = self.mapping_type()
        if (self.value_field is None):
            for (k, v) in value.items():
                if (k in keys):
                    result[keys[k]] = v
        else:
            for (k, v) in value.items():
                result[keys[k]] = self.value_field._serialize(v, None, None, **kwargs)
        return result

    def _deserialize(self, value, attr, data, **kwargs):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        if (not isinstance(value, _Mapping)):
            raise self.make_error('invalid')
        if ((not self.value_field) and (not self.key_field)):
            return value
        errors = collections.defaultdict(dict)
        if (self.key_field is None):
            keys = {k: k for k in value.keys()}
        else:
            keys = {}
            for key in value.keys():
                try:
                    keys[key] = self.key_field.deserialize(key, **kwargs)
                except ValidationError as error:
                    errors[key]['key'] = error.messages
        result = self.mapping_type()
        if (self.value_field is None):
            for (k, v) in value.items():
                if (k in keys):
                    result[keys[k]] = v
        else:
            for (key, val) in value.items():
                try:
                    deser_val = self.value_field.deserialize(val, **kwargs)
                except ValidationError as error:
                    errors[key]['value'] = error.messages
                    if ((error.valid_data is not None) and (key in keys)):
                        result[keys[key]] = error.valid_data
                else:
                    if (key in keys):
                        result[keys[key]] = deser_val
        if errors:
            raise ValidationError(errors, valid_data=result)
        return result

class Dict(Mapping):
    'A dict field. Supports dicts and dict-like objects. Extends\n    Mapping with dict as the mapping_type.\n\n    Example: ::\n\n        numbers = fields.Dict(keys=fields.Str(), values=fields.Float())\n\n    :param kwargs: The same keyword arguments that :class:`Mapping` receives.\n\n    .. versionadded:: 2.1.0\n    '
    mapping_type = dict

class Url(String):
    'A validated URL field. Validation occurs during both serialization and\n    deserialization.\n\n    :param default: Default value for the field if the attribute is not set.\n    :param relative: Whether to allow relative URLs.\n    :param require_tld: Whether to reject non-FQDN hostnames.\n    :param schemes: Valid schemes. By default, ``http``, ``https``,\n        ``ftp``, and ``ftps`` are allowed.\n    :param kwargs: The same keyword arguments that :class:`String` receives.\n    '
    default_error_messages = {'invalid': 'Not a valid URL.'}

    def __init__(self, *, relative: bool=False, schemes: types.StrSequenceOrSet=None, require_tld: bool=True, **kwargs) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        super().__init__(**kwargs)
        self.relative = relative
        self.require_tld = require_tld
        validator = validate.URL(relative=self.relative, schemes=schemes, require_tld=self.require_tld, error=self.error_messages['invalid'])
        self.validators.insert(0, validator)

class Email(String):
    'A validated email field. Validation occurs during both serialization and\n    deserialization.\n\n    :param args: The same positional arguments that :class:`String` receives.\n    :param kwargs: The same keyword arguments that :class:`String` receives.\n    '
    default_error_messages = {'invalid': 'Not a valid email address.'}

    def __init__(self, *args, **kwargs) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        super().__init__(*args, **kwargs)
        validator = validate.Email(error=self.error_messages['invalid'])
        self.validators.insert(0, validator)

class Method(Field):
    'A field that takes the value returned by a `Schema` method.\n\n    :param str serialize: The name of the Schema method from which\n        to retrieve the value. The method must take an argument ``obj``\n        (in addition to self) that is the object to be serialized.\n    :param str deserialize: Optional name of the Schema method for deserializing\n        a value The method must take a single argument ``value``, which is the\n        value to deserialize.\n\n    .. versionchanged:: 2.0.0\n        Removed optional ``context`` parameter on methods. Use ``self.context`` instead.\n\n    .. versionchanged:: 2.3.0\n        Deprecated ``method_name`` parameter in favor of ``serialize`` and allow\n        ``serialize`` to not be passed at all.\n\n    .. versionchanged:: 3.0.0\n        Removed ``method_name`` parameter.\n    '
    _CHECK_ATTRIBUTE = False

    def __init__(self, serialize: str=None, deserialize: str=None, **kwargs) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        kwargs['dump_only'] = (bool(serialize) and (not bool(deserialize)))
        kwargs['load_only'] = (bool(deserialize) and (not bool(serialize)))
        super().__init__(**kwargs)
        self.serialize_method_name = serialize
        self.deserialize_method_name = deserialize

    def _serialize(self, value, attr, obj, **kwargs):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        if (not self.serialize_method_name):
            return missing_
        method = utils.callable_or_raise(getattr(self.parent, self.serialize_method_name, None))
        return method(obj)

    def _deserialize(self, value, attr, data, **kwargs):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        if self.deserialize_method_name:
            method = utils.callable_or_raise(getattr(self.parent, self.deserialize_method_name, None))
            return method(value)
        return value

class Function(Field):
    'A field that takes the value returned by a function.\n\n    :param serialize: A callable from which to retrieve the value.\n        The function must take a single argument ``obj`` which is the object\n        to be serialized. It can also optionally take a ``context`` argument,\n        which is a dictionary of context variables passed to the serializer.\n        If no callable is provided then the ```load_only``` flag will be set\n        to True.\n    :param deserialize: A callable from which to retrieve the value.\n        The function must take a single argument ``value`` which is the value\n        to be deserialized. It can also optionally take a ``context`` argument,\n        which is a dictionary of context variables passed to the deserializer.\n        If no callable is provided then ```value``` will be passed through\n        unchanged.\n\n    .. versionchanged:: 2.3.0\n        Deprecated ``func`` parameter in favor of ``serialize``.\n\n    .. versionchanged:: 3.0.0a1\n        Removed ``func`` parameter.\n    '
    _CHECK_ATTRIBUTE = False

    def __init__(self, serialize: typing.Union[(typing.Callable[([typing.Any], typing.Any)], typing.Callable[([typing.Any, typing.Dict], typing.Any)])]=None, deserialize: typing.Union[(typing.Callable[([typing.Any], typing.Any)], typing.Callable[([typing.Any, typing.Dict], typing.Any)])]=None, **kwargs) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        kwargs['dump_only'] = (bool(serialize) and (not bool(deserialize)))
        kwargs['load_only'] = (bool(deserialize) and (not bool(serialize)))
        super().__init__(**kwargs)
        self.serialize_func = (serialize and utils.callable_or_raise(serialize))
        self.deserialize_func = (deserialize and utils.callable_or_raise(deserialize))

    def _serialize(self, value, attr, obj, **kwargs):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return self._call_or_raise(self.serialize_func, obj, attr)

    def _deserialize(self, value, attr, data, **kwargs):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        if self.deserialize_func:
            return self._call_or_raise(self.deserialize_func, value, attr)
        return value

    def _call_or_raise(self, func, value, attr):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        if (len(utils.get_func_args(func)) > 1):
            if (self.parent.context is None):
                msg = f'No context available for Function field {attr!r}'
                raise ValidationError(msg)
            return func(value, self.parent.context)
        else:
            return func(value)

class Constant(Field):
    'A field that (de)serializes to a preset constant.  If you only want the\n    constant added for serialization or deserialization, you should use\n    ``dump_only=True`` or ``load_only=True`` respectively.\n\n    :param constant: The constant to return for the field attribute.\n\n    .. versionadded:: 2.0.0\n    '
    _CHECK_ATTRIBUTE = False

    def __init__(self, constant: typing.Any, **kwargs) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        super().__init__(**kwargs)
        self.constant = constant
        self.missing = constant
        self.default = constant

    def _serialize(self, value, *args, **kwargs):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return self.constant

    def _deserialize(self, value, *args, **kwargs):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return self.constant

class Inferred(Field):
    'A field that infers how to serialize, based on the value type.\n\n    .. warning::\n\n        This class is treated as private API.\n        Users should not need to use this class directly.\n    '

    def __init__(self) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        super().__init__()
        self._field_cache = {}

    def _serialize(self, value, attr, obj, **kwargs):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        field_cls = self.root.TYPE_MAPPING.get(type(value))
        if (field_cls is None):
            field = super()
        else:
            field = self._field_cache.get(field_cls)
            if (field is None):
                field = field_cls()
                field._bind_to_schema(self.name, self.parent)
                self._field_cache[field_cls] = field
        return field._serialize(value, attr, obj, **kwargs)
URL = Url
Str = String
Bool = Boolean
Int = Integer
