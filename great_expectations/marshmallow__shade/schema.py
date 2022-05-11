"The :class:`Schema` class, including its metaclass and options (class Meta)."
import copy
import datetime as dt
import decimal
import inspect
import json
import typing
import uuid
import warnings
from collections import OrderedDict, defaultdict
from collections.abc import Mapping
from functools import lru_cache

from great_expectations.marshmallow__shade import base, class_registry
from great_expectations.marshmallow__shade import fields as ma_fields
from great_expectations.marshmallow__shade import types
from great_expectations.marshmallow__shade.decorators import (
    POST_DUMP,
    POST_LOAD,
    PRE_DUMP,
    PRE_LOAD,
    VALIDATES,
    VALIDATES_SCHEMA,
)
from great_expectations.marshmallow__shade.error_store import ErrorStore
from great_expectations.marshmallow__shade.exceptions import (
    StringNotCollectionError,
    ValidationError,
)
from great_expectations.marshmallow__shade.orderedset import OrderedSet
from great_expectations.marshmallow__shade.utils import (
    EXCLUDE,
    INCLUDE,
    RAISE,
    get_value,
    is_collection,
    is_instance_or_subclass,
    is_iterable_but_not_string,
    missing,
    set_value,
)
from great_expectations.marshmallow__shade.warnings import RemovedInMarshmallow4Warning

_T = typing.TypeVar("_T")


def _get_fields(attrs, field_class, pop=False, ordered=False):
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
    "Get fields from a class. If ordered=True, fields will sorted by creation index.\n\n    :param attrs: Mapping of class attributes\n    :param type field_class: Base field class\n    :param bool pop: Remove matching fields\n    "
    fields = [
        (field_name, field_value)
        for (field_name, field_value) in attrs.items()
        if is_instance_or_subclass(field_value, field_class)
    ]
    if pop:
        for (field_name, _) in fields:
            del attrs[field_name]
    if ordered:
        fields.sort(key=(lambda pair: pair[1]._creation_index))
    return fields


def _get_fields_by_mro(klass, field_class, ordered=False):
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
    "Collect fields from a class, following its method resolution order. The\n    class itself is excluded from the search; only its parents are checked. Get\n    fields from ``_declared_fields`` if available, else use ``__dict__``.\n\n    :param type klass: Class whose fields to retrieve\n    :param type field_class: Base field class\n    "
    mro = inspect.getmro(klass)
    return sum(
        (
            _get_fields(
                getattr(base, "_declared_fields", base.__dict__),
                field_class,
                ordered=ordered,
            )
            for base in mro[:0:(-1)]
        ),
        [],
    )


class SchemaMeta(type):
    "Metaclass for the Schema class. Binds the declared fields to\n    a ``_declared_fields`` attribute, which is a dictionary mapping attribute\n    names to field objects. Also sets the ``opts`` class attribute, which is\n    the Schema class's ``class Meta`` options.\n"

    def __new__(mcs, name, bases, attrs):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        meta = attrs.get("Meta")
        ordered = getattr(meta, "ordered", False)
        if not ordered:
            for base_ in bases:
                if hasattr(base_, "Meta") and hasattr(base_.Meta, "ordered"):
                    ordered = base_.Meta.ordered
                    break
            else:
                ordered = False
        cls_fields = _get_fields(attrs, base.FieldABC, pop=True, ordered=ordered)
        klass = super().__new__(mcs, name, bases, attrs)
        inherited_fields = _get_fields_by_mro(klass, base.FieldABC, ordered=ordered)
        meta = klass.Meta
        klass.opts = klass.OPTIONS_CLASS(meta, ordered=ordered)
        cls_fields += list(klass.opts.include.items())
        dict_cls = OrderedDict if ordered else dict
        klass._declared_fields = mcs.get_declared_fields(
            klass=klass,
            cls_fields=cls_fields,
            inherited_fields=inherited_fields,
            dict_cls=dict_cls,
        )
        return klass

    @classmethod
    def get_declared_fields(
        mcs,
        klass: type,
        cls_fields: typing.List,
        inherited_fields: typing.List,
        dict_cls: type,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Returns a dictionary of field_name => `Field` pairs declared on the class.\n        This is exposed mainly so that plugins can add additional fields, e.g. fields\n        computed from class Meta options.\n\n        :param klass: The class object.\n        :param cls_fields: The fields declared on the class, including those added\n            by the ``include`` class Meta option.\n        :param inherited_fields: Inherited fields.\n        :param dict_class: Either `dict` or `OrderedDict`, depending on the whether\n            the user specified `ordered=True`.\n        "
        return dict_cls(inherited_fields + cls_fields)

    def __init__(cls, name, bases, attrs) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        super().__init__(name, bases, attrs)
        if name and cls.opts.register:
            class_registry.register(name, cls)
        cls._hooks = cls.resolve_hooks()

    def resolve_hooks(cls) -> typing.Dict[(types.Tag, typing.List[str])]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Add in the decorated processors\n\n        By doing this after constructing the class, we let standard inheritance\n        do all the hard work.\n        "
        mro = inspect.getmro(cls)
        hooks = defaultdict(list)
        for attr_name in dir(cls):
            for parent in mro:
                try:
                    attr = parent.__dict__[attr_name]
                except KeyError:
                    continue
                else:
                    break
            else:
                continue
            try:
                hook_config = attr.__marshmallow_hook__
            except AttributeError:
                pass
            else:
                for key in hook_config.keys():
                    hooks[key].append(attr_name)
        return hooks


class SchemaOpts:
    "class Meta options for the :class:`Schema`. Defines defaults."

    def __init__(self, meta, ordered: bool = False) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        self.fields = getattr(meta, "fields", ())
        if not isinstance(self.fields, (list, tuple)):
            raise ValueError("`fields` option must be a list or tuple.")
        self.additional = getattr(meta, "additional", ())
        if not isinstance(self.additional, (list, tuple)):
            raise ValueError("`additional` option must be a list or tuple.")
        if self.fields and self.additional:
            raise ValueError(
                "Cannot set both `fields` and `additional` options for the same Schema."
            )
        self.exclude = getattr(meta, "exclude", ())
        if not isinstance(self.exclude, (list, tuple)):
            raise ValueError("`exclude` must be a list or tuple.")
        self.dateformat = getattr(meta, "dateformat", None)
        self.datetimeformat = getattr(meta, "datetimeformat", None)
        if hasattr(meta, "json_module"):
            warnings.warn(
                "The json_module class Meta option is deprecated. Use render_module instead.",
                RemovedInMarshmallow4Warning,
            )
            render_module = getattr(meta, "json_module", json)
        else:
            render_module = json
        self.render_module = getattr(meta, "render_module", render_module)
        self.ordered = getattr(meta, "ordered", ordered)
        self.index_errors = getattr(meta, "index_errors", True)
        self.include = getattr(meta, "include", {})
        self.load_only = getattr(meta, "load_only", ())
        self.dump_only = getattr(meta, "dump_only", ())
        self.unknown = getattr(meta, "unknown", RAISE)
        self.register = getattr(meta, "register", True)


class Schema(base.SchemaABC, metaclass=SchemaMeta):
    "Base schema class with which to define custom schemas.\n\n    Example usage:\n\n    .. code-block:: python\n\n        import datetime as dt\n        from dataclasses import dataclass\n\n        from great_expectations.marshmallow__shade import Schema, fields\n\n\n        @dataclass\n        class Album:\n            title: str\n            release_date: dt.date\n\n\n        class AlbumSchema(Schema):\n            title = fields.Str()\n            release_date = fields.Date()\n\n\n        album = Album(\"Beggars Banquet\", dt.date(1968, 12, 6))\n        schema = AlbumSchema()\n        data = schema.dump(album)\n        data  # {'release_date': '1968-12-06', 'title': 'Beggars Banquet'}\n\n    :param only: Whitelist of the declared fields to select when\n        instantiating the Schema. If None, all fields are used. Nested fields\n        can be represented with dot delimiters.\n    :param exclude: Blacklist of the declared fields to exclude\n        when instantiating the Schema. If a field appears in both `only` and\n        `exclude`, it is not used. Nested fields can be represented with dot\n        delimiters.\n    :param many: Should be set to `True` if ``obj`` is a collection\n        so that the object will be serialized to a list.\n    :param context: Optional context passed to :class:`fields.Method` and\n        :class:`fields.Function` fields.\n    :param load_only: Fields to skip during serialization (write-only fields)\n    :param dump_only: Fields to skip during deserialization (read-only fields)\n    :param partial: Whether to ignore missing fields and not require\n        any fields declared. Propagates down to ``Nested`` fields as well. If\n        its value is an iterable, only missing fields listed in that iterable\n        will be ignored. Use dot delimiters to specify nested fields.\n    :param unknown: Whether to exclude, include, or raise an error for unknown\n        fields in the data. Use `EXCLUDE`, `INCLUDE` or `RAISE`.\n\n    .. versionchanged:: 3.0.0\n        `prefix` parameter removed.\n\n    .. versionchanged:: 2.0.0\n        `__validators__`, `__preprocessors__`, and `__data_handlers__` are removed in favor of\n        `marshmallow.decorators.validates_schema`,\n        `marshmallow.decorators.pre_load` and `marshmallow.decorators.post_dump`.\n        `__accessor__` and `__error_handler__` are deprecated. Implement the\n        `handle_error` and `get_attribute` methods instead.\n"
    TYPE_MAPPING = {
        str: ma_fields.String,
        bytes: ma_fields.String,
        dt.datetime: ma_fields.DateTime,
        float: ma_fields.Float,
        bool: ma_fields.Boolean,
        tuple: ma_fields.Raw,
        list: ma_fields.Raw,
        set: ma_fields.Raw,
        int: ma_fields.Integer,
        uuid.UUID: ma_fields.UUID,
        dt.time: ma_fields.Time,
        dt.date: ma_fields.Date,
        dt.timedelta: ma_fields.TimeDelta,
        decimal.Decimal: ma_fields.Decimal,
    }
    error_messages = {}
    _default_error_messages = {
        "type": "Invalid input type.",
        "unknown": "Unknown field.",
    }
    OPTIONS_CLASS = SchemaOpts
    opts = None
    _declared_fields = {}
    _hooks = {}

    class Meta:
        'Options object for a Schema.\n\n        Example usage: ::\n\n            class Meta:\n                fields = ("id", "email", "date_created")\n                exclude = ("password", "secret_attribute")\n\n        Available options:\n\n        - ``fields``: Tuple or list of fields to include in the serialized result.\n        - ``additional``: Tuple or list of fields to include *in addition* to the\n            explicitly declared fields. ``additional`` and ``fields`` are\n            mutually-exclusive options.\n        - ``include``: Dictionary of additional fields to include in the schema. It is\n            usually better to define fields as class variables, but you may need to\n            use this option, e.g., if your fields are Python keywords. May be an\n            `OrderedDict`.\n        - ``exclude``: Tuple or list of fields to exclude in the serialized result.\n            Nested fields can be represented with dot delimiters.\n        - ``dateformat``: Default format for `Date <fields.Date>` fields.\n        - ``datetimeformat``: Default format for `DateTime <fields.DateTime>` fields.\n        - ``render_module``: Module to use for `loads <Schema.loads>` and `dumps <Schema.dumps>`.\n            Defaults to `json` from the standard library.\n        - ``ordered``: If `True`, order serialization output according to the\n            order in which fields were declared. Output of `Schema.dump` will be a\n            `collections.OrderedDict`.\n        - ``index_errors``: If `True`, errors dictionaries will include the index\n            of invalid items in a collection.\n        - ``load_only``: Tuple or list of fields to exclude from serialized results.\n        - ``dump_only``: Tuple or list of fields to exclude from deserialization\n        - ``unknown``: Whether to exclude, include, or raise an error for unknown\n            fields in the data. Use `EXCLUDE`, `INCLUDE` or `RAISE`.\n        - ``register``: Whether to register the `Schema` with marshmallow\'s internal\n            class registry. Must be `True` if you intend to refer to this `Schema`\n            by class name in `Nested` fields. Only set this to `False` when memory\n            usage is critical. Defaults to `True`.\n'

    def __init__(
        self,
        *,
        only: types.StrSequenceOrSet = None,
        exclude: types.StrSequenceOrSet = (),
        many: bool = False,
        context: typing.Dict = None,
        load_only: types.StrSequenceOrSet = (),
        dump_only: types.StrSequenceOrSet = (),
        partial: typing.Union[(bool, types.StrSequenceOrSet)] = False,
        unknown: str = None,
    ) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if (only is not None) and (not is_collection(only)):
            raise StringNotCollectionError('"only" should be a list of strings')
        if not is_collection(exclude):
            raise StringNotCollectionError('"exclude" should be a list of strings')
        self.declared_fields = copy.deepcopy(self._declared_fields)
        self.many = many
        self.only = only
        self.exclude = set(self.opts.exclude) | set(exclude)
        self.ordered = self.opts.ordered
        self.load_only = set(load_only) or set(self.opts.load_only)
        self.dump_only = set(dump_only) or set(self.opts.dump_only)
        self.partial = partial
        self.unknown = unknown or self.opts.unknown
        self.context = context or {}
        self._normalize_nested_options()
        self.fields = {}
        self.load_fields = {}
        self.dump_fields = {}
        self._init_fields()
        messages = {}
        messages.update(self._default_error_messages)
        for cls in reversed(self.__class__.__mro__):
            messages.update(getattr(cls, "error_messages", {}))
        messages.update(self.error_messages or {})
        self.error_messages = messages

    def __repr__(self) -> str:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return f"<{self.__class__.__name__}(many={self.many})>"

    @property
    def dict_class(self) -> type:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return OrderedDict if self.ordered else dict

    @property
    def set_class(self) -> type:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return OrderedSet if self.ordered else set

    @classmethod
    def from_dict(
        cls,
        fields: typing.Dict[(str, typing.Union[(ma_fields.Field, type)])],
        *,
        name: str = "GeneratedSchema",
    ) -> type:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        'Generate a `Schema` class given a dictionary of fields.\n\n        .. code-block:: python\n\n            from great_expectations.marshmallow__shade import Schema, fields\n\n            PersonSchema = Schema.from_dict({"name": fields.Str()})\n            print(PersonSchema().load({"name": "David"}))  # => {\'name\': \'David\'}\n\n        Generated schemas are not added to the class registry and therefore cannot\n        be referred to by name in `Nested` fields.\n\n        :param dict fields: Dictionary mapping field names to field instances.\n        :param str name: Optional name for the class, which will appear in\n            the ``repr`` for the class.\n\n        .. versionadded:: 3.0.0\n        '
        attrs = fields.copy()
        attrs["Meta"] = type(
            "GeneratedMeta", (getattr(cls, "Meta", object),), {"register": False}
        )
        schema_cls = type(name, (cls,), attrs)
        return schema_cls

    def handle_error(
        self, error: ValidationError, data: typing.Any, *, many: bool, **kwargs
    ) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Custom error handler function for the schema.\n\n        :param error: The `ValidationError` raised during (de)serialization.\n        :param data: The original input data.\n        :param many: Value of ``many`` on dump or load.\n        :param partial: Value of ``partial`` on load.\n\n        .. versionadded:: 2.0.0\n\n        .. versionchanged:: 3.0.0rc9\n            Receives `many` and `partial` (on deserialization) as keyword arguments.\n        "
        pass

    def get_attribute(self, obj: typing.Any, attr: str, default: typing.Any):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Defines how to pull values from an object to serialize.\n\n        .. versionadded:: 2.0.0\n\n        .. versionchanged:: 3.0.0a1\n            Changed position of ``obj`` and ``attr``.\n        "
        return get_value(obj, attr, default)

    @staticmethod
    def _call_and_store(getter_func, data, *, field_name, error_store, index=None):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Call ``getter_func`` with ``data`` as its argument, and store any `ValidationErrors`.\n\n        :param callable getter_func: Function for getting the serialized/deserialized\n            value from ``data``.\n        :param data: The data passed to ``getter_func``.\n        :param str field_name: Field name.\n        :param int index: Index of the item being validated, if validating a collection,\n            otherwise `None`.\n        "
        try:
            value = getter_func(data)
        except ValidationError as error:
            error_store.store_error(error.messages, field_name, index=index)
            return error.valid_data or missing
        return value

    def _serialize(
        self, obj: typing.Union[(_T, typing.Iterable[_T])], *, many: bool = False
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Serialize ``obj``.\n\n        :param obj: The object(s) to serialize.\n        :param bool many: `True` if ``data`` should be serialized as a collection.\n        :return: A dictionary of the serialized data\n\n        .. versionchanged:: 1.0.0\n            Renamed from ``marshal``.\n        "
        if many and (obj is not None):
            return [
                self._serialize(d, many=False)
                for d in typing.cast(typing.Iterable[_T], obj)
            ]
        ret = self.dict_class()
        for (attr_name, field_obj) in self.dump_fields.items():
            value = field_obj.serialize(attr_name, obj, accessor=self.get_attribute)
            if value is missing:
                continue
            key = field_obj.data_key if (field_obj.data_key is not None) else attr_name
            ret[key] = value
        return ret

    def dump(self, obj: typing.Any, *, many: bool = None):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Serialize an object to native Python data types according to this\n        Schema's fields.\n\n        :param obj: The object to serialize.\n        :param many: Whether to serialize `obj` as a collection. If `None`, the value\n            for `self.many` is used.\n        :return: A dict of serialized data\n        :rtype: dict\n\n        .. versionadded:: 1.0.0\n        .. versionchanged:: 3.0.0b7\n            This method returns the serialized data rather than a ``(data, errors)`` duple.\n            A :exc:`ValidationError <marshmallow.exceptions.ValidationError>` is raised\n            if ``obj`` is invalid.\n        .. versionchanged:: 3.0.0rc9\n            Validation no longer occurs upon serialization.\n        "
        many = self.many if (many is None) else bool(many)
        if many and is_iterable_but_not_string(obj):
            obj = list(obj)
        if self._has_processors(PRE_DUMP):
            processed_obj = self._invoke_dump_processors(
                PRE_DUMP, obj, many=many, original_data=obj
            )
        else:
            processed_obj = obj
        result = self._serialize(processed_obj, many=many)
        if self._has_processors(POST_DUMP):
            result = self._invoke_dump_processors(
                POST_DUMP, result, many=many, original_data=obj
            )
        return result

    def dumps(self, obj: typing.Any, *args, many: bool = None, **kwargs):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Same as :meth:`dump`, except return a JSON-encoded string.\n\n        :param obj: The object to serialize.\n        :param many: Whether to serialize `obj` as a collection. If `None`, the value\n            for `self.many` is used.\n        :return: A ``json`` string\n        :rtype: str\n\n        .. versionadded:: 1.0.0\n        .. versionchanged:: 3.0.0b7\n            This method returns the serialized data rather than a ``(data, errors)`` duple.\n            A :exc:`ValidationError <marshmallow.exceptions.ValidationError>` is raised\n            if ``obj`` is invalid.\n        "
        serialized = self.dump(obj, many=many)

        def datetime_serializer(o):
            import inspect

            __frame = inspect.currentframe()
            __file = __frame.f_code.co_filename
            __func = __frame.f_code.co_name
            for (k, v) in __frame.f_locals.items():
                if any((var in k) for var in ("__frame", "__file", "__func")):
                    continue
                print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
            if isinstance(o, dt.datetime):
                return o.__str__()

        if "default" not in kwargs:
            kwargs.update({"default": datetime_serializer})
        return self.opts.render_module.dumps(serialized, *args, **kwargs)

    def _deserialize(
        self,
        data: typing.Union[
            (
                typing.Mapping[(str, typing.Any)],
                typing.Iterable[typing.Mapping[(str, typing.Any)]],
            )
        ],
        *,
        error_store: ErrorStore,
        many: bool = False,
        partial=False,
        unknown=RAISE,
        index=None,
    ) -> typing.Union[(_T, typing.List[_T])]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Deserialize ``data``.\n\n        :param dict data: The data to deserialize.\n        :param ErrorStore error_store: Structure to store errors.\n        :param bool many: `True` if ``data`` should be deserialized as a collection.\n        :param bool|tuple partial: Whether to ignore missing fields and not require\n            any fields declared. Propagates down to ``Nested`` fields as well. If\n            its value is an iterable, only missing fields listed in that iterable\n            will be ignored. Use dot delimiters to specify nested fields.\n        :param unknown: Whether to exclude, include, or raise an error for unknown\n            fields in the data. Use `EXCLUDE`, `INCLUDE` or `RAISE`.\n        :param int index: Index of the item being serialized (for storing errors) if\n            serializing a collection, otherwise `None`.\n        :return: A dictionary of the deserialized data.\n        "
        index_errors = self.opts.index_errors
        index = index if index_errors else None
        if many:
            if not is_collection(data):
                error_store.store_error([self.error_messages["type"]], index=index)
                ret = []
            else:
                ret = [
                    typing.cast(
                        _T,
                        self._deserialize(
                            typing.cast(typing.Mapping[(str, typing.Any)], d),
                            error_store=error_store,
                            many=False,
                            partial=partial,
                            unknown=unknown,
                            index=idx,
                        ),
                    )
                    for (idx, d) in enumerate(data)
                ]
            return ret
        ret = self.dict_class()
        if not isinstance(data, Mapping):
            error_store.store_error([self.error_messages["type"]], index=index)
        else:
            partial_is_collection = is_collection(partial)
            for (attr_name, field_obj) in self.load_fields.items():
                field_name = (
                    field_obj.data_key
                    if (field_obj.data_key is not None)
                    else attr_name
                )
                raw_value = data.get(field_name, missing)
                if raw_value is missing:
                    if (partial is True) or (
                        partial_is_collection and (attr_name in partial)
                    ):
                        continue
                d_kwargs = {}
                if partial_is_collection:
                    prefix = f"{field_name}."
                    len_prefix = len(prefix)
                    sub_partial = [
                        f[len_prefix:] for f in partial if f.startswith(prefix)
                    ]
                    d_kwargs["partial"] = sub_partial
                else:
                    d_kwargs["partial"] = partial
                getter = lambda val: field_obj.deserialize(
                    val, field_name, data, **d_kwargs
                )
                value = self._call_and_store(
                    getter_func=getter,
                    data=raw_value,
                    field_name=field_name,
                    error_store=error_store,
                    index=index,
                )
                if value is not missing:
                    key = field_obj.attribute or attr_name
                    set_value(typing.cast(typing.Dict, ret), key, value)
            if unknown != EXCLUDE:
                fields = {
                    (
                        field_obj.data_key
                        if (field_obj.data_key is not None)
                        else field_name
                    )
                    for (field_name, field_obj) in self.load_fields.items()
                }
                for key in set(data) - fields:
                    value = data[key]
                    if unknown == INCLUDE:
                        set_value(typing.cast(typing.Dict, ret), key, value)
                    elif unknown == RAISE:
                        error_store.store_error(
                            [self.error_messages["unknown"]],
                            key,
                            (index if index_errors else None),
                        )
        return ret

    def load(
        self,
        data: typing.Union[
            (
                typing.Mapping[(str, typing.Any)],
                typing.Iterable[typing.Mapping[(str, typing.Any)]],
            )
        ],
        *,
        many: bool = None,
        partial: typing.Union[(bool, types.StrSequenceOrSet)] = None,
        unknown: str = None,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Deserialize a data structure to an object defined by this Schema's fields.\n\n        :param data: The data to deserialize.\n        :param many: Whether to deserialize `data` as a collection. If `None`, the\n            value for `self.many` is used.\n        :param partial: Whether to ignore missing fields and not require\n            any fields declared. Propagates down to ``Nested`` fields as well. If\n            its value is an iterable, only missing fields listed in that iterable\n            will be ignored. Use dot delimiters to specify nested fields.\n        :param unknown: Whether to exclude, include, or raise an error for unknown\n            fields in the data. Use `EXCLUDE`, `INCLUDE` or `RAISE`.\n            If `None`, the value for `self.unknown` is used.\n        :return: Deserialized data\n\n        .. versionadded:: 1.0.0\n        .. versionchanged:: 3.0.0b7\n            This method returns the deserialized data rather than a ``(data, errors)`` duple.\n            A :exc:`ValidationError <marshmallow.exceptions.ValidationError>` is raised\n            if invalid data are passed.\n        "
        return self._do_load(
            data, many=many, partial=partial, unknown=unknown, postprocess=True
        )

    def loads(
        self,
        json_data: str,
        *,
        many: bool = None,
        partial: typing.Union[(bool, types.StrSequenceOrSet)] = None,
        unknown: str = None,
        **kwargs,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Same as :meth:`load`, except it takes a JSON string as input.\n\n        :param json_data: A JSON string of the data to deserialize.\n        :param many: Whether to deserialize `obj` as a collection. If `None`, the\n            value for `self.many` is used.\n        :param partial: Whether to ignore missing fields and not require\n            any fields declared. Propagates down to ``Nested`` fields as well. If\n            its value is an iterable, only missing fields listed in that iterable\n            will be ignored. Use dot delimiters to specify nested fields.\n        :param unknown: Whether to exclude, include, or raise an error for unknown\n            fields in the data. Use `EXCLUDE`, `INCLUDE` or `RAISE`.\n            If `None`, the value for `self.unknown` is used.\n        :return: Deserialized data\n\n        .. versionadded:: 1.0.0\n        .. versionchanged:: 3.0.0b7\n            This method returns the deserialized data rather than a ``(data, errors)`` duple.\n            A :exc:`ValidationError <marshmallow.exceptions.ValidationError>` is raised\n            if invalid data are passed.\n        "
        data = self.opts.render_module.loads(json_data, **kwargs)
        return self.load(data, many=many, partial=partial, unknown=unknown)

    def _run_validator(
        self,
        validator_func,
        output,
        *,
        original_data,
        error_store,
        many,
        partial,
        pass_original,
        index=None,
    ) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        try:
            if pass_original:
                validator_func(output, original_data, partial=partial, many=many)
            else:
                validator_func(output, partial=partial, many=many)
        except ValidationError as err:
            error_store.store_error(err.messages, err.field_name, index=index)

    def validate(
        self,
        data: typing.Mapping,
        *,
        many: bool = None,
        partial: typing.Union[(bool, types.StrSequenceOrSet)] = None,
    ) -> typing.Dict[(str, typing.List[str])]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Validate `data` against the schema, returning a dictionary of\n        validation errors.\n\n        :param data: The data to validate.\n        :param many: Whether to validate `data` as a collection. If `None`, the\n            value for `self.many` is used.\n        :param partial: Whether to ignore missing fields and not require\n            any fields declared. Propagates down to ``Nested`` fields as well. If\n            its value is an iterable, only missing fields listed in that iterable\n            will be ignored. Use dot delimiters to specify nested fields.\n        :return: A dictionary of validation errors.\n\n        .. versionadded:: 1.1.0\n        "
        try:
            self._do_load(data, many=many, partial=partial, postprocess=False)
        except ValidationError as exc:
            return typing.cast(typing.Dict[(str, typing.List[str])], exc.messages)
        return {}

    def _do_load(
        self,
        data: typing.Union[
            (
                typing.Mapping[(str, typing.Any)],
                typing.Iterable[typing.Mapping[(str, typing.Any)]],
            )
        ],
        *,
        many: bool = None,
        partial: typing.Union[(bool, types.StrSequenceOrSet)] = None,
        unknown: str = None,
        postprocess: bool = True,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Deserialize `data`, returning the deserialized result.\n        This method is private API.\n\n        :param data: The data to deserialize.\n        :param many: Whether to deserialize `data` as a collection. If `None`, the\n            value for `self.many` is used.\n        :param partial: Whether to validate required fields. If its\n            value is an iterable, only fields listed in that iterable will be\n            ignored will be allowed missing. If `True`, all fields will be allowed missing.\n            If `None`, the value for `self.partial` is used.\n        :param unknown: Whether to exclude, include, or raise an error for unknown\n            fields in the data. Use `EXCLUDE`, `INCLUDE` or `RAISE`.\n            If `None`, the value for `self.unknown` is used.\n        :param postprocess: Whether to run post_load methods..\n        :return: Deserialized data\n        "
        error_store = ErrorStore()
        errors = {}
        many = self.many if (many is None) else bool(many)
        unknown = unknown or self.unknown
        if partial is None:
            partial = self.partial
        if self._has_processors(PRE_LOAD):
            try:
                processed_data = self._invoke_load_processors(
                    PRE_LOAD, data, many=many, original_data=data, partial=partial
                )
            except ValidationError as err:
                errors = err.normalized_messages()
                result = None
        else:
            processed_data = data
        if not errors:
            result = self._deserialize(
                processed_data,
                error_store=error_store,
                many=many,
                partial=partial,
                unknown=unknown,
            )
            self._invoke_field_validators(
                error_store=error_store, data=result, many=many
            )
            if self._has_processors(VALIDATES_SCHEMA):
                field_errors = bool(error_store.errors)
                self._invoke_schema_validators(
                    error_store=error_store,
                    pass_many=True,
                    data=result,
                    original_data=data,
                    many=many,
                    partial=partial,
                    field_errors=field_errors,
                )
                self._invoke_schema_validators(
                    error_store=error_store,
                    pass_many=False,
                    data=result,
                    original_data=data,
                    many=many,
                    partial=partial,
                    field_errors=field_errors,
                )
            errors = error_store.errors
            if (not errors) and postprocess and self._has_processors(POST_LOAD):
                try:
                    result = self._invoke_load_processors(
                        POST_LOAD,
                        result,
                        many=many,
                        original_data=data,
                        partial=partial,
                    )
                except ValidationError as err:
                    errors = err.normalized_messages()
        if errors:
            exc = ValidationError(errors, data=data, valid_data=result)
            self.handle_error(exc, data, many=many, partial=partial)
            raise exc
        return result

    def _normalize_nested_options(self) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Apply then flatten nested schema options.\n        This method is private API.\n        "
        if self.only is not None:
            self.__apply_nested_option("only", self.only, "intersection")
            self.only = self.set_class([field.split(".", 1)[0] for field in self.only])
        if self.exclude:
            self.__apply_nested_option("exclude", self.exclude, "union")
            self.exclude = self.set_class(
                [field for field in self.exclude if ("." not in field)]
            )

    def __apply_nested_option(self, option_name, field_names, set_operation) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Apply nested options to nested fields"
        nested_fields = [name.split(".", 1) for name in field_names if ("." in name)]
        nested_options = defaultdict(list)
        for (parent, nested_names) in nested_fields:
            nested_options[parent].append(nested_names)
        for (key, options) in iter(nested_options.items()):
            new_options = self.set_class(options)
            original_options = getattr(self.declared_fields[key], option_name, ())
            if original_options:
                if set_operation == "union":
                    new_options |= self.set_class(original_options)
                if set_operation == "intersection":
                    new_options &= self.set_class(original_options)
            setattr(self.declared_fields[key], option_name, new_options)

    def _init_fields(self) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Update self.fields, self.load_fields, and self.dump_fields based on schema options.\n        This method is private API.\n        "
        if self.opts.fields:
            available_field_names = self.set_class(self.opts.fields)
        else:
            available_field_names = self.set_class(self.declared_fields.keys())
            if self.opts.additional:
                available_field_names |= self.set_class(self.opts.additional)
        invalid_fields = self.set_class()
        if self.only is not None:
            field_names = self.set_class(self.only)
            invalid_fields |= field_names - available_field_names
        else:
            field_names = available_field_names
        if self.exclude:
            field_names = field_names - self.exclude
            invalid_fields |= self.exclude - available_field_names
        if invalid_fields:
            message = f"Invalid fields for {self}: {invalid_fields}."
            raise ValueError(message)
        fields_dict = self.dict_class()
        for field_name in field_names:
            field_obj = self.declared_fields.get(field_name, ma_fields.Inferred())
            self._bind_field(field_name, field_obj)
            fields_dict[field_name] = field_obj
        (load_fields, dump_fields) = (self.dict_class(), self.dict_class())
        for (field_name, field_obj) in fields_dict.items():
            if not field_obj.dump_only:
                load_fields[field_name] = field_obj
            if not field_obj.load_only:
                dump_fields[field_name] = field_obj
        dump_data_keys = [
            (field_obj.data_key if (field_obj.data_key is not None) else name)
            for (name, field_obj) in dump_fields.items()
        ]
        if len(dump_data_keys) != len(set(dump_data_keys)):
            data_keys_duplicates = {
                x for x in dump_data_keys if (dump_data_keys.count(x) > 1)
            }
            raise ValueError(
                "The data_key argument for one or more fields collides with another field's name or data_key argument. Check the following field names and data_key arguments: {}".format(
                    list(data_keys_duplicates)
                )
            )
        load_attributes = [
            (obj.attribute or name) for (name, obj) in load_fields.items()
        ]
        if len(load_attributes) != len(set(load_attributes)):
            attributes_duplicates = {
                x for x in load_attributes if (load_attributes.count(x) > 1)
            }
            raise ValueError(
                "The attribute argument for one or more fields collides with another field's name or attribute argument. Check the following field names and attribute arguments: {}".format(
                    list(attributes_duplicates)
                )
            )
        self.fields = fields_dict
        self.dump_fields = dump_fields
        self.load_fields = load_fields

    def on_bind_field(self, field_name: str, field_obj: ma_fields.Field) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Hook to modify a field when it is bound to the `Schema`.\n\n        No-op by default.\n        "
        return None

    def _bind_field(self, field_name: str, field_obj: ma_fields.Field) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Bind field to the schema, setting any necessary attributes on the\n        field (e.g. parent and name).\n\n        Also set field load_only and dump_only values if field_name was\n        specified in ``class Meta``.\n        "
        if field_name in self.load_only:
            field_obj.load_only = True
        if field_name in self.dump_only:
            field_obj.dump_only = True
        try:
            field_obj._bind_to_schema(field_name, self)
        except TypeError as error:
            if isinstance(field_obj, type) and issubclass(field_obj, base.FieldABC):
                msg = 'Field for "{}" must be declared as a Field instance, not a class. Did you mean "fields.{}()"?'.format(
                    field_name, field_obj.__name__
                )
                raise TypeError(msg) from error
            raise error
        self.on_bind_field(field_name, field_obj)

    @lru_cache(maxsize=8)
    def _has_processors(self, tag) -> bool:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return bool(self._hooks[(tag, True)] or self._hooks[(tag, False)])

    def _invoke_dump_processors(
        self, tag: str, data, *, many: bool, original_data=None
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        data = self._invoke_processors(
            tag, pass_many=False, data=data, many=many, original_data=original_data
        )
        data = self._invoke_processors(
            tag, pass_many=True, data=data, many=many, original_data=original_data
        )
        return data

    def _invoke_load_processors(
        self,
        tag: str,
        data,
        *,
        many: bool,
        original_data,
        partial: typing.Union[(bool, types.StrSequenceOrSet)],
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        data = self._invoke_processors(
            tag,
            pass_many=True,
            data=data,
            many=many,
            original_data=original_data,
            partial=partial,
        )
        data = self._invoke_processors(
            tag,
            pass_many=False,
            data=data,
            many=many,
            original_data=original_data,
            partial=partial,
        )
        return data

    def _invoke_field_validators(
        self, *, error_store: ErrorStore, data, many: bool
    ) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        for attr_name in self._hooks[VALIDATES]:
            validator = getattr(self, attr_name)
            validator_kwargs = validator.__marshmallow_hook__[VALIDATES]
            field_name = validator_kwargs["field_name"]
            try:
                field_obj = self.fields[field_name]
            except KeyError as error:
                if field_name in self.declared_fields:
                    continue
                raise ValueError(f'"{field_name}" field does not exist.') from error
            data_key = (
                field_obj.data_key if (field_obj.data_key is not None) else field_name
            )
            if many:
                for (idx, item) in enumerate(data):
                    try:
                        value = item[(field_obj.attribute or field_name)]
                    except KeyError:
                        pass
                    else:
                        validated_value = self._call_and_store(
                            getter_func=validator,
                            data=value,
                            field_name=data_key,
                            error_store=error_store,
                            index=(idx if self.opts.index_errors else None),
                        )
                        if validated_value is missing:
                            data[idx].pop(field_name, None)
            else:
                try:
                    value = data[(field_obj.attribute or field_name)]
                except KeyError:
                    pass
                else:
                    validated_value = self._call_and_store(
                        getter_func=validator,
                        data=value,
                        field_name=data_key,
                        error_store=error_store,
                    )
                    if validated_value is missing:
                        data.pop(field_name, None)

    def _invoke_schema_validators(
        self,
        *,
        error_store: ErrorStore,
        pass_many: bool,
        data,
        original_data,
        many: bool,
        partial: typing.Union[(bool, types.StrSequenceOrSet)],
        field_errors: bool = False,
    ) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        for attr_name in self._hooks[(VALIDATES_SCHEMA, pass_many)]:
            validator = getattr(self, attr_name)
            validator_kwargs = validator.__marshmallow_hook__[
                (VALIDATES_SCHEMA, pass_many)
            ]
            if field_errors and validator_kwargs["skip_on_field_errors"]:
                continue
            pass_original = validator_kwargs.get("pass_original", False)
            if many and (not pass_many):
                for (idx, (item, orig)) in enumerate(zip(data, original_data)):
                    self._run_validator(
                        validator,
                        item,
                        original_data=orig,
                        error_store=error_store,
                        many=many,
                        partial=partial,
                        index=idx,
                        pass_original=pass_original,
                    )
            else:
                self._run_validator(
                    validator,
                    data,
                    original_data=original_data,
                    error_store=error_store,
                    many=many,
                    pass_original=pass_original,
                    partial=partial,
                )

    def _invoke_processors(
        self,
        tag: str,
        *,
        pass_many: bool,
        data,
        many: bool,
        original_data=None,
        **kwargs,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        key = (tag, pass_many)
        for attr_name in self._hooks[key]:
            processor = getattr(self, attr_name)
            processor_kwargs = processor.__marshmallow_hook__[key]
            pass_original = processor_kwargs.get("pass_original", False)
            if many and (not pass_many):
                if pass_original:
                    data = [
                        processor(item, original, many=many, **kwargs)
                        for (item, original) in zip(data, original_data)
                    ]
                else:
                    data = [processor(item, many=many, **kwargs) for item in data]
            elif pass_original:
                data = processor(data, original_data, many=many, **kwargs)
            else:
                data = processor(data, many=many, **kwargs)
        return data


BaseSchema = Schema
