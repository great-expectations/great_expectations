"Exception classes for marshmallow-related errors."
import typing

SCHEMA = "_schema"


class MarshmallowError(Exception):
    "Base class for all marshmallow-related errors."


class ValidationError(MarshmallowError):
    "Raised when validation fails on a field or schema.\n\n    Validators and custom fields should raise this exception.\n\n    :param message: An error message, list of error messages, or dict of\n        error messages. If a dict, the keys are subitems and the values are error messages.\n    :param field_name: Field name to store the error on.\n        If `None`, the error is stored as schema-level error.\n    :param data: Raw input data.\n    :param valid_data: Valid (de)serialized data.\n"

    def __init__(
        self,
        message: typing.Union[(str, typing.List, typing.Dict)],
        field_name: str = SCHEMA,
        data: typing.Union[
            (
                typing.Mapping[(str, typing.Any)],
                typing.Iterable[typing.Mapping[(str, typing.Any)]],
            )
        ] = None,
        valid_data: typing.Union[
            (
                typing.List[typing.Dict[(str, typing.Any)]],
                typing.Dict[(str, typing.Any)],
            )
        ] = None,
        **kwargs,
    ) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        self.messages = [message] if isinstance(message, (str, bytes)) else message
        self.field_name = field_name
        self.data = data
        self.valid_data = valid_data
        self.kwargs = kwargs
        super().__init__(message)

    def normalized_messages(self):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if (self.field_name == SCHEMA) and isinstance(self.messages, dict):
            return self.messages
        return {self.field_name: self.messages}


class RegistryError(NameError):
    "Raised when an invalid operation is performed on the serializer\n    class registry.\n"


class StringNotCollectionError(MarshmallowError, TypeError):
    "Raised when a string is passed when a list of strings is expected."


class FieldInstanceResolutionError(MarshmallowError, TypeError):
    "Raised when schema to instantiate is neither a Schema class nor an instance."
