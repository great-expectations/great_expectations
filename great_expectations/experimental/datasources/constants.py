from typing_extensions import Final

# this field must be added to `__fields_set__` before pydantic model serialization
# methods are called. Otherwise it could be excluded.
# https://docs.pydantic.dev/usage/exporting_models/#modeldict
_FIELD_ALWAYS_SET: Final[str] = "assets"
