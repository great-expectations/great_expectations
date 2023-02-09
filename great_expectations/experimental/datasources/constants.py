from typing import Set

from typing_extensions import Final

# these fields must be added to `__fields_set__` before pydantic model serialization
# methods are called. Otherwise it could be excluded.
# https://docs.pydantic.dev/usage/exporting_models/#modeldict
_FIELDS_ALWAYS_SET: Final[Set[str]] = {
    "assets",
    "type",
}
