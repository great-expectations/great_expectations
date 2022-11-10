from typing import Optional

from great_expectations.util import camel_to_snake


def _remove_suffix(s: str, suffix: str) -> str:
    # NOTE: str.remove_suffix() added in python 3.9
    if s.endswith(suffix):
        s = s[: -len(suffix)]
    return s


def _get_simplified_name_from_type(
    t: type, suffix_to_remove: Optional[str] = None
) -> str:
    result = camel_to_snake(t.__name__)
    if suffix_to_remove:
        return _remove_suffix(result, suffix_to_remove)
    return result
