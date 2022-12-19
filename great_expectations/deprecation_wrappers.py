from typing import Any, Dict, Iterable, Set, Type, Union

import marshmallow
from marshmallow.utils import missing as mm_missing
from packaging.version import LegacyVersion, Version
from packaging.version import parse as parse_version
from typing_extensions import TypeAlias

VersionType: TypeAlias = Union[Version, LegacyVersion]

NOT_PROVIDED = object()


def _raise_for_deprecated_kwargs(
    deprecated_args: Iterable[str], kw_dict: Dict[str, Any]
):
    for arg in deprecated_args:
        if kw_dict.get(arg, NOT_PROVIDED) is NOT_PROVIDED:
            continue
        raise ValueError(f"'{arg}' is deprecated")


# BEGIN `marshmallow`` section #########################################################

# https://marshmallow.readthedocs.io/en/stable/changelog.html
MARSHMALLOW_VERSION: VersionType = parse_version(marshmallow.__version__)

# https://marshmallow.readthedocs.io/en/stable/changelog.html#id9
MM_LOAD_DEFAULT_DEPRECATED_VERSION: VersionType = Version("3.13.0")

# https://marshmallow.readthedocs.io/en/stable/changelog.html#id15
MM_KW_METADATA_DEPRECATED_VERSION: VersionType = Version("3.10.0")


DEPRECATED_FIELD_ARGS: Set[str] = {
    "missing",  # load_default - missing
    "all_none",  # metadata - **additional_metadata
}


def mm_field(
    field_type: Type[marshmallow.fields.Field], load_default: Any = mm_missing, **kwargs
) -> marshmallow.fields.Field:
    """
    Wrapper around `marshmallow.fields.String` that preserves backwards compatibility on older versions.
    """
    _raise_for_deprecated_kwargs(DEPRECATED_FIELD_ARGS, kwargs)

    if MARSHMALLOW_VERSION < MM_LOAD_DEFAULT_DEPRECATED_VERSION:
        kwargs["missing"] = load_default
    else:
        kwargs["load_default"] = load_default
    print(kwargs)
    return field_type(**kwargs)


# END `marshmallow`` section ###########################################################

if __name__ == "__main__":
    print(
        f"{MARSHMALLOW_VERSION} < {MM_LOAD_DEFAULT_DEPRECATED_VERSION} {MARSHMALLOW_VERSION < MM_LOAD_DEFAULT_DEPRECATED_VERSION}"
    )
    print(mm_field(marshmallow.fields.String, load_default="foo"))
