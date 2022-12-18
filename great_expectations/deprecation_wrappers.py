from typing import Any, Type, Union

import marshmallow
from marshmallow.utils import missing as mm_missing
from packaging.version import LegacyVersion, Version
from packaging.version import parse as parse_version
from typing_extensions import TypeAlias

VersionType: TypeAlias = Union[Version, LegacyVersion]

NOT_PROVIDED = object()

# BEGIN `marshmallow`` section #########################################################

# https://marshmallow.readthedocs.io/en/stable/changelog.html
MARSHMALLOW_VERSION: VersionType = parse_version(marshmallow.__version__)

# https://marshmallow.readthedocs.io/en/stable/changelog.html#id9
MM_LOAD_DEFAULT_DEPRECATED_VERSION: VersionType = Version("3.13.0")


def mm_field(
    field_type: Type[marshmallow.fields.Field], load_default: Any = mm_missing, **kwargs
) -> marshmallow.fields.Field:
    """
    Wrapper around `marshmallow.fields.String` that preserves backwards compatibility on older versions.
    """
    missing = kwargs.pop("missing", NOT_PROVIDED)
    if missing is NOT_PROVIDED:
        pass
    else:
        raise ValueError("do not use deprecated arg `missing`")

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
