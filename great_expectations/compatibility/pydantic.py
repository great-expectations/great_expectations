import pydantic

from great_expectations.compatibility.not_imported import (
    is_version_greater_or_equal,
)

if is_version_greater_or_equal(version=pydantic.VERSION, compare_version="2.0.0"):
    # TODO: don't use star imports
    from pydantic.v1 import *  # noqa: F403
    from pydantic.v1 import (
        AnyUrl,
        UrlError,
        error_wrappers,
        errors,
        fields,
        generics,
        json,
        networks,
        schema,
        typing,
    )
    from pydantic.v1.generics import GenericModel
    from pydantic.v1.main import ModelMetaclass

else:
    # TODO: don't use star imports
    from pydantic import *  # type: ignore[assignment,no-redef] # noqa: F403
    from pydantic import (  # type: ignore[no-redef]
        AnyUrl,
        UrlError,
        error_wrappers,
        errors,
        fields,
        generics,
        json,
        networks,
        schema,
        typing,
    )
    from pydantic.generics import GenericModel  # type: ignore[no-redef]
    from pydantic.main import ModelMetaclass  # type: ignore[no-redef]

__all__ = [
    "AnyUrl",
    "error_wrappers",
    "errors",
    "fields",
    "GenericModel",
    "generics",
    "json",
    "ModelMetaclass",
    "networks",
    "schema",
    "typing",
    "UrlError",
]
