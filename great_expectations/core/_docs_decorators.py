from textwrap import dedent
from typing import Callable, TypeVar, Any, List

WHITELISTED_TAG = "--Public API--"


def public_api(func) -> Callable:
    """Add the public API tag for processing by the auto documentation generator.

    This tag is added at import time.
    """

    existing_docstring = func.__doc__ if func.__doc__ else ""

    func.__doc__ = WHITELISTED_TAG + existing_docstring

    return func


F = TypeVar("F", bound=Callable[..., Any])


def deprecated(
    version: str,
    message: str = "",
):
    """Add a deprecation warning to the docstring of the decorated method.

    Args:
        version: Version number when the method was deprecated.
        message: Optional deprecation message.
    """

    def decorate(fn: F) -> F:
        return _decorate_with_deprecation(
            func=fn,
            version=version,
            message=message,  # type: ignore[arg-type]
        )

    return decorate


def _decorate_with_deprecation(
    func: F,
    version: str,
    message: str,
) -> F:
    deprecation_rst = f".. deprecated:: {version}" "\n" f"    {message}"
    existing_docstring = func.__doc__ if func.__doc__ else ""
    split_docstring = existing_docstring.split("\n", 1)

    func.__doc__ = add_rst_directive_to_method_docstring(
        split_docstring=split_docstring,
        rst_directive=deprecation_rst,
    )
    return func


def version_added(
    version: str,
    message: str = "",
):
    """Add a version added note to the docstring of the decorated method.

    Args:
        version: Version number when the method was added.
        message: Optional message.
    """

    def decorate(fn: F) -> F:
        return _decorate_with_version_added(
            func=fn,
            version=version,
            message=message,  # type: ignore[arg-type]
        )

    return decorate


def _decorate_with_version_added(
    func: F,
    version: str,
    message: str,
) -> F:
    version_added_rst = f".. versionadded:: {version}" "\n" f"    {message}"
    existing_docstring = func.__doc__ if func.__doc__ else ""
    split_docstring = existing_docstring.split("\n", 1)

    func.__doc__ = add_rst_directive_to_method_docstring(
        split_docstring=split_docstring,
        rst_directive=version_added_rst,
    )

    return func


def add_rst_directive_to_method_docstring(
    split_docstring: List[str], rst_directive: str
) -> str:
    """Insert rst directive into docstring.

    Args:
        split_docstring: Docstring split into the first line (short description)
            and the rest of the docstring.
        rst_directive: string of the rst directive to add e.g.:
            rst_directive = (
                ".. versionadded:: 1.2.3\n"
                "    Added in version 1.2.3\n"
            )
    """
    if len(split_docstring) == 2:
        short_description, docstring = split_docstring
        return (
            f"{short_description.strip()}\n"
            "\n"
            f"{rst_directive}\n"
            "\n"
            f"{dedent(docstring)}"
        )
    elif len(split_docstring) == 1:
        short_description = split_docstring[0]
        return f"{short_description.strip()}\n" "\n" f"{rst_directive}\n"
    elif len(split_docstring) == 0:
        return f"{rst_directive}\n"
