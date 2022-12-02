from textwrap import dedent
from typing import Callable, TypeVar, Any

WHITELISTED_TAG = "--Public API--"


def public_api(func) -> Callable:
    """Add the public API tag for processing by the auto documentation generator.

    This tag is added at import time.
    """

    existing_docstring = func.__doc__ if func.__doc__ else ""

    func.__doc__ = WHITELISTED_TAG + existing_docstring

    return func

F = TypeVar('F', bound=Callable[..., Any])

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
    existing_docstring = func.__doc__ if func.__doc__ else ""
    short_description, docstring = existing_docstring.split("\n", 1)

    deprecation_rst = (
        f".. deprecated:: {version}"
        "\n"
        f"    {message}"
    )

    func.__doc__ = (
        f"{short_description.strip()}\n"
        "\n"
        f"{deprecation_rst}\n"
        "\n"
        f"{dedent(docstring)}"
    )

    return func
