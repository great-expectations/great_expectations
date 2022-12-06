from textwrap import dedent
from typing import Callable, TypeVar, Any, List

import docstring_parser

WHITELISTED_TAG = "--Public API--"


def public_api(func) -> Callable:
    """Add the public API tag for processing by the auto documentation generator.

    This tag is added at import time.
    """

    existing_docstring = func.__doc__ if func.__doc__ else ""

    func.__doc__ = WHITELISTED_TAG + existing_docstring

    return func


F = TypeVar("F", bound=Callable[..., Any])


def deprecated_method(
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

    func.__doc__ = _add_text_to_method_docstring(
        split_docstring=split_docstring,
        text=deprecation_rst,
    )
    return func


def deprecated_argument(
    argument_name: str,
    version: str,
    message: str = "",
):
    """Add an arg-specific deprecation warning to the docstring of the decorated method.

    Args:
        argument_name: Name of the argument to associate with the deprecation note.
        version: Version number when the method was deprecated.
        message: Optional deprecation message.
    """

    def decorate(fn: F) -> F:
        return _decorate_argument_with_deprecation(
            func=fn,
            argument_name=argument_name,
            version=version,
            message=message,  # type: ignore[arg-type]
        )

    return decorate


def _decorate_argument_with_deprecation(
    func: F,
    argument_name: str,
    version: str,
    message: str,
) -> F:
    deprecation_rst = f".. deprecated:: {version}" "\n" f"    {message}"
    existing_docstring = func.__doc__ if func.__doc__ else ""

    func.__doc__ = _add_text_below_docstring_argument(
        docstring=existing_docstring, argument_name=argument_name, text=deprecation_rst
    )

    return func


def new_argument(
    argument_name: str,
    version: str,
    message: str = "",
):
    """Add note for new arguments about which version the argument was added.

    Args:
        argument_name: Name of the argument to associate with the note.
        version: Version number to associate with the note.
        message: Optional message.
    """

    def decorate(fn: F) -> F:
        return _decorate_argument_with_new_note(
            func=fn,
            argument_name=argument_name,
            version=version,
            message=message,  # type: ignore[arg-type]
        )

    return decorate


def _decorate_argument_with_new_note(
    func: F,
    argument_name: str,
    version: str,
    message: str,
) -> F:
    text = f".. versionadded:: {version}" "\n" f"    {message}"
    existing_docstring = func.__doc__ if func.__doc__ else ""

    func.__doc__ = _add_text_below_docstring_argument(
        docstring=existing_docstring, argument_name=argument_name, text=text
    )

    return func


def _add_text_below_docstring_argument(
    docstring: str, argument_name: str, text: str
) -> str:
    """Add text below an argument in a docstring.

    Note: Can be used for rst directives.

    Args:
        docstring: Docstring to modify.
        argument_name: Argument to place text below.
        text: Text to place below argument. Can be an rst directive.

    Returns:
        Modified docstring.
    """
    parsed_docstring = docstring_parser.parse(docstring)

    if argument_name not in (param.arg_name for param in parsed_docstring.params):
        raise ValueError(
            f"Please specify an existing argument, you specified {argument_name}."
        )

    for idx, param in enumerate(parsed_docstring.params):
        if param.arg_name == argument_name:
            # description can be None
            if not parsed_docstring.params[idx]:
                parsed_docstring.params[idx].description = text
            else:
                parsed_docstring.params[idx].description += "\n\n" + text + "\n\n"  # type: ignore[operator]

    # RenderingStyle.Expanded used to make sure any line breaks before and
    # after the added text are included.
    return docstring_parser.compose(
        docstring=parsed_docstring,
        rendering_style=docstring_parser.RenderingStyle.EXPANDED,
    )


def new_method(
    version: str,
    message: str = "",
):
    """Add a version added note to the docstring of the decorated method.

    Args:
        version: Version number when the method was added.
        message: Optional message.
    """

    def decorate(fn: F) -> F:
        return _decorate_with_new_method(
            func=fn,
            version=version,
            message=message,  # type: ignore[arg-type]
        )

    return decorate


def _decorate_with_new_method(
    func: F,
    version: str,
    message: str,
) -> F:
    version_added_rst = f".. versionadded:: {version}" "\n" f"    {message}"
    existing_docstring = func.__doc__ if func.__doc__ else ""
    split_docstring = existing_docstring.split("\n", 1)

    func.__doc__ = _add_text_to_method_docstring(
        split_docstring=split_docstring,
        text=version_added_rst,
    )

    return func


def _add_text_to_method_docstring(split_docstring: List[str], text: str) -> str:
    """Insert rst directive into docstring.

    Args:
        split_docstring: Docstring split into the first line (short description)
            and the rest of the docstring.
        text: string to add, can be a rst directive e.g.:
            text = (
                ".. versionadded:: 1.2.3\n"
                "    Added in version 1.2.3\n"
            )
    """
    docstring = ""
    if len(split_docstring) == 2:
        short_description, docstring = split_docstring
        docstring = (
            f"{short_description.strip()}\n"
            "\n"
            f"{text}\n"
            "\n"
            f"{dedent(docstring)}"
        )
    elif len(split_docstring) == 1:
        short_description = split_docstring[0]
        docstring = f"{short_description.strip()}\n" "\n" f"{text}\n"
    elif len(split_docstring) == 0:
        docstring = f"{text}\n"

    return docstring
