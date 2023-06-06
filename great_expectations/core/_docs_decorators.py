from textwrap import dedent
from typing import Any, Callable, TypeVar

from great_expectations.compatibility import docstring_parser

WHITELISTED_TAG = "--Public API--"

F = TypeVar("F", bound=Callable[..., Any])


def public_api(func: F) -> F:
    """Add the public API tag for processing by the auto documentation generator.

    Used as a decorator:

        @public_api
        def my_method(some_argument):
            ...

    This tag is added at import time.
    """

    existing_docstring = func.__doc__ if func.__doc__ else ""

    func.__doc__ = WHITELISTED_TAG + existing_docstring

    return func


def deprecated_method_or_class(
    version: str,
    message: str = "",
):
    """Add a deprecation warning to the docstring of the decorated method or class.

    Used as a decorator:

        @deprecated_method_or_class(version="1.2.3", message="Optional message")
        def my_method(some_argument):
            ...

        or

        @deprecated_method_or_class(version="1.2.3", message="Optional message")
        class MyClass:
            ...

    Args:
        version: Version number when the method was deprecated.
        message: Optional deprecation message.
    """

    text = f".. deprecated:: {version}" "\n" f"    {message}"

    def wrapper(func: F) -> F:
        """Wrapper method that accepts func, so we can modify the docstring."""
        return _add_text_to_function_docstring_after_summary(
            func=func,
            text=text,
        )

    return wrapper


def new_method_or_class(
    version: str,
    message: str = "",
):
    """Add a version added note to the docstring of the decorated method or class.

    Used as a decorator:

        @new_method_or_class(version="1.2.3", message="Optional message")
        def my_method(some_argument):
            ...

        or

        @new_method_or_class(version="1.2.3", message="Optional message")
        class MyClass:
            ...

    Args:
        version: Version number when the method was added.
        message: Optional message.
    """

    text = f".. versionadded:: {version}" "\n" f"    {message}"

    def wrapper(func: F) -> F:
        """Wrapper method that accepts func, so we can modify the docstring."""
        return _add_text_to_function_docstring_after_summary(
            func=func,
            text=text,
        )

    return wrapper


def deprecated_argument(
    argument_name: str,
    version: str,
    message: str = "",
):
    """Add an arg-specific deprecation warning to the decorated method or class.

    Used as a decorator:

        @deprecated_argument(argument_name="some_argument", version="1.2.3", message="Optional message")
        def my_method(some_argument):
            ...

        or

        @deprecated_argument(argument_name="some_argument", version="1.2.3", message="Optional message")
        class MyClass:
            ...

    If docstring_parser is not installed, this will not modify the docstring.

    Args:
        argument_name: Name of the argument to associate with the deprecation note.
        version: Version number when the method was deprecated.
        message: Optional deprecation message.
    """

    text = f".. deprecated:: {version}" "\n" f"    {message}"

    def wrapper(func: F) -> F:
        """Wrapper method that accepts func, so we can modify the docstring."""
        if not docstring_parser.docstring_parser:
            return func

        return _add_text_below_function_docstring_argument(
            func=func,
            argument_name=argument_name,
            text=text,
        )

    return wrapper


def new_argument(
    argument_name: str,
    version: str,
    message: str = "",
):
    """Add an arg-specific version added note to the decorated method or class.

    Used as a decorator:

        @new_argument(argument_name="some_argument", version="1.2.3", message="Optional message")
        def my_method(some_argument):
            ...

        or

        @new_argument(argument_name="some_argument", version="1.2.3", message="Optional message")
        class MyClass:
            ...

    If docstring_parser is not installed, this will not modify the docstring.

    Args:
        argument_name: Name of the argument to associate with the note.
        version: The version number to associate with the note.
        message: Optional message.
    """

    text = f".. versionadded:: {version}" "\n" f"    {message}"

    def wrapper(func: F) -> F:
        """Wrapper method that accepts func, so we can modify the docstring."""
        if not docstring_parser.docstring_parser:
            return func

        return _add_text_below_function_docstring_argument(
            func=func,
            argument_name=argument_name,
            text=text,
        )

    return wrapper


def _add_text_to_function_docstring_after_summary(func: F, text: str) -> F:
    """Insert text into docstring, e.g. rst directive.

    Args:
        func: Add text to provided func docstring.
        text: String to add to the docstring, can be a rst directive e.g.:
            text = (
                ".. versionadded:: 1.2.3\n"
                "    Added in version 1.2.3\n"
            )

    Returns:
        func with modified docstring.
    """
    existing_docstring = func.__doc__ if func.__doc__ else ""
    split_docstring = existing_docstring.split("\n", 1)

    docstring = ""
    if len(split_docstring) == 2:  # noqa: PLR2004
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

    func.__doc__ = docstring

    return func


def _add_text_below_function_docstring_argument(
    func: F,
    argument_name: str,
    text: str,
) -> F:
    """Add text below specified docstring argument.

    Args:
        func: Function whose docstring will be modified.
        argument_name: Name of the argument to add text to its description.
        text: Text to add to the argument description.

    Returns:
        func with modified docstring.
    """
    existing_docstring = func.__doc__ if func.__doc__ else ""

    func.__doc__ = _add_text_below_string_docstring_argument(
        docstring=existing_docstring, argument_name=argument_name, text=text
    )

    return func


def _add_text_below_string_docstring_argument(
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
    parsed_docstring = docstring_parser.docstring_parser.parse(
        text=docstring,
        style=docstring_parser.DocstringStyle.GOOGLE,
    )

    arg_list = list(param.arg_name for param in parsed_docstring.params)
    if argument_name not in arg_list:
        raise ValueError(
            f"Please specify an existing argument, you specified {argument_name}."
        )

    for param in parsed_docstring.params:
        if param.arg_name == argument_name:
            if param.description is None:
                param.description = text
            else:
                param.description += "\n\n" + text + "\n"

    # Returns: includes an additional ":\n" that we need to strip out.
    if parsed_docstring.returns:
        if parsed_docstring.returns.description:
            parsed_docstring.returns.description = (
                parsed_docstring.returns.description.strip(":\n")
            )

    # RenderingStyle.EXPANDED used to make sure any line breaks before and
    # after the added text are included (for Sphinx html rendering).
    composed_docstring = docstring_parser.docstring_parser.compose(
        docstring=parsed_docstring,
        style=docstring_parser.DocstringStyle.GOOGLE,
        rendering_style=docstring_parser.docstring_parser.RenderingStyle.EXPANDED,
    )

    return composed_docstring
