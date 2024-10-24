from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass
from textwrap import dedent
from typing import Any, Callable, ClassVar, Optional, TypeVar

from typing_extensions import ParamSpec

from great_expectations.compatibility import docstring_parser
from great_expectations.compatibility.typing_extensions import override

logger = logging.getLogger(__name__)

WHITELISTED_TAG = "--Public API--"

P = ParamSpec("P")
T = TypeVar("T")
F = TypeVar("F", bound=Callable[..., Any])


def _remove_suffix(target: str, suffix: str) -> str:
    end_index = len(target) - len(suffix)
    if target.rfind(suffix) == end_index:
        return target[:end_index]
    return target


@dataclass(frozen=True)
class _PublicApiInfo:
    type: str
    name: str
    qualname: str
    module: Optional[str]


class _PublicApiIntrospector:
    _public_api: dict[str, list[_PublicApiInfo]] = {}

    # Only used for testing
    _class_registry: dict[str, set[str]] = defaultdict(set)

    # This is a special key that is used to indicate that a class definition
    # is being added to the registry.
    CLASS_DEFINITION: ClassVar[str] = "<class_def>"

    @property
    def class_registry(self) -> dict[str, set[str]]:
        return self._class_registry

    def add(self, func: F) -> None:
        self._add_to_class_registry(func)
        try:
            # We use an if statement instead of a ternary to work around
            # mypy's inability to type narrow inside a ternary.
            f: F
            if isinstance(func, classmethod):
                f = func.__func__
            else:
                f = func

            info = _PublicApiInfo(
                name=f.__name__,
                qualname=f.__qualname__,
                type=f.__class__.__name__,
                module=f.__module__ if hasattr(func, "__module__") else None,
            )
            if info.type not in self._public_api:
                self._public_api[info.type] = []
            self._public_api[info.type].append(info)
        except Exception:
            logger.exception(f"Could not add this function to the public API list: {func}")
            raise

    def _add_to_class_registry(self, func: F) -> None:
        if isinstance(func, type):
            self._add_class_definition_to_registry(func)
        else:
            self._add_method_to_registry(func)

    def _add_class_definition_to_registry(self, cls: type) -> None:
        key = f"{cls.__module__}.{cls.__qualname__}"
        self._class_registry[key].add(self.CLASS_DEFINITION)

    def _add_method_to_registry(self, func: F) -> None:
        parts = func.__qualname__.split(".")
        METHOD_PARTS_LENGTH = 2
        if len(parts) == METHOD_PARTS_LENGTH:
            cls = parts[0]
            method = parts[1]
            key = f"{func.__module__}.{cls}"
            self._class_registry[key].add(method)
        elif len(parts) > METHOD_PARTS_LENGTH:
            # public_api interacts oddly with closures so we ignore
            # This is only present in DataSourceManager and its dynamic registry
            logger.info(
                "Skipping registering function %s because it is a closure",
                func.__qualname__,
            )
        else:
            # Standalone functions will have a length of 1
            logger.info(
                "Skipping registering function %s because it does not have a class",
                func.__qualname__,
            )

    @override
    def __str__(self) -> str:
        out = []
        for t in sorted(list(self._public_api.keys())):
            out.append(f"{t}")
            for info in sorted(self._public_api[t], key=lambda info: info.qualname):
                supporting_info = ""
                if info.name != info.qualname:
                    supporting_info = _remove_suffix(info.qualname, "." + info.name)
                elif info.module is not None:
                    supporting_info = info.module
                out.append(f"    {info.name}, {supporting_info}")
        return "\n".join(out)


public_api_introspector = _PublicApiIntrospector()


def public_api(func: F) -> F:
    """Add the public API tag for processing by the auto documentation generator.

    Used as a decorator:

        @public_api
        def my_method(some_argument):
            ...

    This tag is added at import time.
    """
    public_api_introspector.add(func)
    existing_docstring = func.__doc__ if func.__doc__ else ""
    func.__doc__ = WHITELISTED_TAG + existing_docstring
    return func


def deprecated_method_or_class(
    version: str,
    message: str = "",
) -> Callable[[F], F]:
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
) -> Callable[[Callable[P, T]], Callable[P, T]]:
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

    def wrapper(func: Callable[P, T]) -> Callable[P, T]:
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
) -> Callable[[F], F]:
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
    """  # noqa: E501

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
) -> Callable[[F], F]:
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
        docstring = f"{short_description.strip()}\n" "\n" f"{text}\n" "\n" f"{dedent(docstring)}"
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
        func: Callable[P, T]unction whose docstring will be modified.
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


def _add_text_below_string_docstring_argument(docstring: str, argument_name: str, text: str) -> str:
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
        raise ValueError(f"Please specify an existing argument, you specified {argument_name}.")  # noqa: TRY003

    for param in parsed_docstring.params:
        if param.arg_name == argument_name:
            if param.description is None:
                param.description = text
            else:
                param.description += "\n\n" + text + "\n"

    # Returns: includes an additional ":\n" that we need to strip out.
    if parsed_docstring.returns:
        if parsed_docstring.returns.description:
            parsed_docstring.returns.description = parsed_docstring.returns.description.strip(":\n")

    # RenderingStyle.EXPANDED used to make sure any line breaks before and
    # after the added text are included (for Sphinx html rendering).
    composed_docstring = docstring_parser.docstring_parser.compose(
        docstring=parsed_docstring,
        style=docstring_parser.DocstringStyle.GOOGLE,
        rendering_style=docstring_parser.docstring_parser.RenderingStyle.EXPANDED,
    )

    return composed_docstring
