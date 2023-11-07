from __future__ import annotations

from functools import wraps
from typing import Callable, TypeVar

from typing_extensions import ParamSpec

from great_expectations.cli.pretty_printing import cli_message

P = ParamSpec("P")
T = TypeVar("T")


class Mark:
    """
    Marks for feature readiness.

    Usage:
    from great_expectations.cli.mark import Mark as mark

    @mark.blah
    def your_function()
    """

    @staticmethod
    def cli_as_experimental(func: Callable[P, None]) -> Callable[P, None]:
        """Apply as a decorator to CLI commands that are Experimental."""

        @wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> None:
            cli_message(
                "<yellow>Heads up! This feature is Experimental. It may change. "
                "Please give us your feedback!</yellow>"
            )
            func(*args, **kwargs)

        return wrapper

    @staticmethod
    def cli_as_beta(func: Callable[P, None]) -> Callable[P, None]:
        """Apply as a decorator to CLI commands that are beta."""

        @wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> None:
            cli_message(
                "<yellow>Heads up! This feature is in Beta. Please give us "
                "your feedback!</yellow>"
            )
            func(*args, **kwargs)

        return wrapper

    @staticmethod
    def cli_as_deprecation(
        message: str = "<yellow>Heads up! This feature will be deprecated in the next major release</yellow>",
    ) -> Callable[[Callable[P, None]], Callable[P, None]]:
        """Apply as a decorator to CLI commands that will be deprecated."""

        def inner_decorator(func: Callable[P, None]) -> Callable[P, None]:
            @wraps(func)
            def wrapped(*args: P.args, **kwargs: P.kwargs) -> None:
                cli_message(message)
                func(*args, **kwargs)

            return wrapped

        return inner_decorator
