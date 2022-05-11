from functools import wraps
from typing import Callable

from great_expectations.cli.v012.util import cli_message


class Mark:
    "\n    Marks for feature readiness.\n\n    Usage:\n    from great_expectations.cli.v012.mark import Mark as mark\n\n    @mark.blah\n    def your_function()\n"

    @staticmethod
    def cli_as_experimental(func: Callable) -> Callable:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Apply as a decorator to CLI commands that are Experimental."

        @wraps(func)
        def wrapper(*args, **kwargs) -> None:
            import inspect

            __frame = inspect.currentframe()
            __file = __frame.f_code.co_filename
            __func = __frame.f_code.co_name
            for (k, v) in __frame.f_locals.items():
                if any((var in k) for var in ("__frame", "__file", "__func")):
                    continue
                print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
            cli_message(
                "<yellow>Heads up! This feature is Experimental. It may change. Please give us your feedback!</yellow>"
            )
            func(*args, **kwargs)

        return wrapper

    @staticmethod
    def cli_as_beta(func: Callable) -> Callable:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Apply as a decorator to CLI commands that are beta."

        @wraps(func)
        def wrapper(*args, **kwargs) -> None:
            import inspect

            __frame = inspect.currentframe()
            __file = __frame.f_code.co_filename
            __func = __frame.f_code.co_name
            for (k, v) in __frame.f_locals.items():
                if any((var in k) for var in ("__frame", "__file", "__func")):
                    continue
                print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
            cli_message(
                "<yellow>Heads up! This feature is in Beta. Please give us your feedback!</yellow>"
            )
            func(*args, **kwargs)

        return wrapper

    @staticmethod
    def cli_as_deprecation(
        message: str = "<yellow>Heads up! This feature will be deprecated in the next major release</yellow>",
    ) -> Callable:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Apply as a decorator to CLI commands that will be deprecated."

        def inner_decorator(func):
            import inspect

            __frame = inspect.currentframe()
            __file = __frame.f_code.co_filename
            __func = __frame.f_code.co_name
            for (k, v) in __frame.f_locals.items():
                if any((var in k) for var in ("__frame", "__file", "__func")):
                    continue
                print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")

            @wraps(func)
            def wrapped(*args, **kwargs) -> None:
                import inspect

                __frame = inspect.currentframe()
                __file = __frame.f_code.co_filename
                __func = __frame.f_code.co_name
                for (k, v) in __frame.f_locals.items():
                    if any((var in k) for var in ("__frame", "__file", "__func")):
                        continue
                    print(
                        f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}"
                    )
                cli_message(message)
                func(*args, **kwargs)

            return wrapped

        return inner_decorator
