from typing import Callable

WHITELISTED_TAG = "--Public API--"

def public_api(func) -> Callable:
    """Add the public API tag for processing by the auto documentation generator.

    This tag is added at import time.
    """

    existing_docstring = func.__doc__ if func.__doc__ else ""

    func.__doc__ = " ".join([WHITELISTED_TAG + "\n\n", existing_docstring])

    return func
