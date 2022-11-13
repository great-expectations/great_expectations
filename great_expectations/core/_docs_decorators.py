from typing import Callable

WHITELISTED_TAG = "--Public API--"

def public_api(func) -> Callable:
    """Add the public API tag for processing by the auto documentation generator.

    This tag is added at import time.
    """

    existing_docstring = func.__doc__ if func.__doc__ else ""

    # TODO: AJB 20221113 insert this tag where it does not affect styling.
    func.__doc__ = " ".join([existing_docstring, "\n\n", WHITELISTED_TAG])

    return func
