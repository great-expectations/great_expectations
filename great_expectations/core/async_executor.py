"""
Light weight wrappers to facilitate single code path
for both when concurrency is enabled and disabled.

WARNING: This module is experimental.
"""

from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any

from great_expectations.data_context.types.base import ConcurrencyConfig


class AsyncResult:
    """Wrapper around Future to facilitate single code path
    for both when concurrency is enabled and disabled.

    WARNING: This class is experimental.
    """

    def __init__(self, future: Future = None, value: Any = None):
        self._future = future
        self._value = value

    def result(self, *args, **kwargs):
        return (
            self._future.result(*args, **kwargs)
            if self._future is not None
            else self._value
        )


class AsyncExecutor:
    """Wrapper around ThreadPoolExecutor to facilitate single code path
    for both when concurrency is enabled and disabled.

    WARNING: This class is experimental.
    """

    def __init__(self, concurrency_config: ConcurrencyConfig):
        self._concurrency_enabled = concurrency_config.enabled

        self._thread_pool_executor = (
            ThreadPoolExecutor(max_workers=100) if self._concurrency_enabled else None
        )

    @property
    def concurrency_enabled(self):
        return self._concurrency_enabled

    def submit(self, fn, *args, **kwargs) -> AsyncResult:
        return (
            AsyncResult(future=self._thread_pool_executor.submit(fn, *args, **kwargs))
            if self._concurrency_enabled
            else AsyncResult(value=fn(*args, **kwargs))
        )
