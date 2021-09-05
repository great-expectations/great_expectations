from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any


class AsyncResult:
    """Wrapper around Future to facilitate single code path for both when concurrency is enabled and disabled."""

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
    def __init__(self, concurrency_enabled):
        self.concurrency_enabled = concurrency_enabled  # todo(jdimatteo) configurable

        self._thread_pool_executor = (
            ThreadPoolExecutor(max_workers=100) if concurrency_enabled else None
        )
        # todo(jdimatteo) configurable number of threads
        # todo(jdimatteo): use with ConcurrentExecutor, and setup to call shutdown etc.

    def submit(self, fn, *args, **kwargs) -> AsyncResult:
        return (
            AsyncResult(future=self._thread_pool_executor.submit(fn, *args, **kwargs))
            if self.concurrency_enabled
            else AsyncResult(value=fn(*args, **kwargs))
        )
