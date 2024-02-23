from __future__ import annotations

"""
Light weight wrappers to facilitate single code path
for both when concurrency is enabled and disabled.

WARNING: This module is experimental.
"""
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import AbstractContextManager
from typing import Generic, Optional, TypeVar

from great_expectations.compatibility.typing_extensions import override

T = TypeVar("T")


class AsyncResult(Generic[T]):
    """Wrapper around Future to facilitate single code path
    for both when concurrency is enabled and disabled.

    WARNING: This class is experimental.
    """

    def __init__(
        self, future: Optional[Future] = None, value: Optional[T] = None
    ) -> None:
        """AsyncResult instances are created by AsyncExecutor.submit() and should not otherwise be created directly."""
        self._future = future
        self._value = value

    def result(self) -> T:
        """Return the value corresponding to the AsyncExecutor.submit() call, blocking if necessary until the execution
        finishes.
        """
        return self._future.result() if self._future is not None else self._value


class AsyncExecutor(AbstractContextManager):
    """Wrapper around ThreadPoolExecutor to facilitate single code path
    for both when concurrency is enabled and disabled.

    WARNING: This class is experimental.
    """

    def __init__(self) -> None:
        """Initializes a new AsyncExecutor instance used to organize code for multithreaded execution.

        If multithreading is disabled, all execution will be done synchronously (e.g. on the main thread) during the
        call to submit. This is useful for introducing multithreading while still supporting single threaded execution.
        This allows configuration to determine whether or not multithreading is used, which is useful when there are
        concerns that some workflows may not benefit from multithreading or may not be safe to multithread.

        This class is intended to be used as a context manager using the `with` statement.
        """
        self._thread_pool_executor = ThreadPoolExecutor(max_workers=None)

    @override
    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.shutdown()
        # Do NOT use the context manager exception arguments in order to get the desired default behavior (i.e. any
        # exception is NOT suppressed and the exception is propagated normally upon exit from this method). For more
        # info, see https://docs.python.org/3.9/reference/datamodel.html#object.__exit__.

    def submit(self, fn, *args, **kwargs) -> AsyncResult:
        """Submits a callable to be executed with the given arguments.

        Execution occurs either concurrently on a different thread or synchronously (e.g. on the main thread) depending
        on how the AsyncExecutor instance was initialized.
        """
        if self._execute_concurrently:
            return AsyncResult(
                future=self._thread_pool_executor.submit(fn, *args, **kwargs)  # type: ignore[union-attr]
            )
        else:
            return AsyncResult(value=fn(*args, **kwargs))

    def shutdown(self) -> None:
        """Clean-up the resources associated with the AsyncExecutor and blocks until all running async results finish
        executing.

        It is preferable to not call this method explicitly, and instead use the `with` statement to ensure shutdown is
        called.
        """
        if self._thread_pool_executor is not None:
            self._thread_pool_executor.shutdown()

    @property
    def execute_concurrently(self) -> bool:
        return self._execute_concurrently
