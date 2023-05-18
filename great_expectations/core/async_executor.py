"""
Light weight wrappers to facilitate single code path
for both when concurrency is enabled and disabled.

WARNING: This module is experimental.
"""

from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import AbstractContextManager
from typing import Generic, Optional, TypeVar

import requests

from great_expectations.compatibility.google import python_bigquery
from great_expectations.data_context.types.base import ConcurrencyConfig

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

    def __init__(
        self,
        concurrency_config: Optional[ConcurrencyConfig],
        max_workers: int,
    ) -> None:
        """Initializes a new AsyncExecutor instance used to organize code for multithreaded execution.

        If multithreading is disabled, all execution will be done synchronously (e.g. on the main thread) during the
        call to submit. This is useful for introducing multithreading while still supporting single threaded execution.
        This allows configuration to determine whether or not multithreading is used, which is useful when there are
        concerns that some workflows may not benefit from multithreading or may not be safe to multithread.

        This class is intended to be used as a context manager using the `with` statement.

        Args:
            concurrency_config: Configuration used to determine whether or not execution is done concurrently with
                multiple threads. Even if the configuration has concurrency enabled, if max_workers is 1 then all work
                will be done synchronously (e.g. on the main thread) during the call to submit.
            max_workers: The maximum number of threads that can be used to execute concurrently. If concurrency is
                disabled or max_workers is 1, all work will be done synchronously (e.g. on the main thread) during the
                call to submit. Note that the maximum number of threads is also limited by
                concurrency_config.max_database_query_concurrency.
        """
        if concurrency_config is None:
            concurrency_config = ConcurrencyConfig()

        # Only enable concurrent execution if it is enabled in the config AND there is more than 1 max worker specified.
        self._execute_concurrently = concurrency_config.enabled and max_workers > 1

        self._thread_pool_executor = (
            ThreadPoolExecutor(
                max_workers=min(
                    concurrency_config.max_database_query_concurrency,
                    max_workers,
                )
            )
            if self._execute_concurrently
            else None
        )

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


def patch_https_connection_pool(
    concurrency_config: ConcurrencyConfig, google_cloud_project: str
) -> None:
    """Patch urllib3 to enable a higher default max pool size to reduce concurrency bottlenecks.

    To have any effect, this method must be called before any database connections are made, e.g. by scripts leveraging
    Great Expectations before using the Great Expectations library. See example usage in
    tests/performance/test_bigquery_benchmarks.py.

    WARNING: This function is experimental.
    """
    ###
    # NOTE: 20210907 - jdimatteo: The python requests pool size can bottleneck concurrency and result in warnings
    # like "WARNING  urllib3.connectionpool:connectionpool.py:304 Connection pool is full, discarding connection:
    # bigquery.googleapis.com". To remove this bottleneck, following the instructions at
    # https://github.com/googleapis/python-bigquery/issues/59#issuecomment-619047244.
    if not concurrency_config.enabled:
        return

    bq = python_bigquery.Client()

    # Increase the HTTP pool size to avoid the "Connection pool is full, discarding connection: bigquery.googleapis.com"
    adapter = requests.adapters.HTTPAdapter(pool_connections=100, pool_maxsize=100)
    bq._http.mount("https://", adapter)
