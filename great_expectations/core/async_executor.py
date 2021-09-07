"""
Light weight wrappers to facilitate single code path
for both when concurrency is enabled and disabled.

WARNING: This module is experimental.
"""

from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any

from urllib3 import connectionpool, poolmanager

from great_expectations.data_context.types.base import ConcurrencyConfig


class AsyncResult:
    """Wrapper around Future to facilitate single code path
    for both when concurrency is enabled and disabled.

    WARNING: This class is experimental.
    """

    def __init__(self, future: Future = None, value: Any = None):
        """AsyncResult instances are created by AsyncExecutor.submit() and should not otherwise be created directly."""
        self._future = future
        self._value = value

    def result(self):
        """Return the value corresponding to the AsyncExecutor.submit() call, blocking if necessary until the execution
        finishes.
        """
        return self._future.result() if self._future is not None else self._value


class AsyncExecutor:
    """Wrapper around ThreadPoolExecutor to facilitate single code path
    for both when concurrency is enabled and disabled.

    WARNING: This class is experimental.
    """

    def __init__(
        self,
        concurrency_config: ConcurrencyConfig,
        max_workers: int,
    ):
        """Initializes a new AsyncExecutor instance.

        This class is intended to be used as a context manager using the `with` statement.

        Args:
            concurrency_config: Configuration used to determine whether or not execution is done concurrently. Even if
                the configuration has concurrency enabled, if max_workers is 1 then all work will be done synchronously
                (e.g. on the main thread) during the call to submit.
            max_workers: The maximum number of workers that can be used to execute concurrently. If concurrency is
                disabled or max_workers is 1, all work will be done synchronously (e.g. on the main thread) during the
                call to submit.
        """
        # Only enable concurrent execution if it is enabled in the config AND there is more than 1 max worker specified.
        self._execute_concurrently = concurrency_config.enabled and max_workers > 1

        self._thread_pool_executor = (
            ThreadPoolExecutor(
                max_workers=min(
                    # Use no more than 100 threads, because most databases won't benefit from more than 100 concurrent
                    # queries (e.g. see the BigQuery concurrent rate limit of 100 documented at
                    # https://cloud.google.com/bigquery/quotas#query_jobs).
                    100,
                    max_workers,
                )
            )
            if self._execute_concurrently
            else None
        )

    def __enter__(self):
        return self

    def __exit__(self, ext, exv, trb):
        self.shutdown()

    def submit(self, fn, *args, **kwargs) -> AsyncResult:
        """Submits a callable to be executed with the given arguments.

        Execution occurs either concurrently on a different thread or synchronously (e.g. on the main thread) depending
        on how the AsyncExecutor instance was initialized.
        """
        return (
            AsyncResult(future=self._thread_pool_executor.submit(fn, *args, **kwargs))
            if self._execute_concurrently
            else AsyncResult(value=fn(*args, **kwargs))
        )

    def shutdown(self):
        """Clean-up the resources associated with the Executor and blocks until all running async results finish
        executing.

        It is preferable to not call this method explicitly, and instead use the `with` statement to ensure shutdown is
        called.
        """
        if self._thread_pool_executor is not None:
            self._thread_pool_executor.shutdown()

    @property
    def execute_concurrently(self) -> bool:
        return self._execute_concurrently


def patch_https_connection_pool():
    """Patch urllib3 to enable a higher default max pool size to reduce concurrency bottlenecks.

    To have any effect, this method must be called before any database connections are made, e.g. by scripts leveraging
    Great Expectations before using the Great Expectations library. See example usage in
    tests/performance/test_bigquery_benchmarks.py.

    WARNING: This function is experimental.
    """
    ###
    # NOTE: 20210907 - jdimatteo: The python requests pool size can bottle neck concurrency and result in errors
    # like "WARNING  urllib3.connectionpool:connectionpool.py:304 Connection pool is full, discarding connection:
    # bigquery.googleapis.com". To remove this bottleneck, patch the https connection pool as described at
    # https://stackoverflow.com/a/22253656/1007353. After upgrading from the deprecated packages pybigquery and
    # google-cloud-python to python-bigquery-sqlalchemy and python-bigquery, this patching code can be replaced
    # following the instructions at https://github.com/googleapis/python-bigquery/issues/59#issuecomment-619047244.
    class HTTPSConnectionPoolWithHigherMaxSize(connectionpool.HTTPSConnectionPool):
        def __init__(self, *args, **kwargs):
            kwargs.update(maxsize=100)
            super().__init__(*args, **kwargs)

    poolmanager.pool_classes_by_scheme["https"] = HTTPSConnectionPoolWithHigherMaxSize
