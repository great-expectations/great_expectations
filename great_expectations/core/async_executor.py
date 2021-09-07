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

    def __init__(
        self,
        concurrency_config: ConcurrencyConfig,
        max_workers: int,
    ):
        # todo(jdimatteo): add docstrings here and elsewhere
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

    @property
    def execute_concurrently(self) -> bool:
        return self._execute_concurrently

    def submit(self, fn, *args, **kwargs) -> AsyncResult:
        return (
            AsyncResult(future=self._thread_pool_executor.submit(fn, *args, **kwargs))
            if self._execute_concurrently
            else AsyncResult(value=fn(*args, **kwargs))
        )


def patch_https_connection_pool():
    # Note: To have any effect, this method must be called before any database connections are made.
    #
    # WARNING: This function is experimental.

    ###
    # NOTE: 20210907 - jdimatteo: The python requests pool size can bottle neck concurrency and result in errors
    # like "WARNING  urllib3.connectionpool:connectionpool.py:304 Connection pool is full, discarding connection:
    # bigquery.googleapis.com". To remove this bottleneck, patch the https connection pool as described at
    # https://stackoverflow.com/a/22253656/1007353. After upgrading from the deprecated packages pybigquery and
    # google-cloud-python to python-bigquery-sqlalchemy and python-bigquery, remove this patching code and instead
    # follow the instructions at https://github.com/googleapis/python-bigquery/issues/59#issuecomment-619047244.
    #
    class MyHTTPSConnectionPool(connectionpool.HTTPSConnectionPool):
        def __init__(self, *args, **kwargs):
            kwargs.update(maxsize=100)
            super().__init__(*args, **kwargs)

    poolmanager.pool_classes_by_scheme["https"] = MyHTTPSConnectionPool
