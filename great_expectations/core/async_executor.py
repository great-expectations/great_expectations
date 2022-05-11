
'\nLight weight wrappers to facilitate single code path\nfor both when concurrency is enabled and disabled.\n\nWARNING: This module is experimental.\n'
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import AbstractContextManager
from typing import Any, Optional
from urllib3 import connectionpool, poolmanager
from great_expectations.data_context.types.base import ConcurrencyConfig

class AsyncResult():
    'Wrapper around Future to facilitate single code path\n    for both when concurrency is enabled and disabled.\n\n    WARNING: This class is experimental.\n    '

    def __init__(self, future: Optional[Future]=None, value: Optional[Any]=None) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'AsyncResult instances are created by AsyncExecutor.submit() and should not otherwise be created directly.'
        self._future = future
        self._value = value

    def result(self):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Return the value corresponding to the AsyncExecutor.submit() call, blocking if necessary until the execution\n        finishes.\n        '
        return (self._future.result() if (self._future is not None) else self._value)

class AsyncExecutor(AbstractContextManager):
    'Wrapper around ThreadPoolExecutor to facilitate single code path\n    for both when concurrency is enabled and disabled.\n\n    WARNING: This class is experimental.\n    '

    def __init__(self, concurrency_config: Optional[ConcurrencyConfig], max_workers: int) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Initializes a new AsyncExecutor instance used to organize code for multithreaded execution.\n\n        If multithreading is disabled, all execution will be done synchronously (e.g. on the main thread) during the\n        call to submit. This is useful for introducing multithreading while still supporting single threaded execution.\n        This allows configuration to determine whether or not multithreading is used, which is useful when there are\n        concerns that some workflows may not benefit from multithreading or may not be safe to multithread.\n\n        This class is intended to be used as a context manager using the `with` statement.\n\n        Args:\n            concurrency_config: Configuration used to determine whether or not execution is done concurrently with\n                multiple threads. Even if the configuration has concurrency enabled, if max_workers is 1 then all work\n                will be done synchronously (e.g. on the main thread) during the call to submit.\n            max_workers: The maximum number of threads that can be used to execute concurrently. If concurrency is\n                disabled or max_workers is 1, all work will be done synchronously (e.g. on the main thread) during the\n                call to submit. Note that the maximum number of threads is also limited by\n                concurrency_config.max_database_query_concurrency.\n        '
        if (concurrency_config is None):
            concurrency_config = ConcurrencyConfig()
        self._execute_concurrently = (concurrency_config.enabled and (max_workers > 1))
        self._thread_pool_executor = (ThreadPoolExecutor(max_workers=min(concurrency_config.max_database_query_concurrency, max_workers)) if self._execute_concurrently else None)

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        self.shutdown()

    def submit(self, fn, *args, **kwargs) -> AsyncResult:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Submits a callable to be executed with the given arguments.\n\n        Execution occurs either concurrently on a different thread or synchronously (e.g. on the main thread) depending\n        on how the AsyncExecutor instance was initialized.\n        '
        if self._execute_concurrently:
            return AsyncResult(future=self._thread_pool_executor.submit(fn, *args, **kwargs))
        else:
            return AsyncResult(value=fn(*args, **kwargs))

    def shutdown(self) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Clean-up the resources associated with the AsyncExecutor and blocks until all running async results finish\n        executing.\n\n        It is preferable to not call this method explicitly, and instead use the `with` statement to ensure shutdown is\n        called.\n        '
        if (self._thread_pool_executor is not None):
            self._thread_pool_executor.shutdown()

    @property
    def execute_concurrently(self) -> bool:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return self._execute_concurrently

def patch_https_connection_pool(concurrency_config: ConcurrencyConfig) -> None:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    'Patch urllib3 to enable a higher default max pool size to reduce concurrency bottlenecks.\n\n    To have any effect, this method must be called before any database connections are made, e.g. by scripts leveraging\n    Great Expectations before using the Great Expectations library. See example usage in\n    tests/performance/test_bigquery_benchmarks.py.\n\n    WARNING: This function is experimental.\n    '

    class HTTPSConnectionPoolWithHigherMaxSize(connectionpool.HTTPSConnectionPool):

        def __init__(self, *args, **kwargs) -> None:
            import inspect
            __frame = inspect.currentframe()
            __file = __frame.f_code.co_filename
            __func = __frame.f_code.co_name
            for (k, v) in __frame.f_locals.items():
                if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                    continue
                print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
            kwargs.update(maxsize=concurrency_config.max_database_query_concurrency)
            super().__init__(*args, **kwargs)
    poolmanager.pool_classes_by_scheme['https'] = HTTPSConnectionPoolWithHigherMaxSize
