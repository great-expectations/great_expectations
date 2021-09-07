from great_expectations.core.async_executor import AsyncExecutor
from great_expectations.data_context.types.base import ConcurrencyConfig


def test_async_executor_does_execute_concurrently_when_concurrency_enabled_with_multiple_max_workers():
    async_executor = AsyncExecutor(ConcurrencyConfig(enabled=True), max_workers=100)
    assert async_executor.execute_concurrently


def test_async_executor_does_not_execute_concurrently_when_concurrency_enabled_with_multiple_max_workers():
    async_executor = AsyncExecutor(ConcurrencyConfig(enabled=False), max_workers=100)
    assert not async_executor.execute_concurrently


def test_async_executor_does_not_execute_concurrently_when_concurrency_enabled_with_single_max_workers():
    async_executor = AsyncExecutor(ConcurrencyConfig(enabled=True), max_workers=1)
    assert not async_executor.execute_concurrently
