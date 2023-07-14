import pytest

from great_expectations.data_context.types.base import (
    ConcurrencyConfig,
    DataContextConfig,
    InMemoryStoreBackendDefaults,
)
from great_expectations.util import get_context

pytestmark = pytest.mark.unit


def test_concurrency_disabled_by_default():
    data_context_config = DataContextConfig()
    assert data_context_config.concurrency is None


def test_concurrency_enabled_with_dict():
    data_context_config = DataContextConfig(concurrency={"enabled": True})
    assert data_context_config.concurrency.enabled


def test_concurrency_enabled_with_config():
    data_context_config = DataContextConfig(concurrency=ConcurrencyConfig(enabled=True))
    assert data_context_config.concurrency.enabled


def test_data_context_concurrency_property():
    data_context = get_context(
        project_config=DataContextConfig(
            concurrency=ConcurrencyConfig(enabled=True),
            store_backend_defaults=InMemoryStoreBackendDefaults(),
        )
    )
    assert data_context.concurrency.enabled
