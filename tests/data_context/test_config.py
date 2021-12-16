from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import (
    ConcurrencyConfig,
    DataContextConfig,
    InMemoryStoreBackendDefaults,
    ProgressBarsConfig,
)


def test_concurrency_disabled_by_default():
    data_context_config = DataContextConfig()
    assert not data_context_config.concurrency.enabled


def test_concurrency_enabled_with_dict():
    data_context_config = DataContextConfig(concurrency={"enabled": True})
    assert data_context_config.concurrency.enabled


def test_concurrency_enabled_with_config():
    data_context_config = DataContextConfig(concurrency=ConcurrencyConfig(enabled=True))
    assert data_context_config.concurrency.enabled


def test_data_context_concurrency_property():
    data_context = BaseDataContext(
        project_config=DataContextConfig(
            concurrency=ConcurrencyConfig(enabled=True),
            store_backend_defaults=InMemoryStoreBackendDefaults(),
        )
    )
    assert data_context.concurrency.enabled


def test_progress_bars_enabled_by_default():
    progress_bars = ProgressBarsConfig()
    assert progress_bars.is_enabled("profilers") is True
    assert progress_bars.is_enabled("metric_calculations") is True


def test_progress_bars_disabled_globally():
    progress_bars = ProgressBarsConfig(
        globally=False, profilers=True, metric_calculations=True
    )
    assert progress_bars.is_enabled("profilers") is False
    assert progress_bars.is_enabled("metric_calculations") is False


def test_progress_bars_disabled_metric_calculations():
    progress_bars = ProgressBarsConfig(
        globally=True, profilers=True, metric_calculations=False
    )
    assert progress_bars.is_enabled("profilers") is True
    assert progress_bars.is_enabled("metric_calculations") is False


def test_progress_bars_disabled_profilers():
    progress_bars = ProgressBarsConfig(
        globally=True, profilers=False, metric_calculations=True
    )
    assert progress_bars.is_enabled("profilers") is False
    assert progress_bars.is_enabled("metric_calculations") is True
