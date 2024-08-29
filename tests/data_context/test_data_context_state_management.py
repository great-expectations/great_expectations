from __future__ import annotations

from typing import Mapping

import pytest

from great_expectations.data_context.data_context.context_factory import set_context
from great_expectations.data_context.data_context.ephemeral_data_context import (
    EphemeralDataContext,
)
from great_expectations.data_context.store import ExpectationsStore
from great_expectations.data_context.store.checkpoint_store import CheckpointStore
from great_expectations.data_context.store.datasource_store import DatasourceStore
from great_expectations.data_context.types.base import (
    DataContextConfig,
    InMemoryStoreBackendDefaults,
    ProgressBarsConfig,
)
from great_expectations.datasource.fluent.sources import DataSourceManager
from great_expectations.exceptions.exceptions import StoreConfigurationError


class DatasourceStoreSpy(DatasourceStore):
    def __init__(self) -> None:
        self.save_count = 0
        super().__init__()

    def set(self, key, value, **kwargs):
        ret = super().set(key=key, value=value, **kwargs)
        self.save_count += 1
        return ret


class ExpectationsStoreSpy(ExpectationsStore):
    def __init__(self) -> None:
        self.save_count = 0
        super().__init__()

    def add(self, key, value, **kwargs):
        ret = super().add(key=key, value=value, **kwargs)
        self.save_count += 1
        return ret

    def update(self, key, value, **kwargs):
        ret = super().update(key=key, value=value, **kwargs)
        self.save_count += 1
        return ret

    def add_or_update(self, key, value, **kwargs):
        ret = super().add_or_update(key=key, value=value, **kwargs)
        self.save_count += 1
        return ret


class CheckpointStoreSpy(CheckpointStore):
    STORE_NAME = "checkpoint_store"

    def __init__(self) -> None:
        self.save_count = 0
        super().__init__(store_name=CheckpointStoreSpy.STORE_NAME)

    def add(self, key, value, **kwargs):
        ret = super().add(key=key, value=value, **kwargs)
        self.save_count += 1
        return ret

    def update(self, key, value, **kwargs):
        ret = super().update(key=key, value=value, **kwargs)
        self.save_count += 1
        return ret

    def add_or_update(self, key, value, **kwargs):
        ret = super().add_or_update(key=key, value=value, **kwargs)
        self.save_count += 1
        return ret


class EphemeralDataContextSpy(EphemeralDataContext):
    """
    Simply wraps around EphemeralDataContext but keeps tabs on specific method calls around state management.
    """  # noqa: E501

    def __init__(
        self,
        project_config: DataContextConfig,
    ) -> None:
        # expectation store is required for initializing the base DataContext
        self._expectations_store = ExpectationsStoreSpy()
        self._checkpoint_store = CheckpointStoreSpy()
        super().__init__(project_config)
        self.save_count = 0
        self._datasource_store = DatasourceStoreSpy()

    @property
    def datasource_store(self):
        return self._datasource_store

    @property
    def expectations_store(self):
        return self._expectations_store

    @property
    def checkpoint_store(self):
        return self._checkpoint_store

    def _save_project_config(self):
        """
        No-op our persistence mechanism but increment an internal counter to ensure it was used.
        """
        self.save_count += 1


BLOCK_CONFIG_DATASOURCE_NAME = "my_pandas_datasource"


@pytest.fixture
def in_memory_data_context(
    fluent_datasource_config: dict,
) -> EphemeralDataContextSpy:
    config = DataContextConfig(
        store_backend_defaults=InMemoryStoreBackendDefaults(),
    )
    context = EphemeralDataContextSpy(project_config=config)
    ds_type = DataSourceManager.type_lookup[fluent_datasource_config["type"]]
    fluent_datasources = {
        fluent_datasource_config["name"]: ds_type(**fluent_datasource_config),
    }
    context.data_sources.all().update(fluent_datasources)
    set_context(context)
    return context


@pytest.mark.unit
def test_add_store(in_memory_data_context: EphemeralDataContextSpy):
    context = in_memory_data_context

    num_stores_before = len(context.stores)
    num_store_configs_before = len(context.config.stores)

    context.add_store(
        name="my_new_store",
        config={
            "module_name": "great_expectations.data_context.store",
            "class_name": "ExpectationsStore",
        },
    )

    num_stores_after = len(context.stores)
    num_store_configs_after = len(context.config.stores)

    assert num_stores_after == num_stores_before + 1
    assert num_store_configs_after == num_store_configs_before + 1
    assert context.save_count == 1


@pytest.mark.unit
def test_delete_store_success(in_memory_data_context: EphemeralDataContextSpy):
    context = in_memory_data_context

    num_stores_before = len(context.stores)
    num_store_configs_before = len(context.config.stores)

    context.delete_store("checkpoint_store")  # We know this to be a default name

    num_stores_after = len(context.stores)
    num_store_configs_after = len(context.config.stores)

    assert num_stores_after == num_stores_before - 1
    assert num_store_configs_after == num_store_configs_before - 1
    assert context.save_count == 1


@pytest.mark.unit
def test_delete_store_failure(in_memory_data_context: EphemeralDataContextSpy):
    context = in_memory_data_context

    num_stores_before = len(context.stores)
    num_store_configs_before = len(context.config.stores)

    with pytest.raises(StoreConfigurationError):
        context.delete_store("my_fake_store_name")

    num_stores_after = len(context.stores)
    num_store_configs_after = len(context.config.stores)

    assert num_stores_after == num_stores_before
    assert num_store_configs_after == num_store_configs_before
    assert context.save_count == 0


@pytest.mark.unit
@pytest.mark.parametrize(
    "config",
    [
        pytest.param(
            DataContextConfig(progress_bars=ProgressBarsConfig(globally=True)),
            id="DataContextConfig",
        ),
        pytest.param({"progress_bars": ProgressBarsConfig(globally=True)}, id="Mapping"),
    ],
)
def test_update_project_config(
    in_memory_data_context: EphemeralDataContextSpy, config: DataContextConfig | Mapping
):
    context = in_memory_data_context

    assert context.progress_bars is None

    context.update_project_config(config)

    assert context.progress_bars["globally"] is True
