from __future__ import annotations

from typing import Mapping

import pytest

from great_expectations.data_context.data_context.ephemeral_data_context import (
    EphemeralDataContext,
)
from great_expectations.data_context.types.base import (
    DataContextConfig,
    DatasourceConfig,
    InMemoryStoreBackendDefaults,
    ProgressBarsConfig,
)
from great_expectations.exceptions.exceptions import StoreConfigurationError


class EphemeralDataContextSpy(EphemeralDataContext):
    """
    Simply wraps around EphemeralDataContext but keeps tabs on specific method calls around state management.
    """

    def __init__(
        self,
        project_config: DataContextConfig,
    ) -> None:
        super().__init__(project_config)
        self.save_count = 0

    def _save_project_config(self):
        """
        No-op our persistence mechanism but increment an internal counter to ensure it was used.
        """
        self.save_count += 1


@pytest.fixture
def datasource_name() -> str:
    return "my_pandas_datasource"


@pytest.fixture
def in_memory_data_context(
    datasource_name: str,
    datasource_config: DatasourceConfig,
) -> EphemeralDataContextSpy:
    datasources = {
        datasource_name: datasource_config,
    }
    config = DataContextConfig(
        datasources=datasources, store_backend_defaults=InMemoryStoreBackendDefaults()
    )
    context = EphemeralDataContextSpy(project_config=config)
    return context


@pytest.mark.unit
def test_add_store(in_memory_data_context: EphemeralDataContextSpy):
    context = in_memory_data_context

    num_stores_before = len(context.stores)
    num_store_configs_before = len(context.config.stores)

    context.add_store(
        store_name="my_new_store",
        store_config={
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
        pytest.param(
            {"progress_bars": ProgressBarsConfig(globally=True)}, id="Mapping"
        ),
    ],
)
def test_update_project_config(
    in_memory_data_context: EphemeralDataContextSpy, config: DataContextConfig | Mapping
):
    context = in_memory_data_context

    assert context.progress_bars is None

    context.update_project_config(config)

    assert context.progress_bars["globally"] is True


@pytest.mark.unit
@pytest.mark.parametrize(
    "kwargs,expected_id",
    [
        pytest.param(
            {},
            None,
            id="no kwargs",
        ),
        pytest.param(
            {
                "id": "d53c2384-f973-4a0c-9c85-af1d67c06f58",
            },
            "d53c2384-f973-4a0c-9c85-af1d67c06f58",
            id="kwargs",
        ),
    ],
)
def test_add_or_update_datasource_updates_successfully(
    in_memory_data_context: EphemeralDataContextSpy,
    datasource_name: str,
    kwargs: dict,
    expected_id: str | None,
):
    context = in_memory_data_context

    num_datasource_before = len(context.datasources)
    num_datasource_configs_before = len(context.config.datasources)

    assert (
        datasource_name in context.datasources
    ), f"Downstream logic in the test relies on {datasource_name} being a datasource; please check your fixtures."

    datasource = context.add_or_update_datasource(name=datasource_name, **kwargs)
    # Let's `id` as an example attr to change so we don't need to assert against the whole config
    assert datasource.config["id"] == expected_id

    num_datasource_after = len(context.datasources)
    num_datasource_configs_after = len(context.config.datasources)

    assert num_datasource_after == num_datasource_before
    assert num_datasource_configs_after == num_datasource_configs_before
    assert context.save_count == 1


@pytest.mark.unit
def test_add_or_update_datasource_adds_successfully(
    in_memory_data_context: EphemeralDataContextSpy,
):
    context = in_memory_data_context

    num_datasource_before = len(context.datasources)
    num_datasource_configs_before = len(context.config.datasources)

    datasource_name = "my_brand_new_datasource"

    assert datasource_name not in context.datasources

    _ = context.add_or_update_datasource(
        name=datasource_name,
        module_name="great_expectations.datasource",
        class_name="PandasDatasource",
    )

    num_datasource_after = len(context.datasources)
    num_datasource_configs_after = len(context.config.datasources)

    assert datasource_name in context.datasources
    assert num_datasource_after == num_datasource_before + 1
    assert num_datasource_configs_after == num_datasource_configs_before + 1
    assert context.save_count == 1
