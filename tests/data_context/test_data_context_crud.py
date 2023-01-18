import pytest

from great_expectations.data_context.data_context.ephemeral_data_context import (
    EphemeralDataContext,
)
from great_expectations.data_context.types.base import (
    DataContextConfig,
    InMemoryStoreBackendDefaults,
)
from great_expectations.exceptions.exceptions import StoreConfigurationError


class StubEphemeralDataContext(EphemeralDataContext):
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
def in_memory_data_context() -> StubEphemeralDataContext:
    config = DataContextConfig(store_backend_defaults=InMemoryStoreBackendDefaults())
    context = StubEphemeralDataContext(project_config=config)
    return context


@pytest.mark.unit
def test_add_store(in_memory_data_context: StubEphemeralDataContext):
    context = in_memory_data_context
    num_stores_before = len(context.stores)

    context.add_store(
        store_name="my_new_store",
        store_config={
            "module_name": "great_expectations.data_context.store",
            "class_name": "ExpectationsStore",
        },
    )

    assert len(context.stores) == num_stores_before + 1
    assert context.save_count == 1


@pytest.mark.unit
@pytest.mark.parametrize(
    "store_name,raises",
    [
        pytest.param(
            "checkpoint_store",  # We know this to be a default name
            False,
            id="success",
        ),
        pytest.param(
            "my_fake_store",
            True,
            id="failure",
        ),
    ],
)
def test_delete_store(
    in_memory_data_context: StubEphemeralDataContext, store_name: str, raises: bool
):
    context = in_memory_data_context
    num_stores_before = len(context.stores)

    if raises:
        with pytest.raises(StoreConfigurationError):
            context.delete_store(store_name)
        assert len(context.stores) == num_stores_before
        assert context.save_count == 0
    else:
        context.delete_store(store_name)
        assert len(context.stores) == num_stores_before - 1
        assert context.save_count == 1
