import pytest

from great_expectations.data_context.data_context.ephemeral_data_context import (
    EphemeralDataContext,
)
from great_expectations.data_context.types.base import (
    DataContextConfig,
    InMemoryStoreBackendDefaults,
)
from great_expectations.exceptions.exceptions import StoreConfigurationError
from great_expectations.util import get_context


@pytest.fixture
def in_memory_data_context() -> EphemeralDataContext:
    config = DataContextConfig(store_backend_defaults=InMemoryStoreBackendDefaults())
    context = get_context(project_config=config)
    return context


@pytest.mark.unit
def test_add_store(in_memory_data_context: EphemeralDataContext):
    context = in_memory_data_context
    assert len(context.stores) == 5


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
    in_memory_data_context: EphemeralDataContext, store_name: str, raises: bool
):
    context = in_memory_data_context
    num_stores_before = len(context.stores)

    if raises:
        with pytest.raises(StoreConfigurationError):
            context.delete_store(store_name)
        assert len(context.stores) == num_stores_before
    else:
        context.delete_store(store_name)
        assert len(context.stores) == num_stores_before - 1
