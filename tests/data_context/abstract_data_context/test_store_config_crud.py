import pytest

from great_expectations.data_context import EphemeralDataContext


@pytest.mark.unit
@pytest.mark.parametrize(
    "store_setter",
    [
        "expectations_store_name",
        "validations_store_name",
        "profiler_store_name",
        "checkpoint_store_name",
    ],
)
def test_store_name_setters(
    store_setter: str,
    ephemeral_context_with_defaults: EphemeralDataContext,
):
    new_store_name = "new_store_name"
    setattr(ephemeral_context_with_defaults, store_setter, new_store_name)
    assert getattr(ephemeral_context_with_defaults, store_setter) == new_store_name
