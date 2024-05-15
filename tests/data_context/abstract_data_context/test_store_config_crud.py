from unittest import mock

import pytest

from great_expectations.data_context import EphemeralDataContext

SETTER_METHOD_NAMES = [
    "expectations_store_name",
    "validation_results_store_name",
    "checkpoint_store_name",
]


@pytest.mark.unit
@pytest.mark.parametrize(
    "store_setter_method_name",
    SETTER_METHOD_NAMES,
)
def test_store_name_setters(
    store_setter_method_name: str,
    ephemeral_context_with_defaults: EphemeralDataContext,
):
    new_store_name = "new_store_name"
    setattr(ephemeral_context_with_defaults, store_setter_method_name, new_store_name)
    assert getattr(ephemeral_context_with_defaults, store_setter_method_name) == new_store_name


@pytest.mark.unit
@pytest.mark.parametrize(
    "store_setter_method_name",
    SETTER_METHOD_NAMES,
)
def test_store_name_setters_persist(
    store_setter_method_name: str,
    ephemeral_context_with_defaults: EphemeralDataContext,
):
    new_store_name = "new_store_name"
    with mock.patch(
        "great_expectations.data_context.EphemeralDataContext._save_project_config"
    ) as mock_save_project_config:
        setattr(ephemeral_context_with_defaults, store_setter_method_name, new_store_name)

    mock_save_project_config.assert_called_once()
