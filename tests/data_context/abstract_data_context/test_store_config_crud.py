import pathlib
from typing import TYPE_CHECKING
from unittest import mock

import pytest

import great_expectations as gx
from great_expectations.data_context import EphemeralDataContext, FileDataContext
from great_expectations.data_context.types.base import DataContextConfig

if TYPE_CHECKING:
    from great_expectations.data_context.store.store import StoreConfigTypedDict

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


@pytest.mark.filesystem
def test_add_store_for_expectations_store_persists_a_loadable_config(
    empty_data_context: FileDataContext,
):
    """Adding a store under the expectations store's own name must round-trip to disk.

    This path injects the context id into the store backend config so the backend
    adopts the project's id rather than minting its own. That id has to survive YAML
    serialization, so it must be persisted as a string.
    """
    context = empty_data_context
    store_name = context.expectations_store_name
    assert store_name is not None
    config_filepath = pathlib.Path(context.root_directory, context.GX_YML)
    config_before = config_filepath.read_text()

    # Re-supply the store's own current config, unmodified.
    current = context.config.stores[store_name]
    config: StoreConfigTypedDict = {
        "class_name": current["class_name"],
        "store_backend": dict(current["store_backend"]),
    }

    context.add_store(name=store_name, config=config)

    persisted = config_filepath.read_text()
    assert persisted, "great_expectations.yml must not be left empty"
    assert len(persisted) >= len(config_before)

    backend_id = config["store_backend"]["manually_initialize_store_backend_id"]
    assert isinstance(backend_id, str), (
        f"context id must be serializable as a string, got {type(backend_id)}"
    )
    assert backend_id == str(context.variables.data_context_id)

    # The whole point of persisting is being able to read it back.
    reloaded = gx.get_context(mode="file", project_root_dir=context.root_directory)
    assert reloaded.expectations_store_name == store_name


@pytest.mark.filesystem
def test_save_project_config_leaves_config_intact_when_serialization_fails(
    empty_data_context: FileDataContext,
):
    """A failure while serializing must not destroy the config already on disk."""
    context = empty_data_context
    config_filepath = pathlib.Path(context.root_directory, context.GX_YML)
    config_before = config_filepath.read_text()
    assert config_before

    with mock.patch.object(
        DataContextConfig, "to_yaml_str", side_effect=ValueError("cannot serialize")
    ):
        with pytest.raises(ValueError, match="cannot serialize"):
            context._save_project_config()

    assert config_filepath.read_text() == config_before


@pytest.mark.unit
def test_add_store_for_expectations_store_omits_an_absent_context_id(
    ephemeral_context_with_defaults: EphemeralDataContext,
):
    """No context id must stay absent, not become the string "None"."""
    context = ephemeral_context_with_defaults
    store_name = context.expectations_store_name
    assert store_name is not None
    assert context.variables.data_context_id is None

    config: StoreConfigTypedDict = {
        "class_name": "ExpectationsStore",
        "store_backend": {"class_name": "InMemoryStoreBackend"},
    }
    context.add_store(name=store_name, config=config)

    assert config["store_backend"]["manually_initialize_store_backend_id"] == ""
