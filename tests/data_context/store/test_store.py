import uuid
from dataclasses import dataclass, replace
from typing import Any, Optional
from unittest import mock

import pytest

import great_expectations.exceptions as gx_exceptions
from great_expectations.core.configuration import AbstractConfig
from great_expectations.core.data_context_key import DataContextKey, StringKey
from great_expectations.data_context.store.in_memory_store_backend import InMemoryStoreBackend
from great_expectations.data_context.store.store import Store
from great_expectations.exceptions.exceptions import StoreBackendError


@dataclass
class DummyModel:
    id: Optional[str]
    foo: str


@pytest.mark.unit
def test_gx_cloud_response_json_to_object_dict() -> None:
    data = {"foo": "bar", "baz": "qux"}
    assert Store.gx_cloud_response_json_to_object_dict(response_json=data) == data


@pytest.mark.unit
def test_store_name_property_and_defaults() -> None:
    store = Store()
    assert store.store_name == "no_store_name"


@pytest.mark.unit
def test_store_serialize() -> None:
    store = Store()
    value = AbstractConfig(id="abc123", name="my_config")
    assert store.serialize(value) == value


@pytest.mark.unit
def test_store_deserialize() -> None:
    store = Store()
    value = {"a": "b"}
    assert store.deserialize(value) == value


@pytest.mark.unit
def test_build_store_from_config_success():
    store_name = "my_new_store"
    store_config = {
        "module_name": "great_expectations.data_context.store",
        "class_name": "ExpectationsStore",
    }
    store = Store.build_store_from_config(
        name=store_name,
        config=store_config,
    )
    assert isinstance(store, Store)


@pytest.mark.unit
@pytest.mark.parametrize(
    "store_config,module_name",
    [
        pytest.param(None, "great_expectations.data_context.store", id="config_none"),
        pytest.param(
            {
                "module_name": "great_expectations.data_context.store",
                "class_name": "ExpectationsStore",
            },
            None,
            id="module_name_none",
        ),
        pytest.param(None, None, id="config_and_module_name_both_none"),
    ],
)
def test_build_store_from_config_failure(store_config: dict, module_name: str):
    with pytest.raises(gx_exceptions.StoreConfigurationError):
        Store.build_store_from_config(
            name="my_new_store",
            config=store_config,
            module_name=module_name,
        )


@pytest.mark.unit
def test_store_add_success():
    store = Store()
    key = StringKey("foo")
    value = "bar"

    store.add(key=key, value=value)
    assert store.has_key(key)


@pytest.mark.unit
@mock.patch.object(InMemoryStoreBackend, "add")
def test_store_add_success__adds_id(mock_store_backend_add):
    """Ensure that if we get an id on the new object, we add an id to the input value"""
    new_id = str(uuid.uuid4())

    def mock_add(key: DataContextKey, value: Any, **kwargs):
        # our backends currently return new objects, and in the case of cloud, they will have an id
        return replace(value, id=new_id)

    mock_store_backend_add.side_effect = mock_add
    store = Store()
    key = StringKey("foo")
    original_value = DummyModel(id=None, foo="bar")

    store.add(key=key, value=original_value)
    assert original_value.id == new_id


@pytest.mark.unit
@mock.patch.object(InMemoryStoreBackend, "add")
def test_store_add_success__no_id(mock_store_backend_add):
    """Ensure that if we get an id on the new object, we add an id to the input value"""

    def mock_add(key: DataContextKey, value: Any, **kwargs):
        return replace(value)

    mock_store_backend_add.side_effect = mock_add
    store = Store()
    key = StringKey("foo")
    original_value = DummyModel(id=None, foo="bar")

    store.add(key=key, value=original_value)
    assert original_value.id is None


@pytest.mark.unit
def test_store_add_failure():
    store = Store()
    key = StringKey("foo")
    value = "bar"

    store.add(key=key, value=value)
    with pytest.raises(StoreBackendError) as e:
        store.add(key=key, value=value)

    assert "Store already has the following key" in str(e.value)


@pytest.mark.unit
def test_store_update_success():
    store = Store()
    key = StringKey("foo")
    value = "bar"
    updated_value = "baz"

    store.add(key=key, value=value)
    store.update(key=key, value=updated_value)

    assert store.get(key) == updated_value


@pytest.mark.unit
def test_store_update_failure():
    store = Store()
    key = StringKey("foo")
    value = "bar"

    with pytest.raises(StoreBackendError) as e:
        store.update(key=key, value=value)

    assert "Store does not have a value associated the following key" in str(e.value)


@pytest.mark.unit
@pytest.mark.parametrize("previous_key_exists", [True, False])
def test_store_add_or_update(previous_key_exists: bool):
    store = Store()
    key = StringKey("foo")
    value = "bar"

    if previous_key_exists:
        store.add(key=key, value=None)

    store.add_or_update(key=key, value=value)
    assert store.get(key) == value
