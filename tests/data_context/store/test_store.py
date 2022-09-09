import pytest

from great_expectations.core.configuration import AbstractConfig
from great_expectations.data_context.store.store import Store


@pytest.mark.unit
def test_ge_cloud_response_json_to_object_dict() -> None:
    store = Store()
    data = {"foo": "bar", "baz": "qux"}
    assert store.ge_cloud_response_json_to_object_dict(response_json=data) == data


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
