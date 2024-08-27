import os
import uuid

import pytest

from great_expectations.data_context.store.metric_store import MetricStore
from great_expectations.data_context.util import instantiate_class_from_config


@pytest.fixture(
    params=[
        {
            "class_name": "MetricStore",
            "store_backend": {
                "class_name": "DatabaseStoreBackend",
                "credentials": {
                    "drivername": "postgresql",
                    "username": "postgres",
                    "password": "",
                    "host": os.getenv("GE_TEST_LOCAL_DB_HOSTNAME", "localhost"),
                    "port": "5432",
                    "database": "test_ci",
                },
            },
        },
        {
            "class_name": "MetricStore",
            "module_name": "great_expectations.data_context.store",
        },
    ]
)
def param_store(request, test_backends):
    # If we have a backend configuration but we do not have postgres configured, skip
    backend_config = request.param.get("store_backend", None)
    if backend_config:
        if backend_config.get("credentials", {}).get("drivername", None) == "postgresql":
            if "postgresql" not in test_backends:
                pytest.skip("skipping fixture because postgresql not selected")

    return instantiate_class_from_config(
        config=request.param,
        config_defaults={
            "module_name": "great_expectations.data_context.store",
        },
        runtime_environment={},
    )


@pytest.fixture(
    params=[
        {
            "class_name": "MetricStore",
            "store_backend": {
                "class_name": "InMemoryStoreBackend",
            },
        },
        {
            "class_name": "MetricStore",
            "module_name": "great_expectations.data_context.store",
        },
    ]
)
def in_memory_param_store(request, test_backends):
    # If we have a backend configuration but we do not have postgres configured, skip
    backend_config = request.param.get("store_backend", None)
    if backend_config:
        if backend_config.get("credentials", {}).get("drivername", None) == "postgresql":
            if "postgresql" not in test_backends:
                pytest.skip("skipping fixture because postgresql not selected")

    return instantiate_class_from_config(
        config=request.param,
        config_defaults={
            "module_name": "great_expectations.data_context.store",
        },
        runtime_environment={},
    )


@pytest.mark.unit
def test_metric_store_store_backend_id(in_memory_param_store):
    """
    What does this test and why?
    A Store should be able to report it's store_backend_id
    which is set when the StoreBackend is instantiated.
    """
    # Check that store_backend_id exists can be read
    assert in_memory_param_store.store_backend_id is not None
    # Check that store_backend_id is a valid UUID
    assert isinstance(in_memory_param_store.store_backend_id, uuid.UUID)


@pytest.mark.unit
def test_metric_store_serialize() -> None:
    store = MetricStore()

    value = {"foo": "bar"}
    assert store.serialize(value=value) == '{"value": {"foo": "bar"}}'


@pytest.mark.unit
def test_metric_store_deserialize() -> None:
    store = MetricStore()

    value = '{"value": {"foo": "bar"}}'
    assert store.deserialize(value=value) == {"foo": "bar"}
