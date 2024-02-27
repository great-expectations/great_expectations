import pytest

import great_expectations as gx
from great_expectations.core.batch_config import BatchConfig
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.validation_config import ValidationConfig
from great_expectations.data_context.store import ValidationConfigStore


@pytest.fixture
def ephemeral_store():
    return ValidationConfigStore(store_name="ephemeral_validation_config_store")


@pytest.fixture
def file_backed_store(tmp_path):
    base_directory = tmp_path / "base_dir"
    return ValidationConfigStore(
        store_name="file_backed_validation_config_store",
        store_backend={
            "class_name": "TupleFilesystemStoreBackend",
            "base_directory": base_directory,
        },
    )


@pytest.mark.parametrize("store_fixture", ["ephemeral_store", "file_backed_store"])
def test_foo(request, store_fixture):
    _ = gx.get_context(mode="ephemeral")

    store = request.getfixturevalue(store_fixture)
    validation = ValidationConfig(
        name="my_validation",
        data=BatchConfig(name="my_batch_config"),
        suite=ExpectationSuite(name="my_suite"),
    )

    key = store.get_key("foo")
    store.add(key, validation)

    assert store.get(key) == validation
