import logging

import pyparsing as pp
import pytest

import tests.test_utils as test_utils
from great_expectations.data_context.store import DatabaseStoreBackend
from great_expectations.data_context.util import instantiate_class_from_config
from great_expectations.exceptions import StoreBackendError


def test_database_store_backend_schema_spec(caplog, sa, test_backends):
    if "postgresql" not in test_backends:
        pytest.skip("test_database_store_backend_get_url_for_key requires postgresql")

    store_backend = DatabaseStoreBackend(
        credentials={
            "drivername": "postgresql",
            "username": "postgres",
            "password": "",
            "host": "localhost",
            "port": "5432",
            "schema": "special",
            "database": "test_ci",
        },
        table_name="test_database_store_backend_url_key",
        key_columns=["k1"],
    )

    # existing key
    key = ("2",)

    store_backend.set(key, "hello")
    assert "hello" == store_backend.get(key)

    # clean up values
    store_backend.engine.execute(f"DROP TABLE {store_backend._table};")


def test_database_store_backend_get_url_for_key(caplog, sa, test_backends):
    if "postgresql" not in test_backends:
        pytest.skip("test_database_store_backend_get_url_for_key requires postgresql")

    store_backend = DatabaseStoreBackend(
        credentials={
            "drivername": "postgresql",
            "username": "postgres",
            "password": "",
            "host": "localhost",
            "port": "5432",
            "database": "test_ci",
        },
        table_name="test_database_store_backend_url_key",
        key_columns=["k1"],
    )

    # existing key
    key = ("1",)
    assert "postgresql://test_ci/1" == store_backend.get_url_for_key(key)

    # non-existing key : should still work
    key = ("not_here",)
    assert "postgresql://test_ci/not_here" == store_backend.get_url_for_key(key)


def test_database_store_backend_duplicate_key_violation(caplog, sa, test_backends):
    if "postgresql" not in test_backends:
        pytest.skip(
            "test_database_store_backend_duplicate_key_violation requires postgresql"
        )

    store_backend = DatabaseStoreBackend(
        credentials={
            "drivername": "postgresql",
            "username": "postgres",
            "password": "",
            "host": "localhost",
            "port": "5432",
            "database": "test_ci",
        },
        table_name="test_database_store_backend_duplicate_key_violation",
        key_columns=["k1", "k2", "k3"],
    )
    key = ("1", "2", "3")

    store_backend.set(key, "hello")
    assert "hello" == store_backend.get(key)

    # default behavior doesn't throw an error because the key is updated
    store_backend.set(key, "hello")
    assert "hello" == store_backend.get(key)

    assert len(caplog.messages) == 0
    caplog.set_level(logging.INFO, "great_expectations")

    store_backend.set(
        key, "hello", allow_update=False
    )  # the only place we are testing this flag
    assert len(caplog.messages) == 1
    assert "already exists with the same value" in caplog.messages[0]

    with pytest.raises(StoreBackendError) as exc:
        store_backend.set(key, "world", allow_update=False)

    assert "Integrity error" in str(exc.value)


def test_database_store_backend_url_instantiation(caplog, sa, test_backends):
    if "postgresql" not in test_backends:
        pytest.skip("test_database_store_backend_get_url_for_key requires postgresql")

    store_backend = DatabaseStoreBackend(
        url="postgresql://postgres@localhost/test_ci",
        table_name="test_database_store_backend_url_key",
        key_columns=["k1"],
    )

    # existing key
    key = ("1",)
    assert "postgresql://test_ci/1" == store_backend.get_url_for_key(key)

    # non-existing key : should still work
    key = ("not_here",)
    assert "postgresql://test_ci/not_here" == store_backend.get_url_for_key(key)

    store_backend = DatabaseStoreBackend(
        table_name="test_database_store_backend_url_key",
        key_columns=["k1"],
        connection_string="postgresql://postgres@localhost/test_ci",
    )

    # existing key
    key = ("1",)
    assert "postgresql://test_ci/1" == store_backend.get_url_for_key(key)

    # non-existing key : should still work
    key = ("not_here",)
    assert "postgresql://test_ci/not_here" == store_backend.get_url_for_key(key)


def test_database_store_backend_id_initialization(caplog, sa, test_backends):
    """
    What does this test and why?

    NOTE: This test only has one key column which may not mirror actual functionality

    A StoreBackend should have a store_backend_id property. That store_backend_id should be read and initialized
    from an existing persistent store_backend_id during instantiation, or a new store_backend_id should be generated
    and persisted. The store_backend_id should be a valid UUIDv4
    If a new store_backend_id cannot be persisted, use an ephemeral store_backend_id.
    Persistence should be in a .ge_store_id file for for filesystem and blob-stores.
    If an existing data_context_id is available in the great_expectations.yml, use this as the expectation_store id.
    If a store_backend_id is provided via manually_initialize_store_backend_id, make sure it is retrievable.

    Note: StoreBackend & TupleStoreBackend are abstract classes, so we will test the
    concrete classes that inherit from them.
    See also test_store_backends::test_StoreBackend_id_initialization
    """

    if "postgresql" not in test_backends:
        pytest.skip("test_database_store_backend_id_initialization requires postgresql")

    store_backend = DatabaseStoreBackend(
        credentials={
            "drivername": "postgresql",
            "username": "postgres",
            "password": "",
            "host": "localhost",
            "port": "5432",
            "database": "test_ci",
        },
        table_name="test_database_store_backend_id_initialization",
        key_columns=["k1", "k2", "k3"],
    )

    # Check that store_backend_id exists can be read
    assert store_backend.store_backend_id is not None
    # Check that store_backend_id is a valid UUID
    assert test_utils.validate_uuid4(store_backend.store_backend_id)

    # Test that expectations store can be created with specified store_backend_id

    expectations_store_with_database_backend = instantiate_class_from_config(
        config={
            "class_name": "ExpectationsStore",
            "store_backend": {
                "class_name": "DatabaseStoreBackend",
                "credentials": {
                    "drivername": "postgresql",
                    "username": "postgres",
                    "password": "",
                    "host": "localhost",
                    "port": "5432",
                    "database": "test_ci",
                },
                "manually_initialize_store_backend_id": "00000000-0000-0000-0000-000000aaaaaa",
                "table_name": "ge_expectations_store",
                "key_columns": {"expectation_suite_name"},
            },
            # "name": "postgres_expectations_store",
        },
        runtime_environment=None,
        config_defaults={
            "module_name": "great_expectations.data_context.store",
            "store_name": "postgres_expectations_store",
        },
    )

    assert (
        expectations_store_with_database_backend.store_backend_id
        == "00000000-0000-0000-0000-000000aaaaaa"
    )
