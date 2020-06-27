import logging

import pytest

from great_expectations.data_context.store import DatabaseStoreBackend
from great_expectations.exceptions import StoreBackendError


def test_database_store_backend_get_url_for_key(caplog, sa):
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
    key = "1"
    assert "postgresql://test_ci/1" == store_backend.get_url_for_key(key)

    # non-existing key : should still work
    key = "not_here"
    assert "postgresql://test_ci/not_here" == store_backend.get_url_for_key(key)


def test_database_store_backend_duplicate_key_violation(caplog, sa):
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

    assert len(caplog.messages) == 0
    caplog.set_level(logging.INFO, "great_expectations")
    store_backend.set(key, "hello")
    assert len(caplog.messages) == 1
    assert "already exists with the same value" in caplog.messages[0]

    with pytest.raises(StoreBackendError) as exc:
        store_backend.set(key, "world")

    assert "Integrity error" in str(exc.value)
