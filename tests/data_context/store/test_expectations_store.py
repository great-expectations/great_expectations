import pytest

from great_expectations.core import ExpectationSuite
from great_expectations.data_context.store import (
    DatabaseStoreBackend,
    ExpectationsStore,
)
from great_expectations.data_context.types.resource_identifiers import (
    ExpectationSuiteIdentifier,
)
from great_expectations.exceptions import StoreBackendError


def test_expectations_store():
    my_store = ExpectationsStore()

    with pytest.raises(TypeError):
        my_store.set("not_a_ValidationResultIdentifier")

    ns_1 = ExpectationSuiteIdentifier.from_tuple(tuple("a.b.c.warning"))
    my_store.set(ns_1, ExpectationSuite(expectation_suite_name="a.b.c.warning"))
    assert my_store.get(ns_1) == ExpectationSuite(
        expectation_suite_name="a.b.c.warning"
    )

    ns_2 = ExpectationSuiteIdentifier.from_tuple(tuple("a.b.c.failure"))
    my_store.set(ns_2, ExpectationSuite(expectation_suite_name="a.b.c.failure"))
    assert my_store.get(ns_2) == ExpectationSuite(
        expectation_suite_name="a.b.c.failure"
    )

    assert set(my_store.list_keys()) == {
        ns_1,
        ns_2,
    }


def test_ExpectationsStore_with_DatabaseStoreBackend(sa):
    # Use sqlite so we don't require postgres for this test.
    connection_kwargs = {"drivername": "sqlite"}

    # First, demonstrate that we pick up default configuration
    my_store = ExpectationsStore(
        store_backend={
            "class_name": "DatabaseStoreBackend",
            "credentials": connection_kwargs,
        }
    )
    with pytest.raises(TypeError):
        my_store.get("not_a_ExpectationSuiteIdentifier")

    # first suite to add to db
    default_suite = ExpectationSuite(
        expectation_suite_name="a.b.c.warning",
        meta={"test_meta_key": "test_meta_value"},
        expectations=[],
    )

    ns_1 = ExpectationSuiteIdentifier.from_tuple(tuple("a.b.c.warning"))
    # initial set and check if first suite exists
    my_store.set(ns_1, default_suite)
    assert my_store.get(ns_1) == ExpectationSuite(
        expectation_suite_name="a.b.c.warning",
        meta={"test_meta_key": "test_meta_value"},
        expectations=[],
    )

    # update suite and check if new value exists
    updated_suite = ExpectationSuite(
        expectation_suite_name="a.b.c.warning",
        meta={"test_meta_key": "test_new_meta_value"},
        expectations=[],
    )
    my_store.set(ns_1, updated_suite)
    assert my_store.get(ns_1) == ExpectationSuite(
        expectation_suite_name="a.b.c.warning",
        meta={"test_meta_key": "test_new_meta_value"},
        expectations=[],
    )

    ns_2 = ExpectationSuiteIdentifier.from_tuple(tuple("a.b.c.failure"))
    my_store.set(ns_2, ExpectationSuite(expectation_suite_name="a.b.c.failure"))
    assert my_store.get(ns_2) == ExpectationSuite(
        expectation_suite_name="a.b.c.failure"
    )

    assert set(my_store.list_keys()) == {
        ns_1,
        ns_2,
    }
