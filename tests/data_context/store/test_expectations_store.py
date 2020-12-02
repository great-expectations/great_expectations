import pytest

import tests.test_utils as test_utils
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.data_context.store import (
    DatabaseStoreBackend,
    ExpectationsStore,
)
from great_expectations.data_context.types.resource_identifiers import (
    ExpectationSuiteIdentifier,
)
from great_expectations.exceptions import StoreBackendError
from great_expectations.util import gen_directory_tree_str


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


def test_expectations_store_report_store_backend_id_in_memory_store_backend():
    """
    What does this test and why?
    A Store should be able to report it's store_backend_id
    which is set when the StoreBackend is instantiated.
    """
    in_memory_expectations_store = ExpectationsStore()
    # Check that store_backend_id exists can be read
    assert in_memory_expectations_store.store_backend_id is not None
    # Check that store_backend_id is a valid UUID
    assert test_utils.validate_uuid4(in_memory_expectations_store.store_backend_id)


def test_expectations_store_report_same_id_with_same_configuration_TupleFilesystemStoreBackend(
    tmp_path_factory,
):
    """
    What does this test and why?
    A store with the same config (must be persistent store) should report the same store_backend_id
    """
    path = "dummy_str"
    project_path = str(
        tmp_path_factory.mktemp(
            "test_expectations_store_report_same_id_with_same_configuration__dir"
        )
    )

    assert (
        gen_directory_tree_str(project_path)
        == """\
test_expectations_store_report_same_id_with_same_configuration__dir0/
"""
    )

    # Check two stores with the same config
    persistent_expectations_store = ExpectationsStore(
        store_backend={
            "class_name": "TupleFilesystemStoreBackend",
            "base_directory": project_path,
        }
    )
    # Check successful initialization with a store_backend_id
    initialized_directory_tree_with_store_backend_id = """\
test_expectations_store_report_same_id_with_same_configuration__dir0/
    .ge_store_backend_id
"""
    assert (
        gen_directory_tree_str(project_path)
        == initialized_directory_tree_with_store_backend_id
    )
    assert persistent_expectations_store.store_backend_id is not None

    # Check that a duplicate store reports the same store_backend_id
    persistent_expectations_store_duplicate = ExpectationsStore(
        store_backend={
            "class_name": "TupleFilesystemStoreBackend",
            "base_directory": project_path,
        }
    )
    assert persistent_expectations_store_duplicate.store_backend_id is not None
    assert (
        persistent_expectations_store.store_backend_id
        == persistent_expectations_store_duplicate.store_backend_id
    )
    # Check no change to filesystem
    assert (
        gen_directory_tree_str(project_path)
        == initialized_directory_tree_with_store_backend_id
    )
