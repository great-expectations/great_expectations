from unittest import mock

import pytest

from great_expectations import DataContext
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.data_context.store import ExpectationsStore
from great_expectations.data_context.types.resource_identifiers import (
    ExpectationSuiteIdentifier,
)
from great_expectations.util import gen_directory_tree_str
from tests import test_utils
from tests.core.usage_statistics.util import (
    usage_stats_exceptions_exist,
    usage_stats_invalid_messages_exist,
)


@pytest.mark.integration
def test_expectations_store(empty_data_context):
    context: DataContext = empty_data_context
    my_store = ExpectationsStore()

    with pytest.raises(TypeError):
        my_store.set("not_a_ValidationResultIdentifier")

    ns_1 = ExpectationSuiteIdentifier.from_tuple(tuple("a.b.c.warning"))
    my_store.set(
        ns_1,
        ExpectationSuite(expectation_suite_name="a.b.c.warning", data_context=context),
    )

    ns_1_dict: dict = my_store.get(ns_1)
    ns_1_suite = ExpectationSuite(**ns_1_dict, data_context=context)
    assert ns_1_suite == ExpectationSuite(
        expectation_suite_name="a.b.c.warning", data_context=context
    )

    ns_2 = ExpectationSuiteIdentifier.from_tuple(tuple("a.b.c.failure"))
    my_store.set(
        ns_2,
        ExpectationSuite(expectation_suite_name="a.b.c.failure", data_context=context),
    )
    ns_2_dict: dict = my_store.get(ns_2)
    ns_2_suite = ExpectationSuite(**ns_2_dict, data_context=context)
    assert ns_2_suite == ExpectationSuite(
        expectation_suite_name="a.b.c.failure", data_context=context
    )

    assert set(my_store.list_keys()) == {
        ns_1,
        ns_2,
    }


@pytest.mark.integration
def test_ExpectationsStore_with_DatabaseStoreBackend(sa, empty_data_context):
    context: DataContext = empty_data_context
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
        data_context=context,
    )

    ns_1 = ExpectationSuiteIdentifier.from_tuple(tuple("a.b.c.warning"))
    # initial set and check if first suite exists
    my_store.set(ns_1, default_suite)
    ns_1_dict: dict = my_store.get(ns_1)
    ns_1_suite = ExpectationSuite(**ns_1_dict, data_context=context)
    assert ns_1_suite == ExpectationSuite(
        expectation_suite_name="a.b.c.warning",
        meta={"test_meta_key": "test_meta_value"},
        expectations=[],
        data_context=context,
    )

    # update suite and check if new value exists
    updated_suite = ExpectationSuite(
        expectation_suite_name="a.b.c.warning",
        meta={"test_meta_key": "test_new_meta_value"},
        expectations=[],
        data_context=context,
    )
    my_store.set(ns_1, updated_suite)
    ns_1_dict: dict = my_store.get(ns_1)
    ns_1_suite = ExpectationSuite(**ns_1_dict, data_context=context)
    assert ns_1_suite == ExpectationSuite(
        expectation_suite_name="a.b.c.warning",
        meta={"test_meta_key": "test_new_meta_value"},
        expectations=[],
        data_context=context,
    )

    ns_2 = ExpectationSuiteIdentifier.from_tuple(tuple("a.b.c.failure"))
    my_store.set(
        ns_2,
        ExpectationSuite(expectation_suite_name="a.b.c.failure", data_context=context),
    )
    ns_2_dict: dict = my_store.get(ns_2)
    ns_2_suite = ExpectationSuite(**ns_2_dict, data_context=context)
    assert ns_2_suite == ExpectationSuite(
        expectation_suite_name="a.b.c.failure",
        data_context=context,
    )

    assert set(my_store.list_keys()) == {
        ns_1,
        ns_2,
    }


@pytest.mark.unit
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


@pytest.mark.integration
def test_expectations_store_report_same_id_with_same_configuration_TupleFilesystemStoreBackend(
    tmp_path_factory,
):
    """
    What does this test and why?
    A store with the same config (must be persistent store) should report the same store_backend_id
    """
    full_test_dir = tmp_path_factory.mktemp(
        "test_expectations_store_report_same_id_with_same_configuration__dir"
    )
    test_dir = full_test_dir.parts[-1]
    project_path = str(full_test_dir)

    assert (
        gen_directory_tree_str(project_path)
        == f"""\
{test_dir}/
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
    initialized_directory_tree_with_store_backend_id = f"""\
{test_dir}/
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


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
@pytest.mark.integration
def test_instantiation_with_test_yaml_config(
    mock_emit, caplog, empty_data_context_stats_enabled
):
    empty_data_context_stats_enabled.test_yaml_config(
        yaml_config="""
module_name: great_expectations.data_context.store.expectations_store
class_name: ExpectationsStore
store_backend:
    module_name: great_expectations.data_context.store.store_backend
    class_name: InMemoryStoreBackend
"""
    )
    assert mock_emit.call_count == 1
    # Substitute current anonymized name since it changes for each run
    anonymized_name = mock_emit.call_args_list[0][0][0]["event_payload"][
        "anonymized_name"
    ]
    assert mock_emit.call_args_list == [
        mock.call(
            {
                "event": "data_context.test_yaml_config",
                "event_payload": {
                    "anonymized_name": anonymized_name,
                    "parent_class": "ExpectationsStore",
                    "anonymized_store_backend": {
                        "parent_class": "InMemoryStoreBackend"
                    },
                },
                "success": True,
            }
        ),
    ]

    # Confirm that logs do not contain any exceptions or invalid messages
    assert not usage_stats_exceptions_exist(messages=caplog.messages)
    assert not usage_stats_invalid_messages_exist(messages=caplog.messages)


@pytest.mark.unit
@pytest.mark.cloud
def test_ge_cloud_response_json_to_object_dict() -> None:
    store = ExpectationsStore(store_name="expectations_store")

    suite_id = "03d61d4e-003f-48e7-a3b2-f9f842384da3"
    suite_config = {
        "expectation_suite_name": "my_suite",
    }
    response_json = {
        "data": {
            "id": suite_id,
            "attributes": {
                "suite": suite_config,
            },
        }
    }

    expected = suite_config
    expected["ge_cloud_id"] = suite_id

    actual = store.ge_cloud_response_json_to_object_dict(response_json)

    assert actual == expected
