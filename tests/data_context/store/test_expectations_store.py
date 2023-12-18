from __future__ import annotations

from unittest import mock
from uuid import UUID

import pytest

from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.data_context.store import ExpectationsStore
from great_expectations.data_context.types.resource_identifiers import (
    ExpectationSuiteIdentifier,
    GXCloudIdentifier,
)
from great_expectations.expectations.core import ExpectColumnValuesToBeInSet
from great_expectations.util import gen_directory_tree_str
from tests import test_utils
from tests.core.usage_statistics.util import (
    usage_stats_exceptions_exist,
    usage_stats_invalid_messages_exist,
)


@pytest.mark.filesystem
def test_expectations_store(empty_data_context):
    context = empty_data_context
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


@pytest.mark.filesystem
def test_ExpectationsStore_with_DatabaseStoreBackend(sa, empty_data_context):
    context = empty_data_context
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


@pytest.mark.filesystem
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
@pytest.mark.filesystem
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


@pytest.mark.cloud
@pytest.mark.parametrize(
    "response_json, expected, error_type",
    [
        pytest.param(
            {
                "data": {
                    "id": "03d61d4e-003f-48e7-a3b2-f9f842384da3",
                    "attributes": {
                        "suite": {
                            "expectation_suite_name": "my_suite",
                        },
                    },
                }
            },
            {
                "expectation_suite_name": "my_suite",
                "ge_cloud_id": "03d61d4e-003f-48e7-a3b2-f9f842384da3",
            },
            None,
            id="single_config",
        ),
        pytest.param({"data": []}, None, ValueError, id="empty_payload"),
        pytest.param(
            {
                "data": [
                    {
                        "id": "03d61d4e-003f-48e7-a3b2-f9f842384da3",
                        "attributes": {
                            "suite": {
                                "expectation_suite_name": "my_suite",
                            },
                        },
                    }
                ]
            },
            {
                "expectation_suite_name": "my_suite",
                "ge_cloud_id": "03d61d4e-003f-48e7-a3b2-f9f842384da3",
            },
            None,
            id="single_config_in_list",
        ),
    ],
)
def test_gx_cloud_response_json_to_object_dict(
    response_json: dict, expected: dict | None, error_type: Exception | None
) -> None:
    if error_type:
        with pytest.raises(error_type):
            _ = ExpectationsStore.gx_cloud_response_json_to_object_dict(response_json)
    else:
        actual = ExpectationsStore.gx_cloud_response_json_to_object_dict(response_json)
        assert actual == expected


@pytest.mark.unit
def test_get_key_in_non_cloud_mode(empty_data_context):
    name = "test-name"
    suite = ExpectationSuite(expectation_suite_name=name)
    key = empty_data_context.expectations_store.get_key(suite)
    assert isinstance(key, ExpectationSuiteIdentifier)


@pytest.mark.unit
def test_get_key_in_cloud_mode(empty_data_context_in_cloud_mode):
    cloud_data_context = empty_data_context_in_cloud_mode
    name = "test-name"
    suite = ExpectationSuite(expectation_suite_name=name)
    key = cloud_data_context.expectations_store.get_key(suite)
    assert isinstance(key, GXCloudIdentifier)


@pytest.mark.cloud
def test_add_expectation_success_cloud_backend(empty_cloud_data_context):
    # Arrange
    context = empty_cloud_data_context
    store = context.expectations_store
    suite_name = "test-suite"
    suite = context.add_expectation_suite(suite_name)
    key = store.get_key(suite)
    expectation = ExpectColumnValuesToBeInSet(
        column="a",
        value_set=[1, 2, 3],
        result_format="BASIC",
    )

    # Act
    store.add_expectation(suite=suite, expectation=expectation)

    # Assert
    updated_suite_dict = store.get(key=key)
    updated_suite = ExpectationSuite(**updated_suite_dict)
    added_expectation = updated_suite.expectations[0]
    assert UUID(added_expectation.id)
    assert expectation.column == added_expectation.column
    assert expectation.value_set == added_expectation.value_set
    assert expectation.result_format == added_expectation.result_format


@pytest.mark.filesystem
def test_add_expectation_success_file_backend(empty_data_context):
    # Arrange
    context = empty_data_context
    store = context.expectations_store
    suite_name = "test-suite"
    suite = context.add_expectation_suite(suite_name)
    key = store.get_key(suite)
    expectation = ExpectColumnValuesToBeInSet(
        column="a",
        value_set=[1, 2, 3],
        result_format="BASIC",
    )

    # Act
    store.add_expectation(suite=suite, expectation=expectation)

    # Assert
    updated_suite_dict = store.get(key=key)
    updated_suite = ExpectationSuite(**updated_suite_dict)
    added_expectation = updated_suite.expectations[0]
    assert UUID(added_expectation.id)
    assert expectation.column == added_expectation.column
    assert expectation.value_set == added_expectation.value_set
    assert expectation.result_format == added_expectation.result_format


@pytest.mark.filesystem
def test_add_expectation_disregards_provided_id(empty_data_context):
    # Arrange
    context = empty_data_context
    store = context.expectations_store
    suite_name = "test-suite"
    suite = context.add_expectation_suite(suite_name)
    key = store.get_key(suite)
    provided_id = "e86bb8a8-b75f-4efb-a3bb-210b6440661e"
    expectation = ExpectColumnValuesToBeInSet(
        id=provided_id,
        column="a",
        value_set=[1, 2, 3],
        result_format="BASIC",
    )

    # Act
    store.add_expectation(suite=suite, expectation=expectation)

    # Assert
    updated_suite_dict = store.get(key=key)
    updated_suite = ExpectationSuite(**updated_suite_dict)
    added_expectation = updated_suite.expectations[0]
    assert UUID(added_expectation.id)
    assert added_expectation.id != provided_id


@pytest.mark.cloud
def test_update_expectation_success_cloud_backend(empty_cloud_data_context):
    # Arrange
    context = empty_cloud_data_context
    store = context.expectations_store
    suite_name = "test-suite"
    expectation = ExpectColumnValuesToBeInSet(
        column="a",
        value_set=[1, 2, 3],
        result_format="BASIC",
    )
    suite = context.add_expectation_suite(
        suite_name, expectations=[expectation.configuration]
    )
    key = store.get_key(suite)
    expectation = suite.expectations[0]
    updated_column_name = "foo"
    expectation.column = updated_column_name

    # Act
    store.update_expectation(suite=suite, expectation=expectation)

    # Assert
    updated_suite_dict = store.get(key=key)
    updated_suite = ExpectationSuite(**updated_suite_dict)
    updated_expectation = updated_suite.expectations[0]
    assert updated_expectation.id == expectation.id
    assert updated_expectation.column == updated_column_name


@pytest.mark.filesystem
def test_update_expectation_success_file_backend(empty_data_context):
    # Arrange
    context = empty_data_context
    store = context.expectations_store
    suite_name = "test-suite"
    expectation = ExpectColumnValuesToBeInSet(
        column="a",
        value_set=[1, 2, 3],
        result_format="BASIC",
    )
    suite = context.add_expectation_suite(
        suite_name, expectations=[expectation.configuration]
    )
    key = store.get_key(suite)
    expectation = suite.expectations[0]
    updated_column_name = "foo"
    expectation.column = updated_column_name

    # Act
    store.update_expectation(suite=suite, expectation=expectation)

    # Assert
    updated_suite_dict = store.get(key=key)
    updated_suite = ExpectationSuite(**updated_suite_dict)
    updated_expectation = updated_suite.expectations[0]
    assert updated_expectation.id == expectation.id
    assert updated_expectation.column == updated_column_name


@pytest.mark.filesystem
def test_update_expectation_raises_error_for_missing_expectation(empty_data_context):
    # Arrange
    context = empty_data_context
    store = context.expectations_store
    suite_name = "test-suite"
    expectation = ExpectColumnValuesToBeInSet(
        id="2b284004-0e0e-455d-a7f4-11e162fd06c9",
        column="a",
        value_set=[1, 2, 3],
        result_format="BASIC",
    )
    suite = context.add_expectation_suite(
        suite_name, expectations=[expectation.configuration]
    )
    key = store.get_key(suite)

    # Act
    with pytest.raises(
        KeyError, match="Cannot update Expectation because it was not found."
    ):
        store.update_expectation(suite=suite, expectation=expectation)

    # Assert
    updated_suite_dict = store.get(key=key)
    updated_suite = ExpectationSuite(**updated_suite_dict)
    assert suite == updated_suite
