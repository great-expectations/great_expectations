from __future__ import annotations

from uuid import UUID

import pytest

import great_expectations.expectations as gxe
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.data_context.data_context.cloud_data_context import CloudDataContext
from great_expectations.data_context.store import ExpectationsStore
from great_expectations.data_context.types.resource_identifiers import (
    ExpectationSuiteIdentifier,
    GXCloudIdentifier,
)
from great_expectations.util import gen_directory_tree_str


@pytest.mark.filesystem
def test_expectations_store():
    my_store = ExpectationsStore()

    with pytest.raises(TypeError):
        my_store.set("not_a_ValidationResultIdentifier")

    ns_1 = ExpectationSuiteIdentifier.from_tuple(tuple("a.b.c.warning"))
    my_store.set(
        ns_1,
        ExpectationSuite(name="a.b.c.warning"),
    )

    ns_1_dict: dict = my_store.get(ns_1)
    ns_1_suite = ExpectationSuite(**ns_1_dict)
    assert ns_1_suite == ExpectationSuite(name="a.b.c.warning")

    ns_2 = ExpectationSuiteIdentifier.from_tuple(tuple("a.b.c.failure"))
    my_store.set(
        ns_2,
        ExpectationSuite(name="a.b.c.failure"),
    )
    ns_2_dict: dict = my_store.get(ns_2)
    ns_2_suite = ExpectationSuite(**ns_2_dict)
    assert ns_2_suite == ExpectationSuite(name="a.b.c.failure")

    assert set(my_store.list_keys()) == {
        ns_1,
        ns_2,
    }


@pytest.mark.filesystem
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
        name="a.b.c.warning",
        meta={"test_meta_key": "test_meta_value"},
        expectations=[],
    )

    ns_1 = ExpectationSuiteIdentifier.from_tuple(tuple("a.b.c.warning"))
    # initial set and check if first suite exists
    my_store.set(ns_1, default_suite)
    ns_1_dict: dict = my_store.get(ns_1)
    ns_1_suite = ExpectationSuite(**ns_1_dict)
    assert ns_1_suite == ExpectationSuite(
        name="a.b.c.warning",
        meta={"test_meta_key": "test_meta_value"},
        expectations=[],
    )

    # update suite and check if new value exists
    updated_suite = ExpectationSuite(
        name="a.b.c.warning",
        meta={"test_meta_key": "test_new_meta_value"},
        expectations=[],
    )
    my_store.set(ns_1, updated_suite)
    ns_1_dict: dict = my_store.get(ns_1)
    ns_1_suite = ExpectationSuite(**ns_1_dict)
    assert ns_1_suite == ExpectationSuite(
        name="a.b.c.warning",
        meta={"test_meta_key": "test_new_meta_value"},
        expectations=[],
    )

    ns_2 = ExpectationSuiteIdentifier.from_tuple(tuple("a.b.c.failure"))
    my_store.set(
        ns_2,
        ExpectationSuite(name="a.b.c.failure"),
    )
    ns_2_dict: dict = my_store.get(ns_2)
    ns_2_suite = ExpectationSuite(**ns_2_dict)
    assert ns_2_suite == ExpectationSuite(
        name="a.b.c.failure",
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
    assert isinstance(in_memory_expectations_store.store_backend_id, UUID)


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
    assert gen_directory_tree_str(project_path) == initialized_directory_tree_with_store_backend_id

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
    assert gen_directory_tree_str(project_path) == initialized_directory_tree_with_store_backend_id


def _create_suite_config(name: str, id: str, expectations: list[dict] | None = None) -> dict:
    return {
        "id": id,
        "name": name,
        "expectations": expectations or [],
        "meta": {},
        "notes": None,
    }


_SUITE_CONFIG = _create_suite_config("my_suite", "03d61d4e-003f-48e7-a3b2-f9f842384da3")
_SUITE_CONFIG_WITH_EXPECTATIONS = _create_suite_config(
    "my_suite_with_expectations",
    "03d61d4e-003f-48e7-a3b2-f9f842384da3",
    [
        {
            "type": "expect_column_to_exist",
            "id": "c8a239a6-fb80-4f51-a90e-40c38dffdf91",
            "kwargs": {"column": "infinities"},
            "meta": {},
            "expectation_context": None,
            "rendered_content": [],
        }
    ],
)


@pytest.mark.unit
@pytest.mark.parametrize(
    "response_json, expected, error_type",
    [
        pytest.param(
            {"data": _SUITE_CONFIG},
            _SUITE_CONFIG,
            None,
            id="single_config",
        ),
        pytest.param({"data": []}, None, ValueError, id="empty_payload"),
        pytest.param(
            {"data": [_SUITE_CONFIG]},
            _SUITE_CONFIG,
            None,
            id="single_config_in_list",
        ),
        pytest.param(
            {"data": _SUITE_CONFIG_WITH_EXPECTATIONS},
            _SUITE_CONFIG_WITH_EXPECTATIONS,
            None,
            id="null_result_format",
        ),
    ],
)
def test_gx_cloud_response_json_to_object_dict(
    response_json: dict, expected: dict | None, error_type: type[Exception] | None
) -> None:
    if error_type:
        with pytest.raises(error_type):
            _ = ExpectationsStore.gx_cloud_response_json_to_object_dict(response_json)
    else:
        actual = ExpectationsStore.gx_cloud_response_json_to_object_dict(response_json)
        assert actual == expected


@pytest.mark.unit
def test_gx_cloud_response_json_to_object_collection():
    response_json = {
        "data": [
            _SUITE_CONFIG,
            _SUITE_CONFIG_WITH_EXPECTATIONS,
        ]
    }

    result = ExpectationsStore.gx_cloud_response_json_to_object_collection(response_json)

    expected = [_SUITE_CONFIG, _SUITE_CONFIG_WITH_EXPECTATIONS]
    assert result == expected


@pytest.mark.unit
def test_get_key_in_non_cloud_mode(empty_data_context):
    name = "test-name"
    suite = ExpectationSuite(name=name)
    key = empty_data_context.expectations_store.get_key(name=suite.name, id=suite.id)
    assert isinstance(key, ExpectationSuiteIdentifier)
    assert key.name == name


@pytest.mark.unit
def test_get_key_in_cloud_mode(empty_data_context_in_cloud_mode):
    cloud_data_context = empty_data_context_in_cloud_mode
    name = "test-name"
    suite = ExpectationSuite(name=name)
    key = cloud_data_context.expectations_store.get_key(name=suite.name, id=suite.id)
    assert isinstance(key, GXCloudIdentifier)
    assert key.resource_name == name


@pytest.mark.cloud
def test_add_expectation_success_cloud_backend(empty_cloud_data_context):
    context = empty_cloud_data_context
    _test_add_expectation_success(context)


@pytest.mark.filesystem
def test_add_expectation_success_filesystem_backend(empty_data_context):
    context = empty_data_context
    _test_add_expectation_success(context)


def _test_add_expectation_success(context):
    # Arrange
    store = context.expectations_store
    suite_name = "test-suite"
    suite = ExpectationSuite(suite_name)
    context.suites.add(suite)
    expectation = gxe.ExpectColumnValuesToBeInSet(
        column="a",
        value_set=[1, 2, 3],
        result_format="BASIC",
    )
    # Act
    store.add_expectation(suite=suite, expectation=expectation)
    # Assert
    updated_suite_dict = store.get(key=store.get_key(name=suite.name, id=suite.id))
    updated_suite = ExpectationSuite(**updated_suite_dict)
    added_expectation = updated_suite.expectations[0]
    assert UUID(added_expectation.id)
    assert expectation.column == added_expectation.column
    assert expectation.value_set == added_expectation.value_set
    assert expectation.result_format == added_expectation.result_format


@pytest.mark.filesystem
def test_add_expectation_disregards_provided_id_filesystem_backend(empty_data_context):
    context = empty_data_context
    _test_add_expectation_disregards_provided_id(context)


@pytest.mark.cloud
def test_add_expectation_disregards_provided_id_cloud_backend(empty_cloud_data_context):
    context = empty_cloud_data_context
    _test_add_expectation_disregards_provided_id(context)


def _test_add_expectation_disregards_provided_id(context):
    # Arrange
    store = context.expectations_store
    suite_name = "test-suite"
    suite = ExpectationSuite(suite_name)
    context.suites.add(suite)
    provided_id = "e86bb8a8-b75f-4efb-a3bb-210b6440661e"
    expectation = gxe.ExpectColumnValuesToBeInSet(
        id=provided_id,
        column="a",
        value_set=[1, 2, 3],
        result_format="BASIC",
    )
    # Act
    store.add_expectation(suite=suite, expectation=expectation)
    # Assert
    updated_suite_dict = store.get(key=store.get_key(name=suite.name, id=suite.id))
    updated_suite = ExpectationSuite(**updated_suite_dict)
    added_expectation = updated_suite.expectations[0]
    assert UUID(added_expectation.id)
    assert added_expectation.id != provided_id


@pytest.mark.filesystem
def test_add_adds_ids_to_suite_and_expectations(empty_data_context):
    context = empty_data_context

    expectation_a = gxe.ExpectColumnValuesToBeInSet(column="a", value_set=[1, 2, 3])
    expectation_b = gxe.ExpectColumnMaxToBeBetween(column="b", min_value=0, max_value=10)
    suite = ExpectationSuite("test-suite", expectations=[expectation_a, expectation_b])

    assert all(obj.id is None for obj in (expectation_a, expectation_b, suite))

    suite = context.suites.add(suite)

    assert all(obj.id is not None for obj in (expectation_a, expectation_b, suite))


@pytest.mark.cloud
def test_update_expectation_success_cloud_backend(empty_cloud_data_context):
    context = empty_cloud_data_context
    _test_update_expectation_success(context)


@pytest.mark.filesystem
def test_update_expectation_success_file_backend(empty_data_context):
    context = empty_data_context
    _test_update_expectation_success(context)


def _test_update_expectation_success(context):
    # Arrange
    store = context.expectations_store
    suite_name = "test-suite"
    expectation = gxe.ExpectColumnValuesToBeInSet(
        column="a",
        value_set=[1, 2, 3],
        result_format="BASIC",
    )
    suite = ExpectationSuite(suite_name, expectations=[expectation.configuration])
    context.suites.add(suite)
    # Act
    expectation = suite.expectations[0]
    assert expectation.column == "a"
    updated_column_name = "foo"
    expectation.column = updated_column_name
    store.update_expectation(suite=suite, expectation=expectation)
    # Assert
    updated_suite_dict = store.get(key=store.get_key(name=suite.name, id=suite.id))
    updated_suite = ExpectationSuite(**updated_suite_dict)
    updated_expectation = updated_suite.expectations[0]
    assert updated_expectation.id == expectation.id
    assert updated_expectation.column == updated_column_name


@pytest.mark.filesystem
def test_update_expectation_raises_error_for_missing_expectation_filesystem(
    empty_data_context,
):
    # Arrange
    context = empty_data_context
    _test_update_expectation_raises_error_for_missing_expectation(context)


@pytest.mark.cloud
def test_update_expectation_raises_error_for_missing_expectation_cloud(
    empty_cloud_data_context,
):
    # Arrange
    context = empty_cloud_data_context
    _test_update_expectation_raises_error_for_missing_expectation(context)


def _test_update_expectation_raises_error_for_missing_expectation(context):
    store = context.expectations_store
    suite_name = "test-suite"
    expectation = gxe.ExpectColumnValuesToBeInSet(
        id="2b284004-0e0e-455d-a7f4-11e162fd06c9",
        column="a",
        value_set=[1, 2, 3],
        result_format="BASIC",
    )

    suite = ExpectationSuite(suite_name, expectations=[])
    context.suites.add(suite)
    # Act
    with pytest.raises(KeyError, match="Cannot update Expectation because it was not found."):
        store.update_expectation(suite=suite, expectation=expectation)
    # Assert
    updated_suite_dict = store.get(key=store.get_key(name=suite.name, id=suite.id))
    updated_suite = ExpectationSuite(**updated_suite_dict)
    assert suite == updated_suite


@pytest.mark.cloud
def test_delete_expectation_success_cloud_backend(empty_cloud_data_context):
    # Arrange
    context = empty_cloud_data_context
    _test_delete_expectation_success(context)


@pytest.mark.filesystem
def test_delete_expectation_success_filesystem_backend(empty_data_context):
    # Arrange
    context = empty_data_context
    _test_delete_expectation_success(context)


def _test_delete_expectation_success(context):
    store = context.expectations_store
    suite_name = "test-suite"
    expectation = gxe.ExpectColumnValuesToBeInSet(
        column="a",
        value_set=[1, 2, 3],
        result_format="BASIC",
    )
    suite = ExpectationSuite(suite_name, expectations=[expectation.configuration])
    context.suites.add(suite)
    # Act
    expectation = suite.expectations[0]
    store.delete_expectation(suite=suite, expectation=expectation)
    # Assert
    updated_suite_dict = store.get(key=store.get_key(name=suite.name, id=suite.id))
    updated_suite = ExpectationSuite(**updated_suite_dict)
    assert len(updated_suite.expectations) == 0


@pytest.mark.filesystem
def test_delete_expectation_raises_error_for_missing_expectation_filesystem(
    empty_data_context,
):
    context = empty_data_context
    _test_delete_expectation_raises_error_for_missing_expectation(context)


@pytest.mark.cloud
def test_delete_expectation_raises_error_for_missing_expectation_cloud(
    empty_cloud_data_context,
):
    context = empty_cloud_data_context
    _test_delete_expectation_raises_error_for_missing_expectation(context)


def _test_delete_expectation_raises_error_for_missing_expectation(context):
    # Arrange
    store = context.expectations_store
    suite_name = "test-suite"
    existing_expectation = gxe.ExpectColumnValuesToBeInSet(
        column="a",
        value_set=[1, 2, 3],
        result_format="BASIC",
    )
    suite = ExpectationSuite(suite_name, expectations=[existing_expectation.configuration])
    context.suites.add(suite)
    # Act
    nonexistent_expectation = gxe.ExpectColumnValuesToBeInSet(
        # this ID will be different from the ID created by the Suite
        id="1296c5c8-6f7b-4cee-a09c-9037c7e40df7",
        column="a",
        value_set=[1, 2, 3],
        result_format="BASIC",
    )
    with pytest.raises(KeyError, match="Cannot delete Expectation because it was not found."):
        store.delete_expectation(suite=suite, expectation=nonexistent_expectation)
    # Assert
    updated_suite_dict = store.get(key=store.get_key(name=suite.name, id=suite.id))
    updated_suite = ExpectationSuite(**updated_suite_dict)
    if isinstance(context, CloudDataContext):
        updated_suite.render()
    assert suite == updated_suite
    assert len(updated_suite.expectations) == 1


@pytest.mark.cloud
def test_update_cloud_suite(empty_cloud_data_context):
    # Arrange
    context = empty_cloud_data_context
    store = context.expectations_store
    existing_expectation = gxe.ExpectColumnValuesToBeInSet(
        column="a",
        value_set=[1, 2, 3],
        result_format="BASIC",
    )
    suite_name = "test-suite"
    updated_suite_name = "test-suite-22"
    suite = ExpectationSuite(suite_name, expectations=[existing_expectation.configuration])
    context.suites.add(suite)

    # act
    suite.name = updated_suite_name
    store.update(key=store.get_key(name=suite_name, id=suite.id), value=suite)

    # assert
    updated_suite_dict = store.get(key=store.get_key(name=updated_suite_name, id=suite.id))
    assert updated_suite_dict["name"] == updated_suite_name
