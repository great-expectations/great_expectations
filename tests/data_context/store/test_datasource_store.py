from pathlib import Path

import pytest

from great_expectations.core.util import convert_to_json_serializable
from great_expectations.data_context.data_context import DataContext
from great_expectations.data_context.store.datasource_store import DatasourceStore
from great_expectations.data_context.types.base import DatasourceConfig
from great_expectations.data_context.types.resource_identifiers import (
    ConfigurationIdentifier,
)
from great_expectations.util import gen_directory_tree_str
from tests.test_utils import build_datasource_store_using_filesystem


@pytest.fixture
def datasource_name() -> str:
    return "my_first_datasource"


@pytest.fixture
def datasource_store_name() -> str:
    return "datasource_store"


@pytest.fixture
def datasource_key(datasource_name: str) -> ConfigurationIdentifier:
    return ConfigurationIdentifier(configuration_key=datasource_name)


@pytest.fixture
def empty_datasource_store(datasource_store_name: str) -> DatasourceStore:
    return DatasourceStore(datasource_store_name)


@pytest.fixture
def datasource_config() -> DatasourceConfig:
    return DatasourceConfig(
        class_name="Datasource",
        execution_engine={"class_name": "PandasExecutionEngine"},
        data_connectors={
            "tripdata_monthly_configured": {
                "class_name": "ConfiguredAssetFilesystemDataConnector",
                "base_directory": "/path/to/trip_data",
                "assets": {
                    "yellow": {
                        "pattern": r"yellow_tripdata_(\d{4})-(\d{2})\.csv$",
                        "group_names": ["year", "month"],
                    }
                },
            }
        },
    )


def test_expectations_store(datasource_config: DatasourceConfig):
    context: DataContext = empty_data_context
    my_store = DatasourceStore()

    with pytest.raises(TypeError):
        my_store.set(key="not_a_DatasourceIdentifier", value=datasource_config)

    # ns_1 = ExpectationSuiteIdentifier.from_tuple(tuple("a.b.c.warning"))
    # my_store.set(
    #     ns_1,
    #     ExpectationSuite(expectation_suite_name="a.b.c.warning", data_context=context),
    # )

    # ns_1_dict: dict = my_store.get(ns_1)
    # ns_1_suite: ExpectationSuite = ExpectationSuite(**ns_1_dict, data_context=context)
    # assert ns_1_suite == ExpectationSuite(
    #     expectation_suite_name="a.b.c.warning", data_context=context
    # )

    # ns_2 = ExpectationSuiteIdentifier.from_tuple(tuple("a.b.c.failure"))
    # my_store.set(
    #     ns_2,
    #     ExpectationSuite(expectation_suite_name="a.b.c.failure", data_context=context),
    # )
    # ns_2_dict: dict = my_store.get(ns_2)
    # ns_2_suite: ExpectationSuite = ExpectationSuite(**ns_2_dict, data_context=context)
    # assert ns_2_suite == ExpectationSuite(
    #     expectation_suite_name="a.b.c.failure", data_context=context
    # )

    # assert set(my_store.list_keys()) == {
    #     ns_1,
    #     ns_2,
    # }


# def test_ExpectationsStore_with_DatabaseStoreBackend(sa, empty_data_context):
#     context: DataContext = empty_data_context
#     # Use sqlite so we don't require postgres for this test.
#     connection_kwargs = {"drivername": "sqlite"}

#     # First, demonstrate that we pick up default configuration
#     my_store = ExpectationsStore(
#         store_backend={
#             "class_name": "DatabaseStoreBackend",
#             "credentials": connection_kwargs,
#         }
#     )
#     with pytest.raises(TypeError):
#         my_store.get("not_a_ExpectationSuiteIdentifier")

#     # first suite to add to db
#     default_suite = ExpectationSuite(
#         expectation_suite_name="a.b.c.warning",
#         meta={"test_meta_key": "test_meta_value"},
#         expectations=[],
#         data_context=context,
#     )

#     ns_1 = ExpectationSuiteIdentifier.from_tuple(tuple("a.b.c.warning"))
#     # initial set and check if first suite exists
#     my_store.set(ns_1, default_suite)
#     ns_1_dict: dict = my_store.get(ns_1)
#     ns_1_suite: ExpectationSuite = ExpectationSuite(**ns_1_dict, data_context=context)
#     assert ns_1_suite == ExpectationSuite(
#         expectation_suite_name="a.b.c.warning",
#         meta={"test_meta_key": "test_meta_value"},
#         expectations=[],
#         data_context=context,
#     )

#     # update suite and check if new value exists
#     updated_suite = ExpectationSuite(
#         expectation_suite_name="a.b.c.warning",
#         meta={"test_meta_key": "test_new_meta_value"},
#         expectations=[],
#         data_context=context,
#     )
#     my_store.set(ns_1, updated_suite)
#     ns_1_dict: dict = my_store.get(ns_1)
#     ns_1_suite: ExpectationSuite = ExpectationSuite(**ns_1_dict, data_context=context)
#     assert ns_1_suite == ExpectationSuite(
#         expectation_suite_name="a.b.c.warning",
#         meta={"test_meta_key": "test_new_meta_value"},
#         expectations=[],
#         data_context=context,
#     )

#     ns_2 = ExpectationSuiteIdentifier.from_tuple(tuple("a.b.c.failure"))
#     my_store.set(
#         ns_2,
#         ExpectationSuite(expectation_suite_name="a.b.c.failure", data_context=context),
#     )
#     ns_2_dict: dict = my_store.get(ns_2)
#     ns_2_suite: ExpectationSuite = ExpectationSuite(**ns_2_dict, data_context=context)
#     assert ns_2_suite == ExpectationSuite(
#         expectation_suite_name="a.b.c.failure",
#         data_context=context,
#     )

#     assert set(my_store.list_keys()) == {
#         ns_1,
#         ns_2,
#     }


# def test_expectations_store_report_store_backend_id_in_memory_store_backend():
#     """
#     What does this test and why?
#     A Store should be able to report it's store_backend_id
#     which is set when the StoreBackend is instantiated.
#     """
#     in_memory_expectations_store = ExpectationsStore()
#     # Check that store_backend_id exists can be read
#     assert in_memory_expectations_store.store_backend_id is not None
#     # Check that store_backend_id is a valid UUID
#     assert test_utils.validate_uuid4(in_memory_expectations_store.store_backend_id)


# def test_expectations_store_report_same_id_with_same_configuration_TupleFilesystemStoreBackend(
#     tmp_path_factory,
# ):
#     """
#     What does this test and why?
#     A store with the same config (must be persistent store) should report the same store_backend_id
#     """
#     path = "dummy_str"
#     project_path = str(
#         tmp_path_factory.mktemp(
#             "test_expectations_store_report_same_id_with_same_configuration__dir"
#         )
#     )

#     assert (
#         gen_directory_tree_str(project_path)
#         == """\
# test_expectations_store_report_same_id_with_same_configuration__dir0/
# """
#     )

#     # Check two stores with the same config
#     persistent_expectations_store = ExpectationsStore(
#         store_backend={
#             "class_name": "TupleFilesystemStoreBackend",
#             "base_directory": project_path,
#         }
#     )
#     # Check successful initialization with a store_backend_id
#     initialized_directory_tree_with_store_backend_id = """\
# test_expectations_store_report_same_id_with_same_configuration__dir0/
#     .ge_store_backend_id
# """
#     assert (
#         gen_directory_tree_str(project_path)
#         == initialized_directory_tree_with_store_backend_id
#     )
#     assert persistent_expectations_store.store_backend_id is not None

#     # Check that a duplicate store reports the same store_backend_id
#     persistent_expectations_store_duplicate = ExpectationsStore(
#         store_backend={
#             "class_name": "TupleFilesystemStoreBackend",
#             "base_directory": project_path,
#         }
#     )
#     assert persistent_expectations_store_duplicate.store_backend_id is not None
#     assert (
#         persistent_expectations_store.store_backend_id
#         == persistent_expectations_store_duplicate.store_backend_id
#     )
#     # Check no change to filesystem
#     assert (
#         gen_directory_tree_str(project_path)
#         == initialized_directory_tree_with_store_backend_id
#     )


# @mock.patch(
#     "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
# )
# def test_instantiation_with_test_yaml_config(
#     mock_emit, caplog, empty_data_context_stats_enabled
# ):
#     empty_data_context_stats_enabled.test_yaml_config(
#         yaml_config="""
# module_name: great_expectations.data_context.store.expectations_store
# class_name: ExpectationsStore
# store_backend:
#     module_name: great_expectations.data_context.store.store_backend
#     class_name: InMemoryStoreBackend
# """
#     )
#     assert mock_emit.call_count == 1
#     # Substitute current anonymized name since it changes for each run
#     anonymized_name = mock_emit.call_args_list[0][0][0]["event_payload"][
#         "anonymized_name"
#     ]
#     assert mock_emit.call_args_list == [
#         mock.call(
#             {
#                 "event": "data_context.test_yaml_config",
#                 "event_payload": {
#                     "anonymized_name": anonymized_name,
#                     "parent_class": "ExpectationsStore",
#                     "anonymized_store_backend": {
#                         "parent_class": "InMemoryStoreBackend"
#                     },
#                 },
#                 "success": True,
#             }
#         ),
#     ]

#     # Confirm that logs do not contain any exceptions or invalid messages
#     assert not usage_stats_exceptions_exist(messages=caplog.messages)
#     assert not usage_stats_invalid_messages_exist(messages=caplog.messages)
