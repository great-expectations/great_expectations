from unittest import mock

import pytest

import great_expectations.exceptions as ge_exceptions
from great_expectations import DataContext
from tests.core.usage_statistics.util import (
    usage_stats_exceptions_exist,
    usage_stats_invalid_messages_exist,
)

"""This module is for tests related to ensuring that test_yaml_config() emits the correct usage stats messages. Many of the tests for usage stats messages are implemented in other tests, noted below in the checklist"""

# Test usage stats for test_yaml_config
# - [x] test_test_yaml_config_usage_stats_substitution_error
# - [x] test_test_yaml_config_usage_stats_yaml_parse_error
# See test_data_context_test_yaml_config.test_config_with_yaml_error()
# - [x] test_test_yaml_config_usage_stats_store_type
# See test_data_context_test_yaml_config.test_expectations_store_with_filesystem_store_backend()
# - [NA] test_test_yaml_config_usage_stats_datasource_type_v2
# - [x] test_test_yaml_config_usage_stats_datasource_type_v3
# See test_data_context_test_yaml_config.test_datasource_config()
# and test_golden_path_sql_datasource_configuration() etc.
# - [x] test_test_yaml_config_usage_stats_checkpoint_type
# See tests.checkpoint.test_checkpoint.test_basic_checkpoint_config_validation(), etc
# - [x] test_test_yaml_config_usage_stats_data_connector
# See individual data connector tests e.g. tests.datasource.data_connector.test_configured_asset_filesystem_data_connector.test_instantiation_from_a_config(), etc.
# - [x] test_test_yaml_config_usage_stats_custom_type
# - [x] test_test_yaml_config_usage_stats_custom_type_not_ge_subclass
# - [x] test_test_yaml_config_usage_stats_custom_config_class_name_not_provided
# - [x] test_test_yaml_config_usage_stats_simple_sqlalchemy_datasource_subclass
# - [x] test_test_yaml_config_usage_stats_class_name_not_provided
# - [x] test_test_yaml_config_usage_stats_v2_api_custom_datasource
# See test_datasource_anonymizer.test_anonymize_datasource_info_v2_api_custom_subclass


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_test_yaml_config_usage_stats_substitution_error(
    mock_emit, caplog, empty_data_context_stats_enabled
):
    with pytest.raises(ge_exceptions.MissingConfigVariableError):
        _ = empty_data_context_stats_enabled.test_yaml_config(
            yaml_config="""
module_name: great_expectations.data_context.store.expectations_store
class_name: ExpectationsStore
store_backend:
    module_name: "great_expectations.data_context.store.store_backend"
    class_name: InMemoryStoreBackend
    error_on_substitution: $IDONTEXIST
    """
        )
    assert mock_emit.call_count == 1
    assert mock_emit.call_args_list == [
        mock.call(
            {
                "event": "data_context.test_yaml_config",
                "event_payload": {"diagnostic_info": ["__substitution_error__"]},
                "success": False,
            }
        ),
    ]

    # Confirm that logs do not contain any exceptions or invalid messages
    assert not usage_stats_exceptions_exist(messages=caplog.messages)
    assert not usage_stats_invalid_messages_exist(messages=caplog.messages)


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_test_yaml_config_usage_stats_custom_type(
    mock_emit, caplog, empty_data_context_stats_enabled
):
    """
    What does this test and why?
    We should be able to discern the GX parent class for a custom type and construct
    a useful usage stats event message.
    """
    data_context: DataContext = empty_data_context_stats_enabled
    _ = data_context.test_yaml_config(
        yaml_config="""
module_name: tests.data_context.fixtures.plugins
class_name: MyCustomExpectationsStore
store_backend:
    module_name: great_expectations.data_context.store.store_backend
    class_name: InMemoryStoreBackend
"""
    )
    assert mock_emit.call_count == 1
    # Substitute anonymized name & class since it changes for each run
    anonymized_name = mock_emit.call_args_list[0][0][0]["event_payload"][
        "anonymized_name"
    ]
    anonymized_class = mock_emit.call_args_list[0][0][0]["event_payload"][
        "anonymized_class"
    ]
    assert mock_emit.call_args_list == [
        mock.call(
            {
                "event": "data_context.test_yaml_config",
                "event_payload": {
                    "anonymized_name": anonymized_name,
                    "parent_class": "ExpectationsStore",
                    "anonymized_class": anonymized_class,
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


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_test_yaml_config_usage_stats_class_name_not_provided(
    mock_emit, caplog, empty_data_context_stats_enabled
):
    """
    What does this test and why?
    If a class_name is not provided, and we have run into an error state in test_yaml_config() (likely because of the missing class_name) then we should report descriptive diagnostic info.
    """
    with pytest.raises(Exception):
        # noinspection PyUnusedLocal
        my_expectation_store = empty_data_context_stats_enabled.test_yaml_config(
            yaml_config="""
module_name: great_expectations.data_context.store.expectations_store

    """
        )
    assert mock_emit.call_count == 1
    assert mock_emit.call_args_list == [
        mock.call(
            {
                "event": "data_context.test_yaml_config",
                "event_payload": {"diagnostic_info": ["__class_name_not_provided__"]},
                "success": False,
            }
        ),
    ]

    # Confirm that logs do not contain any exceptions or invalid messages
    assert not usage_stats_exceptions_exist(messages=caplog.messages)
    assert not usage_stats_invalid_messages_exist(messages=caplog.messages)


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_test_yaml_config_usage_stats_custom_config_class_name_not_provided(
    mock_emit, caplog, empty_data_context_stats_enabled
):
    """
    What does this test and why?
    If a class_name is not provided, and we have run into an error state in test_yaml_config() (likely because of the missing class_name) then we should report descriptive diagnostic info.
    This should be the case even if we are passing in a custom config.
    """
    data_context: DataContext = empty_data_context_stats_enabled
    with pytest.raises(Exception):
        _ = data_context.test_yaml_config(
            yaml_config="""
module_name: tests.data_context.fixtures.plugins.my_custom_expectations_store
store_backend:
    module_name: great_expectations.data_context.store.store_backend
    class_name: InMemoryStoreBackend
"""
        )
    assert mock_emit.call_count == 1
    assert mock_emit.call_args_list == [
        mock.call(
            {
                "event": "data_context.test_yaml_config",
                "event_payload": {
                    "diagnostic_info": ["__class_name_not_provided__"],
                },
                "success": False,
            }
        ),
    ]

    # Confirm that logs do not contain any exceptions or invalid messages
    assert not usage_stats_exceptions_exist(messages=caplog.messages)
    assert not usage_stats_invalid_messages_exist(messages=caplog.messages)


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_test_yaml_config_usage_stats_custom_type_not_ge_subclass(
    mock_emit, caplog, empty_data_context_stats_enabled
):
    """
    What does this test and why?
    We should be able to discern the GX parent class for a custom type and construct
    a useful usage stats event message.
    """
    data_context: DataContext = empty_data_context_stats_enabled
    _ = data_context.test_yaml_config(
        yaml_config="""
module_name: tests.data_context.fixtures.plugins
class_name: MyCustomNonCoreGeClass
"""
    )
    assert mock_emit.call_count == 1
    assert mock_emit.call_args_list == [
        mock.call(
            {
                "event": "data_context.test_yaml_config",
                "event_payload": {
                    "diagnostic_info": ["__custom_subclass_not_core_ge__"]
                },
                "success": True,
            }
        ),
    ]

    # Confirm that logs do not contain any exceptions or invalid messages
    assert not usage_stats_exceptions_exist(messages=caplog.messages)
    assert not usage_stats_invalid_messages_exist(messages=caplog.messages)


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_test_yaml_config_usage_stats_simple_sqlalchemy_datasource_subclass(
    mock_emit, caplog, sa, test_backends, empty_data_context_stats_enabled
):
    """
    What does this test and why?
    We should be able to discern the GX parent class for a custom type and construct
    a useful usage stats event message. This should be true for SimpleSqlalchemyDatasources.
    """

    if "postgresql" not in test_backends:
        pytest.skip(
            "test_test_yaml_config_usage_stats_simple_sqlalchemy_datasource_subclass requires postgresql"
        )

    data_context: DataContext = empty_data_context_stats_enabled
    _ = data_context.test_yaml_config(
        yaml_config="""
module_name: tests.data_context.fixtures.plugins.my_custom_simple_sqlalchemy_datasource_class
class_name: MyCustomSimpleSqlalchemyDatasource
name: some_name
introspection:
  whole_table:
    data_asset_name_suffix: __whole_table
credentials:
  drivername: postgresql
  host: localhost
  port: '5432'
  username: postgres
  password: ''
  database: postgres
"""
    )
    assert mock_emit.call_count == 1
    # Substitute anonymized name & class since it changes for each run
    anonymized_name = mock_emit.call_args_list[0][0][0]["event_payload"][
        "anonymized_name"
    ]
    anonymized_class = mock_emit.call_args_list[0][0][0]["event_payload"][
        "anonymized_class"
    ]
    anonymized_data_connector_name = mock_emit.call_args_list[0][0][0]["event_payload"][
        "anonymized_data_connectors"
    ][0]["anonymized_name"]
    assert mock_emit.call_args_list == [
        mock.call(
            {
                "event": "data_context.test_yaml_config",
                "event_payload": {
                    "anonymized_name": anonymized_name,
                    "parent_class": "SimpleSqlalchemyDatasource",
                    "anonymized_class": anonymized_class,
                    "anonymized_execution_engine": {
                        "parent_class": "SqlAlchemyExecutionEngine"
                    },
                    "anonymized_data_connectors": [
                        {
                            "anonymized_name": anonymized_data_connector_name,
                            "parent_class": "InferredAssetSqlDataConnector",
                        }
                    ],
                },
                "success": True,
            }
        ),
    ]

    # Confirm that logs do not contain any exceptions or invalid messages
    assert not usage_stats_exceptions_exist(messages=caplog.messages)
    assert not usage_stats_invalid_messages_exist(messages=caplog.messages)


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_rule_based_profiler_emits_valid_usage_stats(
    mock_emit, caplog, empty_data_context_stats_enabled, test_df, tmp_path_factory
):
    context = empty_data_context_stats_enabled
    yaml_config = """
    name: my_profiler
    class_name: RuleBasedProfiler
    module_name: great_expectations.rule_based_profiler
    config_version: 1.0
    variables:
      integer_type: INTEGER
      timestamp_type: TIMESTAMP
      max_user_id: 999999999999
      min_timestamp: 2004-10-19 10:23:54
    rules:
      my_rule_for_user_ids:
        domain_builder:
          class_name: TableDomainBuilder
        expectation_configuration_builders:
          - expectation_type: expect_column_values_to_be_of_type
            class_name: DefaultExpectationConfigurationBuilder
    """
    context.test_yaml_config(
        yaml_config=yaml_config, name="my_profiler", class_name="Profiler"
    )

    # Substitute anonymized name since it changes for each run
    anonymized_name = mock_emit.call_args_list[0][0][0]["event_payload"][
        "anonymized_name"
    ]
    assert mock_emit.call_count == 1
    assert mock_emit.call_args_list == [
        mock.call(
            {
                "event": "data_context.test_yaml_config",
                "event_payload": {
                    "anonymized_name": anonymized_name,
                    "parent_class": "RuleBasedProfiler",
                },
                "success": True,
            }
        )
    ]

    # Confirm that logs do not contain any exceptions or invalid messages
    assert not usage_stats_exceptions_exist(messages=caplog.messages)
    assert not usage_stats_invalid_messages_exist(messages=caplog.messages)
