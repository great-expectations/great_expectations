import datetime
import itertools
import json
import os
import tempfile
from unittest import mock

import pytest

import great_expectations.exceptions as ge_exceptions
from great_expectations import DataContext
from great_expectations.core import ExpectationSuite
from great_expectations.data_context.data_context import BaseDataContext
from great_expectations.data_context.store import CheckpointStore
from great_expectations.data_context.util import file_relative_path
from great_expectations.rule_based_profiler.rule_based_profiler import RuleBasedProfiler
from great_expectations.util import load_class
from tests.core.usage_statistics.util import (
    usage_stats_exceptions_exist,
    usage_stats_invalid_messages_exist,
)
from tests.test_utils import create_files_in_directory, set_directory


@pytest.fixture
def test_connectable_postgresql_db(sa, test_backends, test_df):
    """Populates a postgres DB with a `test_df` table in the `connection_test` schema to test DataConnectors against"""

    if "postgresql" not in test_backends:
        pytest.skip("skipping fixture because postgresql not selected")

    import sqlalchemy as sa

    url = sa.engine.url.URL(
        drivername="postgresql",
        username="postgres",
        password="",
        host=os.getenv("GE_TEST_LOCAL_DB_HOSTNAME", "localhost"),
        port="5432",
        database="test_ci",
    )
    engine = sa.create_engine(url)

    schema_check_results = engine.execute(
        "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'connection_test';"
    ).fetchall()
    if len(schema_check_results) == 0:
        engine.execute("CREATE SCHEMA connection_test;")

    table_check_results = engine.execute(
        """
SELECT EXISTS (
   SELECT FROM information_schema.tables
   WHERE  table_schema = 'connection_test'
   AND    table_name   = 'test_df'
);
"""
    ).fetchall()
    if table_check_results != [(True,)]:
        test_df.to_sql(name="test_df", con=engine, index=True, schema="connection_test")

    # Return a connection string to this newly-created db
    return engine


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_config_with_yaml_error(mock_emit, caplog, empty_data_context_stats_enabled):
    with pytest.raises(Exception):
        # noinspection PyUnusedLocal
        my_expectation_store = empty_data_context_stats_enabled.test_yaml_config(
            yaml_config="""
module_name: great_expectations.data_context.store.expectations_store
class_name: ExpectationsStore
store_backend:
    module_name: "great_expectations.data_context.store.store_backend"
    class_name: InMemoryStoreBackend
EGREGIOUS FORMATTING ERROR
"""
        )
    assert mock_emit.call_count == 1
    assert mock_emit.call_args_list == [
        mock.call(
            {
                "event": "data_context.test_yaml_config",
                "event_payload": {"diagnostic_info": ["__yaml_parse_error__"]},
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
def test_expectations_store_with_filesystem_store_backend(
    mock_emit, caplog, empty_data_context_stats_enabled
):
    tmp_dir = str(tempfile.mkdtemp())
    with open(os.path.join(tmp_dir, "expectations_A1.json"), "w") as f_:
        f_.write("\n")
    with open(os.path.join(tmp_dir, "expectations_A2.json"), "w") as f_:
        f_.write("\n")

    # noinspection PyUnusedLocal
    my_expectation_store = empty_data_context_stats_enabled.test_yaml_config(
        yaml_config=f"""
module_name: great_expectations.data_context.store
class_name: ExpectationsStore
store_backend:
    module_name: "great_expectations.data_context.store"
    class_name: TupleFilesystemStoreBackend
    base_directory: {tmp_dir}
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
                        "parent_class": "TupleFilesystemStoreBackend"
                    },
                },
                "success": True,
            }
        )
    ]

    # Confirm that logs do not contain any exceptions or invalid messages
    assert not usage_stats_exceptions_exist(messages=caplog.messages)
    assert not usage_stats_invalid_messages_exist(messages=caplog.messages)


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_checkpoint_store_with_filesystem_store_backend(
    mock_emit, caplog, empty_data_context_stats_enabled, tmp_path_factory
):
    tmp_dir: str = str(
        tmp_path_factory.mktemp("test_checkpoint_store_with_filesystem_store_backend")
    )
    context: DataContext = empty_data_context_stats_enabled

    yaml_config: str = f"""
    store_name: my_checkpoint_store
    class_name: CheckpointStore
    module_name: great_expectations.data_context.store
    store_backend:
        class_name: TupleFilesystemStoreBackend
        module_name: "great_expectations.data_context.store"
        base_directory: {tmp_dir}/checkpoints
    """

    my_checkpoint_store: CheckpointStore = context.test_yaml_config(
        yaml_config=yaml_config,
        return_mode="instantiated_class",
    )
    assert mock_emit.call_count == 1
    # Substitute anonymized_name since it changes for each run
    anonymized_name = mock_emit.call_args_list[0][0][0]["event_payload"][
        "anonymized_name"
    ]
    assert mock_emit.call_args_list == [
        mock.call(
            {
                "event": "data_context.test_yaml_config",
                "event_payload": {
                    "anonymized_name": anonymized_name,
                    "parent_class": "CheckpointStore",
                    "anonymized_store_backend": {
                        "parent_class": "TupleFilesystemStoreBackend"
                    },
                },
                "success": True,
            }
        ),
    ]

    report_object: dict = context.test_yaml_config(
        yaml_config=yaml_config,
        return_mode="report_object",
    )
    assert mock_emit.call_count == 2
    assert mock_emit.call_args_list == [
        mock.call(
            {
                "event": "data_context.test_yaml_config",
                "event_payload": {
                    "anonymized_name": anonymized_name,
                    "parent_class": "CheckpointStore",
                    "anonymized_store_backend": {
                        "parent_class": "TupleFilesystemStoreBackend"
                    },
                },
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "data_context.test_yaml_config",
                "event_payload": {
                    "anonymized_name": anonymized_name,
                    "parent_class": "CheckpointStore",
                    "anonymized_store_backend": {
                        "parent_class": "TupleFilesystemStoreBackend"
                    },
                },
                "success": True,
            }
        ),
    ]

    assert my_checkpoint_store.config == report_object["config"]

    expected_checkpoint_store_config: dict

    expected_checkpoint_store_config = {
        "store_name": "my_checkpoint_store",
        "class_name": "CheckpointStore",
        "module_name": "great_expectations.data_context.store.checkpoint_store",
        "store_backend": {
            "module_name": "great_expectations.data_context.store",
            "class_name": "TupleFilesystemStoreBackend",
            "base_directory": f"{tmp_dir}/checkpoints",
            "suppress_store_backend_id": True,
            "filepath_suffix": ".yml",
        },
        "overwrite_existing": False,
        "runtime_environment": {
            "root_directory": f"{context.root_directory}",
        },
    }
    assert my_checkpoint_store.config == expected_checkpoint_store_config

    checkpoint_store_name: str = my_checkpoint_store.config["store_name"]
    context.get_config()["checkpoint_store_name"] = checkpoint_store_name

    assert (
        context.get_config_with_variables_substituted().checkpoint_store_name
        == "my_checkpoint_store"
    )
    assert (
        context.get_config_with_variables_substituted().checkpoint_store_name
        == my_checkpoint_store.config["store_name"]
    )

    expected_checkpoint_store_config = {
        "store_name": "my_checkpoint_store",
        "class_name": "CheckpointStore",
        "module_name": "great_expectations.data_context.store",
        "store_backend": {
            "class_name": "TupleFilesystemStoreBackend",
            "module_name": "great_expectations.data_context.store",
            "base_directory": f"{tmp_dir}/checkpoints",
            "suppress_store_backend_id": True,
        },
    }
    assert (
        context.get_config_with_variables_substituted().stores[
            context.get_config_with_variables_substituted().checkpoint_store_name
        ]
        == expected_checkpoint_store_config
    )
    # No other usage stats calls
    assert mock_emit.call_count == 2

    # Confirm that logs do not contain any exceptions or invalid messages
    assert not usage_stats_exceptions_exist(messages=caplog.messages)
    assert not usage_stats_invalid_messages_exist(messages=caplog.messages)


@pytest.mark.filterwarnings(
    "ignore:String run_ids are deprecated*:DeprecationWarning:great_expectations.data_context.types.resource_identifiers"
)
@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_empty_store2(mock_emit, caplog, empty_data_context_stats_enabled):
    empty_data_context_stats_enabled.test_yaml_config(
        yaml_config="""
class_name: ValidationsStore
store_backend:

    module_name: "great_expectations.data_context.store.store_backend"
    class_name: InMemoryStoreBackend
"""
    )
    assert mock_emit.call_count == 1
    # Substitute anonymized_name since it changes for each run
    anonymized_name = mock_emit.call_args_list[0][0][0]["event_payload"][
        "anonymized_name"
    ]
    assert mock_emit.call_args_list == [
        mock.call(
            {
                "event": "data_context.test_yaml_config",
                "event_payload": {
                    "anonymized_name": anonymized_name,
                    "parent_class": "ValidationsStore",
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
def test_datasource_config(mock_emit, caplog, empty_data_context_stats_enabled):
    temp_dir = str(tempfile.mkdtemp())
    create_files_in_directory(
        directory=temp_dir,
        file_name_list=[
            "alex_20200809_1000.csv",
            "eugene_20200809_1500.csv",
            "james_20200811_1009.csv",
            "abe_20200809_1040.csv",
            "will_20200809_1002.csv",
            "james_20200713_1567.csv",
            "eugene_20201129_1900.csv",
            "will_20200810_1001.csv",
            "james_20200810_1003.csv",
            "alex_20200819_1300.csv",
        ],
    )
    print(temp_dir)

    return_obj = empty_data_context_stats_enabled.test_yaml_config(
        yaml_config=f"""
class_name: Datasource

execution_engine:
    class_name: PandasExecutionEngine

data_connectors:
    my_filesystem_data_connector:
        # class_name: ConfiguredAssetFilesystemDataConnector
        class_name: InferredAssetFilesystemDataConnector
        base_directory: {temp_dir}
        glob_directive: '*.csv'
        default_regex:
            pattern: (.+)_(\\d+)\\.csv
            group_names:
            - letter
            - number
""",
        return_mode="report_object",
    )

    # Test usage stats messages
    assert mock_emit.call_count == 1
    # Substitute anonymized names since it changes for each run
    anonymized_datasource_name = mock_emit.call_args_list[0][0][0]["event_payload"][
        "anonymized_name"
    ]
    anonymized_execution_engine_name = mock_emit.call_args_list[0][0][0][
        "event_payload"
    ]["anonymized_execution_engine"]["anonymized_name"]
    anonymized_data_connector_name = mock_emit.call_args_list[0][0][0]["event_payload"][
        "anonymized_data_connectors"
    ][0]["anonymized_name"]
    assert mock_emit.call_args_list == [
        mock.call(
            {
                "event": "data_context.test_yaml_config",
                "event_payload": {
                    "anonymized_name": anonymized_datasource_name,
                    "parent_class": "Datasource",
                    "anonymized_execution_engine": {
                        "anonymized_name": anonymized_execution_engine_name,
                        "parent_class": "PandasExecutionEngine",
                    },
                    "anonymized_data_connectors": [
                        {
                            "anonymized_name": anonymized_data_connector_name,
                            "parent_class": "InferredAssetFilesystemDataConnector",
                        }
                    ],
                },
                "success": True,
            }
        )
    ]

    print(json.dumps(return_obj, indent=2))

    assert set(return_obj.keys()) == {"execution_engine", "data_connectors"}
    sub_obj = return_obj["data_connectors"]["my_filesystem_data_connector"]
    # FIXME: (Sam) example_data_reference removed temporarily in PR #2590:
    # sub_obj.pop("example_data_reference")
    # assert sub_obj == {
    #     "class_name": "InferredAssetFilesystemDataConnector",
    #     "data_asset_count": 1,
    #     "example_data_asset_names": ["DEFAULT_ASSET_NAME"],
    #     "data_assets": {
    #         "DEFAULT_ASSET_NAME": {
    #             "batch_definition_count": 10,
    #             "example_data_references": [
    #                 "abe_20200809_1040.csv",
    #                 "alex_20200809_1000.csv",
    #                 "alex_20200819_1300.csv",
    #             ],
    #         }
    #     },
    #     "example_unmatched_data_references": [],
    #     "unmatched_data_reference_count": 0,
    # }
    # No other usage stats calls
    assert mock_emit.call_count == 1

    # Confirm that logs do not contain any exceptions or invalid messages
    assert not usage_stats_exceptions_exist(messages=caplog.messages)
    assert not usage_stats_invalid_messages_exist(messages=caplog.messages)


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_error_states(mock_emit, caplog, empty_data_context_stats_enabled):
    first_config: str = """
class_name: Datasource

execution_engine:
    class_name: NOT_A_REAL_CLASS_NAME
"""

    with pytest.raises(ge_exceptions.DatasourceInitializationError) as excinfo:
        empty_data_context_stats_enabled.test_yaml_config(yaml_config=first_config)
        # print(excinfo.value.message)
        # shortened_message_len = len(excinfo.value.message)
        # print("="*80)

    assert mock_emit.call_count == 1
    expected_call_args_list = [
        mock.call(
            {
                "event": "data_context.test_yaml_config",
                "event_payload": {"parent_class": "Datasource"},
                "success": False,
            }
        ),
    ]
    assert mock_emit.call_args_list == expected_call_args_list

    # Set shorten_tracebacks=True and verify that no error is thrown, even though the config is the same as before.
    # Note: a more thorough test could also verify that the traceback is indeed short.
    empty_data_context_stats_enabled.test_yaml_config(
        yaml_config=first_config,
        shorten_tracebacks=True,
    )
    assert mock_emit.call_count == 2
    expected_call_args_list.append(
        mock.call(
            {
                "event": "data_context.test_yaml_config",
                "event_payload": {"parent_class": "Datasource"},
                "success": False,
            }
        ),
    )
    assert mock_emit.call_args_list == expected_call_args_list

    # For good measure, do it again, with a different config and a different type of error
    # Note this erroneous key/value does not cause an error and is removed from the Datasource config
    temp_dir = str(tempfile.mkdtemp())
    second_config = f"""
class_name: Datasource

execution_engine:
    class_name: PandasExecutionEngine

data_connectors:
    my_filesystem_data_connector:
        # class_name: ConfiguredAssetFilesystemDataConnector
        class_name: InferredAssetFilesystemDataConnector
        base_directory: {temp_dir}
        glob_directive: '*.csv'
        default_regex:
            pattern: (.+)_(\\d+)\\.csv
            group_names:
            - letter
            - number
        NOT_A_REAL_KEY: nothing
"""

    datasource = empty_data_context_stats_enabled.test_yaml_config(
        yaml_config=second_config
    )
    assert (
        "NOT_A_REAL_KEY"
        not in datasource.config["data_connectors"]["my_filesystem_data_connector"]
    )
    assert mock_emit.call_count == 3
    # Substitute anonymized names since it changes for each run
    anonymized_datasource_name = mock_emit.call_args_list[2][0][0]["event_payload"][
        "anonymized_name"
    ]
    anonymized_execution_engine_name = mock_emit.call_args_list[2][0][0][
        "event_payload"
    ]["anonymized_execution_engine"]["anonymized_name"]
    anonymized_data_connector_name = mock_emit.call_args_list[2][0][0]["event_payload"][
        "anonymized_data_connectors"
    ][0]["anonymized_name"]
    expected_call_args_list.append(
        mock.call(
            {
                "event": "data_context.test_yaml_config",
                "event_payload": {
                    "anonymized_name": anonymized_datasource_name,
                    "parent_class": "Datasource",
                    "anonymized_execution_engine": {
                        "anonymized_name": anonymized_execution_engine_name,
                        "parent_class": "PandasExecutionEngine",
                    },
                    "anonymized_data_connectors": [
                        {
                            "anonymized_name": anonymized_data_connector_name,
                            "parent_class": "InferredAssetFilesystemDataConnector",
                        }
                    ],
                },
                "success": True,
            }
        ),
    )
    assert mock_emit.call_args_list == expected_call_args_list

    # Confirm that logs do not contain any exceptions or invalid messages
    assert not usage_stats_exceptions_exist(messages=caplog.messages)
    assert not usage_stats_invalid_messages_exist(messages=caplog.messages)


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_config_variables_in_test_yaml_config(
    mock_emit, caplog, empty_data_context_stats_enabled, sa
):
    context: DataContext = empty_data_context_stats_enabled

    db_file = file_relative_path(
        __file__,
        os.path.join("..", "test_sets", "test_cases_for_sql_data_connector.db"),
    )

    context.save_config_variable("db_file", db_file)
    context.save_config_variable(
        "data_connector_name", "my_very_awesome_data_connector"
    )
    context.save_config_variable("suffix", "__whole_table")
    context.save_config_variable("sampling_n", "10")

    print(context.config_variables)

    first_config = """
class_name: SimpleSqlalchemyDatasource
connection_string: sqlite:///${db_file}

introspection:
    ${data_connector_name}:
        data_asset_name_suffix: ${suffix}
        sampling_method: _sample_using_limit
        sampling_kwargs:
            n: ${sampling_n}
"""

    my_datasource = context.test_yaml_config(first_config)
    assert (
        "test_cases_for_sql_data_connector.db"
        in my_datasource.execution_engine.connection_string
    )
    assert mock_emit.call_count == 1
    # Substitute anonymized names since it changes for each run
    anonymized_datasource_name = mock_emit.call_args_list[0][0][0]["event_payload"][
        "anonymized_name"
    ]
    anonymized_data_connector_name = mock_emit.call_args_list[0][0][0]["event_payload"][
        "anonymized_data_connectors"
    ][0]["anonymized_name"]
    expected_call_args_list = [
        mock.call(
            {
                "event": "data_context.test_yaml_config",
                "event_payload": {
                    "anonymized_name": anonymized_datasource_name,
                    "parent_class": "SimpleSqlalchemyDatasource",
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
    assert mock_emit.call_args_list == expected_call_args_list

    report_object = context.test_yaml_config(first_config, return_mode="report_object")
    print(json.dumps(report_object, indent=2))
    assert report_object["data_connectors"]["count"] == 1
    assert set(report_object["data_connectors"].keys()) == {
        "count",
        "my_very_awesome_data_connector",
    }
    assert mock_emit.call_count == 2
    expected_call_args_list.append(
        mock.call(
            {
                "event": "data_context.test_yaml_config",
                "event_payload": {
                    "anonymized_name": anonymized_datasource_name,
                    "parent_class": "SimpleSqlalchemyDatasource",
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
    )
    assert mock_emit.call_args_list == expected_call_args_list

    # Confirm that logs do not contain any exceptions or invalid messages
    assert not usage_stats_exceptions_exist(messages=caplog.messages)
    assert not usage_stats_invalid_messages_exist(messages=caplog.messages)


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_golden_path_sql_datasource_configuration(
    mock_emit,
    caplog,
    empty_data_context_stats_enabled,
    sa,
    test_connectable_postgresql_db,
):
    """Tests the golden path for setting up a StreamlinedSQLDatasource using test_yaml_config"""
    context: DataContext = empty_data_context_stats_enabled

    with set_directory(context.root_directory):

        # Everything below this line (except for asserts) is what we expect users to run as part of the golden path.
        import great_expectations as ge

        context = ge.get_context()

        db_hostname = os.getenv("GE_TEST_LOCAL_DB_HOSTNAME", "localhost")
        yaml_config = f"""
    class_name: SimpleSqlalchemyDatasource
    credentials:
        drivername: postgresql
        username: postgres
        password: ""
        host: {db_hostname}
        port: 5432
        database: test_ci

    introspection:
        whole_table_with_limits:
            sampling_method: _sample_using_limit
            sampling_kwargs:
                n: 10
    """
        # noinspection PyUnusedLocal
        report_object = context.test_yaml_config(
            name="my_datasource",
            yaml_config=yaml_config,
            return_mode="report_object",
        )
        assert mock_emit.call_count == 2
        # Substitute anonymized names since it changes for each run
        anonymized_datasource_name = mock_emit.call_args_list[1][0][0]["event_payload"][
            "anonymized_name"
        ]
        anonymized_data_connector_name = mock_emit.call_args_list[1][0][0][
            "event_payload"
        ]["anonymized_data_connectors"][0]["anonymized_name"]
        expected_call_args_list = [
            mock.call(
                {"event_payload": {}, "event": "data_context.__init__", "success": True}
            ),
            mock.call(
                {
                    "event": "data_context.test_yaml_config",
                    "event_payload": {
                        "anonymized_name": anonymized_datasource_name,
                        "parent_class": "SimpleSqlalchemyDatasource",
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
        assert mock_emit.call_args_list == expected_call_args_list

        print(json.dumps(report_object, indent=2))
        print(context.datasources)

        my_batch = context.get_batch(
            "my_datasource",
            "whole_table_with_limits",
            "test_df",
        )
        # assert len(my_batch.data.fetchall()) == 10

        with pytest.raises(KeyError):
            my_batch = context.get_batch(
                "my_datasource",
                "whole_table_with_limits",
                "DOES_NOT_EXIST",
            )

        my_validator = context.get_validator(
            datasource_name="my_datasource",
            data_connector_name="whole_table_with_limits",
            data_asset_name="test_df",
            expectation_suite=ExpectationSuite(
                "my_expectation_suite", data_context=context
            ),
        )
        my_evr = my_validator.expect_table_columns_to_match_set(column_set=[])
        print(my_evr)

        # my_evr = my_validator.expect_column_values_to_be_between(
        #     column="x",
        #     min_value=0,
        #     max_value=4,
        # )
        # assert my_evr.success

        # TODO: <Alex>ALEX</Alex>
        # my_evr = my_validator.expect_table_columns_to_match_ordered_list(ordered_list=["a", "b", "c"])
        # assert my_evr.success

    # Confirm that logs do not contain any exceptions or invalid messages
    assert not usage_stats_exceptions_exist(messages=caplog.messages)
    assert not usage_stats_invalid_messages_exist(messages=caplog.messages)


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
@pytest.mark.filterwarnings(
    "ignore:get_batch is deprecated*:DeprecationWarning:great_expectations.data_context.data_context"
)
def test_golden_path_inferred_asset_pandas_datasource_configuration(
    mock_emit, caplog, empty_data_context_stats_enabled, test_df, tmp_path_factory
):
    """
    Tests the golden path for InferredAssetFilesystemDataConnector with PandasExecutionEngine using test_yaml_config
    """
    base_directory = str(
        tmp_path_factory.mktemp("test_golden_path_pandas_datasource_configuration")
    )

    create_files_in_directory(
        directory=base_directory,
        file_name_list=[
            "test_dir_charlie/A/A-1.csv",
            "test_dir_charlie/A/A-2.csv",
            "test_dir_charlie/A/A-3.csv",
            "test_dir_charlie/B/B-1.csv",
            "test_dir_charlie/B/B-2.csv",
            "test_dir_charlie/B/B-3.csv",
            "test_dir_charlie/C/C-1.csv",
            "test_dir_charlie/C/C-2.csv",
            "test_dir_charlie/C/C-3.csv",
            "test_dir_charlie/D/D-1.csv",
            "test_dir_charlie/D/D-2.csv",
            "test_dir_charlie/D/D-3.csv",
        ],
        file_content_fn=lambda: test_df.to_csv(header=True, index=False),
    )

    context: DataContext = empty_data_context_stats_enabled

    with set_directory(context.root_directory):
        import great_expectations as ge

        context = ge.get_context()
        mock_emit.reset_mock()  # Remove data_context.__init__ call

        yaml_config = f"""
    class_name: Datasource

    execution_engine:
        class_name: PandasExecutionEngine

    data_connectors:
        my_filesystem_data_connector:
            class_name: InferredAssetFilesystemDataConnector
            base_directory: {base_directory}/test_dir_charlie
            glob_directive: "*/*.csv"

            default_regex:
                pattern: (.+)/(.+)-(\\d+)\\.csv
                group_names:
                    - subdirectory
                    - data_asset_name
                    - number
    """

        # noinspection PyUnusedLocal
        report_object = context.test_yaml_config(
            name="my_directory_datasource",
            yaml_config=yaml_config,
            return_mode="report_object",
        )
        # print(json.dumps(report_object, indent=2))
        # print(context.datasources)
        assert mock_emit.call_count == 1
        # Substitute anonymized names since it changes for each run
        anonymized_datasource_name = mock_emit.call_args_list[0][0][0]["event_payload"][
            "anonymized_name"
        ]
        anonymized_execution_engine_name = mock_emit.call_args_list[0][0][0][
            "event_payload"
        ]["anonymized_execution_engine"]["anonymized_name"]
        anonymized_data_connector_name = mock_emit.call_args_list[0][0][0][
            "event_payload"
        ]["anonymized_data_connectors"][0]["anonymized_name"]
        expected_call_args_list = [
            mock.call(
                {
                    "event": "data_context.test_yaml_config",
                    "event_payload": {
                        "anonymized_name": anonymized_datasource_name,
                        "parent_class": "Datasource",
                        "anonymized_execution_engine": {
                            "anonymized_name": anonymized_execution_engine_name,
                            "parent_class": "PandasExecutionEngine",
                        },
                        "anonymized_data_connectors": [
                            {
                                "anonymized_name": anonymized_data_connector_name,
                                "parent_class": "InferredAssetFilesystemDataConnector",
                            }
                        ],
                    },
                    "success": True,
                }
            ),
        ]
        assert mock_emit.call_args_list == expected_call_args_list

        my_batch = context.get_batch(
            datasource_name="my_directory_datasource",
            data_connector_name="my_filesystem_data_connector",
            data_asset_name="A",
            batch_identifiers={
                "number": "2",
            },
            batch_spec_passthrough={
                "sampling_method": "_sample_using_hash",
                "sampling_kwargs": {
                    "column_name": "date",
                    "hash_function_name": "md5",
                    "hash_value": "f",
                },
            },
        )
        assert my_batch.batch_definition["data_asset_name"] == "A"

        # "DataContext.get_batch()" calls "DataContext.get_batch_list()" (decorated by "@usage_statistics_enabled_method").
        assert mock_emit.call_count == 2

        df_data = my_batch.data.dataframe
        assert df_data.shape == (10, 10)
        df_data["date"] = df_data.apply(
            lambda row: datetime.datetime.strptime(row["date"], "%Y-%m-%d").date(),
            axis=1,
        )
        assert (
            test_df[
                (test_df["date"] == datetime.date(2020, 1, 15))
                | (test_df["date"] == datetime.date(2020, 1, 29))
            ]
            .drop("timestamp", axis=1)
            .equals(df_data.drop("timestamp", axis=1))
        )

        with pytest.raises(ValueError):
            # noinspection PyUnusedLocal
            my_batch = context.get_batch(
                datasource_name="my_directory_datasource",
                data_connector_name="my_filesystem_data_connector",
                data_asset_name="DOES_NOT_EXIST",
            )

        # "DataContext.get_batch()" calls "DataContext.get_batch_list()" (decorated by "@usage_statistics_enabled_method").
        assert mock_emit.call_count == 3

        my_validator = context.get_validator(
            datasource_name="my_directory_datasource",
            data_connector_name="my_filesystem_data_connector",
            data_asset_name="D",
            data_connector_query={"batch_filter_parameters": {"number": "3"}},
            expectation_suite=ExpectationSuite(
                "my_expectation_suite", data_context=context
            ),
            batch_spec_passthrough={
                "sampling_method": "_sample_using_hash",
                "sampling_kwargs": {
                    "column_name": "date",
                    "hash_function_name": "md5",
                    "hash_value": "f",
                },
            },
        )

        # "DataContext.get_batch()" calls "DataContext.get_batch_list()" (decorated by "@usage_statistics_enabled_method").
        assert mock_emit.call_count == 4

        my_evr = my_validator.expect_column_values_to_be_between(
            column="d", min_value=1, max_value=31
        )
        assert my_evr.success

        # TODO: <Alex>ALEX</Alex>
        # my_evr = my_validator.expect_table_columns_to_match_ordered_list(ordered_list=["x", "y", "z"])
        # assert my_evr.success

        # No other usage stats calls detected
        # assert mock_emit.call_count == 1
        assert mock_emit.call_count == 4

    # Confirm that logs do not contain any exceptions or invalid messages
    assert not usage_stats_exceptions_exist(messages=caplog.messages)
    assert not usage_stats_invalid_messages_exist(messages=caplog.messages)


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
@pytest.mark.filterwarnings(
    "ignore:get_batch is deprecated*:DeprecationWarning:great_expectations.data_context.data_context"
)
def test_golden_path_configured_asset_pandas_datasource_configuration(
    mock_emit, caplog, empty_data_context_stats_enabled, test_df, tmp_path_factory
):
    """
    Tests the golden path for InferredAssetFilesystemDataConnector with PandasExecutionEngine using test_yaml_config
    """
    base_directory = str(
        tmp_path_factory.mktemp("test_golden_path_pandas_datasource_configuration")
    )

    create_files_in_directory(
        directory=base_directory,
        file_name_list=[
            "test_dir_foxtrot/A/A-1.csv",
            "test_dir_foxtrot/A/A-2.csv",
            "test_dir_foxtrot/A/A-3.csv",
            "test_dir_foxtrot/B/B-1.txt",
            "test_dir_foxtrot/B/B-2.txt",
            "test_dir_foxtrot/B/B-3.txt",
            "test_dir_foxtrot/C/C-2017.csv",
            "test_dir_foxtrot/C/C-2018.csv",
            "test_dir_foxtrot/C/C-2019.csv",
            "test_dir_foxtrot/D/D-aaa.csv",
            "test_dir_foxtrot/D/D-bbb.csv",
            "test_dir_foxtrot/D/D-ccc.csv",
            "test_dir_foxtrot/D/D-ddd.csv",
            "test_dir_foxtrot/D/D-eee.csv",
        ],
        file_content_fn=lambda: test_df.to_csv(header=True, index=False),
    )

    context: DataContext = empty_data_context_stats_enabled

    with set_directory(context.root_directory):
        import great_expectations as ge

        context = ge.get_context()
        mock_emit.reset_mock()  # Remove data_context.__init__ call

        yaml_config = f"""
    class_name: Datasource

    execution_engine:
        class_name: PandasExecutionEngine

    data_connectors:
        my_filesystem_data_connector:
            class_name: ConfiguredAssetFilesystemDataConnector
            base_directory: {base_directory}
            # glob_directive: "*"

            default_regex:
                pattern: (.+)\\.csv
                group_names:
                    - alphanumeric

            assets:
                A:
                    base_directory: {base_directory}/test_dir_foxtrot/A
                    pattern: (.+)-(\\d+)\\.csv
                    group_names:
                        - letter
                        - number
                B:
                    base_directory: {base_directory}/test_dir_foxtrot/B
                    pattern: (.+)-(\\d+)\\.csv
                    group_names:
                        - letter
                        - number
                C:
                    base_directory: {base_directory}/test_dir_foxtrot/C
                    pattern: (.+)-(\\d+)\\.csv
                    group_names:
                        - letter
                        - year
                D:
                    base_directory: {base_directory}/test_dir_foxtrot/D
                    pattern: (.+)-(\\d+)\\.csv
                    group_names:
                        - letter
                        - checksum
    """

        # noinspection PyUnusedLocal
        report_object = context.test_yaml_config(
            name="my_directory_datasource",
            yaml_config=yaml_config,
            return_mode="report_object",
        )
        # print(json.dumps(report_object, indent=2))
        # print(context.datasources)
        assert mock_emit.call_count == 1
        # Substitute anonymized names since it changes for each run
        anonymized_datasource_name = mock_emit.call_args_list[0][0][0]["event_payload"][
            "anonymized_name"
        ]
        anonymized_execution_engine_name = mock_emit.call_args_list[0][0][0][
            "event_payload"
        ]["anonymized_execution_engine"]["anonymized_name"]
        anonymized_data_connector_name = mock_emit.call_args_list[0][0][0][
            "event_payload"
        ]["anonymized_data_connectors"][0]["anonymized_name"]
        expected_call_args_list = [
            mock.call(
                {
                    "event": "data_context.test_yaml_config",
                    "event_payload": {
                        "anonymized_name": anonymized_datasource_name,
                        "parent_class": "Datasource",
                        "anonymized_execution_engine": {
                            "anonymized_name": anonymized_execution_engine_name,
                            "parent_class": "PandasExecutionEngine",
                        },
                        "anonymized_data_connectors": [
                            {
                                "anonymized_name": anonymized_data_connector_name,
                                "parent_class": "ConfiguredAssetFilesystemDataConnector",
                            }
                        ],
                    },
                    "success": True,
                }
            ),
        ]
        assert mock_emit.call_args_list == expected_call_args_list

        my_batch = context.get_batch(
            datasource_name="my_directory_datasource",
            data_connector_name="my_filesystem_data_connector",
            data_asset_name="A",
            batch_identifiers={
                "number": "2",
            },
            batch_spec_passthrough={
                "sampling_method": "_sample_using_hash",
                "sampling_kwargs": {
                    "column_name": "date",
                    "hash_function_name": "md5",
                    "hash_value": "f",
                },
            },
        )
        assert my_batch.batch_definition["data_asset_name"] == "A"

        # "DataContext.get_batch()" calls "DataContext.get_batch_list()" (decorated by "@usage_statistics_enabled_method").
        assert mock_emit.call_count == 2

        my_batch.head()

        df_data = my_batch.data.dataframe
        assert df_data.shape == (10, 10)
        df_data["date"] = df_data.apply(
            lambda row: datetime.datetime.strptime(row["date"], "%Y-%m-%d").date(),
            axis=1,
        )
        assert (
            test_df[
                (test_df["date"] == datetime.date(2020, 1, 15))
                | (test_df["date"] == datetime.date(2020, 1, 29))
            ]
            .drop("timestamp", axis=1)
            .equals(df_data.drop("timestamp", axis=1))
        )

        with pytest.raises(ValueError):
            # noinspection PyUnusedLocal
            my_batch = context.get_batch(
                datasource_name="my_directory_datasource",
                data_connector_name="my_filesystem_data_connector",
                data_asset_name="DOES_NOT_EXIST",
            )

        # "DataContext.get_batch()" calls "DataContext.get_batch_list()" (decorated by "@usage_statistics_enabled_method").
        assert mock_emit.call_count == 3

        my_validator = context.get_validator(
            datasource_name="my_directory_datasource",
            data_connector_name="my_filesystem_data_connector",
            data_asset_name="C",
            data_connector_query={"batch_filter_parameters": {"year": "2019"}},
            create_expectation_suite_with_name="my_expectations",
            batch_spec_passthrough={
                "sampling_method": "_sample_using_hash",
                "sampling_kwargs": {
                    "column_name": "date",
                    "hash_function_name": "md5",
                    "hash_value": "f",
                },
            },
        )
        my_evr = my_validator.expect_column_values_to_be_between(
            column="d", min_value=1, max_value=31
        )
        assert my_evr.success

        # "DataContext.get_batch()" calls "DataContext.get_batch_list()" (decorated by "@usage_statistics_enabled_method").
        assert mock_emit.call_count == 4

        # my_evr = my_validator.expect_table_columns_to_match_ordered_list(ordered_list=["x", "y", "z"])
        # assert my_evr.success

        # No other usage stats calls detected
        assert mock_emit.call_count == 4

    # Confirm that logs do not contain any exceptions or invalid messages
    assert not usage_stats_exceptions_exist(messages=caplog.messages)
    assert not usage_stats_invalid_messages_exist(messages=caplog.messages)


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_golden_path_runtime_data_connector_pandas_datasource_configuration(
    mock_emit, caplog, empty_data_context_stats_enabled, test_df, tmp_path_factory
):
    """
    Tests output of test_yaml_config() for a Datacontext configured with a Datasource with
    RuntimeDataConnector. Even though the test directory contains multiple files that can be read-in
    by GE, the RuntimeDataConnector will output 0 data_assets, and return a "note" to the user.

    This is because the RuntimeDataConnector is not aware of data_assets until they are passed in
    through the RuntimeBatchRequest.

    The test asserts that the proper number of data_asset_names are returned and note is returned to the user.
    """
    base_directory = str(
        tmp_path_factory.mktemp("test_golden_path_pandas_datasource_configuration")
    )

    create_files_in_directory(
        directory=base_directory,
        file_name_list=[
            "test_dir_charlie/A/A-1.csv",
            "test_dir_charlie/A/A-2.csv",
            "test_dir_charlie/A/A-3.csv",
        ],
        file_content_fn=lambda: test_df.to_csv(header=True, index=False),
    )

    context: DataContext = empty_data_context_stats_enabled

    with set_directory(context.root_directory):
        import great_expectations as ge

        context = ge.get_context()
        mock_emit.reset_mock()  # Remove data_context.__init__ call

        yaml_config = f"""
           class_name: Datasource

           execution_engine:
               class_name: PandasExecutionEngine

           data_connectors:
               default_runtime_data_connector_name:
                   class_name: RuntimeDataConnector
                   batch_identifiers:
                       - default_identifier_name
           """

        # noinspection PyUnusedLocal
        report_object = context.test_yaml_config(
            name="my_directory_datasource",
            yaml_config=yaml_config,
            return_mode="report_object",
        )

        assert report_object["execution_engine"] == {
            "caching": True,
            "module_name": "great_expectations.execution_engine.pandas_execution_engine",
            "class_name": "PandasExecutionEngine",
            "discard_subset_failing_expectations": False,
            "boto3_options": {},
            "azure_options": {},
            "gcs_options": {},
        }
        assert report_object["data_connectors"]["count"] == 1

        # checking the correct number of data_assets have come back
        assert (
            report_object["data_connectors"]["default_runtime_data_connector_name"][
                "data_asset_count"
            ]
            == 0
        )

        # checking that note has come back
        assert (
            report_object["data_connectors"]["default_runtime_data_connector_name"][
                "note"
            ]
            == "RuntimeDataConnector will not have data_asset_names until they are passed in through RuntimeBatchRequest"
        )

    # Confirm that logs do not contain any exceptions or invalid messages
    assert not usage_stats_exceptions_exist(messages=caplog.messages)
    assert not usage_stats_invalid_messages_exist(messages=caplog.messages)


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_golden_path_runtime_data_connector_and_inferred_data_connector_pandas_datasource_configuration(
    mock_emit, caplog, empty_data_context_stats_enabled, test_df, tmp_path_factory
):
    """
    Tests output of test_yaml_config() for a Datacontext configured with a Datasource with InferredAssetDataConnector
    and RuntimeDataConnector.

    1. The InferredAssetDataConnector will output 4 data_assets, which correspond to the files in the test_dir_charlie folder

    2.  RuntimeDataConnector will output 0 data_assets, and return a "note" to the user. This is because the
        RuntimeDataConnector is not aware of data_assets until they are passed in through the RuntimeBatchRequest.

    The test asserts that the proper number of data_asset_names are returned for both DataConnectors, and in the case of
    the RuntimeDataConnetor, the proper note is returned to the user.
    """
    base_directory = str(
        tmp_path_factory.mktemp("test_golden_path_pandas_datasource_configuration")
    )

    create_files_in_directory(
        directory=base_directory,
        file_name_list=[
            "test_dir_charlie/A/A-1.csv",
            "test_dir_charlie/A/A-2.csv",
            "test_dir_charlie/A/A-3.csv",
            "test_dir_charlie/B/B-1.csv",
            "test_dir_charlie/B/B-2.csv",
            "test_dir_charlie/B/B-3.csv",
            "test_dir_charlie/C/C-1.csv",
            "test_dir_charlie/C/C-2.csv",
            "test_dir_charlie/C/C-3.csv",
            "test_dir_charlie/D/D-1.csv",
            "test_dir_charlie/D/D-2.csv",
            "test_dir_charlie/D/D-3.csv",
        ],
        file_content_fn=lambda: test_df.to_csv(header=True, index=False),
    )

    context: DataContext = empty_data_context_stats_enabled

    with set_directory(context.root_directory):
        import great_expectations as ge

        context = ge.get_context()
        mock_emit.reset_mock()  # Remove data_context.__init__ call

        yaml_config = f"""
        class_name: Datasource

        execution_engine:
            class_name: PandasExecutionEngine

        data_connectors:
            default_runtime_data_connector_name:
                class_name: RuntimeDataConnector
                batch_identifiers:
                    - default_identifier_name
            default_inferred_data_connector_name:
                class_name: InferredAssetFilesystemDataConnector
                base_directory: {base_directory}/test_dir_charlie
                glob_directive: "*/*.csv"

                default_regex:
                    pattern: (.+)/(.+)-(\\d+)\\.csv
                    group_names:
                        - subdirectory
                        - data_asset_name
                        - number
        """

        # noinspection PyUnusedLocal
        report_object = context.test_yaml_config(
            name="my_directory_datasource",
            yaml_config=yaml_config,
            return_mode="report_object",
        )

        assert report_object["execution_engine"] == {
            "caching": True,
            "module_name": "great_expectations.execution_engine.pandas_execution_engine",
            "class_name": "PandasExecutionEngine",
            "discard_subset_failing_expectations": False,
            "boto3_options": {},
            "azure_options": {},
            "gcs_options": {},
        }
        assert report_object["data_connectors"]["count"] == 2
        assert report_object["data_connectors"][
            "default_runtime_data_connector_name"
        ] == {
            "class_name": "RuntimeDataConnector",
            "data_asset_count": 0,
            "data_assets": {},
            "example_data_asset_names": [],
            "example_unmatched_data_references": [],
            "note": "RuntimeDataConnector will not have data_asset_names until they are "
            "passed in through RuntimeBatchRequest",
            "unmatched_data_reference_count": 0,
        }
        assert report_object["data_connectors"][
            "default_inferred_data_connector_name"
        ] == {
            "class_name": "InferredAssetFilesystemDataConnector",
            "data_asset_count": 4,
            "example_data_asset_names": ["A", "B", "C"],
            "data_assets": {
                "A": {
                    "batch_definition_count": 3,
                    "example_data_references": ["A/A-1.csv", "A/A-2.csv", "A/A-3.csv"],
                },
                "B": {
                    "batch_definition_count": 3,
                    "example_data_references": ["B/B-1.csv", "B/B-2.csv", "B/B-3.csv"],
                },
                "C": {
                    "batch_definition_count": 3,
                    "example_data_references": ["C/C-1.csv", "C/C-2.csv", "C/C-3.csv"],
                },
            },
            "unmatched_data_reference_count": 0,
            "example_unmatched_data_references": [],
        }

    # Confirm that logs do not contain any exceptions or invalid messages
    assert not usage_stats_exceptions_exist(messages=caplog.messages)
    assert not usage_stats_invalid_messages_exist(messages=caplog.messages)


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_rule_based_profiler_integration(
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
    instantiated_class = context.test_yaml_config(
        yaml_config=yaml_config, name="my_profiler", class_name="Profiler"
    )

    # Ensure valid return type and content
    assert isinstance(instantiated_class, RuleBasedProfiler)
    assert instantiated_class.name == "my_profiler"

    # Confirm that logs do not contain any exceptions or invalid messages
    assert not usage_stats_exceptions_exist(messages=caplog.messages)
    assert not usage_stats_invalid_messages_exist(messages=caplog.messages)


def test_test_yaml_config_supported_types_have_self_check():
    # Each major category of test_yaml_config supported types has its own origin module_name
    supported_types = [
        (
            BaseDataContext.TEST_YAML_CONFIG_SUPPORTED_STORE_TYPES,
            "great_expectations.data_context.store",
        ),
        (
            BaseDataContext.TEST_YAML_CONFIG_SUPPORTED_DATASOURCE_TYPES,
            "great_expectations.datasource",
        ),
        (
            BaseDataContext.TEST_YAML_CONFIG_SUPPORTED_DATA_CONNECTOR_TYPES,
            "great_expectations.datasource.data_connector",
        ),
        (
            BaseDataContext.TEST_YAML_CONFIG_SUPPORTED_CHECKPOINT_TYPES,
            "great_expectations.checkpoint",
        ),
        (
            BaseDataContext.TEST_YAML_CONFIG_SUPPORTED_PROFILER_TYPES,
            "great_expectations.rule_based_profiler",
        ),
    ]

    # Quick sanity check to ensure that we are testing ALL supported types herein
    all_types = list(itertools.chain.from_iterable(t[0] for t in supported_types))
    assert sorted(all_types) == sorted(
        BaseDataContext.ALL_TEST_YAML_CONFIG_SUPPORTED_TYPES
    )

    # Use class_name and module_name to get the class type and introspect to confirm adherence to self_check requirement
    for category, module_name in supported_types:
        for class_name in category:
            class_ = load_class(class_name=class_name, module_name=module_name)
            assert hasattr(class_, "self_check") and callable(
                class_.self_check
            ), f"Class '{class_}' is missing the required `self_check()` method"
