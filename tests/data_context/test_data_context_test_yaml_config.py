import json
import os
import tempfile

import pytest

from tests.test_utils import create_files_in_directory
from great_expectations.exceptions import (
    PluginClassNotFoundError,
)
from great_expectations.data_context.util import (
    file_relative_path
)

def test_empty_store(empty_data_context_v3):
    my_expectation_store = empty_data_context_v3.test_yaml_config(
        yaml_config="""
module_name: great_expectations.data_context.store.expectations_store
class_name: ExpectationsStore
store_backend:

    module_name: "great_expectations.data_context.store.store_backend"
    class_name: InMemoryStoreBackend
"""
    )

    # assert False


def test_config_with_yaml_error(empty_data_context_v3):

    with pytest.raises(Exception):
        my_expectation_store = empty_data_context_v3.test_yaml_config(
            yaml_config="""
module_name: great_expectations.data_context.store.expectations_store
class_name: ExpectationsStore
store_backend:
    module_name: "great_expectations.data_context.store.store_backend"
    class_name: InMemoryStoreBackend
EGREGIOUS FORMATTING ERROR
"""
        )


def test_filesystem_store(empty_data_context_v3):
    tmp_dir = str(tempfile.mkdtemp())
    with open(os.path.join(tmp_dir, "expectations_A1.json"), "w") as f_:
        f_.write("\n")
    with open(os.path.join(tmp_dir, "expectations_A2.json"), "w") as f_:
        f_.write("\n")

    my_expectation_store = empty_data_context_v3.test_yaml_config(
        yaml_config=f"""
module_name: great_expectations.data_context.store.expectations_store
class_name: ExpectationsStore
store_backend:

    module_name: "great_expectations.data_context.store"
    class_name: TupleFilesystemStoreBackend
    base_directory: {tmp_dir}
"""
    )


def test_empty_store2(empty_data_context_v3):
    empty_data_context_v3.test_yaml_config(
        yaml_config="""
class_name: ValidationsStore
store_backend:

    module_name: "great_expectations.data_context.store.store_backend"
    class_name: InMemoryStoreBackend
"""
    )


def test_execution_environment_config(empty_data_context_v3):
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

    return_obj = empty_data_context_v3.test_yaml_config(
        yaml_config=f"""
class_name: ExecutionEnvironment

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
""", return_mode="report_object"
    )

    print(json.dumps(return_obj, indent=2))

    assert return_obj == {
        "execution_engine": {
            "class_name": "PandasExecutionEngine"
        },
        "data_connectors": {
            "count": 1,
            "my_filesystem_data_connector": {
                "class_name": "InferredAssetFilesystemDataConnector",
                "data_asset_count": 1,
                "example_data_asset_names": [
                    "DEFAULT_ASSET_NAME"
                ],
                "data_assets": {
                    "DEFAULT_ASSET_NAME": {
                        "batch_definition_count": 10,
                        "example_data_references": [
                            "abe_20200809_1040.csv",
                            "alex_20200809_1000.csv",
                            "alex_20200819_1300.csv"
                        ]
                    }
                },
                "unmatched_data_reference_count": 0,
                "example_unmatched_data_references": []
            }
        }
    }

def test_error_states(empty_data_context_v3):

    first_config = """
class_name: ExecutionEnvironment

execution_engine:
    class_name: NOT_A_REAL_CLASS_NAME
"""

    with pytest.raises(PluginClassNotFoundError) as excinfo:
        empty_data_context_v3.test_yaml_config(
            yaml_config=first_config
        )
    # print(excinfo.value.message)
    # shortened_message_len = len(excinfo.value.message)
    # print("="*80)

    # Set shorten_tracebacks=True and verify that no error is thrown, even though the config is the same as before.
    # Note: a more thorough test could also verify that the traceback is indeed short.
    empty_data_context_v3.test_yaml_config(
        yaml_config=first_config,
        shorten_tracebacks=True,
    )

    # For good measure, do it again, with a different config and a different type of error
    temp_dir = str(tempfile.mkdtemp())
    second_config = f"""
class_name: ExecutionEnvironment

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

    with pytest.raises(TypeError) as excinfo:
        empty_data_context_v3.test_yaml_config(
            yaml_config=second_config,
        )

    empty_data_context_v3.test_yaml_config(
        yaml_config=second_config,
        shorten_tracebacks=True
    )


def test_config_variables_in_test_yaml_config(empty_data_context_v3, sa):
    context = empty_data_context_v3

    db_file = file_relative_path(
        __file__, os.path.join("..", "test_sets", "test_cases_for_sql_data_connector.db"),
    )

    context.save_config_variable("db_file", db_file)
    context.save_config_variable("data_connector_name", "my_very_awesome_data_connector")
    context.save_config_variable("suffix", "__whole_table")
    context.save_config_variable("sampling_n", "10")

    print(context.config_variables)

    first_config = """
class_name: StreamlinedSqlExecutionEnvironment
connection_string: sqlite:///${db_file}

introspection:
    ${data_connector_name}:
        data_asset_name_suffix: ${suffix}
        sampling_method: _sample_using_limit
        sampling_kwargs:
            n: ${sampling_n}
"""

    my_execution_environment = context.test_yaml_config(first_config)
    assert "test_cases_for_sql_data_connector.db" in my_execution_environment.execution_engine.connection_string

    report_object = context.test_yaml_config(first_config, return_mode="report_object")
    print(json.dumps(report_object, indent=2))
    assert report_object["data_connectors"]["count"] == 1
    assert set(report_object["data_connectors"].keys()) == {"count", "my_very_awesome_data_connector"}

def test_golden_path_sql_execution_environment_configuration(sa, empty_data_context_v3, test_connectable_postgresql_db):
    context = empty_data_context_v3

    os.chdir(context.root_directory)
    import great_expectations as ge
    context = ge.get_context()

    yaml_config = """
class_name: StreamlinedSqlExecutionEnvironment
credentials:
    drivername: postgresql
    username: postgres
    password: ""
    host: localhost
    port: 5432
    database: test_ci

introspection:
    whole_table_with_limits:
        sampling_method: _sample_using_limit
        sampling_kwargs:
            n: 100
"""
    report_object = context.test_yaml_config(
        name="my_datasource",
        yaml_config=yaml_config,
        return_mode="report_object",
    )
    print(json.dumps(report_object, indent=2))
    print(context.datasources)

    my_batch = context.get_batch_from_new_style_datasource(
        "my_datasource",
        "whole_table_with_limits",
        "test_df",
    )