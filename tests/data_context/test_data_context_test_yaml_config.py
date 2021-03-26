import datetime
import json
import os
import tempfile

import pytest

import great_expectations.exceptions as ge_exceptions
from great_expectations.core import ExpectationSuite
from great_expectations.data_context.store import CheckpointStore
from great_expectations.data_context.util import file_relative_path
from tests.test_utils import create_files_in_directory


def test_empty_store(empty_data_context):
    # noinspection PyUnusedLocal
    my_expectation_store = empty_data_context.test_yaml_config(
        yaml_config="""
module_name: great_expectations.data_context.store.expectations_store
class_name: ExpectationsStore
store_backend:

    module_name: "great_expectations.data_context.store.store_backend"
    class_name: InMemoryStoreBackend
"""
    )

    # assert False


def test_config_with_yaml_error(empty_data_context):
    with pytest.raises(Exception):
        # noinspection PyUnusedLocal
        my_expectation_store = empty_data_context.test_yaml_config(
            yaml_config="""
module_name: great_expectations.data_context.store.expectations_store
class_name: ExpectationsStore
store_backend:
    module_name: "great_expectations.data_context.store.store_backend"
    class_name: InMemoryStoreBackend
EGREGIOUS FORMATTING ERROR
"""
        )


def test_expectations_store_with_filesystem_store_backend(empty_data_context):
    tmp_dir = str(tempfile.mkdtemp())
    with open(os.path.join(tmp_dir, "expectations_A1.json"), "w") as f_:
        f_.write("\n")
    with open(os.path.join(tmp_dir, "expectations_A2.json"), "w") as f_:
        f_.write("\n")

    # noinspection PyUnusedLocal
    my_expectation_store = empty_data_context.test_yaml_config(
        yaml_config=f"""
module_name: great_expectations.data_context.store
class_name: ExpectationsStore
store_backend:
    module_name: "great_expectations.data_context.store"
    class_name: TupleFilesystemStoreBackend
    base_directory: {tmp_dir}
"""
    )


def test_checkpoint_store_with_filesystem_store_backend(
    empty_data_context, tmp_path_factory
):
    tmp_dir: str = str(
        tmp_path_factory.mktemp("test_checkpoint_store_with_filesystem_store_backend")
    )

    yaml_config: str = f"""
    store_name: my_checkpoint_store
    class_name: CheckpointStore
    module_name: great_expectations.data_context.store
    store_backend:
        class_name: TupleFilesystemStoreBackend
        module_name: "great_expectations.data_context.store"
        base_directory: {tmp_dir}/checkpoints
    """

    my_checkpoint_store: CheckpointStore = empty_data_context.test_yaml_config(
        yaml_config=yaml_config,
        return_mode="instantiated_class",
    )

    report_object: dict = empty_data_context.test_yaml_config(
        yaml_config=yaml_config,
        return_mode="report_object",
    )

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
            "filepath_template": "{0}.yml",
        },
        "overwrite_existing": False,
        "runtime_environment": {
            "root_directory": f"{empty_data_context.root_directory}",
        },
    }
    assert my_checkpoint_store.config == expected_checkpoint_store_config

    checkpoint_store_name: str = my_checkpoint_store.config["store_name"]
    empty_data_context.get_config()["checkpoint_store_name"] = checkpoint_store_name

    assert (
        empty_data_context.get_config_with_variables_substituted().checkpoint_store_name
        == "my_checkpoint_store"
    )
    assert (
        empty_data_context.get_config_with_variables_substituted().checkpoint_store_name
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
        empty_data_context.get_config_with_variables_substituted().stores[
            empty_data_context.get_config_with_variables_substituted().checkpoint_store_name
        ]
        == expected_checkpoint_store_config
    )


def test_empty_store2(empty_data_context):
    empty_data_context.test_yaml_config(
        yaml_config="""
class_name: ValidationsStore
store_backend:

    module_name: "great_expectations.data_context.store.store_backend"
    class_name: InMemoryStoreBackend
"""
    )


def test_datasource_config(empty_data_context):
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

    return_obj = empty_data_context.test_yaml_config(
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


def test_error_states(empty_data_context):
    first_config: str = """
class_name: Datasource

execution_engine:
    class_name: NOT_A_REAL_CLASS_NAME
"""

    with pytest.raises(ge_exceptions.DatasourceInitializationError) as excinfo:
        empty_data_context.test_yaml_config(yaml_config=first_config)
        # print(excinfo.value.message)
        # shortened_message_len = len(excinfo.value.message)
        # print("="*80)

    # Set shorten_tracebacks=True and verify that no error is thrown, even though the config is the same as before.
    # Note: a more thorough test could also verify that the traceback is indeed short.
    empty_data_context.test_yaml_config(
        yaml_config=first_config,
        shorten_tracebacks=True,
    )

    # For good measure, do it again, with a different config and a different type of error
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

    datasource = empty_data_context.test_yaml_config(yaml_config=second_config)
    assert (
        "NOT_A_REAL_KEY"
        not in datasource.config["data_connectors"]["my_filesystem_data_connector"]
    )


def test_config_variables_in_test_yaml_config(empty_data_context, sa):
    context = empty_data_context

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

    report_object = context.test_yaml_config(first_config, return_mode="report_object")
    print(json.dumps(report_object, indent=2))
    assert report_object["data_connectors"]["count"] == 1
    assert set(report_object["data_connectors"].keys()) == {
        "count",
        "my_very_awesome_data_connector",
    }


def test_golden_path_sql_datasource_configuration(
    sa, empty_data_context, test_connectable_postgresql_db
):
    """Tests the golden path for setting up a StreamlinedSQLDatasource using test_yaml_config"""
    context = empty_data_context

    os.chdir(context.root_directory)

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
        expectation_suite=ExpectationSuite("my_expectation_suite"),
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


def test_golden_path_inferred_asset_pandas_datasource_configuration(
    empty_data_context, test_df, tmp_path_factory
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

    context = empty_data_context

    os.chdir(context.root_directory)
    import great_expectations as ge

    context = ge.get_context()

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

    df_data = my_batch.data.dataframe
    assert df_data.shape == (10, 10)
    df_data["date"] = df_data.apply(
        lambda row: datetime.datetime.strptime(row["date"], "%Y-%m-%d").date(), axis=1
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

    my_validator = context.get_validator(
        datasource_name="my_directory_datasource",
        data_connector_name="my_filesystem_data_connector",
        data_asset_name="D",
        data_connector_query={"batch_filter_parameters": {"number": "3"}},
        expectation_suite=ExpectationSuite("my_expectation_suite"),
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

    # TODO: <Alex>ALEX</Alex>
    # my_evr = my_validator.expect_table_columns_to_match_ordered_list(ordered_list=["x", "y", "z"])
    # assert my_evr.success


def test_golden_path_configured_asset_pandas_datasource_configuration(
    empty_data_context, test_df, tmp_path_factory
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

    context = empty_data_context

    os.chdir(context.root_directory)
    import great_expectations as ge

    context = ge.get_context()

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

    my_batch.head()

    df_data = my_batch.data.dataframe
    assert df_data.shape == (10, 10)
    df_data["date"] = df_data.apply(
        lambda row: datetime.datetime.strptime(row["date"], "%Y-%m-%d").date(), axis=1
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

    # my_evr = my_validator.expect_table_columns_to_match_ordered_list(ordered_list=["x", "y", "z"])
    # assert my_evr.success
