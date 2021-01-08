import datetime

import pytest

from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import DataContextConfig
from great_expectations.validator.validator import Validator
from tests.test_utils import create_files_in_directory


def test_get_config(empty_data_context):
    context = empty_data_context

    # We can call get_config in several different modes
    assert type(context.get_config()) == DataContextConfig
    assert type(context.get_config(mode="typed")) == DataContextConfig
    assert type(context.get_config(mode="dict")) == dict
    assert type(context.get_config(mode="yaml")) == str
    with pytest.raises(ValueError):
        context.get_config(mode="foobar")

    print(context.get_config(mode="yaml"))
    print(context.get_config("dict").keys())

    assert set(context.get_config("dict").keys()) == {
        "config_version",
        "datasources",
        "config_variables_file_path",
        "plugins_directory",
        "validation_operators",
        "stores",
        "expectations_store_name",
        "validations_store_name",
        "evaluation_parameter_store_name",
        "data_docs_sites",
        "anonymous_usage_statistics",
        "notebooks",
    }


def test_config_variables(empty_data_context):
    context = empty_data_context
    assert type(context.config_variables) == dict
    assert set(context.config_variables.keys()) == {"instance_id"}


def test_get_batch_of_pipeline_batch_data(empty_data_context, test_df):
    context = empty_data_context

    yaml_config = f"""
        class_name: Datasource

        execution_engine:
            class_name: PandasExecutionEngine

        data_connectors:
          my_runtime_data_connector:
            module_name: great_expectations.datasource.data_connector
            class_name: RuntimeDataConnector
            runtime_keys:
            - airflow_run_id
    """
    # noinspection PyUnusedLocal
    report_object = context.test_yaml_config(
        name="my_pipeline_datasource",
        yaml_config=yaml_config,
        return_mode="report_object",
    )
    # print(json.dumps(report_object, indent=2))
    # print(context.datasources)

    my_batch = context.get_batch(
        datasource_name="my_pipeline_datasource",
        data_connector_name="my_runtime_data_connector",
        data_asset_name="IN_MEMORY_DATA_ASSET",
        batch_data=test_df,
        partition_request={
            "partition_identifiers": {
                "airflow_run_id": 1234567890,
            }
        },
        limit=None,
    )
    assert my_batch.batch_definition["data_asset_name"] == "IN_MEMORY_DATA_ASSET"

    assert my_batch.data.equals(test_df)


def test_conveying_splitting_and_sampling_directives_from_data_context_to_pandas_execution_engine(
    empty_data_context, test_df, tmp_path_factory
):
    base_directory = str(
        tmp_path_factory.mktemp(
            "test_conveying_splitting_and_sampling_directives_from_data_context_to_pandas_execution_engine"
        )
    )

    create_files_in_directory(
        directory=base_directory,
        file_name_list=[
            "somme_file.csv",
        ],
        file_content_fn=lambda: test_df.to_csv(header=True, index=False),
    )

    context = empty_data_context

    yaml_config = f"""
class_name: Datasource

execution_engine:
    class_name: PandasExecutionEngine

data_connectors:
    my_filesystem_data_connector:
        class_name: ConfiguredAssetFilesystemDataConnector
        base_directory: {base_directory}

        default_regex:
            pattern: (.+)\\.csv
            group_names:
                - alphanumeric

        assets:
            A:
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

    df_data = my_batch.data
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

    my_batch = context.get_batch(
        datasource_name="my_directory_datasource",
        data_connector_name="my_filesystem_data_connector",
        data_asset_name="A",
        batch_spec_passthrough={
            "splitter_method": "_split_on_multi_column_values",
            "splitter_kwargs": {
                "column_names": ["y", "m", "d"],
                "partition_definition": {"y": 2020, "m": 1, "d": 5},
            },
        },
    )
    df_data = my_batch.data
    assert df_data.shape == (4, 10)
    df_data["date"] = df_data.apply(
        lambda row: datetime.datetime.strptime(row["date"], "%Y-%m-%d").date(), axis=1
    )
    df_data["belongs_in_split"] = df_data.apply(
        lambda row: row["date"] == datetime.date(2020, 1, 5), axis=1
    )
    df_data = df_data[df_data["belongs_in_split"]]
    assert df_data.drop("belongs_in_split", axis=1).shape == (4, 10)


def test_relative_data_connector_default_and_relative_asset_base_directory_paths(
    empty_data_context, test_df, tmp_path_factory
):
    context = empty_data_context

    create_files_in_directory(
        directory=context.root_directory,
        file_name_list=[
            "test_dir_0/A/B/C/logfile_0.csv",
            "test_dir_0/A/B/C/bigfile_1.csv",
            "test_dir_0/A/filename2.csv",
            "test_dir_0/A/filename3.csv",
        ],
        file_content_fn=lambda: test_df.to_csv(header=True, index=False),
    )

    yaml_config = f"""
class_name: Datasource

execution_engine:
    class_name: PandasExecutionEngine

data_connectors:
    my_filesystem_data_connector:
        class_name: ConfiguredAssetFilesystemDataConnector
        base_directory: test_dir_0/A
        glob_directive: "*"
        default_regex:
            pattern: (.+)\\.csv
            group_names:
            - name

        assets:
            A:
                base_directory: B/C
                glob_directive: "log*.csv"
                pattern: (.+)_(\\d+)\\.csv
                group_names:
                - name
                - number
"""
    my_datasource = context.test_yaml_config(
        name="my_directory_datasource",
        yaml_config=yaml_config,
    )
    assert (
        my_datasource.data_connectors["my_filesystem_data_connector"].base_directory
        == f"{context.root_directory}/test_dir_0/A"
    )
    assert (
        my_datasource.data_connectors[
            "my_filesystem_data_connector"
        ]._get_full_file_path_for_asset(
            path="bigfile_1.csv",
            asset=my_datasource.data_connectors["my_filesystem_data_connector"].assets[
                "A"
            ],
        )
        == f"{context.root_directory}/test_dir_0/A/B/C/bigfile_1.csv"
    )

    my_batch = context.get_batch(
        datasource_name="my_directory_datasource",
        data_connector_name="my_filesystem_data_connector",
        data_asset_name="A",
    )

    df_data = my_batch.data
    assert df_data.shape == (120, 10)


def test__get_data_context_version(empty_data_context, titanic_data_context):
    context = empty_data_context

    assert not context._get_data_context_version("some_datasource_name", **{})
    assert not context._get_data_context_version(arg1="some_datasource_name", **{})

    yaml_config = f"""
class_name: Datasource

execution_engine:
    class_name: PandasExecutionEngine

data_connectors:
    general_runtime_data_connector:
      module_name: great_expectations.datasource.data_connector
      class_name: RuntimeDataConnector
      runtime_keys:
      - airflow_run_id
"""
    # noinspection PyUnusedLocal
    my_datasource = context.test_yaml_config(
        name="some_datasource_name",
        yaml_config=yaml_config,
    )

    assert context._get_data_context_version("some_datasource_name", **{}) == "v3"
    assert context._get_data_context_version(arg1="some_datasource_name", **{}) == "v3"

    context = titanic_data_context
    root_dir = context.root_directory
    batch_kwargs = {
        "datasource": "mydatasource",
        "path": f"{root_dir}/../data/Titanic.csv",
    }
    assert context._get_data_context_version(arg1=batch_kwargs) == "v2"
    assert context._get_data_context_version(batch_kwargs) == "v2"
    assert (
        context._get_data_context_version(
            "some_value", **{"batch_kwargs": batch_kwargs}
        )
        == "v2"
    )


def test_in_memory_data_context_configuration(
    titanic_pandas_multibatch_data_context_v3,
):
    project_config_dict: dict = titanic_pandas_multibatch_data_context_v3.get_config(
        mode="dict"
    )
    project_config_dict["plugins_directory"] = None
    project_config_dict["validation_operators"] = {
        "action_list_operator": {
            "class_name": "ActionListValidationOperator",
            "action_list": [
                {
                    "name": "store_validation_result",
                    "action": {"class_name": "StoreValidationResultAction"},
                },
                {
                    "name": "store_evaluation_params",
                    "action": {"class_name": "StoreEvaluationParametersAction"},
                },
                {
                    "name": "update_data_docs",
                    "action": {"class_name": "UpdateDataDocsAction"},
                },
            ],
        }
    }
    project_config: DataContextConfig = DataContextConfig(**project_config_dict)
    data_context = BaseDataContext(
        project_config=project_config,
        context_root_dir=titanic_pandas_multibatch_data_context_v3.root_directory,
    )

    my_validator: Validator = data_context.get_validator(
        datasource_name="titanic_multi_batch",
        data_connector_name="my_data_connector",
        data_asset_name="Titanic_1912",
        create_expectation_suite_with_name="my_test_titanic_expectation_suite",
    )

    assert my_validator.expect_table_row_count_to_equal(1313)["success"]
    assert my_validator.expect_table_column_count_to_equal(7)["success"]
