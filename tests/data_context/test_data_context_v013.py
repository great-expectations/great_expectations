import datetime
import os
import re

import pandas as pd
import pytest
from ruamel.yaml import YAML

from great_expectations import DataContext
from great_expectations.core import ExpectationSuite
from great_expectations.core.batch import Batch, RuntimeBatchRequest
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import DataContextConfig
from great_expectations.exceptions import BatchSpecError
from great_expectations.execution_engine.pandas_batch_data import PandasBatchData
from great_expectations.execution_engine.sqlalchemy_batch_data import (
    SqlAlchemyBatchData,
)
from great_expectations.validator.validator import Validator
from tests.integration.usage_statistics.test_integration_usage_statistics import (
    USAGE_STATISTICS_QA_URL,
)
from tests.test_utils import create_files_in_directory

yaml = YAML()


@pytest.fixture
def basic_data_context_v013_config():
    return DataContextConfig(
        **{
            "commented_map": {},
            "config_version": 3,
            "plugins_directory": "plugins/",
            "evaluation_parameter_store_name": "evaluation_parameter_store",
            "validations_store_name": "does_not_have_to_be_real",
            "expectations_store_name": "expectations_store",
            "checkpoint_store_name": "checkpoint_store",
            "config_variables_file_path": "uncommitted/config_variables.yml",
            "datasources": {},
            "stores": {
                "expectations_store": {
                    "class_name": "ExpectationsStore",
                    "store_backend": {
                        "class_name": "TupleFilesystemStoreBackend",
                        "base_directory": "expectations/",
                    },
                },
                "evaluation_parameter_store": {
                    "module_name": "great_expectations.data_context.store",
                    "class_name": "EvaluationParameterStore",
                },
                "checkpoint_store": {
                    "class_name": "CheckpointStore",
                    "store_backend": {
                        "class_name": "TupleFilesystemStoreBackend",
                        "base_directory": "checkpoints/",
                    },
                },
            },
            "data_docs_sites": {},
            "anonymous_usage_statistics": {
                "enabled": True,
                "data_context_id": "6a52bdfa-e182-455b-a825-e69f076e67d6",
                "usage_statistics_url": USAGE_STATISTICS_QA_URL,
            },
        }
    )


def test_ConfigOnlyDataContext_v013__initialization(
    tmp_path_factory, basic_data_context_v013_config
):
    config_path = str(
        tmp_path_factory.mktemp("test_ConfigOnlyDataContext__initialization__dir")
    )
    context = BaseDataContext(
        basic_data_context_v013_config,
        config_path,
    )

    assert len(context.plugins_directory.split("/")[-3:]) == 3
    assert "" in context.plugins_directory.split("/")[-3:]

    pattern = re.compile(r"test_ConfigOnlyDataContext__initialization__dir\d*")
    assert (
        len(
            list(
                filter(
                    lambda element: element,
                    sorted(
                        [
                            pattern.match(element) is not None
                            for element in context.plugins_directory.split("/")[-3:]
                        ]
                    ),
                )
            )
        )
        == 1
    )


def test__normalize_absolute_or_relative_path(
    tmp_path_factory, basic_data_context_v013_config
):
    config_path = str(
        tmp_path_factory.mktemp("test__normalize_absolute_or_relative_path__dir")
    )
    context = BaseDataContext(
        basic_data_context_v013_config,
        config_path,
    )

    pattern_string = os.path.join(
        "^.*test__normalize_absolute_or_relative_path__dir\\d*", "yikes$"
    )
    pattern = re.compile(pattern_string)
    assert (
        pattern.match(context._normalize_absolute_or_relative_path("yikes")) is not None
    )

    assert (
        "test__normalize_absolute_or_relative_path__dir"
        not in context._normalize_absolute_or_relative_path("/yikes")
    )
    assert "/yikes" == context._normalize_absolute_or_relative_path("/yikes")


def test_load_config_variables_file(
    basic_data_context_v013_config, tmp_path_factory, monkeypatch
):
    # Setup:
    base_path = str(tmp_path_factory.mktemp("test_load_config_variables_file"))
    os.makedirs(os.path.join(base_path, "uncommitted"), exist_ok=True)
    with open(
        os.path.join(base_path, "uncommitted", "dev_variables.yml"), "w"
    ) as outfile:
        yaml.dump({"env": "dev"}, outfile)
    with open(
        os.path.join(base_path, "uncommitted", "prod_variables.yml"), "w"
    ) as outfile:
        yaml.dump({"env": "prod"}, outfile)
    basic_data_context_v013_config[
        "config_variables_file_path"
    ] = "uncommitted/${TEST_CONFIG_FILE_ENV}_variables.yml"

    try:
        # We should be able to load different files based on an environment variable
        monkeypatch.setenv("TEST_CONFIG_FILE_ENV", "dev")
        context = BaseDataContext(
            basic_data_context_v013_config, context_root_dir=base_path
        )
        config_vars = context._load_config_variables_file()
        assert config_vars["env"] == "dev"
        monkeypatch.setenv("TEST_CONFIG_FILE_ENV", "prod")
        context = BaseDataContext(
            basic_data_context_v013_config, context_root_dir=base_path
        )
        config_vars = context._load_config_variables_file()
        assert config_vars["env"] == "prod"
    except Exception:
        raise
    finally:
        # Make sure we unset the environment variable we're using
        monkeypatch.delenv("TEST_CONFIG_FILE_ENV")


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
        "stores",
        "expectations_store_name",
        "validations_store_name",
        "evaluation_parameter_store_name",
        "checkpoint_store_name",
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
            batch_identifiers:
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
        batch_identifiers={
            "airflow_run_id": 1234567890,
        },
        limit=None,
    )
    assert my_batch.batch_definition["data_asset_name"] == "IN_MEMORY_DATA_ASSET"

    assert my_batch.data.dataframe.equals(test_df)


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

    my_batch = context.get_batch(
        datasource_name="my_directory_datasource",
        data_connector_name="my_filesystem_data_connector",
        data_asset_name="A",
        batch_spec_passthrough={
            "splitter_method": "_split_on_multi_column_values",
            "splitter_kwargs": {
                "column_names": ["y", "m", "d"],
                "batch_identifiers": {"y": 2020, "m": 1, "d": 5},
            },
        },
    )
    df_data = my_batch.data.dataframe
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

    df_data = my_batch.data.dataframe
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
      batch_identifiers:
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
    titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled,
):
    project_config_dict: dict = titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled.get_config(
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
        context_root_dir=titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled.root_directory,
    )

    my_validator: Validator = data_context.get_validator(
        datasource_name="my_datasource",
        data_connector_name="my_basic_data_connector",
        data_asset_name="Titanic_1912",
        create_expectation_suite_with_name="my_test_titanic_expectation_suite",
    )

    assert my_validator.expect_table_row_count_to_equal(1313)["success"]
    assert my_validator.expect_table_column_count_to_equal(7)["success"]


def test_get_batch_with_query_in_runtime_parameters_using_runtime_data_connector(
    sa,
    data_context_with_runtime_sql_datasource_for_testing_get_batch,
):
    context: DataContext = (
        data_context_with_runtime_sql_datasource_for_testing_get_batch
    )

    batch: Batch

    batch = context.get_batch(
        batch_request=RuntimeBatchRequest(
            datasource_name="my_runtime_sql_datasource",
            data_connector_name="my_runtime_data_connector",
            data_asset_name="IN_MEMORY_DATA_ASSET",
            runtime_parameters={
                "query": "SELECT * FROM table_partitioned_by_date_column__A"
            },
            batch_identifiers={
                "pipeline_stage_name": "core_processing",
                "airflow_run_id": 1234567890,
            },
        ),
    )

    assert batch.batch_spec is not None
    assert batch.batch_definition["data_asset_name"] == "IN_MEMORY_DATA_ASSET"
    assert isinstance(batch.data, SqlAlchemyBatchData)

    selectable_table_name = batch.data.selectable.name
    selectable_count_sql_str = f"select count(*) from {selectable_table_name}"
    sa_engine = batch.data.execution_engine.engine

    assert sa_engine.execute(selectable_count_sql_str).scalar() == 120
    assert batch.batch_markers.get("ge_load_time") is not None


def test_get_validator_with_query_in_runtime_parameters_using_runtime_data_connector(
    sa,
    data_context_with_runtime_sql_datasource_for_testing_get_batch,
):
    context: DataContext = (
        data_context_with_runtime_sql_datasource_for_testing_get_batch
    )
    my_expectation_suite: ExpectationSuite = context.create_expectation_suite(
        "my_expectations"
    )

    validator: Validator

    validator = context.get_validator(
        batch_request=RuntimeBatchRequest(
            datasource_name="my_runtime_sql_datasource",
            data_connector_name="my_runtime_data_connector",
            data_asset_name="IN_MEMORY_DATA_ASSET",
            runtime_parameters={
                "query": "SELECT * FROM table_partitioned_by_date_column__A"
            },
            batch_identifiers={
                "pipeline_stage_name": "core_processing",
                "airflow_run_id": 1234567890,
            },
        ),
        expectation_suite=my_expectation_suite,
    )

    assert len(validator.batches) == 1


def test_get_batch_with_path_in_runtime_parameters_using_runtime_data_connector(
    sa,
    titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled,
):
    context: DataContext = titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled
    data_asset_path = os.path.join(
        context.root_directory, "..", "data", "titanic", "Titanic_19120414_1313.csv"
    )

    batch: Batch

    batch = context.get_batch(
        batch_request=RuntimeBatchRequest(
            datasource_name="my_datasource",
            data_connector_name="my_runtime_data_connector",
            data_asset_name="IN_MEMORY_DATA_ASSET",
            runtime_parameters={"path": data_asset_path},
            batch_identifiers={
                "pipeline_stage_name": "core_processing",
                "airflow_run_id": 1234567890,
            },
        ),
    )

    assert batch.batch_spec is not None
    assert batch.batch_definition["data_asset_name"] == "IN_MEMORY_DATA_ASSET"
    assert isinstance(batch.data, PandasBatchData)
    assert len(batch.data.dataframe.index) == 1313
    assert batch.batch_markers.get("ge_load_time") is not None

    # using path with no extension
    data_asset_path_no_extension = os.path.join(
        context.root_directory, "..", "data", "titanic", "Titanic_19120414_1313"
    )

    # with no reader_method in batch_spec_passthrough
    with pytest.raises(BatchSpecError):
        context.get_batch(
            batch_request=RuntimeBatchRequest(
                datasource_name="my_datasource",
                data_connector_name="my_runtime_data_connector",
                data_asset_name="IN_MEMORY_DATA_ASSET",
                runtime_parameters={"path": data_asset_path_no_extension},
                batch_identifiers={
                    "pipeline_stage_name": "core_processing",
                    "airflow_run_id": 1234567890,
                },
            ),
        )

    # with reader_method in batch_spec_passthrough
    batch = context.get_batch(
        batch_request=RuntimeBatchRequest(
            datasource_name="my_datasource",
            data_connector_name="my_runtime_data_connector",
            data_asset_name="IN_MEMORY_DATA_ASSET",
            runtime_parameters={"path": data_asset_path_no_extension},
            batch_identifiers={
                "pipeline_stage_name": "core_processing",
                "airflow_run_id": 1234567890,
            },
            batch_spec_passthrough={"reader_method": "read_csv"},
        ),
    )

    assert batch.batch_spec is not None
    assert batch.batch_definition["data_asset_name"] == "IN_MEMORY_DATA_ASSET"
    assert isinstance(batch.data, PandasBatchData)
    assert len(batch.data.dataframe.index) == 1313
    assert batch.batch_markers.get("ge_load_time") is not None


def test_get_validator_with_path_in_runtime_parameters_using_runtime_data_connector(
    sa,
    titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled,
):
    context: DataContext = titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled
    data_asset_path = os.path.join(
        context.root_directory, "..", "data", "titanic", "Titanic_19120414_1313.csv"
    )
    my_expectation_suite: ExpectationSuite = context.create_expectation_suite(
        "my_expectations"
    )

    validator: Validator

    validator = context.get_validator(
        batch_request=RuntimeBatchRequest(
            datasource_name="my_datasource",
            data_connector_name="my_runtime_data_connector",
            data_asset_name="IN_MEMORY_DATA_ASSET",
            runtime_parameters={"path": data_asset_path},
            batch_identifiers={
                "pipeline_stage_name": "core_processing",
                "airflow_run_id": 1234567890,
            },
        ),
        expectation_suite=my_expectation_suite,
    )

    assert len(validator.batches) == 1

    # using path with no extension
    data_asset_path_no_extension = os.path.join(
        context.root_directory, "..", "data", "titanic", "Titanic_19120414_1313"
    )

    # with no reader_method in batch_spec_passthrough
    with pytest.raises(BatchSpecError):
        context.get_validator(
            batch_request=RuntimeBatchRequest(
                datasource_name="my_datasource",
                data_connector_name="my_runtime_data_connector",
                data_asset_name="IN_MEMORY_DATA_ASSET",
                runtime_parameters={"path": data_asset_path_no_extension},
                batch_identifiers={
                    "pipeline_stage_name": "core_processing",
                    "airflow_run_id": 1234567890,
                },
            ),
            expectation_suite=my_expectation_suite,
        )

    # with reader_method in batch_spec_passthrough
    validator = context.get_validator(
        batch_request=RuntimeBatchRequest(
            datasource_name="my_datasource",
            data_connector_name="my_runtime_data_connector",
            data_asset_name="IN_MEMORY_DATA_ASSET",
            runtime_parameters={"path": data_asset_path_no_extension},
            batch_identifiers={
                "pipeline_stage_name": "core_processing",
                "airflow_run_id": 1234567890,
            },
            batch_spec_passthrough={"reader_method": "read_csv"},
        ),
        expectation_suite=my_expectation_suite,
    )

    assert len(validator.batches) == 1
