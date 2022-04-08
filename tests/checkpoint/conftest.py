import os
import shutil

import pytest

from great_expectations import DataContext
from great_expectations.core import ExpectationConfiguration
from great_expectations.data_context.util import file_relative_path


@pytest.fixture
def titanic_pandas_data_context_stats_enabled_and_expectation_suite_with_one_expectation(
    titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled,
):
    context: DataContext = titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled
    # create expectation suite
    suite = context.create_expectation_suite("my_expectation_suite")
    expectation = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_between",
        kwargs={"column": "col1", "min_value": 1, "max_value": 2},
    )
    # NOTE Will 20211208 _add_expectation() method, although being called by an ExpectationSuite instance, is being
    # called within a fixture, and so will call the private method _add_expectation() and prevent it from sending a
    # usage_event.
    suite._add_expectation(expectation, send_usage_event=False)
    context.save_expectation_suite(suite)
    return context


@pytest.fixture
def titanic_spark_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled(
    tmp_path_factory,
    monkeypatch,
    spark_session,
):
    # Re-enable GE_USAGE_STATS
    monkeypatch.delenv("GE_USAGE_STATS")

    project_path: str = str(tmp_path_factory.mktemp("titanic_data_context"))
    context_path: str = os.path.join(project_path, "great_expectations")
    os.makedirs(os.path.join(context_path, "expectations"), exist_ok=True)
    data_path: str = os.path.join(context_path, "..", "data", "titanic")
    os.makedirs(os.path.join(data_path), exist_ok=True)
    shutil.copy(
        file_relative_path(
            __file__,
            os.path.join(
                "..",
                "test_fixtures",
                "great_expectations_v013_no_datasource_stats_enabled.yml",
            ),
        ),
        str(os.path.join(context_path, "great_expectations.yml")),
    )
    shutil.copy(
        file_relative_path(__file__, os.path.join("..", "test_sets", "Titanic.csv")),
        str(
            os.path.join(
                context_path, "..", "data", "titanic", "Titanic_19120414_1313.csv"
            )
        ),
    )
    shutil.copy(
        file_relative_path(__file__, os.path.join("..", "test_sets", "Titanic.csv")),
        str(
            os.path.join(context_path, "..", "data", "titanic", "Titanic_19120414_1313")
        ),
    )
    shutil.copy(
        file_relative_path(__file__, os.path.join("..", "test_sets", "Titanic.csv")),
        str(os.path.join(context_path, "..", "data", "titanic", "Titanic_1911.csv")),
    )
    shutil.copy(
        file_relative_path(__file__, os.path.join("..", "test_sets", "Titanic.csv")),
        str(os.path.join(context_path, "..", "data", "titanic", "Titanic_1912.csv")),
    )

    context: DataContext = DataContext(context_root_dir=context_path)
    assert context.root_directory == context_path

    datasource_config: str = f"""
        class_name: Datasource

        execution_engine:
            class_name: SparkDFExecutionEngine

        data_connectors:
            my_basic_data_connector:
                class_name: InferredAssetFilesystemDataConnector
                base_directory: {data_path}
                default_regex:
                    pattern: (.*)\\.csv
                    group_names:
                        - data_asset_name

            my_special_data_connector:
                class_name: ConfiguredAssetFilesystemDataConnector
                base_directory: {data_path}
                glob_directive: "*.csv"

                default_regex:
                    pattern: (.+)\\.csv
                    group_names:
                        - name
                assets:
                    users:
                        base_directory: {data_path}
                        pattern: (.+)_(\\d+)_(\\d+)\\.csv
                        group_names:
                            - name
                            - timestamp
                            - size

            my_other_data_connector:
                class_name: ConfiguredAssetFilesystemDataConnector
                base_directory: {data_path}
                glob_directive: "*.csv"

                default_regex:
                    pattern: (.+)\\.csv
                    group_names:
                        - name
                assets:
                    users: {{}}

            my_runtime_data_connector:
                module_name: great_expectations.datasource.data_connector
                class_name: RuntimeDataConnector
                batch_identifiers:
                    - pipeline_stage_name
                    - airflow_run_id
    """

    # noinspection PyUnusedLocal
    context.test_yaml_config(
        name="my_datasource", yaml_config=datasource_config, pretty_print=False
    )
    # noinspection PyProtectedMember
    context._save_project_config()
    return context
