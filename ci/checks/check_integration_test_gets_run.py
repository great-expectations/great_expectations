"""
Purpose: To ensure that all integration test files within `tests/integration/docusaurus/`
are included in the integration test suite.

Example test file:
tests/integration/docusaurus/general_directory/specific_directory/how_to_do_my_operation.py

should be included in the integration test suite as follows:

    IntegrationTestFixture(
        name="<NAME>",
        user_flow_script="tests/integration/docusaurus/general_directory/specific_directory/how_to_do_my_operation.py",
        data_context_dir="<DIR>",
        data_dir="<DATA_DIR>",
    ),

Find all test files, generate the test suite and ensure that all test files are included in the test suite.
Assumes that all integration test dependencies are installed and passed into pytest.
"""

import pathlib
import shutil
import subprocess
import sys
from typing import Set

IGNORED_VIOLATIONS = [
    # TODO: Add IntegrationTestFixture for these tests or remove them if no longer needed
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/database/postgres_python_example.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_data_on_gcs_using_pandas.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/database/trino_yaml_example.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/sql_database/yaml_example_complete.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/reference/glossary/checkpoints.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/fixtures/gcp_deployment/ge_checkpoint_gcs.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/expectations/how_to_edit_an_expectation_suite.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/how_to_create_a_batch_of_data_from_an_in_memory_spark_dataframe.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/reference/glossary/batch_request.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/fixtures/query_store/great_expectations/great_expectations.yml",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/spark/inferred_and_runtime_yaml_example.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/in_memory/pandas_yaml_example.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/validation/checkpoints/how_to_create_a_new_checkpoint.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/filesystem/spark_python_example.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/sql_database/yaml_example_gradual.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/expectations/examples/query_expectation_template.py",
    "/Users/phamt/code/work/great_expectations/tests/expectations/core/test_expect_column_values_to_be_in_set.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/spark/inferred_and_runtime_python_example.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/get_existing_data_asset_from_existing_datasource_pandas_filesystem_example.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_python_configs.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/how_to_choose_which_dataconnector_to_use.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/deployment_patterns/aws_cloud_storage_pandas.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/expectations/auto_initializing_expectations/is_expectation_auto_initializing.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/expectations/examples/column_map_expectation_template.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/deployment_patterns/aws_redshift_deployment_patterns.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/expectations/advanced/data_assistant_cross_table_comparison.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/cloud/azure/pandas/inferred_and_runtime_yaml_example.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/database/mssql_python_example.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/database/mssql_yaml_example.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/db/awsathena.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/cloud/s3/spark/inferred_and_runtime_python_example.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/database/redshift_yaml_example.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/fixtures/gcp_deployment/ge_checkpoint_bigquery.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/setup/setup_overview.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/filesystem/pandas_yaml_example.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/expectations/examples/set_based_column_map_expectation_template.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_data_on_s3_using_pandas.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_one_or_more_files_using_pandas.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/reference/core_concepts/checkpoints_and_actions.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/expectations/examples/batch_expectation_template.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/how_to_configure_a_runtimedataconnector.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/in_memory/pandas_python_example.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/expectations/creating_custom_expectations/expect_queried_table_row_count_to_be.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_one_or_more_files_using_spark.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/how_to_create_a_batch_of_data_from_an_in_memory_pandas_dataframe.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/in_memory/spark_python_example.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/filesystem/spark_yaml_example.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/pandas/inferred_and_runtime_python_example.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_onboarding_data_assistant.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/expectations/creating_custom_expectations/expect_column_max_to_be_between_custom.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/database/mysql_yaml_example.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/expectations/advanced/how_to_create_expectations_that_span_multiple_batches_using_evaluation_parameters.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_gcs.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example_gradual.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/how_to_get_one_or_more_batches_of_data_from_a_configured_datasource.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/fixtures/gcp_deployment/great_expectations/great_expectations.yml",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/expectations/advanced/multi_batch_rule_based_profiler_example.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/cloud/s3/spark/inferred_and_runtime_yaml_example.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/deployment_patterns/aws_cloud_storage_spark.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/cloud/azure/pandas/configured_python_example.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/tutorials/getting-started/getting_started.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/template/script_example.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/database/bigquery_python_example.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/organize_batches_in_sqlite_datasource.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/expectations/how_to_create_and_edit_expectations_with_a_profiler.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/expectations/creating_custom_expectations/expect_column_pair_values_to_have_a_difference_of_three.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/pandas/inferred_and_runtime_yaml_example.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/deployment_patterns/gcp_deployment_patterns_file_gcs.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/expectations/advanced/how_to_dynamically_load_evaluation_parameters_from_a_database.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/reference/glossary/data_docs.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/setup/configuring_metadata_stores/how_to_configure_a_validation_result_store_in_gcs.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/validation/checkpoints/how_to_validate_data_by_running_a_checkpoint.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/expectations/creating_custom_expectations/expect_column_values_to_be_in_solfege_scale_set.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/deployment_patterns/aws_emr_serverless_deployment_patterns_great_expectations.yaml",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_yaml_configs.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_sqlite_data.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/validation/validator/how_to_create_and_edit_expectations_with_instant_feedback_block_config.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/database/bigquery_yaml_example.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/database/snowflake_python_example.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/how_to_configure_an_inferredassetdataconnector.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/expectations/creating_custom_expectations/expect_multicolumn_values_to_be_multiples_of_three.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_explicitly_instantiate_an_ephemeral_data_context.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/expectations/how_to_create_and_edit_an_expectationsuite_domain_knowledge.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example_complete.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/reference/core_concepts/result_format.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_yaml_configs.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/datasource_configuration/how_to_configure_a_pandas_datasource.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_initialize_a_filesystem_data_context_in_python.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/deployment_patterns/aws_glue_deployment_patterns.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/expectations/creating_custom_expectations/expect_column_values_to_equal_three.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/in_memory/spark_yaml_example.py",
    "/Users/phamt/code/work/great_expectations/tests/expectations/core/test_expect_column_mean_to_be_positive.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/deployment_patterns/aws_glue_deployment_patterns_great_expectations.yaml",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/deployment_patterns/gcp_deployment_patterns_file_bigquery.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/tutorials/quickstart/quickstart.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_postgresql_data.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/database/athena_python_example.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/expectations/examples/column_pair_map_expectation_template.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_sql_data.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/expectations/examples/multicolumn_map_expectation_template.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/database/postgres_yaml_example.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/validation/checkpoints/how_to_configure_a_new_checkpoint_using_test_yaml_config.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/expectations/auto_initializing_expectations/auto_initializing_expect_column_mean_to_be_between.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_in_gcs.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/expectations/creating_custom_expectations/expect_column_values_to_only_contain_vowels.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/database/sqlite_yaml_example.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/pandas/configured_python_example.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/expectations/examples/column_aggregate_expectation_template.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_instantiate_a_specific_filesystem_data_context.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/setup/configuring_data_contexts/how_to_configure_credentials.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/database/mysql_python_example.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/database/trino_python_example.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_data_on_s3_using_spark.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/database/sqlite_python_example.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/expectations/creating_custom_expectations/expect_batch_columns_to_be_unique.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_data_on_azure_blob_storage_using_spark.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/cloud/azure/spark/inferred_and_runtime_python_example.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/validation/validator/how_to_create_and_edit_expectations_with_instant_feedback_fluent.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/cloud/azure/pandas/inferred_and_runtime_python_example.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/organize_batches_in_pandas_filesystem_datasource.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/expectations/creating_custom_expectations/expect_queried_column_value_frequency_to_meet_threshold.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_sql_data_using_a_query.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/database/snowflake_yaml_example.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/cloud/azure/pandas/configured_yaml_example.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/expectations/examples/regex_based_column_map_expectation_template.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/cloud/s3/pandas/inferred_and_runtime_python_example.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/filesystem/pandas_python_example.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_data_on_azure_blob_storage_using_pandas.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/deployment_patterns/aws_emr_serverless_deployment_patterns.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_data_on_gcs_using_spark.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/database/redshift_python_example.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/cloud/azure/spark/inferred_and_runtime_yaml_example.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_quickly_connect_to_a_single_file_with_pandas.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_a_sql_table.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/pandas/configured_yaml_example.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_in_memory_data_using_pandas.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/fixtures/query_store/great_expectations/uncommitted/config_variables.yml",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/connect_to_your_data_overview.py",
    "/Users/phamt/code/work/great_expectations/tests/integration/docusaurus/connecting_to_your_data/cloud/s3/pandas/inferred_and_runtime_yaml_example.py",
]


def check_dependencies(*deps: str) -> None:
    for dep in deps:
        if not shutil.which(dep):
            raise Exception(f"Must have `{dep}` installed in PATH to run {__file__}")


def get_test_files(target_dir: pathlib.Path) -> Set[str]:
    try:
        res_snippets = subprocess.run(
            [
                "grep",
                "--recursive",
                "--binary-files=without-match",
                "--ignore-case",
                "--word-regexp",
                "--regexp",
                r"^# <snippet .*name=.*>",
                str(target_dir),
            ],
            text=True,
            capture_output=True,
        )
        res_test_files = subprocess.run(
            ["sed", "s/:.*//"],
            text=True,
            input=res_snippets.stdout,
            capture_output=True,
        )
        return set(res_test_files.stdout.splitlines())
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            f"Command {e.cmd} returned with error (code {e.returncode}): {e.output}"
        ) from e


def get_test_files_in_test_suite(target_dir: pathlib.Path) -> Set[str]:
    try:
        res_test_fixtures = subprocess.run(
            [
                "grep",
                "--recursive",
                "--binary-files=without-match",
                "--ignore-case",
                "-E",
                "--regexp",
                r"^\s*IntegrationTestFixture",
                str(target_dir),
            ],
            text=True,
            capture_output=True,
        )
        res_test_fixture_files = subprocess.run(
            ["sed", "s/:.*//"],
            text=True,
            input=res_test_fixtures.stdout,
            capture_output=True,
        )
        res_unique_test_fixture_files = subprocess.run(
            ["sort", "--unique"],
            text=True,
            input=res_test_fixture_files.stdout,
            capture_output=True,
        )
        res_test_fixture_definitions = subprocess.run(
            [
                "xargs",
                "grep",
                "--binary-files=without-match",
                "--no-filename",
                "--ignore-case",
                "--word-regexp",
                "--regexp",
                r"^\s*user_flow_script=.*",
            ],
            text=True,
            input=res_unique_test_fixture_files.stdout,
            capture_output=True,
        )
        res_test_files_with_fixture_definitions = subprocess.run(
            ["sed", 's/user_flow_script="//;s/",//'],
            text=True,
            input=res_test_fixture_definitions.stdout,
            capture_output=True,
        )
        return {
            s.strip()
            for s in res_test_files_with_fixture_definitions.stdout.splitlines()
        }
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            f"Command {e.cmd} returned with error (code {e.returncode}): {e.output}"
        ) from e


def main() -> None:
    check_dependencies("grep", "sed", "sort", "xargs")
    project_root = pathlib.Path(__file__).parent.parent.parent
    path_to_tests = project_root / "tests"
    assert path_to_tests.exists()

    new_violations = (
        get_test_files(path_to_tests)
        .difference(get_test_files_in_test_suite(path_to_tests))
        .difference(IGNORED_VIOLATIONS)
    )
    if new_violations:
        print(
            f"[ERROR] Found {len(new_violations)} test files which are not used in test suite."
        )
        for line in new_violations:
            print(line)
        sys.exit(1)


if __name__ == "__main__":
    main()
