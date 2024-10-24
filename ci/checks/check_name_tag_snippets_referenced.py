"""
Purpose: To ensure that all named snippets are referenced in the docs.

Python code snippets refers to the Python module, containing the test, as follows:

```python name="tests/integration/docusaurus/general_directory/specific_directory/how_to_do_my_operation.py get_context"
```

whereby "tests/integration/docusaurus/general_directory/specific_directory/how_to_do_my_operation.py get_context", which
is the Python module, containing the integration test in the present example, would contain the following tagged code:

# Python
# <snippet name="tests/integration/docusaurus/general_directory/specific_directory/how_to_do_my_operation.py get_context">
import great_expectations as gx

context = gx.get_context()
# </snippet>

Find all named snippets and ensure that they are referenced in the docs using the above syntax.
"""  # noqa: E501

import pathlib
import shutil
import subprocess
import sys
from typing import List

# TODO: address ignored snippets by deleting snippet or test file, or adding documentation that references them  # noqa: E501
IGNORED_VIOLATIONS = [
    "tests/integration/docusaurus/connecting_to_your_data/cloud/azure/spark/inferred_and_runtime_python_example.py add datasource",  # noqa: E501
    "tests/integration/docusaurus/connecting_to_your_data/cloud/azure/spark/inferred_and_runtime_python_example.py datasource config",  # noqa: E501
    "tests/integration/docusaurus/connecting_to_your_data/cloud/azure/spark/inferred_and_runtime_python_example.py test datasource",  # noqa: E501
    "tests/integration/docusaurus/connecting_to_your_data/cloud/azure/spark/inferred_and_runtime_yaml_example.py add datasource",  # noqa: E501
    "tests/integration/docusaurus/connecting_to_your_data/cloud/azure/spark/inferred_and_runtime_yaml_example.py batch request",  # noqa: E501
    "tests/integration/docusaurus/connecting_to_your_data/cloud/azure/spark/inferred_and_runtime_yaml_example.py datasource config",  # noqa: E501
    "tests/integration/docusaurus/connecting_to_your_data/cloud/azure/spark/inferred_and_runtime_yaml_example.py get validator",  # noqa: E501
    "tests/integration/docusaurus/connecting_to_your_data/cloud/azure/spark/inferred_and_runtime_yaml_example.py imports",  # noqa: E501
    "tests/integration/docusaurus/connecting_to_your_data/cloud/azure/spark/inferred_and_runtime_yaml_example.py test datasource",  # noqa: E501
    "tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/spark/inferred_and_runtime_python_example.py add datasource config",  # noqa: E501
    "tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/spark/inferred_and_runtime_python_example.py datasource config",  # noqa: E501
    "tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/spark/inferred_and_runtime_python_example.py test datasource config",  # noqa: E501
    "tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/spark/inferred_and_runtime_yaml_example.py add_datasource",  # noqa: E501
    "tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/spark/inferred_and_runtime_yaml_example.py batch_request",  # noqa: E501
    "tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/spark/inferred_and_runtime_yaml_example.py datasource_yaml",  # noqa: E501
    "tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/spark/inferred_and_runtime_yaml_example.py imports",  # noqa: E501
    "tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/spark/inferred_and_runtime_yaml_example.py runtime_batch_request",  # noqa: E501
    "tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/spark/inferred_and_runtime_yaml_example.py test_yaml_config",  # noqa: E501
    "tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/spark/inferred_and_runtime_yaml_example.py validator_creation",  # noqa: E501
    "tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/spark/inferred_and_runtime_yaml_example.py validator_creation_2",  # noqa: E501
    "docs/docusaurus/docs/snippets/get_existing_data_asset_from_existing_datasource_pandas_filesystem_example.py my_datasource",  # noqa: E501
    "tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example_complete.py add_datasource",  # noqa: E501
    "tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example_complete.py batch_filter_parameters",  # noqa: E501
    "tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example_complete.py batch_request",  # noqa: E501
    "tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example_complete.py datasource_yaml",  # noqa: E501
    "tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example_complete.py get_context",  # noqa: E501
    "tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example_complete.py imports",  # noqa: E501
    "tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example_complete.py sample_using_random 10 pct",  # noqa: E501
    "tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example_complete.py sampling batch size",  # noqa: E501
    "tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example_complete.py split_on_column_value passenger_count",  # noqa: E501
    "tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example_complete.py test_yaml_config",  # noqa: E501
    "tests/integration/docusaurus/connecting_to_your_data/in_memory/pandas_python_example.py add_datasource",  # noqa: E501
    "tests/integration/docusaurus/connecting_to_your_data/in_memory/pandas_python_example.py datasource_config",  # noqa: E501
    "tests/integration/docusaurus/connecting_to_your_data/in_memory/pandas_python_example.py test_yaml_config",  # noqa: E501
    "docs/docusaurus/docs/snippets/aws_cloud_storage_pandas.py get_batch_list",
    "docs/docusaurus/docs/snippets/aws_cloud_storage_pandas.py get_batch_request",
    "docs/docusaurus/docs/snippets/aws_redshift_deployment_patterns.py add_data_docs_store",
    "docs/docusaurus/docs/snippets/aws_redshift_deployment_patterns.py create_checkpoint",
    "docs/docusaurus/docs/snippets/aws_redshift_deployment_patterns.py existing_expectations_store",
    "docs/docusaurus/docs/snippets/aws_redshift_deployment_patterns.py existing_validation_results_store",  # noqa: E501
    "docs/docusaurus/docs/snippets/aws_redshift_deployment_patterns.py imports",
    "docs/docusaurus/docs/snippets/aws_redshift_deployment_patterns.py new_expectations_store",
    "docs/docusaurus/docs/snippets/aws_redshift_deployment_patterns.py new_validation_results_store",  # noqa: E501
    "docs/docusaurus/docs/snippets/aws_redshift_deployment_patterns.py run checkpoint",
    "docs/docusaurus/docs/snippets/aws_redshift_deployment_patterns.py save_expectations",
    "docs/docusaurus/docs/snippets/aws_redshift_deployment_patterns.py set_new_validation_results_store",  # noqa: E501
    "docs/docusaurus/docs/snippets/aws_redshift_deployment_patterns.py validator_calls",
    "docs/docusaurus/docs/snippets/databricks_deployment_patterns_dataframe_python_configs.py choose context_root_dir",  # noqa: E501
    "docs/docusaurus/docs/snippets/databricks_deployment_patterns_dataframe_python_configs.py imports",  # noqa: E501
    "docs/docusaurus/docs/snippets/databricks_deployment_patterns_dataframe_python_configs.py run checkpoint",  # noqa: E501
    "docs/docusaurus/docs/snippets/databricks_deployment_patterns_dataframe_python_configs.py set up context",  # noqa: E501
    "docs/docusaurus/docs/snippets/databricks_deployment_patterns_dataframe_yaml_configs.py add datasource config",  # noqa: E501
    "docs/docusaurus/docs/snippets/databricks_deployment_patterns_dataframe_yaml_configs.py add expectations",  # noqa: E501
    "docs/docusaurus/docs/snippets/databricks_deployment_patterns_dataframe_yaml_configs.py create batch request",  # noqa: E501
    "docs/docusaurus/docs/snippets/databricks_deployment_patterns_dataframe_yaml_configs.py datasource config",  # noqa: E501
    "docs/docusaurus/docs/snippets/databricks_deployment_patterns_dataframe_yaml_configs.py get validator",  # noqa: E501
    "docs/docusaurus/docs/snippets/databricks_deployment_patterns_dataframe_yaml_configs.py imports",  # noqa: E501
    "docs/docusaurus/docs/snippets/databricks_deployment_patterns_dataframe_yaml_configs.py root directory",  # noqa: E501
    "docs/docusaurus/docs/snippets/databricks_deployment_patterns_dataframe_yaml_configs.py run checkpoint",  # noqa: E501
    "docs/docusaurus/docs/snippets/databricks_deployment_patterns_dataframe_yaml_configs.py save suite",  # noqa: E501
    "docs/docusaurus/docs/snippets/databricks_deployment_patterns_dataframe_yaml_configs.py set up context",  # noqa: E501
    "docs/docusaurus/docs/snippets/databricks_deployment_patterns_dataframe_yaml_configs.py test datasource config",  # noqa: E501
    "docs/docusaurus/docs/snippets/databricks_deployment_patterns_file_python_configs.py add expectations",  # noqa: E501
    "docs/docusaurus/docs/snippets/databricks_deployment_patterns_file_python_configs.py get validator",  # noqa: E501
    "docs/docusaurus/docs/snippets/databricks_deployment_patterns_file_python_configs.py save suite",  # noqa: E501
    "docs/docusaurus/docs/oss/guides/connecting_to_your_data/fluent/database/gcp_deployment_patterns_file_bigquery.py existing_expectations_store",  # noqa: E501
    "docs/docusaurus/docs/oss/guides/connecting_to_your_data/fluent/database/gcp_deployment_patterns_file_bigquery.py get_context",  # noqa: E501
    "docs/docusaurus/docs/oss/guides/connecting_to_your_data/fluent/database/gcp_deployment_patterns_file_gcs.py batch_request",  # noqa: E501
    "docs/docusaurus/docs/snippets/postgres_deployment_patterns.py pg_batch_request",
    "tests/integration/docusaurus/expectations/how_to_create_and_edit_expectations_with_a_profiler build_suite",  # noqa: E501
    "tests/integration/docusaurus/expectations/how_to_create_and_edit_expectations_with_a_profiler create_asset",  # noqa: E501
    "tests/integration/docusaurus/expectations/how_to_create_and_edit_expectations_with_a_profiler create_profiler",  # noqa: E501
    "tests/integration/docusaurus/expectations/how_to_create_and_edit_expectations_with_a_profiler create_validator",  # noqa: E501
    "tests/integration/docusaurus/expectations/how_to_create_and_edit_expectations_with_a_profiler e2e",  # noqa: E501
    "tests/integration/docusaurus/expectations/how_to_create_and_edit_expectations_with_a_profiler get_asset",  # noqa: E501
    "tests/integration/docusaurus/expectations/how_to_create_and_edit_expectations_with_a_profiler name_suite",  # noqa: E501
    "tests/integration/docusaurus/expectations/how_to_create_and_edit_expectations_with_a_profiler optional_params",  # noqa: E501
    "tests/integration/docusaurus/expectations/how_to_create_and_edit_expectations_with_a_profiler semantic",  # noqa: E501
    "docs/docusaurus/docs/snippets/checkpoints_and_actions.py assert_suite",
    "docs/docusaurus/docs/snippets/checkpoints_and_actions.py checkpoint_example",
    "docs/docusaurus/docs/snippets/checkpoints_and_actions.py keys_passed_at_runtime",
    "docs/docusaurus/docs/snippets/checkpoints_and_actions.py nesting_with_defaults",
    "docs/docusaurus/docs/snippets/checkpoints_and_actions.py no_nesting",
    "docs/docusaurus/docs/snippets/checkpoints_and_actions.py run_checkpoint_5",
    "docs/docusaurus/docs/snippets/checkpoints_and_actions.py using_simple_checkpoint",
    "docs/docusaurus/docs/snippets/checkpoints_and_actions.py using_template",
    "docs/docusaurus/docs/snippets/checkpoints.py create_and_run",
    "docs/docusaurus/docs/snippets/checkpoints.py save",
    "docs/docusaurus/docs/snippets/checkpoints.py setup",
    "docs/docusaurus/docs/oss/guides/setup/configuring_data_docs/data_docs.py data_docs",
    "docs/docusaurus/docs/oss/guides/setup/configuring_data_docs/data_docs.py data_docs_site",
    "docs/docusaurus/docs/oss/guides/setup/configuring_data_contexts/how_to_configure_credentials.py add_credential_from_yml",  # noqa: E501
    "docs/docusaurus/docs/oss/guides/setup/configuring_data_contexts/how_to_configure_credentials.py add_credentials_as_connection_string",  # noqa: E501
    "docs/docusaurus/docs/oss/guides/setup/configuring_data_contexts/how_to_configure_credentials.py export_env_vars",  # noqa: E501
    "docs/docusaurus/docs/oss/guides/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_gcs.py build data docs command",  # noqa: E501
    "docs/docusaurus/docs/oss/guides/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_gcs.py build data docs output",  # noqa: E501
    "docs/docusaurus/docs/oss/guides/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_gcs.py create bucket command",  # noqa: E501
    "docs/docusaurus/docs/oss/guides/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_gcs.py create bucket output",  # noqa: E501
    "docs/docusaurus/docs/oss/guides/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_gcs.py gcloud app deploy",  # noqa: E501
    "docs/docusaurus/docs/oss/guides/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_gcs.py gcloud login and set project",  # noqa: E501
    "docs/docusaurus/docs/oss/guides/setup/configuring_metadata_stores/how_to_configure_a_validation_result_store_in_gcs.py copy_validation_command",  # noqa: E501
    "docs/docusaurus/docs/oss/guides/setup/configuring_metadata_stores/how_to_configure_a_validation_result_store_in_gcs.py copy_validation_output",  # noqa: E501
    "docs/docusaurus/docs/oss/guides/setup/configuring_metadata_stores/how_to_configure_a_validation_result_store_in_gcs.py list_validation_stores_command",  # noqa: E501
    "docs/docusaurus/docs/oss/guides/setup/configuring_metadata_stores/how_to_configure_a_validation_result_store_in_gcs.py list_validation_stores_output",  # noqa: E501
    "docs/docusaurus/docs/oss/guides/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_in_gcs.py copy_expectation_command",  # noqa: E501
    "docs/docusaurus/docs/oss/guides/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_in_gcs.py copy_expectation_output",  # noqa: E501
    "docs/docusaurus/docs/oss/guides/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_in_gcs.py list_expectation_stores_command",  # noqa: E501
    "docs/docusaurus/docs/oss/guides/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_in_gcs.py list_expectation_stores_output",  # noqa: E501
    "docs/docusaurus/docs/oss/guides/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_in_gcs.py list_expectation_suites_command",  # noqa: E501
    "tests/integration/docusaurus/setup/setup_overview.py setup",
    "docs/docusaurus/docs/oss/templates/script_example.py full",
    "tests/integration/docusaurus/tutorials/getting-started/getting_started.py checkpoint_yaml_config",  # noqa: E501
    "tests/integration/docusaurus/tutorials/getting-started/getting_started.py datasource_yaml",
    "tests/integration/docusaurus/tutorials/quickstart/v1_pandas_quickstart.py connect_to_data",
    "tests/integration/docusaurus/tutorials/quickstart/v1_pandas_quickstart.py create_expectation",
    "tests/integration/docusaurus/tutorials/quickstart/v1_pandas_quickstart.py get_context",
    "tests/integration/docusaurus/tutorials/quickstart/v1_pandas_quickstart.py import_gx",
    "tests/integration/docusaurus/tutorials/quickstart/v1_pandas_quickstart.py update_expectation",
    "tests/integration/docusaurus/tutorials/quickstart/v1_sql_quickstart.py connect_to_data",
    "tests/integration/docusaurus/tutorials/quickstart/v1_sql_quickstart.py create_expectation",
    "tests/integration/docusaurus/tutorials/quickstart/v1_sql_quickstart.py get_context",
    "tests/integration/docusaurus/tutorials/quickstart/v1_sql_quickstart.py import_gx",
    "tests/integration/docusaurus/tutorials/quickstart/v1_sql_quickstart.py update_expectation",
    "tests/integration/fixtures/gcp_deployment/great_expectations/great_expectations.yml expectations_GCS_store",  # noqa: E501
    "tests/integration/fixtures/gcp_deployment/great_expectations/great_expectations.yml expectations_store_name",  # noqa: E501
    "tests/integration/fixtures/gcp_deployment/great_expectations/great_expectations.yml gs_site",
    "tests/integration/fixtures/gcp_deployment/great_expectations/great_expectations.yml validation_results_GCS_store",  # noqa: E501
    "tests/integration/fixtures/gcp_deployment/great_expectations/great_expectations.yml validation_results_store_name",  # noqa: E501
]


def check_dependencies(*deps: str) -> None:
    for dep in deps:
        if not shutil.which(dep):
            raise Exception(f"Must have `{dep}` installed in PATH to run {__file__}")  # noqa: TRY002, TRY003


def get_snippet_definitions(target_dir: pathlib.Path) -> List[str]:
    try:
        res_snippets = subprocess.run(  # noqa: PLW1510
            [
                "grep",
                "--recursive",
                "--binary-files=without-match",
                "--no-filename",
                "--ignore-case",
                "--word-regexp",
                "--regexp",
                r"^# <snippet .*name=.*>",
                str(target_dir),
            ],
            text=True,
            capture_output=True,
        )
        res_snippet_names = subprocess.run(  # noqa: PLW1510
            ["sed", 's/.*name="//; s/">//; s/version-[0-9\\.]* //'],
            text=True,
            input=res_snippets.stdout,
            capture_output=True,
        )
        return res_snippet_names.stdout.splitlines()
    except subprocess.CalledProcessError as e:
        raise RuntimeError(  # noqa: TRY003
            f"Command {e.cmd} returned with error (code {e.returncode}): {e.output}"
        ) from e


def get_snippets_used(target_dir: pathlib.Path) -> List[str]:
    try:
        res_snippet_usages = subprocess.run(  # noqa: PLW1510
            [
                "grep",
                "--recursive",
                "--binary-files=without-match",
                "--no-filename",
                "--exclude-dir=versioned_code",
                "--exclude-dir=versioned_docs",
                "--ignore-case",
                "-E",
                "--regexp",
                r"```(python|yaml).*name=",
                str(target_dir),
            ],
            text=True,
            capture_output=True,
        )
        res_snippet_used_names = subprocess.run(  # noqa: PLW1510
            ["sed", 's/.*="//; s/".*//; s/version-[0-9\\.]* //'],
            text=True,
            input=res_snippet_usages.stdout,
            capture_output=True,
        )
        return res_snippet_used_names.stdout.splitlines()
    except subprocess.CalledProcessError as e:
        raise RuntimeError(  # noqa: TRY003
            f"Command {e.cmd} returned with error (code {e.returncode}): {e.output}"
        ) from e


def main() -> None:
    check_dependencies("grep", "sed")
    project_root = pathlib.Path(__file__).parent.parent.parent
    docs_dir = project_root / "docs"
    assert docs_dir.exists()
    tests_dir = project_root / "tests"
    assert tests_dir.exists()
    new_violations = sorted(
        set(get_snippet_definitions(tests_dir))
        .difference(set(get_snippets_used(docs_dir)))
        .difference(set(IGNORED_VIOLATIONS))
    )
    if new_violations:
        print(f"[ERROR] Found {len(new_violations)} snippets which are not used within a doc file.")
        for line in new_violations:
            print(line)
        sys.exit(1)


if __name__ == "__main__":
    main()
