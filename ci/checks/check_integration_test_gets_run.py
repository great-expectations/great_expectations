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
"""  # noqa: E501

import pathlib
import shutil
import subprocess
import sys
from typing import Set

IGNORED_VIOLATIONS = [
    # TODO: Add IntegrationTestFixture for these tests or remove them if no longer needed
    "docs/docusaurus/docs/oss/guides/expectations/creating_custom_expectations/test_expect_column_mean_to_be_positive.py",
    "docs/docusaurus/docs/oss/guides/expectations/creating_custom_expectations/test_expect_column_values_to_be_in_set.py",
    "tests/integration/docusaurus/connecting_to_your_data/cloud/azure/pandas/configured_python_example.py",
    "tests/integration/docusaurus/connecting_to_your_data/cloud/azure/pandas/configured_yaml_example.py",
    "tests/integration/docusaurus/connecting_to_your_data/cloud/azure/spark/inferred_and_runtime_python_example.py",
    "tests/integration/docusaurus/connecting_to_your_data/cloud/azure/spark/inferred_and_runtime_yaml_example.py",
    "tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/pandas/configured_python_example.py",
    "tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/spark/inferred_and_runtime_python_example.py",
    "tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/spark/inferred_and_runtime_yaml_example.py",
    "docs/docusaurus/docs/snippets/redshift_python_example.py",
    "docs/docusaurus/docs/snippets/redshift_yaml_example.py",
    "docs/docusaurus/docs/oss/guides/connecting_to_your_data/fluent/database/how_to_connect_to_sql_data.py",
    "docs/docusaurus/docs/oss/guides/connecting_to_your_data/fluent/filesystem/how_to_quickly_connect_to_a_single_file_with_pandas.py",
    "tests/integration/docusaurus/deployment_patterns/aws_emr_serverless_deployment_patterns_great_expectations.yaml",
    "tests/integration/docusaurus/deployment_patterns/aws_glue_deployment_patterns_great_expectations.yaml",
    "docs/docusaurus/docs/snippets/databricks_deployment_patterns_file_python_configs.py",
    "docs/docusaurus/docs/oss/guides/expectations/creating_custom_expectations/expect_batch_columns_to_be_unique.py",
    "docs/docusaurus/docs/oss/guides/expectations/creating_custom_expectations/batch_expectation_template.py",
    "docs/docusaurus/docs/oss/guides/expectations/creating_custom_expectations/column_aggregate_expectation_template.py",
    "docs/docusaurus/docs/oss/guides/expectations/creating_custom_expectations/column_map_expectation_template.py",
    "docs/docusaurus/docs/oss/guides/expectations/creating_custom_expectations/column_pair_map_expectation_template.py",
    "docs/docusaurus/docs/oss/guides/expectations/creating_custom_expectations/multicolumn_map_expectation_template.py",
    "docs/docusaurus/docs/oss/guides/expectations/creating_custom_expectations/query_expectation_template.py",
    "docs/docusaurus/docs/oss/guides/expectations/creating_custom_expectations/regex_based_column_map_expectation_template.py",
    "docs/docusaurus/docs/oss/guides/expectations/creating_custom_expectations/set_based_column_map_expectation_template.py",
    "docs/docusaurus/docs/oss/guides/expectations/how_to_edit_an_expectation_suite.py",
    "docs/docusaurus/docs/oss/guides/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_gcs.py",
    "docs/docusaurus/docs/oss/guides/setup/configuring_metadata_stores/how_to_configure_a_validation_result_store_in_gcs.py",
    "docs/docusaurus/docs/oss/guides/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_in_gcs.py",
    "docs/docusaurus/docs/snippets/quickstart.py",
    "docs/docusaurus/docs/oss/guides/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint.py",
    "docs/docusaurus/docs/oss/guides/connecting_to_your_data/fluent/database/ge_checkpoint_bigquery.py",
    "docs/docusaurus/docs/oss/guides/connecting_to_your_data/fluent/database/ge_checkpoint_gcs.py",
    "tests/integration/fixtures/gcp_deployment/great_expectations/great_expectations.yml",
    "docs/docusaurus/docs/oss/guides/expectations/advanced/great_expectations.yml",
    "docs/docusaurus/docs/oss/guides/expectations/advanced/config_variables.yml",
    "docs/docusaurus/docs/snippets/legacy_docs/ge_checkpoint_gcs.py",
    "docs/docusaurus/docs/snippets/legacy_docs/ge_checkpoint_bigquery.py",
]


def check_dependencies(*deps: str) -> None:
    for dep in deps:
        if not shutil.which(dep):
            raise Exception(f"Must have `{dep}` installed in PATH to run {__file__}")  # noqa: TRY002, TRY003


def get_test_files(target_dir: pathlib.Path) -> Set[str]:
    try:
        res_snippets = subprocess.run(  # noqa: PLW1510
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
        pwd = subprocess.run(["pwd"], text=True, capture_output=True, check=True).stdout.strip("\n")
        res_test_files = subprocess.run(  # noqa: PLW1510
            ["sed", f"s/:.*//;s?{pwd}??"],
            text=True,
            input=res_snippets.stdout,
            capture_output=True,
        )
        return {s.strip("/") for s in res_test_files.stdout.splitlines()}
    except subprocess.CalledProcessError as e:
        raise RuntimeError(  # noqa: TRY003
            f"Command {e.cmd} returned with error (code {e.returncode}): {e.output}"
        ) from e


def get_test_files_in_test_suite(target_dir: pathlib.Path) -> Set[str]:
    try:
        res_test_fixtures = subprocess.run(  # noqa: PLW1510
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
        res_test_fixture_files = subprocess.run(  # noqa: PLW1510
            ["sed", "s/:.*//"],
            text=True,
            input=res_test_fixtures.stdout,
            capture_output=True,
        )
        res_unique_test_fixture_files = subprocess.run(  # noqa: PLW1510
            ["sort", "--unique"],
            text=True,
            input=res_test_fixture_files.stdout,
            capture_output=True,
        )
        res_test_fixture_definitions = subprocess.run(  # noqa: PLW1510
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
        res_test_files_with_fixture_definitions = subprocess.run(  # noqa: PLW1510
            ["sed", 's/user_flow_script="//;s/",//'],
            text=True,
            input=res_test_fixture_definitions.stdout,
            capture_output=True,
        )
        return {s.strip() for s in res_test_files_with_fixture_definitions.stdout.splitlines()}
    except subprocess.CalledProcessError as e:
        raise RuntimeError(  # noqa: TRY003
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
            f"[ERROR] Found {len(new_violations)} test files which are not used in test suite; please add to test script runner!"  # noqa: E501
        )
        for line in new_violations:
            print(line)
        sys.exit(1)


if __name__ == "__main__":
    main()
