import enum
import os
import shutil
import subprocess
import sys

import pytest

from assets.scripts.build_gallery import execute_shell_command
from great_expectations.data_context.util import file_relative_path


class BackendDependencies(enum.Enum):
    BIGQUERY = "BIGQUERY"
    MYSQL = "MYSQL"
    MSSQL = "MSSQL"
    PANDAS = "PANDAS"
    POSTGRESQL = "POSTGRESQL"
    REDSHIFT = "REDSHIFT"
    SPARK = "SPARK"
    SQLALCHEMY = "SQLALCHEMY"
    SNOWFLAKE = "SNOWFLAKE"


docs_test_matrix = [
    {
        "user_flow_script": "tests/integration/docusaurus/connecting_to_your_data/database/bigquery_yaml_example.py",
        "data_context_dir": "tests/integration/fixtures/no_datasources/great_expectations",
        "data_dir": "tests/test_sets/taxi_yellow_trip_data_samples",
        "util_script": "tests/integration/docusaurus/connecting_to_your_data/database/util.py",
        "extra_backend_dependencies": BackendDependencies.BIGQUERY,
    },
    {
        "user_flow_script": "tests/integration/docusaurus/connecting_to_your_data/database/bigquery_python_example.py",
        "data_context_dir": "tests/integration/fixtures/no_datasources/great_expectations",
        "data_dir": "tests/test_sets/taxi_yellow_trip_data_samples",
        "util_script": "tests/integration/docusaurus/connecting_to_your_data/database/util.py",
        "extra_backend_dependencies": BackendDependencies.BIGQUERY,
    },
    # {
    #     "user_flow_script": "tests/integration/docusaurus/connecting_to_your_data/cloud/s3/pandas/yaml_example.py",
    #     "data_context_dir": "tests/integration/fixtures/no_datasources/great_expectations",
    # },
    # {
    #     "user_flow_script": "tests/integration/docusaurus/connecting_to_your_data/cloud/s3/pandas/python_example.py",
    #     "data_context_dir": "tests/integration/fixtures/no_datasources/great_expectations",
    # },
    # {
    #     "user_flow_script": "tests/integration/docusaurus/connecting_to_your_data/cloud/s3/spark/yaml_example.py",
    #     "extra_backend_dependencies": BackendDependencies.SPARK,
    # },
    # {
    #     "user_flow_script": "tests/integration/docusaurus/connecting_to_your_data/cloud/s3/spark/python_example.py",
    #     "extra_backend_dependencies": BackendDependencies.SPARK,
    # },
    # {
    #     "user_flow_script": "tests/integration/docusaurus/connecting_to_your_data/database/redshift_python_example.py",
    #     "data_context_dir": "tests/integration/fixtures/no_datasources/great_expectations",
    #     "data_dir": "tests/test_sets/taxi_yellow_trip_data_samples",
    #     "util_script": "tests/integration/docusaurus/connecting_to_your_data/database/util.py",
    #     "extra_backend_dependencies": BackendDependencies.REDSHIFT,
    # },
    # {
    #     "user_flow_script": "tests/integration/docusaurus/connecting_to_your_data/database/redshift_yaml_example.py",
    #     "data_context_dir": "tests/integration/fixtures/no_datasources/great_expectations",
    #     "data_dir": "tests/test_sets/taxi_yellow_trip_data_samples",
    #     "util_script": "tests/integration/docusaurus/connecting_to_your_data/database/util.py",
    #     "extra_backend_dependencies": BackendDependencies.REDSHIFT,
    # },
    {
        "name": "getting_started",
        "data_context_dir": "tests/integration/fixtures/yellow_trip_data_pandas_fixture/great_expectations",
        "data_dir": "tests/test_sets/taxi_yellow_trip_data_samples",
        "user_flow_script": "tests/integration/docusaurus/tutorials/getting-started/getting_started.py",
    },
    {
        "user_flow_script": "tests/integration/docusaurus/connecting_to_your_data/filesystem/pandas_yaml_example.py",
        "data_context_dir": "tests/integration/fixtures/no_datasources/great_expectations",
        "data_dir": "tests/test_sets/taxi_yellow_trip_data_samples",
    },
    {
        "user_flow_script": "tests/integration/docusaurus/connecting_to_your_data/filesystem/pandas_python_example.py",
        "data_context_dir": "tests/integration/fixtures/no_datasources/great_expectations",
        "data_dir": "tests/test_sets/taxi_yellow_trip_data_samples",
    },
    {
        "user_flow_script": "tests/integration/docusaurus/connecting_to_your_data/database/postgres_yaml_example.py",
        "data_context_dir": "tests/integration/fixtures/no_datasources/great_expectations",
        "data_dir": "tests/test_sets/taxi_yellow_trip_data_samples",
        "util_script": "tests/integration/docusaurus/connecting_to_your_data/database/util.py",
        "extra_backend_dependencies": BackendDependencies.POSTGRESQL,
    },
    {
        "user_flow_script": "tests/integration/docusaurus/connecting_to_your_data/database/postgres_python_example.py",
        "data_context_dir": "tests/integration/fixtures/no_datasources/great_expectations",
        "data_dir": "tests/test_sets/taxi_yellow_trip_data_samples",
        "util_script": "tests/integration/docusaurus/connecting_to_your_data/database/util.py",
        "extra_backend_dependencies": BackendDependencies.POSTGRESQL,
    },
    {
        "user_flow_script": "tests/integration/docusaurus/connecting_to_your_data/database/snowflake_python_example.py",
        "data_context_dir": "tests/integration/fixtures/no_datasources/great_expectations",
        "data_dir": "tests/test_sets/taxi_yellow_trip_data_samples",
        "util_script": "tests/integration/docusaurus/connecting_to_your_data/database/util.py",
        "extra_backend_dependencies": BackendDependencies.SNOWFLAKE,
    },
    {
        "user_flow_script": "tests/integration/docusaurus/connecting_to_your_data/database/snowflake_yaml_example.py",
        "data_context_dir": "tests/integration/fixtures/no_datasources/great_expectations",
        "data_dir": "tests/test_sets/taxi_yellow_trip_data_samples",
        "util_script": "tests/integration/docusaurus/connecting_to_your_data/database/util.py",
        "extra_backend_dependencies": BackendDependencies.SNOWFLAKE,
    },
    {
        "user_flow_script": "tests/integration/docusaurus/connecting_to_your_data/database/sqlite_yaml_example.py",
        "data_context_dir": "tests/integration/fixtures/no_datasources/great_expectations",
        "data_dir": "tests/test_sets/taxi_yellow_trip_data_samples/sqlite/",
        "extra_backend_dependencies": BackendDependencies.SQLALCHEMY,
    },
    {
        "user_flow_script": "tests/integration/docusaurus/connecting_to_your_data/database/sqlite_python_example.py",
        "data_context_dir": "tests/integration/fixtures/no_datasources/great_expectations",
        "data_dir": "tests/test_sets/taxi_yellow_trip_data_samples/sqlite/",
        "extra_backend_dependencies": BackendDependencies.SQLALCHEMY,
    },
    {
        "user_flow_script": "tests/integration/docusaurus/connecting_to_your_data/in_memory/pandas_yaml_example.py",
        "data_context_dir": "tests/integration/fixtures/no_datasources/great_expectations",
    },
    {
        "user_flow_script": "tests/integration/docusaurus/connecting_to_your_data/in_memory/pandas_python_example.py",
        "data_context_dir": "tests/integration/fixtures/no_datasources/great_expectations",
    },
    {
        "user_flow_script": "tests/integration/docusaurus/template/script_example.py",
        "data_context_dir": "tests/integration/fixtures/no_datasources/great_expectations",
    },
    {
        "user_flow_script": "tests/integration/docusaurus/connecting_to_your_data/in_memory/spark_yaml_example.py",
        "extra_backend_dependencies": BackendDependencies.SPARK,
    },
    {
        "user_flow_script": "tests/integration/docusaurus/connecting_to_your_data/in_memory/spark_python_example.py",
        "extra_backend_dependencies": BackendDependencies.SPARK,
    },
    {
        "user_flow_script": "tests/integration/docusaurus/connecting_to_your_data/filesystem/spark_yaml_example.py",
        "data_dir": "tests/test_sets/taxi_yellow_trip_data_samples",
        "extra_backend_dependencies": BackendDependencies.SPARK,
    },
    {
        "user_flow_script": "tests/integration/docusaurus/connecting_to_your_data/filesystem/spark_python_example.py",
        "data_dir": "tests/test_sets/taxi_yellow_trip_data_samples",
        "extra_backend_dependencies": BackendDependencies.SPARK,
    },
    {
        "user_flow_script": "tests/integration/docusaurus/connecting_to_your_data/database/mysql_yaml_example.py",
        "data_context_dir": "tests/integration/fixtures/no_datasources/great_expectations",
        "data_dir": "tests/test_sets/taxi_yellow_trip_data_samples",
        "util_script": "tests/integration/docusaurus/connecting_to_your_data/database/util.py",
        "extra_backend_dependencies": BackendDependencies.MYSQL,
    },
    {
        "user_flow_script": "tests/integration/docusaurus/connecting_to_your_data/database/mysql_python_example.py",
        "data_context_dir": "tests/integration/fixtures/no_datasources/great_expectations",
        "data_dir": "tests/test_sets/taxi_yellow_trip_data_samples",
        "util_script": "tests/integration/docusaurus/connecting_to_your_data/database/util.py",
        "extra_backend_dependencies": BackendDependencies.MYSQL,
    },
    {
        "name": "rule_base_profiler_multi_batch_example",
        "data_context_dir": "tests/integration/fixtures/yellow_trip_data_pandas_fixture/great_expectations",
        "data_dir": "tests/test_sets/taxi_yellow_trip_data_samples",
        "user_flow_script": "tests/integration/docusaurus/expectations/advanced/multi_batch_rule_based_profiler_example.py",
    },
]

integration_test_matrix = [
    {
        "name": "pandas_one_multi_batch_request_one_validator",
        "data_context_dir": "tests/integration/fixtures/yellow_trip_data_pandas_fixture/great_expectations",
        "data_dir": "tests/test_sets/taxi_yellow_trip_data_samples",
        "user_flow_script": "tests/integration/fixtures/yellow_trip_data_pandas_fixture/one_multi_batch_request_one_validator.py",
    },
    {
        "name": "pandas_two_batch_requests_two_validators",
        "data_context_dir": "tests/integration/fixtures/yellow_trip_data_pandas_fixture/great_expectations",
        "data_dir": "tests/test_sets/taxi_yellow_trip_data_samples",
        "user_flow_script": "tests/integration/fixtures/yellow_trip_data_pandas_fixture/two_batch_requests_two_validators.py",
        "expected_stderrs": "",
        "expected_stdouts": "",
    },
    {
        "name": "pandas_multiple_batch_requests_one_validator_multiple_steps",
        "data_context_dir": "tests/integration/fixtures/yellow_trip_data_pandas_fixture/great_expectations",
        "data_dir": "tests/test_sets/taxi_yellow_trip_data_samples",
        "user_flow_script": "tests/integration/fixtures/yellow_trip_data_pandas_fixture/multiple_batch_requests_one_validator_multiple_steps.py",
    },
    {
        "name": "pandas_multiple_batch_requests_one_validator_one_step",
        "data_context_dir": "tests/integration/fixtures/yellow_trip_data_pandas_fixture/great_expectations",
        "data_dir": "tests/test_sets/taxi_yellow_trip_data_samples",
        "user_flow_script": "tests/integration/fixtures/yellow_trip_data_pandas_fixture/multiple_batch_requests_one_validator_one_step.py",
    },
]


def idfn(test_configuration):
    return test_configuration.get("user_flow_script")


@pytest.fixture
def pytest_parsed_arguments(request):
    return request.config.option


@pytest.mark.docs
@pytest.mark.integration
@pytest.mark.parametrize("test_configuration", docs_test_matrix, ids=idfn)
@pytest.mark.skipif(sys.version_info < (3, 7), reason="requires Python3.7")
def test_docs(test_configuration, tmp_path, pytest_parsed_arguments):
    _check_for_skipped_tests(pytest_parsed_arguments, test_configuration)
    _execute_integration_test(test_configuration, tmp_path)


@pytest.mark.integration
@pytest.mark.parametrize("test_configuration", integration_test_matrix, ids=idfn)
@pytest.mark.skipif(sys.version_info < (3, 7), reason="requires Python3.7")
def test_integration_tests(test_configuration, tmp_path, pytest_parsed_arguments):
    _check_for_skipped_tests(pytest_parsed_arguments, test_configuration)
    _execute_integration_test(test_configuration, tmp_path)


def _execute_integration_test(test_configuration, tmp_path):
    """
    Prepare and environment and run integration tests from a list of tests.

    Note that the only required parameter for a test in the matrix is
    `user_flow_script` and that all other parameters are optional.
    """
    assert (
        "user_flow_script" in test_configuration.keys()
    ), "a `user_flow_script` is required"
    workdir = os.getcwd()
    try:
        base_dir = test_configuration.get(
            "base_dir", file_relative_path(__file__, "../../")
        )
        os.chdir(tmp_path)
        # Ensure GE is installed in our environment
        ge_requirement = test_configuration.get("ge_requirement", "great_expectations")
        execute_shell_command(f"pip install {ge_requirement}")

        #
        # Build test state
        #

        # DataContext
        if test_configuration.get("data_context_dir"):
            context_source_dir = os.path.join(
                base_dir, test_configuration.get("data_context_dir")
            )
            test_context_dir = os.path.join(tmp_path, "great_expectations")
            shutil.copytree(
                context_source_dir,
                test_context_dir,
            )

        # Test Data
        if test_configuration.get("data_dir") is not None:
            source_data_dir = os.path.join(base_dir, test_configuration.get("data_dir"))
            test_data_dir = os.path.join(tmp_path, "data")
            shutil.copytree(
                source_data_dir,
                test_data_dir,
            )

        # UAT Script
        script_source = os.path.join(
            base_dir,
            test_configuration.get("user_flow_script"),
        )
        script_path = os.path.join(tmp_path, "test_script.py")
        shutil.copyfile(script_source, script_path)

        # Util Script
        if test_configuration.get("util_script") is not None:
            script_source = os.path.join(
                base_dir,
                test_configuration.get("util_script"),
            )
            util_script_path = os.path.join(tmp_path, "util.py")
            shutil.copyfile(script_source, util_script_path)

        # Check initial state

        # Execute test
        res = subprocess.run(["python", script_path], capture_output=True)
        # Check final state
        expected_stderrs = test_configuration.get("expected_stderrs")
        expected_stdouts = test_configuration.get("expected_stdouts")
        expected_failure = test_configuration.get("expected_failure")
        outs = res.stdout.decode("utf-8")
        errs = res.stderr.decode("utf-8")
        print(outs)
        print(errs)

        if expected_stderrs:
            assert expected_stderrs == errs

        if expected_stdouts:
            assert expected_stdouts == outs

        if expected_failure:
            assert res.returncode != 0
        else:
            assert res.returncode == 0
    except:
        raise
    finally:
        os.chdir(workdir)


def _check_for_skipped_tests(pytest_args, test_configuration) -> None:
    """Enable scripts to be skipped based on pytest invocation flags."""
    dependencies = test_configuration.get("extra_backend_dependencies", None)
    if not dependencies:
        return
    elif dependencies == BackendDependencies.POSTGRESQL and (
        pytest_args.no_postgresql or pytest_args.no_sqlalchemy
    ):
        pytest.skip("Skipping postgres tests")
    elif dependencies == BackendDependencies.MYSQL and (
        not pytest_args.mysql or pytest_args.no_sqlalchemy
    ):
        pytest.skip("Skipping mysql tests")
    elif dependencies == BackendDependencies.MSSQL and (
        pytest_args.no_mssql or pytest_args.no_sqlalchemy
    ):
        pytest.skip("Skipping mssql tests")
    elif dependencies == BackendDependencies.BIGQUERY and pytest_args.no_sqlalchemy:
        pytest.skip("Skipping bigquery tests")
    elif dependencies == BackendDependencies.REDSHIFT and pytest_args.no_sqlalchemy:
        pytest.skip("Skipping redshift tests")
    elif dependencies == BackendDependencies.SPARK and pytest_args.no_spark:
        pytest.skip("Skipping spark tests")
    elif dependencies == BackendDependencies.SNOWFLAKE and pytest_args.no_sqlalchemy:
        pytest.skip("Skipping snowflake tests")
