"""Run integration and docs tests.

Individual tests can be run by setting the '-k' flag and referencing the name of test, like the following example:
    pytest -v --docs-tests -m integration -k "test_docs[quickstart]" tests/integration/test_script_runner.py
"""

import importlib.machinery
import importlib.util
import logging
import os
import pathlib
import shutil
import sys
from typing import List

import pkg_resources
import pytest
from assets.scripts.build_gallery import execute_shell_command

from great_expectations.data_context.util import file_relative_path
from tests.integration.backend_dependencies import BackendDependencies
from tests.integration.integration_test_fixture import IntegrationTestFixture
from tests.integration.test_definitions.abs.integration_tests import (
    abs_integration_tests,
)
from tests.integration.test_definitions.athena.integration_tests import (
    athena_integration_tests,
)
from tests.integration.test_definitions.aws_glue.integration_tests import (
    aws_glue_integration_tests,
)
from tests.integration.test_definitions.bigquery.integration_tests import (
    bigquery_integration_tests,
)
from tests.integration.test_definitions.gcs.integration_tests import (
    gcs_integration_tests,
)
from tests.integration.test_definitions.mssql.integration_tests import (
    mssql_integration_tests,
)
from tests.integration.test_definitions.multiple_backend.integration_tests import (
    multiple_backend,
)
from tests.integration.test_definitions.mysql.integration_tests import (
    mysql_integration_tests,
)
from tests.integration.test_definitions.postgresql.integration_tests import (
    postgresql_integration_tests,
)
from tests.integration.test_definitions.redshift.integration_tests import (
    redshift_integration_tests,
)
from tests.integration.test_definitions.s3.integration_tests import s3_integration_tests
from tests.integration.test_definitions.snowflake.integration_tests import (
    snowflake_integration_tests,
)
from tests.integration.test_definitions.spark.integration_tests import (
    spark_integration_tests,
)
from tests.integration.test_definitions.sqlite.integration_tests import (
    sqlite_integration_tests,
)
from tests.integration.test_definitions.trino.integration_tests import (
    trino_integration_tests,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# to be populated by the smaller lists below
docs_test_matrix: List[IntegrationTestFixture] = []

local_tests = [
    IntegrationTestFixture(
        name="how_to_create_a_batch_of_data_from_an_in_memory_pandas_dataframe",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/how_to_create_a_batch_of_data_from_an_in_memory_pandas_dataframe.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
        backend_dependencies=[BackendDependencies.PANDAS],
    ),
    IntegrationTestFixture(
        name="getting_started",
        data_context_dir="tests/integration/fixtures/yellow_tripdata_pandas_fixture/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
        user_flow_script="tests/integration/docusaurus/tutorials/getting-started/getting_started.py",
        backend_dependencies=[],
    ),
    IntegrationTestFixture(
        name="how_to_get_one_or_more_batches_of_data_from_a_configured_datasource",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/how_to_get_one_or_more_batches_of_data_from_a_configured_datasource.py",
        data_context_dir="tests/integration/fixtures/yellow_tripdata_pandas_fixture/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples",
        backend_dependencies=[],
    ),
    IntegrationTestFixture(
        name="connecting_to_your_data_pandas_yaml",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/filesystem/pandas_yaml_example.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
        backend_dependencies=[BackendDependencies.PANDAS],
    ),
    IntegrationTestFixture(
        name="connecting_to_your_data_pandas_python",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/filesystem/pandas_python_example.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
        backend_dependencies=[BackendDependencies.PANDAS],
    ),
    IntegrationTestFixture(
        name="how_to_introspect_and_partition_your_data_yaml_gradual",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example_gradual.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
        backend_dependencies=[],
    ),
    IntegrationTestFixture(
        name="how_to_introspect_and_partition_your_data_yaml_complete",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example_complete.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
        backend_dependencies=[],
    ),
    IntegrationTestFixture(
        name="in_memory_pandas_python",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/in_memory/pandas_python_example.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        backend_dependencies=[BackendDependencies.PANDAS],
    ),
    IntegrationTestFixture(
        name="docusaurus_template_script_example",
        user_flow_script="tests/integration/docusaurus/template/script_example.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        backend_dependencies=[],
    ),
    IntegrationTestFixture(
        name="how_to_choose_which_dataconnector_to_use",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/how_to_choose_which_dataconnector_to_use.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/dataconnector_docs",
        backend_dependencies=[],
    ),
    IntegrationTestFixture(
        name="how_to_configure_a_pandas_datasource",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/datasource_configuration/how_to_configure_a_pandas_datasource.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/samples_2020",
        backend_dependencies=[BackendDependencies.PANDAS],
    ),
    IntegrationTestFixture(
        name="how_to_configure_a_runtimedataconnector",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/how_to_configure_a_runtimedataconnector.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/dataconnector_docs",
        backend_dependencies=[],
    ),
    IntegrationTestFixture(
        name="auto_initializing_expect_column_mean_to_be_between",
        user_flow_script="tests/integration/docusaurus/expectations/auto_initializing_expectations/auto_initializing_expect_column_mean_to_be_between.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples",
        backend_dependencies=[],
    ),
    IntegrationTestFixture(
        name="is_expectation_auto_initializing",
        user_flow_script="tests/integration/docusaurus/expectations/auto_initializing_expectations/is_expectation_auto_initializing.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        backend_dependencies=[],
    ),
    IntegrationTestFixture(
        name="expect_column_max_to_be_between_custom",
        user_flow_script="tests/integration/docusaurus/expectations/creating_custom_expectations/expect_column_max_to_be_between_custom.py",
        backend_dependencies=[],
    ),
    IntegrationTestFixture(
        name="expect_column_values_to_be_in_solfege_scale_set",
        user_flow_script="tests/integration/docusaurus/expectations/creating_custom_expectations/expect_column_values_to_be_in_solfege_scale_set.py",
        backend_dependencies=[],
    ),
    IntegrationTestFixture(
        name="expect_column_values_to_only_contain_vowels",
        user_flow_script="tests/integration/docusaurus/expectations/creating_custom_expectations/expect_column_values_to_only_contain_vowels.py",
        backend_dependencies=[],
    ),
    IntegrationTestFixture(
        name="how_to_configure_result_format_parameter",
        user_flow_script="tests/integration/docusaurus/reference/core_concepts/result_format.py",
        backend_dependencies=[],
    ),
    IntegrationTestFixture(
        name="how_to_create_and_edit_expectations_with_instant_feedback_block_config",
        user_flow_script="tests/integration/docusaurus/validation/validator/how_to_create_and_edit_expectations_with_instant_feedback_block_config.py",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
        backend_dependencies=[],
    ),
    # Fluent Datasources
    IntegrationTestFixture(
        name="how_to_create_and_edit_expectations_with_instant_feedback_fluent",
        user_flow_script="tests/integration/docusaurus/validation/validator/how_to_create_and_edit_expectations_with_instant_feedback_fluent.py",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
        backend_dependencies=[],
    ),
    IntegrationTestFixture(
        name="how_to_create_an_expectation_suite_with_the_onboarding_data_assistant",
        user_flow_script="tests/integration/docusaurus/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_onboarding_data_assistant.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples",
        backend_dependencies=[],
    ),
    IntegrationTestFixture(
        name="data_docs",
        user_flow_script="tests/integration/docusaurus/reference/glossary/data_docs.py",
        data_context_dir="tests/integration/fixtures/yellow_trip_data_fluent_pandas/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples",
        backend_dependencies=[],
    ),
    IntegrationTestFixture(
        name="how_to_edit_an_existing_expectation_suite",
        user_flow_script="tests/integration/docusaurus/expectations/how_to_edit_an_expectation_suite.py",
        backend_dependencies=[],
    ),
    IntegrationTestFixture(
        name="setup_overview",
        user_flow_script="tests/integration/docusaurus/setup/setup_overview.py",
        data_context_dir=None,
        backend_dependencies=[],
    ),
]

quickstart = [
    IntegrationTestFixture(
        name="quickstart",
        user_flow_script="tests/integration/docusaurus/tutorials/quickstart/quickstart.py",
        backend_dependencies=[],
    ),
]

fluent_datasources = [
    IntegrationTestFixture(
        name="connect_to_your_data_overview",
        data_context_dir=None,
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/connect_to_your_data_overview.py",
        backend_dependencies=[],
    ),
    IntegrationTestFixture(
        name="how_to_create_and_edit_expectations_with_a_profiler",
        data_context_dir=None,
        data_dir=None,
        user_flow_script="tests/integration/docusaurus/expectations/how_to_create_and_edit_expectations_with_a_profiler.py",
        backend_dependencies=[],
    ),
    IntegrationTestFixture(
        name="how_to_create_expectations_that_span_multiple_batches_using_evaluation_parameters",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples",
        user_flow_script="tests/integration/docusaurus/expectations/advanced/how_to_create_expectations_that_span_multiple_batches_using_evaluation_parameters.py",
        backend_dependencies=[],
    ),
    IntegrationTestFixture(
        name="how_to_pass_an_in_memory_dataframe_to_a_checkpoint",
        user_flow_script="tests/integration/docusaurus/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
        backend_dependencies=[],
    ),
    IntegrationTestFixture(
        name="rule_base_profiler_multi_batch_example",
        data_context_dir="tests/integration/fixtures/yellow_tripdata_pandas_fixture/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
        user_flow_script="tests/integration/docusaurus/expectations/advanced/multi_batch_rule_based_profiler_example.py",
        backend_dependencies=[BackendDependencies.PANDAS],
    ),
    IntegrationTestFixture(
        name="glossary_batch_request",
        data_context_dir=None,
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples",
        user_flow_script="tests/integration/docusaurus/reference/glossary/batch_request.py",
        backend_dependencies=[],
    ),
    IntegrationTestFixture(
        name="checkpoints_and_actions_core_concepts",
        user_flow_script="tests/integration/docusaurus/reference/core_concepts/checkpoints_and_actions.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
        backend_dependencies=[],
    ),
    IntegrationTestFixture(
        name="how_to_create_a_new_checkpoint",
        data_context_dir=None,
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples",
        user_flow_script="tests/integration/docusaurus/validation/checkpoints/how_to_create_a_new_checkpoint.py",
        backend_dependencies=[],
    ),
    IntegrationTestFixture(
        name="how_to_configure_a_new_checkpoint_using_test_yaml_config",
        data_context_dir="tests/integration/fixtures/yellow_tripdata_pandas_fixture/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples",
        user_flow_script="tests/integration/docusaurus/validation/checkpoints/how_to_configure_a_new_checkpoint_using_test_yaml_config.py",
        backend_dependencies=[BackendDependencies.PANDAS],
    ),
    IntegrationTestFixture(
        name="validate_data_by_running_a_checkpoint",
        user_flow_script="tests/integration/docusaurus/validation/checkpoints/how_to_validate_data_by_running_a_checkpoint.py",
        data_context_dir=None,
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
        backend_dependencies=[],
    ),
    IntegrationTestFixture(
        name="how_to_create_and_edit_an_expectation_with_domain_knowledge",
        user_flow_script="tests/integration/docusaurus/expectations/how_to_create_and_edit_an_expectationsuite_domain_knowledge.py",
        data_context_dir=None,
        backend_dependencies=[],
    ),
    IntegrationTestFixture(
        name="how_to_request_data_from_a_data_asset",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/get_existing_data_asset_from_existing_datasource_pandas_filesystem_example.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
        backend_dependencies=[BackendDependencies.PANDAS],
    ),
    IntegrationTestFixture(
        name="checkpoints_glossary",
        user_flow_script="tests/integration/docusaurus/reference/glossary/checkpoints.py",
        data_context_dir="tests/integration/fixtures/yellow_trip_data_fluent_pandas/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples",
        backend_dependencies=[BackendDependencies.PANDAS],
    ),
    IntegrationTestFixture(
        name="how_to_organize_batches_in_a_file_based_data_asset",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/organize_batches_in_pandas_filesystem_datasource.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
        backend_dependencies=[BackendDependencies.PANDAS],
    ),
    IntegrationTestFixture(
        name="how_to_organize_batches_in_a_sql_based_data_asset",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/organize_batches_in_sqlite_datasource.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        backend_dependencies=[BackendDependencies.SQLALCHEMY],
    ),
    IntegrationTestFixture(
        name="how_to_connect_to_one_or_more_files_using_pandas",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_one_or_more_files_using_pandas.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
        backend_dependencies=[BackendDependencies.PANDAS],
    ),
    IntegrationTestFixture(
        name="how_to_connect_to_sql_data_using_a_query",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_sql_data_using_a_query.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        backend_dependencies=[],
    ),
    IntegrationTestFixture(
        name="how_to_quickly_connect_to_a_single_file_with_pandas",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_quickly_connect_to_a_single_file_with_pandas.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        backend_dependencies=[BackendDependencies.PANDAS],
    ),
    IntegrationTestFixture(
        name="how_to_connect_to_sqlite_data",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_sqlite_data.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        backend_dependencies=[BackendDependencies.SQLALCHEMY],
    ),
    IntegrationTestFixture(
        name="how_to_connect_to_a_sql_table",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_a_sql_table.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        backend_dependencies=[],
    ),
    IntegrationTestFixture(
        name="how_to_connect_to_sql_data",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_sqlite_data.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        backend_dependencies=[],
    ),
    IntegrationTestFixture(
        name="how_to_instantiate_a_specific_filesystem_data_context",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_instantiate_a_specific_filesystem_data_context.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        backend_dependencies=[],
    ),
    IntegrationTestFixture(
        name="how_to_initialize_a_filesystem_data_context_in_python",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_initialize_a_filesystem_data_context_in_python.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        backend_dependencies=[],
    ),
    IntegrationTestFixture(
        name="how_to_explicitly_instantiate_an_ephemeral_data_context",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_explicitly_instantiate_an_ephemeral_data_context.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        backend_dependencies=[],
    ),
    IntegrationTestFixture(
        name="how_to_connect_to_in_memory_data_using_pandas",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_in_memory_data_using_pandas.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        backend_dependencies=[BackendDependencies.PANDAS],
    ),
]


# populate docs_test_matrix with sub-lists
docs_test_matrix += local_tests
docs_test_matrix += quickstart
docs_test_matrix += fluent_datasources
docs_test_matrix += spark_integration_tests
docs_test_matrix += sqlite_integration_tests
docs_test_matrix += mysql_integration_tests
docs_test_matrix += postgresql_integration_tests
docs_test_matrix += mssql_integration_tests
docs_test_matrix += trino_integration_tests
docs_test_matrix += snowflake_integration_tests
docs_test_matrix += redshift_integration_tests
docs_test_matrix += bigquery_integration_tests
docs_test_matrix += gcs_integration_tests
docs_test_matrix += abs_integration_tests
docs_test_matrix += s3_integration_tests
docs_test_matrix += athena_integration_tests
docs_test_matrix += aws_glue_integration_tests
docs_test_matrix += multiple_backend

pandas_integration_tests = [
    IntegrationTestFixture(
        name="pandas_one_multi_batch_request_one_validator",
        data_context_dir="tests/integration/fixtures/yellow_tripdata_pandas_fixture/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples",
        user_flow_script="tests/integration/fixtures/yellow_tripdata_pandas_fixture/one_multi_batch_request_one_validator.py",
        backend_dependencies=[BackendDependencies.PANDAS],
    ),
    IntegrationTestFixture(
        name="pandas_two_batch_requests_two_validators",
        data_context_dir="tests/integration/fixtures/yellow_tripdata_pandas_fixture/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples",
        user_flow_script="tests/integration/fixtures/yellow_tripdata_pandas_fixture/two_batch_requests_two_validators.py",
        backend_dependencies=[BackendDependencies.PANDAS],
    ),
    IntegrationTestFixture(
        name="pandas_multiple_batch_requests_one_validator_multiple_steps",
        data_context_dir="tests/integration/fixtures/yellow_tripdata_pandas_fixture/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples",
        user_flow_script="tests/integration/fixtures/yellow_tripdata_pandas_fixture/multiple_batch_requests_one_validator_multiple_steps.py",
        backend_dependencies=[BackendDependencies.PANDAS],
    ),
    IntegrationTestFixture(
        name="pandas_multiple_batch_requests_one_validator_one_step",
        data_context_dir="tests/integration/fixtures/yellow_tripdata_pandas_fixture/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples",
        user_flow_script="tests/integration/fixtures/yellow_tripdata_pandas_fixture/multiple_batch_requests_one_validator_one_step.py",
        backend_dependencies=[BackendDependencies.PANDAS],
    ),
    IntegrationTestFixture(
        name="pandas_execution_engine_with_gcp_installed",
        data_context_dir="tests/integration/fixtures/yellow_tripdata_pandas_fixture/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples",
        user_flow_script="tests/integration/common_workflows/pandas_execution_engine_with_gcp_installed.py",
        other_files=(
            (
                "tests/integration/fixtures/cloud_provider_configs/gcp/my_example_creds.json",
                ".gcs/my_example_creds.json",
            ),
        ),
        backend_dependencies=[BackendDependencies.PANDAS],
    ),
]

# populate integration_test_matrix with sub-lists
integration_test_matrix: List[IntegrationTestFixture] = []
integration_test_matrix += pandas_integration_tests


def idfn(test_configuration):
    return test_configuration.name


@pytest.fixture
def pytest_parsed_arguments(request):
    return request.config.option


@pytest.mark.docs
@pytest.mark.integration
@pytest.mark.parametrize("integration_test_fixture", docs_test_matrix, ids=idfn)
@pytest.mark.skipif(sys.version_info < (3, 7), reason="requires Python3.7")
def test_docs(integration_test_fixture, tmp_path, pytest_parsed_arguments):
    _check_for_skipped_tests(pytest_parsed_arguments, integration_test_fixture)
    _execute_integration_test(integration_test_fixture, tmp_path)


@pytest.mark.integration
@pytest.mark.parametrize("test_configuration", integration_test_matrix, ids=idfn)
@pytest.mark.skipif(sys.version_info < (3, 7), reason="requires Python3.7")
@pytest.mark.slow  # 79.77s
def test_integration_tests(test_configuration, tmp_path, pytest_parsed_arguments):
    _check_for_skipped_tests(pytest_parsed_arguments, test_configuration)
    _execute_integration_test(test_configuration, tmp_path)


def _execute_integration_test(  # noqa: PLR0912, PLR0915
    integration_test_fixture: IntegrationTestFixture, tmp_path: pathlib.Path
):
    """
    Prepare and environment and run integration tests from a list of tests.

    Note that the only required parameter for a test in the matrix is
    `user_flow_script` and that all other parameters are optional.
    """
    workdir = pathlib.Path.cwd()
    try:
        base_dir = pathlib.Path(file_relative_path(__file__, "../../"))
        os.chdir(base_dir)
        # Ensure GX is installed in our environment
        installed_packages = [pkg.key for pkg in pkg_resources.working_set]
        if "great-expectations" not in installed_packages:
            execute_shell_command("pip install .")
        os.chdir(tmp_path)

        #
        # Build test state
        # DataContext
        data_context_dir = integration_test_fixture.data_context_dir
        if data_context_dir:
            context_source_dir = base_dir / data_context_dir
            test_context_dir = tmp_path / "great_expectations"
            shutil.copytree(
                context_source_dir,
                test_context_dir,
            )

        # Test Data
        data_dir = integration_test_fixture.data_dir
        if data_dir:
            source_data_dir = base_dir / data_dir
            target_data_dir = tmp_path / "data"
            shutil.copytree(
                source_data_dir,
                target_data_dir,
            )

        # Other files
        # Other files to copy should be supplied as a tuple of tuples with source, dest pairs
        # e.g. (("/source1/file1", "/dest1/file1"), ("/source2/file2", "/dest2/file2"))
        other_files = integration_test_fixture.other_files
        if other_files:
            for file_paths in other_files:
                source_file = base_dir / file_paths[0]
                dest_file = tmp_path / file_paths[1]
                dest_dir = dest_file.parent
                if not dest_dir.exists():
                    dest_dir.mkdir()

                shutil.copyfile(src=source_file, dst=dest_file)

        # UAT Script
        user_flow_script = integration_test_fixture.user_flow_script
        script_source = base_dir / user_flow_script

        script_path = tmp_path / "test_script.py"
        shutil.copyfile(script_source, script_path)
        logger.debug(
            f"(_execute_integration_test) script_source -> {script_source} :: copied to {script_path}"
        )
        if script_source.suffix != ".py":
            logger.error(f"{script_source} is not a python script!")
            text = script_source.read_text()
            print(f"contents of script_path:\n\n{text}\n\n")
            return

        util_script = integration_test_fixture.util_script
        if util_script:
            script_source = base_dir / util_script
            tmp_path.joinpath("tests/").mkdir()
            util_script_path = tmp_path / "tests/test_utils.py"
            shutil.copyfile(script_source, util_script_path)

        # Run script as module, using python's importlib machinery (https://docs.python.org/3/library/importlib.htm)
        loader = importlib.machinery.SourceFileLoader(
            "test_script_module", str(script_path)
        )
        spec = importlib.util.spec_from_loader("test_script_module", loader)
        test_script_module = importlib.util.module_from_spec(spec)
        loader.exec_module(test_script_module)
    except Exception as e:
        logger.error(str(e))
        if "JavaPackage" in str(e) and "aws_glue" in user_flow_script:
            logger.debug("This is something aws_glue related, so just going to return")
            # Should try to copy aws-glue-libs jar files to Spark jar during pipeline setup
            #   - see https://stackoverflow.com/a/67371827
            return
        else:
            raise
    finally:
        os.chdir(workdir)


def _check_for_skipped_tests(  # noqa: PLR0912
    pytest_args,
    integration_test_fixture,
) -> None:
    """Enable scripts to be skipped based on pytest invocation flags."""
    dependencies = integration_test_fixture.backend_dependencies
    if not dependencies:
        return
    elif BackendDependencies.POSTGRESQL in dependencies and (
        not pytest_args.postgresql or pytest_args.no_sqlalchemy
    ):
        pytest.skip("Skipping postgres tests")
    elif BackendDependencies.MYSQL in dependencies and (
        not pytest_args.mysql or pytest_args.no_sqlalchemy
    ):
        pytest.skip("Skipping mysql tests")
    elif BackendDependencies.MSSQL in dependencies and (
        not pytest_args.mssql or pytest_args.no_sqlalchemy
    ):
        pytest.skip("Skipping mssql tests")
    elif BackendDependencies.BIGQUERY in dependencies and (
        pytest_args.no_sqlalchemy or not pytest_args.bigquery
    ):
        # TODO : Investigate whether this test should be handled by azure-pipelines-cloud-db-integration.yml
        pytest.skip("Skipping bigquery tests")
    elif BackendDependencies.GCS in dependencies and not pytest_args.bigquery:
        # TODO : Investigate whether this test should be handled by azure-pipelines-cloud-db-integration.yml
        pytest.skip("Skipping GCS tests")
    elif BackendDependencies.AWS in dependencies and not pytest_args.aws:
        pytest.skip("Skipping AWS tests")
    elif BackendDependencies.REDSHIFT in dependencies and (
        pytest_args.no_sqlalchemy or not pytest_args.redshift
    ):
        pytest.skip("Skipping redshift tests")
    elif BackendDependencies.SPARK in dependencies and not pytest_args.spark:
        pytest.skip("Skipping spark tests")
    elif BackendDependencies.SNOWFLAKE in dependencies and (
        pytest_args.no_sqlalchemy or not pytest_args.snowflake
    ):
        pytest.skip("Skipping snowflake tests")
    elif BackendDependencies.AZURE in dependencies and not pytest_args.azure:
        pytest.skip("Skipping Azure tests")
    elif BackendDependencies.TRINO in dependencies and not pytest_args.trino:
        pytest.skip("Skipping Trino tests")
    elif BackendDependencies.ATHENA in dependencies and not pytest_args.athena:
        pytest.skip("Skipping Athena tests")
