"""
This file contains the integration test fixtures for documentation example scripts that are
under CI test.
"""

from tests.integration.backend_dependencies import BackendDependencies
from tests.integration.integration_test_fixture import IntegrationTestFixture

docs_tests = []

connect_to_filesystem_data_create_a_data_source = [
    # Local, pandas/spark
    IntegrationTestFixture(
        # To test, run:
        # pytest --docs-tests -k "create_a_datasource_filesystem_local_pandas" tests/integration/test_script_runner.py
        name="create_a_datasource_filesystem_local_pandas",
        user_flow_script="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_data_source/_local_or_networked/_pandas.py",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
        backend_dependencies=[],
    ),
    IntegrationTestFixture(
        # To test, run:
        # pytest --docs-tests --spark -k "create_a_datasource_filesystem_local_spark" tests/integration/test_script_runner.py
        name="create_a_datasource_filesystem_local_spark",
        user_flow_script="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_data_source/_local_or_networked/_spark.py",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
        backend_dependencies=[BackendDependencies.SPARK],
    ),
    # ABS, pandas/spark
    IntegrationTestFixture(
        # To test, run:
        # pytest --docs-tests --azure -k "create_a_datasource_filesystem_abs_pandas" tests/integration/test_script_runner.py
        name="create_a_datasource_filesystem_abs_pandas",
        user_flow_script="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_data_source/_abs/_pandas.py",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
        backend_dependencies=[BackendDependencies.AZURE],
    ),
    IntegrationTestFixture(
        # To test, run:
        # pytest --docs-tests --azure --spark -k "create_a_datasource_filesystem_abs_spark" tests/integration/test_script_runner.py
        name="create_a_datasource_filesystem_abs_spark",
        user_flow_script="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_data_source/_abs/_spark.py",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
        backend_dependencies=[BackendDependencies.AZURE, BackendDependencies.SPARK],
    ),
    # # GCS, pandas/spark
    # IntegrationTestFixture(
    #     name="create_a_datasource_filesystem_gcs_pandas",
    #     user_flow_script="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_data_source/_gcs/_spark.py",
    #     data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
    # ),
    # IntegrationTestFixture(
    #     name="create_a_datasource_filesystem_gcs_spark",
    #     user_flow_script="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_data_source/_gcs/_spark.py",
    #     data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
    # ),
    # # S3, pandas/spark
    # IntegrationTestFixture(
    #     name="create_a_datasource_filesystem_s3_pandas",
    #     user_flow_script="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_data_source/_s3/_spark.py",
    #     data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
    # ),
    # IntegrationTestFixture(
    #     name="create_a_datasource_filesystem_s3_spark",
    #     user_flow_script="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_data_source/_s3/_spark.py",
    #     data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
    # ),
]

# TODO: As we get these example tests working, uncomment/update to add them to CI.
connecting_to_a_datasource = [
    # # Create a Data Source
    # IntegrationTestFixture(
    #     name="create_a_datasource_postgres",
    #     user_flow_script="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_data_source/_local_or_networked/_spark.py",
    #     data_context_dir="docs/docusaurus/docs/components/_testing/create_datasource/great_expectations/",
    #     data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
    #     util_script="tests/test_utils.py",
    #     other_files=(
    #         (
    #             "tests/integration/fixtures/partition_and_sample_data/postgres_connection_string.yml",
    #             "connection_string.yml",
    #         ),
    #     ),
    #     backend_dependencies=[BackendDependencies.POSTGRESQL],
    # ),
    # # Create a Data Asset
    # IntegrationTestFixture(
    #     name="create_a_data_asset_postgres",
    #     user_flow_script="docs/docusaurus/docs/core/connect_to_data/sql_data/_create_a_data_asset/create_a_data_asset.py",
    #     data_context_dir="docs/docusaurus/docs/components/_testing/create_datasource/great_expectations/",
    #     data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
    #     util_script="tests/test_utils.py",
    #     other_files=(
    #         (
    #             "tests/integration/fixtures/partition_and_sample_data/postgres_connection_string.yml",
    #             "connection_string.yml",
    #         ),
    #     ),
    #     backend_dependencies=[BackendDependencies.POSTGRESQL],
    # ),
    # # Create a Batch Definition
    # IntegrationTestFixture(
    #     name="create_a_batch_definition_postgres",
    #     user_flow_script="docs/docusaurus/docs/core/connect_to_data/sql_data/_create_a_batch_definition/create_a_batch_definition.py",
    #     data_context_dir="docs/docusaurus/docs/components/_testing/create_datasource/great_expectations/",
    #     data_dir="tests/test_sets/taxi_yellow_tripdata_samples/samples_2020",
    #     util_script="tests/test_utils.py",
    #     other_files=(
    #         (
    #             "tests/integration/fixtures/partition_and_sample_data/postgres_connection_string.yml",
    #             "connection_string.yml",
    #         ),
    #     ),
    #     backend_dependencies=[BackendDependencies.POSTGRESQL],
    # ),
]

# TODO: As we get these example tests working, uncomment/update to add them to CI.
expectation_tests = [
    # # Expectation example scripts
    # IntegrationTestFixture(
    #     name="create_an_expectation.py",
    #     user_flow_script="docs/docusaurus/docs/core/_create_expectations/expectations/_examples/create_an_expectation.py",
    #     backend_dependencies=[],
    # ),
    # IntegrationTestFixture(
    #     name="edit_an_expectation.py",
    #     user_flow_script="docs/docusaurus/docs/core/_create_expectations/expectations/_examples/edit_an_expectation.py",
    #     backend_dependencies=[],
    # ),
    # # Expectation Suite example scripts
    # IntegrationTestFixture(
    #     name="add_expectations_to_an_expectation_suite.py",
    #     user_flow_script="docs/docusaurus/docs/core/_create_expectations/expectation_suites/_examples/add_expectations_to_an_expectation_suite.py",
    #     backend_dependencies=[],
    # ),
    # IntegrationTestFixture(
    #     name="create_an_expectation_suite.py",
    #     user_flow_script="docs/docusaurus/docs/core/_create_expectations/expectation_suites/_examples/create_an_expectation_suite.py",
    #     backend_dependencies=[],
    # ),
    # IntegrationTestFixture(
    #     name="delete_an_expectation_in_an_expectation_suite.py",
    #     user_flow_script="docs/docusaurus/docs/core/_create_expectations/expectation_suites/_examples/delete_an_expectation_in_an_expectation_suite.py",
    #     backend_dependencies=[],
    # ),
    # IntegrationTestFixture(
    #     name="delete_an_expectation_suite",
    #     user_flow_script="docs/docusaurus/docs/core/_create_expectations/expectation_suites/_examples/delete_an_expectation_suite.py",
    #     backend_dependencies=[],
    # ),
    # IntegrationTestFixture(
    #     name="edit_a_single_expectation.py",
    #     user_flow_script="docs/docusaurus/docs/core/_create_expectations/expectation_suites/_examples/edit_a_single_expectation.py",
    #     backend_dependencies=[],
    # ),
    # IntegrationTestFixture(
    #     name="edit_all_expectations_in_an_expectation_suite.py",
    #     user_flow_script="docs/docusaurus/docs/core/_create_expectations/expectation_suites/_examples/edit_all_expectations_in_an_expectation_suite.py",
    #     backend_dependencies=[],
    # ),
    # IntegrationTestFixture(
    #     name="get_a_specific_expectation_from_an_expectation_suite.py",
    #     user_flow_script="docs/docusaurus/docs/core/_create_expectations/expectation_suites/_examples/get_a_specific_expectation_from_an_expectation_suite.py",
    #     backend_dependencies=[],
    # ),
    # IntegrationTestFixture(
    #     name="get_an_existing_expectation_suite.py",
    #     user_flow_script="docs/docusaurus/docs/core/_create_expectations/expectation_suites/_examples/get_an_existing_expectation_suite.py",
    #     backend_dependencies=[],
    # ),
]


learn_data_quality_use_cases = [
    # Schema.
    IntegrationTestFixture(
        name="data_quality_use_case_schema_expectations",
        user_flow_script="docs/docusaurus/docs/reference/learn/data_quality_use_cases/schema_resources/schema_expectations.py",
        data_dir="tests/test_sets/learn_data_quality_use_cases/",
        util_script="tests/test_utils.py",
        backend_dependencies=[BackendDependencies.POSTGRESQL],
    ),
    IntegrationTestFixture(
        name="data_quality_use_case_schema_validation_over_time",
        user_flow_script="docs/docusaurus/docs/reference/learn/data_quality_use_cases/schema_resources/schema_validation_over_time.py",
        data_dir="tests/test_sets/learn_data_quality_use_cases/",
        util_script="tests/test_utils.py",
        backend_dependencies=[BackendDependencies.POSTGRESQL],
    ),
    IntegrationTestFixture(
        name="data_quality_use_case_schema_strict_and_relaxed_validation",
        user_flow_script="docs/docusaurus/docs/reference/learn/data_quality_use_cases/schema_resources/schema_strict_and_relaxed.py",
        data_dir="tests/test_sets/learn_data_quality_use_cases/",
        util_script="tests/test_utils.py",
        backend_dependencies=[BackendDependencies.POSTGRESQL],
    ),
]

# Extend the docs_tests list with the above sublists (only the docs_tests list is imported
# into `test_script_runner.py` and actually used in CI checks).
docs_tests.extend(connect_to_filesystem_data_create_a_data_source)
docs_tests.extend(connecting_to_a_datasource)
docs_tests.extend(learn_data_quality_use_cases)
docs_tests.extend(expectation_tests)
