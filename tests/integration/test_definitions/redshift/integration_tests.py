from tests.integration.backend_dependencies import BackendDependencies
from tests.integration.integration_test_fixture import IntegrationTestFixture

redshift_integration_tests = []

connecting_to_your_data = [
    # TODO: <Alex>ALEX: Rename test modules to include "configured" and "inferred_and_runtime" suffixes in names.</Alex>  # noqa: E501
    # IntegrationTestFixture(
    #     name = "redshift_python_example",
    #     user_flow_script= "docs/docusaurus/docs/snippets/redshift_python_example.py",
    #     data_context_dir= "tests/integration/fixtures/no_datasources/great_expectations",
    #     data_dir= "tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
    #     backend_dependencies=[ [BackendDependencies.AWS, BackendDependencies.REDSHIFT]],
    #     util_script= "tests/test_utils.py",
    # ),
    # IntegrationTestFixture(
    #     name = "redshift_yaml_example",
    #     user_flow_script= "docs/docusaurus/docs/snippets/redshift_yaml_example.py",
    #     data_context_dir= "tests/integration/fixtures/no_datasources/great_expectations",
    #     data_dir= "tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
    #     backend_dependencies=[ [BackendDependencies.AWS, BackendDependencies.REDSHIFT]],
    #     util_script= "tests/test_utils.py",
    # ),
]

partition_data = [
    IntegrationTestFixture(
        name="partition_data_on_whole_table_redshift",
        user_flow_script="tests/integration/db/test_sql_data_partitioned_on_whole_table.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
        util_script="tests/test_utils.py",
        other_files=(
            (
                "tests/integration/fixtures/partition_and_sample_data/redshift_connection_string.yml",
                "connection_string.yml",
            ),
        ),
        backend_dependencies=[BackendDependencies.REDSHIFT],
    ),
    IntegrationTestFixture(
        name="partition_data_on_datetime_redshift",
        user_flow_script="tests/integration/db/test_sql_data_partitioned_on_datetime_and_day_part.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
        util_script="tests/test_utils.py",
        other_files=(
            (
                "tests/integration/fixtures/partition_and_sample_data/redshift_connection_string.yml",
                "connection_string.yml",
            ),
        ),
        backend_dependencies=[BackendDependencies.REDSHIFT],
    ),
]

sample_data = []

deployment_patterns = [
    IntegrationTestFixture(
        name="deployment_patterns_redshift",
        user_flow_script="docs/docusaurus/docs/snippets/aws_redshift_deployment_patterns.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
        backend_dependencies=[BackendDependencies.REDSHIFT],
    ),
]

redshift_integration_tests += connecting_to_your_data
redshift_integration_tests += partition_data
redshift_integration_tests += sample_data
redshift_integration_tests += deployment_patterns
