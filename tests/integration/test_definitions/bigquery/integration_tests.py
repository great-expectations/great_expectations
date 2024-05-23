from typing import List

from tests.integration.backend_dependencies import BackendDependencies
from tests.integration.integration_test_fixture import IntegrationTestFixture

bigquery_integration_tests = []

connecting_to_your_data: List[IntegrationTestFixture] = []

partition_data = [
    IntegrationTestFixture(
        name="partition_data_on_whole_table_bigquery",
        user_flow_script="tests/integration/db/test_sql_data_partitioned_on_whole_table.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
        util_script="tests/test_utils.py",
        other_files=(
            (
                "tests/integration/fixtures/partition_and_sample_data/bigquery_connection_string.yml",
                "connection_string.yml",
            ),
        ),
        backend_dependencies=[BackendDependencies.BIGQUERY],
    ),
    IntegrationTestFixture(
        name="partition_data_on_datetime_bigquery",
        user_flow_script="tests/integration/db/test_sql_data_partitioned_on_datetime_and_day_part.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
        util_script="tests/test_utils.py",
        other_files=(
            (
                "tests/integration/fixtures/partition_and_sample_data/bigquery_connection_string.yml",
                "connection_string.yml",
            ),
        ),
        backend_dependencies=[BackendDependencies.BIGQUERY],
    ),
]

sample_data: List[IntegrationTestFixture] = []

deployment_patterns = [
    IntegrationTestFixture(
        name="deployment_patterns_file_bigquery",
        user_flow_script="docs/docusaurus/docs/oss/guides/connecting_to_your_data/fluent/database/gcp_deployment_patterns_file_bigquery.py",
        data_context_dir=None,
        backend_dependencies=[BackendDependencies.BIGQUERY],
    ),
]

runtime_parameters: List[IntegrationTestFixture] = []

bigquery_integration_tests += connecting_to_your_data
bigquery_integration_tests += partition_data
bigquery_integration_tests += sample_data
bigquery_integration_tests += deployment_patterns
bigquery_integration_tests += runtime_parameters
