from typing import List

from tests.integration.backend_dependencies import BackendDependencies
from tests.integration.integration_test_fixture import IntegrationTestFixture

postgresql_integration_tests = []


connecting_to_your_data = [
    IntegrationTestFixture(
        name="how_to_configure_credentials",
        user_flow_script="docs/docusaurus/docs/oss/guides/setup/configuring_data_contexts/how_to_configure_credentials.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
        backend_dependencies=[BackendDependencies.POSTGRESQL],
    ),
]

partition_data = [
    IntegrationTestFixture(
        name="partition_data_on_whole_table_postgres",
        user_flow_script="tests/integration/db/test_sql_data_partitioned_on_whole_table.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
        util_script="tests/test_utils.py",
        other_files=(
            (
                "tests/integration/fixtures/partition_and_sample_data/postgres_connection_string.yml",
                "connection_string.yml",
            ),
        ),
        backend_dependencies=[BackendDependencies.POSTGRESQL],
    ),
    IntegrationTestFixture(
        name="partition_data_on_datetime_postgres",
        user_flow_script="tests/integration/db/test_sql_data_partitioned_on_datetime_and_day_part.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
        util_script="tests/test_utils.py",
        other_files=(
            (
                "tests/integration/fixtures/partition_and_sample_data/postgres_connection_string.yml",
                "connection_string.yml",
            ),
        ),
        backend_dependencies=[BackendDependencies.POSTGRESQL],
    ),
]

sample_data: List[IntegrationTestFixture] = []


fluent_datasources = [
    IntegrationTestFixture(
        name="how_to_connect_to_postgresql_data",
        user_flow_script="docs/docusaurus/docs/oss/guides/connecting_to_your_data/fluent/database/how_to_connect_to_postgresql_data.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        backend_dependencies=[BackendDependencies.POSTGRESQL],
    ),
]

getting_started = []

unexpected_row_expectation = [
    IntegrationTestFixture(
        name="how_to_write_an_unexpected_row_expectation",
        user_flow_script="docs/docusaurus/docs/snippets/unexpected_row_expectation.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
        util_script="tests/test_utils.py",
        backend_dependencies=[BackendDependencies.POSTGRESQL],
    )
]

postgresql_integration_tests += connecting_to_your_data
postgresql_integration_tests += partition_data
postgresql_integration_tests += sample_data
postgresql_integration_tests += fluent_datasources
postgresql_integration_tests += getting_started
postgresql_integration_tests += unexpected_row_expectation
