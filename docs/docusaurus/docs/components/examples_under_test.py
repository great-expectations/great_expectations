from tests.integration.backend_dependencies import BackendDependencies
from tests.integration.integration_test_fixture import IntegrationTestFixture

docs_tests = []

connecting_to_a_datasource = [
    # PostgreSQL
    IntegrationTestFixture(
        name="create_a_datasource_postgres",
        user_flow_script="docs/docusaurus/docs/core/connect_to_data/sql_data/_create_a_data_source/postgres.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
        util_script="tests/test_utils.py",
        other_files=(
            (
                "tests/integration/fixtures/partition_and_sample_data/postgres_connection_string.yml",
                "connection_string.yml",
            ),
            ("uncomitted/config_variables.yml", "uncomitted/config_variables.yml"),
        ),
        backend_dependencies=[BackendDependencies.POSTGRESQL],
    ),
]


docs_tests.extend(connecting_to_a_datasource)
