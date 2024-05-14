from typing import List

from tests.integration.backend_dependencies import BackendDependencies
from tests.integration.integration_test_fixture import IntegrationTestFixture

mysql_integration_tests = []

connecting_to_your_data: List[IntegrationTestFixture] = []

partition_data = [
    IntegrationTestFixture(
        name="partition_data_on_whole_table_mysql",
        user_flow_script="tests/integration/db/test_sql_data_partitioned_on_whole_table.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
        util_script="tests/test_utils.py",
        other_files=(
            (
                "tests/integration/fixtures/partition_and_sample_data/mysql_connection_string.yml",
                "connection_string.yml",
            ),
        ),
        backend_dependencies=[BackendDependencies.MYSQL],
    ),
    IntegrationTestFixture(
        name="partition_data_on_datetime_mysql",
        user_flow_script="tests/integration/db/test_sql_data_partitioned_on_datetime_and_day_part.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
        util_script="tests/test_utils.py",
        other_files=(
            (
                "tests/integration/fixtures/partition_and_sample_data/mysql_connection_string.yml",
                "connection_string.yml",
            ),
        ),
        backend_dependencies=[BackendDependencies.MYSQL],
    ),
]

sample_data: List[IntegrationTestFixture] = []

mysql_integration_tests += connecting_to_your_data
mysql_integration_tests += partition_data
mysql_integration_tests += sample_data
