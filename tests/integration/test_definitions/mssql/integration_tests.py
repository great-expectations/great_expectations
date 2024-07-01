from typing import List

from tests.integration.backend_dependencies import BackendDependencies
from tests.integration.integration_test_fixture import IntegrationTestFixture

mssql_integration_tests = []

partition_data = [
    IntegrationTestFixture(
        name="partition_data_on_whole_table_mssql",
        user_flow_script="tests/integration/db/test_sql_data_partitioned_on_whole_table.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
        util_script="tests/test_utils.py",
        other_files=(
            (
                "tests/integration/fixtures/partition_and_sample_data/mssql_connection_string.yml",
                "connection_string.yml",
            ),
        ),
        backend_dependencies=[BackendDependencies.MSSQL],
    ),
    IntegrationTestFixture(
        name="partition_data_on_datetime_mssql",
        user_flow_script="tests/integration/db/test_sql_data_partitioned_on_datetime_and_day_part.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
        util_script="tests/test_utils.py",
        other_files=(
            (
                "tests/integration/fixtures/partition_and_sample_data/mssql_connection_string.yml",
                "connection_string.yml",
            ),
        ),
        backend_dependencies=[BackendDependencies.MSSQL],
    ),
]

sample_data: List[IntegrationTestFixture] = []

mssql_integration_tests += partition_data
mssql_integration_tests += sample_data
