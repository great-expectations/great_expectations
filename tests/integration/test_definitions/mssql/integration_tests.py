from tests.integration.backend_dependencies import BackendDependencies
from tests.integration.integration_test_fixture import IntegrationTestFixture

mssql_integration_tests = []

split_data = [
    IntegrationTestFixture(
        name="split_data_on_whole_table_mssql",
        user_flow_script="tests/integration/db/test_sql_data_split_on_whole_table.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
        util_script="tests/test_utils.py",
        other_files=(
            (
                "tests/integration/fixtures/split_and_sample_data/mssql_connection_string.yml",
                "connection_string.yml",
            ),
        ),
        backend_dependencies=[BackendDependencies.MSSQL],
    ),
    IntegrationTestFixture(
        name="split_data_on_column_value_mssql",
        user_flow_script="tests/integration/db/test_sql_data_split_on_column_value.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
        util_script="tests/test_utils.py",
        other_files=(
            (
                "tests/integration/fixtures/split_and_sample_data/mssql_connection_string.yml",
                "connection_string.yml",
            ),
        ),
        backend_dependencies=[BackendDependencies.MSSQL],
    ),
    IntegrationTestFixture(
        name="split_data_on_divided_integer_mssql",
        user_flow_script="tests/integration/db/test_sql_data_split_on_divided_integer.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
        util_script="tests/test_utils.py",
        other_files=(
            (
                "tests/integration/fixtures/split_and_sample_data/mssql_connection_string.yml",
                "connection_string.yml",
            ),
        ),
        backend_dependencies=[BackendDependencies.MSSQL],
    ),
    IntegrationTestFixture(
        name="split_data_on_mod_integer_mssql",
        user_flow_script="tests/integration/db/test_sql_data_split_on_mod_integer.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
        util_script="tests/test_utils.py",
        other_files=(
            (
                "tests/integration/fixtures/split_and_sample_data/mssql_connection_string.yml",
                "connection_string.yml",
            ),
        ),
        backend_dependencies=[BackendDependencies.MSSQL],
    ),
    # TODO: <Alex>ALEX -- Uncomment next statement when "split_on_hashed_column" for MSSQL is implemented.</Alex>
    # IntegrationTestFixture(
    #     name="split_data_on_hashed_column_mssql",
    #     user_flow_script="tests/integration/db/test_sql_data_split_on_hashed_column.py",
    #     data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
    #     data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
    #     util_script="tests/test_utils.py",
    #     other_files=(
    #         (
    #             "tests/integration/fixtures/split_and_sample_data/mssql_connection_string.yml",
    #             "connection_string.yml",
    #         ),
    #     ),
    #     backend_dependencies=[BackendDependencies.MSSQL],
    # ),
    IntegrationTestFixture(
        name="split_data_on_multi_column_values_mssql",
        user_flow_script="tests/integration/db/test_sql_data_split_on_multi_column_values.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
        util_script="tests/test_utils.py",
        other_files=(
            (
                "tests/integration/fixtures/split_and_sample_data/mssql_connection_string.yml",
                "connection_string.yml",
            ),
        ),
        backend_dependencies=[BackendDependencies.MSSQL],
    ),
    IntegrationTestFixture(
        name="split_data_on_datetime_mssql",
        user_flow_script="tests/integration/db/test_sql_data_split_on_datetime_and_day_part.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
        util_script="tests/test_utils.py",
        other_files=(
            (
                "tests/integration/fixtures/split_and_sample_data/mssql_connection_string.yml",
                "connection_string.yml",
            ),
        ),
        backend_dependencies=[BackendDependencies.MSSQL],
    ),
    # TODO: <Alex>ALEX -- Uncomment next statement when "split_on_converted_datetime" for MSSQL is implemented.</Alex>
    # IntegrationTestFixture(
    #     name="split_data_on_converted_datetime_mssql",
    #     user_flow_script="tests/integration/db/test_sql_data_split_on_converted_datetime.py",
    #     data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
    #     data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
    #     util_script="tests/test_utils.py",
    #     other_files=(
    #         (
    #             "tests/integration/fixtures/split_and_sample_data/mssql_connection_string.yml",
    #             "connection_string.yml",
    #         ),
    #     ),
    #     backend_dependencies=[BackendDependencies.MSSQL],
    # ),
]

sample_data = [
    IntegrationTestFixture(
        name="sample_data_using_limit_mssql",
        user_flow_script="tests/integration/db/test_sql_data_sampling.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
        util_script="tests/test_utils.py",
        other_files=(
            (
                "tests/integration/fixtures/split_and_sample_data/mssql_connection_string.yml",
                "connection_string.yml",
            ),
        ),
        backend_dependencies=[BackendDependencies.MSSQL],
    ),
]

mssql_integration_tests += split_data
mssql_integration_tests += sample_data
