from tests.integration.backend_dependencies import BackendDependencies
from tests.integration.integration_test_fixture import IntegrationTestFixture

mysql_integration_tests = []

connecting_to_your_data = [
    IntegrationTestFixture(
        name="mysql_python_example",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/database/mysql_python_example.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
        util_script="tests/test_utils.py",
        backend_dependencies=[BackendDependencies.MYSQL],
    ),
]

split_data = [
    IntegrationTestFixture(
        name="split_data_on_whole_table_mysql",
        user_flow_script="tests/integration/db/test_sql_data_split_on_whole_table.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
        util_script="tests/test_utils.py",
        other_files=(
            (
                "tests/integration/fixtures/split_and_sample_data/mysql_connection_string.yml",
                "connection_string.yml",
            ),
        ),
        backend_dependencies=[BackendDependencies.MYSQL],
    ),
    IntegrationTestFixture(
        name="split_data_on_column_value_mysql",
        user_flow_script="tests/integration/db/test_sql_data_split_on_column_value.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
        util_script="tests/test_utils.py",
        other_files=(
            (
                "tests/integration/fixtures/split_and_sample_data/mysql_connection_string.yml",
                "connection_string.yml",
            ),
        ),
        backend_dependencies=[BackendDependencies.MYSQL],
    ),
    IntegrationTestFixture(
        name="split_data_on_divided_integer_mysql",
        user_flow_script="tests/integration/db/test_sql_data_split_on_divided_integer.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
        util_script="tests/test_utils.py",
        other_files=(
            (
                "tests/integration/fixtures/split_and_sample_data/mysql_connection_string.yml",
                "connection_string.yml",
            ),
        ),
        backend_dependencies=[BackendDependencies.MYSQL],
    ),
    IntegrationTestFixture(
        name="split_data_on_mod_integer_mysql",
        user_flow_script="tests/integration/db/test_sql_data_split_on_mod_integer.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
        util_script="tests/test_utils.py",
        other_files=(
            (
                "tests/integration/fixtures/split_and_sample_data/mysql_connection_string.yml",
                "connection_string.yml",
            ),
        ),
        backend_dependencies=[BackendDependencies.MYSQL],
    ),
    # TODO: <Alex>ALEX -- Uncomment next statement when "split_on_hashed_column" for MYSQL is implemented.</Alex>
    # IntegrationTestFixture(
    #     name="split_data_on_hashed_column_mysql",
    #     user_flow_script="tests/integration/db/test_sql_data_split_on_hashed_column.py",
    #     data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
    #     data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
    #     util_script="tests/test_utils.py",
    #     other_files=(
    #         (
    #             "tests/integration/fixtures/split_and_sample_data/mysql_connection_string.yml",
    #             "connection_string.yml",
    #         ),
    #     ),
    #     backend_dependencies=[BackendDependencies.MYSQL],
    # ),
    IntegrationTestFixture(
        name="split_data_on_multi_column_values_mysql",
        user_flow_script="tests/integration/db/test_sql_data_split_on_multi_column_values.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
        util_script="tests/test_utils.py",
        other_files=(
            (
                "tests/integration/fixtures/split_and_sample_data/mysql_connection_string.yml",
                "connection_string.yml",
            ),
        ),
        backend_dependencies=[BackendDependencies.MYSQL],
    ),
    IntegrationTestFixture(
        name="split_data_on_datetime_mysql",
        user_flow_script="tests/integration/db/test_sql_data_split_on_datetime_and_day_part.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
        util_script="tests/test_utils.py",
        other_files=(
            (
                "tests/integration/fixtures/split_and_sample_data/mysql_connection_string.yml",
                "connection_string.yml",
            ),
        ),
        backend_dependencies=[BackendDependencies.MYSQL],
    ),
    # TODO: <Alex>ALEX -- Uncomment next statement when "split_on_converted_datetime" for MYSQL is implemented.</Alex>
    # IntegrationTestFixture(
    #     name="split_data_on_converted_datetime_mysql",
    #     user_flow_script="tests/integration/db/test_sql_data_split_on_converted_datetime.py",
    #     data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
    #     data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
    #     util_script="tests/test_utils.py",
    #     other_files=(
    #         (
    #             "tests/integration/fixtures/split_and_sample_data/mysql_connection_string.yml",
    #             "connection_string.yml",
    #         ),
    #     ),
    #     backend_dependencies=[BackendDependencies.MYSQL],
    # ),
]

sample_data = [
    IntegrationTestFixture(
        name="sample_data_using_limit_mysql",
        user_flow_script="tests/integration/db/test_sql_data_sampling.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
        util_script="tests/test_utils.py",
        other_files=(
            (
                "tests/integration/fixtures/split_and_sample_data/mysql_connection_string.yml",
                "connection_string.yml",
            ),
        ),
        backend_dependencies=[BackendDependencies.MYSQL],
    ),
]

mysql_integration_tests += connecting_to_your_data
mysql_integration_tests += split_data
mysql_integration_tests += sample_data
