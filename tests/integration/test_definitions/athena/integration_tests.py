from tests.integration.backend_dependencies import BackendDependencies
from tests.integration.integration_test_fixture import IntegrationTestFixture

athena_integration_tests = []

connecting_to_your_data = [
    IntegrationTestFixture(
        name="awsathena_test_yaml",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        user_flow_script="tests/integration/db/awsathena.py",
        backend_dependencies=[BackendDependencies.AWS, BackendDependencies.ATHENA],
        util_script="tests/test_utils.py",
    ),
    IntegrationTestFixture(
        name="awsathena_test_python",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/database/athena_python_example.py",
        backend_dependencies=[BackendDependencies.AWS, BackendDependencies.ATHENA],
        util_script="tests/test_utils.py",
    ),
]

split_data = [
    IntegrationTestFixture(
        name="split_data_on_whole_table_awsathena",
        user_flow_script="tests/integration/db/test_sql_data_split_on_whole_table.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
        util_script="tests/test_utils.py",
        other_files=(
            (
                "tests/integration/fixtures/split_and_sample_data/awsathena_connection_string.yml",
                "connection_string.yml",
            ),
        ),
        backend_dependencies=[BackendDependencies.AWS, BackendDependencies.ATHENA],
    ),
    IntegrationTestFixture(
        name="split_data_on_column_value_awsathena",
        user_flow_script="tests/integration/db/test_sql_data_split_on_column_value.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
        util_script="tests/test_utils.py",
        other_files=(
            (
                "tests/integration/fixtures/split_and_sample_data/awsathena_connection_string.yml",
                "connection_string.yml",
            ),
        ),
        backend_dependencies=[BackendDependencies.AWS, BackendDependencies.ATHENA],
    ),
    IntegrationTestFixture(
        name="split_data_on_divided_integer_awsathena",
        user_flow_script="tests/integration/db/test_sql_data_split_on_divided_integer.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
        util_script="tests/test_utils.py",
        other_files=(
            (
                "tests/integration/fixtures/split_and_sample_data/awsathena_connection_string.yml",
                "connection_string.yml",
            ),
        ),
        backend_dependencies=[BackendDependencies.AWS, BackendDependencies.ATHENA],
    ),
    IntegrationTestFixture(
        name="split_data_on_mod_integer_awsathena",
        user_flow_script="tests/integration/db/test_sql_data_split_on_mod_integer.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
        util_script="tests/test_utils.py",
        other_files=(
            (
                "tests/integration/fixtures/split_and_sample_data/awsathena_connection_string.yml",
                "connection_string.yml",
            ),
        ),
        backend_dependencies=[BackendDependencies.AWS, BackendDependencies.ATHENA],
    ),
    # TODO: <Alex>ALEX -- Uncomment next statement when "split_on_hashed_column" for AWS ATHENA is implemented.</Alex>
    # IntegrationTestFixture(
    #     name="split_data_on_hashed_column_awsathena",
    #     user_flow_script="tests/integration/db/test_sql_data_split_on_hashed_column.py",
    #     data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
    #     data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
    #     util_script="tests/test_utils.py",
    #     other_files=(
    #         (
    #             "tests/integration/fixtures/split_and_sample_data/awsathena_connection_string.yml",
    #             "connection_string.yml",
    #         ),
    #     ),
    #     backend_dependencies=[BackendDependencies.AWS, BackendDependencies.ATHENA],
    # ),
    IntegrationTestFixture(
        name="split_data_on_multi_column_values_awsathena",
        user_flow_script="tests/integration/db/test_sql_data_split_on_multi_column_values.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
        util_script="tests/test_utils.py",
        other_files=(
            (
                "tests/integration/fixtures/split_and_sample_data/awsathena_connection_string.yml",
                "connection_string.yml",
            ),
        ),
        backend_dependencies=[BackendDependencies.AWS, BackendDependencies.ATHENA],
    ),
    IntegrationTestFixture(
        name="split_data_on_datetime_awsathena",
        user_flow_script="tests/integration/db/test_sql_data_split_on_datetime_and_day_part.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
        util_script="tests/test_utils.py",
        other_files=(
            (
                "tests/integration/fixtures/split_and_sample_data/awsathena_connection_string.yml",
                "connection_string.yml",
            ),
        ),
        backend_dependencies=[BackendDependencies.AWS, BackendDependencies.ATHENA],
    ),
    # TODO: <Alex>ALEX -- Uncomment next statement when "split_on_converted_datetime" for AWS ATHENA is implemented.</Alex>
    # IntegrationTestFixture(
    #     name="split_data_on_converted_datetime_awsathena",
    #     user_flow_script="tests/integration/db/test_sql_data_split_on_converted_datetime.py",
    #     data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
    #     data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
    #     util_script="tests/test_utils.py",
    #     other_files=(
    #         (
    #             "tests/integration/fixtures/split_and_sample_data/awsathena_connection_string.yml",
    #             "connection_string.yml",
    #         ),
    #     ),
    #     backend_dependencies=[BackendDependencies.AWS, BackendDependencies.ATHENA],
    # ),
]

sample_data = [
    IntegrationTestFixture(
        name="sample_data_using_limit_awsathena",
        user_flow_script="tests/integration/db/test_sql_data_sampling.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
        util_script="tests/test_utils.py",
        other_files=(
            (
                "tests/integration/fixtures/split_and_sample_data/awsathena_connection_string.yml",
                "connection_string.yml",
            ),
        ),
        backend_dependencies=[BackendDependencies.AWS, BackendDependencies.ATHENA],
    ),
]

athena_integration_tests += connecting_to_your_data
athena_integration_tests += split_data
athena_integration_tests += sample_data
