from tests.integration.backend_dependencies import BackendDependencies
from tests.integration.integration_test_fixture import IntegrationTestFixture

bigquery_integration_tests = []

connecting_to_your_data = [
    IntegrationTestFixture(
        name="bigquery_python_example",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/database/bigquery_python_example.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
        util_script="tests/test_utils.py",
        backend_dependencies=[BackendDependencies.BIGQUERY],
    ),
]

split_data = [
    IntegrationTestFixture(
        name="split_data_on_whole_table_bigquery",
        user_flow_script="tests/integration/db/test_sql_data_split_on_whole_table.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
        util_script="tests/test_utils.py",
        other_files=(
            (
                "tests/integration/fixtures/split_and_sample_data/bigquery_connection_string.yml",
                "connection_string.yml",
            ),
        ),
        backend_dependencies=[BackendDependencies.BIGQUERY],
    ),
    IntegrationTestFixture(
        name="split_data_on_column_value_bigquery",
        user_flow_script="tests/integration/db/test_sql_data_split_on_column_value.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
        util_script="tests/test_utils.py",
        other_files=(
            (
                "tests/integration/fixtures/split_and_sample_data/bigquery_connection_string.yml",
                "connection_string.yml",
            ),
        ),
        backend_dependencies=[BackendDependencies.BIGQUERY],
    ),
    IntegrationTestFixture(
        name="split_data_on_divided_integer_bigquery",
        user_flow_script="tests/integration/db/test_sql_data_split_on_divided_integer.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
        util_script="tests/test_utils.py",
        other_files=(
            (
                "tests/integration/fixtures/split_and_sample_data/bigquery_connection_string.yml",
                "connection_string.yml",
            ),
        ),
        backend_dependencies=[BackendDependencies.BIGQUERY],
    ),
    IntegrationTestFixture(
        name="split_data_on_mod_integer_bigquery",
        user_flow_script="tests/integration/db/test_sql_data_split_on_mod_integer.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
        util_script="tests/test_utils.py",
        other_files=(
            (
                "tests/integration/fixtures/split_and_sample_data/bigquery_connection_string.yml",
                "connection_string.yml",
            ),
        ),
        backend_dependencies=[BackendDependencies.BIGQUERY],
    ),
    # TODO: <Alex>ALEX -- Uncomment next statement when "split_on_hashed_column" for BIGQUERY is implemented.</Alex>
    # IntegrationTestFixture(
    #     name="split_data_on_hashed_column_bigquery",
    #     user_flow_script="tests/integration/db/test_sql_data_split_on_hashed_column.py",
    #     data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
    #     data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
    #     util_script="tests/test_utils.py",
    #     other_files=(
    #         (
    #             "tests/integration/fixtures/split_and_sample_data/bigquery_connection_string.yml",
    #             "connection_string.yml",
    #         ),
    #     ),
    #     backend_dependencies=[BackendDependencies.BIGQUERY],
    # ),
    IntegrationTestFixture(
        name="split_data_on_multi_column_values_bigquery",
        user_flow_script="tests/integration/db/test_sql_data_split_on_multi_column_values.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
        util_script="tests/test_utils.py",
        other_files=(
            (
                "tests/integration/fixtures/split_and_sample_data/bigquery_connection_string.yml",
                "connection_string.yml",
            ),
        ),
        backend_dependencies=[BackendDependencies.BIGQUERY],
    ),
    IntegrationTestFixture(
        name="split_data_on_datetime_bigquery",
        user_flow_script="tests/integration/db/test_sql_data_split_on_datetime_and_day_part.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
        util_script="tests/test_utils.py",
        other_files=(
            (
                "tests/integration/fixtures/split_and_sample_data/bigquery_connection_string.yml",
                "connection_string.yml",
            ),
        ),
        backend_dependencies=[BackendDependencies.BIGQUERY],
    ),
    # TODO: <Alex>ALEX -- Uncomment next statement when "split_on_converted_datetime" for BIGQUERY is implemented.</Alex>
    # IntegrationTestFixture(
    #     name="split_data_on_converted_datetime_bigquery",
    #     user_flow_script="tests/integration/db/test_sql_data_split_on_converted_datetime.py",
    #     data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
    #     data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
    #     util_script="tests/test_utils.py",
    #     other_files=(
    #         (
    #             "tests/integration/fixtures/split_and_sample_data/bigquery_connection_string.yml",
    #             "connection_string.yml",
    #         ),
    #     ),
    #     backend_dependencies=[BackendDependencies.BIGQUERY],
    # ),
]

sample_data = [
    IntegrationTestFixture(
        name="sample_data_using_limit_bigquery",
        user_flow_script="tests/integration/db/test_sql_data_sampling.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
        util_script="tests/test_utils.py",
        other_files=(
            (
                "tests/integration/fixtures/split_and_sample_data/bigquery_connection_string.yml",
                "connection_string.yml",
            ),
        ),
        backend_dependencies=[BackendDependencies.BIGQUERY],
    ),
]

deployment_patterns = [
    IntegrationTestFixture(
        name="deployment_patterns_file_bigquery",
        user_flow_script="tests/integration/docusaurus/deployment_patterns/gcp_deployment_patterns_file_bigquery.py",
        data_context_dir=None,
        backend_dependencies=[BackendDependencies.BIGQUERY],
    ),
]

runtime_parameters = [
    IntegrationTestFixture(
        name="test_runtime_parameters_bigquery",
        user_flow_script="tests/integration/db/bigquery.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        backend_dependencies=[BackendDependencies.BIGQUERY],
    ),
]

bigquery_integration_tests += connecting_to_your_data
bigquery_integration_tests += split_data
bigquery_integration_tests += sample_data
bigquery_integration_tests += deployment_patterns
bigquery_integration_tests += runtime_parameters
