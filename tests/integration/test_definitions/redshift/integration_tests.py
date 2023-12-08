from tests.integration.backend_dependencies import BackendDependencies
from tests.integration.integration_test_fixture import IntegrationTestFixture

redshift_integration_tests = []

connecting_to_your_data = [
    # TODO: <Alex>ALEX: Rename test modules to include "configured" and "inferred_and_runtime" suffixes in names.</Alex>
    # IntegrationTestFixture(
    #     name = "redshift_python_example",
    #     user_flow_script= "tests/integration/docusaurus/connecting_to_your_data/database/redshift_python_example.py",
    #     data_context_dir= "tests/integration/fixtures/no_datasources/great_expectations",
    #     data_dir= "tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
    #     backend_dependencies=[ [BackendDependencies.AWS, BackendDependencies.REDSHIFT]],
    #     util_script= "tests/test_utils.py",
    # ),
    # IntegrationTestFixture(
    #     name = "redshift_yaml_example",
    #     user_flow_script= "tests/integration/docusaurus/connecting_to_your_data/database/redshift_yaml_example.py",
    #     data_context_dir= "tests/integration/fixtures/no_datasources/great_expectations",
    #     data_dir= "tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
    #     backend_dependencies=[ [BackendDependencies.AWS, BackendDependencies.REDSHIFT]],
    #     util_script= "tests/test_utils.py",
    # ),
]

split_data = [
    IntegrationTestFixture(
        name="split_data_on_whole_table_redshift",
        user_flow_script="tests/integration/db/test_sql_data_split_on_whole_table.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
        util_script="tests/test_utils.py",
        other_files=(
            (
                "tests/integration/fixtures/split_and_sample_data/redshift_connection_string.yml",
                "connection_string.yml",
            ),
        ),
        backend_dependencies=[BackendDependencies.REDSHIFT],
    ),
    IntegrationTestFixture(
        name="split_data_on_column_value_redshift",
        user_flow_script="tests/integration/db/test_sql_data_split_on_column_value.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
        util_script="tests/test_utils.py",
        other_files=(
            (
                "tests/integration/fixtures/split_and_sample_data/redshift_connection_string.yml",
                "connection_string.yml",
            ),
        ),
        backend_dependencies=[BackendDependencies.REDSHIFT],
    ),
    IntegrationTestFixture(
        name="split_data_on_divided_integer_redshift",
        user_flow_script="tests/integration/db/test_sql_data_split_on_divided_integer.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
        util_script="tests/test_utils.py",
        other_files=(
            (
                "tests/integration/fixtures/split_and_sample_data/redshift_connection_string.yml",
                "connection_string.yml",
            ),
        ),
        backend_dependencies=[BackendDependencies.REDSHIFT],
    ),
    IntegrationTestFixture(
        name="split_data_on_mod_integer_redshift",
        user_flow_script="tests/integration/db/test_sql_data_split_on_mod_integer.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
        util_script="tests/test_utils.py",
        other_files=(
            (
                "tests/integration/fixtures/split_and_sample_data/redshift_connection_string.yml",
                "connection_string.yml",
            ),
        ),
        backend_dependencies=[BackendDependencies.REDSHIFT],
    ),
    # TODO: <Alex>ALEX -- Uncomment next statement when "split_on_hashed_column" for REDSHIFT is implemented.</Alex>
    # IntegrationTestFixture(
    #     name="split_data_on_hashed_column_redshift",
    #     user_flow_script="tests/integration/db/test_sql_data_split_on_hashed_column.py",
    #     data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
    #     data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
    #     util_script="tests/test_utils.py",
    #     other_files=(
    #         (
    #             "tests/integration/fixtures/split_and_sample_data/redshift_connection_string.yml",
    #             "connection_string.yml",
    #         ),
    #     ),
    #     backend_dependencies=[BackendDependencies.REDSHIFT],
    # ),
    IntegrationTestFixture(
        name="split_data_on_multi_column_values_redshift",
        user_flow_script="tests/integration/db/test_sql_data_split_on_multi_column_values.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
        util_script="tests/test_utils.py",
        other_files=(
            (
                "tests/integration/fixtures/split_and_sample_data/redshift_connection_string.yml",
                "connection_string.yml",
            ),
        ),
        backend_dependencies=[BackendDependencies.REDSHIFT],
    ),
    IntegrationTestFixture(
        name="split_data_on_datetime_redshift",
        user_flow_script="tests/integration/db/test_sql_data_split_on_datetime_and_day_part.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
        util_script="tests/test_utils.py",
        other_files=(
            (
                "tests/integration/fixtures/split_and_sample_data/redshift_connection_string.yml",
                "connection_string.yml",
            ),
        ),
        backend_dependencies=[BackendDependencies.REDSHIFT],
    ),
    # TODO: <Alex>ALEX -- Uncomment next statement when "split_on_converted_datetime" for REDSHIFT is implemented.</Alex>
    # IntegrationTestFixture(
    #     name="split_data_on_converted_datetime_redshift",
    #     user_flow_script="tests/integration/db/test_sql_data_split_on_converted_datetime.py",
    #     data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
    #     data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
    #     util_script="tests/test_utils.py",
    #     other_files=(
    #         (
    #             "tests/integration/fixtures/split_and_sample_data/redshift_connection_string.yml",
    #             "connection_string.yml",
    #         ),
    #     ),
    #     backend_dependencies=[BackendDependencies.REDSHIFT],
    # ),
]

sample_data = []

deployment_patterns = [
    IntegrationTestFixture(
        name="deployment_patterns_redshift",
        user_flow_script="tests/integration/docusaurus/deployment_patterns/aws_redshift_deployment_patterns.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
        backend_dependencies=[BackendDependencies.REDSHIFT],
    ),
]

redshift_integration_tests += connecting_to_your_data
redshift_integration_tests += split_data
redshift_integration_tests += sample_data
redshift_integration_tests += deployment_patterns
