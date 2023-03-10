from tests.integration.backend_dependencies import BackendDependencies
from tests.integration.integration_test_fixture import IntegrationTestFixture

mysql_integration_tests = [
    IntegrationTestFixture(
        name="mysql_yaml_example",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/database/mysql_yaml_example.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
        util_script="tests/test_utils.py",
        extra_backend_dependencies=BackendDependencies.MYSQL,
    ),
    IntegrationTestFixture(
        name="mysql_python_example",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/database/mysql_python_example.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
        util_script="tests/test_utils.py",
        extra_backend_dependencies=BackendDependencies.MYSQL,
    ),
]
