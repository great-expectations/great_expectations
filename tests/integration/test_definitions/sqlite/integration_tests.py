from tests.integration.backend_dependencies import BackendDependencies
from tests.integration.integration_test_fixture import IntegrationTestFixture

sqlite_integration_tests = []

connecting_to_your_data = [
    IntegrationTestFixture(
        name="how_to_configure_a_sql_datasource",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/datasource_configuration/how_to_configure_a_sql_datasource.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/sqlite/",
        util_script="tests/test_utils.py",
        backend_dependencies=[BackendDependencies.SQLALCHEMY],
    ),
    IntegrationTestFixture(
        name="sqlite_python_example",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/database/sqlite_python_example.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/sqlite/",
        util_script="tests/test_utils.py",
        backend_dependencies=[BackendDependencies.SQLALCHEMY],
    ),
    IntegrationTestFixture(
        name="introspect_and_partition_yaml_example_gradual_sql",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/sql_database/yaml_example_gradual.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/sqlite/",
        util_script="tests/test_utils.py",
        backend_dependencies=[BackendDependencies.SQLALCHEMY],
    ),
    IntegrationTestFixture(
        name="introspect_and_partition_yaml_example_complete_sql",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/sql_database/yaml_example_complete.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/sqlite/",
        util_script="tests/test_utils.py",
        backend_dependencies=[BackendDependencies.SQLALCHEMY],
    ),
]

split_data = []

sample_data = []

sqlite_integration_tests += connecting_to_your_data
sqlite_integration_tests += split_data
sqlite_integration_tests += sample_data
