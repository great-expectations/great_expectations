from tests.integration.backend_dependencies import BackendDependencies
from tests.integration.integration_test_fixture import IntegrationTestFixture

trino_integration_tests = []

connecting_to_your_data = [
    IntegrationTestFixture(
        name="trino_python_example",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/database/trino_python_example.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
        util_script="tests/test_utils.py",
        backend_dependencies=[BackendDependencies.TRINO],
    ),
]


trino_integration_tests += connecting_to_your_data
