from tests.integration.backend_dependencies import BackendDependencies
from tests.integration.integration_test_fixture import IntegrationTestFixture

multiple_database = []

cross_table_comparisons = [
    IntegrationTestFixture(
        name="cross_table_comparisons_from_query",
        user_flow_script="tests/integration/docusaurus/expectations/advanced/user_configurable_profiler_cross_table_comparison_from_query.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
        util_script="tests/test_utils.py",
        backend_dependencies=BackendDependencies.MYSQL,
    ),
    IntegrationTestFixture(
        name="cross_table_comparisons",
        user_flow_script="tests/integration/docusaurus/expectations/advanced/user_configurable_profiler_cross_table_comparison.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
        util_script="tests/test_utils.py",
        backend_dependencies=BackendDependencies.MYSQL,
    ),
]

multiple_database += cross_table_comparisons
