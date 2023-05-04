from tests.integration.backend_dependencies import BackendDependencies
from tests.integration.integration_test_fixture import IntegrationTestFixture

multiple_backend = []

cross_table_comparisons = [
    # Uncomment after mysql warnings are resolved and mysql stage of docs-integration is uncommented
    # IntegrationTestFixture(
    #     name="RUNME_POSTGRES_MYSQL cross_table_comparisons",
    #     user_flow_script="tests/integration/docusaurus/expectations/advanced/data_assistant_cross_table_comparison.py",
    #     data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
    #     data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
    #     util_script="tests/test_utils.py",
    #     backend_dependencies=[
    #         BackendDependencies.MYSQL,
    #         BackendDependencies.POSTGRESQL,
    #     ],
    # ),
]

creating_custom_expectations = [
    IntegrationTestFixture(
        name="expect_column_values_to_equal_three",
        user_flow_script="tests/integration/docusaurus/expectations/creating_custom_expectations/expect_column_values_to_equal_three.py",
        backend_dependencies=[
            BackendDependencies.SPARK,
            BackendDependencies.POSTGRESQL,
        ],
    ),
    IntegrationTestFixture(
        name="expect_column_pair_values_to_have_a_difference_of_three",
        user_flow_script="tests/integration/docusaurus/expectations/creating_custom_expectations/expect_column_pair_values_to_have_a_difference_of_three.py",
        backend_dependencies=[
            BackendDependencies.SPARK,
            BackendDependencies.POSTGRESQL,
        ],
    ),
]

multiple_backend += cross_table_comparisons
multiple_backend += creating_custom_expectations
