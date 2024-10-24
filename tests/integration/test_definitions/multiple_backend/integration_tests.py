from tests.integration.backend_dependencies import BackendDependencies
from tests.integration.integration_test_fixture import IntegrationTestFixture

multiple_backend = []

connecting_to_your_data: list[IntegrationTestFixture] = []

deployment_patterns: list[IntegrationTestFixture] = []

creating_custom_expectations = [
    IntegrationTestFixture(
        name="expect_column_values_to_equal_three",
        user_flow_script="docs/docusaurus/docs/snippets/expect_column_values_to_equal_three.py",
        backend_dependencies=[
            BackendDependencies.SPARK,
            BackendDependencies.POSTGRESQL,
        ],
    ),
    IntegrationTestFixture(
        name="expect_column_pair_values_to_have_a_difference_of_three",
        user_flow_script="docs/docusaurus/docs/oss/guides/expectations/creating_custom_expectations/expect_column_pair_values_to_have_a_difference_of_three.py",
        backend_dependencies=[
            BackendDependencies.SPARK,
            BackendDependencies.POSTGRESQL,
        ],
    ),
]

fluent_datasources = [
    IntegrationTestFixture(
        name="how_to_connect_to_data_on_s3_using_spark",
        user_flow_script="docs/docusaurus/docs/oss/guides/connecting_to_your_data/fluent/filesystem/how_to_connect_to_data_on_s3_using_spark.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        backend_dependencies=[BackendDependencies.AWS, BackendDependencies.SPARK],
    ),
    IntegrationTestFixture(
        name="how_to_connect_to_data_on_gcs_using_spark",
        user_flow_script="docs/docusaurus/docs/oss/guides/connecting_to_your_data/fluent/filesystem/how_to_connect_to_data_on_gcs_using_spark.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        backend_dependencies=[BackendDependencies.GCS, BackendDependencies.SPARK],
    ),
    IntegrationTestFixture(
        name="how_to_connect_to_data_on_azure_blob_storage_using_spark",
        user_flow_script="docs/docusaurus/docs/oss/guides/connecting_to_your_data/fluent/filesystem/how_to_connect_to_data_on_azure_blob_storage_using_spark.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        backend_dependencies=[BackendDependencies.AZURE, BackendDependencies.SPARK],
    ),
]

multiple_backend += connecting_to_your_data
multiple_backend += deployment_patterns
multiple_backend += creating_custom_expectations
multiple_backend += fluent_datasources
