from tests.integration.backend_dependencies import BackendDependencies
from tests.integration.integration_test_fixture import IntegrationTestFixture

multiple_backend = []

connecting_to_your_data = [
    IntegrationTestFixture(
        name="s3_spark_inferred_and_runtime_yaml_example",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/cloud/s3/spark/inferred_and_runtime_yaml_example.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        backend_dependencies=[BackendDependencies.SPARK, BackendDependencies.AWS],
    ),
    IntegrationTestFixture(
        name="s3_spark_inferred_and_runtime_python_example",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/cloud/s3/spark/inferred_and_runtime_python_example.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        backend_dependencies=[BackendDependencies.SPARK, BackendDependencies.AWS],
    ),
]

deployment_patterns = [
    IntegrationTestFixture(
        name="deployment_pattern_spark_s3",
        user_flow_script="tests/integration/docusaurus/deployment_patterns/aws_cloud_storage_spark.py",
        data_context_dir=None,
        backend_dependencies=[BackendDependencies.AWS, BackendDependencies.SPARK],
    ),
]

cross_table_comparisons = [
    IntegrationTestFixture(
        name="cross_table_comparisons",
        user_flow_script="tests/integration/docusaurus/expectations/advanced/data_assistant_cross_table_comparison.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
        util_script="tests/test_utils.py",
        backend_dependencies=[
            BackendDependencies.MYSQL,
            BackendDependencies.POSTGRESQL,
        ],
    ),
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

fluent_datasources = [
    IntegrationTestFixture(
        name="how_to_connect_to_data_on_s3_using_spark",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_data_on_s3_using_spark.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        backend_dependencies=[BackendDependencies.AWS, BackendDependencies.SPARK],
    ),
    IntegrationTestFixture(
        name="how_to_connect_to_data_on_gcs_using_spark",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_data_on_gcs_using_spark.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        backend_dependencies=[BackendDependencies.GCS, BackendDependencies.SPARK],
    ),
    IntegrationTestFixture(
        name="how_to_connect_to_data_on_azure_blob_storage_using_spark",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_data_on_azure_blob_storage_using_spark.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        backend_dependencies=[BackendDependencies.AZURE, BackendDependencies.SPARK],
    ),
]

multiple_backend += connecting_to_your_data
multiple_backend += deployment_patterns
multiple_backend += cross_table_comparisons
multiple_backend += creating_custom_expectations
multiple_backend += fluent_datasources
