from typing import List

from tests.integration.backend_dependencies import BackendDependencies
from tests.integration.integration_test_fixture import IntegrationTestFixture

spark_integration_tests = []

connecting_to_your_data: List[IntegrationTestFixture] = []

databricks_deployment_patterns: List[IntegrationTestFixture] = [
    # unable to mock dbfs in CI
    # IntegrationTestFixture(
    #     name="databricks_deployment_patterns_file_python_configs",
    #     user_flow_script="docs/docusaurus/docs/snippets/databricks_deployment_patterns_file_python_configs.py",  # noqa: E501
    #     data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
    #     backend_dependencies=[BackendDependencies.SPARK],
    # ),
]

emr_deployment_patterns = [
    IntegrationTestFixture(
        name="how_to_use_great_expectations_in_aws_emr_serverless",
        user_flow_script="tests/integration/docusaurus/deployment_patterns/aws_emr_serverless_deployment_patterns.py",
        backend_dependencies=[BackendDependencies.SPARK],
    ),
]

creating_custom_expectations = [
    IntegrationTestFixture(
        name="expect_queried_column_value_frequency_to_meet_threshold",
        user_flow_script="docs/docusaurus/docs/oss/guides/expectations/creating_custom_expectations/expect_queried_column_value_frequency_to_meet_threshold.py",
        backend_dependencies=[BackendDependencies.SPARK],
    ),
    IntegrationTestFixture(
        name="expect_queried_table_row_count_to_be",
        user_flow_script="docs/docusaurus/docs/oss/guides/expectations/creating_custom_expectations/expect_queried_table_row_count_to_be.py",
        backend_dependencies=[BackendDependencies.SPARK],
    ),
]

fluent_datasources = [
    IntegrationTestFixture(
        name="how_to_connect_to_one_or_more_files_using_spark",
        user_flow_script="docs/docusaurus/docs/oss/guides/connecting_to_your_data/fluent/filesystem/how_to_connect_to_one_or_more_files_using_spark.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
        backend_dependencies=[BackendDependencies.SPARK],
    ),
]

spark_integration_tests += connecting_to_your_data
spark_integration_tests += databricks_deployment_patterns
spark_integration_tests += fluent_datasources
