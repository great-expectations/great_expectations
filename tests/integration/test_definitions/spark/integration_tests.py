from tests.integration.backend_dependencies import BackendDependencies
from tests.integration.integration_test_fixture import IntegrationTestFixture

spark_integration_tests = []

connecting_to_your_data = [
    IntegrationTestFixture(
        name="how_to_create_a_batch_of_data_from_an_in_memory_spark_dataframe",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/how_to_create_a_batch_of_data_from_an_in_memory_spark_dataframe.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
        backend_dependencies=[BackendDependencies.SPARK],
    ),
    IntegrationTestFixture(
        name="in_memory_spark_yaml",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/in_memory/spark_yaml_example.py",
        backend_dependencies=[BackendDependencies.SPARK],
    ),
    IntegrationTestFixture(
        name="in_memory_spark_python",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/in_memory/spark_python_example.py",
        backend_dependencies=[BackendDependencies.SPARK],
    ),
    #
    #   Re-enable these after this issue is resolved
    #       - pyspark.sql.utils.AnalysisException: Path does not exist: file:/private/var/folders/j2/v50yt91d2yj7sn2mpt61q96w0000gn/T/pytest-of-ken/pytest-113/test_docs_how_to_configure_a_s0/data/yellow_tripdata_sample_2019-01.csv
    #
    #   IntegrationTestFixture(
    #       name="filesystem_spark_yaml",
    #       user_flow_script="tests/integration/docusaurus/connecting_to_your_data/filesystem/spark_yaml_example.py",
    #       data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
    #       backend_dependencies=[BackendDependencies.SPARK],
    #   ),
    #   IntegrationTestFixture(
    #       name="filesystem_spark_python",
    #       user_flow_script="tests/integration/docusaurus/connecting_to_your_data/filesystem/spark_python_example.py",
    #       data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
    #       backend_dependencies=[BackendDependencies.SPARK],
    #   ),
]

databricks_deployment_patterns = [
    IntegrationTestFixture(
        name="databricks_deployment_patterns_dataframe_yaml_configs",
        user_flow_script="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_yaml_configs.py",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
        backend_dependencies=[BackendDependencies.SPARK],
    ),
    IntegrationTestFixture(
        name="databricks_deployment_patterns_dataframe_python_configs",
        user_flow_script="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_python_configs.py",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
        backend_dependencies=[BackendDependencies.SPARK],
    ),
    IntegrationTestFixture(
        name="databricks_deployment_patterns_file_yaml_configs",
        user_flow_script="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_yaml_configs.py",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
        backend_dependencies=[BackendDependencies.SPARK],
    ),
    # unable to mock dbfs in CI
    # IntegrationTestFixture(
    #     name="databricks_deployment_patterns_file_python_configs",
    #     user_flow_script="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py",
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

migration_guide = [
    IntegrationTestFixture(
        name="migration_guide_spark_v3_api",
        user_flow_script="tests/integration/docusaurus/miscellaneous/migration_guide_spark_v3_api.py",
        data_context_dir="tests/test_fixtures/configuration_for_testing_v2_v3_migration/spark/v3/great_expectations/",
        data_dir="tests/test_fixtures/configuration_for_testing_v2_v3_migration/data",
        backend_dependencies=[BackendDependencies.SPARK],
    ),
]

creating_custom_expectations = [
    IntegrationTestFixture(
        name="expect_queried_column_value_frequency_to_meet_threshold",
        user_flow_script="tests/integration/docusaurus/expectations/creating_custom_expectations/expect_queried_column_value_frequency_to_meet_threshold.py",
        backend_dependencies=[BackendDependencies.SPARK],
    ),
    IntegrationTestFixture(
        name="expect_queried_table_row_count_to_be",
        user_flow_script="tests/integration/docusaurus/expectations/creating_custom_expectations/expect_queried_table_row_count_to_be.py",
        backend_dependencies=[BackendDependencies.SPARK],
    ),
]

fluent_datasources = [
    IntegrationTestFixture(
        name="how_to_connect_to_one_or_more_files_using_spark",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_one_or_more_files_using_spark.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
        backend_dependencies=[BackendDependencies.SPARK],
    ),
]

spark_integration_tests += connecting_to_your_data
spark_integration_tests += databricks_deployment_patterns
spark_integration_tests += fluent_datasources
spark_integration_tests += migration_guide
