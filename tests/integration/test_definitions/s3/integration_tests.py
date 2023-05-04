from tests.integration.backend_dependencies import BackendDependencies
from tests.integration.integration_test_fixture import IntegrationTestFixture

s3_integration_tests = []

connecting_to_your_data = [
    IntegrationTestFixture(
        name="s3_pandas_inferred_and_runtime_yaml",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/cloud/s3/pandas/inferred_and_runtime_yaml_example.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        backend_dependencies=[BackendDependencies.AWS],
    ),
    IntegrationTestFixture(
        name="s3_pandas_inferred_and_runtime_python",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/cloud/s3/pandas/inferred_and_runtime_python_example.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        backend_dependencies=[BackendDependencies.AWS],
    ),
    IntegrationTestFixture(
        name="how_to_configure_an_inferredassetdataconnector",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/how_to_configure_an_inferredassetdataconnector.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/dataconnector_docs",
        backend_dependencies=[BackendDependencies.AWS],
    ),
    IntegrationTestFixture(
        name="how_to_configure_a_configuredassetdataconnector",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/how_to_configure_a_configuredassetdataconnector.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/dataconnector_docs",
        backend_dependencies=[BackendDependencies.AWS],
    ),
    # TODO: <Alex>ALEX -- uncomment all S3 tests once S3 testing in Azure Pipelines is re-enabled and items for specific tests below are addressed.</Alex>
    # TODO: <Alex>ALEX -- Implement S3 Configured YAML Example</Alex>
    # TODO: <Alex>ALEX -- uncomment next test once S3 Configured YAML Example is implemented.</Alex>
    # IntegrationTestFixture(
    #     name = "s3_pandas_configured_yaml_example",
    #     user_flow_script= "tests/integration/docusaurus/connecting_to_your_data/cloud/s3/pandas/configured_yaml_example.py",
    #     data_context_dir= "tests/integration/fixtures/no_datasources/great_expectations",
    #     backend_dependencies=[ BackendDependencies.AWS],
    # ),
    # TODO: <Alex>ALEX -- Implement S3 Configured Python Example</Alex>
    # TODO: <Alex>ALEX -- uncomment next test once S3 Configured Python Example is implemented.</Alex>
    # IntegrationTestFixture(
    #     name = "s3_pandas_configured_python_example",
    #     user_flow_script= "tests/integration/docusaurus/connecting_to_your_data/cloud/s3/pandas/configured_python_example.py",
    #     data_context_dir= "tests/integration/fixtures/no_datasources/great_expectations",
    #     backend_dependencies=[ BackendDependencies.AWS],
    # ),
    # TODO: <Alex>ALEX -- Implement S3 Configured YAML Example</Alex>
    # TODO: <Alex>ALEX -- uncomment next test once Spark in Azure Pipelines is enabled and S3 Configured YAML Example is implemented.</Alex>
    # IntegrationTestFixture(
    #     name = "s3_spark_configured_yaml_example",
    #     user_flow_script= "tests/integration/docusaurus/connecting_to_your_data/cloud/s3/spark/configured_yaml_example.py",
    #     backend_dependencies=[ [BackendDependencies.SPARK, BackendDependencies.AWS]],
    # ),
    # TODO: <Alex>ALEX -- Implement S3 Configured Python Example</Alex>
    # TODO: <Alex>ALEX -- uncomment next test once Spark in Azure Pipelines is enabled and S3 Configured Python Example is implemented.</Alex>
    # IntegrationTestFixture(
    #     name = "s3_spark_configured_python_example",
    #     user_flow_script= "tests/integration/docusaurus/connecting_to_your_data/cloud/s3/spark/configured_python_example.py",
    #     backend_dependencies=[ [BackendDependencies.SPARK, BackendDependencies.AWS]],
    # ),
    # TODO: <Alex>ALEX -- uncomment next two (2) tests once Spark in Azure Pipelines is enabled.</Alex>
    # IntegrationTestFixture(
    #     name = "s3_spark_inferred_and_runtime_yaml_example",
    #     user_flow_script= "tests/integration/docusaurus/connecting_to_your_data/cloud/s3/spark/inferred_and_runtime_yaml_example.py",
    #     data_context_dir= "tests/integration/fixtures/no_datasources/great_expectations",
    #     backend_dependencies=[ [BackendDependencies.SPARK, BackendDependencies.AWS]],
    # ),
    # IntegrationTestFixture(
    #     name = "s3_spark_inferred_and_runtime_python_example",
    #     user_flow_script= "tests/integration/docusaurus/connecting_to_your_data/cloud/s3/spark/inferred_and_runtime_python_example.py",
    #     data_context_dir= "tests/integration/fixtures/no_datasources/great_expectations",
    #     backend_dependencies=[ [BackendDependencies.SPARK, BackendDependencies.AWS]],
    # ),
]

deployment_patterns = [
    # IntegrationTestFixture(
    #    name="RUNME_AWS_deployment_pattern_pandas_s3",
    #    user_flow_script="tests/integration/docusaurus/deployment_patterns/aws_cloud_storage_pandas.py",
    #    data_context_dir=None,
    #    backend_dependencies=[BackendDependencies.AWS],
    # ),
    # TODO: This will currently work locally, but the Azure CI/CD will need to be updated to enable
    IntegrationTestFixture(
        name="RUNME_AWS_deployment_pattern_spark_s3",
        user_flow_script="tests/integration/docusaurus/deployment_patterns/aws_cloud_storage_spark.py",
        data_context_dir=None,
        backend_dependencies=[BackendDependencies.AWS, BackendDependencies.SPARK],
    )
]

split_data = []

sample_data = []

s3_integration_tests += connecting_to_your_data
s3_integration_tests += deployment_patterns
s3_integration_tests += split_data
s3_integration_tests += sample_data
