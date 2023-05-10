from tests.integration.backend_dependencies import BackendDependencies
from tests.integration.integration_test_fixture import IntegrationTestFixture

abs_integration_tests = []

connecting_to_your_data = [
    # Uncomment after resolving
    # E               ValueError: No data reference for data asset name "taxi_data" matches the given
    # E               batch identifiers {} from batch definition {
    # E                 "datasource_name": "my_azure_datasource",
    # E                 "data_connector_name": "configured_data_connector_name",
    # E                 "data_asset_name": "taxi_data",
    # E                 "batch_identifiers": {}
    # E               }.
    # IntegrationTestFixture(
    #     name="azure_pandas_configured_yaml",
    #     user_flow_script="tests/integration/docusaurus/connecting_to_your_data/cloud/azure/pandas/configured_yaml_example.py",
    #     data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
    #     backend_dependencies=[BackendDependencies.AZURE],
    # ),
    # IntegrationTestFixture(
    #     name="azure_pandas_configured_python",
    #     user_flow_script="tests/integration/docusaurus/connecting_to_your_data/cloud/azure/pandas/configured_python_example.py",
    #     data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
    #     backend_dependencies=[BackendDependencies.AZURE],
    # ),
    IntegrationTestFixture(
        name="azure_pandas_inferred_and_runtime_yaml",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/cloud/azure/pandas/inferred_and_runtime_yaml_example.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        backend_dependencies=[BackendDependencies.AZURE],
    ),
    IntegrationTestFixture(
        name="azure_pandas_inferred_and_runtime_python",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/cloud/azure/pandas/inferred_and_runtime_python_example.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        backend_dependencies=[BackendDependencies.AZURE],
    ),
    # TODO: <Alex>ALEX -- uncomment next four (4) tests once Spark in Azure Pipelines is enabled.</Alex>
    # IntegrationTestFixture(
    #     name = "azure_spark_configured_yaml",
    #     user_flow_script= "tests/integration/docusaurus/connecting_to_your_data/cloud/azure/spark/configured_yaml_example.py",
    #     data_context_dir= "tests/integration/fixtures/no_datasources/great_expectations",
    #     backend_dependencies = [BackendDependencies.AZURE]
    # ),
    # IntegrationTestFixture(
    #     name = "azure_spark_configured_python",
    #     user_flow_script= "tests/integration/docusaurus/connecting_to_your_data/cloud/azure/spark/configured_python_example.py",
    #     data_context_dir= "tests/integration/fixtures/no_datasources/great_expectations",
    #     backend_dependencies = [BackendDependencies.AZURE]
    # ),
    # IntegrationTestFixture(
    #     name = "azure_spark_inferred_and_runtime_yaml",
    #     user_flow_script= "tests/integration/docusaurus/connecting_to_your_data/cloud/azure/spark/inferred_and_runtime_yaml_example.py",
    #     data_context_dir= "tests/integration/fixtures/no_datasources/great_expectations",
    #     backend_dependencies = [BackendDependencies.AZURE]
    # ),
    # IntegrationTestFixture(
    #     name = "azure_spark_inferred_and_runtime_python",
    #     user_flow_script= "tests/integration/docusaurus/connecting_to_your_data/cloud/azure/spark/inferred_and_runtime_python_example.py",
    #     data_context_dir= "tests/integration/fixtures/no_datasources/great_expectations",
    #     backend_dependencies = [BackendDependencies.AZURE]
    # ),
]

split_data = []

sample_data = []

abs_integration_tests += connecting_to_your_data
abs_integration_tests += split_data
abs_integration_tests += sample_data
