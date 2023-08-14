from tests.integration.backend_dependencies import BackendDependencies
from tests.integration.integration_test_fixture import IntegrationTestFixture

abs_integration_tests = []

connecting_to_your_data = [
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

fluent_datasources = [
    IntegrationTestFixture(
        name="how_to_connect_to_data_on_azure_blob_storage_using_pandas",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_data_on_azure_blob_storage_using_pandas.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        backend_dependencies=[BackendDependencies.AZURE],
    ),
]

abs_integration_tests += connecting_to_your_data
abs_integration_tests += split_data
abs_integration_tests += sample_data
abs_integration_tests += fluent_datasources
