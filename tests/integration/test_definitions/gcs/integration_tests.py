from tests.integration.backend_dependencies import BackendDependencies
from tests.integration.integration_test_fixture import IntegrationTestFixture

gcs_integration_tests = []

connecting_to_your_data = [
    IntegrationTestFixture(
        name="gcs_pandas_inferred_and_runtime_yaml",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/pandas/inferred_and_runtime_yaml_example.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        backend_dependencies=[BackendDependencies.GCS],
    ),
    IntegrationTestFixture(
        name="gcs_pandas_inferred_and_runtime_python",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/pandas/inferred_and_runtime_python_example.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        backend_dependencies=[BackendDependencies.GCS],
    ),
    # TODO: <Alex>ALEX -- Implement GCS Configured YAML Example</Alex>
    # TODO: <Alex>ALEX -- uncomment next test once Spark in Azure Pipelines is enabled and GCS Configured YAML Example is implemented.</Alex>
    # IntegrationTestFixture(
    #     name = "gcs_spark_configured_yaml",
    #     user_flow_script= "tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/spark/configured_yaml_example.py",
    #     data_context_dir= "tests/integration/fixtures/no_datasources/great_expectations",
    #     backend_dependencies=[ BackendDependencies.GCS],
    # ),
    # TODO: <Alex>ALEX -- Implement GCS Configured Python Example</Alex>
    # TODO: <Alex>ALEX -- uncomment next test once Spark in Azure Pipelines is enabled and GCS Configured Python Example is implemented.</Alex>
    # IntegrationTestFixture(
    #     name = "gcs_spark_configured_python",
    #     user_flow_script= "tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/spark/configured_python_example.py",
    #     data_context_dir= "tests/integration/fixtures/no_datasources/great_expectations",
    #     backend_dependencies=[ BackendDependencies.GCS],
    # ),
    # TODO: <Alex>ALEX -- uncomment next two (2) tests once Spark in Azure Pipelines is enabled.</Alex>
    # IntegrationTestFixture(
    #     name = "gcs_spark_inferred_and_runtime_yaml",
    #     user_flow_script= "tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/spark/inferred_and_runtime_yaml_example.py",
    #     data_context_dir= "tests/integration/fixtures/no_datasources/great_expectations",
    #     backend_dependencies=[ BackendDependencies.GCS],
    # ),
    # IntegrationTestFixture(
    #     name = "gcs_spark_inferred_and_runtime_python",
    #     user_flow_script= "tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/spark/inferred_and_runtime_python_example.py",
    #     data_context_dir= "tests/integration/fixtures/no_datasources/great_expectations",
    #     backend_dependencies=[ BackendDependencies.GCS],
    # ),
]

how_to_configure_metadata_store = [
    IntegrationTestFixture(
        name="how_to_configure_an_expectation_store_in_gcs",
        user_flow_script="tests/integration/docusaurus/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_in_gcs.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        backend_dependencies=[BackendDependencies.GCS],
    ),
    IntegrationTestFixture(
        name="how_to_host_and_share_data_docs_on_gcs",
        user_flow_script="tests/integration/docusaurus/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_gcs.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
        backend_dependencies=[BackendDependencies.GCS],
    ),
    IntegrationTestFixture(
        name="how_to_configure_a_validation_result_store_in_gcs",
        user_flow_script="tests/integration/docusaurus/setup/configuring_metadata_stores/how_to_configure_a_validation_result_store_in_gcs.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
        backend_dependencies=[BackendDependencies.GCS],
    ),
]

split_data = []

sample_data = []

deployment_patterns = [
    IntegrationTestFixture(
        name="deployment_patterns_file_gcs",
        user_flow_script="tests/integration/docusaurus/deployment_patterns/gcp_deployment_patterns_file_gcs.py",
        data_context_dir=None,
        backend_dependencies=[BackendDependencies.GCS],
    ),
]

fluent_datasources = [
    IntegrationTestFixture(
        name="how_to_connect_to_data_on_gcs_using_pandas",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_data_on_gcs_using_pandas.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        backend_dependencies=[BackendDependencies.GCS],
    ),
]

gcs_integration_tests += connecting_to_your_data
gcs_integration_tests += how_to_configure_metadata_store
gcs_integration_tests += split_data
gcs_integration_tests += sample_data
gcs_integration_tests += deployment_patterns
gcs_integration_tests += fluent_datasources
