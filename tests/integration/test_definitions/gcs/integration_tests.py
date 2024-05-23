from typing import List

from tests.integration.backend_dependencies import BackendDependencies
from tests.integration.integration_test_fixture import IntegrationTestFixture

gcs_integration_tests = []

connecting_to_your_data: List[IntegrationTestFixture] = []

how_to_configure_metadata_store = [
    # Chetan - 20231117 - These have been commented out due to their reliance on the CLI (which has been deleted).  # noqa: E501
    #                     They should be re-enabled once they have been updated.
    # IntegrationTestFixture(
    #     name="how_to_configure_an_expectation_store_in_gcs",
    #     user_flow_script="docs/docusaurus/docs/oss/guides/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_in_gcs.py",  # noqa: E501
    #     data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
    #     backend_dependencies=[BackendDependencies.GCS],
    # ),
    # IntegrationTestFixture(
    #     name="how_to_host_and_share_data_docs_on_gcs",
    #     user_flow_script="docs/docusaurus/docs/oss/guides/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_gcs.py",  # noqa: E501
    #     data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
    #     data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
    #     backend_dependencies=[BackendDependencies.GCS],
    # ),
    # IntegrationTestFixture(
    #     name="how_to_configure_a_validation_result_store_in_gcs",
    #     user_flow_script="docs/docusaurus/docs/oss/guides/setup/configuring_metadata_stores/how_to_configure_a_validation_result_store_in_gcs.py",  # noqa: E501
    #     data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
    #     data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
    #     backend_dependencies=[BackendDependencies.GCS],
    # ),
]

partition_data = [
    IntegrationTestFixture(
        name="partition_data_on_datetime_gcs",
        user_flow_script="tests/integration/test_definitions/gcs/partitioned_on_datetime.py",
        data_context_dir=None,
        backend_dependencies=[BackendDependencies.GCS],
    ),
    IntegrationTestFixture(
        name="gcs_by_path",
        user_flow_script="tests/integration/test_definitions/gcs/select_batch_by_path.py",
        data_context_dir=None,
        backend_dependencies=[BackendDependencies.GCS],
    ),
]

sample_data: List[IntegrationTestFixture] = []

deployment_patterns = [
    IntegrationTestFixture(
        name="deployment_patterns_file_gcs",
        user_flow_script="docs/docusaurus/docs/oss/guides/connecting_to_your_data/fluent/database/gcp_deployment_patterns_file_gcs.py",
        data_context_dir=None,
        backend_dependencies=[BackendDependencies.GCS],
    ),
]

fluent_datasources = [
    IntegrationTestFixture(
        name="how_to_connect_to_data_on_gcs_using_pandas",
        user_flow_script="docs/docusaurus/docs/oss/guides/connecting_to_your_data/fluent/filesystem/how_to_connect_to_data_on_gcs_using_pandas.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        backend_dependencies=[BackendDependencies.GCS],
    ),
]

gcs_integration_tests += connecting_to_your_data
gcs_integration_tests += how_to_configure_metadata_store
gcs_integration_tests += partition_data
gcs_integration_tests += sample_data
gcs_integration_tests += deployment_patterns
gcs_integration_tests += fluent_datasources
