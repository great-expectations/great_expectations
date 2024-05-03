from typing import List

from tests.integration.backend_dependencies import BackendDependencies
from tests.integration.integration_test_fixture import IntegrationTestFixture

abs_integration_tests = []

connecting_to_your_data: List[IntegrationTestFixture] = []

partition_data = [
    IntegrationTestFixture(
        name="azure_pandas_by_path",
        user_flow_script="tests/integration/test_definitions/abs/select_batch_by_path.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        backend_dependencies=[BackendDependencies.AZURE],
    ),
    IntegrationTestFixture(
        name="partition_data_on_datetime_azure_pandas",
        user_flow_script="tests/integration/test_definitions/abs/partitioned_on_datetime.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        backend_dependencies=[BackendDependencies.AZURE],
    ),
]

sample_data: List[IntegrationTestFixture] = []

fluent_datasources = [
    IntegrationTestFixture(
        name="how_to_connect_to_data_on_azure_blob_storage_using_pandas",
        user_flow_script="docs/docusaurus/docs/oss/guides/connecting_to_your_data/fluent/filesystem/how_to_connect_to_data_on_azure_blob_storage_using_pandas.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        backend_dependencies=[BackendDependencies.AZURE],
    ),
]

abs_integration_tests += connecting_to_your_data
abs_integration_tests += partition_data
abs_integration_tests += sample_data
abs_integration_tests += fluent_datasources
