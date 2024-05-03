from typing import List

from tests.integration.backend_dependencies import BackendDependencies
from tests.integration.integration_test_fixture import IntegrationTestFixture

s3_integration_tests = []

connecting_to_your_data: List[IntegrationTestFixture] = []

deployment_patterns = [
    IntegrationTestFixture(
        name="deployment_pattern_pandas_s3",
        user_flow_script="docs/docusaurus/docs/snippets/aws_cloud_storage_pandas.py",
        data_context_dir=None,
        backend_dependencies=[BackendDependencies.AWS],
    ),
]

partition_data = [
    IntegrationTestFixture(
        name="partition_on_datetime_s3",
        user_flow_script="tests/integration/test_definitions/s3/partition_on_datetime.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        backend_dependencies=[BackendDependencies.AWS],
    ),
    IntegrationTestFixture(
        name="select_batch_by_path",
        user_flow_script="tests/integration/test_definitions/s3/select_batch_by_path.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        backend_dependencies=[BackendDependencies.AWS],
    ),
]

sample_data: List[IntegrationTestFixture] = []

fluent_datasources = [
    IntegrationTestFixture(
        name="how_to_connect_to_data_on_s3_using_pandas",
        user_flow_script="docs/docusaurus/docs/oss/guides/connecting_to_your_data/fluent/filesystem/how_to_connect_to_data_on_s3_using_pandas.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        backend_dependencies=[BackendDependencies.AWS],
    ),
]

s3_integration_tests += connecting_to_your_data
s3_integration_tests += deployment_patterns
s3_integration_tests += partition_data
s3_integration_tests += sample_data
s3_integration_tests += fluent_datasources
