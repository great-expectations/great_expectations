"""Note: AWS Glue split from spark since it requires different test dependencies."""

from tests.integration.backend_dependencies import BackendDependencies
from tests.integration.integration_test_fixture import IntegrationTestFixture

aws_glue_integration_tests = []

deployment_patterns = [
    # TODO: The AWS_GLUE dependency is only being marked and not run at this time.
    IntegrationTestFixture(
        name="how_to_use_great_expectations_in_aws_glue",
        user_flow_script="tests/integration/docusaurus/deployment_patterns/aws_glue_deployment_patterns.py",
        backend_dependencies=[
            BackendDependencies.SPARK,
            BackendDependencies.AWS,
            BackendDependencies.AWS_GLUE,
        ],
    ),
]

aws_glue_integration_tests += deployment_patterns
