"""
This file contains the integration test fixtures for documentation example scripts that are
under CI test.
"""

from tests.integration.backend_dependencies import BackendDependencies
from tests.integration.integration_test_fixture import IntegrationTestFixture

docs_tests = []

create_a_data_context = [
    IntegrationTestFixture(
        # To test, run:
        # pytest --docs-tests -k "create_an_ephemeral_data_context" tests/integration/test_script_runner.py
        name="create_an_ephemeral_data_context",
        user_flow_script="docs/docusaurus/docs/core/set_up_a_gx_environment/_create_a_data_context/ephemeral_data_context.py",
        # data_dir="",
        # data_context_dir="",
        backend_dependencies=[],
    ),
    IntegrationTestFixture(
        # To test, run:
        # pytest --docs-tests --cloud -k "create_a_cloud_data_context" tests/integration/test_script_runner.py
        name="create_a_cloud_data_context",
        user_flow_script="docs/docusaurus/docs/core/set_up_a_gx_environment/_create_a_data_context/cloud_data_context.py",
        # data_dir="",
        # data_context_dir="",
        backend_dependencies=[],
    ),
    IntegrationTestFixture(
        # To test, run:
        # pytest --docs-tests -k "create_a_file_data_context" tests/integration/test_script_runner.py
        name="create_a_file_data_context",
        user_flow_script="docs/docusaurus/docs/core/set_up_a_gx_environment/_create_a_data_context/file_data_context.py",
        # data_dir="",
        # data_context_dir="",
        backend_dependencies=[],
    ),
    IntegrationTestFixture(
        # To test, run:
        # pytest --docs-tests -k "create_a_quick_data_context" tests/integration/test_script_runner.py
        name="create_a_quick_data_context",
        user_flow_script="docs/docusaurus/docs/core/set_up_a_gx_environment/_create_a_data_context/quick_start.py",
        # data_dir="",
        # data_context_dir="",
        backend_dependencies=[],
    ),
]


learn_data_quality_use_cases = [
    # Schema.
    IntegrationTestFixture(
        name="data_quality_use_case_schema_expectations",
        user_flow_script="docs/docusaurus/docs/reference/learn/data_quality_use_cases/schema_resources/schema_expectations.py",
        data_dir="tests/test_sets/learn_data_quality_use_cases/",
        util_script="tests/test_utils.py",
        backend_dependencies=[BackendDependencies.POSTGRESQL],
    ),
    IntegrationTestFixture(
        name="data_quality_use_case_schema_validation_over_time",
        user_flow_script="docs/docusaurus/docs/reference/learn/data_quality_use_cases/schema_resources/schema_validation_over_time.py",
        data_dir="tests/test_sets/learn_data_quality_use_cases/",
        util_script="tests/test_utils.py",
        backend_dependencies=[BackendDependencies.POSTGRESQL],
    ),
    IntegrationTestFixture(
        name="data_quality_use_case_schema_strict_and_relaxed_validation",
        user_flow_script="docs/docusaurus/docs/reference/learn/data_quality_use_cases/schema_resources/schema_strict_and_relaxed.py",
        data_dir="tests/test_sets/learn_data_quality_use_cases/",
        util_script="tests/test_utils.py",
        backend_dependencies=[BackendDependencies.POSTGRESQL],
    ),
]

# Extend the docs_tests list with the above sublists (only the docs_tests list is imported
# into `test_script_runner.py` and actually used in CI checks).
docs_tests.extend(create_a_data_context)
docs_tests.extend(learn_data_quality_use_cases)
