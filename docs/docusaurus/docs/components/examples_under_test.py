"""
This file contains the integration test fixtures for documentation example scripts that are
under CI test.
"""

from tests.integration.backend_dependencies import BackendDependencies
from tests.integration.integration_test_fixture import IntegrationTestFixture

docs_tests = []

docs_examples_customize_expectations = [
    IntegrationTestFixture(
        # To test, run:
        # pytest --docs-tests -k "docs_example_define_a_custom_expectation_class" tests/integration/test_script_runner.py
        name="docs_example_define_a_custom_expectation_class",
        user_flow_script="docs/docusaurus/docs/core/customize_expectations/_examples/define_a_custom_expectation_class.py",
        data_dir="docs/docusaurus/docs/components/_testing/test_data_sets/single_test_file",
        # data_context_dir="",
        backend_dependencies=[],
    ),
    IntegrationTestFixture(
        # To test, run:
        # pytest --docs-tests -k "doc_example_expectation_row_conditions" tests/integration/test_script_runner.py
        name="doc_example_expectation_row_conditions",
        user_flow_script="docs/docusaurus/docs/core/customize_expectations/_examples/expectation_row_conditions.py",
        data_dir="docs/docusaurus/docs/components/_testing/test_data_sets/titantic_test_file",
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
docs_tests.extend(docs_examples_customize_expectations)
docs_tests.extend(learn_data_quality_use_cases)
