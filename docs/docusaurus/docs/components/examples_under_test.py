"""
This file contains the integration test fixtures for documentation example scripts that are
under CI test.
"""

from tests.integration.backend_dependencies import BackendDependencies
from tests.integration.integration_test_fixture import IntegrationTestFixture

docs_tests = []

example_scripts_for_define_expectations = [
    # Create an Expectation
    IntegrationTestFixture(
        # To test, run:
        # pytest --docs-tests -k "doc_example_create_an_expectation" tests/integration/test_script_runner.py
        name="doc_example_create_an_expectation",
        user_flow_script="docs/docusaurus/docs/core/define_expectations/_examples/create_an_expectation.py",
        # data_dir="docs/docusaurus/docs/components/_testing/test_data_sets/single_test_file",
        # data_context_dir="",
        backend_dependencies=[],
    ),
    # Retrieve a Batch of test data (using pandas_default)
    IntegrationTestFixture(
        # To test, run:
        # pytest --docs-tests -k "doc_example_retrieve_a_batch_of_test_data_with_pandas_default" tests/integration/test_script_runner.py
        name="doc_example_retrieve_a_batch_of_test_data_with_pandas_default",
        user_flow_script="docs/docusaurus/docs/core/define_expectations/_examples/retrieve_a_batch_of_test_data_pandas_default.py",
        data_dir="docs/docusaurus/docs/components/_testing/test_data_sets/single_test_file",
        # data_context_dir="",
        backend_dependencies=[],
    ),
    # Retrieve a Batch of test data (from a Batch Definition)
    IntegrationTestFixture(
        # To test, run:
        # pytest --docs-tests -k "doc_example_retrieve_a_batch_of_test_data_from_batch_definition" tests/integration/test_script_runner.py
        name="doc_example_retrieve_a_batch_of_test_data_from_batch_definition",
        user_flow_script="docs/docusaurus/docs/core/define_expectations/_examples/retrieve_a_batch_of_test_data_from_a_batch_definition.py",
        data_dir="docs/docusaurus/docs/components/_testing/test_data_sets/single_test_file",
        # data_context_dir="",
        backend_dependencies=[],
    ),
    # Test an Expectation
    IntegrationTestFixture(
        # To test, run:
        # pytest --docs-tests -k "doc_example_test_an_expectation" tests/integration/test_script_runner.py
        name="doc_example_test_an_expectation",
        user_flow_script="docs/docusaurus/docs/core/define_expectations/_examples/test_an_expectation.py",
        data_dir="docs/docusaurus/docs/components/_testing/test_data_sets/single_test_file",
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
docs_tests.extend(example_scripts_for_define_expectations)
docs_tests.extend(learn_data_quality_use_cases)
