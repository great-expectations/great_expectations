"""
This file contains the integration test fixtures for documentation example scripts that are
under CI test.
"""

from tests.integration.backend_dependencies import BackendDependencies
from tests.integration.integration_test_fixture import IntegrationTestFixture

docs_tests = []

connect_to_dataframe_data = [
    # Create a Data Source, pandas/spark
    IntegrationTestFixture(
        # To test, run:
        # pytest --docs-tests --spark -k "create_a_df_data_source_spark" tests/integration/test_script_runner.py
        name="create_a_df_data_source_spark",
        user_flow_script="docs/docusaurus/docs/core/connect_to_data/dataframes/_examples/_spark_df_data_source.py",
        # data_dir="",
        # data_context_dir="",
        backend_dependencies=[BackendDependencies.SPARK],
    ),
    IntegrationTestFixture(
        # To test, run:
        # pytest --docs-tests -k "create_a_df_data_source_pandas" tests/integration/test_script_runner.py
        name="create_a_df_data_source_pandas",
        user_flow_script="docs/docusaurus/docs/core/connect_to_data/dataframes/_examples/_pandas_df_data_source.py",
        # data_dir="",
        # data_context_dir="",
        backend_dependencies=[],
    ),
    # Create a Data Asset, pandas
    IntegrationTestFixture(
        # To test, run:
        # pytest --docs-tests -k "create_a_df_data_asset_pandas" tests/integration/test_script_runner.py
        name="create_a_df_data_asset_pandas",
        user_flow_script="docs/docusaurus/docs/core/connect_to_data/dataframes/_examples/_pandas_df_data_asset.py",
        data_dir="docs/docusaurus/docs/components/_testing/test_data_sets/single_test_file",
        # data_context_dir="",
        backend_dependencies=[],
    ),
    # Create a Batch Definition, pandas
    IntegrationTestFixture(
        # To test, run:
        # pytest --docs-tests -k "create_a_df_batch_definition_pandas" tests/integration/test_script_runner.py
        name="create_a_df_batch_definition_pandas",
        user_flow_script="docs/docusaurus/docs/core/connect_to_data/dataframes/_examples/_pandas_df_batch_definition.py",
        data_dir="docs/docusaurus/docs/components/_testing/test_data_sets/single_test_file",
        # data_context_dir="",
        backend_dependencies=[],
    ),
    # Batch Parameters, for a Batch Definition/for a Validation Definition
    IntegrationTestFixture(
        # To test, run:
        # pytest --docs-tests --spark -k "df_batch_parameters_for_batch_definition" tests/integration/test_script_runner.py
        name="df_batch_parameters_for_batch_definition",
        user_flow_script="docs/docusaurus/docs/core/connect_to_data/dataframes/_examples/_batch_parameters_batch_definition.py",
        data_dir="docs/docusaurus/docs/components/_testing/test_data_sets/single_test_file",
        # data_context_dir="",
        backend_dependencies=[BackendDependencies.SPARK],
    ),
    IntegrationTestFixture(
        # To test, run:
        # pytest --docs-tests -k "df_batch_parameters_for_validation_definition" tests/integration/test_script_runner.py
        name="df_batch_parameters_for_validation_definition",
        user_flow_script="docs/docusaurus/docs/core/connect_to_data/dataframes/_examples/_batch_parameters_validation_definition.py",
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
docs_tests.extend(connect_to_dataframe_data)

docs_tests.extend(learn_data_quality_use_cases)
