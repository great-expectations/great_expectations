"""
This file contains the integration test fixtures for documentation example scripts that are
under CI test.
"""

from tests.integration.backend_dependencies import BackendDependencies
from tests.integration.integration_test_fixture import IntegrationTestFixture

docs_tests = []

install_gx = [
    IntegrationTestFixture(
        # To test, run:
        # pytest --docs-tests -k "verify_gx_version" tests/integration/test_script_runner.py
        name="verify_gx_version",
        user_flow_script="docs/docusaurus/docs/core/set_up_a_gx_environment/_install_gx/_local_installation_verification.py",
        # data_dir="",
        # data_context_dir="",
        backend_dependencies=[],
    ),
]

try_gx = [
    IntegrationTestFixture(
        # To test, run:
        # pytest --docs-tests -k "try_gx_exploratory" tests/integration/test_script_runner.py
        name="try_gx_exploratory",
        user_flow_script="docs/docusaurus/docs/core/introduction/try_gx_exploratory.py",
        backend_dependencies=[],
    ),
    # To test, run:
    # pytest --docs-tests --postgresql -k "try_gx_end_to_end" tests/integration/test_script_runner.py
    IntegrationTestFixture(
        name="try_gx_end_to_end",
        user_flow_script="docs/docusaurus/docs/core/introduction/try_gx_end_to_end.py",
        backend_dependencies=[BackendDependencies.POSTGRESQL],
    ),
]

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
    # TODO: Re-enable this once a --docs-tests-cloud environment is available.
    # IntegrationTestFixture(
    #     # To test, run:
    #     # pytest --docs-tests --cloud -k "create_a_cloud_data_context" tests/integration/test_script_runner.py
    #     name="create_a_cloud_data_context",
    #     user_flow_script="docs/docusaurus/docs/core/set_up_a_gx_environment/_create_a_data_context/cloud_data_context.py",
    #     # data_dir="",
    #     # data_context_dir="",
    #     backend_dependencies=[],
    # ),
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

connect_to_filesystem_data_create_a_data_source = [
    # Local, pandas/spark
    IntegrationTestFixture(
        # To test, run:
        # pytest --docs-tests -k "create_a_data_source_filesystem_local_pandas" tests/integration/test_script_runner.py
        name="create_a_data_source_filesystem_local_pandas",
        user_flow_script="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_data_source/_local_or_networked/_pandas.py",
        data_dir="docs/docusaurus/docs/components/_testing/test_data_sets/single_test_file",
        backend_dependencies=[],
    ),
    IntegrationTestFixture(
        # To test, run:
        # pytest --docs-tests --spark -k "create_a_data_source_filesystem_local_spark" tests/integration/test_script_runner.py
        name="create_a_data_source_filesystem_local_spark",
        user_flow_script="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_data_source/_local_or_networked/_spark.py",
        data_dir="docs/docusaurus/docs/components/_testing/test_data_sets/single_test_file",
        backend_dependencies=[BackendDependencies.SPARK],
    ),
    # ABS, pandas/spark
    IntegrationTestFixture(
        # To test, run:
        # pytest --docs-tests --azure -k "create_a_data_source_filesystem_abs_pandas" tests/integration/test_script_runner.py
        name="create_a_data_source_filesystem_abs_pandas",
        user_flow_script="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_data_source/_abs/_pandas.py",
        backend_dependencies=[BackendDependencies.AZURE],
    ),
    IntegrationTestFixture(
        # To test, run:
        # pytest --docs-tests --azure --spark -k "create_a_datasource_filesystem_abs_spark" tests/integration/test_script_runner.py
        name="create_a_datasource_filesystem_abs_spark",
        user_flow_script="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_data_source/_abs/_spark.py",
        backend_dependencies=[BackendDependencies.AZURE, BackendDependencies.SPARK],
    ),
    # GCS, pandas/spark
    IntegrationTestFixture(
        # To test, run:
        # pytest --docs-tests --bigquery -k "create_a_data_source_filesystem_gcs_pandas" tests/integration/test_script_runner.py
        name="create_a_data_source_filesystem_gcs_pandas",
        user_flow_script="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_data_source/_gcs/_pandas.py",
        backend_dependencies=[BackendDependencies.GCS],
    ),
    IntegrationTestFixture(
        # To test, run:
        # pytest --docs-tests --bigquery --spark -k "create_a_data_source_filesystem_gcs_spark" tests/integration/test_script_runner.py
        name="create_a_data_source_filesystem_gcs_spark",
        user_flow_script="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_data_source/_gcs/_spark.py",
        backend_dependencies=[BackendDependencies.GCS, BackendDependencies.SPARK],
    ),
    # # S3, pandas/spark
    IntegrationTestFixture(
        # To test, run:
        # pytest --docs-tests --aws -k "create_a_data_source_filesystem_s3_pandas" tests/integration/test_script_runner.py
        name="create_a_data_source_filesystem_s3_pandas",
        user_flow_script="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_data_source/_s3/_pandas.py",
        backend_dependencies=[BackendDependencies.AWS],
    ),
    IntegrationTestFixture(
        # To test, run:
        # pytest --docs-tests --aws --spark -k "create_a_datasource_filesystem_s3_spark" tests/integration/test_script_runner.py
        name="create_a_datasource_filesystem_s3_spark",
        user_flow_script="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_data_source/_s3/_spark.py",
        backend_dependencies=[BackendDependencies.AWS, BackendDependencies.SPARK],
    ),
]

connect_to_filesystem_data_create_a_data_asset = [
    # local, directory asset/file asset
    IntegrationTestFixture(
        # To test, run:
        # pytest --docs-tests -k "create_a_data_asset_filesystem_local_file_asset" tests/integration/test_script_runner.py
        name="create_a_data_asset_filesystem_local_file_asset",
        user_flow_script="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_data_asset/_local_or_networked/_file_asset.py",
        data_dir="docs/docusaurus/docs/components/_testing/test_data_sets/single_test_file",
        data_context_dir="docs/docusaurus/docs/components/_testing/test_data_contexts/filesystem_datasource_local_pandas_no_assets/gx",
        backend_dependencies=[],
    ),
    IntegrationTestFixture(
        # To test, run:
        # pytest --docs-tests --spark -k "create_a_data_asset_filesystem_local_directory_asset" tests/integration/test_script_runner.py
        name="create_a_data_asset_filesystem_local_directory_asset",
        user_flow_script="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_data_asset/_local_or_networked/_directory_asset.py",
        data_dir="docs/docusaurus/docs/components/_testing/test_data_sets/three_test_files",
        data_context_dir="docs/docusaurus/docs/components/_testing/test_data_contexts/filesystem_datasource_local_spark_no_assets/gx",
        backend_dependencies=[BackendDependencies.SPARK],
    ),
    # ABS, directory asset/file asset
    IntegrationTestFixture(
        # To test, run:
        # pytest --docs-tests --azure -k "create_a_data_asset_filesystem_abs_file_asset" tests/integration/test_script_runner.py
        name="create_a_data_asset_filesystem_abs_file_asset",
        user_flow_script="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_data_asset/_abs/_file_asset.py",
        data_context_dir="docs/docusaurus/docs/components/_testing/test_data_contexts/filesystem_datasource_azure_pandas_no_assets/gx",
        backend_dependencies=[BackendDependencies.AZURE],
    ),
    IntegrationTestFixture(
        # To test, run:
        # pytest --docs-tests --azure --spark -k "create_a_data_asset_filesystem_abs_directory_asset" tests/integration/test_script_runner.py
        name="create_a_data_asset_filesystem_abs_directory_asset",
        user_flow_script="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_data_asset/_abs/_directory_asset.py",
        data_context_dir="docs/docusaurus/docs/components/_testing/test_data_contexts/filesystem_datasource_azure_spark_no_assets/gx",
        backend_dependencies=[BackendDependencies.AZURE, BackendDependencies.SPARK],
    ),
    # GCS, directory asset/file asset
    IntegrationTestFixture(
        # To test, run:
        # pytest --docs-tests --bigquery -k "create_a_data_asset_filesystem_gcs_file_asset" tests/integration/test_script_runner.py
        name="create_a_data_asset_filesystem_gcs_file_asset",
        user_flow_script="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_data_asset/_gcs/_file_asset.py",
        data_context_dir="docs/docusaurus/docs/components/_testing/test_data_contexts/filesystem_datasource_gcs_pandas_no_assets/gx",
        backend_dependencies=[BackendDependencies.GCS],
    ),
    IntegrationTestFixture(
        # To test, run:
        # pytest --docs-tests --bigquery --spark -k "create_a_data_asset_filesystem_gcs_directory_asset" tests/integration/test_script_runner.py
        name="create_a_data_asset_filesystem_gcs_directory_asset",
        user_flow_script="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_data_asset/_gcs/_directory_asset.py",
        data_context_dir="docs/docusaurus/docs/components/_testing/test_data_contexts/filesystem_datasource_gcs_spark_no_assets/gx",
        backend_dependencies=[BackendDependencies.GCS, BackendDependencies.SPARK],
    ),
    # S3, directory asset/file asset
    IntegrationTestFixture(
        # To test, run:
        # pytest --docs-tests --aws -k "create_a_data_asset_filesystem_s3_file_asset" tests/integration/test_script_runner.py
        name="create_a_data_asset_filesystem_s3_file_asset",
        user_flow_script="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_data_asset/_s3/_file_asset.py",
        data_context_dir="docs/docusaurus/docs/components/_testing/test_data_contexts/filesystem_datasource_aws_pandas_no_assets/gx",
        backend_dependencies=[BackendDependencies.AWS],
    ),
    IntegrationTestFixture(
        # To test, run:
        # pytest --docs-tests --aws --spark -k "create_a_data_asset_filesystem_s3_directory_asset" tests/integration/test_script_runner.py
        name="create_a_data_asset_filesystem_s3_directory_asset",
        user_flow_script="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_data_asset/_s3/_directory_asset.py",
        data_context_dir="docs/docusaurus/docs/components/_testing/test_data_contexts/filesystem_datasource_aws_spark_no_assets/gx",
        backend_dependencies=[BackendDependencies.AWS, BackendDependencies.SPARK],
    ),
]

connect_to_filesystem_data_create_a_batch_definition = [
    # directory, whole/daily/monthly/yearly
    IntegrationTestFixture(
        # To test, run:
        # pytest --docs-tests --spark -k "create_a_batch_definition_filesystem_directory_whole" tests/integration/test_script_runner.py
        name="create_a_batch_definition_filesystem_directory_whole",
        user_flow_script="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_batch_definition/_examples/_directory_whole_directory.py",
        data_dir="docs/docusaurus/docs/components/_testing/test_data_sets/three_test_files",
        data_context_dir="docs/docusaurus/docs/components/_testing/test_data_contexts/filesystem_datasource_local_spark_directory_asset/gx",
        backend_dependencies=[BackendDependencies.SPARK],
    ),
    IntegrationTestFixture(
        # To test, run:
        # pytest --docs-tests --spark -k "create_a_batch_definition_filesystem_directory_daily" tests/integration/test_script_runner.py
        name="create_a_batch_definition_filesystem_directory_daily",
        user_flow_script="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_batch_definition/_examples/_directory_partitioned_daily.py",
        data_dir="docs/docusaurus/docs/components/_testing/test_data_sets/partitioned_file_sample_data/daily_file_sample_data",
        data_context_dir="docs/docusaurus/docs/components/_testing/test_data_contexts/filesystem_datasource_local_spark_directory_asset/gx",
        backend_dependencies=[BackendDependencies.SPARK],
    ),
    IntegrationTestFixture(
        # To test, run:
        # pytest --docs-tests --spark -k "create_a_batch_definition_filesystem_directory_monthly" tests/integration/test_script_runner.py
        name="create_a_batch_definition_filesystem_directory_monthly",
        user_flow_script="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_batch_definition/_examples/_directory_partitioned_monthly.py",
        data_dir="docs/docusaurus/docs/components/_testing/test_data_sets/partitioned_file_sample_data/monthly_file_sample_data",
        data_context_dir="docs/docusaurus/docs/components/_testing/test_data_contexts/filesystem_datasource_local_spark_directory_asset/gx",
        backend_dependencies=[BackendDependencies.SPARK],
    ),
    IntegrationTestFixture(
        # To test, run:
        # pytest --docs-tests --spark -k "create_a_batch_definition_filesystem_directory_yearly" tests/integration/test_script_runner.py
        name="create_a_batch_definition_filesystem_directory_yearly",
        user_flow_script="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_batch_definition/_examples/_directory_partitioned_yearly.py",
        data_dir="docs/docusaurus/docs/components/_testing/test_data_sets/partitioned_file_sample_data/yearly_file_sample_data",
        data_context_dir="docs/docusaurus/docs/components/_testing/test_data_contexts/filesystem_datasource_local_spark_directory_asset/gx",
        backend_dependencies=[BackendDependencies.SPARK],
    ),
    # file, path/daily/monthly/yearly
    IntegrationTestFixture(
        # To test, run:
        # pytest --docs-tests -k "create_a_batch_definition_filesystem_file_path" tests/integration/test_script_runner.py
        name="create_a_batch_definition_filesystem_file_path",
        user_flow_script="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_batch_definition/_examples/_file_path.py",
        data_dir="docs/docusaurus/docs/components/_testing/test_data_sets/single_test_file",
        data_context_dir="docs/docusaurus/docs/components/_testing/test_data_contexts/filesystem_datasource_local_pandas_file_asset/gx",
        backend_dependencies=[],
    ),
    IntegrationTestFixture(
        # To test, run:
        # pytest --docs-tests -k "create_a_batch_definition_filesystem_file_daily" tests/integration/test_script_runner.py
        name="create_a_batch_definition_filesystem_file_daily",
        user_flow_script="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_batch_definition/_examples/_file_partitioned_daily.py",
        data_dir="docs/docusaurus/docs/components/_testing/test_data_sets/partitioned_file_sample_data/daily_file_sample_data/",
        data_context_dir="docs/docusaurus/docs/components/_testing/test_data_contexts/filesystem_datasource_local_pandas_file_asset/gx",
        backend_dependencies=[],
    ),
    IntegrationTestFixture(
        # To test, run:
        # pytest --docs-tests -k "create_a_batch_definition_filesystem_file_monthly" tests/integration/test_script_runner.py
        name="create_a_batch_definition_filesystem_file_monthly",
        user_flow_script="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_batch_definition/_examples/_file_partitioned_monthly.py",
        data_dir="docs/docusaurus/docs/components/_testing/test_data_sets/partitioned_file_sample_data/monthly_file_sample_data/",
        data_context_dir="docs/docusaurus/docs/components/_testing/test_data_contexts/filesystem_datasource_local_pandas_file_asset/gx",
        backend_dependencies=[],
    ),
    IntegrationTestFixture(
        # To test, run:
        # pytest --docs-tests -k "create_a_batch_definition_filesystem_file_yearly" tests/integration/test_script_runner.py
        name="create_a_batch_definition_filesystem_file_yearly",
        user_flow_script="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_batch_definition/_examples/_file_partitioned_yearly.py",
        data_dir="docs/docusaurus/docs/components/_testing/test_data_sets/partitioned_file_sample_data/yearly_file_sample_data/",
        data_context_dir="docs/docusaurus/docs/components/_testing/test_data_contexts/filesystem_datasource_local_pandas_file_asset/gx",
        backend_dependencies=[],
    ),
]

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
        # data_context_dir="",
        backend_dependencies=[],
    ),
]

docs_example_scripts_run_validations = [
    # Create a Validation Definition
    IntegrationTestFixture(
        # To test, run:
        # pytest --docs-tests -k "docs_example_create_a_validation_definition" tests/integration/test_script_runner.py
        name="docs_example_create_a_validation_definition",
        user_flow_script="docs/docusaurus/docs/core/run_validations/_examples/create_a_validation_definition.py",
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
    # Run a Validation Definition
    IntegrationTestFixture(
        # To test, run:
        # pytest --docs-tests -k "docs_example_run_a_validation_definition" tests/integration/test_script_runner.py
        name="docs_example_run_a_validation_definition",
        user_flow_script="docs/docusaurus/docs/core/run_validations/_examples/run_a_validation_definition.py",
        data_dir="docs/docusaurus/docs/components/_testing/test_data_sets/single_test_file",
        # data_context_dir="",
        backend_dependencies=[],
    ),
]

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
    # Organize Expectations into Expectation Suites
    IntegrationTestFixture(
        # To test, run:
        # pytest --docs-tests -k "doc_example_organize_expectations_into_expectation_suites" tests/integration/test_script_runner.py
        name="doc_example_organize_expectations_into_expectation_suites",
        user_flow_script="docs/docusaurus/docs/core/define_expectations/_examples/organize_expectations_into_suites.py",
        data_dir="docs/docusaurus/docs/components/_testing/test_data_sets/single_test_file",
        # data_context_dir="",
        backend_dependencies=[],
    ),
]

docs_examples_trigger_actions_based_on_validation_results = [
    # Create a Checkpoint
    IntegrationTestFixture(
        # To test, run:
        # pytest --docs-tests -k "docs_example_create_a_checkpoint" tests/integration/test_script_runner.py
        name="docs_example_create_a_checkpoint",
        user_flow_script="docs/docusaurus/docs/core/trigger_actions_based_on_results/_examples/create_a_checkpoint_with_actions.py",
        data_dir="docs/docusaurus/docs/components/_testing/test_data_sets/single_test_file",
        # data_context_dir="",
        backend_dependencies=[],
    ),
    # Run a Checkpoint
    IntegrationTestFixture(
        # To test, run:
        # pytest --docs-tests -k "docs_example_run_a_checkpoint" tests/integration/test_script_runner.py
        name="docs_example_run_a_checkpoint",
        user_flow_script="docs/docusaurus/docs/core/trigger_actions_based_on_results/_examples/run_a_checkpoint.py",
        data_dir="docs/docusaurus/docs/components/_testing/test_data_sets/single_test_file",
        # data_context_dir="",
        backend_dependencies=[],
    ),
    # Choose a Result Format
    IntegrationTestFixture(
        # To test, run:
        # pytest --docs-tests -k "docs_example_choose_result_format" tests/integration/test_script_runner.py
        name="docs_example_choose_result_format",
        user_flow_script="docs/docusaurus/docs/core/trigger_actions_based_on_results/_examples/choose_result_format.py",
        data_dir="docs/docusaurus/docs/components/_testing/test_data_sets/single_test_file",
        # data_context_dir="",
        backend_dependencies=[],
    ),
]

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
        # pytest --docs-tests -k "docs_example_expectation_row_conditions" tests/integration/test_script_runner.py
        name="docs_example_expectation_row_conditions",
        user_flow_script="docs/docusaurus/docs/core/customize_expectations/_examples/expectation_row_conditions.py",
        data_dir="docs/docusaurus/docs/components/_testing/test_data_sets/titantic_test_file",
        # data_context_dir="",
        backend_dependencies=[],
    ),
    IntegrationTestFixture(
        # To test, run:
        # pytest --docs-tests -k "docs_example_use_sql_to_define_a_custom_expectation" tests/integration/test_script_runner.py
        name="docs_example_use_sql_to_define_a_custom_expectation",
        user_flow_script="docs/docusaurus/docs/core/customize_expectations/_examples/use_sql_to_define_a_custom_expectation.py",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/sqlite",
        # data_context_dir="",
        backend_dependencies=[],
    ),
]

docs_example_configure_project_settings = [
    # Toggle analytics events
    IntegrationTestFixture(
        # To test, run:
        # pytest --docs-tests -k "docs_example_toggle_analytics_events" tests/integration/test_script_runner.py
        name="docs_example_toggle_analytics_events",
        user_flow_script="docs/docusaurus/docs/core/configure_project_settings/_examples/toggle_analytics_events.py",
        # data_dir="docs/docusaurus/docs/components/_testing/test_data_sets/single_test_file",
        # data_context_dir="",
        backend_dependencies=[],
    ),
    # Configure Metadata Stores
    IntegrationTestFixture(
        # To test, run:
        # pytest --docs-tests -k "docs_example_configure_metadata_stores" tests/integration/test_script_runner.py
        name="docs_example_configure_metadata_stores",
        user_flow_script="docs/docusaurus/docs/core/configure_project_settings/_examples/configure_metadata_stores.py",
        # data_dir="docs/docusaurus/docs/components/_testing/test_data_sets/single_test_file",
        # data_context_dir="",
        backend_dependencies=[],
    ),
]

docs_examples_configure_data_docs = [
    IntegrationTestFixture(
        # To test, run:
        # pytest --docs-tests -k "docs_example_configure_data_docs_filesystem" tests/integration/test_script_runner.py
        name="docs_example_configure_data_docs_filesystem",
        user_flow_script="docs/docusaurus/docs/core/configure_project_settings/configure_data_docs/_examples/data_docs_local_or_networked.py",
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
    # Missingness.
    IntegrationTestFixture(
        name="data_quality_use_case_missingness_expectations",
        user_flow_script="docs/docusaurus/docs/reference/learn/data_quality_use_cases/missingness_resources/missingness_expectations.py",
        data_dir="tests/test_sets/learn_data_quality_use_cases/",
        util_script="tests/test_utils.py",
        backend_dependencies=[BackendDependencies.POSTGRESQL],
    ),
    # Volume.
    IntegrationTestFixture(
        name="data_quality_use_case_volume_expectations",
        user_flow_script="docs/docusaurus/docs/reference/learn/data_quality_use_cases/volume_resources/volume_expectations.py",
        data_dir="tests/test_sets/learn_data_quality_use_cases/",
        util_script="tests/test_utils.py",
        backend_dependencies=[BackendDependencies.POSTGRESQL],
    ),
    IntegrationTestFixture(
        name="data_quality_use_case_volume_workflow",
        user_flow_script="docs/docusaurus/docs/reference/learn/data_quality_use_cases/volume_resources/volume_workflow.py",
        data_dir="tests/test_sets/learn_data_quality_use_cases/",
        util_script="tests/test_utils.py",
        backend_dependencies=[BackendDependencies.POSTGRESQL],
    ),
    # Distribution.
    IntegrationTestFixture(
        name="data_quality_use_case_distribution_expectations",
        user_flow_script="docs/docusaurus/docs/reference/learn/data_quality_use_cases/distribution_resources/distribution_expectations.py",
        data_dir="tests/test_sets/learn_data_quality_use_cases/",
        util_script="tests/test_utils.py",
        backend_dependencies=[BackendDependencies.POSTGRESQL],
    ),
    IntegrationTestFixture(
        name="data_quality_use_case_distribution_workflow",
        user_flow_script="docs/docusaurus/docs/reference/learn/data_quality_use_cases/distribution_resources/distribution_workflow.py",
        data_dir="tests/test_sets/learn_data_quality_use_cases/",
        util_script="tests/test_utils.py",
        backend_dependencies=[BackendDependencies.POSTGRESQL],
    ),
]

# Extend the docs_tests list with the above sublists (only the docs_tests list is imported
# into `test_script_runner.py` and actually used in CI checks).
docs_tests.extend(install_gx)

docs_tests.extend(try_gx)

docs_tests.extend(create_a_data_context)

docs_tests.extend(connect_to_filesystem_data_create_a_data_source)
docs_tests.extend(connect_to_filesystem_data_create_a_data_asset)
docs_tests.extend(connect_to_filesystem_data_create_a_batch_definition)

docs_tests.extend(connect_to_dataframe_data)

docs_tests.extend(docs_example_scripts_run_validations)

docs_tests.extend(example_scripts_for_define_expectations)

docs_tests.extend(docs_examples_customize_expectations)

docs_tests.extend(docs_examples_trigger_actions_based_on_validation_results)

docs_tests.extend(docs_example_configure_project_settings)

docs_tests.extend(docs_examples_configure_data_docs)

docs_tests.extend(learn_data_quality_use_cases)
