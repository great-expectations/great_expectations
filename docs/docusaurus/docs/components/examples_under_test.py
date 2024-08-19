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

docs_tests.extend(connect_to_filesystem_data_create_a_data_source)
docs_tests.extend(connect_to_filesystem_data_create_a_data_asset)
docs_tests.extend(connect_to_filesystem_data_create_a_batch_definition)

docs_tests.extend(learn_data_quality_use_cases)
