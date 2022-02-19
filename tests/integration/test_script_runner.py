import enum
import importlib.machinery
import importlib.util
import logging
import os
import shutil
import sys
from dataclasses import dataclass
from typing import List, Optional, Tuple

import pytest

from assets.scripts.build_gallery import execute_shell_command
from great_expectations.data_context.util import file_relative_path

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class BackendDependencies(enum.Enum):
    AWS = "AWS"
    BIGQUERY = "BIGQUERY"
    GCS = "GCS"
    MYSQL = "MYSQL"
    MSSQL = "MSSQL"
    PANDAS = "PANDAS"
    POSTGRESQL = "POSTGRESQL"
    REDSHIFT = "REDSHIFT"
    SPARK = "SPARK"
    SQLALCHEMY = "SQLALCHEMY"
    SNOWFLAKE = "SNOWFLAKE"


@dataclass
class IntegrationTestFixture:
    """IntegrationTestFixture

    Configurations for integration tests are defined as IntegrationTestFixture dataclass objects.

    Individual tests can also be run by setting the '-k' flag and referencing the name of test, like the following example:
    pytest -v --docs-tests -m integration -k "test_docs[migration_guide_spark_v2_api]" tests/integration/test_script_runner.py

    Args:
        name: Name for integration test. Individual tests can be run by using the -k option and specifying the name of the test.
        user_flow_script: Required script for integration test.
        data_context_dir: Path of great_expectations/ that is used in the test.
        data_dir: Folder that contains data used in the test.
        extra_backend_dependencies: Optional flag allows you to tie an individual test with a BackendDependency. Allows for tests to be run / disabled using cli flags (like --aws which enables AWS integration tests).
        other_files: other files (like credential information) to copy into the test environment. These are presented as Tuple(path_to_source_file, path_to_target_file), where path_to_target_file is relative to the test_script.py file in our test environment
        util_script: Path of optional util script that is used in test script (for loading test_specific methods like load_data_into_test_database())
    """

    name: str
    user_flow_script: str
    data_context_dir: Optional[str] = None
    data_dir: Optional[str] = None
    extra_backend_dependencies: Optional[BackendDependencies] = None
    other_files: Optional[Tuple[Tuple[str, str]]] = None
    util_script: Optional[str] = None


# to be populated by the smaller lists below
docs_test_matrix: List[IntegrationTestFixture] = []

local_tests = [
    IntegrationTestFixture(
        name="how_to_create_a_batch_of_data_from_an_in_memory_pandas_dataframe",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/how_to_create_a_batch_of_data_from_an_in_memory_pandas_dataframe.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
    ),
    IntegrationTestFixture(
        name="how_to_create_a_batch_of_data_from_an_in_memory_spark_dataframe",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/how_to_create_a_batch_of_data_from_an_in_memory_spark_dataframe.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
        extra_backend_dependencies=BackendDependencies.SPARK,
    ),
    IntegrationTestFixture(
        name="getting_started",
        data_context_dir="tests/integration/fixtures/yellow_tripdata_pandas_fixture/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
        user_flow_script="tests/integration/docusaurus/tutorials/getting-started/getting_started.py",
    ),
    IntegrationTestFixture(
        name="how_to_get_a_batch_of_data_from_a_configured_datasource",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/how_to_get_a_batch_of_data_from_a_configured_datasource.py",
        data_context_dir="tests/integration/fixtures/yellow_tripdata_pandas_fixture/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples",
    ),
    IntegrationTestFixture(
        name="connecting_to_your_data_pandas_yaml",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/filesystem/pandas_yaml_example.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
    ),
    IntegrationTestFixture(
        name="connecting_to_your_data_pandas_python",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/filesystem/pandas_python_example.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
    ),
    IntegrationTestFixture(
        name="how_to_introspect_and_partition_your_data_yaml_gradual",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example_gradual.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
    ),
    IntegrationTestFixture(
        name="how_to_introspect_and_partition_your_data_yaml_complete",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example_complete.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
    ),
    IntegrationTestFixture(
        name="in_memory_pandas_yaml",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/in_memory/pandas_yaml_example.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
    ),
    IntegrationTestFixture(
        name="in_memory_pandas_python",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/in_memory/pandas_python_example.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
    ),
    IntegrationTestFixture(
        name="docusaurus_template_script_example",
        user_flow_script="tests/integration/docusaurus/template/script_example.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
    ),
    IntegrationTestFixture(
        name="in_memory_spark_yaml",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/in_memory/spark_yaml_example.py",
        extra_backend_dependencies=BackendDependencies.SPARK,
    ),
    IntegrationTestFixture(
        name="in_memory_spark_python",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/in_memory/spark_python_example.py",
        extra_backend_dependencies=BackendDependencies.SPARK,
    ),
    IntegrationTestFixture(
        name="filesystem_spark_yaml",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/filesystem/spark_yaml_example.py",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
        extra_backend_dependencies=BackendDependencies.SPARK,
    ),
    IntegrationTestFixture(
        name="filesystem_spark_python",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/filesystem/spark_python_example.py",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
        extra_backend_dependencies=BackendDependencies.SPARK,
    ),
    IntegrationTestFixture(
        name="how_to_choose_which_dataconnector_to_use",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/how_to_choose_which_dataconnector_to_use.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/dataconnector_docs",
    ),
    IntegrationTestFixture(
        name="how_to_configure_a_runtimedataconnector",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/how_to_configure_a_runtimedataconnector.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/dataconnector_docs",
    ),
    IntegrationTestFixture(
        name="rule_base_profiler_multi_batch_example",
        data_context_dir="tests/integration/fixtures/yellow_tripdata_pandas_fixture/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
        user_flow_script="tests/integration/docusaurus/expectations/advanced/multi_batch_rule_based_profiler_example.py",
    ),
    IntegrationTestFixture(
        name="databricks_deployment_patterns_file_yaml_configs",
        user_flow_script="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_yaml_configs.py",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
        extra_backend_dependencies=BackendDependencies.SPARK,
    ),
    IntegrationTestFixture(
        name="databricks_deployment_patterns_file_python_configs",
        user_flow_script="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_python_configs.py",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
        extra_backend_dependencies=BackendDependencies.SPARK,
    ),
    IntegrationTestFixture(
        name="databricks_deployment_patterns_file_yaml_configs",
        user_flow_script="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_yaml_configs.py",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
        extra_backend_dependencies=BackendDependencies.SPARK,
    ),
    IntegrationTestFixture(
        name="databricks_deployment_patterns_file_python_configs",
        user_flow_script="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
        extra_backend_dependencies=BackendDependencies.SPARK,
    ),
    IntegrationTestFixture(
        name="checkpoints_and_actions_core_concepts",
        user_flow_script="tests/integration/docusaurus/reference/core_concepts/checkpoints_and_actions.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
    ),
    IntegrationTestFixture(
        name="how_to_pass_an_in_memory_dataframe_to_a_checkpoint",
        user_flow_script="tests/integration/docusaurus/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
    ),
    IntegrationTestFixture(
        name="how_to_configure_credentials",
        user_flow_script="tests/integration/docusaurus/setup/configuring_data_contexts/how_to_configure_credentials.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
    ),
    IntegrationTestFixture(
        name="migration_guide_pandas_v3_api",
        user_flow_script="tests/integration/docusaurus/miscellaneous/migration_guide_pandas_v3_api.py",
        data_context_dir="tests/test_fixtures/configuration_for_testing_v2_v3_migration/pandas/v3/great_expectations/",
        data_dir="tests/test_fixtures/configuration_for_testing_v2_v3_migration/data",
    ),
    IntegrationTestFixture(
        name="migration_guide_pandas_v2_api",
        user_flow_script="tests/integration/docusaurus/miscellaneous/migration_guide_pandas_v2_api.py",
        data_context_dir="tests/test_fixtures/configuration_for_testing_v2_v3_migration/pandas/v2/great_expectations/",
        data_dir="tests/test_fixtures/configuration_for_testing_v2_v3_migration/data",
    ),
    IntegrationTestFixture(
        name="migration_guide_spark_v3_api",
        user_flow_script="tests/integration/docusaurus/miscellaneous/migration_guide_spark_v3_api.py",
        data_context_dir="tests/test_fixtures/configuration_for_testing_v2_v3_migration/spark/v3/great_expectations/",
        data_dir="tests/test_fixtures/configuration_for_testing_v2_v3_migration/data",
        extra_backend_dependencies=BackendDependencies.SPARK,
    ),
    IntegrationTestFixture(
        name="migration_guide_spark_v2_api",
        user_flow_script="tests/integration/docusaurus/miscellaneous/migration_guide_spark_v2_api.py",
        data_context_dir="tests/test_fixtures/configuration_for_testing_v2_v3_migration/spark/v2/great_expectations/",
        data_dir="tests/test_fixtures/configuration_for_testing_v2_v3_migration/data",
        extra_backend_dependencies=BackendDependencies.SPARK,
    ),
]


dockerized_db_tests = [
    IntegrationTestFixture(
        name="postgres_yaml_example",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/database/postgres_yaml_example.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
        util_script="tests/test_utils.py",
        extra_backend_dependencies=BackendDependencies.POSTGRESQL,
    ),
    IntegrationTestFixture(
        name="postgres_python_example",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/database/postgres_python_example.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
        util_script="tests/test_utils.py",
        extra_backend_dependencies=BackendDependencies.POSTGRESQL,
    ),
    IntegrationTestFixture(
        name="sqlite_yaml_example",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/database/sqlite_yaml_example.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/sqlite/",
        util_script="tests/test_utils.py",
        extra_backend_dependencies=BackendDependencies.SQLALCHEMY,
    ),
    IntegrationTestFixture(
        name="sqlite_python_example",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/database/sqlite_python_example.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/sqlite/",
        util_script="tests/test_utils.py",
        extra_backend_dependencies=BackendDependencies.SQLALCHEMY,
    ),
    IntegrationTestFixture(
        name="introspect_and_partition_yaml_example_gradual",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/sql_database/yaml_example_gradual.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/sqlite/",
        util_script="tests/test_utils.py",
        extra_backend_dependencies=BackendDependencies.SQLALCHEMY,
    ),
    IntegrationTestFixture(
        name="introspect_and_partition_yaml_example_complete",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/sql_database/yaml_example_complete.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/sqlite/",
        util_script="tests/test_utils.py",
        extra_backend_dependencies=BackendDependencies.SQLALCHEMY,
    ),
    IntegrationTestFixture(
        name="mssql_yaml_example",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/database/mssql_yaml_example.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
        util_script="tests/test_utils.py",
        extra_backend_dependencies=BackendDependencies.MSSQL,
    ),
    IntegrationTestFixture(
        name="mssql_python_example",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/database/mssql_python_example.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
        util_script="tests/test_utils.py",
        extra_backend_dependencies=BackendDependencies.MSSQL,
    ),
    IntegrationTestFixture(
        name="mysql_yaml_example",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/database/mysql_yaml_example.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
        util_script="tests/test_utils.py",
        extra_backend_dependencies=BackendDependencies.MYSQL,
    ),
    IntegrationTestFixture(
        name="mysql_python_example",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/database/mysql_python_example.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
        util_script="tests/test_utils.py",
        extra_backend_dependencies=BackendDependencies.MYSQL,
    ),
    IntegrationTestFixture(
        name="migration_guide_postgresql_v3_api",
        user_flow_script="tests/integration/docusaurus/miscellaneous/migration_guide_postgresql_v3_api.py",
        data_context_dir="tests/test_fixtures/configuration_for_testing_v2_v3_migration/postgresql/v3/great_expectations/",
        data_dir="tests/test_fixtures/configuration_for_testing_v2_v3_migration/data/",
        util_script="tests/test_utils.py",
        extra_backend_dependencies=BackendDependencies.POSTGRESQL,
    ),
    IntegrationTestFixture(
        name="migration_guide_postgresql_v2_api",
        user_flow_script="tests/integration/docusaurus/miscellaneous/migration_guide_postgresql_v2_api.py",
        data_context_dir="tests/test_fixtures/configuration_for_testing_v2_v3_migration/postgresql/v2/great_expectations/",
        data_dir="tests/test_fixtures/configuration_for_testing_v2_v3_migration/data/",
        util_script="tests/test_utils.py",
        extra_backend_dependencies=BackendDependencies.POSTGRESQL,
    ),
    IntegrationTestFixture(
        name="how_to_configure_credentials",
        user_flow_script="tests/integration/docusaurus/setup/configuring_data_contexts/how_to_configure_credentials.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        extra_backend_dependencies=BackendDependencies.POSTGRESQL,
    ),
]


# CLOUD
cloud_snowflake_tests = [
    IntegrationTestFixture(
        name="snowflake_python_example",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/database/snowflake_python_example.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
        extra_backend_dependencies=BackendDependencies.SNOWFLAKE,
        util_script="tests/test_utils.py",
    ),
    IntegrationTestFixture(
        name="snowflake_yaml_example",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/database/snowflake_yaml_example.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
        extra_backend_dependencies=BackendDependencies.SNOWFLAKE,
        util_script="tests/test_utils.py",
    ),
]

cloud_gcp_tests = [
    IntegrationTestFixture(
        name="gcp_deployment_patterns_file_gcs_yaml_configs",
        user_flow_script="tests/integration/docusaurus/deployment_patterns/gcp_deployment_patterns_file_gcs_yaml_configs.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        extra_backend_dependencies=BackendDependencies.GCS,
    ),
    IntegrationTestFixture(
        name="how_to_configure_an_expectation_store_in_gcs",
        user_flow_script="tests/integration/docusaurus/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_in_gcs.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        extra_backend_dependencies=BackendDependencies.GCS,
    ),
    IntegrationTestFixture(
        name="how_to_host_and_share_data_docs_on_gcs",
        user_flow_script="tests/integration/docusaurus/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_gcs.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
        extra_backend_dependencies=BackendDependencies.GCS,
    ),
    IntegrationTestFixture(
        name="how_to_configure_a_validation_result_store_in_gcs",
        user_flow_script="tests/integration/docusaurus/setup/configuring_metadata_stores/how_to_configure_a_validation_result_store_in_gcs.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
        extra_backend_dependencies=BackendDependencies.GCS,
    ),
    IntegrationTestFixture(
        name="gcs_pandas_configured_yaml",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/pandas/configured_yaml_example.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        extra_backend_dependencies=BackendDependencies.GCS,
    ),
    IntegrationTestFixture(
        name="gcs_pandas_configured_python",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/pandas/configured_python_example.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        extra_backend_dependencies=BackendDependencies.GCS,
    ),
    IntegrationTestFixture(
        name="gcs_pandas_inferred_and_runtime_yaml",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/pandas/inferred_and_runtime_yaml_example.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        extra_backend_dependencies=BackendDependencies.GCS,
    ),
    IntegrationTestFixture(
        name="gcs_pandas_inferred_and_runtime_python",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/pandas/inferred_and_runtime_python_example.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        extra_backend_dependencies=BackendDependencies.GCS,
    ),
    # TODO: <Alex>ALEX -- Implement GCS Configured YAML Example</Alex>
    # TODO: <Alex>ALEX -- uncomment next test once Spark in Azure Pipelines is enabled and GCS Configured YAML Example is implemented.</Alex>
    # IntegrationTestFixture(
    #     name = "gcs_spark_configured_yaml",
    #     user_flow_script= "tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/spark/configured_yaml_example.py",
    #     data_context_dir= "tests/integration/fixtures/no_datasources/great_expectations",
    #     extra_backend_dependencies= BackendDependencies.GCS,
    # ),
    # TODO: <Alex>ALEX -- Implement GCS Configured Python Example</Alex>
    # TODO: <Alex>ALEX -- uncomment next test once Spark in Azure Pipelines is enabled and GCS Configured Python Example is implemented.</Alex>
    # IntegrationTestFixture(
    #     name = "gcs_spark_configured_python",
    #     user_flow_script= "tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/spark/configured_python_example.py",
    #     data_context_dir= "tests/integration/fixtures/no_datasources/great_expectations",
    #     extra_backend_dependencies= BackendDependencies.GCS,
    # ),
    # TODO: <Alex>ALEX -- uncomment next two (2) tests once Spark in Azure Pipelines is enabled.</Alex>
    # IntegrationTestFixture(
    #     name = "gcs_spark_inferred_and_runtime_yaml",
    #     user_flow_script= "tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/spark/inferred_and_runtime_yaml_example.py",
    #     data_context_dir= "tests/integration/fixtures/no_datasources/great_expectations",
    #     extra_backend_dependencies= BackendDependencies.GCS,
    # ),
    # IntegrationTestFixture(
    #     name = "gcs_spark_inferred_and_runtime_python",
    #     user_flow_script= "tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/spark/inferred_and_runtime_python_example.py",
    #     data_context_dir= "tests/integration/fixtures/no_datasources/great_expectations",
    #     extra_backend_dependencies= BackendDependencies.GCS,
    # ),
]

cloud_bigquery_tests = [
    IntegrationTestFixture(
        name="bigquery_yaml_example",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/database/bigquery_yaml_example.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
        util_script="tests/test_utils.py",
        extra_backend_dependencies=BackendDependencies.BIGQUERY,
    ),
    IntegrationTestFixture(
        name="bigquery_python_example",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/database/bigquery_python_example.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
        util_script="tests/test_utils.py",
        extra_backend_dependencies=BackendDependencies.BIGQUERY,
    ),
    IntegrationTestFixture(
        name="gcp_deployment_patterns_file_bigquery_yaml_configs",
        user_flow_script="tests/integration/docusaurus/deployment_patterns/gcp_deployment_patterns_file_bigquery_yaml_configs.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        extra_backend_dependencies=BackendDependencies.BIGQUERY,
    ),
]

cloud_azure_tests = [
    IntegrationTestFixture(
        name="azure_pandas_configured_yaml",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/cloud/azure/pandas/configured_yaml_example.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
    ),
    IntegrationTestFixture(
        name="azure_pandas_configured_python",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/cloud/azure/pandas/configured_python_example.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
    ),
    IntegrationTestFixture(
        name="azure_pandas_inferred_and_runtime_yaml",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/cloud/azure/pandas/inferred_and_runtime_yaml_example.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
    ),
    IntegrationTestFixture(
        name="azure_pandas_inferred_and_runtime_python",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/cloud/azure/pandas/inferred_and_runtime_python_example.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
    ),
    # TODO: <Alex>ALEX -- uncomment next four (4) tests once Spark in Azure Pipelines is enabled.</Alex>
    # IntegrationTestFixture(
    #     name = "azure_spark_configured_yaml",
    #     user_flow_script= "tests/integration/docusaurus/connecting_to_your_data/cloud/azure/spark/configured_yaml_example.py",
    #     data_context_dir= "tests/integration/fixtures/no_datasources/great_expectations",
    # ),
    # IntegrationTestFixture(
    #     name = "azure_spark_configured_python",
    #     user_flow_script= "tests/integration/docusaurus/connecting_to_your_data/cloud/azure/spark/configured_python_example.py",
    #     data_context_dir= "tests/integration/fixtures/no_datasources/great_expectations",
    # ),
    # IntegrationTestFixture(
    #     name = "azure_spark_inferred_and_runtime_yaml",
    #     user_flow_script= "tests/integration/docusaurus/connecting_to_your_data/cloud/azure/spark/inferred_and_runtime_yaml_example.py",
    #     data_context_dir= "tests/integration/fixtures/no_datasources/great_expectations",
    # ),
    # IntegrationTestFixture(
    #     name = "azure_spark_inferred_and_runtime_python",
    #     user_flow_script= "tests/integration/docusaurus/connecting_to_your_data/cloud/azure/spark/inferred_and_runtime_python_example.py",
    #     data_context_dir= "tests/integration/fixtures/no_datasources/great_expectations",
    # ),
]

cloud_s3_tests = [
    IntegrationTestFixture(
        name="s3_pandas_inferred_and_runtime_yaml",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/cloud/s3/pandas/inferred_and_runtime_yaml_example.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        extra_backend_dependencies=BackendDependencies.AWS,
    ),
    IntegrationTestFixture(
        name="s3_pandas_inferred_and_runtime_python",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/cloud/s3/pandas/inferred_and_runtime_python_example.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        extra_backend_dependencies=BackendDependencies.AWS,
    ),
    IntegrationTestFixture(
        name="how_to_configure_an_inferredassetdataconnector",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/how_to_configure_an_inferredassetdataconnector.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/dataconnector_docs",
        extra_backend_dependencies=BackendDependencies.AWS,
    ),
    IntegrationTestFixture(
        name="how_to_configure_a_configuredassetdataconnector",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/how_to_configure_a_configuredassetdataconnector.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/dataconnector_docs",
        extra_backend_dependencies=BackendDependencies.AWS,
    ),
    # TODO: <Alex>ALEX -- uncomment all S3 tests once S3 testing in Azure Pipelines is re-enabled and items for specific tests below are addressed.</Alex>
    # TODO: <Alex>ALEX -- Implement S3 Configured YAML Example</Alex>
    # TODO: <Alex>ALEX -- uncomment next test once S3 Configured YAML Example is implemented.</Alex>
    # IntegrationTestFixture(
    #     name = "s3_pandas_configured_yaml_example",
    #     user_flow_script= "tests/integration/docusaurus/connecting_to_your_data/cloud/s3/pandas/configured_yaml_example.py",
    #     data_context_dir= "tests/integration/fixtures/no_datasources/great_expectations",
    #     extra_backend_dependencies= BackendDependencies.AWS,
    # ),
    # TODO: <Alex>ALEX -- Implement S3 Configured Python Example</Alex>
    # TODO: <Alex>ALEX -- uncomment next test once S3 Configured Python Example is implemented.</Alex>
    # IntegrationTestFixture(
    #     name = "s3_pandas_configured_python_example",
    #     user_flow_script= "tests/integration/docusaurus/connecting_to_your_data/cloud/s3/pandas/configured_python_example.py",
    #     data_context_dir= "tests/integration/fixtures/no_datasources/great_expectations",
    #     extra_backend_dependencies= BackendDependencies.AWS,
    # ),
    # TODO: <Alex>ALEX -- Implement S3 Configured YAML Example</Alex>
    # TODO: <Alex>ALEX -- uncomment next test once Spark in Azure Pipelines is enabled and S3 Configured YAML Example is implemented.</Alex>
    # IntegrationTestFixture(
    #     name = "s3_spark_configured_yaml_example",
    #     user_flow_script= "tests/integration/docusaurus/connecting_to_your_data/cloud/s3/spark/configured_yaml_example.py",
    #     extra_backend_dependencies= [BackendDependencies.SPARK, BackendDependencies.AWS],
    # ),
    # TODO: <Alex>ALEX -- Implement S3 Configured Python Example</Alex>
    # TODO: <Alex>ALEX -- uncomment next test once Spark in Azure Pipelines is enabled and S3 Configured Python Example is implemented.</Alex>
    # IntegrationTestFixture(
    #     name = "s3_spark_configured_python_example",
    #     user_flow_script= "tests/integration/docusaurus/connecting_to_your_data/cloud/s3/spark/configured_python_example.py",
    #     extra_backend_dependencies= [BackendDependencies.SPARK, BackendDependencies.AWS],
    # ),
    # TODO: <Alex>ALEX -- uncomment next two (2) tests once Spark in Azure Pipelines is enabled.</Alex>
    # IntegrationTestFixture(
    #     name = "s3_spark_inferred_and_runtime_yaml_example",
    #     user_flow_script= "tests/integration/docusaurus/connecting_to_your_data/cloud/s3/spark/inferred_and_runtime_yaml_example.py",
    #     data_context_dir= "tests/integration/fixtures/no_datasources/great_expectations",
    #     extra_backend_dependencies= [BackendDependencies.SPARK, BackendDependencies.AWS],
    # ),
    # IntegrationTestFixture(
    #     name = "s3_spark_inferred_and_runtime_python_example",
    #     user_flow_script= "tests/integration/docusaurus/connecting_to_your_data/cloud/s3/spark/inferred_and_runtime_python_example.py",
    #     data_context_dir= "tests/integration/fixtures/no_datasources/great_expectations",
    #     extra_backend_dependencies= [BackendDependencies.SPARK, BackendDependencies.AWS],
    # ),
]

cloud_redshift_tests = [
    # TODO: <Alex>ALEX: Rename test modules to include "configured" and "inferred_and_runtime" suffixes in names.</Alex>
    # IntegrationTestFixture(
    #     name = "azure_python_example",
    #     user_flow_script= "tests/integration/docusaurus/connecting_to_your_data/database/redshift_python_example.py",
    #     data_context_dir= "tests/integration/fixtures/no_datasources/great_expectations",
    #     data_dir= "tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
    #     extra_backend_dependencies= [BackendDependencies.AWS, BackendDependencies.REDSHIFT],
    #     util_script= "tests/test_utils.py",
    # ),
    # IntegrationTestFixture(
    #     name = "azure_yaml_example",
    #     user_flow_script= "tests/integration/docusaurus/connecting_to_your_data/database/redshift_yaml_example.py",
    #     data_context_dir= "tests/integration/fixtures/no_datasources/great_expectations",
    #     data_dir= "tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
    #     extra_backend_dependencies= [BackendDependencies.AWS, BackendDependencies.REDSHIFT],
    #     util_script= "tests/test_utils.py",
    # ),
]

# populate docs_test_matrix with sub-lists
docs_test_matrix += local_tests
docs_test_matrix += dockerized_db_tests
docs_test_matrix += cloud_snowflake_tests
docs_test_matrix += cloud_gcp_tests
docs_test_matrix += cloud_bigquery_tests
docs_test_matrix += cloud_azure_tests
docs_test_matrix += cloud_s3_tests
docs_test_matrix += cloud_redshift_tests
docs_test_matrix += dockerized_db_tests

pandas_integration_tests = [
    IntegrationTestFixture(
        name="pandas_one_multi_batch_request_one_validator",
        data_context_dir="tests/integration/fixtures/yellow_tripdata_pandas_fixture/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples",
        user_flow_script="tests/integration/fixtures/yellow_tripdata_pandas_fixture/one_multi_batch_request_one_validator.py",
    ),
    IntegrationTestFixture(
        name="pandas_two_batch_requests_two_validators",
        data_context_dir="tests/integration/fixtures/yellow_tripdata_pandas_fixture/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples",
        user_flow_script="tests/integration/fixtures/yellow_tripdata_pandas_fixture/two_batch_requests_two_validators.py",
    ),
    IntegrationTestFixture(
        name="pandas_multiple_batch_requests_one_validator_multiple_steps",
        data_context_dir="tests/integration/fixtures/yellow_tripdata_pandas_fixture/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples",
        user_flow_script="tests/integration/fixtures/yellow_tripdata_pandas_fixture/multiple_batch_requests_one_validator_multiple_steps.py",
    ),
    IntegrationTestFixture(
        name="pandas_multiple_batch_requests_one_validator_one_step",
        data_context_dir="tests/integration/fixtures/yellow_tripdata_pandas_fixture/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples",
        user_flow_script="tests/integration/fixtures/yellow_tripdata_pandas_fixture/multiple_batch_requests_one_validator_one_step.py",
    ),
    IntegrationTestFixture(
        name="pandas_execution_engine_with_gcp_installed",
        data_context_dir="tests/integration/fixtures/yellow_tripdata_pandas_fixture/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples",
        user_flow_script="tests/integration/common_workflows/pandas_execution_engine_with_gcp_installed.py",
        other_files=(
            (
                "tests/integration/fixtures/cloud_provider_configs/gcp/my_example_creds.json",
                ".gcs/my_example_creds.json",
            ),
        ),
    ),
    IntegrationTestFixture(
        name="build_data_docs",
        user_flow_script="tests/integration/common_workflows/simple_build_data_docs.py",
    ),
]
aws_integration_tests = [
    IntegrationTestFixture(
        name="awsathena_test",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        user_flow_script="tests/integration/db/awsathena.py",
        extra_backend_dependencies=BackendDependencies.AWS,
        util_script="tests/test_utils.py",
    )
]

# populate integration_test_matrix with sub-lists
integration_test_matrix = []
integration_test_matrix += aws_integration_tests
integration_test_matrix += pandas_integration_tests


def idfn(test_configuration):
    return test_configuration.name


@pytest.fixture
def pytest_parsed_arguments(request):
    return request.config.option


@pytest.mark.docs
@pytest.mark.integration
@pytest.mark.parametrize("integration_test_fixture", docs_test_matrix, ids=idfn)
@pytest.mark.skipif(sys.version_info < (3, 7), reason="requires Python3.7")
def test_docs(integration_test_fixture, tmp_path, pytest_parsed_arguments):
    _check_for_skipped_tests(pytest_parsed_arguments, integration_test_fixture)
    _execute_integration_test(integration_test_fixture, tmp_path)


@pytest.mark.integration
@pytest.mark.parametrize("test_configuration", integration_test_matrix, ids=idfn)
@pytest.mark.skipif(sys.version_info < (3, 7), reason="requires Python3.7")
def test_integration_tests(test_configuration, tmp_path, pytest_parsed_arguments):
    _check_for_skipped_tests(pytest_parsed_arguments, test_configuration)
    _execute_integration_test(test_configuration, tmp_path)


def _execute_integration_test(integration_test_fixture, tmp_path):
    """
    Prepare and environment and run integration tests from a list of tests.

    Note that the only required parameter for a test in the matrix is
    `user_flow_script` and that all other parameters are optional.
    """
    workdir = os.getcwd()
    try:
        base_dir = file_relative_path(__file__, "../../")
        os.chdir(base_dir)
        # Ensure GE is installed in our environment
        execute_shell_command("pip install .")
        os.chdir(tmp_path)

        #
        # Build test state
        # DataContext
        data_context_dir = integration_test_fixture.data_context_dir
        if data_context_dir:
            context_source_dir = os.path.join(base_dir, data_context_dir)
            test_context_dir = os.path.join(tmp_path, "great_expectations")
            shutil.copytree(
                context_source_dir,
                test_context_dir,
            )

        # Test Data
        data_dir = integration_test_fixture.data_dir
        if data_dir:
            source_data_dir = os.path.join(base_dir, data_dir)
            target_data_dir = os.path.join(tmp_path, "data")
            shutil.copytree(
                source_data_dir,
                target_data_dir,
            )

        # Other files
        # Other files to copy should be supplied as a tuple of tuples with source, dest pairs
        # e.g. (("/source1/file1", "/dest1/file1"), ("/source2/file2", "/dest2/file2"))
        other_files = integration_test_fixture.other_files
        if other_files:
            for file_paths in other_files:
                source_file = os.path.join(base_dir, file_paths[0])
                dest_file = os.path.join(tmp_path, file_paths[1])
                dest_dir = os.path.dirname(dest_file)
                if not os.path.exists(dest_dir):
                    os.makedirs(dest_dir)
                shutil.copyfile(src=source_file, dst=dest_file)

        # UAT Script
        user_flow_script = integration_test_fixture.user_flow_script
        script_source = os.path.join(
            base_dir,
            user_flow_script,
        )
        script_path = os.path.join(tmp_path, "test_script.py")
        shutil.copyfile(script_source, script_path)

        util_script = integration_test_fixture.util_script
        if util_script:
            script_source = os.path.join(base_dir, util_script)
            os.makedirs(os.path.join(tmp_path, "tests/"))
            util_script_path = os.path.join(tmp_path, "tests/test_utils.py")
            shutil.copyfile(script_source, util_script_path)

        # Run script as module, using python's importlib machinery (https://docs.python.org/3/library/importlib.htm)
        loader = importlib.machinery.SourceFileLoader("test_script_module", script_path)
        spec = importlib.util.spec_from_loader("test_script_module", loader)
        test_script_module = importlib.util.module_from_spec(spec)
        loader.exec_module(test_script_module)
    except Exception as e:
        logger.error(str(e))
        raise
    finally:
        os.chdir(workdir)


def _check_for_skipped_tests(pytest_args, integration_test_fixture) -> None:
    """Enable scripts to be skipped based on pytest invocation flags."""
    dependencies = integration_test_fixture.extra_backend_dependencies
    if not dependencies:
        return
    elif dependencies == BackendDependencies.POSTGRESQL and (
        pytest_args.no_postgresql or pytest_args.no_sqlalchemy
    ):
        pytest.skip("Skipping postgres tests")
    elif dependencies == BackendDependencies.MYSQL and (
        not pytest_args.mysql or pytest_args.no_sqlalchemy
    ):
        pytest.skip("Skipping mysql tests")
    elif dependencies == BackendDependencies.MSSQL and (
        not pytest_args.mssql or pytest_args.no_sqlalchemy
    ):
        pytest.skip("Skipping mssql tests")
    elif dependencies == BackendDependencies.BIGQUERY and (
        pytest_args.no_sqlalchemy or not pytest_args.bigquery
    ):
        pytest.skip("Skipping bigquery tests")
    elif dependencies == BackendDependencies.GCS and not pytest_args.bigquery:
        pytest.skip("Skipping GCS tests")
    elif dependencies == BackendDependencies.AWS and not pytest_args.aws:
        pytest.skip("Skipping AWS tests")
    elif dependencies == BackendDependencies.REDSHIFT and pytest_args.no_sqlalchemy:
        pytest.skip("Skipping redshift tests")
    elif dependencies == BackendDependencies.SPARK and pytest_args.no_spark:
        pytest.skip("Skipping spark tests")
    elif dependencies == BackendDependencies.SNOWFLAKE and pytest_args.no_sqlalchemy:
        pytest.skip("Skipping snowflake tests")
