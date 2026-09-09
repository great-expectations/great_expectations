"""Run integration and docs tests.

Individual tests can be run by setting the '-k' flag and referencing the name of test, like the following example:
    pytest -v --docs-tests -k "test_docs[quickstart]" tests/integration/test_script_runner.py
"""  # noqa: E501 # FIXME CoP

import importlib.machinery
import importlib.metadata
import importlib.util
import logging
import os
import pathlib
import shutil
import subprocess
import sys
import traceback
from typing import List

import pandas
import pytest
import sqlalchemy
from docs.docusaurus.docs.components.examples_under_test import (
    docs_tests,
)
from flaky import flaky

from great_expectations.data_context.cloud_constants import GXCloudEnvironmentVariable
from great_expectations.data_context.data_context.file_data_context import (
    FileDataContext,
)
from great_expectations.data_context.util import file_relative_path
from tests.integration.backend_dependencies import BackendDependencies
from tests.integration.integration_test_fixture import (
    IntegrationTestFixture,
    substitute_test_buckets,
)
from tests.integration.test_definitions.abs.integration_tests import (
    abs_integration_tests,
)
from tests.integration.test_definitions.athena.integration_tests import (
    athena_integration_tests,
)
from tests.integration.test_definitions.aws_glue.integration_tests import (
    aws_glue_integration_tests,
)
from tests.integration.test_definitions.bigquery.integration_tests import (
    bigquery_integration_tests,
)
from tests.integration.test_definitions.gcs.integration_tests import (
    gcs_integration_tests,
)
from tests.integration.test_definitions.multiple_backend.integration_tests import (
    multiple_backend,
)
from tests.integration.test_definitions.mysql.integration_tests import (
    mysql_integration_tests,
)
from tests.integration.test_definitions.postgresql.integration_tests import (
    postgresql_integration_tests,
)
from tests.integration.test_definitions.redshift.integration_tests import (
    redshift_integration_tests,
)
from tests.integration.test_definitions.s3.integration_tests import s3_integration_tests
from tests.integration.test_definitions.snowflake.integration_tests import (
    snowflake_integration_tests,
)
from tests.integration.test_definitions.spark.integration_tests import (
    spark_integration_tests,
)
from tests.integration.test_definitions.sql_server.integration_tests import (
    sql_server_integration_tests,
)
from tests.integration.test_definitions.sqlite.integration_tests import (
    sqlite_integration_tests,
)
from tests.integration.test_definitions.trino.integration_tests import (
    trino_integration_tests,
)

pytestmark = pytest.mark.docs

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


import time


def delay_rerun(*args):
    """Delay for flaky tests

    Returns:
        True: After sleeping for 5 seconds.
    """
    time.sleep(5)
    return True


# to be populated by the smaller lists below
docs_test_matrix: List[IntegrationTestFixture] = []

local_tests = [
    # IntegrationTestFixture(
    #     name="how_to_add_validations_data_or_suites_to_a_checkpoint.py",
    #     user_flow_script="docs/docusaurus/docs/oss/guides/validation/checkpoints/how_to_add_validations_data_or_suites_to_a_checkpoint.py",  # noqa: E501 # FIXME CoP
    #     data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
    #     backend_dependencies=[],
    # ),
    # IntegrationTestFixture(
    #     name="how_to_validate_multiple_batches_within_single_checkpoint",
    #     user_flow_script="docs/docusaurus/docs/oss/guides/validation/checkpoints/how_to_validate_multiple_batches_within_single_checkpoint.py",  # noqa: E501 # FIXME CoP
    #     data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
    #     backend_dependencies=[BackendDependencies.PANDAS],
    # ),
    IntegrationTestFixture(
        name="expect_column_max_to_be_between_custom",
        user_flow_script="docs/docusaurus/docs/snippets/expect_column_max_to_be_between_custom.py",
        backend_dependencies=[],
    ),
    # IntegrationTestFixture(
    #     name="expect_column_values_to_be_in_solfege_scale_set",
    #     user_flow_script="docs/docusaurus/docs/oss/guides/expectations/creating_custom_expectations/expect_column_values_to_be_in_solfege_scale_set.py",  # noqa: E501 # FIXME CoP
    #     backend_dependencies=[],
    # ),
    # IntegrationTestFixture(
    #     name="expect_column_values_to_only_contain_vowels",
    #     user_flow_script="docs/docusaurus/docs/oss/guides/expectations/creating_custom_expectations/expect_column_values_to_only_contain_vowels.py",  # noqa: E501 # FIXME CoP
    #     backend_dependencies=[],
    # ),
    IntegrationTestFixture(
        name="how_to_configure_result_format_parameter",
        user_flow_script="docs/docusaurus/docs/snippets/result_format.py",
        backend_dependencies=[],
    ),
    # Fluent Datasources
    # IntegrationTestFixture(
    #     name="how_to_create_and_edit_expectations_with_instant_feedback_fluent",
    #     user_flow_script="docs/docusaurus/docs/oss/guides/expectations/how_to_create_and_edit_expectations_with_instant_feedback_fluent.py",  # noqa: E501 # FIXME CoP
    #     data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
    #     backend_dependencies=[],
    # ),
    # IntegrationTestFixture(
    #     name="data_docs",
    #     user_flow_script="docs/docusaurus/docs/oss/guides/setup/configuring_data_docs/data_docs.py",  # noqa: E501 # FIXME CoP
    #     data_context_dir="tests/integration/fixtures/yellow_trip_data_fluent_pandas/great_expectations",  # noqa: E501 # FIXME CoP
    #     data_dir="tests/test_sets/taxi_yellow_tripdata_samples",
    #     backend_dependencies=[],
    # ),
    # IntegrationTestFixture(
    #     name="how_to_edit_an_existing_expectation_suite",
    #     user_flow_script="docs/docusaurus/docs/oss/guides/expectations/how_to_edit_an_expectation_suite.py",  # noqa: E501 # FIXME CoP
    #     backend_dependencies=[],
    # ),
    IntegrationTestFixture(
        name="expectation_management",
        user_flow_script="tests/integration/docusaurus/expectations/expectation_management.py",
        data_context_dir=None,
        backend_dependencies=[],
    ),
]

quickstart = [
    IntegrationTestFixture(
        name="quickstart",
        user_flow_script="docs/docusaurus/docs/snippets/quickstart.py",
        backend_dependencies=[BackendDependencies.PANDAS],
    ),
]

fluent_datasources = [
    IntegrationTestFixture(
        name="connect_to_your_data_overview",
        data_context_dir=None,
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples",
        user_flow_script="docs/docusaurus/docs/snippets/connect_to_your_data_overview.py",
        backend_dependencies=[],
    ),
    # IntegrationTestFixture(
    #     name="how_to_pass_an_in_memory_dataframe_to_a_checkpoint",
    #     user_flow_script="docs/docusaurus/docs/oss/guides/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint.py",  # noqa: E501 # FIXME CoP
    #     data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
    #     data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
    #     backend_dependencies=[],
    # ),
    IntegrationTestFixture(
        name="glossary_batch_request",
        data_context_dir=None,
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples",
        user_flow_script="docs/docusaurus/docs/snippets/batch_request.py",
        backend_dependencies=[],
    ),
    # IntegrationTestFixture(
    #     name="how_to_create_and_edit_an_expectation_with_domain_knowledge",
    #     user_flow_script="docs/docusaurus/docs/oss/guides/expectations/how_to_create_and_edit_an_expectationsuite_domain_knowledge.py",  # noqa: E501 # FIXME CoP
    #     data_context_dir=None,
    #     backend_dependencies=[],
    # ),
    IntegrationTestFixture(
        name="how_to_request_data_from_a_data_asset",
        user_flow_script="docs/docusaurus/docs/snippets/get_existing_data_asset_from_existing_datasource_pandas_filesystem_example.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
        backend_dependencies=[BackendDependencies.PANDAS],
    ),
    # IntegrationTestFixture(
    #     name="how_to_organize_batches_in_a_file_based_data_asset",
    #     user_flow_script="docs/docusaurus/docs/oss/guides/connecting_to_your_data/fluent/data_assets/organize_batches_in_pandas_filesystem_datasource.py",  # noqa: E501 # FIXME CoP
    #     data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
    #     data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
    #     backend_dependencies=[BackendDependencies.PANDAS],
    # ),
    IntegrationTestFixture(
        name="how_to_organize_batches_in_a_sql_based_data_asset",
        user_flow_script="docs/docusaurus/docs/snippets/organize_batches_in_sqlite_datasource.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        backend_dependencies=[BackendDependencies.SQLALCHEMY],
    ),
    # IntegrationTestFixture(
    #     name="how_to_connect_to_one_or_more_files_using_pandas",
    #     user_flow_script="docs/docusaurus/docs/oss/guides/connecting_to_your_data/fluent/filesystem/how_to_connect_to_one_or_more_files_using_pandas.py",  # noqa: E501 # FIXME CoP
    #     data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
    #     data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
    #     backend_dependencies=[BackendDependencies.PANDAS],
    # ),
    # IntegrationTestFixture(
    #     name="how_to_connect_to_sql_data_using_a_query",
    #     user_flow_script="docs/docusaurus/docs/oss/guides/connecting_to_your_data/fluent/database/how_to_connect_to_sql_data_using_a_query.py",  # noqa: E501 # FIXME CoP
    #     data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
    #     backend_dependencies=[],
    # ),
    # IntegrationTestFixture(
    #     name="how_to_quickly_connect_to_a_single_file_with_pandas",
    #     user_flow_script="docs/docusaurus/docs/oss/guides/connecting_to_your_data/fluent/filesystem/how_to_quickly_connect_to_a_single_file_with_pandas.py",  # noqa: E501 # FIXME CoP
    #     data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
    #     backend_dependencies=[BackendDependencies.PANDAS],
    # ),
    # IntegrationTestFixture(
    #     name="how_to_connect_to_sqlite_data",
    #     user_flow_script="docs/docusaurus/docs/oss/guides/connecting_to_your_data/fluent/database/how_to_connect_to_sqlite_data.py",  # noqa: E501 # FIXME CoP
    #     data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
    #     backend_dependencies=[BackendDependencies.SQLALCHEMY],
    # ),
    IntegrationTestFixture(
        name="how_to_connect_to_a_sql_table",
        user_flow_script="docs/docusaurus/docs/snippets/how_to_connect_to_a_sql_table.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        backend_dependencies=[],
    ),
    # IntegrationTestFixture(
    #     name="how_to_connect_to_sql_data",
    #     user_flow_script="docs/docusaurus/docs/oss/guides/connecting_to_your_data/fluent/database/how_to_connect_to_sqlite_data.py",  # noqa: E501 # FIXME CoP
    #     data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
    #     backend_dependencies=[],
    # ),
    # IntegrationTestFixture(
    #     name="how_to_instantiate_a_specific_filesystem_data_context",
    #     user_flow_script="docs/docusaurus/docs/oss/guides/setup/configuring_data_contexts/instantiating_data_contexts/how_to_instantiate_a_specific_filesystem_data_context.py",  # noqa: E501 # FIXME CoP
    #     data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
    #     backend_dependencies=[],
    # ),
    # IntegrationTestFixture(
    #     name="how_to_initialize_a_filesystem_data_context_in_python",
    #     user_flow_script="docs/docusaurus/docs/oss/guides/setup/configuring_data_contexts/instantiating_data_contexts/how_to_initialize_a_filesystem_data_context_in_python.py",  # noqa: E501 # FIXME CoP
    #     data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
    #     backend_dependencies=[],
    # ),
    IntegrationTestFixture(
        name="how_to_explicitly_instantiate_an_ephemeral_data_context",
        user_flow_script="docs/docusaurus/docs/snippets/how_to_explicitly_instantiate_an_ephemeral_data_context.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        backend_dependencies=[],
    ),
    # IntegrationTestFixture(
    #     name="how_to_connect_to_in_memory_data_using_pandas",
    #     user_flow_script="docs/docusaurus/docs/oss/guides/connecting_to_your_data/fluent/in_memory/how_to_connect_to_in_memory_data_using_pandas.py",  # noqa: E501 # FIXME CoP
    #     data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
    #     backend_dependencies=[BackendDependencies.PANDAS],
    # ),
    # IntegrationTestFixture(
    #     name="how_to_connect_to_in_memory_data_using_spark",
    #     user_flow_script="docs/docusaurus/docs/oss/guides/connecting_to_your_data/fluent/in_memory/how_to_connect_to_in_memory_data_using_spark.py",  # noqa: E501 # FIXME CoP
    #     data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
    #     backend_dependencies=[BackendDependencies.SPARK],
    # ),
]

failed_rows_tests: List[IntegrationTestFixture] = [
    # IntegrationTestFixture(
    #     name="failed_rows_pandas",
    #     data_context_dir="tests/integration/fixtures/failed_rows/great_expectations",
    #     data_dir="tests/test_sets/visits",
    #     user_flow_script="docs/docusaurus/docs/oss/guides/expectations/advanced/failed_rows_pandas.py",  # noqa: E501 # FIXME CoP
    #     backend_dependencies=[],
    # ),
    # IntegrationTestFixture(
    #     name="failed_rows_sqlalchemy",
    #     data_context_dir="tests/integration/fixtures/failed_rows/great_expectations",
    #     data_dir="tests/test_sets/visits",
    #     user_flow_script="docs/docusaurus/docs/oss/guides/expectations/advanced/failed_rows_sql.py",  # noqa: E501 # FIXME CoP
    #     backend_dependencies=[BackendDependencies.SQLALCHEMY],
    # ),
    # IntegrationTestFixture(
    #     name="failed_rows_spark",
    #     data_context_dir="tests/integration/fixtures/failed_rows/great_expectations",
    #     data_dir="tests/test_sets/visits",
    #     user_flow_script="docs/docusaurus/docs/oss/guides/expectations/advanced/failed_rows_spark.py",  # noqa: E501 # FIXME CoP
    #     backend_dependencies=[BackendDependencies.SPARK],
    # ),
]


# populate docs_test_matrix with sub-lists
docs_test_matrix += docs_tests  # this has to go first. TODO: Fix in V1-481
docs_test_matrix += local_tests
docs_test_matrix += quickstart
docs_test_matrix += fluent_datasources
docs_test_matrix += spark_integration_tests
docs_test_matrix += sqlite_integration_tests
docs_test_matrix += mysql_integration_tests
docs_test_matrix += postgresql_integration_tests
docs_test_matrix += sql_server_integration_tests
docs_test_matrix += trino_integration_tests
docs_test_matrix += snowflake_integration_tests
docs_test_matrix += redshift_integration_tests
docs_test_matrix += bigquery_integration_tests
docs_test_matrix += gcs_integration_tests
docs_test_matrix += abs_integration_tests
docs_test_matrix += s3_integration_tests
docs_test_matrix += athena_integration_tests
docs_test_matrix += aws_glue_integration_tests
docs_test_matrix += multiple_backend
docs_test_matrix += failed_rows_tests

pandas_integration_tests: List[IntegrationTestFixture] = []

# populate integration_test_matrix with sub-lists
integration_test_matrix: List[IntegrationTestFixture] = []
integration_test_matrix += pandas_integration_tests


def idfn(test_configuration):
    return test_configuration.name


@pytest.fixture
def pytest_parsed_arguments(request):
    return request.config.option


@pytest.fixture
def prepare_cloud_env_vars(monkeypatch):
    """Fixture that returns a callable to prepare cloud environment variables."""

    def _prepare_cloud_env_vars_callable(backend_dependencies: list[BackendDependencies]):
        # This is necessary because Cloud environment variables
        # are always set in the pipeline (ci.yml).
        # Non-Cloud tests will try to instantiate
        # CloudDataContexts if these env vars are set, resulting in test failures
        if BackendDependencies.CLOUD not in backend_dependencies:
            monkeypatch.delenv(GXCloudEnvironmentVariable.BASE_URL, raising=False)
            monkeypatch.delenv(GXCloudEnvironmentVariable.ACCESS_TOKEN, raising=False)
            monkeypatch.delenv(GXCloudEnvironmentVariable.ORGANIZATION_ID, raising=False)
            monkeypatch.delenv(GXCloudEnvironmentVariable.WORKSPACE_ID, raising=False)

    return _prepare_cloud_env_vars_callable


@flaky(rerun_filter=delay_rerun, max_runs=3, min_passes=1)
@pytest.mark.parametrize("integration_test_fixture", docs_test_matrix, ids=idfn)
def test_docs(
    integration_test_fixture: IntegrationTestFixture,
    tmp_path: pathlib.Path,
    pytest_parsed_arguments,
    prepare_cloud_env_vars,
):
    _check_for_skipped_tests(pytest_parsed_arguments, integration_test_fixture)
    _execute_integration_test(
        integration_test_fixture=integration_test_fixture,
        tmp_path=tmp_path,
        prepare_cloud_env_vars=prepare_cloud_env_vars,
    )


@pytest.mark.parametrize("test_configuration", integration_test_matrix, ids=idfn)
@pytest.mark.slow  # 79.77s
def test_integration_tests(
    test_configuration: IntegrationTestFixture,
    tmp_path: pathlib.Path,
    pytest_parsed_arguments,
    prepare_cloud_env_vars,
):
    _check_for_skipped_tests(pytest_parsed_arguments, test_configuration)
    _execute_integration_test(
        integration_test_fixture=test_configuration,
        tmp_path=tmp_path,
        prepare_cloud_env_vars=prepare_cloud_env_vars,
    )


def _execute_shell_command(command: str) -> int:
    """Run a command through a shell, with the current working directory on PATH.

    :param command: shell command -- as if typed in a terminal window
    :return: exit status -- 0 if successful; any other value indicates an error
    """
    cwd: str = os.getcwd()  # noqa: PTH109

    path_env_var: str = os.pathsep.join([os.environ.get("PATH", os.defpath), cwd])
    env: dict = dict(os.environ, PATH=path_env_var)

    status_code: int = 0
    try:
        res: subprocess.CompletedProcess = subprocess.run(
            args=["bash", "-c", command],
            stdin=None,
            input=None,
            capture_output=True,
            shell=False,
            cwd=cwd,
            timeout=None,
            check=True,
            encoding=None,
            errors=None,
            text=True,
            env=env,
        )
        sh_out: str = res.stdout.strip()
        logger.info(sh_out)
    except subprocess.CalledProcessError as cpe:
        status_code = cpe.returncode
        sys.stderr.write(cpe.output)
        sys.stderr.flush()
        exception_message: str = "A Sub-Process call Exception occurred.\n"
        exception_traceback: str = traceback.format_exc()
        exception_message += (
            f'{type(cpe).__name__}: "{cpe!s}".  Traceback: "{exception_traceback}".'
        )
        logger.error(exception_message)  # noqa: TRY400

    return status_code


def _execute_integration_test(  # noqa: C901, PLR0915 # FIXME CoP
    integration_test_fixture: IntegrationTestFixture,
    tmp_path: pathlib.Path,
    prepare_cloud_env_vars,
):
    """
    Prepare and environment and run integration tests from a list of tests.

    Note that the only required parameter for a test in the matrix is
    `user_flow_script` and that all other parameters are optional.
    """
    workdir = pathlib.Path.cwd()
    try:
        base_dir = pathlib.Path(file_relative_path(__file__, "../../"))
        os.chdir(base_dir)
        # Ensure GX is installed in our environment
        installed_packages = [
            dist.metadata["name"].lower() for dist in importlib.metadata.distributions()
        ]
        if "great-expectations" not in installed_packages:
            _execute_shell_command("pip install .")
        os.chdir(tmp_path)

        # Build test state
        data_context_dir = integration_test_fixture.data_context_dir
        if data_context_dir:
            context_source_dir = base_dir / data_context_dir
            test_context_dir = tmp_path / FileDataContext.GX_DIR
            shutil.copytree(
                context_source_dir,
                test_context_dir,
            )
            substitute_test_buckets(test_context_dir / FileDataContext.GX_YML)

        # Test Data
        data_dir = integration_test_fixture.data_dir
        if data_dir:
            source_data_dir = base_dir / data_dir
            target_data_dir = tmp_path / "data"
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
                source_file = base_dir / file_paths[0]
                dest_file = tmp_path / file_paths[1]
                dest_dir = dest_file.parent
                if not dest_dir.exists():
                    dest_dir.mkdir()

                shutil.copyfile(src=source_file, dst=dest_file)

        # UAT Script
        user_flow_script = integration_test_fixture.user_flow_script
        script_source = base_dir / user_flow_script

        script_path = tmp_path / "test_script.py"
        shutil.copyfile(script_source, script_path)
        logger.debug(
            f"(_execute_integration_test) script_source -> {script_source} :: copied to {script_path}"  # noqa: E501 # FIXME CoP
        )
        if script_source.suffix != ".py":
            logger.error(f"{script_source} is not a python script!")
            text = script_source.read_text()
            print(f"contents of script_path:\n\n{text}\n\n")
            return

        util_script = integration_test_fixture.util_script
        if util_script:
            script_source = base_dir / util_script
            tmp_path.joinpath("tests/").mkdir()
            util_script_path = tmp_path / "tests/test_utils.py"
            shutil.copyfile(script_source, util_script_path)

        prepare_cloud_env_vars(
            backend_dependencies=integration_test_fixture.backend_dependencies,
        )

        # Run script as module, using python's importlib machinery (https://docs.python.org/3/library/importlib.htm)
        loader = importlib.machinery.SourceFileLoader("test_script_module", str(script_path))
        spec = importlib.util.spec_from_loader("test_script_module", loader)
        assert spec is not None
        test_script_module = importlib.util.module_from_spec(spec)
        loader.exec_module(test_script_module)
    except Exception as e:
        logger.error(str(e))  # noqa: TRY400 # FIXME CoP
        # Read straight from the fixture rather than the local `user_flow_script`, which may
        # not have been assigned yet if the exception was raised earlier in the try block.
        if "JavaPackage" in str(e) and "aws_glue" in integration_test_fixture.user_flow_script:
            logger.debug("This is something aws_glue related, so just going to return")
            # Should try to copy aws-glue-libs jar files to Spark jar during pipeline setup
            #   - see https://stackoverflow.com/a/67371827
            return
        else:
            raise
    finally:
        os.chdir(workdir)


def _check_for_skipped_tests(  # noqa: C901, PLR0912 # FIXME CoP
    pytest_args,
    integration_test_fixture,
) -> None:
    """Enable scripts to be skipped based on pytest invocation flags."""
    # Pandas 2.2 dropped support for SQLAlchemy < 2 in to_sql/read_sql_table; this list
    # captures the tests that rely on those code paths and therefore can't run under that
    # combination. See https://github.com/great-expectations/great_expectations/pull/11417.
    TESTS_TO_SKIP_UNDER_SQLA_LT_2_AND_PANDAS_GTE_2_2 = [
        "expect_column_max_to_be_between_custom",
    ]
    IS_RUNNING_SQLA_LT_2_AND_PANDAS_GTE_2_2 = (
        sqlalchemy.__version__ < "2.0" and pandas.__version__ >= "2.2"
    )
    if (
        IS_RUNNING_SQLA_LT_2_AND_PANDAS_GTE_2_2
        and integration_test_fixture.name in TESTS_TO_SKIP_UNDER_SQLA_LT_2_AND_PANDAS_GTE_2_2
    ):
        pytest.skip(
            "This test requires sqlalchemy version 2.0 or higher when running pandas >= 2.2"
        )
    # TEMPORARY: These docs examples connect to a hosted sample Postgres database
    # whose CI infrastructure is being torn down, so the connection fails during the
    # transition. They are gated by name (not the POSTGRESQL backend flag) because the
    # local-docker Postgres docs tests share that dependency and must keep running.
    # Remove this block to re-enable once the hosted database is back.
    TESTS_TO_SKIP_DURING_CI_TRANSITION = [
        "try_gx_end_to_end",
        "data_quality_use_case_distribution_workflow",
        "data_quality_use_case_freshness_workflow",
        "data_quality_use_case_integrity_workflow",
        "data_quality_use_case_uniqueness_workflow",
        "data_quality_use_case_volume_workflow",
    ]
    if integration_test_fixture.name in TESTS_TO_SKIP_DURING_CI_TRANSITION:
        pytest.skip("CI TRANSITION")
    dependencies = integration_test_fixture.backend_dependencies
    if not dependencies:
        return
    elif BackendDependencies.POSTGRESQL in dependencies and (
        not pytest_args.postgresql or pytest_args.no_sqlalchemy
    ):
        pytest.skip("Skipping postgres tests")
    elif BackendDependencies.MYSQL in dependencies and (
        not pytest_args.mysql or pytest_args.no_sqlalchemy
    ):
        pytest.skip("Skipping mysql tests")
    elif BackendDependencies.SQL_SERVER in dependencies and (
        not pytest_args.sql_server or pytest_args.no_sqlalchemy
    ):
        pytest.skip("Skipping SQL Server tests")
    elif BackendDependencies.BIGQUERY in dependencies and (
        pytest_args.no_sqlalchemy or not pytest_args.bigquery
    ):
        # TODO : Investigate whether this test should be handled by azure-pipelines-cloud-db-integration.yml  # noqa: E501 # FIXME CoP
        pytest.skip("Skipping bigquery tests")
    elif BackendDependencies.GCS in dependencies and not pytest_args.gcs:
        pytest.skip("Skipping GCS tests")
    elif BackendDependencies.AWS in dependencies and not pytest_args.aws:
        pytest.skip("Skipping AWS tests")
    elif BackendDependencies.REDSHIFT in dependencies and (
        pytest_args.no_sqlalchemy or not pytest_args.redshift
    ):
        pytest.skip("Skipping redshift tests")
    elif BackendDependencies.SPARK in dependencies and not pytest_args.spark:
        pytest.skip("Skipping spark tests")
    elif BackendDependencies.SNOWFLAKE in dependencies and (
        pytest_args.no_sqlalchemy or not pytest_args.snowflake
    ):
        pytest.skip("Skipping snowflake tests")
    elif BackendDependencies.AZURE in dependencies and not pytest_args.azure:
        pytest.skip("Skipping Azure tests")
    elif BackendDependencies.TRINO in dependencies and not pytest_args.trino:
        pytest.skip("Skipping Trino tests")
    elif BackendDependencies.ATHENA in dependencies and not pytest_args.athena:
        pytest.skip("Skipping Athena tests")
    elif BackendDependencies.CLOUD in dependencies and not pytest_args.cloud:
        pytest.skip("Skipping GX Cloud tests")
