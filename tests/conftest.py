from __future__ import annotations

import copy
import datetime
import locale
import logging
import os
import pathlib
import random
import shutil
import urllib.parse
import warnings
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, Final, Generator, List, Optional
from unittest import mock

import numpy as np
import packaging
import pandas as pd
import pytest

import great_expectations as gx
from great_expectations.analytics.config import ENV_CONFIG
from great_expectations.compatibility import pyspark
from great_expectations.compatibility.sqlalchemy_compatibility_wrappers import (
    add_dataframe_to_db,
)
from great_expectations.core.batch_definition import BatchDefinition
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.expectation_validation_result import (
    ExpectationValidationResult,
)
from great_expectations.core.metric_function_types import MetricPartialFunctionTypes
from great_expectations.core.validation_definition import ValidationDefinition
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context import (
    AbstractDataContext,
    CloudDataContext,
    get_context,
)
from great_expectations.data_context._version_checker import _VersionChecker
from great_expectations.data_context.cloud_constants import (
    GXCloudEnvironmentVariable,
)
from great_expectations.data_context.data_context.context_factory import (
    project_manager,
    set_context,
)
from great_expectations.data_context.data_context.ephemeral_data_context import (
    EphemeralDataContext,
)
from great_expectations.data_context.data_context.file_data_context import (
    FileDataContext,
)
from great_expectations.data_context.store.gx_cloud_store_backend import (
    GXCloudStoreBackend,
)
from great_expectations.data_context.types.base import (
    DataContextConfig,
    GXCloudConfig,
    InMemoryStoreBackendDefaults,
)
from great_expectations.data_context.types.resource_identifiers import (
    ExpectationSuiteIdentifier,
)
from great_expectations.data_context.util import (
    file_relative_path,
)
from great_expectations.datasource.fluent import GxDatasourceWarning, PandasDatasource
from great_expectations.execution_engine import SparkDFExecutionEngine
from great_expectations.expectations.expectation_configuration import (
    ExpectationConfiguration,
)
from great_expectations.self_check.util import (
    build_test_backends_list as build_test_backends_list_v3,
)
from great_expectations.self_check.util import (
    expectationSuiteValidationResultSchema,
)
from great_expectations.util import (
    build_in_memory_runtime_context,
    is_library_loadable,
)
from great_expectations.validator.metric_configuration import MetricConfiguration
from great_expectations.validator.validator import Validator
from tests.datasource.fluent._fake_cloud_api import (
    DUMMY_JWT_TOKEN,
    FAKE_ORG_ID,
    GX_CLOUD_MOCK_BASE_URL,
    CloudDetails,
    gx_cloud_api_fake_ctx,
)

if TYPE_CHECKING:
    from unittest.mock import MagicMock  # noqa: TID251 # type-checking only

    from pytest_mock import MockerFixture

    from great_expectations.compatibility.sqlalchemy import Engine

yaml = YAMLHandler()
###
#
# NOTE: THESE TESTS ARE WRITTEN WITH THE en_US.UTF-8 LOCALE AS DEFAULT FOR STRING FORMATTING
#
###

locale.setlocale(locale.LC_ALL, "en_US.UTF-8")

logger = logging.getLogger(__name__)

REQUIRED_MARKERS: Final[set[str]] = {
    "all_backends",
    "athena",
    "aws_creds",
    "aws_deps",
    "big",
    "cli",
    "clickhouse",
    "cloud",
    "databricks",
    "docs",
    "filesystem",
    "mssql",
    "mysql",
    "openpyxl",
    "performance",
    "postgresql",
    "project",
    "pyarrow",
    "snowflake",
    "spark",
    "spark_connect",
    "sqlite",
    "trino",
    "unit",
}


@pytest.fixture()
def unset_gx_env_variables(monkeypatch: pytest.MonkeyPatch) -> None:
    for var in GXCloudEnvironmentVariable:
        monkeypatch.delenv(var, raising=False)


@pytest.mark.order(index=2)
@pytest.fixture(scope="module")
def spark_warehouse_session(tmp_path_factory):
    # Note this fixture will configure spark to use in-memory metastore
    pytest.importorskip("pyspark")

    spark_warehouse_path: str = str(tmp_path_factory.mktemp("spark-warehouse"))
    spark: pyspark.SparkSession = SparkDFExecutionEngine.get_or_create_spark_session(
        spark_config={
            "spark.sql.warehouse.dir": spark_warehouse_path,
        }
    )
    yield spark
    spark.stop()


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "smoketest: mark test as smoketest--it does not have useful assertions but may produce side effects "  # noqa: E501
        "that require manual inspection.",
    )
    config.addinivalue_line(
        "markers",
        "rendered_output: produces rendered output that should be manually reviewed.",
    )
    config.addinivalue_line(
        "markers",
        "aws_integration: runs aws integration test that may be very slow and requires credentials",
    )
    config.addinivalue_line(
        "markers",
        "cloud: runs GX Cloud tests that may be slow and requires credentials",
    )


def pytest_addoption(parser):
    parser.addoption(
        "--verify-marker-coverage-and-exit",
        action="store_true",
        help="If set, checks that all tests have one of the markers necessary " "for it to be run.",
    )

    # note: --no-spark will be deprecated in favor of --spark
    parser.addoption(
        "--no-spark",
        action="store_true",
        help="If set, suppress tests against the spark test suite",
    )
    parser.addoption(
        "--spark",
        action="store_true",
        help="If set, execute tests against the spark test suite",
    )
    parser.addoption(
        "--spark_connect",
        action="store_true",
        help="If set, execute tests against the spark-connect test suite",
    )
    parser.addoption(
        "--no-sqlalchemy",
        action="store_true",
        help="If set, suppress all tests using sqlalchemy",
    )
    parser.addoption(
        "--postgresql",
        action="store_true",
        help="If set, execute tests against postgresql",
    )
    # note: --no-postgresql will be deprecated in favor of --postgresql
    parser.addoption(
        "--no-postgresql",
        action="store_true",
        help="If set, supress tests against postgresql",
    )
    parser.addoption(
        "--mysql",
        action="store_true",
        help="If set, execute tests against mysql",
    )
    parser.addoption(
        "--mssql",
        action="store_true",
        help="If set, execute tests against mssql",
    )
    parser.addoption(
        "--bigquery",
        action="store_true",
        help="If set, execute tests against bigquery",
    )
    parser.addoption(
        "--aws",
        action="store_true",
        help="If set, execute tests against AWS resources like S3, RedShift and Athena",
    )
    parser.addoption(
        "--trino",
        action="store_true",
        help="If set, execute tests against trino",
    )
    parser.addoption(
        "--redshift",
        action="store_true",
        help="If set, execute tests against redshift",
    )
    parser.addoption(
        "--athena",
        action="store_true",
        help="If set, execute tests against athena",
    )
    parser.addoption(
        "--snowflake",
        action="store_true",
        help="If set, execute tests against snowflake",
    )
    parser.addoption(
        "--clickhouse",
        action="store_true",
        help="If set, execute tests against clickhouse",
    )
    parser.addoption(
        "--docs-tests",
        action="store_true",
        help="If set, run integration tests for docs",
    )
    parser.addoption("--azure", action="store_true", help="If set, execute tests against Azure")
    parser.addoption(
        "--cloud",
        action="store_true",
        help="If set, execute tests against GX Cloud API",
    )
    parser.addoption(
        "--performance-tests",
        action="store_true",
        help="If set, run performance tests (which might also require additional arguments like --bigquery)",  # noqa: E501
    )


def build_test_backends_list_v2_api(metafunc):
    test_backend_names: List[str] = build_test_backends_list_v3_api(metafunc)
    return test_backend_names


def build_test_backends_list_v3_api(metafunc):
    # adding deprecation warnings
    if metafunc.config.getoption("--no-postgresql"):
        warnings.warn(
            "--no-sqlalchemy is deprecated as of v0.14 in favor of the --postgresql flag. It will be removed in v0.16. Please adjust your tests accordingly",  # noqa: E501
            DeprecationWarning,
        )
    if metafunc.config.getoption("--no-spark"):
        warnings.warn(
            "--no-spark is deprecated as of v0.14 in favor of the --spark flag. It will be removed in v0.16. Please adjust your tests accordingly.",  # noqa: E501
            DeprecationWarning,
        )
    include_pandas: bool = True
    include_spark: bool = metafunc.config.getoption("--spark")
    include_sqlalchemy: bool = not metafunc.config.getoption("--no-sqlalchemy")
    include_postgresql: bool = metafunc.config.getoption("--postgresql")
    include_mysql: bool = metafunc.config.getoption("--mysql")
    include_mssql: bool = metafunc.config.getoption("--mssql")
    include_bigquery: bool = metafunc.config.getoption("--bigquery")
    include_aws: bool = metafunc.config.getoption("--aws")
    include_trino: bool = metafunc.config.getoption("--trino")
    include_azure: bool = metafunc.config.getoption("--azure")
    include_redshift: bool = metafunc.config.getoption("--redshift")
    include_athena: bool = metafunc.config.getoption("--athena")
    include_snowflake: bool = metafunc.config.getoption("--snowflake")
    include_clickhouse: bool = metafunc.config.getoption("--clickhouse")
    test_backend_names: List[str] = build_test_backends_list_v3(
        include_pandas=include_pandas,
        include_spark=include_spark,
        include_sqlalchemy=include_sqlalchemy,
        include_postgresql=include_postgresql,
        include_mysql=include_mysql,
        include_mssql=include_mssql,
        include_bigquery=include_bigquery,
        include_aws=include_aws,
        include_trino=include_trino,
        include_azure=include_azure,
        include_redshift=include_redshift,
        include_athena=include_athena,
        include_snowflake=include_snowflake,
        include_clickhouse=include_clickhouse,
    )
    return test_backend_names


def pytest_generate_tests(metafunc):
    test_backends = build_test_backends_list_v2_api(metafunc)
    if "test_backend" in metafunc.fixturenames:
        metafunc.parametrize("test_backend", test_backends, scope="module")
    if "test_backends" in metafunc.fixturenames:
        metafunc.parametrize("test_backends", [test_backends], scope="module")


@dataclass(frozen=True)
class TestMarkerCoverage:
    path: str
    name: str
    markers: set[str]

    def __str__(self):  # type: ignore[explicit-override] # FIXME
        return f"{self.path}, {self.name}, {self.markers}"


def _verify_marker_coverage(
    session,
) -> tuple[list[TestMarkerCoverage], list[TestMarkerCoverage]]:
    uncovered: list[TestMarkerCoverage] = []
    multiple_markers: list[TestMarkerCoverage] = []
    for test in session.items:
        markers = {m.name for m in test.iter_markers()}
        required_intersection = markers.intersection(REQUIRED_MARKERS)
        required_intersection_size = len(required_intersection)
        # required_intersection_size is a non-zero integer so there 3 cases we care about:
        #  0 => no marker coverage for this test
        #  1 => the marker coverage for this test is correct
        # >1 => too many markers are covering this test
        if required_intersection_size == 0:
            uncovered.append(
                TestMarkerCoverage(path=str(test.path), name=test.name, markers=markers)
            )
        elif required_intersection_size > 1:
            multiple_markers.append(
                TestMarkerCoverage(
                    path=str(test.path), name=test.name, markers=required_intersection
                )
            )
    return uncovered, multiple_markers


def pytest_collection_finish(session):
    if session.config.option.verify_marker_coverage_and_exit:
        uncovered, multiply_covered = _verify_marker_coverage(session)
        if uncovered or multiply_covered:
            print("*** Every test should be covered by exactly 1 of our required markers ***")
            if uncovered:
                print(f"*** {len(uncovered)} tests have no marker coverage ***")
                for test_info in uncovered:
                    print(test_info)
                print()
            else:
                print(f"*** {len(multiply_covered)} tests have multiple marker coverage ***")
                for test_info in multiply_covered:
                    print(test_info)
                print()

            print("*** The required markers follow. ***")
            print(
                "*** Tests marked with 'performance' are not run in the PR or release pipeline. ***"
            )
            print("*** All other tests are. ***")
            for m in REQUIRED_MARKERS:
                print(m)
            pytest.exit(
                reason="Marker coverage verification failed",
                returncode=pytest.ExitCode.TESTS_FAILED,
            )
        pytest.exit(
            reason="Marker coverage verification succeeded",
            returncode=pytest.ExitCode.OK,
        )


def pytest_collection_modifyitems(config, items):
    @dataclass
    class Category:
        mark: str
        flag: str
        reason: str

    categories = (
        Category(
            mark="docs",
            flag="--docs-tests",
            reason="need --docs-tests option to run",
        ),
        Category(mark="cloud", flag="--cloud", reason="need --cloud option to run"),
    )

    for category in categories:
        # If flag is provided, exit early so we don't add `pytest.mark.skip`
        if config.getoption(category.flag):
            continue

        # For each test collected, check if they use a mark that matches our flag name.
        # If so, add a `pytest.mark.skip` dynamically.
        for item in items:
            if category.mark in item.keywords:
                marker = pytest.mark.skip(reason=category.reason)
                item.add_marker(marker)


@pytest.fixture(autouse=True)
def no_usage_stats(monkeypatch):
    # Do not generate usage stats from test runs
    monkeypatch.setattr(ENV_CONFIG, "gx_analytics_enabled", False)


@pytest.fixture(scope="session", autouse=True)
def preload_latest_gx_cache():
    """
    Pre-load the _VersionChecker version cache so that we don't attempt to call pypi
    when creating contexts as part of normal testing.
    """
    # setup
    import great_expectations as gx

    current_version = packaging.version.Version(gx.__version__)
    logger.info(f"Seeding _VersionChecker._LATEST_GX_VERSION_CACHE with {current_version}")
    _VersionChecker._LATEST_GX_VERSION_CACHE = current_version
    yield current_version
    # teardown
    logger.info("Clearing _VersionChecker._LATEST_GX_VERSION_CACHE ")
    _VersionChecker._LATEST_GX_VERSION_CACHE = None


@pytest.fixture(scope="module")
def sa(test_backends):
    if not any(
        dbms in test_backends
        for dbms in [
            "postgresql",
            "sqlite",
            "mysql",
            "mssql",
            "bigquery",
            "trino",
            "redshift",
            "athena",
            "snowflake",
        ]
    ):
        pytest.skip("No recognized sqlalchemy backend selected.")
    else:
        try:
            from great_expectations.compatibility.sqlalchemy import sqlalchemy as sa

            return sa
        except ImportError:
            raise ValueError("SQL Database tests require sqlalchemy to be installed.")


@pytest.mark.order(index=2)
@pytest.fixture
def spark_session(test_backends) -> pyspark.SparkSession:
    from great_expectations.compatibility import pyspark

    if pyspark.SparkSession:  # type: ignore[truthy-function]
        return SparkDFExecutionEngine.get_or_create_spark_session()

    raise ValueError("spark tests are requested, but pyspark is not installed")


@pytest.fixture
def spark_connect_session(test_backends):
    from great_expectations.compatibility import pyspark

    if pyspark.SparkConnectSession:  # type: ignore[truthy-function]
        spark_connect_session = pyspark.SparkSession.builder.remote(
            "sc://localhost:15002"
        ).getOrCreate()
        assert isinstance(spark_connect_session, pyspark.SparkConnectSession)
        return spark_connect_session

    raise ValueError("spark tests are requested, but pyspark is not installed")


@pytest.fixture
def basic_spark_df_execution_engine(spark_session):
    from great_expectations.execution_engine import SparkDFExecutionEngine

    conf: List[tuple] = spark_session.sparkContext.getConf().getAll()
    spark_config: Dict[str, Any] = dict(conf)
    execution_engine = SparkDFExecutionEngine(
        spark_config=spark_config,
    )
    return execution_engine


@pytest.fixture
def spark_df_taxi_data_schema(spark_session):
    """
    Fixture used by tests for providing schema to SparkDFExecutionEngine.
    The schema returned by this fixture corresponds to taxi_tripdata
    """

    # will not import unless we have a spark_session already passed in as fixture
    from great_expectations.compatibility import pyspark

    schema = pyspark.types.StructType(
        [
            pyspark.types.StructField("vendor_id", pyspark.types.IntegerType(), True, None),
            pyspark.types.StructField("pickup_datetime", pyspark.types.TimestampType(), True, None),
            pyspark.types.StructField(
                "dropoff_datetime", pyspark.types.TimestampType(), True, None
            ),
            pyspark.types.StructField("passenger_count", pyspark.types.IntegerType(), True, None),
            pyspark.types.StructField("trip_distance", pyspark.types.DoubleType(), True, None),
            pyspark.types.StructField("rate_code_id", pyspark.types.IntegerType(), True, None),
            pyspark.types.StructField("store_and_fwd_flag", pyspark.types.StringType(), True, None),
            pyspark.types.StructField(
                "pickup_location_id", pyspark.types.IntegerType(), True, None
            ),
            pyspark.types.StructField(
                "dropoff_location_id", pyspark.types.IntegerType(), True, None
            ),
            pyspark.types.StructField("payment_type", pyspark.types.IntegerType(), True, None),
            pyspark.types.StructField("fare_amount", pyspark.types.DoubleType(), True, None),
            pyspark.types.StructField("extra", pyspark.types.DoubleType(), True, None),
            pyspark.types.StructField("mta_tax", pyspark.types.DoubleType(), True, None),
            pyspark.types.StructField("tip_amount", pyspark.types.DoubleType(), True, None),
            pyspark.types.StructField("tolls_amount", pyspark.types.DoubleType(), True, None),
            pyspark.types.StructField(
                "improvement_surcharge", pyspark.types.DoubleType(), True, None
            ),
            pyspark.types.StructField("total_amount", pyspark.types.DoubleType(), True, None),
            pyspark.types.StructField(
                "congestion_surcharge", pyspark.types.DoubleType(), True, None
            ),
        ]
    )
    return schema


@pytest.mark.order(index=3)
@pytest.fixture
def spark_session_v012(test_backends):
    try:
        import pyspark  # noqa: F401
        from pyspark.sql import SparkSession  # noqa: F401

        return SparkDFExecutionEngine.get_or_create_spark_session()
    except ImportError:
        raise ValueError("spark tests are requested, but pyspark is not installed")


@pytest.fixture
def basic_expectation_suite():
    expectation_suite = ExpectationSuite(
        name="default",
        meta={},
        expectations=[
            ExpectationConfiguration(
                type="expect_column_to_exist",
                kwargs={"column": "infinities"},
            ),
            ExpectationConfiguration(type="expect_column_to_exist", kwargs={"column": "nulls"}),
            ExpectationConfiguration(type="expect_column_to_exist", kwargs={"column": "naturals"}),
            ExpectationConfiguration(
                type="expect_column_values_to_be_unique",
                kwargs={"column": "naturals"},
            ),
        ],
    )
    return expectation_suite


@pytest.fixture(scope="function")
def empty_data_context(
    tmp_path,
) -> FileDataContext:
    project_path = tmp_path / "empty_data_context"
    project_path.mkdir()
    project_path = str(project_path)
    context = gx.get_context(mode="file", project_root_dir=project_path)
    context_path = os.path.join(project_path, FileDataContext.GX_DIR)  # noqa: PTH118
    asset_config_path = os.path.join(context_path, "expectations")  # noqa: PTH118
    os.makedirs(asset_config_path, exist_ok=True)  # noqa: PTH103
    assert context.list_datasources() == []
    project_manager.set_project(context)
    return context


@pytest.fixture
def titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled(  # noqa: E501
    tmp_path_factory,
    monkeypatch,
):
    project_path: str = str(tmp_path_factory.mktemp("titanic_data_context_013"))
    context_path: str = os.path.join(  # noqa: PTH118
        project_path, FileDataContext.GX_DIR
    )
    os.makedirs(  # noqa: PTH103
        os.path.join(context_path, "expectations"),  # noqa: PTH118
        exist_ok=True,
    )
    os.makedirs(  # noqa: PTH103
        os.path.join(context_path, "plugins"),  # noqa: PTH118
        exist_ok=True,
    )
    shutil.copy(
        file_relative_path(
            __file__,
            str(
                pathlib.Path(
                    "data_context",
                    "fixtures",
                    "plugins",
                    "extended_checkpoint.py",
                )
            ),
        ),
        pathlib.Path(context_path) / "plugins" / "extended_checkpoint.py",
    )
    data_path: str = os.path.join(context_path, "..", "data", "titanic")  # noqa: PTH118
    os.makedirs(os.path.join(data_path), exist_ok=True)  # noqa: PTH118, PTH103
    shutil.copy(
        file_relative_path(
            __file__,
            str(
                pathlib.Path(
                    "test_fixtures",
                    "great_expectations_v013_no_datasource_stats_enabled.yml",
                )
            ),
        ),
        str(os.path.join(context_path, FileDataContext.GX_YML)),  # noqa: PTH118
    )
    shutil.copy(
        file_relative_path(
            __file__,
            os.path.join("test_sets", "Titanic.csv"),  # noqa: PTH118
        ),
        str(
            os.path.join(  # noqa: PTH118
                context_path, "..", "data", "titanic", "Titanic_19120414_1313.csv"
            )
        ),
    )
    shutil.copy(
        file_relative_path(
            __file__,
            os.path.join("test_sets", "Titanic.csv"),  # noqa: PTH118
        ),
        str(
            os.path.join(  # noqa: PTH118
                context_path, "..", "data", "titanic", "Titanic_19120414_1313"
            )
        ),
    )
    shutil.copy(
        file_relative_path(
            __file__,
            os.path.join("test_sets", "Titanic.csv"),  # noqa: PTH118
        ),
        str(
            os.path.join(  # noqa: PTH118
                context_path, "..", "data", "titanic", "Titanic_1911.csv"
            )
        ),
    )
    shutil.copy(
        file_relative_path(
            __file__,
            os.path.join("test_sets", "Titanic.csv"),  # noqa: PTH118
        ),
        str(
            os.path.join(  # noqa: PTH118
                context_path, "..", "data", "titanic", "Titanic_1912.csv"
            )
        ),
    )

    context = get_context(context_root_dir=context_path)
    assert context.root_directory == context_path

    context._save_project_config()
    project_manager.set_project(context)
    return context


@pytest.fixture
def titanic_v013_multi_datasource_pandas_data_context_with_checkpoints_v1_with_empty_store_stats_enabled(  # noqa: E501
    titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled,
    tmp_path_factory,
    monkeypatch,
):
    context = titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled  # noqa: E501

    project_manager.set_project(context)
    return context


@pytest.fixture
def titanic_v013_multi_datasource_pandas_and_sqlalchemy_execution_engine_data_context_with_checkpoints_v1_with_empty_store_stats_enabled(  # noqa: E501
    sa,
    titanic_v013_multi_datasource_pandas_data_context_with_checkpoints_v1_with_empty_store_stats_enabled: AbstractDataContext,  # noqa: E501
    tmp_path_factory,
    test_backends,
    monkeypatch,
):
    context = titanic_v013_multi_datasource_pandas_data_context_with_checkpoints_v1_with_empty_store_stats_enabled  # noqa: E501

    project_dir = context.root_directory
    assert isinstance(project_dir, str)
    data_path: str = os.path.join(project_dir, "..", "data", "titanic")  # noqa: PTH118

    if (
        any(dbms in test_backends for dbms in ["postgresql", "sqlite", "mysql", "mssql"])
        and (sa is not None)
        and is_library_loadable(library_name="sqlalchemy")
    ):
        db_fixture_file_path: str = file_relative_path(
            __file__,
            os.path.join("test_sets", "titanic_sql_test_cases.db"),  # noqa: PTH118
        )
        db_file_path: str = os.path.join(  # noqa: PTH118
            data_path,
            "titanic_sql_test_cases.db",
        )
        shutil.copy(
            db_fixture_file_path,
            db_file_path,
        )

        context.data_sources.add_sqlite(
            name="my_sqlite_db_datasource",
            connection_string=f"sqlite:///{db_file_path}",
        )

    return context


@pytest.fixture
def titanic_v013_multi_datasource_multi_execution_engine_data_context_with_checkpoints_v1_with_empty_store_stats_enabled(  # noqa: E501
    sa,
    spark_session,
    titanic_v013_multi_datasource_pandas_and_sqlalchemy_execution_engine_data_context_with_checkpoints_v1_with_empty_store_stats_enabled,
    tmp_path_factory,
    test_backends,
    monkeypatch,
):
    context = titanic_v013_multi_datasource_pandas_and_sqlalchemy_execution_engine_data_context_with_checkpoints_v1_with_empty_store_stats_enabled  # noqa: E501
    project_manager.set_project(context)
    return context


@pytest.fixture
def deterministic_asset_data_connector_context(
    tmp_path_factory,
    monkeypatch,
):
    project_path = str(tmp_path_factory.mktemp("titanic_data_context"))
    context_path = os.path.join(project_path, FileDataContext.GX_DIR)  # noqa: PTH118
    os.makedirs(  # noqa: PTH103
        os.path.join(context_path, "expectations"),  # noqa: PTH118
        exist_ok=True,
    )
    data_path = os.path.join(context_path, "..", "data", "titanic")  # noqa: PTH118
    os.makedirs(os.path.join(data_path), exist_ok=True)  # noqa: PTH118, PTH103
    shutil.copy(
        file_relative_path(
            __file__,
            str(
                pathlib.Path(
                    "test_fixtures",
                    "great_expectations_v013_no_datasource_stats_enabled.yml",
                )
            ),
        ),
        str(os.path.join(context_path, FileDataContext.GX_YML)),  # noqa: PTH118
    )
    shutil.copy(
        file_relative_path(__file__, "./test_sets/Titanic.csv"),
        str(
            os.path.join(  # noqa: PTH118
                context_path, "..", "data", "titanic", "Titanic_19120414_1313.csv"
            )
        ),
    )
    shutil.copy(
        file_relative_path(__file__, "./test_sets/Titanic.csv"),
        str(
            os.path.join(  # noqa: PTH118
                context_path, "..", "data", "titanic", "Titanic_1911.csv"
            )
        ),
    )
    shutil.copy(
        file_relative_path(__file__, "./test_sets/Titanic.csv"),
        str(
            os.path.join(  # noqa: PTH118
                context_path, "..", "data", "titanic", "Titanic_1912.csv"
            )
        ),
    )
    context = get_context(context_root_dir=context_path)
    assert context.root_directory == context_path

    context._save_project_config()
    project_manager.set_project(context)
    return context


@pytest.fixture
def titanic_data_context_with_fluent_pandas_datasources_with_checkpoints_v1_with_empty_store_stats_enabled(  # noqa: E501
    tmp_path_factory,
    monkeypatch,
):
    project_path: str = str(tmp_path_factory.mktemp("titanic_data_context_013"))
    context_path: str = os.path.join(  # noqa: PTH118
        project_path, FileDataContext.GX_DIR
    )
    os.makedirs(  # noqa: PTH103
        os.path.join(context_path, "expectations"),  # noqa: PTH118
        exist_ok=True,
    )
    data_path: str = os.path.join(context_path, "..", "data", "titanic")  # noqa: PTH118
    os.makedirs(os.path.join(data_path), exist_ok=True)  # noqa: PTH118, PTH103
    shutil.copy(
        file_relative_path(
            __file__,
            str(
                pathlib.Path(
                    "test_fixtures",
                    "great_expectations_no_block_no_fluent_datasources_stats_enabled.yml",
                )
            ),
        ),
        str(os.path.join(context_path, FileDataContext.GX_YML)),  # noqa: PTH118
    )
    os.makedirs(  # noqa: PTH103
        os.path.join(context_path, "plugins"),  # noqa: PTH118
        exist_ok=True,
    )
    shutil.copy(
        file_relative_path(
            __file__,
            str(
                pathlib.Path(
                    "data_context",
                    "fixtures",
                    "plugins",
                    "extended_checkpoint.py",
                )
            ),
        ),
        pathlib.Path(context_path) / "plugins" / "extended_checkpoint.py",
    )
    shutil.copy(
        file_relative_path(
            __file__,
            os.path.join("test_sets", "Titanic.csv"),  # noqa: PTH118
        ),
        str(
            os.path.join(  # noqa: PTH118
                context_path, "..", "data", "titanic", "Titanic_19120414_1313.csv"
            )
        ),
    )
    shutil.copy(
        file_relative_path(
            __file__,
            os.path.join("test_sets", "Titanic.csv"),  # noqa: PTH118
        ),
        str(
            os.path.join(  # noqa: PTH118
                context_path, "..", "data", "titanic", "Titanic_19120414_1313"
            )
        ),
    )
    shutil.copy(
        file_relative_path(
            __file__,
            os.path.join("test_sets", "Titanic.csv"),  # noqa: PTH118
        ),
        str(
            os.path.join(  # noqa: PTH118
                context_path, "..", "data", "titanic", "Titanic_1911.csv"
            )
        ),
    )
    shutil.copy(
        file_relative_path(
            __file__,
            os.path.join("test_sets", "Titanic.csv"),  # noqa: PTH118
        ),
        str(
            os.path.join(  # noqa: PTH118
                context_path, "..", "data", "titanic", "Titanic_1912.csv"
            )
        ),
    )

    context = get_context(context_root_dir=context_path)
    assert context.root_directory == context_path

    path_to_folder_containing_csv_files = pathlib.Path(data_path)

    datasource_name = "my_pandas_filesystem_datasource"
    datasource = context.data_sources.add_pandas_filesystem(
        name=datasource_name, base_directory=path_to_folder_containing_csv_files
    )

    batching_regex = r"(?P<name>.+)\.csv"
    glob_directive = "*.csv"
    datasource.add_csv_asset(
        name="exploration", batching_regex=batching_regex, glob_directive=glob_directive
    )

    batching_regex = r"(.+)_(?P<timestamp>\d{8})_(?P<size>\d{4})\.csv"
    glob_directive = "*.csv"
    datasource.add_csv_asset(
        name="users", batching_regex=batching_regex, glob_directive=glob_directive
    )

    datasource_name = "my_pandas_dataframes_datasource"
    datasource = context.data_sources.add_pandas(name=datasource_name)

    csv_source_path = pathlib.Path(
        context_path,
        "..",
        "data",
        "titanic",
        "Titanic_1911.csv",
    )
    df = pd.read_csv(filepath_or_buffer=csv_source_path)

    dataframe_asset_name = "my_dataframe_asset"
    asset = datasource.add_dataframe_asset(name=dataframe_asset_name)
    _ = asset.build_batch_request(options={"dataframe": df})

    # noinspection PyProtectedMember
    context._save_project_config()
    project_manager.set_project(context)
    return context


@pytest.fixture
def titanic_data_context_with_fluent_pandas_and_spark_datasources_with_checkpoints_v1_with_empty_store_stats_enabled(  # noqa: E501
    titanic_data_context_with_fluent_pandas_datasources_with_checkpoints_v1_with_empty_store_stats_enabled,
    spark_df_from_pandas_df,
    spark_session,
):
    context = titanic_data_context_with_fluent_pandas_datasources_with_checkpoints_v1_with_empty_store_stats_enabled  # noqa: E501
    context_path: str = context.root_directory
    path_to_folder_containing_csv_files = pathlib.Path(
        context_path,
        "..",
        "data",
        "titanic",
    )

    datasource_name = "my_spark_filesystem_datasource"
    datasource = context.data_sources.add_spark_filesystem(
        name=datasource_name, base_directory=path_to_folder_containing_csv_files
    )

    batching_regex = r"(?P<name>.+)\.csv"
    glob_directive = "*.csv"
    datasource.add_csv_asset(
        name="exploration", batching_regex=batching_regex, glob_directive=glob_directive
    )

    batching_regex = r"(.+)_(?P<timestamp>\d{8})_(?P<size>\d{4})\.csv"
    glob_directive = "*.csv"
    datasource.add_csv_asset(
        name="users", batching_regex=batching_regex, glob_directive=glob_directive
    )

    datasource_name = "my_spark_dataframes_datasource"
    datasource = context.data_sources.add_spark(name=datasource_name)

    csv_source_path = pathlib.Path(
        context_path,
        "..",
        "data",
        "titanic",
        "Titanic_1911.csv",
    )
    pandas_df = pd.read_csv(filepath_or_buffer=csv_source_path)
    spark_df = spark_df_from_pandas_df(spark_session, pandas_df)

    dataframe_asset_name = "my_dataframe_asset"
    asset = datasource.add_dataframe_asset(name=dataframe_asset_name)
    _ = asset.build_batch_request(options={"dataframe": spark_df})

    # noinspection PyProtectedMember
    context._save_project_config()
    project_manager.set_project(context)
    return context


@pytest.fixture
def titanic_data_context_with_fluent_pandas_and_sqlite_datasources_with_checkpoints_v1_with_empty_store_stats_enabled(  # noqa: E501
    titanic_data_context_with_fluent_pandas_datasources_with_checkpoints_v1_with_empty_store_stats_enabled,
    db_file,
    sa,
):
    context = titanic_data_context_with_fluent_pandas_datasources_with_checkpoints_v1_with_empty_store_stats_enabled  # noqa: E501

    datasource_name = "my_sqlite_datasource"
    connection_string = f"sqlite:///{db_file}"
    datasource = context.data_sources.add_sqlite(
        name=datasource_name,
        connection_string=connection_string,
    )

    query = "SELECT * from table_partitioned_by_date_column__A LIMIT 5"
    datasource.add_query_asset(
        name="table_partitioned_by_date_column__A_query_asset_limit_5", query=query
    )

    query = "SELECT * from table_partitioned_by_date_column__A LIMIT 10"
    datasource.add_query_asset(
        name="table_partitioned_by_date_column__A_query_asset_limit_10", query=query
    )

    # noinspection PyProtectedMember
    context._save_project_config()
    project_manager.set_project(context)
    return context


@pytest.fixture
def empty_context_with_checkpoint(empty_data_context):
    context = empty_data_context
    root_dir = empty_data_context.root_directory
    fixture_name = "my_checkpoint.yml"
    fixture_path = file_relative_path(__file__, f"./data_context/fixtures/contexts/{fixture_name}")
    checkpoints_file = os.path.join(  # noqa: PTH118
        root_dir, "checkpoints", fixture_name
    )
    shutil.copy(fixture_path, checkpoints_file)
    assert os.path.isfile(checkpoints_file)  # noqa: PTH113
    project_manager.set_project(context)
    return context


@pytest.fixture
def empty_data_context_stats_enabled(tmp_path_factory, monkeypatch):
    project_path = str(tmp_path_factory.mktemp("empty_data_context"))
    context = gx.get_context(mode="file", project_root_dir=project_path)
    context_path = os.path.join(project_path, FileDataContext.GX_DIR)  # noqa: PTH118
    asset_config_path = os.path.join(context_path, "expectations")  # noqa: PTH118
    os.makedirs(asset_config_path, exist_ok=True)  # noqa: PTH103
    project_manager.set_project(context)
    return context


@pytest.fixture
def titanic_data_context(tmp_path_factory) -> FileDataContext:
    project_path = str(tmp_path_factory.mktemp("titanic_data_context"))
    context_path = os.path.join(project_path, FileDataContext.GX_DIR)  # noqa: PTH118
    os.makedirs(  # noqa: PTH103
        os.path.join(context_path, "expectations"),  # noqa: PTH118
        exist_ok=True,
    )
    os.makedirs(  # noqa: PTH103
        os.path.join(context_path, "checkpoints"),  # noqa: PTH118
        exist_ok=True,
    )
    data_path = os.path.join(context_path, "..", "data")  # noqa: PTH118
    os.makedirs(os.path.join(data_path), exist_ok=True)  # noqa: PTH118, PTH103
    titanic_yml_path = file_relative_path(
        __file__, "./test_fixtures/great_expectations_v013_titanic.yml"
    )
    shutil.copy(
        titanic_yml_path,
        str(os.path.join(context_path, FileDataContext.GX_YML)),  # noqa: PTH118
    )
    titanic_csv_path = file_relative_path(__file__, "./test_sets/Titanic.csv")
    shutil.copy(
        titanic_csv_path,
        str(os.path.join(context_path, "..", "data", "Titanic.csv")),  # noqa: PTH118
    )
    context = get_context(context_root_dir=context_path)
    project_manager.set_project(context)
    return context


@pytest.fixture
def titanic_data_context_no_data_docs_no_checkpoint_store(tmp_path_factory):
    project_path = str(tmp_path_factory.mktemp("titanic_data_context"))
    context_path = os.path.join(project_path, FileDataContext.GX_DIR)  # noqa: PTH118
    os.makedirs(  # noqa: PTH103
        os.path.join(context_path, "expectations"),  # noqa: PTH118
        exist_ok=True,
    )
    os.makedirs(  # noqa: PTH103
        os.path.join(context_path, "checkpoints"),  # noqa: PTH118
        exist_ok=True,
    )
    data_path = os.path.join(context_path, "..", "data")  # noqa: PTH118
    os.makedirs(os.path.join(data_path), exist_ok=True)  # noqa: PTH118, PTH103
    titanic_yml_path = file_relative_path(
        __file__, "./test_fixtures/great_expectations_titanic_pre_v013_no_data_docs.yml"
    )
    shutil.copy(
        titanic_yml_path,
        str(os.path.join(context_path, FileDataContext.GX_YML)),  # noqa: PTH118
    )
    titanic_csv_path = file_relative_path(__file__, "./test_sets/Titanic.csv")
    shutil.copy(
        titanic_csv_path,
        str(os.path.join(context_path, "..", "data", "Titanic.csv")),  # noqa: PTH118
    )
    context = get_context(context_root_dir=context_path)
    project_manager.set_project(context)
    return context


@pytest.fixture
def titanic_data_context_no_data_docs(tmp_path_factory):
    project_path = str(tmp_path_factory.mktemp("titanic_data_context"))
    context_path = os.path.join(project_path, FileDataContext.GX_DIR)  # noqa: PTH118
    os.makedirs(  # noqa: PTH103
        os.path.join(context_path, "expectations"),  # noqa: PTH118
        exist_ok=True,
    )
    os.makedirs(  # noqa: PTH103
        os.path.join(context_path, "checkpoints"),  # noqa: PTH118
        exist_ok=True,
    )
    data_path = os.path.join(context_path, "..", "data")  # noqa: PTH118
    os.makedirs(os.path.join(data_path), exist_ok=True)  # noqa: PTH118, PTH103
    titanic_yml_path = file_relative_path(
        __file__, "./test_fixtures/great_expectations_titanic_no_data_docs.yml"
    )
    shutil.copy(
        titanic_yml_path,
        str(os.path.join(context_path, FileDataContext.GX_YML)),  # noqa: PTH118
    )
    titanic_csv_path = file_relative_path(__file__, "./test_sets/Titanic.csv")
    shutil.copy(
        titanic_csv_path,
        str(os.path.join(context_path, "..", "data", "Titanic.csv")),  # noqa: PTH118
    )
    context = get_context(context_root_dir=context_path)
    project_manager.set_project(context)
    return context


@pytest.fixture
def titanic_data_context_stats_enabled(tmp_path_factory, monkeypatch):
    project_path = str(tmp_path_factory.mktemp("titanic_data_context"))
    context_path = os.path.join(project_path, FileDataContext.GX_DIR)  # noqa: PTH118
    os.makedirs(  # noqa: PTH103
        os.path.join(context_path, "expectations"),  # noqa: PTH118
        exist_ok=True,
    )
    os.makedirs(  # noqa: PTH103
        os.path.join(context_path, "checkpoints"),  # noqa: PTH118
        exist_ok=True,
    )
    data_path = os.path.join(context_path, "..", "data")  # noqa: PTH118
    os.makedirs(os.path.join(data_path), exist_ok=True)  # noqa: PTH118, PTH103
    titanic_yml_path = file_relative_path(
        __file__, "./test_fixtures/great_expectations_v013_titanic.yml"
    )
    shutil.copy(
        titanic_yml_path,
        str(os.path.join(context_path, FileDataContext.GX_YML)),  # noqa: PTH118
    )
    titanic_csv_path = file_relative_path(__file__, "./test_sets/Titanic.csv")
    shutil.copy(
        titanic_csv_path,
        str(os.path.join(context_path, "..", "data", "Titanic.csv")),  # noqa: PTH118
    )
    context = get_context(context_root_dir=context_path)
    project_manager.set_project(context)
    return context


@pytest.fixture
def titanic_data_context_stats_enabled_config_version_2(tmp_path_factory, monkeypatch):
    project_path = str(tmp_path_factory.mktemp("titanic_data_context"))
    context_path = os.path.join(project_path, FileDataContext.GX_DIR)  # noqa: PTH118
    os.makedirs(  # noqa: PTH103
        os.path.join(context_path, "expectations"),  # noqa: PTH118
        exist_ok=True,
    )
    os.makedirs(  # noqa: PTH103
        os.path.join(context_path, "checkpoints"),  # noqa: PTH118
        exist_ok=True,
    )
    data_path = os.path.join(context_path, "..", "data")  # noqa: PTH118
    os.makedirs(os.path.join(data_path), exist_ok=True)  # noqa: PTH118, PTH103
    titanic_yml_path = file_relative_path(
        __file__, "./test_fixtures/great_expectations_titanic.yml"
    )
    shutil.copy(
        titanic_yml_path,
        str(os.path.join(context_path, FileDataContext.GX_YML)),  # noqa: PTH118
    )
    titanic_csv_path = file_relative_path(__file__, "./test_sets/Titanic.csv")
    shutil.copy(
        titanic_csv_path,
        str(os.path.join(context_path, "..", "data", "Titanic.csv")),  # noqa: PTH118
    )
    context = get_context(context_root_dir=context_path)
    project_manager.set_project(context)
    return context


@pytest.fixture
def titanic_data_context_stats_enabled_config_version_3(tmp_path_factory, monkeypatch):
    project_path = str(tmp_path_factory.mktemp("titanic_data_context"))
    context_path = os.path.join(project_path, FileDataContext.GX_DIR)  # noqa: PTH118
    os.makedirs(  # noqa: PTH103
        os.path.join(context_path, "expectations"),  # noqa: PTH118
        exist_ok=True,
    )
    os.makedirs(  # noqa: PTH103
        os.path.join(context_path, "checkpoints"),  # noqa: PTH118
        exist_ok=True,
    )
    data_path = os.path.join(context_path, "..", "data")  # noqa: PTH118
    os.makedirs(os.path.join(data_path), exist_ok=True)  # noqa: PTH118, PTH103
    titanic_yml_path = file_relative_path(
        __file__, "./test_fixtures/great_expectations_v013_upgraded_titanic.yml"
    )
    shutil.copy(
        titanic_yml_path,
        str(os.path.join(context_path, FileDataContext.GX_YML)),  # noqa: PTH118
    )
    titanic_csv_path = file_relative_path(__file__, "./test_sets/Titanic.csv")
    shutil.copy(
        titanic_csv_path,
        str(os.path.join(context_path, "..", "data", "Titanic.csv")),  # noqa: PTH118
    )
    context = get_context(context_root_dir=context_path)
    project_manager.set_project(context)
    return context


@pytest.fixture(scope="module")
def titanic_spark_db(tmp_path_factory, spark_warehouse_session):
    try:
        from pyspark.sql import DataFrame  # noqa: TCH002
    except ImportError:
        raise ValueError("spark tests are requested, but pyspark is not installed")

    titanic_database_name: str = "db_test"
    titanic_csv_path: str = file_relative_path(__file__, "./test_sets/Titanic.csv")
    project_path: str = str(tmp_path_factory.mktemp("data"))
    project_dataset_path: str = str(
        os.path.join(project_path, "Titanic.csv")  # noqa: PTH118
    )

    shutil.copy(titanic_csv_path, project_dataset_path)
    titanic_df: DataFrame = spark_warehouse_session.read.csv(project_dataset_path, header=True)

    spark_warehouse_session.sql(f"CREATE DATABASE IF NOT EXISTS {titanic_database_name}")
    spark_warehouse_session.catalog.setCurrentDatabase(titanic_database_name)
    titanic_df.write.saveAsTable(
        "tb_titanic_with_partitions",
        partitionBy=["PClass", "SexCode"],
        mode="overwrite",
    )
    titanic_df.write.saveAsTable("tb_titanic_without_partitions", mode="overwrite")

    row_count = spark_warehouse_session.sql(
        f"SELECT COUNT(*) from {titanic_database_name}.tb_titanic_without_partitions"
    ).collect()
    assert row_count and row_count[0][0] == 1313
    yield spark_warehouse_session
    spark_warehouse_session.sql(f"DROP DATABASE IF EXISTS {titanic_database_name} CASCADE")
    spark_warehouse_session.catalog.setCurrentDatabase("default")


@pytest.fixture
def titanic_sqlite_db(sa):
    try:
        import sqlalchemy as sa
        from sqlalchemy import create_engine

        titanic_db_path = file_relative_path(__file__, "./test_sets/titanic.db")
        engine = create_engine(f"sqlite:///{titanic_db_path}")
        with engine.begin() as connection:
            assert connection.execute(sa.text("select count(*) from titanic")).fetchall()[0] == (
                1313,
            )
            return engine
    except ImportError:
        raise ValueError("sqlite tests require sqlalchemy to be installed")


@pytest.fixture
def titanic_sqlite_db_connection_string(sa):
    try:
        import sqlalchemy as sa
        from sqlalchemy import create_engine

        titanic_db_path = file_relative_path(__file__, "./test_sets/titanic.db")
        engine = create_engine(f"sqlite:////{titanic_db_path}")
        with engine.begin() as connection:
            assert connection.execute(sa.text("select count(*) from titanic")).fetchall()[0] == (
                1313,
            )
        return f"sqlite:///{titanic_db_path}"
    except ImportError:
        raise ValueError("sqlite tests require sqlalchemy to be installed")


@pytest.fixture
def titanic_expectation_suite(empty_data_context_stats_enabled):
    data_context = empty_data_context_stats_enabled
    return ExpectationSuite(
        name="Titanic.warning",
        meta={},
        expectations=[
            ExpectationConfiguration(type="expect_column_to_exist", kwargs={"column": "PClass"}),
            ExpectationConfiguration(
                type="expect_column_values_to_not_be_null",
                kwargs={"column": "Name"},
            ),
            ExpectationConfiguration(
                type="expect_table_row_count_to_equal",
                kwargs={"value": 1313},
            ),
        ],
        data_context=data_context,
    )


@pytest.fixture
def empty_sqlite_db(sa):
    """An empty in-memory sqlite db that always gets run."""
    try:
        import sqlalchemy as sa
        from sqlalchemy import create_engine

        engine = create_engine("sqlite://")
        with engine.begin() as connection:
            assert connection.execute(sa.text("select 1")).fetchall()[0] == (1,)
        return engine
    except ImportError:
        raise ValueError("sqlite tests require sqlalchemy to be installed")


@pytest.fixture
def data_context_parameterized_expectation_suite(tmp_path_factory):
    """
    This data_context is *manually* created to have the config we want, vs
    created with gx.get_context()
    """
    project_path = str(tmp_path_factory.mktemp("data_context"))
    context_path = os.path.join(project_path, FileDataContext.GX_DIR)  # noqa: PTH118
    asset_config_path = os.path.join(context_path, "expectations")  # noqa: PTH118
    fixture_dir = file_relative_path(__file__, "./test_fixtures")
    os.makedirs(  # noqa: PTH103
        os.path.join(asset_config_path, "my_dag_node"),  # noqa: PTH118
        exist_ok=True,
    )
    shutil.copy(
        os.path.join(fixture_dir, "great_expectations_v013_basic.yml"),  # noqa: PTH118
        str(os.path.join(context_path, FileDataContext.GX_YML)),  # noqa: PTH118
    )
    shutil.copy(
        os.path.join(  # noqa: PTH118
            fixture_dir,
            "expectation_suites/parameterized_expectation_suite_fixture.json",
        ),
        os.path.join(asset_config_path, "my_dag_node", "default.json"),  # noqa: PTH118
    )
    os.makedirs(  # noqa: PTH103
        os.path.join(context_path, "plugins"),  # noqa: PTH118
        exist_ok=True,
    )
    return get_context(context_root_dir=context_path, cloud_mode=False)


@pytest.fixture
def titanic_profiled_evrs_1():
    with open(
        file_relative_path(__file__, "./render/fixtures/BasicDatasetProfiler_evrs.json"),
    ) as infile:
        return expectationSuiteValidationResultSchema.loads(infile.read())


# various types of evr
@pytest.fixture
def evr_failed():
    return ExpectationValidationResult(
        success=False,
        result={
            "element_count": 1313,
            "missing_count": 0,
            "missing_percent": 0.0,
            "unexpected_count": 3,
            "unexpected_percent": 0.2284843869002285,
            "unexpected_percent_nonmissing": 0.2284843869002285,
            "partial_unexpected_list": [
                "Daly, Mr Peter Denis ",
                "Barber, Ms ",
                "Geiger, Miss Emily ",
            ],
            "partial_unexpected_index_list": [77, 289, 303],
            "partial_unexpected_counts": [
                {"value": "Barber, Ms ", "count": 1},
                {"value": "Daly, Mr Peter Denis ", "count": 1},
                {"value": "Geiger, Miss Emily ", "count": 1},
            ],
        },
        exception_info={
            "raised_exception": False,
            "exception_message": None,
            "exception_traceback": None,
        },
        expectation_config=ExpectationConfiguration(
            type="expect_column_values_to_not_match_regex",
            kwargs={
                "column": "Name",
                "regex": "^\\s+|\\s+$",
                "result_format": "SUMMARY",
            },
        ),
    )


@pytest.fixture
def evr_success():
    return ExpectationValidationResult(
        success=True,
        result={"observed_value": 1313},
        exception_info={
            "raised_exception": False,
            "exception_message": None,
            "exception_traceback": None,
        },
        expectation_config=ExpectationConfiguration(
            type="expect_table_row_count_to_be_between",
            kwargs={"min_value": 0, "max_value": None, "result_format": "SUMMARY"},
        ),
    )


@pytest.fixture
def sqlite_view_engine(test_backends) -> Engine:  # type: ignore[return]
    # Create a small in-memory engine with two views, one of which is temporary
    if "sqlite" in test_backends:
        try:
            import sqlalchemy as sa

            sqlite_engine = sa.create_engine("sqlite://")
            df = pd.DataFrame({"a": [1, 2, 3, 4, 5]})
            add_dataframe_to_db(
                df=df,
                name="test_table",
                con=sqlite_engine,
                index=True,
            )
            with sqlite_engine.begin() as connection:
                connection.execute(
                    sa.text(
                        "CREATE TEMP VIEW test_temp_view AS SELECT * FROM test_table where a < 4;"
                    )
                )
                connection.execute(
                    sa.text("CREATE VIEW test_view AS SELECT * FROM test_table where a > 4;")
                )
            return sqlite_engine
        except ImportError:
            sa = None  # type: ignore[assignment]
    else:
        pytest.skip("SqlAlchemy tests disabled; not testing views")


@pytest.fixture
def expectation_suite_identifier():
    return ExpectationSuiteIdentifier("my.expectation.suite.name")


@pytest.fixture
def test_folder_connection_path_csv(tmp_path_factory):
    df1 = pd.DataFrame({"col_1": [1, 2, 3, 4, 5], "col_2": ["a", "b", "c", "d", "e"]})
    path = str(tmp_path_factory.mktemp("test_folder_connection_path_csv"))
    df1.to_csv(path_or_buf=os.path.join(path, "test.csv"), index=False)  # noqa: PTH118
    return str(path)


@pytest.fixture
def test_db_connection_string(tmp_path_factory, test_backends):
    if "sqlite" not in test_backends:
        pytest.skip("skipping fixture because sqlite not selected")
    df1 = pd.DataFrame({"col_1": [1, 2, 3, 4, 5], "col_2": ["a", "b", "c", "d", "e"]})
    df2 = pd.DataFrame({"col_1": [0, 1, 2, 3, 4], "col_2": ["b", "c", "d", "e", "f"]})

    try:
        import sqlalchemy as sa

        basepath = str(tmp_path_factory.mktemp("db_context"))
        path = os.path.join(basepath, "test.db")  # noqa: PTH118
        engine = sa.create_engine("sqlite:///" + str(path))
        add_dataframe_to_db(df=df1, name="table_1", con=engine, index=True)
        add_dataframe_to_db(df=df2, name="table_2", con=engine, index=True, schema="main")

        # Return a connection string to this newly-created db
        return "sqlite:///" + str(path)
    except ImportError:
        raise ValueError("SQL Database tests require sqlalchemy to be installed.")


@pytest.fixture
def test_df(tmp_path_factory):
    def generate_ascending_list_of_datetimes(
        k, start_date=datetime.date(2020, 1, 1), end_date=datetime.date(2020, 12, 31)
    ):
        start_time = datetime.datetime(start_date.year, start_date.month, start_date.day)  # noqa: DTZ001
        days_between_dates = (end_date - start_date).total_seconds()

        datetime_list = [
            start_time + datetime.timedelta(seconds=random.randrange(round(days_between_dates)))
            for i in range(k)
        ]
        datetime_list.sort()
        return datetime_list

    k = 120
    random.seed(1)

    timestamp_list = generate_ascending_list_of_datetimes(k, end_date=datetime.date(2020, 1, 31))
    date_list = [datetime.date(ts.year, ts.month, ts.day) for ts in timestamp_list]

    batch_ids = [random.randint(0, 10) for i in range(k)]
    batch_ids.sort()

    session_ids = [random.randint(2, 60) for i in range(k)]
    session_ids.sort()
    session_ids = [i - random.randint(0, 2) for i in session_ids]

    events_df = pd.DataFrame(
        {
            "id": range(k),
            "batch_id": batch_ids,
            "date": date_list,
            "y": [d.year for d in date_list],
            "m": [d.month for d in date_list],
            "d": [d.day for d in date_list],
            "timestamp": timestamp_list,
            "session_ids": session_ids,
            "event_type": [random.choice(["start", "stop", "continue"]) for i in range(k)],
            "favorite_color": [
                "#" + "".join([random.choice(list("0123456789ABCDEF")) for j in range(6)])
                for i in range(k)
            ],
        }
    )
    return events_df


@pytest.fixture
def sqlite_connection_string() -> str:
    db_file_path: str = file_relative_path(
        __file__,
        os.path.join(  # noqa: PTH118
            "test_sets", "test_cases_for_sql_data_connector.db"
        ),
    )
    return f"sqlite:///{db_file_path}"


@pytest.fixture
def fds_data_context_datasource_name() -> str:
    return "sqlite_datasource"


@pytest.fixture
def fds_data_context(
    sa,
    fds_data_context_datasource_name: str,
    empty_data_context: AbstractDataContext,
    sqlite_connection_string: str,
) -> AbstractDataContext:
    context = empty_data_context
    datasource = context.data_sources.add_sqlite(
        name=fds_data_context_datasource_name,
        connection_string=sqlite_connection_string,
        create_temp_table=True,
    )

    datasource.add_query_asset(
        name="trip_asset",
        query="SELECT * FROM table_partitioned_by_date_column__A",
    )
    datasource.add_query_asset(
        name="trip_asset_partition_by_event_type",
        query="SELECT * FROM table_partitioned_by_date_column__A",
    )
    return context


@pytest.fixture
def db_file():
    return file_relative_path(
        __file__,
        os.path.join(  # noqa: PTH118
            "test_sets", "test_cases_for_sql_data_connector.db"
        ),
    )


@pytest.fixture
def ge_cloud_id():
    # Fake id but adheres to the format required of a UUID
    return "731ee1bd-604a-4851-9ee8-bca8ffb32bce"


@pytest.fixture
def ge_cloud_base_url() -> str:
    return GX_CLOUD_MOCK_BASE_URL


@pytest.fixture
def v1_cloud_base_url(ge_cloud_base_url: str) -> str:
    return urllib.parse.urljoin(ge_cloud_base_url, "api/v1/")


@pytest.fixture
def ge_cloud_organization_id() -> str:
    return FAKE_ORG_ID


@pytest.fixture
def ge_cloud_access_token() -> str:
    return DUMMY_JWT_TOKEN


@pytest.fixture
def request_headers(ge_cloud_access_token: str) -> Dict[str, str]:
    return {
        "Content-Type": "application/vnd.api+json",
        "Authorization": f"Bearer {ge_cloud_access_token}",
        "Gx-Version": gx.__version__,
    }


@pytest.fixture
def ge_cloud_config(ge_cloud_base_url, ge_cloud_organization_id, ge_cloud_access_token):
    return GXCloudConfig(
        base_url=ge_cloud_base_url,
        organization_id=ge_cloud_organization_id,
        access_token=ge_cloud_access_token,
    )


@pytest.fixture(scope="function")
def empty_ge_cloud_data_context_config(
    ge_cloud_base_url, ge_cloud_organization_id, ge_cloud_access_token
):
    config_yaml_str = f"""
stores:
  default_expectations_store:
    class_name: ExpectationsStore
    store_backend:
      class_name: {GXCloudStoreBackend.__name__}
      ge_cloud_base_url: {ge_cloud_base_url}
      ge_cloud_resource_type: expectation_suite
      ge_cloud_credentials:
        access_token: {ge_cloud_access_token}
        organization_id: {ge_cloud_organization_id}
      suppress_store_backend_id: True

  default_validation_results_store:
    class_name: ValidationResultsStore
    store_backend:
      class_name: {GXCloudStoreBackend.__name__}
      ge_cloud_base_url: {ge_cloud_base_url}
      ge_cloud_resource_type: validation_result
      ge_cloud_credentials:
        access_token: {ge_cloud_access_token}
        organization_id: {ge_cloud_organization_id}
      suppress_store_backend_id: True

  validation_definition_store:
    class_name: ValidationDefinitionStore
    store_backend:
      class_name: {GXCloudStoreBackend.__name__}
      ge_cloud_base_url: {ge_cloud_base_url}
      ge_cloud_resource_type: validation_definition
      ge_cloud_credentials:
        access_token: {ge_cloud_access_token}
        organization_id: {ge_cloud_organization_id}
      suppress_store_backend_id: True

  default_checkpoint_store:
    class_name: CheckpointStore
    store_backend:
      class_name: {GXCloudStoreBackend.__name__}
      ge_cloud_base_url: {ge_cloud_base_url}
      ge_cloud_resource_type: checkpoint
      ge_cloud_credentials:
        access_token: {ge_cloud_access_token}
        organization_id: {ge_cloud_organization_id}
      suppress_store_backend_id: True

expectations_store_name: default_expectations_store
validation_results_store_name: default_validation_results_store
checkpoint_store_name: default_checkpoint_store
"""
    data_context_config_dict = yaml.load(config_yaml_str)
    return DataContextConfig(**data_context_config_dict)


@pytest.fixture
def ge_cloud_config_e2e() -> GXCloudConfig:
    """
    Uses live credentials stored in the Great Expectations Cloud backend.
    """
    env_vars = os.environ

    base_url = env_vars.get(
        GXCloudEnvironmentVariable.BASE_URL,
    )
    organization_id = env_vars.get(
        GXCloudEnvironmentVariable.ORGANIZATION_ID,
    )
    access_token = env_vars.get(
        GXCloudEnvironmentVariable.ACCESS_TOKEN,
    )
    cloud_config = GXCloudConfig(
        base_url=base_url,  # type: ignore[arg-type]
        organization_id=organization_id,
        access_token=access_token,
    )
    return cloud_config


@pytest.fixture
@mock.patch(
    "great_expectations.data_context.store.DatasourceStore.list_keys",
    return_value=[],
)
def empty_base_data_context_in_cloud_mode(
    mock_list_keys: MagicMock,  # Avoid making a call to Cloud backend during datasource instantiation  # noqa: E501
    tmp_path: pathlib.Path,
    empty_ge_cloud_data_context_config: DataContextConfig,
    ge_cloud_config: GXCloudConfig,
) -> CloudDataContext:
    project_path = tmp_path / "empty_data_context"
    project_path.mkdir(exist_ok=True)

    context = CloudDataContext(
        project_config=empty_ge_cloud_data_context_config,
        context_root_dir=project_path,
        cloud_base_url=ge_cloud_config.base_url,
        cloud_access_token=ge_cloud_config.access_token,
        cloud_organization_id=ge_cloud_config.organization_id,
    )
    set_context(context)
    return context


@pytest.fixture
def empty_data_context_in_cloud_mode(
    tmp_path: pathlib.Path,
    ge_cloud_config: GXCloudConfig,
    empty_ge_cloud_data_context_config: DataContextConfig,
):
    """This fixture is a DataContext in cloud mode that mocks calls to the cloud backend during setup so that it can be instantiated in tests."""  # noqa: E501
    project_path = tmp_path / "empty_data_context"
    project_path.mkdir(exist_ok=True)

    def mocked_config(*args, **kwargs) -> DataContextConfig:
        return empty_ge_cloud_data_context_config

    def mocked_get_cloud_config(*args, **kwargs) -> GXCloudConfig:
        return ge_cloud_config

    with (
        mock.patch(
            "great_expectations.data_context.data_context.serializable_data_context.SerializableDataContext._save_project_config"
        ),
        mock.patch(
            "great_expectations.data_context.data_context.cloud_data_context.CloudDataContext.retrieve_data_context_config_from_cloud",
            autospec=True,
            side_effect=mocked_config,
        ),
        mock.patch(
            "great_expectations.data_context.data_context.CloudDataContext.get_cloud_config",
            autospec=True,
            side_effect=mocked_get_cloud_config,
        ),
    ):
        context = CloudDataContext(context_root_dir=project_path)

    context._datasources = {}  # type: ignore[assignment] # Basic in-memory mock for DatasourceDict to avoid HTTP calls
    return context


@pytest.fixture
def empty_cloud_data_context(
    cloud_api_fake,
    tmp_path: pathlib.Path,
    empty_ge_cloud_data_context_config: DataContextConfig,
    ge_cloud_config: GXCloudConfig,
) -> CloudDataContext:
    project_path = tmp_path / "empty_data_context"
    project_path.mkdir()
    project_path_name: str = str(project_path)

    context = CloudDataContext(
        project_config=empty_ge_cloud_data_context_config,
        context_root_dir=project_path_name,
        cloud_base_url=ge_cloud_config.base_url,
        cloud_access_token=ge_cloud_config.access_token,
        cloud_organization_id=ge_cloud_config.organization_id,
    )
    set_context(context)
    return context


@pytest.fixture
def cloud_details(
    ge_cloud_base_url, ge_cloud_organization_id, ge_cloud_access_token
) -> CloudDetails:
    return CloudDetails(
        base_url=ge_cloud_base_url,
        org_id=ge_cloud_organization_id,
        access_token=ge_cloud_access_token,
    )


@pytest.fixture
def cloud_api_fake(cloud_details: CloudDetails):
    with gx_cloud_api_fake_ctx(cloud_details=cloud_details) as requests_mock:
        yield requests_mock


@pytest.fixture
def empty_cloud_context_fluent(cloud_api_fake, cloud_details: CloudDetails) -> CloudDataContext:
    context = gx.get_context(
        cloud_access_token=cloud_details.access_token,
        cloud_organization_id=cloud_details.org_id,
        cloud_base_url=cloud_details.base_url,
        cloud_mode=True,
    )
    set_context(context)
    return context


@pytest.fixture
@mock.patch(
    "great_expectations.data_context.store.DatasourceStore.get_all",
    return_value=[],
)
def empty_base_data_context_in_cloud_mode_custom_base_url(
    mock_get_all: MagicMock,  # Avoid making a call to Cloud backend during datasource instantiation
    tmp_path: pathlib.Path,
    empty_ge_cloud_data_context_config: DataContextConfig,
    ge_cloud_config: GXCloudConfig,
) -> CloudDataContext:
    project_path = tmp_path / "empty_data_context"
    project_path.mkdir()
    project_path = str(project_path)  # type: ignore[assignment]

    custom_base_url: str = "https://some_url.org/"
    custom_ge_cloud_config = copy.deepcopy(ge_cloud_config)
    custom_ge_cloud_config.base_url = custom_base_url

    context = CloudDataContext(
        project_config=empty_ge_cloud_data_context_config,
        context_root_dir=project_path,
        cloud_base_url=custom_ge_cloud_config.base_url,
        cloud_access_token=custom_ge_cloud_config.access_token,
        cloud_organization_id=custom_ge_cloud_config.organization_id,
    )
    assert context.list_datasources() == []
    assert context.ge_cloud_config.base_url != ge_cloud_config.base_url
    assert context.ge_cloud_config.base_url == custom_base_url
    return context


@pytest.fixture
def cloud_data_context_with_datasource_pandas_engine(
    empty_cloud_data_context: CloudDataContext, db_file
):
    context: CloudDataContext = empty_cloud_data_context

    fds = PandasDatasource(name="my_datasource")
    context.add_datasource(datasource=fds)
    return context


# TODO: AJB 20210525 This fixture is not yet used but may be helpful to generate batches for unit tests of multibatch  # noqa: E501
#  workflows.  It should probably be extended to add different column types / data.
@pytest.fixture
def multibatch_generic_csv_generator():
    """
    Construct a series of csv files with many data types for use in multibatch testing
    """

    def _multibatch_generic_csv_generator(
        data_path: str | pathlib.Path,
        start_date: Optional[datetime.datetime] = None,
        num_event_batches: Optional[int] = 20,
        num_events_per_batch: Optional[int] = 5,
    ) -> List[str]:
        data_path = pathlib.Path(data_path)
        if start_date is None:
            start_date = datetime.datetime(2000, 1, 1)  # noqa: DTZ001

        file_list = []
        category_strings = {
            0: "category0",
            1: "category1",
            2: "category2",
            3: "category3",
            4: "category4",
            5: "category5",
            6: "category6",
        }
        for batch_num in range(num_event_batches):  # type: ignore[arg-type]
            # generate a dataframe with multiple column types
            batch_start_date = start_date + datetime.timedelta(
                days=(batch_num * num_events_per_batch)  # type: ignore[operator]
            )
            # TODO: AJB 20210416 Add more column types
            df = pd.DataFrame(
                {
                    "event_date": [
                        (batch_start_date + datetime.timedelta(days=i)).strftime("%Y-%m-%d")
                        for i in range(num_events_per_batch)  # type: ignore[arg-type]
                    ],
                    "batch_num": [batch_num + 1 for _ in range(num_events_per_batch)],  # type: ignore[arg-type]
                    "string_cardinality_3": [
                        category_strings[i % 3]
                        for i in range(num_events_per_batch)  # type: ignore[arg-type]
                    ],
                }
            )
            filename = f"csv_batch_{batch_num + 1:03}_of_{num_event_batches:03}.csv"
            file_list.append(filename)
            # noinspection PyTypeChecker
            df.to_csv(
                data_path / filename,
                index_label="intra_batch_index",
            )

        return file_list

    return _multibatch_generic_csv_generator


@pytest.fixture
def in_memory_runtime_context() -> AbstractDataContext:
    return build_in_memory_runtime_context()


@pytest.fixture
def table_row_count_metric_config() -> MetricConfiguration:
    return MetricConfiguration(
        metric_name="table.row_count",
        metric_domain_kwargs={},
        metric_value_kwargs=None,
    )


@pytest.fixture
def table_row_count_aggregate_fn_metric_config() -> MetricConfiguration:
    return MetricConfiguration(
        metric_name=f"table.row_count.{MetricPartialFunctionTypes.AGGREGATE_FN.metric_suffix}",
        metric_domain_kwargs={},
        metric_value_kwargs=None,
    )


@pytest.fixture
def table_head_metric_config() -> MetricConfiguration:
    return MetricConfiguration(
        metric_name="table.head",
        metric_domain_kwargs={
            "batch_id": "abc123",
        },
        metric_value_kwargs={
            "n_rows": 5,
        },
    )


@pytest.fixture
def column_histogram_metric_config() -> MetricConfiguration:
    return MetricConfiguration(
        metric_name="column.histogram",
        metric_domain_kwargs={
            "column": "my_column",
            "batch_id": "def456",
        },
        metric_value_kwargs={
            "bins": 5,
        },
    )


@pytest.fixture()
def test_df_pandas():
    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})
    return test_df


@pytest.fixture
def spark_df_from_pandas_df():
    """
    Construct a spark dataframe from pandas dataframe.
    Returns:
        Function that can be used in your test e.g.:
        spark_df = spark_df_from_pandas_df(spark_session, pandas_df)
    """

    def _construct_spark_df_from_pandas(
        spark_session,
        pandas_df,
    ):
        spark_df = spark_session.createDataFrame(
            [
                tuple(
                    None if isinstance(x, (float, int)) and np.isnan(x) else x
                    for x in record.tolist()
                )
                for record in pandas_df.to_records(index=False)
            ],
            pandas_df.columns.tolist(),
        )
        return spark_df

    return _construct_spark_df_from_pandas


@pytest.fixture
def pandas_animals_dataframe_for_unexpected_rows_and_index():
    return pd.DataFrame(
        {
            "pk_1": [0, 1, 2, 3, 4, 5],
            "pk_2": ["zero", "one", "two", "three", "four", "five"],
            "animals": [
                "cat",
                "fish",
                "dog",
                "giraffe",
                "lion",
                "zebra",
            ],
        }
    )


@pytest.fixture
def pandas_column_pairs_dataframe_for_unexpected_rows_and_index():
    return pd.DataFrame(
        {
            "pk_1": [0, 1, 2, 3, 4, 5],
            "pk_2": ["zero", "one", "two", "three", "four", "five"],
            "ordered_item": [
                "pencil",
                "pencil",
                "pencil",
                "eraser",
                "eraser",
                "eraser",
            ],
            "received_item": [
                "pencil",
                "pencil",
                "pencil",
                "desk",
                "desk",
                "desk",
            ],
        }
    )


@pytest.fixture
def pandas_multicolumn_sum_dataframe_for_unexpected_rows_and_index() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "pk_1": [0, 1, 2, 3, 4, 5],
            "pk_2": ["zero", "one", "two", "three", "four", "five"],
            "a": [10, 20, 30, 40, 50, 60],
            "b": [10, 20, 30, 40, 50, 60],
            "c": [10, 20, 30, 40, 50, 60],
        }
    )


@pytest.fixture
def spark_column_pairs_dataframe_for_unexpected_rows_and_index(
    spark_session,
) -> pyspark.DataFrame:
    df: pd.DataFrame = pd.DataFrame(
        {
            "pk_1": [0, 1, 2, 3, 4, 5],
            "pk_2": ["zero", "one", "two", "three", "four", "five"],
            "ordered_item": [
                "pencil",
                "pencil",
                "pencil",
                "eraser",
                "eraser",
                "eraser",
            ],
            "received_item": [
                "pencil",
                "pencil",
                "pencil",
                "desk",
                "desk",
                "desk",
            ],
        }
    )
    test_df = spark_session.createDataFrame(data=df)
    return test_df


@pytest.fixture
def spark_multicolumn_sum_dataframe_for_unexpected_rows_and_index(
    spark_session,
) -> pyspark.DataFrame:
    df: pd.DataFrame = pd.DataFrame(
        {
            "pk_1": [0, 1, 2, 3, 4, 5],
            "pk_2": ["zero", "one", "two", "three", "four", "five"],
            "a": [10, 20, 30, 40, 50, 60],
            "b": [10, 20, 30, 40, 50, 60],
            "c": [10, 20, 30, 40, 50, 60],
        }
    )
    test_df = spark_session.createDataFrame(data=df)
    return test_df


@pytest.fixture
def spark_dataframe_for_unexpected_rows_with_index(
    spark_session,
) -> pyspark.DataFrame:
    df: pd.DataFrame = pd.DataFrame(
        {
            "pk_1": [0, 1, 2, 3, 4, 5],
            "pk_2": ["zero", "one", "two", "three", "four", "five"],
            "animals": [
                "cat",
                "fish",
                "dog",
                "giraffe",
                "lion",
                "zebra",
            ],
        }
    )
    test_df = spark_session.createDataFrame(
        data=df,
    )
    return test_df


@pytest.fixture
def ephemeral_context_with_defaults() -> EphemeralDataContext:
    project_config = DataContextConfig(
        store_backend_defaults=InMemoryStoreBackendDefaults(init_temp_docs_sites=True)
    )
    return get_context(project_config=project_config, mode="ephemeral")


@pytest.fixture
def arbitrary_batch_definition(empty_data_context: AbstractDataContext) -> BatchDefinition:
    return (
        empty_data_context.data_sources.add_pandas("my_datasource_with_batch_def")
        .add_dataframe_asset("my_asset")
        .add_batch_definition_whole_dataframe(name="my_dataframe_batch_definition")
    )


@pytest.fixture
def arbitrary_suite(empty_data_context: AbstractDataContext) -> ExpectationSuite:
    return empty_data_context.suites.add(ExpectationSuite("my_suite"))


@pytest.fixture
def arbitrary_validation_definition(
    empty_data_context: AbstractDataContext,
    arbitrary_suite: ExpectationSuite,
    arbitrary_batch_definition: BatchDefinition,
) -> ValidationDefinition:
    return empty_data_context.validation_definitions.add(
        ValidationDefinition(
            name="my_validation_definition",
            suite=arbitrary_suite,
            data=arbitrary_batch_definition,
        )
    )


@pytest.fixture
def validator_with_mock_execution_engine(mocker: MockerFixture) -> Validator:
    execution_engine = mocker.MagicMock()
    validator = Validator(execution_engine=execution_engine)
    return validator


@pytest.fixture
def csv_path() -> pathlib.Path:
    relative_path = pathlib.Path("test_sets", "taxi_yellow_tripdata_samples")
    abs_csv_path = pathlib.Path(__file__).parent.joinpath(relative_path).resolve(strict=True)
    return abs_csv_path


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "testing"


@pytest.fixture(scope="function")
def filter_gx_datasource_warnings() -> Generator[None, None, None]:
    """Filter out GxDatasourceWarning warnings."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=GxDatasourceWarning)
        yield


@pytest.fixture(scope="function")
def param_id(request: pytest.FixtureRequest) -> str:
    """Return the parameter id of the current test.

    Example:

    ```python
    @pytest.mark.parametrize("my_param", ["a", "b", "c"], ids=lambda x: x.upper())
    def test_something(param_id: str, my_param: str):
        assert my_param != param_id
        assert my_param.upper() == param_id
    ```
    """
    raw_name: str = request.node.name
    return raw_name.split("[")[1].split("]")[0]
