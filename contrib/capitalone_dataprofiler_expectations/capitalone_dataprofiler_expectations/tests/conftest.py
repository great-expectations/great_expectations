from __future__ import annotations

import os
import shutil
import sys

import pytest

from great_expectations import get_context
from great_expectations.data_context import FileDataContext
from great_expectations.data_context.types.base import AnonymizedUsageStatisticsConfig
from great_expectations.data_context.util import file_relative_path
from great_expectations.self_check.util import build_test_backends_list
from tests.conftest import (  # noqa: F401  # registers implicitly used fixture and prevents removal of "unused" import
    set_consistent_seed_within_numeric_metric_range_multi_batch_parameter_builder,
)

sys.path.insert(0, os.path.abspath("../.."))  # noqa: PTH100

test_root_path: str = os.path.dirname(  # noqa: PTH120
    os.path.dirname(os.path.dirname(os.path.realpath(__file__)))  # noqa: PTH120
)


class BaseProfiler:
    """
    This class should ideally be named "MockBaseProfiler"; however, it has to be called "BaseProfiler", because its
    "load()" method returns "BaseProfiler" type, which is type of class itself (using "fluent" programming style).
    """

    # noinspection PyMethodMayBeStatic,PyMethodParameters
    def load(cls, filepath: str) -> BaseProfiler:
        return cls

    # noinspection PyMethodMayBeStatic
    def report(self, report_options: dict = None) -> dict:
        return {
            "global_stats": {
                "profile_schema": {},
            },
            "data_stats": [
                {
                    "column_name": "vendor_id",
                    "data_type": "int",
                },
                {
                    "column_name": "passenger_count",
                    "data_type": "int",
                },
                {
                    "column_name": "total_amount",
                    "data_type": "float",
                },
                {
                    "column_name": "congestion_surcharge",
                    "data_type": "float",
                },
            ],
        }


def pytest_addoption(parser):
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
        "--aws-integration",
        action="store_true",
        help="If set, run aws integration tests for usage_statistics",
    )
    parser.addoption(
        "--docs-tests",
        action="store_true",
        help="If set, run integration tests for docs",
    )
    parser.addoption(
        "--azure", action="store_true", help="If set, execute tests against Azure"
    )
    parser.addoption(
        "--cloud", action="store_true", help="If set, execute tests against GX Cloud"
    )
    parser.addoption(
        "--performance-tests",
        action="store_true",
        help="If set, run performance tests (which might also require additional arguments like --bigquery)",
    )


def pytest_generate_tests(metafunc):
    test_backends = build_test_backends_list(metafunc)
    if "test_backend" in metafunc.fixturenames:
        metafunc.parametrize("test_backend", test_backends, scope="module")
    if "test_backends" in metafunc.fixturenames:
        metafunc.parametrize("test_backends", [test_backends], scope="module")


@pytest.fixture(scope="function")
def mock_base_data_profiler() -> BaseProfiler:
    return BaseProfiler()


@pytest.fixture(scope="function")
def bobby_columnar_table_multi_batch_deterministic_data_context(
    set_consistent_seed_within_numeric_metric_range_multi_batch_parameter_builder,  # noqa: F811
    tmp_path_factory,
    monkeypatch,
) -> FileDataContext:
    # Re-enable GE_USAGE_STATS
    monkeypatch.delenv("GE_USAGE_STATS", raising=False)
    monkeypatch.setattr(AnonymizedUsageStatisticsConfig, "enabled", True)

    project_path: str = str(tmp_path_factory.mktemp("taxi_data_context"))
    context_path: str = os.path.join(project_path, "great_expectations")  # noqa: PTH118
    os.makedirs(  # noqa: PTH103
        os.path.join(context_path, "expectations"), exist_ok=True  # noqa: PTH118
    )
    data_path: str = os.path.join(context_path, "..", "data")  # noqa: PTH118
    os.makedirs(os.path.join(data_path), exist_ok=True)  # noqa: PTH118, PTH103
    shutil.copy(
        file_relative_path(
            __file__,
            os.path.join(  # noqa: PTH118
                test_root_path,
                "capitalone_dataprofiler_expectations",
                "tests",
                "data_profiler_files",
                "fixtures",
                "yellow_tripdata_pandas_fixture",
                "great_expectations",
                "great_expectations.yml",
            ),
        ),
        str(os.path.join(context_path, "great_expectations.yml")),  # noqa: PTH118
    )
    shutil.copy(
        file_relative_path(
            __file__,
            os.path.join(  # noqa: PTH118
                test_root_path,
                "capitalone_dataprofiler_expectations",
                "tests",
                "data_profiler_files",
                "taxi_yellow_tripdata_samples",
                "random_subsamples",
                "yellow_tripdata_7500_lines_sample_2019-01.csv",
            ),
        ),
        str(
            os.path.join(  # noqa: PTH118
                context_path, "..", "data", "yellow_tripdata_sample_2019-01.csv"
            )
        ),
    )
    shutil.copy(
        file_relative_path(
            __file__,
            os.path.join(  # noqa: PTH118
                test_root_path,
                "capitalone_dataprofiler_expectations",
                "tests",
                "data_profiler_files",
                "taxi_yellow_tripdata_samples",
                "random_subsamples",
                "yellow_tripdata_8500_lines_sample_2019-02.csv",
            ),
        ),
        str(
            os.path.join(  # noqa: PTH118
                context_path, "..", "data", "yellow_tripdata_sample_2019-02.csv"
            )
        ),
    )
    shutil.copy(
        file_relative_path(
            __file__,
            os.path.join(  # noqa: PTH118
                test_root_path,
                "capitalone_dataprofiler_expectations",
                "tests",
                "data_profiler_files",
                "taxi_yellow_tripdata_samples",
                "random_subsamples",
                "yellow_tripdata_9000_lines_sample_2019-03.csv",
            ),
        ),
        str(
            os.path.join(  # noqa: PTH118
                context_path, "..", "data", "yellow_tripdata_sample_2019-03.csv"
            )
        ),
    )

    context = get_context(context_root_dir=context_path)
    assert context.root_directory == context_path

    return context


@pytest.fixture(scope="module")
def bobby_columnar_table_multi_batch_probabilistic_data_context(
    tmp_path_factory,
) -> FileDataContext:
    project_path: str = str(tmp_path_factory.mktemp("taxi_data_context"))
    context_path: str = os.path.join(project_path, "great_expectations")  # noqa: PTH118
    os.makedirs(  # noqa: PTH103
        os.path.join(context_path, "expectations"), exist_ok=True  # noqa: PTH118
    )
    data_path: str = os.path.join(context_path, "..", "data")  # noqa: PTH118
    os.makedirs(os.path.join(data_path), exist_ok=True)  # noqa: PTH118, PTH103
    shutil.copy(
        file_relative_path(
            __file__,
            os.path.join(  # noqa: PTH118
                test_root_path,
                "capitalone_dataprofiler_expectations",
                "tests",
                "data_profiler_files",
                "fixtures",
                "yellow_tripdata_pandas_fixture",
                "great_expectations",
                "great_expectations.yml",
            ),
        ),
        str(os.path.join(context_path, "great_expectations.yml")),  # noqa: PTH118
    )
    shutil.copy(
        file_relative_path(
            __file__,
            os.path.join(  # noqa: PTH118
                test_root_path,
                "capitalone_dataprofiler_expectations",
                "tests",
                "data_profiler_files",
                "taxi_yellow_tripdata_samples",
                "random_subsamples",
                "yellow_tripdata_7500_lines_sample_2019-01.csv",
            ),
        ),
        str(
            os.path.join(  # noqa: PTH118
                context_path, "..", "data", "yellow_tripdata_sample_2019-01.csv"
            )
        ),
    )
    shutil.copy(
        file_relative_path(
            __file__,
            os.path.join(  # noqa: PTH118
                test_root_path,
                "capitalone_dataprofiler_expectations",
                "tests",
                "data_profiler_files",
                "taxi_yellow_tripdata_samples",
                "random_subsamples",
                "yellow_tripdata_8500_lines_sample_2019-02.csv",
            ),
        ),
        str(
            os.path.join(  # noqa: PTH118
                context_path, "..", "data", "yellow_tripdata_sample_2019-02.csv"
            )
        ),
    )
    shutil.copy(
        file_relative_path(
            __file__,
            os.path.join(  # noqa: PTH118
                test_root_path,
                "capitalone_dataprofiler_expectations",
                "tests",
                "data_profiler_files",
                "taxi_yellow_tripdata_samples",
                "random_subsamples",
                "yellow_tripdata_9000_lines_sample_2019-03.csv",
            ),
        ),
        str(
            os.path.join(  # noqa: PTH118
                context_path, "..", "data", "yellow_tripdata_sample_2019-03.csv"
            )
        ),
    )

    context = get_context(context_root_dir=context_path)
    assert context.root_directory == context_path

    return context
