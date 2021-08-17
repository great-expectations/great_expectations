# todo(jdimatteo): add a performance test change log and only run performance test when that file changes.
#  include git describe output in json

import os
import time

from pytest_benchmark.fixture import BenchmarkFixture

from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import (
    DataContextConfig,
    InMemoryStoreBackendDefaults,
)


def foo(seconds):
    time.sleep(seconds)
    return 123


def test_bikeshare_trips_1_table(benchmark: BenchmarkFixture):
    context = _create_context()
    assert 123 == benchmark.pedantic(foo, args=(0.042,), iterations=1, rounds=1)


# todo(jdimatteo): test with multiple datasources?
_datasource_name = "my_bigquery_datasource"


def _html_dir() -> str:
    return os.path.join(os.path.abspath(os.path.dirname(__file__)), "html")


def _create_context() -> BaseDataContext:
    store_backend = {
        "class_name": "TupleFilesystemStoreBackend",
        "base_directory": _html_dir(),
    }
    data_docs_sites = {
        "local_site": {
            "class_name": "SiteBuilder",
            "show_how_to_buttons": False,
            "store_backend": store_backend,
        }
    }
    bigquery_project = os.environ["GE_TEST_BIGQUERY_PROJECT"]
    bigquery_dataset = os.environ.get("GE_TEST_BIGQUERY_PROJECT", "performance_ci")

    data_context_config = DataContextConfig(
        store_backend_defaults=InMemoryStoreBackendDefaults(),
        data_docs_sites=data_docs_sites,
        anonymous_usage_statistics={"enabled": False},
        datasources={
            _datasource_name: {
                "class_name": "Datasource",
                "execution_engine": {
                    "class_name": "SqlAlchemyExecutionEngine",
                    "connection_string": f"bigquery://{bigquery_project}/{bigquery_dataset}",
                },
                "data_connectors": {
                    _datasource_name: {
                        "class_name": "ConfiguredAssetSqlDataConnector",
                        "name": "whole_table",
                        "assets": {f"bikeshare_trips_{i}": {} for i in range(1, 100)},
                    },
                },
            },
        },
    )
    return BaseDataContext(project_config=data_context_config)
