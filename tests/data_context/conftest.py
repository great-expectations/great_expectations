from __future__ import annotations

import json
import os
import pathlib
import shutil
import unittest.mock
from typing import Any, Callable, Dict, Optional, Union, cast
from unittest.mock import Mock  # noqa: TID251

import pytest
import requests

import great_expectations as gx
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context.data_context.file_data_context import (
    FileDataContext,
)
from great_expectations.data_context.store.gx_cloud_store_backend import (
    GXCloudStoreBackend,
)
from great_expectations.data_context.types.base import DataContextConfig
from great_expectations.data_context.util import file_relative_path
from great_expectations.datasource.fluent.interfaces import Datasource

yaml = YAMLHandler()

# No longer used but keeping around for tests
USAGE_STATISTICS_QA_URL = (
    "https://qa.stats.greatexpectations.io/great_expectations/v1/usage_statistics"
)


@pytest.fixture()
def data_context_without_config_variables_filepath_configured(tmp_path_factory):
    # This data_context is *manually* created to have the config we want, vs created with DataContext.create  # noqa: E501
    project_path = str(tmp_path_factory.mktemp("data_context"))
    context_path = os.path.join(project_path, FileDataContext.GX_DIR)  # noqa: PTH118
    asset_config_path = os.path.join(context_path, "expectations")  # noqa: PTH118

    create_data_context_files(
        context_path,
        asset_config_path,
        ge_config_fixture_filename="great_expectations_basic_without_config_variables_filepath.yml",
        config_variables_fixture_filename=None,
    )

    return gx.get_context(context_root_dir=context_path)


@pytest.fixture()
def data_context_with_variables_in_config(tmp_path_factory, monkeypatch):
    monkeypatch.setenv("FOO", "BAR")
    monkeypatch.setenv("REPLACE_ME_ESCAPED_ENV", "ive_been_$--replaced")
    # This data_context is *manually* created to have the config we want, vs created with DataContext.create  # noqa: E501
    project_path = str(tmp_path_factory.mktemp("data_context"))
    context_path = os.path.join(project_path, FileDataContext.GX_DIR)  # noqa: PTH118
    asset_config_path = os.path.join(context_path, "expectations")  # noqa: PTH118

    create_data_context_files(
        context_path,
        asset_config_path,
        ge_config_fixture_filename="great_expectations_basic_with_variables.yml",
        config_variables_fixture_filename="config_variables.yml",
    )

    return gx.get_context(context_root_dir=context_path)


def create_data_context_files(
    context_path,
    asset_config_path,
    ge_config_fixture_filename,
    config_variables_fixture_filename=None,
):
    if config_variables_fixture_filename:
        os.makedirs(context_path, exist_ok=True)  # noqa: PTH103
        os.makedirs(  # noqa: PTH103
            os.path.join(context_path, "uncommitted"),  # noqa: PTH118
            exist_ok=True,
        )
        copy_relative_path(
            f"../test_fixtures/{config_variables_fixture_filename}",
            str(
                os.path.join(  # noqa: PTH118
                    context_path, "uncommitted/config_variables.yml"
                )
            ),
        )
        copy_relative_path(
            f"../test_fixtures/{ge_config_fixture_filename}",
            str(os.path.join(context_path, FileDataContext.GX_YML)),  # noqa: PTH118
        )
    else:
        os.makedirs(context_path, exist_ok=True)  # noqa: PTH103
        copy_relative_path(
            f"../test_fixtures/{ge_config_fixture_filename}",
            str(os.path.join(context_path, FileDataContext.GX_YML)),  # noqa: PTH118
        )
    create_common_data_context_files(context_path, asset_config_path)


def create_common_data_context_files(context_path, asset_config_path):
    os.makedirs(  # noqa: PTH103
        os.path.join(  # noqa: PTH118
            asset_config_path, "mydatasource/mygenerator/my_dag_node"
        ),
        exist_ok=True,
    )
    copy_relative_path(
        "../test_fixtures/" "expectation_suites/parameterized_expectation_suite_fixture.json",
        os.path.join(  # noqa: PTH118
            asset_config_path, "mydatasource/mygenerator/my_dag_node/default.json"
        ),
    )
    os.makedirs(  # noqa: PTH103
        os.path.join(context_path, "plugins"),  # noqa: PTH118
        exist_ok=True,
    )


def copy_relative_path(relative_src, dest):
    shutil.copy(file_relative_path(__file__, relative_src), dest)


@pytest.fixture
def basic_data_context_config():
    return DataContextConfig(
        **{
            "commented_map": {},
            "config_version": 2,
            "plugins_directory": "plugins/",
            "validation_results_store_name": "does_not_have_to_be_real",
            "expectations_store_name": "expectations_store",
            "checkpoint_store_name": "checkpoint_store",
            "config_variables_file_path": "uncommitted/config_variables.yml",
            "stores": {
                "expectations_store": {
                    "class_name": "ExpectationsStore",
                    "store_backend": {
                        "class_name": "TupleFilesystemStoreBackend",
                        "base_directory": "expectations/",
                    },
                },
                "checkpoint_store": {
                    "class_name": "CheckpointStore",
                    "store_backend": {
                        "class_name": "TupleFilesystemStoreBackend",
                        "base_directory": "checkpoints/",
                    },
                },
            },
            "data_docs_sites": {},
            "analytics_enabled": True,
            "data_context_id": "6a52bdfa-e182-455b-a825-e69f076e67d6",
        }
    )


@pytest.fixture
def basic_data_context_config_dict(basic_data_context_config):
    """Wrapper fixture to transform `basic_data_context_config` to a json dict"""
    return basic_data_context_config.to_json_dict()


@pytest.fixture
def conn_string_password():
    """Returns a stable password for mocking connection strings"""
    return "not_a_password"


@pytest.fixture
def conn_string_with_embedded_password(conn_string_password):
    """
    A mock connection string with the `conn_string_password` fixture embedded.
    """
    return f"redshift+psycopg2://no_user:{conn_string_password}@111.11.1.1:1111/foo"


@pytest.fixture
def data_context_config_with_datasources(conn_string_password):
    return DataContextConfig(
        **{
            "commented_map": {},
            "config_version": 2,
            "plugins_directory": "plugins/",
            "validation_results_store_name": "does_not_have_to_be_real",
            "expectations_store_name": "expectations_store",
            "checkpoint_store_name": "checkpoint_store",
            "config_variables_file_path": "uncommitted/config_variables.yml",
            "fluent_datasources": {
                "Datasource 2: Postgres": {
                    "type": "pandas_filesystem",
                    "base_directory": "/path/to/trip_data",
                }
            },
            "stores": {
                "expectations_store": {
                    "class_name": "ExpectationsStore",
                    "store_backend": {
                        "class_name": "TupleFilesystemStoreBackend",
                        "base_directory": "expectations/",
                    },
                },
                "checkpoint_store": {
                    "class_name": "CheckpointStore",
                    "store_backend": {
                        "class_name": "TupleFilesystemStoreBackend",
                        "base_directory": "checkpoints/",
                    },
                },
            },
            "data_docs_sites": {},
            "analytics_enabled": True,
            "data_context_id": "6a52bdfa-e182-455b-a825-e69f076e67d6",
        }
    )


@pytest.fixture
def data_context_config_dict_with_datasources(data_context_config_with_datasources):
    """Wrapper fixture to transform `data_context_config_with_datasources` to a json dict"""
    return data_context_config_with_datasources.to_json_dict()


@pytest.fixture
def data_context_config_with_cloud_backed_stores(ge_cloud_access_token):
    org_id = "a34595b2-267e-4469-b18f-774e65dc556f"
    return DataContextConfig(
        **{
            "commented_map": {},
            "config_version": 2,
            "plugins_directory": "plugins/",
            "validation_results_store_name": "does_not_have_to_be_real",
            "expectations_store_name": "expectations_store",
            "config_variables_file_path": "uncommitted/config_variables.yml",
            "stores": {
                "default_checkpoint_store": {
                    "class_name": "CheckpointStore",
                    "store_backend": {
                        "class_name": GXCloudStoreBackend.__name__,
                        "ge_cloud_base_url": "http://foo/bar/",
                        "ge_cloud_credentials": {
                            "access_token": ge_cloud_access_token,
                            "organization_id": org_id,
                        },
                        "ge_cloud_resource_type": "checkpoint",
                        "suppress_store_backend_id": True,
                    },
                },
                "default_expectations_store": {
                    "class_name": "ExpectationsStore",
                    "store_backend": {
                        "class_name": GXCloudStoreBackend.__name__,
                        "ge_cloud_base_url": "http://foo/bar/",
                        "ge_cloud_credentials": {
                            "access_token": ge_cloud_access_token,
                            "organization_id": org_id,
                        },
                        "ge_cloud_resource_type": "expectation_suite",
                        "suppress_store_backend_id": True,
                    },
                },
                "default_validation_results_store": {
                    "class_name": "ValidationResultsStore",
                    "store_backend": {
                        "class_name": GXCloudStoreBackend.__name__,
                        "ge_cloud_base_url": "http://foo/bar/",
                        "ge_cloud_credentials": {
                            "access_token": ge_cloud_access_token,
                            "organization_id": org_id,
                        },
                        "ge_cloud_resource_type": "validation_result",
                        "suppress_store_backend_id": True,
                    },
                },
            },
            "data_docs_sites": {},
            "analytics_enabled": True,
            "data_context_id": "6a52bdfa-e182-455b-a825-e69f076e67d6",
        }
    )


@pytest.fixture
def data_context_config_dict_with_cloud_backed_stores(
    data_context_config_with_cloud_backed_stores,
):
    """Wrapper fixture to transform `data_context_config_with_cloud_backed_stores` to a json dict"""
    return data_context_config_with_cloud_backed_stores.to_json_dict()


@pytest.fixture
def ge_cloud_runtime_base_url():
    return "https://api.dev.greatexpectations.io/runtime"


@pytest.fixture
def ge_cloud_runtime_organization_id():
    return "a8a35168-68d5-4366-90ae-00647463d37e"


@pytest.fixture
def datasource_name() -> str:
    return "my_first_datasource"


@pytest.fixture
def datasource_store_name() -> str:
    return "datasource_store"


@pytest.fixture
def fake_datasource_id() -> str:
    return "aaa7cfdd-4aa4-4f3d-a979-fe2ea5203cbf"


@pytest.fixture
def fake_data_connector_id() -> str:
    return "0c08e6ba-8ed9-4715-a179-da2f08aab13e"


JSONData = Union[Dict[str, Any]]
RequestError = Union[requests.exceptions.HTTPError, requests.exceptions.Timeout]


class MockResponse:
    # TODO: GG 08232022 update signature to accept arbitrary content types
    def __init__(
        self,
        json_data: JSONData,
        status_code: int,
        headers: Optional[Dict[str, str]] = None,
        exc_to_raise: Optional[RequestError] = None,
    ) -> None:
        self._json_data = json_data
        self.status_code = status_code
        self.headers = headers or {"content-type": "application/json" if json_data else "text/html"}
        self._exc_to_raise = exc_to_raise

    def json(self):
        if self.headers.get("content-type") == "application/json":
            return self._json_data
        raise json.JSONDecodeError("Uh oh - check content-type", "foobar", 1)

    def raise_for_status(self):
        if self._exc_to_raise:
            raise self._exc_to_raise
        if self.status_code >= 400:
            raise requests.exceptions.HTTPError(f"Mock {self.status_code} HTTPError", response=self)

    def __repr__(self):  # type: ignore[explicit-override] # FIXME
        return f"<Response [{self.status_code}]>"


@pytest.fixture
def mock_response_factory() -> Callable[[JSONData, int, Optional[RequestError]], MockResponse]:
    def _make_mock_response(
        json_data: JSONData,
        status_code: int,
        exc_to_raise: Optional[RequestError] = None,
    ) -> MockResponse:
        return MockResponse(json_data=json_data, status_code=status_code, exc_to_raise=exc_to_raise)

    return _make_mock_response


def basic_fluent_datasource_config() -> dict:
    return {
        "type": "pandas_filesystem",
        "name": "my_fluent_pandas_filesystem_datasource",
        "assets": [
            {
                "name": "my_csv",
                "type": "csv",
            }
        ],
        "base_directory": pathlib.PosixPath("/path/to/trip_data"),
    }


def basic_fluent_datasource() -> Datasource:
    context = gx.get_context(mode="ephemeral")
    datasource = context.data_sources.add_pandas_filesystem(
        name="pandas_filesystem",
        base_directory="/path/to/trip_data",  # type: ignore [arg-type]
    )
    datasource.add_csv_asset(name="my_csv")
    return datasource


@pytest.fixture
def fluent_datasource_config() -> dict:
    return basic_fluent_datasource_config()


@pytest.fixture
def mock_http_unavailable(mock_response_factory: Callable):
    """Mock all request http calls to return a 503 Unavailable response."""

    def mocked_response(*args, **kwargs):
        return MockResponse(
            {"code": 503, "detail": "API is unavailable"},
            503,
        )

    # should have been able to do this by mocking `requests.request` but this didn't work
    with unittest.mock.patch.multiple(
        "requests.Session",
        autospec=True,
        get=unittest.mock.DEFAULT,
        post=unittest.mock.DEFAULT,
        put=unittest.mock.DEFAULT,
        patch=unittest.mock.DEFAULT,
        delete=unittest.mock.DEFAULT,
    ) as mock_requests:
        for name, mock in cast(Dict[str, Mock], mock_requests).items():
            mock.side_effect = mocked_response
            print(f"Mocking `requests.{name}` with `{mocked_response.__name__}()`")

        yield mock_requests


@pytest.fixture
def checkpoint_config() -> dict:
    checkpoint_config = {
        "name": "oss_test_checkpoint",
        "expectation_suite_name": "oss_test_expectation_suite",
        "validations": [
            {
                "name": None,
                "id": None,
                "expectation_suite_name": "taxi.demo_pass",
                "expectation_suite_id": None,
                "batch_request": None,
            },
            {
                "name": None,
                "id": None,
                "expectation_suite_name": None,
                "expectation_suite_id": None,
                "batch_request": {
                    "datasource_name": "oss_test_datasource",
                    "data_connector_name": "oss_test_data_connector",
                    "data_asset_name": "users",
                },
            },
        ],
        "action_list": [
            {
                "action": {"class_name": "StoreValidationResultAction"},
                "name": "store_validation_result",
            },
        ],
    }
    return checkpoint_config


@pytest.fixture
def mocked_datasource_post_response(
    mock_response_factory: Callable,
    fake_datasource_id: str,
) -> Callable[[], MockResponse]:
    def _mocked_post_response(*args, **kwargs):
        return mock_response_factory(
            {
                "data": {
                    "id": fake_datasource_id,
                }
            },
            201,
        )

    return _mocked_post_response
