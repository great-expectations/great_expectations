import json
import os
import shutil
import unittest.mock
from typing import Any, Callable, Dict, Optional, Union, cast
from unittest.mock import Mock, PropertyMock, patch

import pytest
import requests

import great_expectations as ge
from great_expectations.data_context.store import GeCloudStoreBackend
from great_expectations.data_context.store.ge_cloud_store_backend import AnyPayload
from great_expectations.data_context.types.base import (
    DataContextConfig,
    DatasourceConfig,
)
from great_expectations.data_context.util import file_relative_path
from tests.integration.usage_statistics.test_integration_usage_statistics import (
    USAGE_STATISTICS_QA_URL,
)


@pytest.fixture()
def data_context_without_config_variables_filepath_configured(tmp_path_factory):
    # This data_context is *manually* created to have the config we want, vs created with DataContext.create
    project_path = str(tmp_path_factory.mktemp("data_context"))
    context_path = os.path.join(project_path, "great_expectations")
    asset_config_path = os.path.join(context_path, "expectations")

    create_data_context_files(
        context_path,
        asset_config_path,
        ge_config_fixture_filename="great_expectations_basic_without_config_variables_filepath.yml",
        config_variables_fixture_filename=None,
    )

    return ge.data_context.DataContext(context_path)


@pytest.fixture()
def data_context_with_variables_in_config(tmp_path_factory, monkeypatch):
    monkeypatch.setenv("FOO", "BAR")
    monkeypatch.setenv("REPLACE_ME_ESCAPED_ENV", "ive_been_$--replaced")
    # This data_context is *manually* created to have the config we want, vs created with DataContext.create
    project_path = str(tmp_path_factory.mktemp("data_context"))
    context_path = os.path.join(project_path, "great_expectations")
    asset_config_path = os.path.join(context_path, "expectations")

    create_data_context_files(
        context_path,
        asset_config_path,
        ge_config_fixture_filename="great_expectations_basic_with_variables.yml",
        config_variables_fixture_filename="config_variables.yml",
    )

    return ge.data_context.DataContext(context_path)


@pytest.fixture()
def data_context_with_variables_in_config_exhaustive(tmp_path_factory):
    # This data_context is *manually* created to have the config we want, vs created with DataContext.create
    project_path = str(tmp_path_factory.mktemp("data_context"))
    context_path = os.path.join(project_path, "great_expectations")
    asset_config_path = os.path.join(context_path, "expectations")

    create_data_context_files(
        context_path,
        asset_config_path,
        ge_config_fixture_filename="great_expectations_basic_with_exhaustive_variables.yml",
        config_variables_fixture_filename="config_variables_exhaustive.yml",
    )

    return ge.data_context.DataContext(context_path)


def create_data_context_files(
    context_path,
    asset_config_path,
    ge_config_fixture_filename,
    config_variables_fixture_filename=None,
):
    if config_variables_fixture_filename:
        os.makedirs(context_path, exist_ok=True)
        os.makedirs(os.path.join(context_path, "uncommitted"), exist_ok=True)
        copy_relative_path(
            f"../test_fixtures/{config_variables_fixture_filename}",
            str(os.path.join(context_path, "uncommitted/config_variables.yml")),
        )
        copy_relative_path(
            f"../test_fixtures/{ge_config_fixture_filename}",
            str(os.path.join(context_path, "great_expectations.yml")),
        )
    else:
        os.makedirs(context_path, exist_ok=True)
        copy_relative_path(
            f"../test_fixtures/{ge_config_fixture_filename}",
            str(os.path.join(context_path, "great_expectations.yml")),
        )
    create_common_data_context_files(context_path, asset_config_path)


def create_common_data_context_files(context_path, asset_config_path):
    os.makedirs(
        os.path.join(asset_config_path, "mydatasource/mygenerator/my_dag_node"),
        exist_ok=True,
    )
    copy_relative_path(
        "../test_fixtures/"
        "expectation_suites/parameterized_expectation_suite_fixture.json",
        os.path.join(
            asset_config_path, "mydatasource/mygenerator/my_dag_node/default.json"
        ),
    )
    os.makedirs(os.path.join(context_path, "plugins"), exist_ok=True)
    copy_relative_path(
        "../test_fixtures/custom_pandas_dataset.py",
        str(os.path.join(context_path, "plugins", "custom_pandas_dataset.py")),
    )
    copy_relative_path(
        "../test_fixtures/custom_sqlalchemy_dataset.py",
        str(os.path.join(context_path, "plugins", "custom_sqlalchemy_dataset.py")),
    )
    copy_relative_path(
        "../test_fixtures/custom_sparkdf_dataset.py",
        str(os.path.join(context_path, "plugins", "custom_sparkdf_dataset.py")),
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
            "evaluation_parameter_store_name": "evaluation_parameter_store",
            "validations_store_name": "does_not_have_to_be_real",
            "expectations_store_name": "expectations_store",
            "config_variables_file_path": "uncommitted/config_variables.yml",
            "datasources": {},
            "stores": {
                "expectations_store": {
                    "class_name": "ExpectationsStore",
                    "store_backend": {
                        "class_name": "TupleFilesystemStoreBackend",
                        "base_directory": "expectations/",
                    },
                },
                "evaluation_parameter_store": {
                    "module_name": "great_expectations.data_context.store",
                    "class_name": "EvaluationParameterStore",
                },
            },
            "data_docs_sites": {},
            "validation_operators": {
                "default": {
                    "class_name": "ActionListValidationOperator",
                    "action_list": [],
                }
            },
            "anonymous_usage_statistics": {
                "enabled": True,
                "data_context_id": "6a52bdfa-e182-455b-a825-e69f076e67d6",
                "usage_statistics_url": USAGE_STATISTICS_QA_URL,
            },
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
            "evaluation_parameter_store_name": "evaluation_parameter_store",
            "validations_store_name": "does_not_have_to_be_real",
            "expectations_store_name": "expectations_store",
            "config_variables_file_path": "uncommitted/config_variables.yml",
            "datasources": {
                "Datasource 1: Redshift": {
                    "class_name": "Datasource",
                    "data_connectors": {
                        "default_configured_asset_sql_data_connector": {
                            "assets": {},
                            "batch_spec_passthrough": {"sample": "value"},
                            "class_name": "ConfiguredAssetSqlDataConnector",
                        }
                    },
                    "execution_engine": {
                        "class_name": "SqlAlchemyExecutionEngine",
                        "connection_string": f"redshift+psycopg2://no_user:{conn_string_password}@111.11.1.1:1111/foo",
                    },
                    "module_name": "great_expectations.datasource",
                },
                "Datasource 2: Postgres": {
                    "class_name": "Datasource",
                    "data_connectors": {
                        "default_configured_asset_sql_data_connector_sqlalchemy": {
                            "assets": {},
                            "batch_spec_passthrough": {"sample": "value"},
                            "class_name": "ConfiguredAssetSqlDataConnector",
                        }
                    },
                    "execution_engine": {
                        "class_name": "SqlAlchemyExecutionEngine",
                        "connection_string": f"postgresql://no_user:{conn_string_password}@some_url:1111/postgres?sslmode=prefer",
                    },
                    "module_name": "great_expectations.datasource",
                },
                "Datasource 3: MySQL": {
                    "class_name": "Datasource",
                    "data_connectors": {
                        "default_configured_asset_sql_data_connector_sqlalchemy": {
                            "assets": {},
                            "batch_spec_passthrough": {"sample": "value"},
                            "class_name": "ConfiguredAssetSqlDataConnector",
                        }
                    },
                    "execution_engine": {
                        "class_name": "SqlAlchemyExecutionEngine",
                        "connection_string": f"mysql+pymysql://no_user:{conn_string_password}@some_url:1111/foo",
                    },
                    "module_name": "great_expectations.datasource",
                },
                "Datasource 4: Pandas": {
                    # no creds to be masked here, shouldnt be affected
                    "class_name": "Datasource",
                    "data_connectors": {
                        "default_runtime_data_connector": {
                            "batch_identifiers": ["col"],
                            "batch_spec_passthrough": {"sample": "value"},
                            "class_name": "RuntimeDataConnector",
                        }
                    },
                    "execution_engine": {"class_name": "PandasExecutionEngine"},
                    "module_name": "great_expectations.datasource",
                },
                "Datasource 5: Snowflake": {
                    "class_name": "Datasource",
                    "data_connectors": {
                        "default_configured_asset_sql_data_connector_snowflake": {
                            "assets": {
                                "taxi_data": {
                                    "table_name": "taxi_data",
                                    "type": "table",
                                }
                            },
                            "batch_spec_passthrough": {"sample": "value"},
                            "class_name": "ConfiguredAssetSqlDataConnector",
                        }
                    },
                    "execution_engine": {
                        "class_name": "SqlAlchemyExecutionEngine",
                        "connection_string": f"snowflake://no_user:{conn_string_password}@some_url/foo/PUBLIC?role=PUBLIC&warehouse=bar",
                    },
                    "module_name": "great_expectations.datasource",
                },
                "Datasource 6: Spark": {
                    "class_name": "Datasource",
                    "data_connectors": {
                        "default_runtime_data_connector": {
                            "batch_identifiers": ["batch", "identifiers", "here"],
                            "batch_spec_passthrough": {"sample": "value"},
                            "class_name": "RuntimeDataConnector",
                        }
                    },
                    "execution_engine": {"class_name": "SparkDFExecutionEngine"},
                    "module_name": "great_expectations.datasource",
                },
            },
            "stores": {
                "expectations_store": {
                    "class_name": "ExpectationsStore",
                    "store_backend": {
                        "class_name": "TupleFilesystemStoreBackend",
                        "base_directory": "expectations/",
                    },
                },
                "evaluation_parameter_store": {
                    "module_name": "great_expectations.data_context.store",
                    "class_name": "EvaluationParameterStore",
                },
            },
            "data_docs_sites": {},
            "validation_operators": {
                "default": {
                    "class_name": "ActionListValidationOperator",
                    "action_list": [],
                }
            },
            "anonymous_usage_statistics": {
                "enabled": True,
                "data_context_id": "6a52bdfa-e182-455b-a825-e69f076e67d6",
                "usage_statistics_url": USAGE_STATISTICS_QA_URL,
            },
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
            "evaluation_parameter_store_name": "evaluation_parameter_store",
            "validations_store_name": "does_not_have_to_be_real",
            "expectations_store_name": "expectations_store",
            "config_variables_file_path": "uncommitted/config_variables.yml",
            "datasources": {},
            "stores": {
                "default_checkpoint_store": {
                    "class_name": "CheckpointStore",
                    "store_backend": {
                        "class_name": "GeCloudStoreBackend",
                        "ge_cloud_base_url": "http://foo/bar/",
                        "ge_cloud_credentials": {
                            "access_token": ge_cloud_access_token,
                            "organization_id": org_id,
                        },
                        "ge_cloud_resource_type": "checkpoint",
                        "suppress_store_backend_id": True,
                    },
                },
                "default_evaluation_parameter_store": {
                    "class_name": "EvaluationParameterStore"
                },
                "default_expectations_store": {
                    "class_name": "ExpectationsStore",
                    "store_backend": {
                        "class_name": "GeCloudStoreBackend",
                        "ge_cloud_base_url": "http://foo/bar/",
                        "ge_cloud_credentials": {
                            "access_token": ge_cloud_access_token,
                            "organization_id": org_id,
                        },
                        "ge_cloud_resource_type": "expectation_suite",
                        "suppress_store_backend_id": True,
                    },
                },
                "default_validations_store": {
                    "class_name": "ValidationsStore",
                    "store_backend": {
                        "class_name": "GeCloudStoreBackend",
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
            "validation_operators": {
                "default": {
                    "class_name": "ActionListValidationOperator",
                    "action_list": [],
                }
            },
            "anonymous_usage_statistics": {
                "enabled": True,
                "data_context_id": "6a52bdfa-e182-455b-a825-e69f076e67d6",
                "usage_statistics_url": USAGE_STATISTICS_QA_URL,
            },
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
def mocked_global_config_dirs(tmp_path):
    mock_global_config_dot_dir = tmp_path / ".great_expectations"
    mock_global_config_dot_dir_file = (
        mock_global_config_dot_dir / "great_expectations.conf"
    )
    mock_global_config_dot_dir.mkdir(parents=True)
    mock_global_config_etc_dir = tmp_path / "etc"
    mock_global_config_etc_file = mock_global_config_etc_dir / "great_expectations.conf"
    mock_global_config_etc_dir.mkdir(parents=True)

    mock_global_config_paths = [
        str(mock_global_config_dot_dir_file),
        str(mock_global_config_etc_file),
    ]

    return (
        mock_global_config_dot_dir,
        mock_global_config_etc_dir,
        mock_global_config_paths,
    )


@pytest.fixture
def data_context_with_empty_global_config_dirs(
    mocked_global_config_dirs,
):
    with patch(
        "great_expectations.data_context.data_context.AbstractDataContext.GLOBAL_CONFIG_PATHS",
        new_callable=PropertyMock,
    ) as mock:
        (
            mock_global_config_dot_dir,
            mock_global_config_etc_dir,
            mock_global_config_paths,
        ) = mocked_global_config_dirs
        mock.return_value = mock_global_config_paths

        yield


@pytest.fixture
def data_context_with_complete_global_config_in_dot_and_etc_dirs(
    mocked_global_config_dirs,
):
    with patch(
        "great_expectations.data_context.data_context.AbstractDataContext.GLOBAL_CONFIG_PATHS",
        new_callable=PropertyMock,
    ) as mock:
        (
            mock_global_config_dot_dir,
            mock_global_config_etc_dir,
            mock_global_config_paths,
        ) = mocked_global_config_dirs
        mock.return_value = mock_global_config_paths

        shutil.copy(
            file_relative_path(
                __file__,
                "fixtures/conf/great_expectations_cloud_config_complete_1.conf",
            ),
            str(os.path.join(mock_global_config_dot_dir, "great_expectations.conf")),
        )
        shutil.copy(
            file_relative_path(
                __file__,
                "fixtures/conf/great_expectations_cloud_config_complete_2.conf",
            ),
            str(os.path.join(mock_global_config_etc_dir, "great_expectations.conf")),
        )
        yield


@pytest.fixture
def data_context_with_complete_global_config_in_dot_dir_only(mocked_global_config_dirs):
    with patch(
        "great_expectations.data_context.data_context.AbstractDataContext.GLOBAL_CONFIG_PATHS",
        new_callable=PropertyMock,
    ) as mock:
        (
            mock_global_config_dot_dir,
            mock_global_config_etc_dir,
            mock_global_config_paths,
        ) = mocked_global_config_dirs
        mock.return_value = mock_global_config_paths

        shutil.copy(
            file_relative_path(
                __file__,
                "fixtures/conf/great_expectations_cloud_config_complete_1.conf",
            ),
            str(os.path.join(mock_global_config_dot_dir, "great_expectations.conf")),
        )
        yield


@pytest.fixture
def data_context_with_complete_global_config_with_usage_stats_section_in_dot_dir_only(
    mocked_global_config_dirs,
):
    with patch(
        "great_expectations.data_context.data_context.AbstractDataContext.GLOBAL_CONFIG_PATHS",
        new_callable=PropertyMock,
    ) as mock:
        (
            mock_global_config_dot_dir,
            mock_global_config_etc_dir,
            mock_global_config_paths,
        ) = mocked_global_config_dirs
        mock.return_value = mock_global_config_paths

        shutil.copy(
            file_relative_path(
                __file__,
                "fixtures/conf/great_expectations_cloud_config_complete_with_usage_stats_section.conf",
            ),
            str(os.path.join(mock_global_config_dot_dir, "great_expectations.conf")),
        )
        yield


@pytest.fixture
def data_context_with_complete_global_config_in_etc_dir_only(mocked_global_config_dirs):
    with patch(
        "great_expectations.data_context.data_context.AbstractDataContext.GLOBAL_CONFIG_PATHS",
        new_callable=PropertyMock,
    ) as mock:
        (
            mock_global_config_dot_dir,
            mock_global_config_etc_dir,
            mock_global_config_paths,
        ) = mocked_global_config_dirs
        mock.return_value = mock_global_config_paths

        shutil.copy(
            file_relative_path(
                __file__,
                "fixtures/conf/great_expectations_cloud_config_complete_2.conf",
            ),
            str(os.path.join(mock_global_config_etc_dir, "great_expectations.conf")),
        )
        yield


@pytest.fixture
def data_context_with_incomplete_global_config_in_dot_dir_only(
    mocked_global_config_dirs,
):
    # missing access_token
    with patch(
        "great_expectations.data_context.data_context.AbstractDataContext.GLOBAL_CONFIG_PATHS",
        new_callable=PropertyMock,
    ) as mock:
        (
            mock_global_config_dot_dir,
            mock_global_config_etc_dir,
            mock_global_config_paths,
        ) = mocked_global_config_dirs
        mock.return_value = mock_global_config_paths

        shutil.copy(
            file_relative_path(
                __file__,
                "fixtures/conf/great_expectations_cloud_config_minimal_missing_token_1.conf",
            ),
            str(os.path.join(mock_global_config_dot_dir, "great_expectations.conf")),
        )
        yield


@pytest.fixture
def datasource_name() -> str:
    return "my_first_datasource"


@pytest.fixture
def datasource_store_name() -> str:
    return "datasource_store"


@pytest.fixture
def request_headers(ge_cloud_access_token) -> Dict[str, str]:
    return {
        "Content-Type": "application/vnd.api+json",
        "Authorization": f"Bearer {ge_cloud_access_token}",
        "Gx-Version": ge.__version__,
    }


@pytest.fixture
def datasource_config() -> DatasourceConfig:
    return DatasourceConfig(
        class_name="Datasource",
        execution_engine={
            "class_name": "PandasExecutionEngine",
            "module_name": "great_expectations.execution_engine",
        },
        data_connectors={
            "tripdata_monthly_configured": {
                "class_name": "ConfiguredAssetFilesystemDataConnector",
                "module_name": "great_expectations.datasource.data_connector",
                "base_directory": "/path/to/trip_data",
                "assets": {
                    "yellow": {
                        "class_name": "Asset",
                        "module_name": "great_expectations.datasource.data_connector.asset",
                        "pattern": r"yellow_tripdata_(\d{4})-(\d{2})\.csv$",
                        "group_names": ["year", "month"],
                    }
                },
            }
        },
    )


@pytest.fixture
def shared_called_with_request_kwargs(request_headers) -> dict:
    """
    Standard request kwargs that all GeCloudStoreBackend http calls are made with.
    Use in combination with `assert_called_with()`
    """
    return dict(timeout=GeCloudStoreBackend.TIMEOUT, headers=request_headers)


JSONData = Union[AnyPayload, Dict[str, Any]]
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
        self.headers = headers or {
            "content-type": "application/json" if json_data else "text/html"
        }
        self._exc_to_raise = exc_to_raise

    def json(self):
        if self.headers.get("content-type") == "application/json":
            return self._json_data
        raise json.JSONDecodeError("Uh oh - check content-type", "foobar", 1)

    def raise_for_status(self):
        if self._exc_to_raise:
            raise self._exc_to_raise
        if self.status_code >= 400:
            raise requests.exceptions.HTTPError(
                f"Mock {self.status_code} HTTPError", response=self
            )
        return None

    def __repr__(self):
        return f"<Response [{self.status_code}]>"


@pytest.fixture
def mock_response_factory() -> Callable[
    [JSONData, int, Optional[RequestError]], MockResponse
]:
    def _make_mock_response(
        json_data: JSONData,
        status_code: int,
        exc_to_raise: Optional[RequestError] = None,
    ) -> MockResponse:
        return MockResponse(
            json_data=json_data, status_code=status_code, exc_to_raise=exc_to_raise
        )

    return _make_mock_response


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
        "requests",
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
