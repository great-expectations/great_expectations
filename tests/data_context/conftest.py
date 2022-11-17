import copy
import json
import os
import shutil
import unittest.mock
from typing import Any, Callable, Dict, Optional, Union, cast
from unittest.mock import Mock, patch

import pytest
import requests

import great_expectations as ge
from great_expectations import DataContext
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context.store.gx_cloud_store_backend import (
    AnyPayload,
    GXCloudStoreBackend,
)
from great_expectations.data_context.types.base import (
    DataContextConfig,
    DatasourceConfig,
)
from great_expectations.data_context.util import file_relative_path
from tests.integration.usage_statistics.test_integration_usage_statistics import (
    USAGE_STATISTICS_QA_URL,
)

yaml = YAMLHandler()


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
                "default_evaluation_parameter_store": {
                    "class_name": "EvaluationParameterStore"
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
                "default_validations_store": {
                    "class_name": "ValidationsStore",
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
def datasource_config_with_names_and_ids(
    datasource_config_with_names: DatasourceConfig,
    fake_datasource_id: str,
    fake_data_connector_id: str,
) -> DatasourceConfig:
    """
    An extension of the `datasource_config_with_names` fixture
    but contains ids for BOTH the top-level Datasource as well
    as the nested DataConnectors.
    """
    updated_config = copy.deepcopy(datasource_config_with_names)

    # Update top-level Datasource
    updated_config["id"] = fake_datasource_id

    # Update nested DataConnectors
    data_connector_name = tuple(datasource_config_with_names.data_connectors.keys())[0]
    updated_config.data_connectors[data_connector_name]["name"] = data_connector_name
    updated_config.data_connectors[data_connector_name]["id"] = fake_data_connector_id

    return updated_config


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
        "config_version": 1.0,
        "class_name": "Checkpoint",
        "expectation_suite_name": "oss_test_expectation_suite",
        "validations": [
            {
                "expectation_suite_name": "taxi.demo_pass",
            },
            {
                "batch_request": {
                    "datasource_name": "oss_test_datasource",
                    "data_connector_name": "oss_test_data_connector",
                    "data_asset_name": "users",
                },
            },
        ],
    }
    return checkpoint_config


@pytest.fixture
def mocked_datasource_get_response(
    mock_response_factory: Callable,
    datasource_config_with_names_and_ids: DatasourceConfig,
    fake_datasource_id: str,
) -> Callable[[], MockResponse]:
    def _mocked_get_response(*args, **kwargs):
        created_by_id = "c06ac6a2-52e0-431e-b878-9df624edc8b8"
        organization_id = "046fe9bc-c85b-4e95-b1af-e4ce36ba5384"

        return mock_response_factory(
            {
                "data": {
                    "attributes": {
                        "datasource_config": datasource_config_with_names_and_ids.to_json_dict(),
                        "created_at": "2022-08-02T17:55:45.107550",
                        "created_by_id": created_by_id,
                        "deleted": False,
                        "deleted_at": None,
                        "desc": None,
                        "name": datasource_config_with_names_and_ids.name,
                        "organization_id": f"{organization_id}",
                        "updated_at": "2022-08-02T17:55:45.107550",
                    },
                    "id": fake_datasource_id,
                    "links": {
                        "self": f"/organizations/{organization_id}/datasources/{fake_datasource_id}"
                    },
                    "type": "datasource",
                },
            },
            200,
        )

    return _mocked_get_response


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


@pytest.fixture
def cloud_data_context_in_cloud_mode_with_datasource_pandas_engine(
    empty_data_context_in_cloud_mode: DataContext,
    db_file,
    mocked_datasource_get_response,
):
    context: DataContext = empty_data_context_in_cloud_mode
    config = yaml.load(
        f"""
    class_name: Datasource
    execution_engine:
        class_name: PandasExecutionEngine
    data_connectors:
        default_runtime_data_connector_name:
            class_name: RuntimeDataConnector
            batch_identifiers:
                - default_identifier_name
        """,
    )
    with patch(
        "great_expectations.data_context.store.gx_cloud_store_backend.GXCloudStoreBackend.list_keys"
    ), patch(
        "great_expectations.data_context.store.gx_cloud_store_backend.GXCloudStoreBackend._set"
    ), patch(
        "requests.Session.get",
        autospec=True,
        side_effect=mocked_datasource_get_response,
    ):
        context.add_datasource(
            "my_datasource",
            **config,
        )
    return context
