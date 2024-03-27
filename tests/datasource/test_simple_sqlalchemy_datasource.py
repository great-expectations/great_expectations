import logging
from typing import Any, Dict, List, Tuple, Type, Union

import pytest

try:
    import sqlalchemy as sa
except ImportError:
    sa = None
    pytest.skip("sqlalchemy is not installed", allow_module_level=True)

from great_expectations.datasource.data_connector import (
    ConfiguredAssetSqlDataConnector,
    InferredAssetSqlDataConnector,
)
from great_expectations.datasource.simple_sqlalchemy_datasource import (
    SimpleSqlalchemyDatasource,
)
from great_expectations.exceptions import ExecutionEngineError
from great_expectations.execution_engine import SqlAlchemyExecutionEngine

# We only create these test doubles when sqlalchemy is importable
if sa:
    # This is a SQLAlchemy object we are creating a test double for. We need this double because
    # the SimpleSqlalchemyDatasource under test takes it as an input.
    class DummySAEngine(sa.engine.Engine):
        # Logger is configured to log nothing
        logger = logging.Logger(name="DummySAEngineLogger", level=55)

        def __init__(self, *args, **kwargs):
            # We intentionally don't call super.__init__ because this is a dummy.
            self.dialect = DummySAEngine.DummyDialect()
            self.url = None

        def raw_connection(self, _connection=None):
            return DummySAEngine.DummyConnection()

        class DummyDialect:
            def __init__(self):
                self.name = "dummy_dialect"
                self.pool = DummySAEngine.DummyPool

            def get_schema_names(self, *args, **kwargs) -> List[str]:
                # This is a list of schema names we expect to see in the test cases, which is empty list.  # noqa: E501
                return []

            def get_table_names(self, *args, **kwargs) -> List[str]:
                # This is a list of table names we expect to see in the test cases, which is an empty list.  # noqa: E501
                return []

            def get_view_names(self, *args, **kwargs) -> List[str]:
                # This is a list of view names we expect to see in the test cases, which is an empty list.  # noqa: E501
                return []

        class DummyConnection:
            def close(self):
                pass

        class DummyPool:
            connect = None

    # This is Great Expectations' SQLAlchemy wrapper. We need to mock this out because
    # SimpleSqlalchemyDatasource, the class under test, creates this under the hood, wrapping
    # the passed in `sa.engine.Engine` object. However, we don't want to test the
    # functionality of SqlAlchemyExecutionEngine in the SimpleSqlalchemyDatasource unit tests.
    class DummySqlAlchemyExecutionEngine(SqlAlchemyExecutionEngine):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)

        def get_data_for_batch_identifiers(
            self,
            selectable: sa.sql.Selectable,
            partitioner_method_name: str,
            partitioner_kwargs: dict,
        ) -> List[dict]:
            return [{}]


def _datasource_asserts(
    ds: SimpleSqlalchemyDatasource,
    connection_str: str,
    url: str,
    credentials: Dict[str, str],
    expected_data_connector_types: Dict[str, Type],
    expected_data_assets_with_types: Dict[str, List[Tuple[str, str]]],
):
    # Some of this is checks on BaseDatasource
    assert ds.name == "simple_sqlalchemy_datasource"
    assert ds.id is None
    assert ds.config == {}
    assert isinstance(ds.execution_engine, SqlAlchemyExecutionEngine)
    assert ds.execution_engine.credentials == credentials
    assert ds.recognized_batch_parameters == {"limit"}

    # We assert we only see the data connector names we expect. Then we assert
    # their types are correct.
    assert ds.data_connectors.keys() == expected_data_connector_types.keys()
    for name, expected_type in expected_data_connector_types.items():
        assert type(ds.data_connectors[name]) is expected_type
    # We assert the data assets and types are exactly what we expect. Then we
    # extract the names from `expected_data_assets_with_types` and verify the
    # names match what is returned from ds.get_available_data_asset_names()
    assert ds.get_available_data_asset_names_and_types() == expected_data_assets_with_types
    expected_data_assets: Dict[str, List[str]] = {}
    for key, asset_with_type in expected_data_assets_with_types.items():
        expected_data_assets[key] = []
        for asset_name, _ in asset_with_type:
            expected_data_assets[name].append(asset_name)
    assert ds.get_available_data_asset_names() == expected_data_assets


# The configured data sources, "whole_table", "hourly", and "daily", all have unique names
# because if any of them share a name they will clobber each other.
@pytest.mark.unit
@pytest.mark.parametrize("connection", [None, "lets-connect"])
@pytest.mark.parametrize("url", [None, "https://url.com"])
@pytest.mark.parametrize("credentials", [None, {"username": "foo", "password": "bar"}])
@pytest.mark.parametrize(
    "introspection",
    [
        {},
        {
            "whole_table": {"excluded_tables": ["table1"]},
            "hourly": {
                "included_tables": ["table2"],
                "partitioner_kwargs": {
                    "column_name": "timestamp",
                    "date_format_string": "%Y-%m-%d:%H",
                },
                "partitioner_method": "_partition_on_converted_datetime",
            },
        },
    ],
)
@pytest.mark.parametrize(
    "tables",
    [
        {},
        {
            "my_table": {
                "partitioners": {
                    "daily": {
                        "partitioner_kwargs": {
                            "column_name": "date",
                            "date_format_string": "%Y-%m-%d",
                        },
                        "partitioner_method": "_partition_on_converted_datetime",
                    }
                }
            }
        },
    ],
)
def test_simple_sqlalchemy_datasource_init(connection, url, credentials, introspection, tables):
    kwargs = {
        "module_name": "tests.datasource.test_simple_sqlalchemy_datasource",
        "class_name": "DummySqlAlchemyExecutionEngine",
    }
    datasource = SimpleSqlalchemyDatasource(
        name="simple_sqlalchemy_datasource",
        connection_string=connection,
        url=url,
        credentials=credentials,
        engine=DummySAEngine(),
        introspection=introspection,
        tables=tables,
        **kwargs,
    )
    expected_data_connector_types = _expected_data_connector_types(introspection, tables)
    expected_data_assets_with_types = _expected_data_assets_with_types(introspection, tables)
    _datasource_asserts(
        datasource,
        connection,
        url,
        credentials,
        expected_data_connector_types,
        expected_data_assets_with_types,
    )


SqlDataConnectorType = Union[
    Type[InferredAssetSqlDataConnector], Type[ConfiguredAssetSqlDataConnector]
]


def _expected_data_connector_types(
    introspection: Dict[str, Any], tables: Dict[str, Dict[str, Any]]
) -> Dict[str, SqlDataConnectorType]:
    expected_data_connector_types: Dict[str, SqlDataConnectorType] = {}
    for key in introspection.keys():
        expected_data_connector_types[key] = InferredAssetSqlDataConnector
    for _, table_dict in tables.items():
        for key in table_dict["partitioners"].keys():
            expected_data_connector_types[key] = ConfiguredAssetSqlDataConnector
    return expected_data_connector_types


def _expected_data_assets_with_types(
    introspection: Dict[str, Any], tables: Dict[str, Dict[str, Any]]
) -> Dict[str, List[Tuple[str, str]]]:
    expected_data_assets_with_types: Dict[str, List[Tuple[str, str]]] = {}
    for key in introspection.keys():
        expected_data_assets_with_types[key] = []
    for table_name, table_dict in tables.items():
        for key in table_dict["partitioners"].keys():
            if key not in expected_data_assets_with_types:
                expected_data_assets_with_types[key] = []
            expected_data_assets_with_types[key].append((table_name, "table"))
    return expected_data_assets_with_types


@pytest.mark.unit
def test_simple_sqlalchemy_datasource_init_fails_with_no_engine():
    with pytest.raises(ExecutionEngineError):
        SimpleSqlalchemyDatasource(
            name="simple_sqlalchemy_datasource",
            connection_string="connect",
            url="http://url.com",
            credentials={"user": "name"},
            engine=None,
            introspection={},
            tables={},
        )
