import copy
import logging

from great_expectations.datasource.data_connector.configured_asset_sql_data_connector import (
    ConfiguredAssetSqlDataConnector,
)
from great_expectations.datasource.new_datasource import BaseDatasource

logger = logging.getLogger(__name__)


class SimpleSqlalchemyDatasource(BaseDatasource):
    """A specialized Datasource for SQL backends

    SimpleSqlalchemyDatasource is designed to minimize boilerplate configuration and new concepts
    """

    def __init__(
        self,
        name: str,
        connection_string: str = None,
        url: str = None,
        credentials: dict = None,
        engine=None,  # sqlalchemy.engine.Engine
        introspection: dict = None,
        tables: dict = None,
        **kwargs
    ) -> None:
        introspection = introspection or {}
        tables = tables or {}

        self._execution_engine_config = {
            "class_name": "SqlAlchemyExecutionEngine",
            "connection_string": connection_string,
            "url": url,
            "credentials": credentials,
            "engine": engine,
        }

        self._execution_engine_config.update(**kwargs)

        super().__init__(name=name, execution_engine=self._execution_engine_config)

        self._data_connectors = {}
        self._init_data_connectors(
            introspection_configs=introspection,
            table_configs=tables,
        )

        # NOTE: Abe 20201111 : This is incorrect. Will need to be fixed when we reconcile all the configs.
        self._datasource_config = {}

    # noinspection PyMethodOverriding
    # Note: This method is meant to overwrite Datasource._init_data_connectors (despite signature mismatch).
    def _init_data_connectors(
        self,
        introspection_configs: dict,
        table_configs: dict,
    ) -> None:

        # Step-1: Build DataConnectors for introspected assets
        for name, config in introspection_configs.items():
            data_connector_config: dict = dict(
                **{
                    "class_name": "InferredAssetSqlDataConnector",
                    "name": name,
                },
                **config
            )
            self._build_data_connector_from_config(
                name,
                data_connector_config,
            )

        # Step-2: Build DataConnectors for tables. They will map to configured assets
        for table_name, table_config in table_configs.items():
            for partitioner_name, partitioner_config in table_config[
                "partitioners"
            ].items():

                data_connector_name: str = partitioner_name
                if data_connector_name not in self.data_connectors:
                    data_connector_config: dict = {
                        "class_name": "ConfiguredAssetSqlDataConnector",
                        "assets": {},
                    }
                    self._build_data_connector_from_config(
                        data_connector_name, data_connector_config
                    )

                data_connector: ConfiguredAssetSqlDataConnector = self.data_connectors[
                    data_connector_name
                ]

                data_asset_config: dict = copy.deepcopy(partitioner_config)
                data_asset_config["table_name"] = table_name
                data_asset_name: str = table_name

                # If config contains any prefix, suffix or schema_name values,
                # they will be handled at the ConfiguredAssetSqlDataConnector-level
                data_connector.add_data_asset(
                    data_asset_name,
                    data_asset_config,
                )
