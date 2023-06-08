from __future__ import annotations

import copy
import logging
from typing import TYPE_CHECKING, Optional

from great_expectations.datasource.data_connector.configured_asset_sql_data_connector import (
    ConfiguredAssetSqlDataConnector,  # noqa: TCH001
)
from great_expectations.datasource.new_datasource import BaseDatasource

if TYPE_CHECKING:
    from great_expectations.compatibility import sqlalchemy
    from great_expectations.execution_engine import SqlAlchemyExecutionEngine

logger = logging.getLogger(__name__)


class SimpleSqlalchemyDatasource(BaseDatasource):
    """A specialized Datasource for SQL backends

    SimpleSqlalchemyDatasource is designed to minimize boilerplate configuration and new concepts

    # <Alex>8/25/2022</Alex>
    Formal "SQLAlchemy" "DataConnector" classes, "ConfiguredAssetSqlDataConnector" and "InferredAssetSqlDataConnector",
    have been certified; hence, they should be used going forward.  On the other hand, "SimpleSqlalchemyDatasource",
    fully implemented on top of "ConfiguredAssetSqlDataConnector" and "InferredAssetSqlDataConnector", remains
    potentially useful for demonstration purposes, and should be maintained, even though it violates abstractions.
    """

    execution_engine: SqlAlchemyExecutionEngine

    def __init__(  # noqa: PLR0913
        self,
        name: str,
        connection_string: Optional[str] = None,
        url: Optional[str] = None,
        credentials: Optional[dict] = None,
        engine: Optional[sqlalchemy.Engine] = None,  # sqlalchemy.engine.Engine
        introspection: Optional[dict] = None,
        tables: Optional[dict] = None,
        **kwargs,
    ) -> None:
        introspection = introspection or {}
        tables = tables or {}

        # In "SQLAlchemy" datasources, "connection_string" resides in "SqlAlchemyExecutionEngine" (not "DataConnector").
        self._execution_engine_config = {
            "class_name": "SqlAlchemyExecutionEngine",
            "connection_string": connection_string,
            "url": url,
            "credentials": credentials,
            "engine": engine,
        }
        self._execution_engine_config.update(**kwargs)

        super().__init__(name=name, execution_engine=self._execution_engine_config)

        self._data_connectors: dict = {}

        self._init_data_connectors(
            introspection_configs=introspection,
            table_configs=tables,
        )

        # NOTE: Abe 20201111 : This is incorrect. Will need to be fixed when we reconcile all the configs.
        self._datasource_config: dict = {}

    # noinspection PyMethodOverriding
    # Note: This method is meant to overwrite Datasource._init_data_connectors (despite signature mismatch).
    def _init_data_connectors(
        self,
        introspection_configs: dict,
        table_configs: dict,
    ) -> None:
        # Step-1: Build "DataConnector" objects for introspected tables/views (using "InferredAssetSqlDataConnector").
        for name, config in introspection_configs.items():
            data_connector_config = dict(
                **{
                    "class_name": "InferredAssetSqlDataConnector",
                    "name": name,
                },
                **config,
            )
            self._build_data_connector_from_config(
                name=name,
                config=data_connector_config,
            )

        # Step-2: Build "DataConnector" objects for tables (using "ConfiguredAssetSqlDataConnector").
        for data_asset_name, table_config in table_configs.items():
            for partitioner_name, partitioner_config in table_config[
                "partitioners"
            ].items():
                data_connector_name: str = partitioner_name
                if data_connector_name not in self.data_connectors:
                    data_connector_config = {
                        "class_name": "ConfiguredAssetSqlDataConnector",
                        "assets": {},
                    }
                    self._build_data_connector_from_config(
                        name=data_connector_name, config=data_connector_config
                    )

                data_connector: ConfiguredAssetSqlDataConnector = self.data_connectors[
                    data_connector_name
                ]

                data_asset_config: dict = copy.deepcopy(partitioner_config)

                if "table_name" not in data_asset_config:
                    data_asset_config["table_name"] = data_asset_name

                # "ConfiguredAssetSqlDataConnector" has only one asset type: "table" (adding for interface consistency).
                data_asset_config["type"] = "table"

                # If config contains any prefix, suffix or schema_name values,
                # they will be handled at the ConfiguredAssetSqlDataConnector-level
                data_connector.add_data_asset(
                    name=data_asset_name,
                    config=data_asset_config,
                )
