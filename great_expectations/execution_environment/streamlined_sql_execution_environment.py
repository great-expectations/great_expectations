import logging
from typing import List, Dict
import copy

from great_expectations.data_context.util import instantiate_class_from_config
from great_expectations.execution_environment import BaseExecutionEnvironment
from great_expectations.execution_environment.data_connector import (
    DataConnector,
    ConfiguredAssetSqlDataConnector,
)

logger = logging.getLogger(__name__)


class StreamlinedSqlExecutionEnvironment(BaseExecutionEnvironment):
    """A specialized ExecutionEnvironment for SQL backends

    StreamlinedSqlExecutionEnvironment is designed to minimize boilerplate configuration and new concepts
    """

    def __init__(
        self,
        name: str,
        connection_string: str=None,
        url: str=None,
        credentials: dict=None,
        engine=None, #SqlAlchemyExecutionEngine
        introspection: Dict={},
        tables: Dict={},
    ):
        super().__init__(
            name=name,
        )

        self._execution_engine_config = {
            "class_name": "SqlAlchemyExecutionEngine",
            "connection_string": connection_string,
            "url": url,
            "credentials": credentials,
            "engine": engine,
        }
        self._execution_engine = instantiate_class_from_config(
            config=self._execution_engine_config,
            runtime_environment={},
            config_defaults={"module_name": "great_expectations.execution_engine"},
        )

        self._data_connectors = {}
        self._init_data_connectors(
            introspection,
            tables,
        )

        # NOTE: Abe 20201111 : This is incorrect. Will need to be fixed when we reconcile all the configs.
        self._execution_environment_config = {}

    def _init_data_connectors(
        self,
        introspection_configs: Dict,
        table_configs: Dict,
    ):

        # First, build DataConnectors for introspected assets
        for name, config in introspection_configs.items():
            data_connector_config = dict(**{
                "class_name": "InferredAssetSqlDataConnector",
                "name": name,
            }, **config)
            self._build_data_connector_from_config(
                name,
                data_connector_config,
            )

        # Second, build DataConnectors for tables. They will map to configured data_assets
        for table_name, table_config in table_configs.items():
            for partitioner_name, partitioner_config in table_config["partitioners"].items():

                data_connector_name = partitioner_name
                if not data_connector_name in self.data_connectors:
                    data_connector_config = {
                        "class_name": "ConfiguredAssetSqlDataConnector",
                        "data_assets": {}
                    }
                    self._build_data_connector_from_config(data_connector_name, data_connector_config)

                data_connector = self.data_connectors[data_connector_name]

                data_asset_config = copy.deepcopy(partitioner_config)
                data_asset_config["table_name"] = table_name

                data_asset_name_suffix = data_asset_config.pop("data_asset_name_suffix", "__"+data_connector_name)
                data_asset_name = table_name+data_asset_name_suffix

                data_connector.add_data_asset(
                    data_asset_name,
                    data_asset_config,
                )

    def _build_data_connector_from_config(
        self,
        name: str,
        config: Dict,
    ) -> DataConnector:
        """Build a DataConnector using the provided configuration and return the newly-built DataConnector."""

        new_data_connector: DataConnector = instantiate_class_from_config(
            config=config,
            runtime_environment={
                "name": name,
                "execution_environment_name": self.name,
                "execution_engine": self.execution_engine,
            },
            config_defaults={
                "module_name": "great_expectations.execution_environment.data_connector"
            },
        )

        self._data_connectors[name] = new_data_connector
        return new_data_connector
