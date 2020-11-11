import logging
from typing import List, Dict

from great_expectations.data_context.util import instantiate_class_from_config
from great_expectations.execution_environment import BaseExecutionEnvironment
# from great_expectations.execution_engine import SqlAlchemyExecutionEngine
from great_expectations.execution_environment.data_connector import (
    DataConnector,
    SqlDataConnector,
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
        # data_context_root_directory=None,
        introspection: Dict={},
        tables: Dict={},
    ):
        super().__init__(
            name=name,
            # data_context_root_directory=data_context_root_directory,
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
        self._init_data_connectors(introspection)

        # THIS IS WRONG.
        self._execution_environment_config = {}
        #     "execution_engine": self._execution_engine_config,
        #     "data_connectors" : {
        #         self._data_connector_config["name"] : self._data_connector_config
        #     },
        # }


    def _init_data_connectors(
        self,
        introspection_configs: Dict,
    ):

        for name, config in introspection_configs.items():
            data_connector_config = self._gen_data_connector_config(
                name=name,
                config=config,
                introspecting=True,
            )
            self._build_data_connector_from_config(
                name,
                data_connector_config,
            )

    def _gen_data_connector_config(
        self,
        name: str,
        config: Dict,
        introspecting,
    ):
        if introspecting:
            data_connector_config = dict(**{
                "class_name": "IntrospectingSqlDataConnector",
                "name": name,
            }, **config)

        else:
            data_connector_config = {
                "class_name": "SqlDataConnector",
                "name": name,
                "data_assets": {},
            }
        return data_connector_config

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
