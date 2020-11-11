import logging

from great_expectations.data_context.util import instantiate_class_from_config
from great_expectations.execution_environment import BaseExecutionEnvironment
# from great_expectations.execution_engine import SqlAlchemyExecutionEngine
from great_expectations.execution_environment.data_connector import SqlDataConnector

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
        data_context_root_directory=None,
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

        self._data_connector_config = {
            "class_name": "SqlDataConnector",
            "name": "ONLY_DATA_CONNECTOR",
            "data_assets": {},
        }
        self._data_connector = instantiate_class_from_config(
            config=self._data_connector_config,
            runtime_environment={
                "execution_engine": self._execution_engine,
                "execution_environment_name": self._name,
            },
            config_defaults={
                "module_name": "great_expectations.execution_environment.data_connector"
            },
        )

        # THIS IS WRONG.
        self._execution_environment_config = {}
        #     "execution_engine": self._execution_engine_config,
        #     "data_connectors" : {
        #         self._data_connector_config["name"] : self._data_connector_config
        #     },
        # }

        self._data_connectors = {
            "ONLY_DATA_CONNECTOR" : self._data_connector
        }
