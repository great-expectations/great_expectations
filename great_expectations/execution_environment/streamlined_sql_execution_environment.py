import logging

from great_expectations.data_context.util import instantiate_class_from_config
from great_expectations.execution_environment import BaseExecutionEnvironment
# from great_expectations.execution_engine import SqlAlchemyExecutionEngine

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

        execution_engine_config = {
            "class_name": "SqlAlchemyExecutionEngine",
            "connection_string": connection_string,
            "url": url,
            "credentials": credentials,
            "engine": engine,
        }
        self._execution_engine = instantiate_class_from_config(
            config=execution_engine_config,
            runtime_environment={},
            config_defaults={"module_name": "great_expectations.execution_engine"},
        )
        
        self._data_connectors_cache = {}
