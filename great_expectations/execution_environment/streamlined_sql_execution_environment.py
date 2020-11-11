import logging
from typing import List, Dict
import copy

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
        self._init_data_connectors(
            introspection,
            tables,
        )

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
        table_configs: Dict,
    ):

        for name, config in introspection_configs.items():
            data_connector_config = dict(**{
                "class_name": "IntrospectingSqlDataConnector",
                "name": name,
            }, **config)
            self._build_data_connector_from_config(
                name,
                data_connector_config,
            )

# tables:
#     table_partitioned_by_date_column__A:
#         partitioners:
#             daily: 
#                 data_asset_name_suffix: __daily
#                 splitter_method: _split_on_converted_datetime
#                 splitter_kwargs:
#                     column_name: date
#                     date_format_string: "%Y-%m-%d"
#             weekly:
#                 include_schema_name: False
#                 data_asset_name_suffix: __some_other_string
#                 splitter_method: _split_on_converted_datetime
#                 splitter_kwargs:
#                     column_name: date
#                     date_format_string: "%Y-%W"
#             by_id_dozens:
#                 include_schema_name: True
#                 # Note: no data_asset_name_suffix
#                 splitter_method: _split_on_divided_integer
#                 splitter_kwargs:
#                     column_name: id
#                     divisor: 12

        for table_name, table_config in table_configs.items():
            for partitioner_name, partitioner_config in table_config["partitioners"].items():

                data_connector_name = partitioner_name
                if not data_connector_name in self.data_connectors:
                    data_connector_config = {
                        "class_name": "SqlDataConnector",
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




    # def _gen_configured_asset_data_connector_config(
    #     self,
    #     name: str,
    #     config: Dict,
    # ):
    #     data_connector_config = {
    #         "class_name": "SqlDataConnector",
    #         "name": name,
    #         "data_assets": {},
    #     }
    #     return data_connector_config

    # def _gen_introspecting_data_connector_config(
    #     self,
    #     name: str,
    #     config: Dict,
    # ):
    #     data_connector_config = dict(**{
    #         "class_name": "IntrospectingSqlDataConnector",
    #         "name": name,
    #     }, **config)
    #     return data_connector_config

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
