# -*- coding: utf-8 -*-

import copy
import logging
from typing import Union, List, Any

from great_expectations.data_context.types.base import (
    DataConnectorConfig,
    dataConnectorConfigSchema
)
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.data_context.util import instantiate_class_from_config
from ruamel.yaml.comments import CommentedMap
import great_expectations.exceptions as ge_exceptions
from great_expectations.validator.validator import Validator
from great_expectations.execution_environment.data_connector.data_connector import DataConnector
from great_expectations.execution_environment.data_connector.pipeline_data_connector import PipelineDataConnector
from great_expectations.execution_environment.types import BatchSpec
from great_expectations.core.batch import Batch

logger = logging.getLogger(__name__)


class ExecutionEnvironment(object):
    """
An ExecutionEnvironment is the glue between an ExecutionEngine and a DataConnector.
    """

    recognized_batch_parameters: set = {"limit"}

    def __init__(
        self,
        name: str,
        execution_engine=None,
        data_connectors=None,
        in_memory_dataset: Any = None,
        data_context=None,
        **kwargs
    ):
        """
        Build a new ExecutionEnvironment.

        Args:
            name: the name for the datasource
            execution_engine (ClassConfig): the type of compute engine to produce
            data_connectors: DataConnectors to add to the datasource
        """
        self._name = name
        self._execution_engine = instantiate_class_from_config(
            config=execution_engine, runtime_environment={},
        )
        self._execution_environment_config = kwargs

        self._execution_environment_config["execution_engine"] = execution_engine

        if data_connectors is None:
            data_connectors = {}
        self._execution_environment_config["data_connectors"] = data_connectors

        self._data_connectors_cache = {}

        self._in_memory_dataset = in_memory_dataset

        self._build_data_connectors()

        self._data_context = data_context

    def get_batch(
        self,
        batch_definition: dict
    ) -> Batch:
        if not batch_definition:
            logger.info("loading a batch without a batch_definition")
            batch_definition = {}

        data_connector_name: str = batch_definition.get("data_connector")
        if not data_connector_name:
            raise ge_exceptions.BatchDefinitionError(message="Batch definition must specify a data_connector.")

        batch_spec: BatchSpec = self.build_batch_spec(
            data_connector_name=data_connector_name,
            batch_definition=batch_definition
        )
        batch_spec = self.execution_engine.process_batch_definition(
            batch_definition=batch_definition,
            batch_spec=batch_spec
        )

        batch: Batch = self.execution_engine.load_batch(batch_spec=batch_spec)
        return batch

    def get_validator(
        self,
        batch_definition: dict,
        expectation_suite_name: Union[str, ExpectationSuite],
    ):
        # noinspection PyUnusedLocal
        batch: Batch = self.get_batch(batch_definition=batch_definition)
        return Validator(
            data_context=self.data_context,
            execution_engine=self.execution_engine,
            expectation_suite_name=expectation_suite_name,
        )

    @classmethod
    def from_configuration(cls, **kwargs):
        """
        Build a new datasource from a configuration dictionary.

        Args:
            **kwargs: configuration key-value pairs

        Returns:
            datasource (Datasource): the newly-created datasource

        """
        return cls(**kwargs)

    @property
    def name(self):
        """
        Property for datasource name
        """
        return self._name

    @property
    def execution_engine(self):
        return self._execution_engine

    @property
    def in_memory_dataset(self) -> Any:
        return self._in_memory_dataset

    @in_memory_dataset.setter
    def in_memory_dataset(self, in_memory_dataset: Any):
        self._in_memory_dataset = in_memory_dataset

    @property
    def data_context(self):
        return self._data_context

    @property
    def config(self):
        return copy.deepcopy(self._execution_environment_config)

    def _build_data_connectors(self):
        """
        Build DataConnector objects from the ExecutionEnvironment configuration.

        Returns:
            None
        """
        for data_connector in self._execution_environment_config["data_connectors"].keys():
            self.get_data_connector(name=data_connector)

    # TODO: <Alex>The style and readability of this method must be improved.</Alex>
    def get_data_connector(self, name: str) -> DataConnector:
        """Get the (named) DataConnector from an ExecutionEnvironment)

        Args:
            name (str): name of DataConnector
            runtime_environment (dict):

        Returns:
            DataConnector (DataConnector)
        """
        data_connector: DataConnector
        if name in self._data_connectors_cache:
            data_connector = self._data_connectors_cache[name]
            if isinstance(data_connector, PipelineDataConnector):
                data_connector.in_memory_dataset = self.in_memory_dataset
            return data_connector
            # TODO: <Alex>The style and readability of this method must be improved.</Alex>
            # return self._data_connectors_cache[name]
        elif (
            "data_connectors" in self._execution_environment_config
            and name in self._execution_environment_config["data_connectors"]
        ):
            data_connector_config: dict = copy.deepcopy(
                self._execution_environment_config["data_connectors"][name]
            )
        else:
            raise ge_exceptions.DataConnectorError(
                f'Unable to load data connector "{name}" -- no configuration found or invalid configuration.'
            )
        data_connector_config: CommentedMap = dataConnectorConfigSchema.load(
            data_connector_config
        )
        # TODO: <Alex>The style and readability of this method must be improved.</Alex>
        # data_connector: DataConnector = self._build_data_connector_from_config(
        #     name=name, config=data_connector_config
        # )
        data_connector = self._build_data_connector_from_config(
            name=name, config=data_connector_config
        )
        if isinstance(data_connector, PipelineDataConnector):
            data_connector.in_memory_dataset = self.in_memory_dataset
        self._data_connectors_cache[name] = data_connector
        return data_connector

    def _build_data_connector_from_config(self, name: str, config: CommentedMap) -> DataConnector:
        """Build a DataConnector using the provided configuration and return the newly-built DataConnector."""
        # We convert from the type back to a dictionary for purposes of instantiation
        if isinstance(config, DataConnectorConfig):
            config: dict = dataConnectorConfigSchema.dump(config)
        config.update({"name": name})
        module_name: str = "great_expectations.execution_environment.data_connector.data_connector"
        runtime_environment: dict = {"execution_environment": self}
        data_connector: DataConnector = instantiate_class_from_config(
            config=config,
            runtime_environment=runtime_environment,
            config_defaults={"module_name": module_name},
        )
        if not data_connector:
            raise ge_exceptions.ClassInstantiationError(
                module_name="great_expectations.execution_environment.data_connector.data_connector",
                package_name=None,
                class_name=config["class_name"],
            )
        return data_connector

    def list_data_connectors(self) -> List[dict]:
        """List currently-configured DataConnector for this ExecutionEnvironment.

        Returns:
            List(dict): each dictionary includes "name" and "type" keys
        """
        data_connectors: List[dict] = []

        if "data_connectors" in self._execution_environment_config:
            for key, value in self._execution_environment_config[
                "data_connectors"
            ].items():
                data_connectors.append({"name": key, "class_name": value["class_name"]})

        return data_connectors

    # TODO: <Alex>This needs to be reviewed...</Alex>
    # TODO: <Alex>What is this for, and how is this used?  This looks like it is for DataSource -- do we still need it for backward compatibility?</Alex>
    def get_available_data_asset_names(self, data_connector_names: list = None) -> dict:
        """
        Returns a dictionary of data_asset_names that the specified data
        connector can provide. Note that some data_connectors may not be
        capable of describing specific named data assets, and some (such as
        files_data_connectors) require the user to configure
        data asset names.

        Args:
            data_connector_names: the DataConnector for which to get available data asset names.

        Returns:
            dictionary consisting of sets of data assets available for the specified data connectors:
            ::

                {
                  data_connector_name: {
                    names: [ (data_asset_1, data_asset_1_type), (data_asset_2, data_asset_2_type) ... ]
                  }
                  ...
                }

        """
        available_data_asset_names: dict = {}
        if data_connector_names is None:
            data_connector_names = [
                data_connector["name"] for data_connector in self.list_data_connectors()
            ]
        elif isinstance(data_connector_names, str):
            data_connector_names = [data_connector_names]

        for data_connector_name in data_connector_names:
            data_connector = self.get_data_connector(name=data_connector_name)
            available_data_asset_names[
                data_connector_name
            ] = data_connector.get_available_data_asset_names()
        return available_data_asset_names

    def build_batch_spec(self, data_connector_name: str, batch_definition: dict) -> BatchSpec:
        """Builds batch_spec using the provided data_connector and batch_definition.

        Args:
            data_connector_name (str): the name of the data_connector to use to build batch_spec
            batch_definition (dict): dict specifying batch - used to generate a batch_spec

        Returns:
            BatchSpec
        """
        data_connector: DataConnector = self.get_data_connector(name=data_connector_name)
        return data_connector.build_batch_spec(batch_definition=batch_definition)
