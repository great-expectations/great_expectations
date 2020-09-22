# -*- coding: utf-8 -*-

import copy
import logging
from typing import Union

from great_expectations.data_context.types.base import (
    DataConnectorConfig,
    dataConnectorConfigSchema
)
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.data_context.util import (
    instantiate_class_from_config,
    load_class,
    verify_dynamic_loading_support,
)
from great_expectations.exceptions import ClassInstantiationError
from great_expectations.types import ClassConfig
from great_expectations.validator.validator import Validator

logger = logging.getLogger(__name__)


class ExecutionEnvironment(object):
    """
An ExecutionEnvironment is the glue between an ExecutionEngine and a DataConnector.
    """

    recognized_batch_parameters = {"limit"}

    # TODO: <Alex>Is it correct to expect execution_engine and data_connectors as arguments?  Or should they be obtained from **kwargs (config)?</Alex>
    def __init__(
        self,
        name,
        execution_engine=None,
        data_connectors=None,
        data_context=None,
        **kwargs
    ):
        """
        Build a new ExecutionEnvironment.

        Args:
            name: the name for the datasource
            data_context: data context to which to connect
            execution_engine (ClassConfig): the type of DataAsset to produce # TODO: <Alex>Is this description correct?</Alex>
            data_connectors: DataConnectors to add to the datasource
        """
        self._name = name
        self._data_context = data_context
        self._execution_engine = instantiate_class_from_config(
            config=execution_engine, runtime_environment={},
        )
        self._execution_engine._data_context = (
            data_context  # do this here because data_context not in DataAsset # TODO: <Alex>What does this comment mean?</Alex>
        )
        # __init__ argspec, so not added when provided in runtime_environment above # TODO: <Alex>What does this comment mean?</Alex>
        self._execution_environment_config = kwargs

        self._execution_environment_config["execution_engine"] = execution_engine
        # TODO: <Alex>Check maybe next 2 lines not needed, because config requires data_connectors to be initialized to {} if none.</Alex>
        if data_connectors is None:
            data_connectors = {}
        self._execution_environment_config["data_connectors"] = data_connectors

        self._data_connectors = {}
        # TODO: <Alex>Delete next two lines if works without them.</Alex>
        # if data_connectors is not None:
        #     self._execution_environment_config["data_connectors"] = data_connectors
        self._build_data_connectors()

    def get_batch(
        self,
        batch_definition: dict,
        in_memory_dataset: any = None,  # TODO: should this be any to accommodate the different engines?
    ):
        # TODO: <Alex>To delete datasources or keep for backward compatibility?</Alex>
        self.execution_engine.load_batch(
            batch_definition=batch_definition, in_memory_dataset=in_memory_dataset
        )
        return self.execution_engine.loaded_batch

    def get_validator(
        self,
        batch_definition: dict,
        expectation_suite_name: Union[str, ExpectationSuite],
        in_memory_dataset: any = None,  # TODO: should this be any to accommodate the different engines?
    ):
        self.execution_engine.load_batch(
            batch_definition=batch_definition, in_memory_dataset=in_memory_dataset
        )
        return Validator(
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

    # TODO: <Alex>What is this for, and how is this used?</Alex>
    @classmethod
    def build_configuration(
        cls,
        class_name,
        module_name="great_expectations.datasource",  # TODO: should this live in a new directory?
        execution_engine=None,
        data_connectors=None,
        **kwargs
    ):
        """
        Build a full configuration object for an execution environment, potentially including DataConnectors with
        defaults.

        Args:
            class_name: The name of the class for which to build the config
            module_name: The name of the module in which the datasource class is located
            execution_engine: A ClassConfig dictionary
            data_connectors: DataConnector configuration dictionary
            **kwargs: Additional kwargs to be part of the datasource constructor's initialization

        Returns:
            A complete ExecutionEnvironment configuration.

        """
        verify_dynamic_loading_support(module_name=module_name)
        class_ = load_class(class_name=class_name, module_name=module_name)
        configuration = class_.build_configuration(
            execution_engine=execution_engine, data_connectors=data_connectors, **kwargs
        )
        return configuration

    @property
    def execution_engine(self):
        return self._execution_engine

    @property
    def name(self):
        """
        Property for datasource name
        """
        return self._name

    @property
    def config(self):
        return copy.deepcopy(self._execution_environment_config)

    @property
    def data_context(self):
        """
        Property for attached DataContext
        """
        return self._data_context

    def _build_data_connectors(self):
        """
        Build DataConnector objects from the ExecutionEnvironment configuration.

        Returns:
            None
        """
        for data_connector in self._execution_environment_config["data_connectors"].keys():
            self.get_data_connector(name=data_connector)

    # TODO: <Alex>Is this needed?  If so, how is it used, and why is it commented out?</Alex>
    # def add_data_connector(self, name, class_name, **kwargs):
    #     """Add a DataConnector to the ExecutionEnvironment.
    #
    #     Args:
    #         name (str): the name of the new DataConnector to add
    #         class_name: class of the DataConnector to add
    #         kwargs: additional keyword arguments will be passed directly to the new DataConnector's constructor
    #
    #     Returns:
    #          DataConnector (DataConnector)
    #     """
    #     kwargs["class_name"] = class_name
    #     data_connector = self._build_data_connector(**kwargs)
    #     if "data_connectors" not in self._execution_environment_config:
    #         self._execution_environment_config["data_connectors"] = dict()
    #     self._execution_environment_config["data_connectors"][name] = kwargs
    #
    #     return data_connector

    def get_data_connector(self, name):
        """Get the (named) DataConnector from an ExecutionEnvironment)

        Args:
            name (str): name of DataConnector #  TODO: Should there be a default value? TODO: <Alex>No</Alex>

        Returns:
            DataConnector (DataConnector)
        """
        if name in self._data_connectors:
            return self._data_connectors[name]
        elif (
                "data_connectors" in self._execution_environment_config
                and name in self._execution_environment_config["data_connectors"]
        ):
            data_connector_config = copy.deepcopy(
                self._execution_environment_config["data_connectors"][name]
            )
        else:
            raise ValueError(
                "Unable to load data connector %s -- no configuration found or invalid configuration."
                % name
            )
        data_connector_config = dataConnectorConfigSchema.load(
            data_connector_config
        )
        # TODO: <Alex>Delete next line if everything works</Alex>
        # data_connector_config.update({"name": name})
        data_connector = self._build_data_connector_from_config(
            name=name, config=data_connector_config
        )
        self._data_connectors[name] = data_connector
        return data_connector

    # TODO: <Alex>This is a good place to check that all defaults from base.py / Config Schemas are set properly.</Alex>
    def _build_data_connector_from_config(self, name, config):
        """Build a DataConnector using the provided configuration and return the newly-built DataConnector."""
        # We convert from the type back to a dictionary for purposes of instantiation
        if isinstance(config, DataConnectorConfig):
            config = dataConnectorConfigSchema.dump(config)
        config.update({"name": name})
        data_connector = instantiate_class_from_config(
            config=config,
            runtime_environment={"execution_environment": self},
            config_defaults={
                "module_name": "great_expectations.execution_environment.data_connector.data_connector"
            },
        )
        if not data_connector:
            raise ClassInstantiationError(
                module_name="great_expectations.execution_environment.data_connector.data_connector",
                package_name=None,
                class_name=config["class_name"],
            )
        return data_connector

    def list_data_connectors(self):
        """List currently-configured DataConnector for this ExecutionEnvironment.

        Returns:
            List(dict): each dictionary includes "name" and "type" keys
        """
        data_connectors = []

        if "data_connectors" in self._execution_environment_config:
            for key, value in self._execution_environment_config[
                "data_connectors"
            ].items():
                data_connectors.append({"name": key, "class_name": value["class_name"]})

        return data_connectors

    # TODO: <Alex>This needs to be reviewed...</Alex>
    def get_available_data_asset_names(self, data_connector_names=None):
        """
        Returns a dictionary of data_asset_names that the specified data
        connector can provide. Note that some data_connectors may not be
        capable of describing specific named data assets, and some (such as
        filesystem glob data_connectors) require the user to configure
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
        available_data_asset_names = {}
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

    def build_batch_spec(self, data_connector_name, batch_definition):
        data_connector_obj = self.get_data_connector(name=data_connector_name)
        return data_connector_obj.build_batch_spec(batch_definition=batch_definition)
