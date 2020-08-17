# -*- coding: utf-8 -*-

import copy
import logging
import warnings

from ruamel.yaml import YAML

from great_expectations.data_context.util import (
    instantiate_class_from_config,
    load_class,
    verify_dynamic_loading_support,
)
from great_expectations.exceptions import ClassInstantiationError
from great_expectations.types import ClassConfig

logger = logging.getLogger(__name__)
yaml = YAML()
yaml.default_flow_style = False


class ExecutionEnvironment(object):
    """
An ExecutionEnvironment is the glue between an ExecutionEngine and a DataConnector.
    """

    recognized_batch_parameters = {"limit"}

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

    @classmethod
    def build_configuration(
        cls,
        class_name,
        module_name="great_expectations.datasource",  # TODO: should this live in a new directory?
        data_asset_type=None,
        data_connectors=None,
        **kwargs
    ):
        """
        Build a full configuration object for an execution environment, potentially including DataConnectors with
        defaults.

        Args:
            class_name: The name of the class for which to build the config
            module_name: The name of the module in which the datasource class is located
            data_asset_type: A ClassConfig dictionary
            data_connectors: DataConnector configuration dictionary
            **kwargs: Additional kwargs to be part of the datasource constructor's initialization

        Returns:
            A complete ExecutionEnvironment configuration.

        """
        verify_dynamic_loading_support(module_name=module_name)
        class_ = load_class(class_name=class_name, module_name=module_name)
        configuration = class_.build_configuration(
            data_asset_type=data_asset_type, data_connectors=data_connectors, **kwargs
        )
        return configuration

    def __init__(
        self,
        name,
        data_context=None,
        data_asset_type=None,
        data_connectors=None,
        **kwargs
    ):
        """
        Build a new ExecutionEnvironment.

        Args:
            name: the name for the datasource
            data_context: data context to which to connect
            data_asset_type (ClassConfig): the type of DataAsset to produce
            batch_kwargs_generators: BatchKwargGenerators to add to the datasource
        """
        self._data_context = data_context
        self._name = name
        if isinstance(data_asset_type, str):
            warnings.warn(
                "String-only configuration for data_asset_type is deprecated. Use module_name and class_name instead.",
                DeprecationWarning,
            )
        self._data_asset_type = data_asset_type
        self._execution_environment_config = kwargs
        self._data_connectors = {}

        self._execution_environment_config["data_asset_type"] = data_asset_type
        if data_connectors is not None:
            self._execution_environment_config["data_connectors"] = data_connectors

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
        try:
            for data_connector in self._execution_environment_config[
                "data_connectors"
            ].keys():
                self.get_data_connector(data_connector)
        except KeyError:
            pass

    def add_batch_kwargs_generator(self, name, class_name, **kwargs):
        """Add a DataConnector to the ExecutionEnvironment.

        Args:
            name (str): the name of the new BatchKwargGenerator to add
            class_name: class of the BatchKwargGenerator to add
            kwargs: additional keyword arguments will be passed directly to the new BatchKwargGenerator's constructor

        Returns:
             DataConnector (DataConnector)
        """
        kwargs["class_name"] = class_name
        data_connector = self._build_data_connector(**kwargs)
        if "data_connectors" not in self._execution_environment_config:
            self._execution_environment_config["data_connectors"] = dict()
        self._execution_environment_config["data_connectors"][name] = kwargs

        return data_connector

    def _build_data_connector(self, **kwargs):
        """Build a DataConnector using the provided configuration and return the newly-built DataConnector."""
        data_connector = instantiate_class_from_config(
            config=kwargs,
            runtime_environment={"execution_environment": self},
            config_defaults={
                "module_name": "great_expectations.datasource.data_connector"  # TODO: confirm directory location
            },
        )
        if not data_connector:
            raise ClassInstantiationError(
                module_name="great_expectations.datasource.data_connector",
                package_name=None,
                class_name=kwargs["class_name"],
            )

        return data_connector

    def get_data_connector(self, name):
        """Get the (named) DataConnector from an ExecutionEnvironment)

        Args:
            name (str): name of DataConnector #  TODO: Should there be a default value?

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
        data_connector = self._build_data_connector(**data_connector_config)
        self._data_connectors[name] = data_connector
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
