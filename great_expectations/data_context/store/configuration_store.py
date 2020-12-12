import copy
import logging
from typing import Union

from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap

import great_expectations.exceptions as ge_exceptions
from great_expectations.data_context.store.store import Store
from great_expectations.data_context.store.tuple_store_backend import TupleStoreBackend
from great_expectations.data_context.types.base import BaseConfig
from great_expectations.data_context.types.resource_identifiers import (
    ConfigurationIdentifier,
)
from great_expectations.data_context.util import load_class
from great_expectations.util import (
    filter_properties_dict,
    get_currently_executing_function_call_arguments,
    verify_dynamic_loading_support,
)

yaml = YAML()

yaml.indent(mapping=2, sequence=4, offset=2)
yaml.default_flow_style = False

logger = logging.getLogger(__name__)


class ConfigurationStore(Store):
    """
    A Configuration Store provides a way to store GreatExpectations Configuration accessible to a Data Context.
    """

    _key_class = ConfigurationIdentifier

    def __init__(
        self,
        configuration_class: BaseConfig,
        store_name: str,
        store_backend: dict = None,
        overwrite_existing: bool = False,
        runtime_environment: dict = None,
    ):
        self._configuration_class = configuration_class

        self._store_name = store_name

        if store_backend is not None:
            store_backend_module_name = store_backend.get(
                "module_name", "great_expectations.data_context.store"
            )
            store_backend_class_name = store_backend.get(
                "class_name", "InMemoryStoreBackend"
            )
            verify_dynamic_loading_support(module_name=store_backend_module_name)
            store_backend_class = load_class(
                store_backend_class_name, store_backend_module_name
            )

            # Store Backend Class was loaded successfully; verify that it is of a correct subclass.
            if issubclass(store_backend_class, TupleStoreBackend):
                # Provide defaults for this common case
                store_backend["filepath_suffix"] = store_backend.get(
                    "filepath_suffix", ".yml"
                )

        super().__init__(
            store_backend=store_backend, runtime_environment=runtime_environment
        )

        # Gather the call arguments of the present function (include the "module_name" and add the "class_name"), filter
        # out the Falsy values, and set the instance "_config" variable equal to the resulting dictionary.
        self._config = get_currently_executing_function_call_arguments(
            include_module_name=True, **{"class_name": self.__class__.__name__,}
        )
        filter_properties_dict(properties=self._config, inplace=True)

        self._overwrite_existing = overwrite_existing

    def load_configuration(self) -> BaseConfig:
        logger.debug("Starting ConfigurationStore.load_configuration")

        try:
            key: ConfigurationIdentifier = ConfigurationIdentifier(
                configuration_name=self.store_name
            )
            config_yaml: str = self.get(key)
            config_commented_map_from_yaml: CommentedMap = yaml.load(config_yaml)
            try:
                return self.configuration_class.from_commented_map(
                    config_commented_map_from_yaml
                )
            except ge_exceptions.InvalidBaseConfigError:
                # Just to be explicit about what we intended to catch
                raise
        except ge_exceptions.InvalidKeyError:
            raise ge_exceptions.ConfigNotFoundError()

    def save_configuration(self, configuration: BaseConfig) -> None:
        logger.debug("Starting ConfigurationStore.save_configuration")

        # noinspection PyUnusedLocal
        do_store: bool = False
        if self.overwrite_existing:
            do_store = True
        else:
            if self._retrieve_configuration() is None:
                do_store = True
            else:
                raise ge_exceptions.InvalidBaseConfigError(
                    f"""Configuration named "{self.store_name}" already exists.
Set the property "overwrite_existing" to True in order to overwrite the previously saved configuration.
                    """
                )

        if do_store:
            self._store_configuration(configuration=configuration)

    def _retrieve_configuration(self) -> Union[BaseConfig, None]:
        configuration: Union[BaseConfig, None]
        try:
            configuration = self.load_configuration()
        except ge_exceptions.ConfigNotFoundError:
            configuration = None
        return configuration

    def _store_configuration(self, configuration: BaseConfig,) -> None:
        config: BaseConfig = copy.deepcopy(configuration)
        config_yaml: str = config.to_yaml_str()
        key: ConfigurationIdentifier = ConfigurationIdentifier(
            configuration_name=self.store_name
        )
        self.set(key, config_yaml)

    def delete_configuration(self):
        key: ConfigurationIdentifier = ConfigurationIdentifier(
            configuration_name=self.store_name
        )
        self.remove_key(key)

    def remove_key(self, key):
        return self.store_backend.remove_key(key)

    @property
    def configuration_class(self) -> BaseConfig:
        return self._configuration_class

    @property
    def overwrite_existing(self) -> bool:
        return self._overwrite_existing

    @overwrite_existing.setter
    def overwrite_existing(self, overwrite_existing: bool):
        self._overwrite_existing = overwrite_existing

    @property
    def store_name(self):
        return self._store_name

    @property
    def config(self) -> dict:
        return self._config
