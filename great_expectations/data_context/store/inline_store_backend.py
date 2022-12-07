import logging
import pathlib
from typing import TYPE_CHECKING, Any, List, Optional, Tuple

from great_expectations.core.data_context_key import DataContextVariableKey
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context.data_context_variables import (
    DataContextVariableSchema,
)
from great_expectations.data_context.store.store_backend import StoreBackend
from great_expectations.data_context.types.base import DataContextConfig
from great_expectations.exceptions.exceptions import StoreBackendError
from great_expectations.util import filter_properties_dict

if TYPE_CHECKING:
    from great_expectations.data_context import DataContext

logger = logging.getLogger(__name__)

yaml = YAMLHandler()


class InlineStoreBackend(StoreBackend):
    """
    The InlineStoreBackend enables CRUD behavior with the fields noted in a user's project config (`great_expectations.yml`).

    The primary value of the InlineStoreBackend is the ability to modify either the entire config or very granular parts of it through the
    same interface. Whether it be replacing the entire config with a new one or tweaking an individual datasource nested within the config,
    a user of the backend is able to do so through the same key structure.

    For example:
        ("data_context", "")             -> Key used to get/set an entire config
        ("datasources", "my_datasource") -> Key used to get/set a specific datasource named "my_datasource"

    It performs these actions through a reference to a DataContext instance.
    Please note that is it only to be used with file-backed DataContexts (DataContext and FileDataContext).
    """

    def __init__(
        self,
        data_context: "DataContext",
        resource_type: DataContextVariableSchema,
        runtime_environment: Optional[dict] = None,
        fixed_length_key: bool = False,
        suppress_store_backend_id: bool = False,
        manually_initialize_store_backend_id: str = "",
        store_name: Optional[str] = None,
    ) -> None:
        super().__init__(
            fixed_length_key=fixed_length_key,
            suppress_store_backend_id=suppress_store_backend_id,
            manually_initialize_store_backend_id=manually_initialize_store_backend_id,
            store_name=store_name,
        )

        self._data_context = data_context
        self._resource_type = resource_type

        # Gather the call arguments of the present function (include the "module_name" and add the "class_name"), filter
        # out the Falsy values, and set the instance "_config" variable equal to the resulting dictionary.
        self._config = {
            "runtime_environment": runtime_environment,
            "fixed_length_key": fixed_length_key,
            "suppress_store_backend_id": suppress_store_backend_id,
            "manually_initialize_store_backend_id": manually_initialize_store_backend_id,
            "store_name": store_name,
            "module_name": self.__class__.__module__,
            "class_name": self.__class__.__name__,
        }
        filter_properties_dict(properties=self._config, clean_falsy=True, inplace=True)

    @property
    def config(self) -> dict:
        return self._config

    def _get(self, key: Tuple[str, ...]) -> Any:
        resource_name = InlineStoreBackend._determine_resource_name(key)
        project_config: DataContextConfig = self._data_context.config
        resource_type = self._resource_type

        if resource_type is DataContextVariableSchema.ALL_VARIABLES:
            return project_config

        variable_config: Any = project_config[resource_type]

        if resource_name is not None:
            return variable_config[resource_name]

        return variable_config

    def _set(self, key: Tuple[str, ...], value: Any, **kwargs: dict) -> None:
        resource_name = InlineStoreBackend._determine_resource_name(key)
        project_config: DataContextConfig = self._data_context.config
        resource_type = self._resource_type

        if resource_type is DataContextVariableSchema.ALL_VARIABLES:
            config_commented_map_from_yaml = yaml.load(value)
            value = DataContextConfig.from_commented_map(
                commented_map=config_commented_map_from_yaml
            )
            self._data_context.set_config(value)
        elif resource_name is not None:
            project_config[resource_type][resource_name] = value
        else:
            project_config[resource_type] = value

        self._save_changes()

    def _move(
        self, source_key: Tuple[str, ...], dest_key: Tuple[str, ...], **kwargs: dict
    ) -> None:
        raise StoreBackendError(
            "InlineStoreBackend does not support moving of keys; the DataContext's config variables schema is immutable"
        )

    def list_keys(self, prefix: Tuple[str, ...] = ()) -> List[str]:
        """
        See `StoreBackend.list_keys` for more information.

        Args:
            prefix: If supplied, allows for a more granular listing of nested values within the config.
                    Example: prefix=(datasources,) will list all datasource configs instead of top level keys.

        Returns:
            A list of string keys from the user's project config.
        """
        config_section: Optional[str] = None
        if prefix:
            config_section = prefix[0]

        keys: List[str]
        config_dict: dict = self._data_context.config.to_dict()
        if config_section is None:
            keys = list(key for key in config_dict.keys())
        else:
            config_values: dict = config_dict[config_section]
            if not isinstance(config_values, dict):
                raise StoreBackendError(
                    "Cannot list keys in a non-iterable section of a project config"
                )
            keys = list(key for key in config_values.keys())

        return keys

    def remove_key(self, key: Tuple[str, ...]) -> None:
        """
        See `StoreBackend.remove_key` for more information.
        """
        resource_name = InlineStoreBackend._determine_resource_name(key)
        resource_type = self._resource_type

        if resource_type is DataContextVariableSchema.ALL_VARIABLES:
            raise StoreBackendError(
                "InlineStoreBackend does not support the deletion of the overall DataContext project config"
            )
        if resource_name is None:
            raise StoreBackendError(
                "InlineStoreBackend does not support the deletion of top level keys; the DataContext's config variables schema is immutable"
            )
        elif not self._has_key(key):
            raise StoreBackendError(
                f"Could not find a value associated with key `{key}`"
            )

        del self._data_context.config[resource_type][resource_name]

        self._save_changes()

    def build_key(
        self,
        id: Optional[str] = None,
        name: Optional[str] = None,
    ) -> DataContextVariableKey:
        """Get the store backend specific implementation of the key. id included for super class compatibility."""
        return DataContextVariableKey(
            resource_name=name,
        )

    def _has_key(self, key: Tuple[str, ...]) -> bool:
        resource_name = InlineStoreBackend._determine_resource_name(key)
        resource_type = self._resource_type

        if resource_name is not None:
            res: dict = self._data_context.config.get(resource_type) or {}
            return resource_name in res

        return resource_type in self._data_context.config

    def _save_changes(self) -> None:
        context = self._data_context
        config_filepath = pathlib.Path(context.root_directory) / context.GX_YML  # type: ignore[arg-type]

        try:
            with open(config_filepath, "w") as outfile:
                context.config.to_yaml(outfile)
        # In environments where wrting to disk is not allowed, it is impossible to
        # save changes. As such, we log a warning but do not raise.
        except PermissionError as e:
            logger.warning(f"Could not save project config to disk: {e}")

    @staticmethod
    def _determine_resource_name(key: Tuple[str, ...]) -> Optional[str]:
        resource_name: Optional[str] = key[0] or None
        return resource_name
