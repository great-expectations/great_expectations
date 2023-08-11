import logging
from typing import TYPE_CHECKING, Optional, Union

from ruamel.yaml import YAML

import great_expectations.exceptions as gx_exceptions
from great_expectations.data_context.cloud_constants import GXCloudRESTResource
from great_expectations.data_context.store.store import Store
from great_expectations.data_context.store.tuple_store_backend import TupleStoreBackend
from great_expectations.data_context.types.base import BaseYamlConfig
from great_expectations.data_context.types.resource_identifiers import (
    ConfigurationIdentifier,
    GXCloudIdentifier,
)
from great_expectations.data_context.util import load_class
from great_expectations.util import (
    filter_properties_dict,
    verify_dynamic_loading_support,
)

if TYPE_CHECKING:
    from ruamel.yaml.comments import CommentedMap

yaml = YAML()

yaml.indent(mapping=2, sequence=4, offset=2)
yaml.default_flow_style = False

logger = logging.getLogger(__name__)


class ConfigurationStore(Store):

    """
    Configuration Store provides a way to store any Marshmallow Schema compatible Configuration (using the YAML format).
    """

    _key_class = ConfigurationIdentifier

    _configuration_class = BaseYamlConfig

    def __init__(
        self,
        store_name: str,
        store_backend: Optional[dict] = None,
        overwrite_existing: bool = False,
        runtime_environment: Optional[dict] = None,
    ) -> None:
        if not issubclass(self._configuration_class, BaseYamlConfig):
            raise gx_exceptions.DataContextError(
                "Invalid configuration: A configuration_class needs to inherit from the BaseYamlConfig class."
            )

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
            store_backend=store_backend,
            runtime_environment=runtime_environment,
            store_name=store_name,
        )

        # Gather the call arguments of the present function (include the "module_name" and add the "class_name"), filter
        # out the Falsy values, and set the instance "_config" variable equal to the resulting dictionary.
        self._config = {
            "store_name": store_name,
            "store_backend": store_backend,
            "overwrite_existing": overwrite_existing,
            "runtime_environment": runtime_environment,
            "module_name": self.__class__.__module__,
            "class_name": self.__class__.__name__,
        }
        filter_properties_dict(properties=self._config, clean_falsy=True, inplace=True)

        self._overwrite_existing = overwrite_existing

    def remove_key(self, key):
        return self.store_backend.remove_key(key)

    def serialize(self, value):
        if self.cloud_mode:
            # GXCloudStoreBackend expects a json str
            config_schema = value.get_schema_class()()
            return config_schema.dump(value)
        return value.to_yaml_str()

    def deserialize(self, value):
        config = value
        if isinstance(value, str):
            config: CommentedMap = yaml.load(value)
        try:
            return self._configuration_class.from_commented_map(commented_map=config)
        except gx_exceptions.InvalidBaseYamlConfigError:
            # Just to be explicit about what we intended to catch
            raise

    @property
    def overwrite_existing(self) -> bool:
        return self._overwrite_existing

    @overwrite_existing.setter
    def overwrite_existing(self, overwrite_existing: bool) -> None:
        self._overwrite_existing = overwrite_existing

    @property
    def config(self) -> dict:
        return self._config

    def self_check(self, pretty_print: bool = True) -> dict:  # type: ignore[override]
        # Provide visibility into parameters that ConfigurationStore was instantiated with.
        report_object: dict = {"config": self.config}

        if pretty_print:
            print("Checking for existing keys...")

        report_object["keys"] = sorted(
            key.configuration_key for key in self.list_keys()  # type: ignore[attr-defined]
        )

        report_object["len_keys"] = len(report_object["keys"])
        len_keys: int = report_object["len_keys"]

        if pretty_print:
            print(f"\t{len_keys} keys found")
            if report_object["len_keys"] > 0:
                for key in report_object["keys"][:10]:
                    print(f"		{str(key)}")
            if len_keys > 10:  # noqa: PLR2004
                print("\t\t...")
            print()

        self.serialization_self_check(pretty_print=pretty_print)

        return report_object

    def serialization_self_check(self, pretty_print: bool) -> None:
        raise NotImplementedError

    def _determine_key(
        self, name: Optional[str] = None, id: Optional[str] = None
    ) -> Union[GXCloudIdentifier, ConfigurationIdentifier]:
        assert bool(name) ^ bool(id), "Must provide either name or id."

        key: Union[GXCloudIdentifier, ConfigurationIdentifier]
        if id or self.ge_cloud_mode:
            key = GXCloudIdentifier(
                resource_type=GXCloudRESTResource.CHECKPOINT,
                id=id,
                resource_name=name,
            )
        else:
            key = ConfigurationIdentifier(configuration_key=name)  # type: ignore[arg-type]

        return key
