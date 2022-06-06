import abc
import enum
from abc import abstractmethod
from dataclasses import dataclass
from typing import Any, Optional

from great_expectations.core.data_context_key import StringKey


@dataclass
class DataContextVariables(abc.ABC):
    """
    Wrapper object around data context variables set in the `great_expectations.yml` config file.

    Child classes should instantiate their own stores to ensure that changes made to this object
    are persisted for future usage (i.e. filesystem I/O or HTTP request to a Cloud endpoint).

    Should maintain parity with the `DataContextConfig`.
    """

    config_version: Optional[float]
    datasources: Optional[dict]
    expectations_store_name: Optional[str]
    validations_store_name: Optional[str]
    evaluation_parameter_store_name: Optional[str]
    checkpoint_store_name: Optional[str]
    profiler_store_name: Optional[str]
    plugins_directory: Optional[str]
    validation_operators: Optional[dict]
    stores: Optional[dict]
    data_docs_sites: Optional[dict]
    notebooks: Optional[dict]
    config_variables_file_path: Optional[str]
    anonymous_usage_statistics: Optional[dict]
    store_backend_defaults: Optional[dict]
    concurrency: Optional[dict]
    progress_bars: Optional[dict]

    class DataContextVariablesSchema(enum.Enum):
        """
        Internal helper to ensure usage of `getattr/setattr` adheres to a strict schema.
        """

        CONFIG_VERSION = "config_version"
        DATASOURCES = "datasources"
        EXPECTATIONS_STORE_NAME = "expectations_store_name"
        VALIDATIONS_STORE_NAME = "validations_store_name"
        EVALUATION_PARAMETER_STORE_NAME = "evaluation_parameter_store_name"
        CHECKPOINT_STORE_NAME = "checkpoint_store_name"
        PROFILER_STORE_NAME = "profiler_store_name"
        PLUGINS_DIRECTORY = "plugins_directory"
        VALIDATION_OPERATORS = "validation_operators"
        STORES = "stores"
        DATA_DOCS_SITES = "data_docs_sites"
        NOTEBOOKS = "notebooks"
        CONFIG_VARIABLES_FILE_PATH = "config_variables_file_path"
        ANONYMIZED_USAGE_STATISTICS = "anonymous_usage_statistics"
        STORE_BACKEND_DEFAULTS = "store_backend_defaults"
        CONCURRENCY = "concurrency"
        PROGRESS_BARS = "progress_bars"

        @classmethod
        def has_value(cls, value: str) -> bool:
            """
            Checks whether or not a string is a value from the possible enum pairs.
            """
            return value in cls._value2member_map_

    def __post_init__(self) -> None:
        self._store = None

    @property
    def store(self) -> "DataContextVariablesStore":  # noqa: F821
        if self._store is None:
            self._store = self._init_store()
        return self._store

    @abstractmethod
    def _init_store(self) -> "DataContextVariablesStore":  # noqa: F821
        raise NotImplementedError

    @staticmethod
    def _get_key(attr: DataContextVariablesSchema) -> StringKey:
        key: StringKey = StringKey(key=attr.value)
        return key

    def _set(self, attr: DataContextVariablesSchema, value: Any) -> None:
        setattr(self, attr.value, value)
        key: StringKey = DataContextVariables._get_key(attr)
        self.store.set(key=key, value=value)

    def _get(self, attr: DataContextVariablesSchema) -> Any:
        val: Any = getattr(self, attr.value)
        return val

    def set_config_version(self, config_version: float) -> None:
        """
        Setter for `config_version`.
        """
        self._set(self.DataContextVariablesSchema.CONFIG_VERSION, config_version)

    def get_config_version(self) -> Optional[float]:
        """
        Getter for `config_version`.
        """
        return self._get(self.DataContextVariablesSchema.CONFIG_VERSION)


@dataclass
class EphemeralDataContextVariables(DataContextVariables):
    def _init_store(self) -> "DataContextVariablesStore":  # noqa: F821
        from great_expectations.data_context.store.data_context_variables_store import (
            DataContextVariablesStore,
        )

        store: DataContextVariablesStore = DataContextVariablesStore(
            store_name="ephemeral_data_context_variables_store",
            store_backend=None,  # Defaults to InMemoryStoreBackend
            runtime_environment=None,
        )
        return store


@dataclass
class FileDataContextVariables(DataContextVariables):
    def _init_store(self) -> "DataContextVariablesStore":  # noqa: F821
        from great_expectations.data_context.store.data_context_variables_store import (
            DataContextVariablesStore,
        )

        store_backend: dict = {"class_name": "InlineStoreBackend"}  # TBD
        store: DataContextVariablesStore = DataContextVariablesStore(
            store_name="file_data_context_variables_store",
            store_backend=store_backend,
            runtime_environment=None,
        )
        return store


@dataclass
class CloudDataContextVariables(DataContextVariables):
    ge_cloud_runtime_base_url: str
    ge_cloud_runtime_organization_id: str
    ge_cloud_runtime_access_token: str

    def _init_store(self) -> "DataContextVariablesStore":  # noqa: F821
        from great_expectations.data_context.store.data_context_variables_store import (
            DataContextVariablesStore,
        )

        store_backend: dict = {
            "class_name": "GeCloudStoreBackend",
            "ge_cloud_base_url": self.ge_cloud_runtime_base_url,
            "ge_cloud_resource_type": "variables",
            "ge_cloud_credentials": {
                "access_token": self.ge_cloud_runtime_access_token,
                "organization_id": self.ge_cloud_runtime_organization_id,
            },
            "suppress_store_backend_id": True,
        }
        store: DataContextVariablesStore = DataContextVariablesStore(
            store_name="cloud_data_context_variables_store",
            store_backend=store_backend,
            runtime_environment=None,
        )
        return store
