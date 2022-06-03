import enum
from abc import abstractmethod
from dataclasses import dataclass
from typing import Any, Optional

from great_expectations.core.data_context_key import StringKey
from great_expectations.data_context.store.variables_store import VariablesStore


class VariablesSchema(enum.Enum):
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


@dataclass
class DataContextVariables:
    config_version: Optional[float] = None
    datasources: Optional[dict] = None
    expectations_store_name: Optional[str] = None
    validations_store_name: Optional[str] = None
    evaluation_parameter_store_name: Optional[str] = None
    checkpoint_store_name: Optional[str] = None
    profiler_store_name: Optional[str] = None
    plugins_directory: Optional[str] = None
    validation_operators: Optional[dict] = None
    stores: Optional[dict] = None
    data_docs_sites: Optional[dict] = None
    notebooks: Optional[dict] = None
    config_variables_file_path: Optional[str] = None
    anonymous_usage_statistics: Optional[dict] = None
    store_backend_defaults: Optional[dict] = None
    concurrency: Optional[dict] = None
    progress_bars: Optional[dict] = None

    def __post_init__(self) -> None:
        self._store = None

    @property
    def store(self) -> VariablesStore:
        if self._store is None:
            self._store = self._init_store()
        return self._store

    @abstractmethod
    def _init_store(self) -> VariablesStore:
        raise NotImplementedError

    @staticmethod
    def _get_key(attr: VariablesSchema) -> StringKey:
        key: StringKey = StringKey(key=attr.value)
        return key

    def _set(self, attr: VariablesSchema, value: Any) -> None:
        setattr(self, attr.value, value)
        key: StringKey = DataContextVariables._get_key(attr)
        self.store.set(key=key, value=value)

    def _get(self, attr: VariablesSchema) -> Any:
        key: StringKey = DataContextVariables._get_key(attr)
        val: Any = self.store.get(key=key)
        return val

    def set_config_version(self, config_version: float) -> None:
        self._set(VariablesSchema.CONFIG_VERSION, config_version)

    def get_config_version(self) -> Optional[float]:
        return self._get(VariablesSchema.CONFIG_VERSION)


@dataclass
class EphemeralDataContextVariables(DataContextVariables):
    def _init_store(self) -> VariablesStore:
        store: VariablesStore = VariablesStore(
            store_name="ephemeral_data_context_variables_store",
            store_backend=None,  # Defaults to InMemoryStoreBackend
            runtime_environment=None,
        )
        return store


@dataclass
class FileDataContextVariables(DataContextVariables):
    def _init_store(self) -> VariablesStore:
        store_backend: dict = {"class_name": "InlineStoreBackend"}  # TBD
        store: VariablesStore = VariablesStore(
            store_name="file_data_context_variables_store",
            store_backend=store_backend,
            runtime_environment=None,
        )
        return store


@dataclass
class CloudDataContextVariables(DataContextVariables):
    def __post_init__(
        self,
        ge_cloud_runtime_base_url: str,
        ge_cloud_runtime_organization_id: str,
        ge_cloud_runtime_access_token: str,
    ) -> None:
        self._ge_cloud_runtime_base_url = ge_cloud_runtime_base_url
        self._ge_cloud_runtime_organization_id = ge_cloud_runtime_organization_id
        self._ge_cloud_runtime_access_token = ge_cloud_runtime_access_token

    def _init_store(self) -> VariablesStore:
        store_backend: dict = {
            "class_name": "GeCloudStoreBackend",
            "ge_cloud_base_url": self._ge_cloud_runtime_base_url,
            "ge_cloud_resource_type": "variables",
            "ge_cloud_credentials": {
                "access_token": self._ge_cloud_runtime_access_token,
                "organization_id": self._ge_cloud_runtime_organization_id,
            },
            "suppress_store_backend_id": True,
        }
        store: VariablesStore = VariablesStore(
            store_name="cloud_data_context_variables_store",
            store_backend=store_backend,
            runtime_environment=None,
        )
        return store
