import enum
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Optional

from great_expectations.core.data_context_key import DataContextKey
from great_expectations.data_context.types.base import (
    AnonymizedUsageStatisticsConfig,
    ConcurrencyConfig,
    DataContextConfig,
    NotebookConfig,
    ProgressBarsConfig,
)
from great_expectations.data_context.types.resource_identifiers import (
    ConfigurationIdentifier,
    GeCloudIdentifier,
)
from great_expectations.data_context.util import substitute_config_variable


class DataContextVariableSchema(str, enum.Enum):
    CONFIG_VERSION = "config_version"
    DATASOURCES = "datasources"
    EXPECTATIONS_STORE_NAME = "expectations_store_name"
    VALIDATIONS_STORE_NAME = "validations_store_name"
    EVALUATION_PARAMETER_STORE_NAME = "evaluation_parameter_store_name"
    CHECKPOINT_STORE_NAME = "checkpoint_store_name"
    PROFILER_STORE_NAME = "profiler_store_name"
    PLUGINS_DIRECTORY = "plugins_directory"
    STORES = "stores"
    DATA_DOCS_SITES = "data_docs_sites"
    NOTEBOOKS = "notebooks"
    CONFIG_VARIABLES_FILE_PATH = "config_variables_file_path"
    ANONYMOUS_USAGE_STATISTICS = "anonymous_usage_statistics"
    CONCURRENCY = "concurrency"
    PROGRESS_BARS = "progress_bars"

    @classmethod
    def has_value(cls, value: str) -> bool:
        """
        Checks whether or not a string is a value from the possible enum pairs.
        """
        return value in cls._value2member_map_


@dataclass
class DataContextVariables(ABC):
    """
    Wrapper object around data context variables set in the `great_expectations.yml` config file.

    Child classes should instantiate their own stores to ensure that changes made to this object
    are persisted for future usage (i.e. filesystem I/O or HTTP request to a Cloud endpoint).

    Should maintain parity with the `DataContextConfig`.
    """

    config: DataContextConfig
    substitutions: Optional[dict] = None
    _store: Optional["DataContextVariablesStore"] = None  # noqa: F821

    def __post_init__(self) -> None:
        if self.substitutions is None:
            self.substitutions = {}

    @property
    def store(self) -> "DataContextVariablesStore":  # noqa: F821
        if self._store is None:
            self._store = self._init_store()
        return self._store

    @abstractmethod
    def _init_store(self) -> "DataContextVariablesStore":  # noqa: F821
        raise NotImplementedError

    def get_key(self) -> DataContextKey:
        key: ConfigurationIdentifier = ConfigurationIdentifier(
            configuration_key="data_context"
        )
        return key

    def _set(self, attr: DataContextVariableSchema, value: Any) -> None:
        key: str = attr.value
        self.config[key] = value

    def _get(self, attr: DataContextVariableSchema) -> Any:
        key: str = attr.value
        val: Any = self.config[key]
        substituted_val: Any = substitute_config_variable(val, self.substitutions)
        return substituted_val

    def save_config(self) -> None:
        key: ConfigurationIdentifier = self.get_key()
        self.store.set(key=key, value=self.config)

    def set_config_version(self, config_version: float) -> None:
        """
        Setter for `config_version`.
        """
        self._set(DataContextVariableSchema.CONFIG_VERSION, config_version)

    def get_config_version(self) -> Optional[float]:
        """
        Getter for `config_version`.
        """
        return self._get(DataContextVariableSchema.CONFIG_VERSION)

    def set_config_variables_file_path(self, config_variables_file_path: str) -> None:
        """
        Setter for `config_variables_file_path`.
        """
        self._set(
            DataContextVariableSchema.CONFIG_VARIABLES_FILE_PATH,
            config_variables_file_path,
        )

    def get_config_variables_file_path(self) -> Optional[str]:
        """
        Getter for `config_version`.
        """
        return self._get(DataContextVariableSchema.CONFIG_VARIABLES_FILE_PATH)

    def set_plugins_directory(self, plugins_directory: str) -> None:
        """
        Setter for `plugins_directory`.
        """
        self._set(DataContextVariableSchema.PLUGINS_DIRECTORY, plugins_directory)

    def get_plugins_directory(self) -> Optional[str]:
        """
        Getter for `plugins_directory`.
        """
        return self._get(DataContextVariableSchema.PLUGINS_DIRECTORY)

    def set_expectations_store_name(self, expectations_store_name: str) -> None:
        """
        Setter for `expectations_store_name`.
        """
        self._set(
            DataContextVariableSchema.EXPECTATIONS_STORE_NAME, expectations_store_name
        )

    def get_expectations_store_name(self) -> Optional[str]:
        """
        Getter for `expectations_store_name`.
        """
        return self._get(DataContextVariableSchema.EXPECTATIONS_STORE_NAME)

    def set_validations_store_name(self, validations_store_name: str) -> None:
        """
        Setter for `validations_store_name`.
        """
        self._set(
            DataContextVariableSchema.VALIDATIONS_STORE_NAME, validations_store_name
        )

    def get_validations_store_name(self) -> Optional[str]:
        """
        Getter for `validations_store_name`.
        """
        return self._get(DataContextVariableSchema.VALIDATIONS_STORE_NAME)

    def set_evaluation_parameter_store_name(
        self, evaluation_parameter_store_name: str
    ) -> None:
        """
        Setter for `evaluation_parameter_store_name`.
        """
        self._set(
            DataContextVariableSchema.EVALUATION_PARAMETER_STORE_NAME,
            evaluation_parameter_store_name,
        )

    def get_evaluation_parameter_store_name(self) -> Optional[str]:
        """
        Getter for `evaluation_parameter_store_name`.
        """
        return self._get(DataContextVariableSchema.EVALUATION_PARAMETER_STORE_NAME)

    def set_checkpoint_store_name(self, checkpoint_store_name: str) -> None:
        """
        Setter for `checkpoint_store_name`.
        """
        self._set(
            DataContextVariableSchema.CHECKPOINT_STORE_NAME,
            checkpoint_store_name,
        )

    def get_checkpoint_store_name(self) -> Optional[str]:
        """
        Getter for `checkpoint_store_name`.
        """
        return self._get(DataContextVariableSchema.CHECKPOINT_STORE_NAME)

    def set_profiler_store_name(self, profiler_store_name: str) -> None:
        """
        Setter for `profiler_store_name`.
        """
        self._set(
            DataContextVariableSchema.PROFILER_STORE_NAME,
            profiler_store_name,
        )

    def get_profiler_store_name(self) -> Optional[str]:
        """
        Getter for `profiler_store_name`.
        """
        return self._get(DataContextVariableSchema.PROFILER_STORE_NAME)

    def set_stores(self, stores: dict) -> None:
        """
        Setter for `stores`.
        """
        self._set(DataContextVariableSchema.STORES, stores)

    def get_stores(self) -> Optional[dict]:
        """
        Getter for `stores`.
        """
        return self._get(DataContextVariableSchema.STORES)

    def set_data_docs_sites(self, data_docs_sites: dict) -> None:
        """
        Setter for `data_docs_sites`.
        """
        self._set(DataContextVariableSchema.DATA_DOCS_SITES, data_docs_sites)

    def get_data_docs_sites(self) -> Optional[dict]:
        """
        Getter for `data_docs_sites`.
        """
        return self._get(DataContextVariableSchema.DATA_DOCS_SITES)

    def set_anonymous_usage_statistics(
        self, anonymous_usage_statistics: AnonymizedUsageStatisticsConfig
    ) -> None:
        """
        Setter for `anonymous_usage_statistics`.
        """
        self._set(
            DataContextVariableSchema.ANONYMOUS_USAGE_STATISTICS,
            anonymous_usage_statistics,
        )

    def get_anonymous_usage_statistics(
        self,
    ) -> Optional[AnonymizedUsageStatisticsConfig]:
        """
        Getter for `anonymous_usage_statistics`.
        """
        return self._get(DataContextVariableSchema.ANONYMOUS_USAGE_STATISTICS)

    def set_notebooks(self, notebooks: NotebookConfig) -> None:
        """
        Setter for `notebooks`.
        """
        self._set(
            DataContextVariableSchema.NOTEBOOKS,
            notebooks,
        )

    def get_notebooks(self) -> Optional[NotebookConfig]:
        """
        Getter for `notebooks`.
        """
        return self._get(DataContextVariableSchema.NOTEBOOKS)

    def set_concurrency(self, concurrency: ConcurrencyConfig) -> None:
        """
        Setter for `concurrency`.
        """
        self._set(
            DataContextVariableSchema.CONCURRENCY,
            concurrency,
        )

    def get_concurrency(self) -> Optional[ConcurrencyConfig]:
        """
        Getter for `concurrency`.
        """
        return self._get(DataContextVariableSchema.CONCURRENCY)

    def set_progress_bars(self, progress_bars: ProgressBarsConfig) -> None:
        """
        Setter for `progress_bars`.
        """
        self._set(
            DataContextVariableSchema.PROGRESS_BARS,
            progress_bars,
        )

    def get_progress_bars(self) -> Optional[ProgressBarsConfig]:
        """
        Getter for `progress_bars`.
        """
        return self._get(DataContextVariableSchema.PROGRESS_BARS)


@dataclass
class EphemeralDataContextVariables(DataContextVariables):
    def _init_store(self) -> "DataContextVariablesStore":  # noqa: F821
        from great_expectations.data_context.store.data_context_variables_store import (
            DataContextStore,
        )

        store: DataContextStore = DataContextStore(
            store_name="ephemeral_data_context_variables_store",
            store_backend=None,  # Defaults to InMemoryStoreBackend
            runtime_environment=None,
        )
        return store


@dataclass
class FileDataContextVariables(DataContextVariables):
    data_context: Optional["DataContext"] = None  # noqa: F821

    def __post_init__(self) -> None:
        # Chetan - 20220607 - Although the above argument is not truly optional, we are
        # required to use default values because the parent class defines arguments with default values
        # ("Fields without default values cannot appear after fields with default values").
        #
        # Python 3.10 resolves this issue around dataclass inheritance using `kw_only=True` (https://docs.python.org/3/library/dataclasses.html)
        # This should be modified once our lowest supported version is 3.10.

        if self.data_context is None:
            raise ValueError(
                f"A reference to a data context is required for {self.__class__.__name__}"
            )

    def _init_store(self) -> "DataContextVariablesStore":  # noqa: F821
        from great_expectations.data_context.store.data_context_variables_store import (
            DataContextStore,
        )

        store_backend: dict = {
            "class_name": "InlineStoreBackend",
            "data_context": self.data_context,
        }
        store: DataContextStore = DataContextStore(
            store_name="file_data_context_variables_store",
            store_backend=store_backend,
            runtime_environment=None,
        )
        return store


@dataclass
class CloudDataContextVariables(DataContextVariables):
    ge_cloud_base_url: Optional[str] = None
    ge_cloud_organization_id: Optional[str] = None
    ge_cloud_access_token: Optional[str] = None

    def __post_init__(self) -> None:
        # Chetan - 20220607 - Although the above arguments are not truly optional, we are
        # required to use default values because the parent class defines arguments with default values
        # ("Fields without default values cannot appear after fields with default values").
        #
        # Python 3.10 resolves this issue around dataclass inheritance using `kw_only=True` (https://docs.python.org/3/library/dataclasses.html)
        # This should be modified once our lowest supported version is 3.10.

        if any(
            attr is None
            for attr in (
                self.ge_cloud_base_url,
                self.ge_cloud_organization_id,
                self.ge_cloud_access_token,
            )
        ):
            raise ValueError(
                f"All of the following attributes are required for{ self.__class__.__name__}:\n  self.ge_cloud_base_url\n  self.ge_cloud_organization_id\n  self.ge_cloud_access_token"
            )

    def _init_store(self) -> "DataContextVariablesStore":  # noqa: F821
        from great_expectations.data_context.store.data_context_variables_store import (
            DataContextStore,
        )

        store_backend: dict = {
            "class_name": "GeCloudStoreBackend",
            "ge_cloud_base_url": self.ge_cloud_base_url,
            "ge_cloud_resource_type": "data_context",
            "ge_cloud_credentials": {
                "access_token": self.ge_cloud_access_token,
                "organization_id": self.ge_cloud_organization_id,
            },
            "suppress_store_backend_id": True,
        }
        store: DataContextStore = DataContextStore(
            store_name="cloud_data_context_variables_store",
            store_backend=store_backend,
            runtime_environment=None,
        )
        return store

    def get_key(self) -> GeCloudIdentifier:
        key: GeCloudIdentifier = GeCloudIdentifier(resource_type="data_context")
        return key
