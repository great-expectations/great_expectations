import enum
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Optional

from great_expectations.core.data_context_key import DataContextVariableKey
from great_expectations.data_context.types.resource_identifiers import GeCloudIdentifier


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
    ANONYMIZED_USAGE_STATISTICS = "anonymous_usage_statistics"
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

    config_version: Optional[float] = None
    expectations_store_name: Optional[str] = None
    validations_store_name: Optional[str] = None
    evaluation_parameter_store_name: Optional[str] = None
    checkpoint_store_name: Optional[str] = None
    profiler_store_name: Optional[str] = None
    plugins_directory: Optional[str] = None
    stores: Optional[dict] = None
    data_docs_sites: Optional[dict] = None
    notebooks: Optional[dict] = None
    config_variables_file_path: Optional[str] = None
    anonymous_usage_statistics: Optional[dict] = None
    concurrency: Optional[dict] = None
    progress_bars: Optional[dict] = None
    _store: Optional["DataContextVariablesStore"] = None  # noqa: F821

    @property
    def store(self) -> "DataContextVariablesStore":  # noqa: F821
        if self._store is None:
            self._store = self._init_store()
        return self._store

    @abstractmethod
    def _init_store(self) -> "DataContextVariablesStore":  # noqa: F821
        raise NotImplementedError

    def _get_key(self, attr: DataContextVariableSchema) -> DataContextVariableKey:
        # Chetan - 20220607 - As it stands, DataContextVariables can only perform CRUD on entire objects.
        # The DataContextVariablesKey, when used with an appropriate Store and StoreBackend, gives the ability to modify
        # individual elements of nested config objects.

        key: DataContextVariableKey = DataContextVariableKey(resource_type=attr.value)
        return key

    def _set(self, attr: DataContextVariableSchema, value: Any) -> None:
        setattr(self, attr.value, value)
        key: DataContextVariableKey = self._get_key(attr)
        self.store.set(key=key, value=value)

    def _get(self, attr: DataContextVariableSchema) -> Any:
        val: Any = getattr(self, attr.value)
        return val

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
            DataContextVariablesStore,
        )

        store_backend: dict = {
            "class_name": "InlineStoreBackend",
            "data_context": self.data_context,
        }
        store: DataContextVariablesStore = DataContextVariablesStore(
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
            DataContextVariablesStore,
        )

        store_backend: dict = {
            "class_name": "GeCloudStoreBackend",
            "ge_cloud_base_url": self.ge_cloud_base_url,
            "ge_cloud_resource_type": "data_context_variable",
            "ge_cloud_credentials": {
                "access_token": self.ge_cloud_access_token,
                "organization_id": self.ge_cloud_organization_id,
            },
            "suppress_store_backend_id": True,
        }
        store: DataContextVariablesStore = DataContextVariablesStore(
            store_name="cloud_data_context_variables_store",
            store_backend=store_backend,
            runtime_environment=None,
        )
        return store

    def _get_key(
        self, attr: "DataContextVariablesSchema"  # noqa: F821
    ) -> GeCloudIdentifier:
        key: GeCloudIdentifier = GeCloudIdentifier(resource_type=attr.value)
        return key

    def _set(self, attr: DataContextVariableSchema, value: Any) -> None:
        setattr(self, attr.value, value)
        key: GeCloudIdentifier = self._get_key(attr)
        self.store.set(key=key, value=value, variable_type=attr.value)
