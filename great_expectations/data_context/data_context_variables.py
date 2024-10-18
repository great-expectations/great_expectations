from __future__ import annotations

import contextlib
import enum
import logging
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, Generator, Optional

from great_expectations._docs_decorators import public_api
from great_expectations.compatibility.typing_extensions import override
from great_expectations.data_context.types.resource_identifiers import (
    ConfigurationIdentifier,
    GXCloudIdentifier,
)

if TYPE_CHECKING:
    from great_expectations.core.config_provider import (
        _ConfigurationProvider,
    )
    from great_expectations.core.data_context_key import DataContextKey
    from great_expectations.data_context.data_context.file_data_context import (
        FileDataContext,
    )
    from great_expectations.data_context.store import DataContextStore
    from great_expectations.data_context.types.base import (
        DataContextConfig,
        ProgressBarsConfig,
    )
    from great_expectations.datasource.fluent.interfaces import (
        Datasource as FluentDatasource,
    )

logger = logging.getLogger(__file__)


class DataContextVariableSchema(str, enum.Enum):
    ALL_VARIABLES = "data_context_variables"  # If retrieving/setting the entire config at once
    CONFIG_VERSION = "config_version"
    DATASOURCES = "datasources"
    FLUENT_DATASOURCES = "fluent_datasources"
    EXPECTATIONS_STORE_NAME = "expectations_store_name"
    VALIDATIONS_STORE_NAME = "validation_results_store_name"
    CHECKPOINT_STORE_NAME = "checkpoint_store_name"
    PLUGINS_DIRECTORY = "plugins_directory"
    STORES = "stores"
    DATA_DOCS_SITES = "data_docs_sites"
    CONFIG_VARIABLES_FILE_PATH = "config_variables_file_path"
    ANALYTICS_ENABLED = "analytics_enabled"
    DATA_CONTEXT_ID = "data_context_id"
    PROGRESS_BARS = "progress_bars"

    @classmethod
    def has_value(cls, value: str) -> bool:
        """
        Checks whether or not a string is a value from the possible enum pairs.
        """
        return value in cls._value2member_map_


@public_api
@dataclass
class DataContextVariables(ABC):
    """
    Wrapper object around data context variables set in the `great_expectations.yml` config file.

    Child classes should instantiate their own stores to ensure that changes made to this object
    are persisted for future usage (i.e. filesystem I/O or HTTP request to a Cloud endpoint).

    Should maintain parity with the `DataContextConfig`.

    Args:
        config:          A reference to the DataContextConfig to perform CRUD on.
        config_provider: Responsible for determining config values and substituting them in GET calls.
        _store:          An instance of a DataContextStore with the appropriate backend to persist config changes.
    """  # noqa: E501

    config: DataContextConfig
    config_provider: _ConfigurationProvider
    _store: Optional[DataContextStore] = None

    @override
    def __str__(self) -> str:
        return str(self.config)

    @override
    def __repr__(self) -> str:
        return repr(self.config)

    @property
    def store(self) -> DataContextStore:
        if self._store is None:
            self._store = self._init_store()
        return self._store

    @abstractmethod
    def _init_store(self) -> DataContextStore:
        raise NotImplementedError

    def get_key(self) -> DataContextKey:
        """
        Generates the appropriate Store key to retrieve/store configs.
        """
        key = ConfigurationIdentifier(configuration_key=DataContextVariableSchema.ALL_VARIABLES)
        return key

    def _set(self, attr: DataContextVariableSchema, value: Any) -> None:
        key: str = attr.value
        self.config[key] = value

    def _get(self, attr: DataContextVariableSchema) -> Any:
        key: str = attr.value
        val: Any = self.config[key]
        substituted_val: Any = self.config_provider.substitute_config(val)
        return substituted_val

    @public_api
    def save(self) -> Any:
        """
        Persist any changes made to variables utilizing the configured Store.
        """
        key: ConfigurationIdentifier = self.get_key()  # type: ignore[assignment]
        return self.store.set(key=key, value=self.config)

    @property
    def config_version(self) -> Optional[float]:
        return self._get(DataContextVariableSchema.CONFIG_VERSION)

    @config_version.setter
    def config_version(self, config_version: float) -> None:
        self._set(DataContextVariableSchema.CONFIG_VERSION, config_version)

    @property
    def config_variables_file_path(self) -> Optional[str]:
        return self._get(DataContextVariableSchema.CONFIG_VARIABLES_FILE_PATH)

    @config_variables_file_path.setter
    def config_variables_file_path(self, config_variables_file_path: str) -> None:
        self._set(
            DataContextVariableSchema.CONFIG_VARIABLES_FILE_PATH,
            config_variables_file_path,
        )

    @property
    def plugins_directory(self) -> Optional[str]:
        return self._get(DataContextVariableSchema.PLUGINS_DIRECTORY)

    @plugins_directory.setter
    def plugins_directory(self, plugins_directory: str) -> None:
        self._set(DataContextVariableSchema.PLUGINS_DIRECTORY, plugins_directory)

    @property
    def expectations_store_name(self) -> Optional[str]:
        return self._get(DataContextVariableSchema.EXPECTATIONS_STORE_NAME)

    @expectations_store_name.setter
    def expectations_store_name(self, expectations_store_name: str) -> None:
        self._set(DataContextVariableSchema.EXPECTATIONS_STORE_NAME, expectations_store_name)

    @property
    def validation_results_store_name(self) -> Optional[str]:
        return self._get(DataContextVariableSchema.VALIDATIONS_STORE_NAME)

    @validation_results_store_name.setter
    def validation_results_store_name(self, validation_results_store_name: str) -> None:
        self._set(DataContextVariableSchema.VALIDATIONS_STORE_NAME, validation_results_store_name)

    @property
    def checkpoint_store_name(self) -> Optional[str]:
        return self._get(DataContextVariableSchema.CHECKPOINT_STORE_NAME)

    @checkpoint_store_name.setter
    def checkpoint_store_name(self, checkpoint_store_name: str) -> None:
        self._set(
            DataContextVariableSchema.CHECKPOINT_STORE_NAME,
            checkpoint_store_name,
        )

    @property
    def stores(self) -> Optional[dict]:
        return self._get(DataContextVariableSchema.STORES)

    @stores.setter
    def stores(self, stores: dict) -> None:
        self._set(DataContextVariableSchema.STORES, stores)

    @property
    def data_docs_sites(self) -> Optional[dict]:
        return self._get(DataContextVariableSchema.DATA_DOCS_SITES)

    @data_docs_sites.setter
    def data_docs_sites(self, data_docs_sites: dict) -> None:
        self._set(DataContextVariableSchema.DATA_DOCS_SITES, data_docs_sites)

    @property
    def analytics_enabled(
        self,
    ) -> Optional[bool]:
        return self._get(DataContextVariableSchema.ANALYTICS_ENABLED)

    @analytics_enabled.setter
    def analytics_enabled(self, analytics_enabled: bool) -> None:
        self._set(
            DataContextVariableSchema.ANALYTICS_ENABLED,
            analytics_enabled,
        )

    @property
    def data_context_id(
        self,
    ) -> Optional[uuid.UUID]:
        return self._get(DataContextVariableSchema.DATA_CONTEXT_ID)

    @data_context_id.setter
    def data_context_id(self, data_context_id: uuid.UUID) -> None:
        self._set(
            DataContextVariableSchema.DATA_CONTEXT_ID,
            data_context_id,
        )

    @property
    def progress_bars(self) -> Optional[ProgressBarsConfig]:
        return self._get(DataContextVariableSchema.PROGRESS_BARS)

    @progress_bars.setter
    def progress_bars(self, progress_bars: ProgressBarsConfig) -> None:
        self._set(
            DataContextVariableSchema.PROGRESS_BARS,
            progress_bars,
        )


@dataclass(repr=False)
class EphemeralDataContextVariables(DataContextVariables):
    @override
    def _init_store(self) -> DataContextStore:
        from great_expectations.data_context.store.data_context_store import (
            DataContextStore,
        )

        store = DataContextStore(
            store_name="ephemeral_data_context_store",
            store_backend=None,  # Defaults to InMemoryStoreBackend
            runtime_environment=None,
        )
        return store


@dataclass(repr=False)
class FileDataContextVariables(DataContextVariables):
    data_context: FileDataContext = None  # type: ignore[assignment] # post_init ensures field always set

    def __post_init__(self) -> None:
        # Chetan - 20220607 - Although the above argument is not truly optional, we are
        # required to use default values because the parent class defines arguments with default values  # noqa: E501
        # ("Fields without default values cannot appear after fields with default values").
        #
        # Python 3.10 resolves this issue around dataclass inheritance using `kw_only=True` (https://docs.python.org/3/library/dataclasses.html)
        # This should be modified once our lowest supported version is 3.10.

        if self.data_context is None:
            raise ValueError(  # noqa: TRY003
                f"A reference to a data context is required for {self.__class__.__name__}"
            )

    @override
    def _init_store(self) -> DataContextStore:
        from great_expectations.data_context.store.data_context_store import (
            DataContextStore,
        )
        from great_expectations.data_context.store.inline_store_backend import (
            InlineStoreBackend,
        )

        # Chetan - 20230222 - `instantiate_class_from_config` used in the Store constructor
        # causes a runtime error with InlineStoreBackend due to attempting to deepcopy a DataContext.  # noqa: E501
        #
        # This should be resolved by moving the specific logic required from the context to a class
        # and injecting that object instead of the entire context.
        store_backend = InlineStoreBackend(
            data_context=self.data_context,
            resource_type=DataContextVariableSchema.ALL_VARIABLES,
        )
        store = DataContextStore(
            store_name="file_data_context_store",
        )
        store._store_backend = store_backend
        return store

    @override
    def save(self) -> Any:
        """
        Persist any changes made to variables utilizing the configured Store.
        """
        # overridden in order to prevent calling `instantiate_class_from_config` on fluent objects
        # parent class does not have access to the `data_context`
        with self._fluent_objects_stash():
            save_result = super().save()
        return save_result

    @contextlib.contextmanager
    def _fluent_objects_stash(
        self: FileDataContextVariables,
    ) -> Generator[None, None, None]:
        """
        Temporarily remove and stash fluent objects from the datacontext.
        Replace them once the with block ends.

        NOTE: This could be generalized into a stand-alone context manager function,
        but it would need to take in the data_context containing the fluent objects.
        """
        config_fluent_datasources_stash: Dict[str, FluentDatasource] = (
            self.data_context._synchronize_fluent_datasources()
        )
        try:
            if config_fluent_datasources_stash:
                logger.info(
                    f"Stashing `FluentDatasource` during {type(self).__name__}.save() - {len(config_fluent_datasources_stash)} stashed"  # noqa: E501
                )
                for fluent_datasource_name in config_fluent_datasources_stash:
                    self.data_context.data_sources.all().pop(fluent_datasource_name)
                # this would be `deep_copy'ed in `instantiate_class_from_config` too
                self.data_context.fluent_config.fluent_datasources = []
            yield
        except Exception:  # noqa: TRY302
            raise
        finally:
            if config_fluent_datasources_stash:
                logger.info(
                    f"Replacing {len(config_fluent_datasources_stash)} stashed `FluentDatasource`s"
                )
                self.data_context.data_sources.all().update(config_fluent_datasources_stash)
                self.data_context.fluent_config.fluent_datasources = list(
                    config_fluent_datasources_stash.values()
                )


@dataclass(repr=False)
class CloudDataContextVariables(DataContextVariables):
    ge_cloud_base_url: str = None  # type: ignore[assignment] # post_init ensures field always set
    ge_cloud_organization_id: str = None  # type: ignore[assignment] # post_init ensures field always set
    ge_cloud_access_token: str = None  # type: ignore[assignment] # post_init ensures field always set

    def __post_init__(self) -> None:
        # Chetan - 20220607 - Although the above arguments are not truly optional, we are
        # required to use default values because the parent class defines arguments with default values  # noqa: E501
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
            raise ValueError(  # noqa: TRY003
                f"All of the following attributes are required for{ self.__class__.__name__}:\n  self.ge_cloud_base_url\n  self.ge_cloud_organization_id\n  self.ge_cloud_access_token"  # noqa: E501
            )

    @override
    def _init_store(self) -> DataContextStore:
        from great_expectations.data_context.cloud_constants import GXCloudRESTResource
        from great_expectations.data_context.store.data_context_store import (
            DataContextStore,
        )
        from great_expectations.data_context.store.gx_cloud_store_backend import (
            GXCloudStoreBackend,
        )

        store_backend: dict = {
            "class_name": GXCloudStoreBackend.__name__,
            "ge_cloud_base_url": self.ge_cloud_base_url,
            "ge_cloud_resource_type": GXCloudRESTResource.DATA_CONTEXT_VARIABLES,
            "ge_cloud_credentials": {
                "access_token": self.ge_cloud_access_token,
                "organization_id": self.ge_cloud_organization_id,
            },
            "suppress_store_backend_id": True,
        }
        store = DataContextStore(
            store_name="cloud_data_context_store",
            store_backend=store_backend,
            runtime_environment=None,
        )
        return store

    @override
    def get_key(self) -> GXCloudIdentifier:
        """
        Generates a GX Cloud-specific key for use with Stores. See parent "DataContextVariables.get_key" for more details.
        """  # noqa: E501
        from great_expectations.data_context.cloud_constants import GXCloudRESTResource

        key = GXCloudIdentifier(resource_type=GXCloudRESTResource.DATA_CONTEXT_VARIABLES)
        return key
