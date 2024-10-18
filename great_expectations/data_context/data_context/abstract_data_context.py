from __future__ import annotations

import configparser
import copy
import logging
import os
import pathlib
import sys
import uuid
import warnings
import webbrowser
from abc import ABC, abstractmethod
from collections import OrderedDict
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    Union,
    overload,
)

import great_expectations as gx
import great_expectations.exceptions as gx_exceptions
from great_expectations._docs_decorators import (
    new_argument,
    new_method_or_class,
    public_api,
)
from great_expectations.analytics.client import init as init_analytics
from great_expectations.analytics.client import submit as submit_event
from great_expectations.analytics.config import ENV_CONFIG
from great_expectations.analytics.events import DataContextInitializedEvent
from great_expectations.compatibility.typing_extensions import override
from great_expectations.core import ExpectationSuite
from great_expectations.core.batch import (
    Batch,
    BatchRequestBase,
    IDDict,
    get_batch_request_from_acceptable_arguments,
)
from great_expectations.core.config_peer import ConfigPeer
from great_expectations.core.config_provider import (
    _ConfigurationProvider,
    _ConfigurationVariablesConfigurationProvider,
    _EnvironmentConfigurationProvider,
    _RuntimeEnvironmentConfigurationProvider,
)
from great_expectations.core.expectation_validation_result import get_metric_kwargs_id
from great_expectations.core.factory import (
    CheckpointFactory,
    SuiteFactory,
    ValidationDefinitionFactory,
)
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context.store import Store, TupleStoreBackend
from great_expectations.data_context.templates import CONFIG_VARIABLES_TEMPLATE
from great_expectations.data_context.types.base import (
    DataContextConfig,
    DataContextConfigDefaults,
    ProgressBarsConfig,
    dataContextConfigSchema,
)
from great_expectations.data_context.types.resource_identifiers import (
    ExpectationSuiteIdentifier,
    ValidationMetricIdentifier,
    ValidationResultIdentifier,
)
from great_expectations.data_context.util import (
    PasswordMasker,
    instantiate_class_from_config,
    parse_substitution_variable,
)
from great_expectations.datasource.datasource_dict import CacheableDatasourceDict
from great_expectations.datasource.fluent.config import GxConfig
from great_expectations.datasource.fluent.interfaces import Batch as FluentBatch
from great_expectations.datasource.fluent.interfaces import (
    Datasource as FluentDatasource,
)
from great_expectations.datasource.fluent.sources import DataSourceManager
from great_expectations.exceptions.exceptions import DataContextError
from great_expectations.validator.validator import Validator

if TYPE_CHECKING:
    from typing_extensions import TypeAlias

    from great_expectations.checkpoint.checkpoint import CheckpointResult
    from great_expectations.core.expectation_validation_result import (
        ExpectationValidationResult,
    )
    from great_expectations.data_context.data_context_variables import (
        DataContextVariables,
    )
    from great_expectations.data_context.store.checkpoint_store import CheckpointStore
    from great_expectations.data_context.store.datasource_store import DatasourceStore
    from great_expectations.data_context.store.expectations_store import (
        ExpectationsStore,
    )
    from great_expectations.data_context.store.store import (
        DataDocsSiteConfigTypedDict,
        StoreConfigTypedDict,
    )
    from great_expectations.data_context.store.validation_definition_store import (
        ValidationDefinitionStore,
    )
    from great_expectations.data_context.store.validation_results_store import (
        ValidationResultsStore,
    )
    from great_expectations.datasource.datasource_dict import DatasourceDict
    from great_expectations.datasource.fluent.interfaces import (
        BatchParameters,
    )
    from great_expectations.datasource.fluent.interfaces import (
        BatchRequest as FluentBatchRequest,
    )
    from great_expectations.execution_engine import ExecutionEngine
    from great_expectations.render.renderer.site_builder import SiteBuilder

BlockConfigDataAssetNames: TypeAlias = Dict[str, List[str]]
FluentDataAssetNames: TypeAlias = List[str]

logger = logging.getLogger(__name__)
yaml = YAMLHandler()


T = TypeVar("T", dict, list, str)


@public_api
class AbstractDataContext(ConfigPeer, ABC):
    """Base class for all Data Contexts that contains shared functionality.

    The class encapsulates most store / core components and convenience methods used to access them, meaning the
    majority of Data Context functionality lives here.

    One of the primary responsibilities of the DataContext is managing CRUD operations for core GX objects:

    .. list-table:: Supported CRUD Methods
       :widths: 10 18 18 18 18
       :header-rows: 1

       * -
         - Stores
         - Datasources
         - ExpectationSuites
         - Checkpoints
       * - `get`
         - ❌
         - ✅
         - ✅
         - ✅
       * - `add`
         - ✅
         - ✅
         - ✅
         - ✅
       * - `update`
         - ❌
         - ✅
         - ✅
         - ✅
       * - `add_or_update`
         - ❌
         - ✅
         - ✅
         - ✅
       * - `delete`
         - ✅
         - ✅
         - ✅
         - ✅
    """  # noqa: E501

    # NOTE: <DataContextRefactor> These can become a property like ExpectationsStore.__name__ or placed in a separate  # noqa: E501
    # test_yml_config module so AbstractDataContext is not so cluttered.
    _ROOT_CONF_DIR = pathlib.Path.home() / ".great_expectations"
    _ROOT_CONF_FILE = _ROOT_CONF_DIR / "great_expectations.conf"
    _ETC_CONF_DIR = pathlib.Path("/etc")
    _ETC_CONF_FILE = _ETC_CONF_DIR / "great_expectations.conf"
    GLOBAL_CONFIG_PATHS = [_ROOT_CONF_FILE, _ETC_CONF_FILE]
    DOLLAR_SIGN_ESCAPE_STRING = r"\$"

    # instance attribute type annotations
    fluent_config: GxConfig

    def __init__(self, runtime_environment: Optional[dict] = None) -> None:
        """
        Constructor for AbstractDataContext. Will handle instantiation logic that is common to all DataContext objects

        Args:
            runtime_environment (dict): a dictionary of config variables that
                override those set in config_variables.yml and the environment
        """  # noqa: E501

        if runtime_environment is None:
            runtime_environment = {}
        self.runtime_environment = runtime_environment

        self._config_provider = self._init_config_provider()
        self._config_variables = self._load_config_variables()
        self._variables = self._init_variables()

        # config providers must be provisioned before loading zep_config
        self.fluent_config = self._load_fluent_config(self._config_provider)

        # Init plugin support
        if self.plugins_directory is not None and os.path.exists(  # noqa: PTH110
            self.plugins_directory
        ):
            sys.path.append(self.plugins_directory)

        # We want to have directories set up before initializing usage statistics so
        # that we can obtain a context instance id
        self._in_memory_instance_id: str | None = (
            None  # This variable *may* be used in case we cannot save an instance id
        )
        # Init stores
        self._stores: dict = {}
        self._init_primary_stores(self.project_config_with_variables_substituted.stores)

        # The DatasourceStore is inherent to all DataContexts but is not an explicit part of the project config.  # noqa: E501
        # As such, it must be instantiated separately.
        self._datasource_store = self._init_datasource_store()
        self._init_datasources()

        # Init data_context_id
        self._data_context_id = self._construct_data_context_id()

        # Override the project_config data_context_id if an expectations_store was already set up
        self.config.data_context_id = self._data_context_id

        self._suite_parameter_dependencies: dict = {}

        self._init_data_source_manager()

        self._attach_fluent_config_datasources_and_build_data_connectors(self.fluent_config)
        self._init_analytics()
        submit_event(event=DataContextInitializedEvent())

    def _init_data_source_manager(self) -> None:
        self._data_sources: DataSourceManager = DataSourceManager(self)

        self._suites: SuiteFactory | None = None
        if expectations_store := self.stores.get(self.expectations_store_name):
            self._suites = SuiteFactory(
                store=expectations_store,
            )

        self._checkpoints: CheckpointFactory | None = None
        if checkpoint_store := self.stores.get(self.checkpoint_store_name):
            self._checkpoints = CheckpointFactory(
                store=checkpoint_store,
            )

        self._validation_definitions: ValidationDefinitionFactory = ValidationDefinitionFactory(
            store=self.validation_definition_store
        )

    def _init_analytics(self) -> None:
        init_analytics(
            enable=self._determine_analytics_enabled(),
            user_id=None,
            data_context_id=self._data_context_id,
            organization_id=None,
            oss_id=self._get_oss_id(),
        )

    def _determine_analytics_enabled(self) -> bool:
        """
        Determine if analytics are enabled using the following precedence
          - The `analytics_enabled` key in the GX config
          - The `GX_ANALYTICS_ENABLED` environment variable
          - Otherwise, assume True
        """
        config_enabled = self.config.analytics_enabled
        env_var_enabled = ENV_CONFIG.posthog_enabled
        if config_enabled is not None:
            return config_enabled
        elif env_var_enabled is not None:
            return env_var_enabled
        else:
            return True

    def _init_config_provider(self) -> _ConfigurationProvider:
        config_provider = _ConfigurationProvider()
        self._register_providers(config_provider)
        return config_provider

    def _register_providers(self, config_provider: _ConfigurationProvider) -> None:
        """
        Registers any relevant ConfigurationProvider instances to self._config_provider.

        Note that order matters here - if there is a namespace collision, later providers will overwrite
        the values derived from previous ones. The order of precedence is as follows:
            - Config variables
            - Environment variables
            - Runtime environment
        """  # noqa: E501
        config_variables_file_path = self._project_config.config_variables_file_path
        if config_variables_file_path:
            config_provider.register_provider(
                _ConfigurationVariablesConfigurationProvider(
                    config_variables_file_path=config_variables_file_path,
                    root_directory=self.root_directory,
                )
            )
        config_provider.register_provider(_EnvironmentConfigurationProvider())
        config_provider.register_provider(
            _RuntimeEnvironmentConfigurationProvider(self.runtime_environment)
        )

    @abstractmethod
    def _init_project_config(
        self, project_config: DataContextConfig | Mapping
    ) -> DataContextConfig:
        raise NotImplementedError

    @abstractmethod
    def _init_variables(self) -> DataContextVariables:
        raise NotImplementedError

    def _save_project_config(self) -> None:
        """
        Each DataContext will define how its project_config will be saved through its internal 'variables'.
            - FileDataContext : Filesystem.
            - CloudDataContext : Cloud endpoint
            - Ephemeral : not saved, and logging message outputted
        """  # noqa: E501
        return self.variables.save()

    @public_api
    def enable_analytics(self, enable: Optional[bool]) -> None:
        """
        Enable or disable analytics for this DataContext.
        With non-ephemeral contexts, this can be preserved via context.variables.save().

        If set to None, the `GX_ANALYTICS_ENABLED` environment variable will be used.
        """
        self.config.analytics_enabled = enable
        self._init_analytics()
        self.variables.save()

    @public_api
    def update_project_config(
        self, project_config: DataContextConfig | Mapping
    ) -> DataContextConfig:
        """Update the context's config with the values from another config object.

        Args:
            project_config: The config to use to update the context's internal state.

        Returns:
            The updated project config.
        """
        self.config.update(project_config)
        return self.config

    # Properties
    @property
    def instance_id(self) -> str:
        instance_id: Optional[str] = self.config_variables.get("instance_id")
        if instance_id is None:
            if self._in_memory_instance_id is not None:
                return self._in_memory_instance_id
            instance_id = str(uuid.uuid4())
            self._in_memory_instance_id = instance_id
        return instance_id

    @property
    def config_variables(self) -> Dict:
        """Loads config variables into cache, by calling _load_config_variables()

        Returns: A dictionary containing config_variables from file or empty dictionary.
        """
        if not self._config_variables:
            self._config_variables = self._load_config_variables()
        return self._config_variables

    @property
    @override
    def config(self) -> DataContextConfig:
        """
        Returns current DataContext's project_config
        """
        # NOTE: <DataContextRefactor> _project_config is currently only defined in child classes.
        # See if this can this be also defined in AbstractDataContext as abstract property
        return self.variables.config

    @property
    def config_provider(self) -> _ConfigurationProvider:
        return self._config_provider

    @property
    def root_directory(self) -> Optional[str]:  # TODO: This should be a `pathlib.Path`
        """The root directory for configuration objects in the data context; the location in which
        ``great_expectations.yml`` is located.
        """
        # NOTE: <DataContextRefactor>  Why does this exist in AbstractDataContext? CloudDataContext and  # noqa: E501
        # FileDataContext both use it. Determine whether this should stay here or in child classes
        return getattr(self, "_context_root_directory", None)

    @property
    def project_config_with_variables_substituted(self) -> DataContextConfig:
        return self.get_config_with_variables_substituted()

    @property
    def plugins_directory(self) -> Optional[str]:
        """The directory in which custom plugin modules should be placed."""
        # NOTE: <DataContextRefactor>  Why does this exist in AbstractDataContext? CloudDataContext and  # noqa: E501
        # FileDataContext both use it. Determine whether this should stay here or in child classes
        return self._normalize_absolute_or_relative_path(self.variables.plugins_directory)

    @property
    def stores(self) -> dict:
        """A single holder for all Stores in this context"""
        return self._stores

    @property
    def datasource_store(self) -> DatasourceStore:
        return self._datasource_store

    @property
    @public_api
    def suites(self) -> SuiteFactory:
        if not self._suites:
            raise gx_exceptions.DataContextError(  # noqa: TRY003
                "DataContext requires a configured ExpectationsStore to persist ExpectationSuites."
            )
        return self._suites

    @property
    @public_api
    def checkpoints(self) -> CheckpointFactory:
        if not self._checkpoints:
            raise gx_exceptions.DataContextError(  # noqa: TRY003
                "DataContext requires a configured CheckpointStore to persist Checkpoints."
            )
        return self._checkpoints

    @property
    @public_api
    def validation_definitions(self) -> ValidationDefinitionFactory:
        if not self._validation_definitions:
            raise gx_exceptions.DataContextError(  # noqa: TRY003
                "DataContext requires a configured ValidationDefinitionStore to persist "
                "Validations."
            )
        return self._validation_definitions

    @property
    def expectations_store_name(self) -> Optional[str]:
        return self.variables.expectations_store_name

    @expectations_store_name.setter
    @new_method_or_class(version="0.17.2")
    def expectations_store_name(self, value: str) -> None:
        """Set the name of the expectations store.

        Args:
            value: New value for the expectations store name.
        """

        self.variables.expectations_store_name = value
        self._save_project_config()

    @property
    def expectations_store(self) -> ExpectationsStore:
        return self.stores[self.expectations_store_name]

    @property
    def validation_results_store_name(self) -> Optional[str]:
        return self.variables.validation_results_store_name

    @validation_results_store_name.setter
    @new_method_or_class(version="0.17.2")
    def validation_results_store_name(self, value: str) -> None:
        """Set the name of the validations store.

        Args:
            value: New value for the validations store name.
        """
        self.variables.validation_results_store_name = value
        self._save_project_config()

    @property
    def validation_results_store(self) -> ValidationResultsStore:
        return self.stores[self.validation_results_store_name]

    @property
    def validation_definition_store(self) -> ValidationDefinitionStore:
        # Purposely not exposing validation_definition_store_name as a user-configurable property
        return self.stores[DataContextConfigDefaults.DEFAULT_VALIDATION_DEFINITION_STORE_NAME.value]

    @property
    def checkpoint_store_name(self) -> Optional[str]:
        from great_expectations.data_context.store.checkpoint_store import (
            CheckpointStore,
        )

        if name := self.variables.checkpoint_store_name:
            return name

        if CheckpointStore.default_checkpoints_exist(
            directory_path=self.root_directory  # type: ignore[arg-type]
        ):
            return DataContextConfigDefaults.DEFAULT_CHECKPOINT_STORE_NAME.value

        return None

    @checkpoint_store_name.setter
    @new_method_or_class(version="0.17.2")
    def checkpoint_store_name(self, value: str) -> None:
        """Set the name of the checkpoint store.

        Args:
            value: New value for the checkpoint store name.
        """
        self.variables.checkpoint_store_name = value
        self._save_project_config()

    @property
    def checkpoint_store(self) -> CheckpointStore:
        return self.stores[self.checkpoint_store_name]

    @property
    @public_api
    def data_sources(self) -> DataSourceManager:
        return self._data_sources

    @property
    def _include_rendered_content(self) -> bool:
        return False

    def _add_fluent_datasource(
        self, datasource: Optional[FluentDatasource] = None, save_changes: bool = True, **kwargs
    ) -> FluentDatasource:
        if datasource:
            datasource_name = datasource.name
        else:
            datasource_name = kwargs.get("name", "")

        if not datasource_name:
            raise gx_exceptions.DataContextError(  # noqa: TRY003
                "Can not write the fluent datasource, because no name was provided."
            )

        # We currently don't allow one to overwrite a datasource with this internal method
        if datasource_name in self.data_sources.all():
            raise gx_exceptions.DataContextError(  # noqa: TRY003
                f"Can not write the fluent datasource {datasource_name} because a datasource of that "  # noqa: E501
                "name already exists in the data context."
            )

        if not datasource:
            ds_type = DataSourceManager.type_lookup[kwargs["type"]]
            datasource = ds_type(**kwargs)
        assert isinstance(datasource, FluentDatasource)

        return_obj = self.data_sources.all().set_datasource(name=datasource_name, ds=datasource)
        assert isinstance(return_obj, FluentDatasource)
        return_obj._data_context = self
        if save_changes:
            self._save_project_config()
            self.config.fluent_datasources[return_obj.name] = return_obj

        return return_obj

    def _update_fluent_datasource(
        self, datasource: Optional[FluentDatasource] = None, **kwargs
    ) -> FluentDatasource:
        if datasource:
            datasource_name = datasource.name
        else:
            datasource_name = kwargs.get("name", "")

        if not datasource_name:
            raise gx_exceptions.DataContextError(  # noqa: TRY003
                "Can not write the fluent datasource, because no name was provided."
            )

        if not datasource:
            ds_type = DataSourceManager.type_lookup[kwargs["type"]]
            updated_datasource = ds_type(**kwargs)
        else:
            updated_datasource = datasource

        updated_datasource._rebuild_asset_data_connectors()

        updated_datasource = self.data_sources.all().set_datasource(
            name=datasource_name, ds=updated_datasource
        )
        updated_datasource._data_context = self  # TODO: move from here?
        self._save_project_config()

        assert isinstance(updated_datasource, FluentDatasource)
        self.config.fluent_datasources[datasource_name] = updated_datasource
        return updated_datasource

    def _delete_fluent_datasource(self, name: str, _call_store: bool = True) -> None:
        """
        _call_store = False allows for local deletes without deleting the persisted storage datasource.
        This should generally be avoided.
        """  # noqa: E501
        self.fluent_config.pop_datasource(name, None)
        datasource = self.data_sources.all().get(name)
        if datasource:
            if self._datasource_store.cloud_mode and _call_store:
                self._datasource_store.delete(datasource)
        else:
            # Raise key error instead?
            logger.info(f"No Datasource '{name}' to delete")
        self.data_sources.all().pop(name, None)
        del self.config.fluent_datasources[name]

    def set_config(self, project_config: DataContextConfig) -> None:
        self._project_config = project_config
        self.variables.config = project_config

    @overload
    def add_datasource(
        self,
        name: str = ...,
        initialize: bool = ...,
        datasource: None = ...,
        **kwargs,
    ) -> FluentDatasource | None:
        """
        A `name` is provided.
        `datasource` should not be provided.
        """
        ...

    @overload
    def add_datasource(
        self,
        name: None = ...,
        initialize: bool = ...,
        datasource: FluentDatasource = ...,
        **kwargs,
    ) -> FluentDatasource | None:
        """
        A `datasource` is provided.
        `name` should not be provided.
        """
        ...

    @new_argument(
        argument_name="datasource",
        version="0.15.49",
        message="Pass in an existing Datasource instead of individual constructor arguments",
    )
    def add_datasource(
        self,
        name: str | None = None,
        initialize: bool = True,
        datasource: FluentDatasource | None = None,
        **kwargs,
    ) -> FluentDatasource | None:
        """Add a new Datasource to the data context, with configuration provided as kwargs.

        --Documentation--
            - https://docs.greatexpectations.io/docs/terms/datasource

        Args:
            name: the name of the new Datasource to add
            initialize: if False, add the Datasource to the config, but do not
                initialize it, for example if a user needs to debug database connectivity.
            datasource: an existing Datasource you wish to persist
            kwargs: the configuration for the new Datasource

        Returns:
            Datasource instance added.
        """
        return self._add_datasource(
            name=name,
            initialize=initialize,
            datasource=datasource,
            **kwargs,
        )

    @staticmethod
    def _validate_add_datasource_args(
        name: str | None,
        datasource: FluentDatasource | None,
        **kwargs,
    ) -> None:
        if not ((datasource is None) ^ (name is None)):
            error_message = (
                "Must either pass in an existing 'datasource' or individual constructor arguments"
            )
            if datasource and name:
                error_message += " (but not both)"
            raise TypeError(error_message)

        # "type" is only used in FDS so we check for its existence (equivalent for block-style would be "class_name" and "module_name")  # noqa: E501
        if "type" in kwargs:
            raise TypeError(  # noqa: TRY003
                "Creation of fluent-datasources with individual arguments is not supported and should be done through the `context.sources` API."  # noqa: E501
            )

    def _add_datasource(
        self,
        name: str | None = None,
        initialize: bool = True,
        datasource: FluentDatasource | None = None,
        **kwargs,
    ) -> FluentDatasource | None:
        self._validate_add_datasource_args(name=name, datasource=datasource, **kwargs)
        if isinstance(datasource, FluentDatasource):
            self._add_fluent_datasource(
                datasource=datasource,
            )
        else:
            raise DataContextError("Datasource is not a FluentDatasource")  # noqa: TRY003
        return datasource

    def update_datasource(
        self,
        datasource: FluentDatasource,
    ) -> FluentDatasource:
        """Updates a Datasource that already exists in the store.

        Args:
            datasource: The Datasource object to update.

        Returns:
            The updated Datasource.
        """
        return self._update_fluent_datasource(datasource=datasource)

    @overload
    def add_or_update_datasource(
        self,
        name: str = ...,
        datasource: None = ...,
        **kwargs,
    ) -> FluentDatasource:
        """
        A `name` is provided.
        `datasource` should not be provided.
        """
        ...

    @overload
    def add_or_update_datasource(
        self,
        name: None = ...,
        datasource: FluentDatasource = ...,
        **kwargs,
    ) -> FluentDatasource:
        """
        A `datasource` is provided.
        `name` should not be provided.
        """
        ...

    @new_method_or_class(version="0.15.48")
    def add_or_update_datasource(
        self,
        name: str | None = None,
        datasource: FluentDatasource | None = None,
        **kwargs,
    ) -> FluentDatasource:
        """Add a new Datasource or update an existing one on the context depending on whether
        it already exists or not. The configuration is provided as kwargs.

        Args:
            name: The name of the Datasource to add or update.
            datasource: an existing Datasource you wish to persist.
            kwargs: Any relevant keyword args to use when adding or updating the target Datasource named `name`.

        Returns:
            The Datasource added or updated by the input `kwargs`.
        """  # noqa: E501
        self._validate_add_datasource_args(name=name, datasource=datasource)
        return_datasource: FluentDatasource

        if "type" in kwargs:
            assert name, 'Fluent Datasource kwargs must include the keyword "name"'
            kwargs["name"] = name
            if name in self.data_sources.all():
                self._update_fluent_datasource(**kwargs)
            else:
                self._add_fluent_datasource(**kwargs)
            return_datasource = self.data_sources.all()[name]
        else:
            if datasource is None:
                raise ValueError("Either datasource or kwargs are required")  # noqa: TRY003
            if datasource.name in self.data_sources.all():
                self._update_fluent_datasource(datasource=datasource)
            else:
                self._add_fluent_datasource(datasource=datasource)
            return_datasource = self.data_sources.all()[datasource.name]

        return return_datasource

    def get_site_names(self) -> List[str]:
        """Get a list of configured site names."""
        return list(self.variables.data_docs_sites.keys())  # type: ignore[union-attr]

    def get_config_with_variables_substituted(
        self, config: Optional[DataContextConfig] = None
    ) -> DataContextConfig:
        """
        Substitute vars in config of form ${var} or $(var) with values found in the following places,
        in order of precedence: gx_cloud_config (for Data Contexts in GX Cloud mode), runtime_environment,
        environment variables, config_variables, or gx_cloud_config_variable_defaults (allows certain variables to
        be optional in GX Cloud mode).
        """  # noqa: E501
        if not config:
            config = self._project_config
        return DataContextConfig(**self.config_provider.substitute_config(config))

    def list_stores(self) -> List[Store]:
        """List currently-configured Stores on this context"""
        stores = []
        for (
            name,
            value,
        ) in self.variables.stores.items():  # type: ignore[union-attr]
            store_config = copy.deepcopy(value)
            store_config["name"] = name
            masked_config = PasswordMasker.sanitize_config(store_config)
            stores.append(masked_config)
        return stores  # type: ignore[return-value]

    def list_active_stores(self) -> List[Store]:
        """
        List active Stores on this context. Active stores are identified by setting the following parameters:
            expectations_store_name,
            validation_results_store_name,
            checkpoint_store_name
        """  # noqa: E501
        active_store_names: List[str] = [
            self.expectations_store_name,  # type: ignore[list-item]
            self.validation_results_store_name,  # type: ignore[list-item]
        ]

        try:
            active_store_names.append(self.checkpoint_store_name)  # type: ignore[arg-type]
        except (AttributeError, gx_exceptions.InvalidTopLevelConfigKeyError):
            logger.info("Checkpoint store is not configured; omitting it from active stores")

        return [
            store
            for store in self.list_stores()
            if store.get("name") in active_store_names  # type: ignore[arg-type,operator]
        ]

    def get_datasource(self, name: str = "default") -> FluentDatasource:
        """Retrieve a given Datasource by name from the context's underlying DatasourceStore.

        Args:
            name: The name of the target datasource.

        Returns:
            The target datasource.

        Raises:
            ValueError: The input `datasource_name` is None.
        """
        # deprecated-v1.2.0
        warnings.warn(
            "context.get_datasource is deprecated as of v1.2.0. "
            "Please use context.data_sources.get instead",
            category=DeprecationWarning,
        )
        try:
            return self.data_sources.get(name)
        except KeyError as e:
            raise ValueError(str(e)) from e

    def add_store(self, name: str, config: StoreConfigTypedDict) -> Store:
        """Add a new Store to the DataContext.

        Args:
            name: the name to associate with the created store.
            config: the config to use to construct the store.

        Returns:
            The instantiated Store.
        """
        store = self._build_store_from_config(name, config)

        # Both the config and the actual stores need to be kept in sync
        self.config.stores[name] = config
        self._stores[name] = store

        self._save_project_config()
        return store

    @public_api
    @new_method_or_class(version="0.17.2")
    def add_data_docs_site(self, site_name: str, site_config: DataDocsSiteConfigTypedDict) -> None:
        """Add a new Data Docs Site to the DataContext.

        Example site config dicts can be found in our "Host and share Data Docs" guides.

        Args:
            site_name: New site name to add.
            site_config: Config dict for the new site.
        """
        if self.config.data_docs_sites is not None:
            if site_name in self.config.data_docs_sites:
                raise gx_exceptions.InvalidKeyError(  # noqa: TRY003
                    f"Data Docs Site `{site_name}` already exists in the Data Context."
                )

            sites = self.config.data_docs_sites
            sites[site_name] = site_config
            self.variables.data_docs_sites = sites
            self._save_project_config()

    @public_api
    @new_method_or_class(version="0.17.2")
    def list_data_docs_sites(
        self,
    ) -> dict[str, DataDocsSiteConfigTypedDict]:
        """List all Data Docs Sites with configurations."""

        if self.config.data_docs_sites is None:
            return {}
        else:
            return self.config.data_docs_sites

    @public_api
    @new_method_or_class(version="0.17.2")
    def update_data_docs_site(
        self, site_name: str, site_config: DataDocsSiteConfigTypedDict
    ) -> None:
        """Update an existing Data Docs Site.

        Example site config dicts can be found in our "Host and share Data Docs" guides.

        Args:
            site_name: Site name to update.
            site_config: Config dict that replaces the existing.
        """
        if self.config.data_docs_sites is not None:
            if site_name not in self.config.data_docs_sites:
                raise gx_exceptions.InvalidKeyError(  # noqa: TRY003
                    f"Data Docs Site `{site_name}` does not already exist in the Data Context."
                )

            sites = self.config.data_docs_sites
            sites[site_name] = site_config
            self.variables.data_docs_sites = sites
            self._save_project_config()

    @public_api
    @new_method_or_class(version="0.17.2")
    def delete_data_docs_site(self, site_name: str):
        """Delete an existing Data Docs Site.

        Args:
            site_name: Site name to delete.
        """
        if self.config.data_docs_sites is not None:
            if site_name not in self.config.data_docs_sites:
                raise gx_exceptions.InvalidKeyError(  # noqa: TRY003
                    f"Data Docs Site `{site_name}` does not already exist in the Data Context."
                )

            sites = self.config.data_docs_sites
            sites.pop(site_name)
            self.variables.data_docs_sites = sites
            self._save_project_config()

    @new_method_or_class(version="0.15.48")
    def delete_store(self, name: str) -> None:
        """Delete an existing Store from the DataContext.

        Args:
            name: The name of the Store to be deleted.

        Raises:
            StoreConfigurationError if the target Store is not found.
        """
        if name not in self.config.stores and name not in self._stores:
            raise gx_exceptions.StoreConfigurationError(  # noqa: TRY003
                f'Attempted to delete a store named: "{name}". It is not a configured store.'
            )

        # Both the config and the actual stores need to be kept in sync
        self.config.stores.pop(name, None)
        self._stores.pop(name, None)

        self._save_project_config()

    def list_datasources(self) -> List[dict]:
        """List the configurations of the datasources associated with this context.

        Note that any sensitive values are obfuscated before being returned.

        Returns:
            A list of dictionaries representing datasource configurations.
        """
        return [ds.dict() for ds in self.data_sources.all().values()]

    def delete_datasource(self, name: Optional[str]) -> None:
        """Delete a given Datasource by name.

        Note that this method causes deletion from the underlying DatasourceStore.

        Args:
            name: The name of the target datasource.

        Raises:
            ValueError: The `datasource_name` isn't provided or cannot be found.
        """

        if not name:
            raise ValueError("Datasource names must be a datasource name")  # noqa: TRY003

        self._delete_fluent_datasource(name)

        self._save_project_config()

    def get_validator(  # noqa: PLR0913
        self,
        datasource_name: Optional[str] = None,
        data_connector_name: Optional[str] = None,
        data_asset_name: Optional[str] = None,
        batch: Optional[Batch] = None,
        batch_list: Optional[List[Batch]] = None,
        batch_request: Optional[Union[BatchRequestBase, FluentBatchRequest]] = None,
        batch_request_list: Optional[List[BatchRequestBase]] = None,
        batch_data: Optional[Any] = None,
        data_connector_query: Optional[Union[IDDict, dict]] = None,
        batch_identifiers: Optional[dict] = None,
        limit: Optional[int] = None,
        index: Optional[Union[int, list, tuple, slice, str]] = None,
        custom_filter_function: Optional[Callable] = None,
        sampling_method: Optional[str] = None,
        sampling_kwargs: Optional[dict] = None,
        partitioner_method: Optional[str] = None,
        partitioner_kwargs: Optional[dict] = None,
        runtime_parameters: Optional[dict] = None,
        query: Optional[str] = None,
        path: Optional[str] = None,
        batch_filter_parameters: Optional[dict] = None,
        batch_spec_passthrough: Optional[dict] = None,
        expectation_suite_id: Optional[str] = None,
        expectation_suite_name: Optional[str] = None,
        expectation_suite: Optional[ExpectationSuite] = None,
        create_expectation_suite_with_name: Optional[str] = None,
        **kwargs,
    ) -> Validator:
        """Retrieve a Validator with a batch list and an `ExpectationSuite`.

        `get_validator` first calls `get_batch_list` to retrieve a batch list, then creates or retrieves
        an `ExpectationSuite` used to validate the Batches in the list.

        Args:
            datasource_name: The name of the Datasource that defines the Data Asset to retrieve the batch for
            data_connector_name: The Data Connector within the datasource for the Data Asset
            data_asset_name: The name of the Data Asset within the Data Connector
            batch: The Batch to use with the Validator
            batch_list: The List of Batches to use with the Validator
            batch_request: Encapsulates all the parameters used here to retrieve a BatchList. Use either
                `batch_request` or the other params (but not both)
            batch_request_list: A List of `BatchRequest` to use with the Validator
            batch_data: Provides runtime data for the batch; is added as the key `batch_data` to
                the `runtime_parameters` dictionary of a BatchRequest
            query: Provides runtime data for the batch; is added as the key `query` to
                the `runtime_parameters` dictionary of a BatchRequest
            path: Provides runtime data for the batch; is added as the key `path` to
                the `runtime_parameters` dictionary of a BatchRequest
            runtime_parameters: Specifies runtime parameters for the BatchRequest; can includes keys `batch_data`,
                `query`, and `path`
            data_connector_query: Used to specify connector query parameters; specifically `batch_filter_parameters`,
                `limit`, `index`, and `custom_filter_function`
            batch_identifiers: Any identifiers of batches for the BatchRequest
            batch_filter_parameters: Filter parameters used in the data connector query
            limit: Part of the data_connector_query, limits the number of batches in the batch list
            index: Part of the data_connector_query, used to specify the index of which batch to return. Negative
                numbers retrieve from the end of the list (ex: `-1` retrieves the last or latest batch)
            custom_filter_function: A `Callable` function that accepts `batch_identifiers` and returns a `bool`
            sampling_method: The method used to sample Batch data (see: Partitioning and Sampling)
            sampling_kwargs: Arguments for the sampling method
            partitioner_method: The method used to partition the Data Asset into Batches
            partitioner_kwargs: Arguments for the partitioning method
            batch_spec_passthrough: Arguments specific to the `ExecutionEngine` that aid in Batch retrieval
            expectation_suite_id: The identifier of the ExpectationSuite to retrieve from the DataContext
                (can be used in place of `expectation_suite_name`)
            expectation_suite_name: The name of the ExpectationSuite to retrieve from the DataContext
            expectation_suite: The ExpectationSuite to use with the validator
            create_expectation_suite_with_name: Creates a Validator with a new ExpectationSuite with the provided name
            **kwargs: Used to specify either `batch_identifiers` or `batch_filter_parameters`

        Returns:
            Validator: A Validator with the specified Batch list and ExpectationSuite

        Raises:
            DatasourceError: If the specified `datasource_name` does not exist in the DataContext
            TypeError: If the specified types of the `batch_request` are not supported, or if the
                `datasource_name` is not a `str`
            ValueError: If more than one exclusive parameter is specified (ex: specifing more than one
                of `batch_data`, `query` or `path`), or if the `ExpectationSuite` cannot be created or
                retrieved using either the provided name or identifier
        """  # noqa: E501
        expectation_suite = self._get_expectation_suite_from_inputs(
            expectation_suite=expectation_suite,
            expectation_suite_name=expectation_suite_name,
            create_expectation_suite_with_name=create_expectation_suite_with_name,
            expectation_suite_id=expectation_suite_id,
        )
        batch_list = self._get_batch_list_from_inputs(
            datasource_name=datasource_name,
            data_connector_name=data_connector_name,
            data_asset_name=data_asset_name,
            batch=batch,
            batch_list=batch_list,
            batch_request=batch_request,
            batch_request_list=batch_request_list,
            batch_data=batch_data,
            data_connector_query=data_connector_query,
            batch_identifiers=batch_identifiers,
            limit=limit,
            index=index,
            custom_filter_function=custom_filter_function,
            sampling_method=sampling_method,
            sampling_kwargs=sampling_kwargs,
            partitioner_method=partitioner_method,
            partitioner_kwargs=partitioner_kwargs,
            runtime_parameters=runtime_parameters,
            query=query,
            path=path,
            batch_filter_parameters=batch_filter_parameters,
            batch_spec_passthrough=batch_spec_passthrough,
            **kwargs,
        )
        return self.get_validator_using_batch_list(
            expectation_suite=expectation_suite,
            batch_list=batch_list,
        )

    def _get_batch_list_from_inputs(  # noqa: PLR0913
        self,
        datasource_name: str | None,
        data_connector_name: str | None,
        data_asset_name: str | None,
        batch: Batch | None,
        batch_list: List[Batch] | None,
        batch_request: BatchRequestBase | FluentBatchRequest | None,
        batch_request_list: List[BatchRequestBase] | None,
        batch_data: Any,
        data_connector_query: Union[IDDict, dict] | None,
        batch_identifiers: dict | None,
        limit: int | None,
        index: int | list | tuple | slice | str | None,
        custom_filter_function: Callable | None,
        sampling_method: str | None,
        sampling_kwargs: dict | None,
        partitioner_method: str | None,
        partitioner_kwargs: dict | None,
        runtime_parameters: dict | None,
        query: str | None,
        path: str | None,
        batch_filter_parameters: dict | None,
        batch_spec_passthrough: dict | None,
        **kwargs,
    ) -> List[Batch]:
        if (
            sum(
                bool(x)
                for x in [
                    batch is not None,
                    batch_list is not None,
                    batch_request is not None,
                    batch_request_list is not None,
                ]
            )
            > 1
        ):
            raise ValueError(  # noqa: TRY003
                "No more than one of batch, batch_list, batch_request, or batch_request_list can be specified"  # noqa: E501
            )

        if batch_list:
            return batch_list

        if batch:
            return [batch]

        computed_batch_list: List[Batch] = []
        if not batch_request_list:
            # batch_request could actually be None here since we do explicit None checks in the
            # sum check above while here we do a truthy check.
            batch_request_list = [batch_request]  # type: ignore[list-item]
        for batch_req in batch_request_list:
            computed_batch_list.append(
                self.get_last_batch(
                    datasource_name=datasource_name,
                    data_connector_name=data_connector_name,
                    data_asset_name=data_asset_name,
                    batch_request=batch_req,
                    batch_data=batch_data,
                    data_connector_query=data_connector_query,
                    batch_identifiers=batch_identifiers,
                    limit=limit,
                    index=index,
                    custom_filter_function=custom_filter_function,
                    sampling_method=sampling_method,
                    sampling_kwargs=sampling_kwargs,
                    partitioner_method=partitioner_method,
                    partitioner_kwargs=partitioner_kwargs,
                    runtime_parameters=runtime_parameters,
                    query=query,
                    path=path,
                    batch_filter_parameters=batch_filter_parameters,
                    batch_spec_passthrough=batch_spec_passthrough,
                    **kwargs,
                )
            )
        return computed_batch_list

    def _get_expectation_suite_from_inputs(
        self,
        expectation_suite: ExpectationSuite | None = None,
        expectation_suite_name: str | None = None,
        create_expectation_suite_with_name: str | None = None,
        expectation_suite_id: str | None = None,
    ) -> ExpectationSuite | None:
        """Get an expectation suite from optional inputs. Also validates inputs.

        Args:
            expectation_suite: An ExpectationSuite object
            expectation_suite_name: The name of the ExpectationSuite to retrieve from the DataContext
            create_expectation_suite_with_name: Creates a new ExpectationSuite with the provided name
            expectation_suite_id: The identifier of the ExpectationSuite to retrieve from the DataContext
                (can be used in place of `expectation_suite_name`)

        Returns:
            An ExpectationSuite instance

        Raises:
            ValueError if the inputs are not valid

        """  # noqa: E501
        if (
            sum(
                bool(x)
                for x in [
                    expectation_suite is not None,
                    expectation_suite_name is not None,
                    create_expectation_suite_with_name is not None,
                    expectation_suite_id is not None,
                ]
            )
            > 1
        ):
            raise ValueError(  # noqa: TRY003
                "No more than one of expectation_suite_name, "
                f"{'expectation_suite_id, ' if expectation_suite_id else ''}"
                "expectation_suite, or create_expectation_suite_with_name can be specified"
            )
        if expectation_suite_id is not None:
            expectation_suite = next(
                suite for suite in self.suites.all() if suite.id == expectation_suite_id
            )
        if expectation_suite_name is not None:
            expectation_suite = self.suites.get(
                expectation_suite_name,
            )
        if create_expectation_suite_with_name is not None:
            expectation_suite = self.suites.add(
                ExpectationSuite(name=create_expectation_suite_with_name)
            )

        return expectation_suite

    # noinspection PyUnusedLocal
    def get_validator_using_batch_list(
        self,
        expectation_suite: ExpectationSuite | None,
        batch_list: Sequence[Union[Batch, FluentBatch]],
        **kwargs: Optional[dict],
    ) -> Validator:
        """

        Args:
            expectation_suite ():
            batch_list ():
            **kwargs ():

        Returns:

        """
        if len(batch_list) == 0:
            raise gx_exceptions.InvalidBatchRequestError(  # noqa: TRY003
                """Validator could not be created because BatchRequest returned an empty batch_list.
                Please check your parameters and try again."""
            )

        # We get a single batch_definition so we can get the execution_engine here. All batches will share the same one  # noqa: E501
        # So the batch itself doesn't matter. But we use -1 because that will be the latest batch loaded.  # noqa: E501
        execution_engine: ExecutionEngine
        batch = batch_list[-1]
        assert isinstance(batch, FluentBatch)
        execution_engine = batch.data.execution_engine

        validator = Validator(
            execution_engine=execution_engine,
            interactive_evaluation=True,
            expectation_suite=expectation_suite,
            data_context=self,
            batches=batch_list,
        )

        return validator

    def get_last_batch(  # noqa: PLR0913
        self,
        datasource_name: Optional[str] = None,
        data_connector_name: Optional[str] = None,
        data_asset_name: Optional[str] = None,
        batch_request: Optional[BatchRequestBase] = None,
        batch_data: Optional[Any] = None,
        data_connector_query: Optional[dict] = None,
        batch_identifiers: Optional[dict] = None,
        limit: Optional[int] = None,
        index: Optional[Union[int, list, tuple, slice, str]] = None,
        custom_filter_function: Optional[Callable] = None,
        sampling_method: Optional[str] = None,
        sampling_kwargs: Optional[dict] = None,
        partitioner_method: Optional[str] = None,
        partitioner_kwargs: Optional[dict] = None,
        runtime_parameters: Optional[dict] = None,
        query: Optional[str] = None,
        path: Optional[str] = None,
        batch_filter_parameters: Optional[dict] = None,
        batch_spec_passthrough: Optional[dict] = None,
        batch_parameters: Optional[Union[dict, BatchParameters]] = None,
        **kwargs: Optional[dict],
    ) -> Batch:
        """Get the list of zero or more batches, based on a variety of flexible input types.

        `get_batch_list` is the main user-facing API for getting batches.
        In contrast to virtually all other methods in the class, it does not require typed or nested inputs.
        Instead, this method is intended to help the user pick the right parameters

        This method attempts to return any number of batches, including an empty list.

        Args:
            datasource_name: The name of the Datasource that defines the Data Asset to retrieve the batch for
            data_connector_name: The Data Connector within the datasource for the Data Asset
            data_asset_name: The name of the Data Asset within the Data Connector
            batch_request: Encapsulates all the parameters used here to retrieve a BatchList. Use either
                `batch_request` or the other params (but not both)
            batch_data: Provides runtime data for the batch; is added as the key `batch_data` to
                the `runtime_parameters` dictionary of a BatchRequest
            query: Provides runtime data for the batch; is added as the key `query` to
                the `runtime_parameters` dictionary of a BatchRequest
            path: Provides runtime data for the batch; is added as the key `path` to
                the `runtime_parameters` dictionary of a BatchRequest
            runtime_parameters: Specifies runtime parameters for the BatchRequest; can includes keys `batch_data`,
                `query`, and `path`
            data_connector_query: Used to specify connector query parameters; specifically `batch_filter_parameters`,
                `limit`, `index`, and `custom_filter_function`
            batch_identifiers: Any identifiers of batches for the BatchRequest
            batch_filter_parameters: Filter parameters used in the data connector query
            limit: Part of the data_connector_query, limits the number of batches in the batch list
            index: Part of the data_connector_query, used to specify the index of which batch to return. Negative
                numbers retrieve from the end of the list (ex: `-1` retrieves the last or latest batch)
            custom_filter_function: A `Callable` function that accepts `batch_identifiers` and returns a `bool`
            sampling_method: The method used to sample Batch data (see: Partitioning and Sampling)
            sampling_kwargs: Arguments for the sampling method
            partitioner_method: The method used to partition the Data Asset into Batches
            partitioner_kwargs: Arguments for the partitioning method
            batch_spec_passthrough: Arguments specific to the `ExecutionEngine` that aid in Batch retrieval
            batch_parameters: Options for `FluentBatchRequest`
            **kwargs: Used to specify either `batch_identifiers` or `batch_filter_parameters`

        Returns:
            (Batch) The `list` of requested Batch instances

        Raises:
            DatasourceError: If the specified `datasource_name` does not exist in the DataContext
            TypeError: If the specified types of the `batch_request` are not supported, or if the
                `datasource_name` is not a `str`
            ValueError: If more than one exclusive parameter is specified (ex: specifing more than one
                of `batch_data`, `query` or `path`)

        """  # noqa: E501
        return self._get_last_batch(
            datasource_name=datasource_name,
            data_connector_name=data_connector_name,
            data_asset_name=data_asset_name,
            batch_request=batch_request,
            batch_data=batch_data,
            data_connector_query=data_connector_query,
            batch_identifiers=batch_identifiers,
            limit=limit,
            index=index,
            custom_filter_function=custom_filter_function,
            sampling_method=sampling_method,
            sampling_kwargs=sampling_kwargs,
            partitioner_method=partitioner_method,
            partitioner_kwargs=partitioner_kwargs,
            runtime_parameters=runtime_parameters,
            query=query,
            path=path,
            batch_filter_parameters=batch_filter_parameters,
            batch_spec_passthrough=batch_spec_passthrough,
            batch_parameters=batch_parameters,
            **kwargs,
        )

    def _get_last_batch(  # noqa: PLR0913
        self,
        datasource_name: Optional[str] = None,
        data_connector_name: Optional[str] = None,
        data_asset_name: Optional[str] = None,
        batch_request: Optional[BatchRequestBase] = None,
        batch_data: Optional[Any] = None,
        data_connector_query: Optional[dict] = None,
        batch_identifiers: Optional[dict] = None,
        limit: Optional[int] = None,
        index: Optional[Union[int, list, tuple, slice, str]] = None,
        custom_filter_function: Optional[Callable] = None,
        sampling_method: Optional[str] = None,
        sampling_kwargs: Optional[dict] = None,
        partitioner_method: Optional[str] = None,
        partitioner_kwargs: Optional[dict] = None,
        runtime_parameters: Optional[dict] = None,
        query: Optional[str] = None,
        path: Optional[str] = None,
        batch_filter_parameters: Optional[dict] = None,
        batch_spec_passthrough: Optional[dict] = None,
        batch_parameters: Optional[Union[dict, BatchParameters]] = None,
        **kwargs: Optional[dict],
    ) -> Batch:
        result = get_batch_request_from_acceptable_arguments(
            datasource_name=datasource_name,
            data_connector_name=data_connector_name,
            data_asset_name=data_asset_name,
            batch_request=batch_request,
            batch_data=batch_data,
            data_connector_query=data_connector_query,
            batch_identifiers=batch_identifiers,
            limit=limit,
            index=index,
            custom_filter_function=custom_filter_function,
            sampling_method=sampling_method,
            sampling_kwargs=sampling_kwargs,
            partitioner_method=partitioner_method,
            partitioner_kwargs=partitioner_kwargs,
            runtime_parameters=runtime_parameters,
            query=query,
            path=path,
            batch_filter_parameters=batch_filter_parameters,
            batch_spec_passthrough=batch_spec_passthrough,
            batch_parameters=batch_parameters,
            **kwargs,
        )
        datasource_name = result.datasource_name

        datasource = self.data_sources.all().get(datasource_name)
        if not datasource:
            raise gx_exceptions.DatasourceError(
                datasource_name,
                "The given datasource could not be retrieved from the DataContext; "
                "please confirm that your configuration is accurate.",
            )

        return datasource.get_batch(batch_request=result)

    def _validate_datasource_names(self, datasource_names: list[str] | str | None) -> list[str]:
        if datasource_names is None:
            datasource_names = [datasource["name"] for datasource in self.list_datasources()]
        elif isinstance(datasource_names, str):
            datasource_names = [datasource_names]
        elif not isinstance(datasource_names, list):
            raise ValueError(  # noqa: TRY003
                "Datasource names must be a datasource name, list of datasource names or None (to list all datasources)"  # noqa: E501
            )
        return datasource_names

    def get_available_data_asset_names(
        self,
        datasource_names: str | list[str] | None = None,
        batch_kwargs_generator_names: str | list[str] | None = None,
    ) -> dict[str, BlockConfigDataAssetNames | FluentDataAssetNames]:
        """Inspect datasource and batch kwargs generators to provide available data_asset objects.

        Args:
            datasource_names: List of datasources for which to provide available data asset name objects.
                              If None, return available data assets for all datasources.
            batch_kwargs_generator_names: List of batch kwargs generators for which to provide available data_asset_name objects.

        Returns:
            data_asset_names: Dictionary describing available data assets

        Raises:
            ValueError: `datasource_names` is not None, a string, or list of strings.
        """  # noqa: E501
        fluent_data_asset_names: dict[str, BlockConfigDataAssetNames | FluentDataAssetNames] = {}
        datasource_names = self._validate_datasource_names(datasource_names)

        # TODO: V1-222 batch_kwargs_generator_names is legacy and should be removed for V1
        # TODO: conditional FDS vs BDS datasource logic should be removed for V1
        if batch_kwargs_generator_names is not None:
            if isinstance(batch_kwargs_generator_names, str):
                batch_kwargs_generator_names = [batch_kwargs_generator_names]
            if len(batch_kwargs_generator_names) == len(datasource_names):
                for datasource_name in datasource_names:
                    datasource = self.data_sources.get(datasource_name)
                    fluent_data_asset_names[datasource_name] = sorted(datasource.get_asset_names())

            elif len(batch_kwargs_generator_names) == 1:
                datasource = self.data_sources.get(datasource_names[0])
                fluent_data_asset_names[datasource_names[0]] = sorted(datasource.get_asset_names())

            else:
                raise ValueError(  # noqa: TRY003
                    "If providing batch kwargs generator, you must either specify one for each datasource or only "  # noqa: E501
                    "one datasource."
                )
        else:  # generator_names is None
            for datasource_name in datasource_names:
                try:
                    datasource = self.data_sources.get(datasource_name)
                    fluent_data_asset_names[datasource_name] = sorted(datasource.get_asset_names())

                except KeyError:
                    # handle the edge case of a non-existent datasource
                    fluent_data_asset_names[datasource_name] = {}

        return fluent_data_asset_names

    def build_batch_kwargs(
        self,
        datasource,
        batch_kwargs_generator,
        data_asset_name=None,
        partition_id=None,
        **kwargs,
    ):
        """Builds batch kwargs using the provided datasource, batch kwargs generator, and batch_parameters.

        Args:
            datasource (str): the name of the datasource for which to build batch_kwargs
            batch_kwargs_generator (str): the name of the batch kwargs generator to use to build batch_kwargs
            data_asset_name (str): an optional name batch_parameter
            **kwargs: additional batch_parameters

        Returns:
            BatchKwargs

        """  # noqa: E501
        datasource_obj = self.data_sources.get(datasource)
        batch_kwargs = datasource_obj.build_batch_kwargs(
            batch_kwargs_generator=batch_kwargs_generator,
            data_asset_name=data_asset_name,
            partition_id=partition_id,
            **kwargs,
        )
        return batch_kwargs

    def open_data_docs(
        self,
        resource_identifier: Optional[str] = None,
        site_name: Optional[str] = None,
        only_if_exists: bool = True,
    ) -> None:
        """
        A stdlib cross-platform way to open a file in a browser.

        Args:
            resource_identifier: ExpectationSuiteIdentifier,
                ValidationResultIdentifier or any other type's identifier. The
                argument is optional - when not supplied, the method returns the
                URL of the index page.
            site_name: Optionally specify which site to open. If not specified,
                open all docs found in the project.
            only_if_exists: Optionally specify flag to pass to "self.get_docs_sites_urls()".
        """
        return self._open_data_docs(
            resource_identifier=resource_identifier,
            site_name=site_name,
            only_if_exists=only_if_exists,
        )

    def _open_data_docs(
        self,
        resource_identifier: Optional[str] = None,
        site_name: Optional[str] = None,
        only_if_exists: bool = True,
    ) -> None:
        data_docs_urls = self.get_docs_sites_urls(
            resource_identifier=resource_identifier,
            site_name=site_name,
            only_if_exists=only_if_exists,
        )
        nullable_urls = [site["site_url"] for site in data_docs_urls]
        urls_to_open = [url for url in nullable_urls if url is not None]

        if not urls_to_open:
            raise gx.exceptions.NoDataDocsError

        for url in urls_to_open:
            logger.debug(f"Opening Data Docs found here: {url}")
            self._open_url_in_browser(url)

    @staticmethod
    def _open_url_in_browser(url: str) -> None:
        webbrowser.open(url)

    def get_docs_sites_urls(
        self,
        resource_identifier: ExpectationSuiteIdentifier
        | ValidationResultIdentifier
        | str
        | None = None,
        site_name: Optional[str] = None,
        only_if_exists: bool = True,
        site_names: Optional[List[str]] = None,
    ) -> List[Dict[str, Optional[str]]]:
        """
        Get URLs for a resource for all data docs sites.

        This function will return URLs for any configured site even if the sites
        have not been built yet.

        Args:
            resource_identifier (object): optional. It can be an identifier of
                ExpectationSuite's, ValidationResults and other resources that
                have typed identifiers. If not provided, the method will return
                the URLs of the index page.
            site_name: Optionally specify which site to open. If not specified,
                return all urls in the project.
            site_names: Optionally specify which sites are active. Sites not in
                this list are not processed, even if specified in site_name.

        Returns:
            list: a list of URLs. Each item is the URL for the resource for a
                data docs site
        """
        unfiltered_sites = self.variables.data_docs_sites

        # Filter out sites that are not in site_names
        sites = (
            {k: v for k, v in unfiltered_sites.items() if k in site_names}  # type: ignore[union-attr]
            if site_names
            else unfiltered_sites
        )

        if not sites:
            logger.debug("Found no data_docs_sites.")
            return []
        logger.debug(f"Found {len(sites)} data_docs_sites.")

        if site_name:
            if site_name not in sites:
                raise gx_exceptions.DataContextError(  # noqa: TRY003
                    f"Could not find site named {site_name}. Please check your configurations"
                )
            site = sites[site_name]
            site_builder = self._load_site_builder_from_site_config(site)
            url = site_builder.get_resource_url(
                resource_identifier=resource_identifier, only_if_exists=only_if_exists
            )
            return [{"site_name": site_name, "site_url": url}]

        site_urls: List[Dict[str, Optional[str]]] = []
        for _site_name, site_config in sites.items():
            site_builder = self._load_site_builder_from_site_config(site_config)
            url = site_builder.get_resource_url(
                resource_identifier=resource_identifier, only_if_exists=only_if_exists
            )
            site_urls.append({"site_name": _site_name, "site_url": url})

        return site_urls

    def _load_site_builder_from_site_config(self, site_config) -> SiteBuilder:
        default_module_name = "great_expectations.render.renderer.site_builder"
        site_builder = instantiate_class_from_config(
            config=site_config,
            runtime_environment={
                "data_context": self,
                "root_directory": self.root_directory,
            },
            config_defaults={"module_name": default_module_name},
        )
        if not site_builder:
            raise gx_exceptions.ClassInstantiationError(
                module_name=default_module_name,
                package_name=None,
                class_name=site_config["class_name"],
            )
        return site_builder

    def clean_data_docs(self, site_name=None) -> bool:
        """
        Clean a given data docs site.

        This removes all files from the configured Store.

        Args:
            site_name (str): Optional, the name of the site to clean. If not
            specified, all sites will be cleaned.
        """
        data_docs_sites = self.variables.data_docs_sites
        if not data_docs_sites:
            raise gx_exceptions.DataContextError(  # noqa: TRY003
                "No data docs sites were found on this DataContext, therefore no sites will be cleaned.",  # noqa: E501
            )

        data_docs_site_names = list(data_docs_sites.keys())
        if site_name:
            if site_name not in data_docs_site_names:
                raise gx_exceptions.DataContextError(  # noqa: TRY003
                    f"The specified site name `{site_name}` does not exist in this project."
                )
            return self._clean_data_docs_site(site_name)

        cleaned = []
        for existing_site_name in data_docs_site_names:
            cleaned.append(self._clean_data_docs_site(existing_site_name))
        return all(cleaned)

    def _clean_data_docs_site(self, site_name: str) -> bool:
        sites = self.variables.data_docs_sites
        if not sites:
            return False
        site_config = sites.get(site_name)

        site_builder = instantiate_class_from_config(
            config=site_config,
            runtime_environment={
                "data_context": self,
                "root_directory": self.root_directory,
            },
            config_defaults={"module_name": "great_expectations.render.renderer.site_builder"},
        )
        site_builder.clean_site()
        return True

    @staticmethod
    def _get_global_config_value(
        environment_variable: str,
        conf_file_section: Optional[str] = None,
        conf_file_option: Optional[str] = None,
    ) -> Optional[str]:
        """
        Method to retrieve config value.
        Looks for config value in environment_variable and config file section

        Args:
            environment_variable (str): name of environment_variable to retrieve
            conf_file_section (str): section of config
            conf_file_option (str): key in section

        Returns:
            Optional string representing config value
        """
        assert (conf_file_section and conf_file_option) or (
            not conf_file_section and not conf_file_option
        ), "Must pass both 'conf_file_section' and 'conf_file_option' or neither."
        if environment_variable and os.environ.get(  # noqa: TID251
            environment_variable, ""
        ):
            return os.environ.get(environment_variable)  # noqa: TID251
        if conf_file_section and conf_file_option:
            for config_path in AbstractDataContext.GLOBAL_CONFIG_PATHS:
                config: configparser.ConfigParser = configparser.ConfigParser()
                config.read(config_path)
                config_value: Optional[str] = config.get(
                    conf_file_section, conf_file_option, fallback=None
                )
                if config_value:
                    return config_value
        return None

    @staticmethod
    def _get_metric_configuration_tuples(  # noqa: C901
        metric_configuration: Union[str, dict], base_kwargs: Optional[dict] = None
    ) -> List[Tuple[str, Union[dict, Any]]]:
        if base_kwargs is None:
            base_kwargs = {}

        if isinstance(metric_configuration, str):
            return [(metric_configuration, base_kwargs)]

        metric_configurations_list = []
        for kwarg_name in metric_configuration:
            if not isinstance(metric_configuration[kwarg_name], dict):
                raise gx_exceptions.DataContextError(  # noqa: TRY003
                    "Invalid metric_configuration: each key must contain a " "dictionary."
                )
            if (
                kwarg_name == "metric_kwargs_id"
            ):  # this special case allows a hash of multiple kwargs
                for metric_kwargs_id in metric_configuration[kwarg_name]:
                    if base_kwargs != {}:
                        raise gx_exceptions.DataContextError(  # noqa: TRY003
                            "Invalid metric_configuration: when specifying "
                            "metric_kwargs_id, no other keys or values may be defined."
                        )
                    if not isinstance(metric_configuration[kwarg_name][metric_kwargs_id], list):
                        raise gx_exceptions.DataContextError(  # noqa: TRY003
                            "Invalid metric_configuration: each value must contain a " "list."
                        )
                    metric_configurations_list += [
                        (metric_name, {"metric_kwargs_id": metric_kwargs_id})
                        for metric_name in metric_configuration[kwarg_name][metric_kwargs_id]
                    ]
            else:
                for kwarg_value in metric_configuration[kwarg_name]:
                    base_kwargs.update({kwarg_name: kwarg_value})
                    if not isinstance(metric_configuration[kwarg_name][kwarg_value], list):
                        raise gx_exceptions.DataContextError(  # noqa: TRY003
                            "Invalid metric_configuration: each value must contain a " "list."
                        )
                    for nested_configuration in metric_configuration[kwarg_name][kwarg_value]:
                        metric_configurations_list += (
                            AbstractDataContext._get_metric_configuration_tuples(
                                nested_configuration, base_kwargs=base_kwargs
                            )
                        )

        return metric_configurations_list

    @classmethod
    def get_or_create_data_context_config(
        cls, project_config: DataContextConfig | Mapping
    ) -> DataContextConfig:
        """Utility method to take in an input config and ensure its conversion to a rich
        DataContextConfig. If the input is already of the appropriate type, the function
        exits early.

        Args:
            project_config: The input config to be evaluated.

        Returns:
            An instance of DataContextConfig.

        Raises:
            ValidationError if the input config does not adhere to the required shape of a DataContextConfig.
        """  # noqa: E501
        if isinstance(project_config, DataContextConfig):
            return project_config

        # Roundtrip through schema validation to remove any illegal fields add/or restore any missing fields.  # noqa: E501
        project_config_dict = dataContextConfigSchema.dump(project_config)
        project_config_dict = dataContextConfigSchema.load(project_config_dict)
        context_config: DataContextConfig = DataContextConfig(**project_config_dict)
        return context_config

    @overload
    def _normalize_absolute_or_relative_path(self, path: str) -> str: ...

    @overload
    def _normalize_absolute_or_relative_path(self, path: None) -> None: ...

    def _normalize_absolute_or_relative_path(self, path: Optional[str]) -> Optional[str]:
        """
        Why does this exist in AbstractDataContext? CloudDataContext and FileDataContext both use it
        """
        if path is None:
            return None
        if os.path.isabs(path):  # noqa: PTH117
            return path
        else:
            return os.path.join(self.root_directory, path)  # type: ignore[arg-type]  # noqa: PTH118

    def _load_config_variables(self) -> Dict:
        config_var_provider = self.config_provider.get_provider(
            _ConfigurationVariablesConfigurationProvider
        )
        if config_var_provider:
            return config_var_provider.get_values()
        return {}

    def _build_store_from_config(self, name: str, config: dict | StoreConfigTypedDict) -> Store:
        module_name = "great_expectations.data_context.store"
        # Set expectations_store.store_backend_id to the data_context_id from the project_config if
        # the expectations_store does not yet exist by:
        # adding the data_context_id from the project_config
        # to the store_config under the key manually_initialize_store_backend_id
        if (name == self.expectations_store_name) and config.get("store_backend"):
            config["store_backend"].update(
                {"manually_initialize_store_backend_id": self.variables.data_context_id}
            )

        # Set suppress_store_backend_id = True if store is inactive and has a store_backend.
        if (
            name not in [store["name"] for store in self.list_active_stores()]  # type: ignore[index]
            and config.get("store_backend") is not None
        ):
            config["store_backend"].update({"suppress_store_backend_id": True})

        new_store = Store.build_store_from_config(
            name=name,
            config=config,
            module_name=module_name,
            runtime_environment={
                "root_directory": self.root_directory,
            },
        )
        self._stores[name] = new_store
        return new_store

    # properties
    @property
    def variables(self) -> DataContextVariables:
        if self._variables is None:
            self._variables = self._init_variables()
        return self._variables

    @property
    def progress_bars(self) -> Optional[ProgressBarsConfig]:
        return self.variables.progress_bars

    # TODO: All datasources should now be fluent so we should be able to delete this
    @property
    def fluent_datasources(self) -> Dict[str, FluentDatasource]:
        return {
            name: ds
            for (name, ds) in self.data_sources.all().items()
            if isinstance(ds, FluentDatasource)
        }

    @property
    def data_context_id(self) -> uuid.UUID | None:
        return self.variables.data_context_id

    def _init_primary_stores(self, store_configs: Dict[str, StoreConfigTypedDict]) -> None:
        """Initialize all Stores for this DataContext.

        Stores are a good fit for reading/writing objects that:
            1. follow a clear key-value pattern, and
            2. are usually edited programmatically, using the Context

        Note that stores do NOT manage plugins.
        """
        for store_name, store_config in store_configs.items():
            self._build_store_from_config(store_name, store_config)

    @abstractmethod
    def _init_datasource_store(self) -> DatasourceStore:
        """Internal utility responsible for creating a DatasourceStore to persist and manage a user's Datasources.

        Please note that the DatasourceStore lacks the same extensibility that other analagous Stores do; a default
        implementation is provided based on the user's environment but is not customizable.
        """  # noqa: E501
        raise NotImplementedError

    def _update_config_variables(self) -> None:
        """Updates config_variables cache by re-calling _load_config_variables().
        Necessary after running methods that modify config AND could contain config_variables for credentials
        (example is add_datasource())
        """  # noqa: E501
        self._config_variables = self._load_config_variables()

    @classmethod
    def _get_oss_id(cls) -> uuid.UUID | None:
        """
        Retrieves a user's `oss_id` from disk ($HOME/.great_expectations/great_expectations.conf).

        If no such value is present, a new UUID is generated and written to disk for subsequent usage.
        If there is an error when reading from / writing to disk, we default to a NoneType.
        """  # noqa: E501
        config = configparser.ConfigParser()

        if not cls._ROOT_CONF_FILE.exists():
            success = cls._scaffold_root_conf()
            if not success:
                return None
            return cls._set_oss_id(config)

        try:
            config.read(cls._ROOT_CONF_FILE)
        except OSError as e:
            logger.info(f"Something went wrong when trying to read from the user's conf file: {e}")
            return None

        oss_id = config.get("analytics", "oss_id", fallback=None)
        if not oss_id:
            return cls._set_oss_id(config)

        return uuid.UUID(oss_id)

    @classmethod
    def _set_oss_id(cls, config: configparser.ConfigParser) -> uuid.UUID | None:
        """
        Generates a random UUID and writes it to disk for subsequent usage.
        Assumes that the root conf file exists.

        Args:
            config: The parser used to read/write the oss_id.

        If there is an error when writing to disk, we default to a NoneType.
        """
        oss_id = uuid.uuid4()

        # If the section already exists, don't overwrite
        section = "analytics"
        if not config.has_section(section):
            config[section] = {}
        config[section]["oss_id"] = str(oss_id)

        try:
            with cls._ROOT_CONF_FILE.open("w") as f:
                config.write(f)
        except OSError as e:
            logger.info(
                f"Something went wrong when trying to write the user's conf file to disk: {e}"
            )
            return None

        return oss_id

    @classmethod
    def _scaffold_root_conf(cls) -> bool:
        """
        Set up an empty root conf file ($HOME/.great_expectations/great_expectations.conf)

        Returns:
            Whether or not directory/file creation was successful.
        """
        try:
            cls._ROOT_CONF_DIR.mkdir(exist_ok=True)
            cls._ROOT_CONF_FILE.touch()
        except OSError as e:
            logger.info(
                f"Something went wrong when trying to write the user's conf file to disk: {e}"
            )
            return False
        return True

    def _init_datasources(self) -> None:
        """Initialize the datasources in store"""
        self._datasources: DatasourceDict = CacheableDatasourceDict(
            context=self,
            datasource_store=self._datasource_store,
        )

        config: DataContextConfig = self.config

        if self._datasource_store.cloud_mode:
            for fds in config.fluent_datasources.values():
                datasource = self._add_fluent_datasource(**fds)
                datasource._rebuild_asset_data_connectors()

    def _construct_data_context_id(self) -> uuid.UUID | None:
        # Choose the id of the currently-configured expectations store, if it is a persistent store
        expectations_store = self.stores[self.expectations_store_name]
        if isinstance(expectations_store.store_backend, TupleStoreBackend):
            # suppress_warnings since a warning will already have been issued during the store creation  # noqa: E501
            # if there was an invalid store config
            return expectations_store.store_backend_id_warnings_suppressed

        # Otherwise choose the id stored in the project_config
        else:
            return self.variables.data_context_id

    def get_validation_result(  # noqa: C901
        self,
        expectation_suite_name,
        run_id=None,
        batch_identifier=None,
        validation_results_store_name=None,
        failed_only=False,
    ) -> ExpectationValidationResult | dict:
        """Get validation results from a configured store.

        Args:
            expectation_suite_name: expectation_suite name for which to get validation result (default: "default")
            run_id: run_id for which to get validation result (if None, fetch the latest result by alphanumeric sort)
            validation_results_store_name: the name of the store from which to get validation results
            failed_only: if True, filter the result to return only failed expectations

        Returns:
            validation_result

        """  # noqa: E501
        if validation_results_store_name is None:
            validation_results_store_name = self.validation_results_store_name
        selected_store = self.stores[validation_results_store_name]

        if run_id is None or batch_identifier is None:
            # Get most recent run id
            # NOTE : This method requires a (potentially very inefficient) list_keys call.
            # It should probably move to live in an appropriate Store class,
            # but when we do so, that Store will need to function as more than just a key-value Store.  # noqa: E501
            key_list = selected_store.list_keys()
            filtered_key_list = []
            for key in key_list:
                if run_id is not None and key.run_id != run_id:
                    continue
                if batch_identifier is not None and key.batch_identifier != batch_identifier:
                    continue
                filtered_key_list.append(key)

            # run_id_set = set([key.run_id for key in filtered_key_list])
            if len(filtered_key_list) == 0:
                logger.warning("No valid run_id values found.")
                return {}

            filtered_key_list = sorted(filtered_key_list, key=lambda x: x.run_id)

            if run_id is None:
                run_id = filtered_key_list[-1].run_id
            if batch_identifier is None:
                batch_identifier = filtered_key_list[-1].batch_identifier

        key = ValidationResultIdentifier(
            expectation_suite_identifier=ExpectationSuiteIdentifier(name=expectation_suite_name),
            run_id=run_id,
            batch_identifier=batch_identifier,
        )
        results_dict = selected_store.get(key)

        validation_result = (
            results_dict.get_failed_validation_results() if failed_only else results_dict
        )

        if self._include_rendered_content:
            for expectation_validation_result in validation_result.results:
                expectation_validation_result.render()

        return validation_result

    def store_validation_result_metrics(
        self, requested_metrics, validation_results, target_store_name
    ) -> None:
        self._store_metrics(
            requested_metrics=requested_metrics,
            validation_results=validation_results,
            target_store_name=target_store_name,
        )

    def _store_metrics(self, requested_metrics, validation_results, target_store_name) -> None:
        """
        requested_metrics is a dictionary like this:

          requested_metrics:
            *: The asterisk here matches *any* expectation suite name
               use the 'kwargs' key to request metrics that are defined by kwargs,
               for example because they are defined only for a particular column
               - column:
                   Age:
                     - expect_column_min_to_be_between.result.observed_value
                - statistics.evaluated_expectations
                - statistics.successful_expectations
        """
        expectation_suite_name = validation_results.meta["expectation_suite_name"]
        run_id = validation_results.meta["run_id"]
        data_asset_name = validation_results.meta.get("active_batch_definition", {}).get(
            "data_asset_name"
        )

        for expectation_suite_dependency, metrics_list in requested_metrics.items():
            if (expectation_suite_dependency != "*") and (  # noqa: PLR1714
                expectation_suite_dependency != expectation_suite_name
            ):
                continue

            if not isinstance(metrics_list, list):
                raise gx_exceptions.DataContextError(  # noqa: TRY003
                    "Invalid requested_metrics configuration: metrics requested for "
                    "each expectation suite must be a list."
                )

            for metric_configuration in metrics_list:
                metric_configurations = AbstractDataContext._get_metric_configuration_tuples(
                    metric_configuration
                )
                for metric_name, metric_kwargs in metric_configurations:
                    try:
                        metric_value = validation_results.get_metric(metric_name, **metric_kwargs)
                        self.stores[target_store_name].set(
                            ValidationMetricIdentifier(
                                run_id=run_id,
                                data_asset_name=data_asset_name,
                                expectation_suite_identifier=ExpectationSuiteIdentifier(
                                    expectation_suite_name
                                ),
                                metric_name=metric_name,
                                metric_kwargs_id=get_metric_kwargs_id(metric_kwargs=metric_kwargs),
                            ),
                            metric_value,
                        )
                    except gx_exceptions.UnavailableMetricError:
                        # This will happen frequently in larger pipelines
                        logger.debug(
                            f"metric {metric_name} was requested by another expectation suite but is not available in "  # noqa: E501
                            "this validation result."
                        )

    @public_api
    def build_data_docs(
        self,
        site_names: list[str] | None = None,
        resource_identifiers: list[ExpectationSuiteIdentifier]
        | list[ValidationResultIdentifier]
        | None = None,
        dry_run: bool = False,
        build_index: bool = True,
    ) -> dict[str, str]:
        """Build Data Docs for your project.

        --Documentation--
            - https://docs.greatexpectations.io/docs/terms/data_docs/

        Args:
            site_names: if specified, build data docs only for these sites, otherwise,
                build all the sites specified in the context's config
            resource_identifiers: a list of resource identifiers (ExpectationSuiteIdentifier,
                ValidationResultIdentifier). If specified, rebuild HTML
                (or other views the data docs sites are rendering) only for
                the resources in this list. This supports incremental build
                of data docs sites (e.g., when a new validation result is created)
                and avoids full rebuild.
            dry_run: a flag, if True, the method returns a structure containing the
                URLs of the sites that *would* be built, but it does not build
                these sites.
            build_index: a flag if False, skips building the index page

        Returns:
            A dictionary with the names of the updated data documentation sites as keys and the location info
            of their index.html files as values

        Raises:
            ClassInstantiationError: Site config in your Data Context config is not valid.
        """  # noqa: E501
        return self._build_data_docs(
            site_names=site_names,
            resource_identifiers=resource_identifiers,
            dry_run=dry_run,
            build_index=build_index,
        )

    def _build_data_docs(
        self,
        site_names: list[str] | None = None,
        resource_identifiers: list | None = None,
        dry_run: bool = False,
        build_index: bool = True,
    ) -> dict:
        logger.debug("Starting DataContext.build_data_docs")

        index_page_locator_infos = {}

        sites = self.variables.data_docs_sites
        if sites:
            logger.debug("Found data_docs_sites. Building sites...")

            for site_name, site_config in sites.items():
                logger.debug(
                    f"Building Data Docs Site {site_name}",
                )

                if (site_names and (site_name in site_names)) or not site_names:
                    complete_site_config = site_config
                    module_name = "great_expectations.render.renderer.site_builder"
                    site_builder: SiteBuilder = self._init_site_builder_for_data_docs_site_creation(
                        site_name=site_name,
                        site_config=site_config,
                    )
                    if not site_builder:
                        raise gx_exceptions.ClassInstantiationError(
                            module_name=module_name,
                            package_name=None,
                            class_name=complete_site_config["class_name"],
                        )
                    if dry_run:
                        index_page_locator_infos[site_name] = site_builder.get_resource_url(
                            only_if_exists=False
                        )
                    else:
                        index_page_resource_identifier_tuple = site_builder.build(
                            resource_identifiers,
                            build_index=build_index,
                        )
                        if index_page_resource_identifier_tuple:
                            index_page_locator_infos[site_name] = (
                                index_page_resource_identifier_tuple[0]
                            )

        else:
            logger.debug("No data_docs_config found. No site(s) built.")

        return index_page_locator_infos

    def _init_site_builder_for_data_docs_site_creation(
        self,
        site_name: str,
        site_config: dict,
    ) -> SiteBuilder:
        site_builder: SiteBuilder = instantiate_class_from_config(
            config=site_config,
            runtime_environment={
                "data_context": self,
                "root_directory": self.root_directory,
                "site_name": site_name,
            },
            config_defaults={
                "class_name": "SiteBuilder",
                "module_name": "great_expectations.render.renderer.site_builder",
            },
        )
        return site_builder

    @public_api
    @new_method_or_class(version="0.16.15")
    def view_validation_result(self, result: CheckpointResult) -> None:
        """
        Opens a validation result in a browser.

        Args:
            result: The result of a Checkpoint run.
        """
        self._view_validation_result(result)

    def _view_validation_result(self, result: CheckpointResult) -> None:
        validation_result_identifier = tuple(result.run_results.keys())[0]
        self.open_data_docs(resource_identifier=validation_result_identifier)  # type: ignore[arg-type]

    def escape_all_config_variables(
        self,
        value: T,
        dollar_sign_escape_string: str = DOLLAR_SIGN_ESCAPE_STRING,
        skip_if_substitution_variable: bool = True,
    ) -> T:
        """
        Replace all `$` characters with the DOLLAR_SIGN_ESCAPE_STRING

        Args:
            value: config variable value
            dollar_sign_escape_string: replaces instances of `$`
            skip_if_substitution_variable: skip if the value is of the form ${MYVAR} or $MYVAR

        Returns:
            input value with all `$` characters replaced with the escape string
        """
        if isinstance(value, (dict, OrderedDict)):
            return {
                k: self.escape_all_config_variables(
                    value=v,
                    dollar_sign_escape_string=dollar_sign_escape_string,
                    skip_if_substitution_variable=skip_if_substitution_variable,
                )
                for k, v in value.items()
            }
        elif isinstance(value, list):
            return [
                self.escape_all_config_variables(
                    value=v,
                    dollar_sign_escape_string=dollar_sign_escape_string,
                    skip_if_substitution_variable=skip_if_substitution_variable,
                )
                for v in value
            ]
        if skip_if_substitution_variable:
            if parse_substitution_variable(value) is None:
                return value.replace("$", dollar_sign_escape_string)
            return value
        return value.replace("$", dollar_sign_escape_string)

    def save_config_variable(
        self,
        name: str,
        value: Any,
        skip_if_substitution_variable: bool = True,
    ) -> None:
        r"""Save config variable value
        Escapes $ unless they are used in substitution variables e.g. the $ characters in ${SOME_VAR} or $SOME_VAR are not escaped

        Args:
            name: name of the property
            value: the value to save for the property
            skip_if_substitution_variable: set to False to escape $ in values in substitution variable form e.g. ${SOME_VAR} -> r"\${SOME_VAR}" or $SOME_VAR -> r"\$SOME_VAR"

        Returns:
            None
        """  # noqa: E501
        config_variables = self.config_variables
        value = self.escape_all_config_variables(
            value,
            self.DOLLAR_SIGN_ESCAPE_STRING,
            skip_if_substitution_variable=skip_if_substitution_variable,
        )
        config_variables[name] = value
        # Required to call _variables instead of variables property because we don't want to trigger substitutions  # noqa: E501
        config = self._variables.config
        config_variables_filepath = config.config_variables_file_path
        if not config_variables_filepath:
            raise gx_exceptions.InvalidConfigError(  # noqa: TRY003
                "'config_variables_file_path' property is not found in config - setting it is required to use this feature"  # noqa: E501
            )

        config_variables_filepath = os.path.join(  # noqa: PTH118
            self.root_directory,  # type: ignore[arg-type]
            config_variables_filepath,
        )

        os.makedirs(  # noqa: PTH103
            os.path.dirname(config_variables_filepath),  # noqa: PTH120
            exist_ok=True,
        )
        if not os.path.isfile(config_variables_filepath):  # noqa: PTH113
            logger.info(f"Creating new substitution_variables file at {config_variables_filepath}")
            with open(config_variables_filepath, "w") as template:
                template.write(CONFIG_VARIABLES_TEMPLATE)

        with open(config_variables_filepath, "w") as config_variables_file:
            yaml.dump(config_variables, config_variables_file)

    def _load_fluent_config(self, config_provider: _ConfigurationProvider) -> GxConfig:
        """Called at beginning of DataContext __init__ after config_providers init."""
        logger.debug(
            f"{self.__class__.__name__} has not implemented `_load_fluent_config()` returning empty `GxConfig`"  # noqa: E501
        )
        return GxConfig(fluent_datasources=[])

    def _attach_fluent_config_datasources_and_build_data_connectors(self, config: GxConfig):
        """Called at end of __init__"""
        for datasource in config.datasources:
            ds_name = datasource.name
            logger.info(f"Loaded '{ds_name}' from fluent config")

            datasource._rebuild_asset_data_connectors()
            # since we are loading the datasource from existing config, we do not need to save it
            self._add_fluent_datasource(datasource=datasource, save_changes=False)

    def _synchronize_fluent_datasources(self) -> Dict[str, FluentDatasource]:
        """
        Update `self.fluent_config.fluent_datasources` with any newly added datasources.
        Should be called before serializing `fluent_config`.
        """
        fluent_datasources = self.fluent_datasources
        if fluent_datasources:
            self.fluent_config.update_datasources(datasources=fluent_datasources)

        return self.fluent_config.get_datasources_as_dict()
