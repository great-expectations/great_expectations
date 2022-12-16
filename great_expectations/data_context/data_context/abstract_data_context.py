from __future__ import annotations

import configparser
import copy
import datetime
import json
import logging
import os
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
    cast,
)

from dateutil.parser import parse
from marshmallow import ValidationError
from ruamel.yaml.comments import CommentedMap
from typing_extensions import Literal

import great_expectations.exceptions as ge_exceptions
from great_expectations.core import ExpectationSuite
from great_expectations.core._docs_decorators import public_api
from great_expectations.core.batch import (
    Batch,
    BatchRequestBase,
    IDDict,
    get_batch_request_from_acceptable_arguments,
)
from great_expectations.core.config_provider import (
    _ConfigurationProvider,
    _ConfigurationVariablesConfigurationProvider,
    _EnvironmentConfigurationProvider,
    _RuntimeEnvironmentConfigurationProvider,
)
from great_expectations.core.expectation_validation_result import get_metric_kwargs_id
from great_expectations.core.id_dict import BatchKwargs
from great_expectations.core.metric import ValidationMetricIdentifier
from great_expectations.core.run_identifier import RunIdentifier
from great_expectations.core.serializer import (
    AbstractConfigSerializer,
    DictConfigSerializer,
)
from great_expectations.core.usage_statistics.events import UsageStatsEvents
from great_expectations.core.util import nested_update
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_asset import DataAsset
from great_expectations.data_context.config_validator.yaml_config_validator import (
    _YamlConfigValidator,
)
from great_expectations.data_context.data_context_variables import DataContextVariables
from great_expectations.data_context.store import Store, TupleStoreBackend
from great_expectations.data_context.store.expectations_store import ExpectationsStore
from great_expectations.data_context.store.profiler_store import ProfilerStore
from great_expectations.data_context.store.validations_store import ValidationsStore
from great_expectations.data_context.templates import CONFIG_VARIABLES_TEMPLATE
from great_expectations.data_context.types.base import (
    CURRENT_GX_CONFIG_VERSION,
    AnonymizedUsageStatisticsConfig,
    CheckpointConfig,
    ConcurrencyConfig,
    DataContextConfig,
    DataContextConfigDefaults,
    DatasourceConfig,
    IncludeRenderedContentConfig,
    NotebookConfig,
    ProgressBarsConfig,
    anonymizedUsageStatisticsSchema,
    dataContextConfigSchema,
    datasourceConfigSchema,
)
from great_expectations.data_context.types.refs import (
    GXCloudIDAwareRef,
    GXCloudResourceRef,
)
from great_expectations.data_context.types.resource_identifiers import (
    ConfigurationIdentifier,
    ExpectationSuiteIdentifier,
    ValidationResultIdentifier,
)
from great_expectations.data_context.util import (
    PasswordMasker,
    build_store_from_config,
    instantiate_class_from_config,
    parse_substitution_variable,
)
from great_expectations.dataset.dataset import Dataset
from great_expectations.datasource import LegacyDatasource
from great_expectations.datasource.datasource_serializer import (
    NamedDatasourceSerializer,
)
from great_expectations.datasource.new_datasource import BaseDatasource, Datasource
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.profile.basic_dataset_profiler import BasicDatasetProfiler
from great_expectations.rule_based_profiler.config.base import (
    RuleBasedProfilerConfig,
    ruleBasedProfilerConfigSchema,
)
from great_expectations.rule_based_profiler.data_assistant.data_assistant_dispatcher import (
    DataAssistantDispatcher,
)
from great_expectations.rule_based_profiler.rule_based_profiler import RuleBasedProfiler
from great_expectations.util import load_class, verify_dynamic_loading_support
from great_expectations.validator.validator import BridgeValidator, Validator

from great_expectations.core.usage_statistics.usage_statistics import (  # isort: skip
    UsageStatisticsHandler,
    add_datasource_usage_statistics,
    get_batch_list_usage_statistics,
    run_validation_operator_usage_statistics,
    save_expectation_suite_usage_statistics,
    send_usage_message,
    usage_statistics_enabled_method,
)

try:
    from sqlalchemy.exc import SQLAlchemyError
except ImportError:
    # We'll redefine this error in code below to catch ProfilerError, which is caught above, so SA errors will
    # just fall through
    SQLAlchemyError = ge_exceptions.ProfilerError

if TYPE_CHECKING:
    from great_expectations.checkpoint import Checkpoint
    from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
    from great_expectations.data_context.store import (
        CheckpointStore,
        EvaluationParameterStore,
    )
    from great_expectations.data_context.types.resource_identifiers import (
        GXCloudIdentifier,
    )
    from great_expectations.experimental.datasources.interfaces import Batch as XBatch
    from great_expectations.experimental.datasources.interfaces import (
        Datasource as XDatasource,
    )
    from great_expectations.render.renderer.site_builder import SiteBuilder
    from great_expectations.rule_based_profiler import RuleBasedProfilerResult
    from great_expectations.validation_operators.validation_operators import (
        ValidationOperator,
    )

logger = logging.getLogger(__name__)
yaml = YAMLHandler()


T = TypeVar("T", dict, list, str)


class AbstractDataContext(ABC):
    """
    Base class for all DataContexts that contain all context-agnostic data context operations.

    The class encapsulates most store / core components and convenience methods used to access them, meaning the
    majority of DataContext functionality lives here.
    """

    # NOTE: <DataContextRefactor> These can become a property like ExpectationsStore.__name__ or placed in a separate
    # test_yml_config module so AbstractDataContext is not so cluttered.
    FALSEY_STRINGS = ["FALSE", "false", "False", "f", "F", "0"]
    GLOBAL_CONFIG_PATHS = [
        os.path.expanduser("~/.great_expectations/great_expectations.conf"),
        "/etc/great_expectations.conf",
    ]
    DOLLAR_SIGN_ESCAPE_STRING = r"\$"
    MIGRATION_WEBSITE: str = "https://docs.greatexpectations.io/docs/guides/miscellaneous/migration_guide#migrating-to-the-batch-request-v3-api"

    PROFILING_ERROR_CODE_TOO_MANY_DATA_ASSETS = 2
    PROFILING_ERROR_CODE_SPECIFIED_DATA_ASSETS_NOT_FOUND = 3
    PROFILING_ERROR_CODE_NO_BATCH_KWARGS_GENERATORS_FOUND = 4
    PROFILING_ERROR_CODE_MULTIPLE_BATCH_KWARGS_GENERATORS_FOUND = 5

    def __init__(self, runtime_environment: Optional[dict] = None) -> None:
        """
        Constructor for AbstractDataContext. Will handle instantiation logic that is common to all DataContext objects

        Args:
            runtime_environment (dict): a dictionary of config variables that
                override those set in config_variables.yml and the environment
        """
        if runtime_environment is None:
            runtime_environment = {}
        self.runtime_environment = runtime_environment

        self._config_provider = self._init_config_provider()
        self._config_variables = self._load_config_variables()
        self._variables = self._init_variables()

        # Init plugin support
        if self.plugins_directory is not None and os.path.exists(
            self.plugins_directory
        ):
            sys.path.append(self.plugins_directory)

        # We want to have directories set up before initializing usage statistics so
        # that we can obtain a context instance id
        self._in_memory_instance_id = (
            None  # This variable *may* be used in case we cannot save an instance id
        )
        # Init stores
        self._stores: dict = {}
        self._init_stores(self.project_config_with_variables_substituted.stores)  # type: ignore[arg-type]

        # Init data_context_id
        self._data_context_id = self._construct_data_context_id()

        # Override the project_config data_context_id if an expectations_store was already set up
        self.config.anonymous_usage_statistics.data_context_id = self._data_context_id
        self._initialize_usage_statistics(
            self.project_config_with_variables_substituted.anonymous_usage_statistics
        )

        # Store cached datasources but don't init them
        self._cached_datasources: dict = {}

        # Build the datasources we know about and have access to
        self._init_datasources()

        self._evaluation_parameter_dependencies_compiled = False
        self._evaluation_parameter_dependencies: dict = {}

        self._assistants = DataAssistantDispatcher(data_context=self)

        # NOTE - 20210112 - Alex Sherstinsky - Validation Operators are planned to be deprecated.
        self.validation_operators: dict = {}

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
        """
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
    def _init_variables(self) -> DataContextVariables:
        raise NotImplementedError

    def _save_project_config(self) -> None:
        """
        Each DataContext will define how its project_config will be saved through its internal 'variables'.
            - FileDataContext : Filesystem.
            - CloudDataContext : Cloud endpoint
            - Ephemeral : not saved, and logging message outputted
        """
        self.variables.save_config()

    @usage_statistics_enabled_method(
        event_name=UsageStatsEvents.DATA_CONTEXT_SAVE_EXPECTATION_SUITE,
        args_payload_fn=save_expectation_suite_usage_statistics,
    )
    def save_expectation_suite(
        self,
        expectation_suite: ExpectationSuite,
        expectation_suite_name: Optional[str] = None,
        overwrite_existing: bool = True,
        include_rendered_content: Optional[bool] = None,
        **kwargs: Optional[dict],
    ) -> None:
        """
        Each DataContext will define how ExpectationSuite will be saved.
        """
        if expectation_suite_name is None:
            key = ExpectationSuiteIdentifier(
                expectation_suite_name=expectation_suite.expectation_suite_name
            )
        else:
            expectation_suite.expectation_suite_name = expectation_suite_name
            key = ExpectationSuiteIdentifier(
                expectation_suite_name=expectation_suite_name
            )
        if (
            self.expectations_store.has_key(key)  # noqa: @601
            and not overwrite_existing
        ):
            raise ge_exceptions.DataContextError(
                "expectation_suite with name {} already exists. If you would like to overwrite this "
                "expectation_suite, set overwrite_existing=True.".format(
                    expectation_suite_name
                )
            )
        self._evaluation_parameter_dependencies_compiled = False
        include_rendered_content = (
            self._determine_if_expectation_suite_include_rendered_content(
                include_rendered_content=include_rendered_content
            )
        )
        if include_rendered_content:
            expectation_suite.render()
        return self.expectations_store.set(key, expectation_suite, **kwargs)

    # Properties
    @property
    def instance_id(self) -> str:
        instance_id: Optional[str] = self.config_variables.get("instance_id")
        if instance_id is None:
            if self._in_memory_instance_id is not None:
                return self._in_memory_instance_id
            instance_id = str(uuid.uuid4())
            self._in_memory_instance_id = instance_id  # type: ignore[assignment]
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
    def root_directory(self) -> Optional[str]:
        """The root directory for configuration objects in the data context; the location in which
        ``great_expectations.yml`` is located.
        """
        # NOTE: <DataContextRefactor>  Why does this exist in AbstractDataContext? CloudDataContext and
        # FileDataContext both use it. Determine whether this should stay here or in child classes
        return getattr(self, "_context_root_directory", None)

    @property
    def project_config_with_variables_substituted(self) -> DataContextConfig:
        return self.get_config_with_variables_substituted()

    @property
    def plugins_directory(self) -> Optional[str]:
        """The directory in which custom plugin modules should be placed."""
        # NOTE: <DataContextRefactor>  Why does this exist in AbstractDataContext? CloudDataContext and
        # FileDataContext both use it. Determine whether this should stay here or in child classes
        return self._normalize_absolute_or_relative_path(
            self.variables.plugins_directory
        )

    @property
    def stores(self) -> dict:
        """A single holder for all Stores in this context"""
        return self._stores

    @property
    def expectations_store_name(self) -> Optional[str]:
        return self.variables.expectations_store_name

    @property
    def expectations_store(self) -> ExpectationsStore:
        return self.stores[self.expectations_store_name]

    @property
    def evaluation_parameter_store_name(self) -> Optional[str]:
        return self.variables.evaluation_parameter_store_name

    @property
    def evaluation_parameter_store(self) -> EvaluationParameterStore:
        return self.stores[self.evaluation_parameter_store_name]

    @property
    def validations_store_name(self) -> Optional[str]:
        return self.variables.validations_store_name

    @property
    def validations_store(self) -> ValidationsStore:
        return self.stores[self.validations_store_name]

    @property
    def checkpoint_store_name(self) -> Optional[str]:
        try:
            return self.variables.checkpoint_store_name
        except AttributeError:
            from great_expectations.data_context.store.checkpoint_store import (
                CheckpointStore,
            )

            if CheckpointStore.default_checkpoints_exist(
                directory_path=self.root_directory  # type: ignore[arg-type]
            ):
                return DataContextConfigDefaults.DEFAULT_CHECKPOINT_STORE_NAME.value
            if self.root_directory:
                checkpoint_store_directory: str = os.path.join(
                    self.root_directory,
                    DataContextConfigDefaults.DEFAULT_CHECKPOINT_STORE_BASE_DIRECTORY_RELATIVE_NAME.value,
                )
                error_message: str = (
                    f"Attempted to access the 'checkpoint_store_name' field "
                    f"with no `checkpoints` directory.\n "
                    f"Please create the following directory: {checkpoint_store_directory}.\n "
                    f"To use the new 'Checkpoint Store' feature, please update your configuration "
                    f"to the new version number {float(CURRENT_GX_CONFIG_VERSION)}.\n  "
                    f"Visit {AbstractDataContext.MIGRATION_WEBSITE} "
                    f"to learn more about the upgrade process."
                )
            else:
                error_message = (
                    f"Attempted to access the 'checkpoint_store_name' field "
                    f"with no `checkpoints` directory.\n  "
                    f"Please create a `checkpoints` directory in your Great Expectations directory."
                    f"To use the new 'Checkpoint Store' feature, please update your configuration "
                    f"to the new version number {float(CURRENT_GX_CONFIG_VERSION)}.\n  "
                    f"Visit {AbstractDataContext.MIGRATION_WEBSITE} "
                    f"to learn more about the upgrade process."
                )

            raise ge_exceptions.InvalidTopLevelConfigKeyError(error_message)

    @property
    def checkpoint_store(self) -> CheckpointStore:
        checkpoint_store_name: str = self.checkpoint_store_name  # type: ignore[assignment]
        try:
            return self.stores[checkpoint_store_name]
        except KeyError:
            from great_expectations.data_context.store.checkpoint_store import (
                CheckpointStore,
            )

            if CheckpointStore.default_checkpoints_exist(
                directory_path=self.root_directory  # type: ignore[arg-type]
            ):
                logger.warning(
                    f"Checkpoint store named '{checkpoint_store_name}' is not a configured store, "
                    f"so will try to use default Checkpoint store.\n  Please update your configuration "
                    f"to the new version number {float(CURRENT_GX_CONFIG_VERSION)} in order to use the new "
                    f"'Checkpoint Store' feature.\n  Visit {AbstractDataContext.MIGRATION_WEBSITE} "
                    f"to learn more about the upgrade process."
                )
                return self._build_store_from_config(  # type: ignore[return-value]
                    checkpoint_store_name,
                    DataContextConfigDefaults.DEFAULT_STORES.value[  # type: ignore[arg-type]
                        checkpoint_store_name
                    ],
                )
            raise ge_exceptions.StoreConfigurationError(
                f'Attempted to access the Checkpoint store: "{checkpoint_store_name}". It is not a configured store.'
            )

    @property
    def profiler_store_name(self) -> Optional[str]:
        try:
            return self.variables.profiler_store_name
        except AttributeError:
            if AbstractDataContext._default_profilers_exist(
                directory_path=self.root_directory
            ):
                return DataContextConfigDefaults.DEFAULT_PROFILER_STORE_NAME.value
            if self.root_directory:
                checkpoint_store_directory: str = os.path.join(
                    self.root_directory,
                    DataContextConfigDefaults.DEFAULT_CHECKPOINT_STORE_BASE_DIRECTORY_RELATIVE_NAME.value,
                )
                error_message: str = (
                    f"Attempted to access the 'profiler_store_name' field "
                    f"with no `profilers` directory.\n  "
                    f"Please create the following directory: {checkpoint_store_directory}\n"
                    f"To use the new 'Profiler Store' feature, please update your configuration "
                    f"to the new version number {float(CURRENT_GX_CONFIG_VERSION)}.\n  "
                    f"Visit {AbstractDataContext.MIGRATION_WEBSITE} to learn more about the "
                    f"upgrade process."
                )
            else:
                error_message = (
                    f"Attempted to access the 'profiler_store_name' field "
                    f"with no `profilers` directory.\n  "
                    f"Please create a `profilers` directory in your Great Expectations project "
                    f"directory.\n  "
                    f"To use the new 'Profiler Store' feature, please update your configuration "
                    f"to the new version number {float(CURRENT_GX_CONFIG_VERSION)}.\n  "
                    f"Visit {AbstractDataContext.MIGRATION_WEBSITE} to learn more about the "
                    f"upgrade process."
                )

            raise ge_exceptions.InvalidTopLevelConfigKeyError(error_message)

    @property
    def profiler_store(self) -> ProfilerStore:
        profiler_store_name: Optional[str] = self.profiler_store_name
        try:
            return self.stores[profiler_store_name]
        except KeyError:
            if AbstractDataContext._default_profilers_exist(
                directory_path=self.root_directory
            ):
                logger.warning(
                    f"Profiler store named '{profiler_store_name}' is not a configured store, so will try to use "
                    f"default Profiler store.\n  Please update your configuration to the new version number "
                    f"{float(CURRENT_GX_CONFIG_VERSION)} in order to use the new 'Profiler Store' feature.\n  "
                    f"Visit {AbstractDataContext.MIGRATION_WEBSITE} to learn more about the upgrade process."
                )
                built_store: Optional[Store] = self._build_store_from_config(
                    profiler_store_name,  # type: ignore[arg-type]
                    DataContextConfigDefaults.DEFAULT_STORES.value[profiler_store_name],  # type: ignore[index,arg-type]
                )
                return cast(ProfilerStore, built_store)

            raise ge_exceptions.StoreConfigurationError(
                f"Attempted to access the Profiler store: '{profiler_store_name}'. It is not a configured store."
            )

    @property
    def concurrency(self) -> Optional[ConcurrencyConfig]:
        return self.variables.concurrency

    @property
    def assistants(self) -> DataAssistantDispatcher:
        return self._assistants

    def set_config(self, project_config: DataContextConfig) -> None:
        self._project_config = project_config
        self.variables.config = project_config

    def save_datasource(
        self, datasource: Union[LegacyDatasource, BaseDatasource]
    ) -> Union[LegacyDatasource, BaseDatasource]:
        """Save a Datasource to the configured DatasourceStore.

        Stores the underlying DatasourceConfig in the store and Data Context config,
        updates the cached Datasource and returns the Datasource.
        The cached and returned Datasource is re-constructed from the config
        that was stored as some store implementations make edits to the stored
        config (e.g. adding identifiers).

        Args:
            datasource: Datasource to store.

        Returns:
            The datasource, after storing and retrieving the stored config.
        """
        # Chetan - 20221103 - Directly accessing private attr in order to patch security vulnerabiliy around credential leakage.
        # This is to be removed once substitution logic is migrated from the context to the individual object level.
        config = datasource._raw_config

        datasource_config_dict: dict = datasourceConfigSchema.dump(config)
        # Manually need to add in class name to the config since it is not part of the runtime obj
        datasource_config_dict["class_name"] = datasource.__class__.__name__

        datasource_config = datasourceConfigSchema.load(datasource_config_dict)
        datasource_name: str = datasource.name

        updated_datasource_config_from_store: DatasourceConfig = self._datasource_store.set(  # type: ignore[attr-defined]
            key=None, value=datasource_config
        )
        # Use the updated datasource config, since the store may populate additional info on update.
        self.config.datasources[datasource_name] = updated_datasource_config_from_store  # type: ignore[index,assignment]

        # Also use the updated config to initialize a datasource for the cache and overwrite the existing datasource.
        substituted_config = self._perform_substitutions_on_datasource_config(
            updated_datasource_config_from_store
        )
        updated_datasource: Union[
            LegacyDatasource, BaseDatasource
        ] = self._instantiate_datasource_from_config(
            raw_config=updated_datasource_config_from_store,
            substituted_config=substituted_config,
        )
        self._cached_datasources[datasource_name] = updated_datasource
        return updated_datasource

    @usage_statistics_enabled_method(
        event_name=UsageStatsEvents.DATA_CONTEXT_ADD_DATASOURCE,
        args_payload_fn=add_datasource_usage_statistics,
    )
    def add_datasource(
        self,
        name: str,
        initialize: bool = True,
        save_changes: Optional[bool] = None,
        **kwargs: Optional[dict],
    ) -> Optional[Union[LegacyDatasource, BaseDatasource]]:
        """Add a new datasource to the data context, with configuration provided as kwargs.
        Args:
            name: the name for the new datasource to add
            initialize: if False, add the datasource to the config, but do not
                initialize it, for example if a user needs to debug database connectivity.
            save_changes (bool): should GX save the Datasource config?
            kwargs (keyword arguments): the configuration for the new datasource

        Returns:
            datasource (Datasource)
        """
        save_changes = self._determine_save_changes_flag(save_changes)

        logger.debug(f"Starting BaseDataContext.add_datasource for {name}")

        module_name: str = kwargs.get("module_name", "great_expectations.datasource")  # type: ignore[assignment]
        verify_dynamic_loading_support(module_name=module_name)
        class_name: Optional[str] = kwargs.get("class_name")  # type: ignore[assignment]
        datasource_class = load_class(module_name=module_name, class_name=class_name)  # type: ignore[arg-type]

        # For any class that should be loaded, it may control its configuration construction
        # by implementing a classmethod called build_configuration
        config: Union[CommentedMap, dict]
        if hasattr(datasource_class, "build_configuration"):
            config = datasource_class.build_configuration(**kwargs)
        else:
            config = kwargs

        datasource_config: DatasourceConfig = datasourceConfigSchema.load(
            CommentedMap(**config)
        )
        datasource_config.name = name

        datasource: Optional[
            Union[LegacyDatasource, BaseDatasource]
        ] = self._instantiate_datasource_from_config_and_update_project_config(
            config=datasource_config,
            initialize=initialize,
            save_changes=save_changes,
        )
        return datasource

    def update_datasource(
        self,
        datasource: Union[LegacyDatasource, BaseDatasource],
        save_changes: Optional[bool] = None,
    ) -> None:
        """
        Updates a DatasourceConfig that already exists in the store.

        Args:
            datasource_config: The config object to persist using the DatasourceStore.
            save_changes: do I save changes to disk?
        """
        save_changes = self._determine_save_changes_flag(save_changes)

        datasource_config_dict: dict = datasourceConfigSchema.dump(datasource.config)
        datasource_config = DatasourceConfig(**datasource_config_dict)
        datasource_name: str = datasource.name

        if save_changes:
            self._datasource_store.update_by_name(  # type: ignore[attr-defined]
                datasource_name=datasource_name, datasource_config=datasource_config
            )
        self.config.datasources[datasource_name] = datasource_config  # type: ignore[assignment,index]
        self._cached_datasources[datasource_name] = datasource_config

    def get_site_names(self) -> List[str]:
        """Get a list of configured site names."""
        return list(self.variables.data_docs_sites.keys())  # type: ignore[union-attr]

    def get_config_with_variables_substituted(
        self, config: Optional[DataContextConfig] = None
    ) -> DataContextConfig:
        """
        Substitute vars in config of form ${var} or $(var) with values found in the following places,
        in order of precedence: ge_cloud_config (for Data Contexts in GX Cloud mode), runtime_environment,
        environment variables, config_variables, or ge_cloud_config_variable_defaults (allows certain variables to
        be optional in GX Cloud mode).
        """
        if not config:
            config = self._project_config
        return DataContextConfig(**self.config_provider.substitute_config(config))

    def get_batch(
        self, arg1: Any = None, arg2: Any = None, arg3: Any = None, **kwargs
    ) -> Union[Batch, DataAsset]:
        """Get exactly one batch, based on a variety of flexible input types.
        The method `get_batch` is the main user-facing method for getting batches; it supports both the new (V3) and the
        Legacy (V2) Datasource schemas.  The version-specific implementations are contained in "_get_batch_v2()" and
        "_get_batch_v3()", respectively, both of which are in the present module.

        For the V3 API parameters, please refer to the signature and parameter description of method "_get_batch_v3()".
        For the Legacy usage, please refer to the signature and parameter description of the method "_get_batch_v2()".

        Args:
            arg1: the first positional argument (can take on various types)
            arg2: the second positional argument (can take on various types)
            arg3: the third positional argument (can take on various types)

            **kwargs: variable arguments

        Returns:
            Batch (V3) or DataAsset (V2) -- the requested batch

        Processing Steps:
        1. Determine the version (possible values are "v3" or "v2").
        2. Convert the positional arguments to the appropriate named arguments, based on the version.
        3. Package the remaining arguments as variable keyword arguments (applies only to V3).
        4. Call the version-specific method ("_get_batch_v3()" or "_get_batch_v2()") with the appropriate arguments.
        """

        api_version: Optional[str] = self._get_data_context_version(arg1=arg1, **kwargs)
        if api_version == "v3":
            if "datasource_name" in kwargs:
                datasource_name = kwargs.pop("datasource_name", None)
            else:
                datasource_name = arg1
            if "data_connector_name" in kwargs:
                data_connector_name = kwargs.pop("data_connector_name", None)
            else:
                data_connector_name = arg2
            if "data_asset_name" in kwargs:
                data_asset_name = kwargs.pop("data_asset_name", None)
            else:
                data_asset_name = arg3
            return self._get_batch_v3(
                datasource_name=datasource_name,
                data_connector_name=data_connector_name,
                data_asset_name=data_asset_name,
                **kwargs,
            )
        if "batch_kwargs" in kwargs:
            batch_kwargs = kwargs.get("batch_kwargs", None)
        else:
            batch_kwargs = arg1
        if "expectation_suite_name" in kwargs:
            expectation_suite_name = kwargs.get("expectation_suite_name", None)
        else:
            expectation_suite_name = arg2
        if "data_asset_type" in kwargs:
            data_asset_type = kwargs.get("data_asset_type", None)
        else:
            data_asset_type = arg3
        batch_parameters = kwargs.get("batch_parameters")
        return self._get_batch_v2(
            batch_kwargs=batch_kwargs,
            expectation_suite_name=expectation_suite_name,
            data_asset_type=data_asset_type,
            batch_parameters=batch_parameters,
        )

    def _get_data_context_version(self, arg1: Any, **kwargs) -> Optional[str]:
        """
        arg1: the first positional argument (can take on various types)

        **kwargs: variable arguments

        First check:
        Returns "v3" if the "0.13" entities are specified in the **kwargs.

        Otherwise:
        Returns None if no datasources have been configured (or if there is an exception while getting the datasource).
        Returns "v3" if the datasource is a subclass of the BaseDatasource class.
        Returns "v2" if the datasource is an instance of the LegacyDatasource class.
        """

        if {
            "datasource_name",
            "data_connector_name",
            "data_asset_name",
            "batch_request",
            "batch_data",
        }.intersection(set(kwargs.keys())):
            return "v3"

        if not self.datasources:
            return None

        api_version: Optional[str] = None
        datasource_name: Any
        if "datasource_name" in kwargs:
            datasource_name = kwargs.pop("datasource_name", None)
        else:
            datasource_name = arg1
        try:
            datasource: Union[LegacyDatasource, BaseDatasource] = self.get_datasource(  # type: ignore[assignment]
                datasource_name=datasource_name
            )
            if issubclass(type(datasource), BaseDatasource):
                api_version = "v3"
        except (ValueError, TypeError):
            if "batch_kwargs" in kwargs:
                batch_kwargs = kwargs.get("batch_kwargs", None)
            else:
                batch_kwargs = arg1
            if isinstance(batch_kwargs, dict):
                datasource_name = batch_kwargs.get("datasource")
                if datasource_name is not None:
                    try:
                        datasource: Union[  # type: ignore[no-redef]
                            LegacyDatasource, BaseDatasource
                        ] = self.get_datasource(datasource_name=datasource_name)
                        if isinstance(datasource, LegacyDatasource):
                            api_version = "v2"
                    except (ValueError, TypeError):
                        pass
        return api_version

    def _get_batch_v2(
        self,
        batch_kwargs: Union[dict, BatchKwargs],
        expectation_suite_name: Union[str, ExpectationSuite],
        data_asset_type=None,
        batch_parameters=None,
    ) -> DataAsset:
        """Build a batch of data using batch_kwargs, and return a DataAsset with expectation_suite_name attached. If
        batch_parameters are included, they will be available as attributes of the batch.
        Args:
            batch_kwargs: the batch_kwargs to use; must include a datasource key
            expectation_suite_name: The ExpectationSuite or the name of the expectation_suite to get
            data_asset_type: the type of data_asset to build, with associated expectation implementations. This can
                generally be inferred from the datasource.
            batch_parameters: optional parameters to store as the reference description of the batch. They should
                reflect parameters that would provide the passed BatchKwargs.
        Returns:
            DataAsset
        """
        if isinstance(batch_kwargs, dict):
            batch_kwargs = BatchKwargs(batch_kwargs)

        if not isinstance(batch_kwargs, BatchKwargs):
            raise ge_exceptions.BatchKwargsError(
                "BatchKwargs must be a BatchKwargs object or dictionary."
            )

        if not isinstance(
            expectation_suite_name, (ExpectationSuite, ExpectationSuiteIdentifier, str)
        ):
            raise ge_exceptions.DataContextError(
                "expectation_suite_name must be an ExpectationSuite, "
                "ExpectationSuiteIdentifier or string."
            )

        if isinstance(expectation_suite_name, ExpectationSuite):
            expectation_suite = expectation_suite_name
        elif isinstance(expectation_suite_name, ExpectationSuiteIdentifier):
            expectation_suite = self.get_expectation_suite(
                expectation_suite_name.expectation_suite_name
            )
        else:
            expectation_suite = self.get_expectation_suite(expectation_suite_name)

        datasource = self.get_datasource(batch_kwargs.get("datasource"))  # type: ignore[arg-type]
        batch = datasource.get_batch(  # type: ignore[union-attr]
            batch_kwargs=batch_kwargs, batch_parameters=batch_parameters
        )
        if data_asset_type is None:
            data_asset_type = datasource.config.get("data_asset_type")  # type: ignore[union-attr]

        validator = BridgeValidator(
            batch=batch,
            expectation_suite=expectation_suite,
            expectation_engine=data_asset_type,
        )
        return validator.get_dataset()

    def _get_batch_v3(
        self,
        datasource_name: Optional[str] = None,
        data_connector_name: Optional[str] = None,
        data_asset_name: Optional[str] = None,
        *,
        batch_request: Optional[BatchRequestBase] = None,
        batch_data: Optional[Any] = None,
        data_connector_query: Optional[Union[IDDict, dict]] = None,
        batch_identifiers: Optional[dict] = None,
        limit: Optional[int] = None,
        index: Optional[Union[int, list, tuple, slice, str]] = None,
        custom_filter_function: Optional[Callable] = None,
        batch_spec_passthrough: Optional[dict] = None,
        sampling_method: Optional[str] = None,
        sampling_kwargs: Optional[dict] = None,
        splitter_method: Optional[str] = None,
        splitter_kwargs: Optional[dict] = None,
        runtime_parameters: Optional[dict] = None,
        query: Optional[str] = None,
        path: Optional[str] = None,
        batch_filter_parameters: Optional[dict] = None,
        **kwargs,
    ) -> Union[Batch, DataAsset]:
        """Get exactly one batch, based on a variety of flexible input types.

        Args:
            datasource_name
            data_connector_name
            data_asset_name

            batch_request
            batch_data
            data_connector_query
            batch_identifiers
            batch_filter_parameters

            limit
            index
            custom_filter_function

            batch_spec_passthrough

            sampling_method
            sampling_kwargs

            splitter_method
            splitter_kwargs

            **kwargs

        Returns:
            (Batch) The requested batch

        This method does not require typed or nested inputs.
        Instead, it is intended to help the user pick the right parameters.

        This method attempts to return exactly one batch.
        If 0 or more than 1 batches would be returned, it raises an error.
        """
        batch_list: List[Batch] = self.get_batch_list(
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
            batch_spec_passthrough=batch_spec_passthrough,
            sampling_method=sampling_method,
            sampling_kwargs=sampling_kwargs,
            splitter_method=splitter_method,
            splitter_kwargs=splitter_kwargs,
            runtime_parameters=runtime_parameters,
            query=query,
            path=path,
            batch_filter_parameters=batch_filter_parameters,
            **kwargs,
        )
        # NOTE: Alex 20201202 - The check below is duplicate of code in Datasource.get_single_batch_from_batch_request()
        # deprecated-v0.13.20
        warnings.warn(
            "get_batch is deprecated for the V3 Batch Request API as of v0.13.20 and will be removed in v0.16. Please use "
            "get_batch_list instead.",
            DeprecationWarning,
        )
        if len(batch_list) != 1:
            raise ValueError(
                f"Got {len(batch_list)} batches instead of a single batch. If you would like to use a BatchRequest to "
                f"return multiple batches, please use get_batch_list directly instead of calling get_batch"
            )
        return batch_list[0]

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
            validations_store_name,
            evaluation_parameter_store_name,
            checkpoint_store_name
            profiler_store_name
        """
        active_store_names: List[str] = [
            self.expectations_store_name,  # type: ignore[list-item]
            self.validations_store_name,  # type: ignore[list-item]
            self.evaluation_parameter_store_name,  # type: ignore[list-item]
        ]

        try:
            active_store_names.append(self.checkpoint_store_name)  # type: ignore[arg-type]
        except (AttributeError, ge_exceptions.InvalidTopLevelConfigKeyError):
            logger.info(
                "Checkpoint store is not configured; omitting it from active stores"
            )

        try:
            active_store_names.append(self.profiler_store_name)  # type: ignore[arg-type]
        except (AttributeError, ge_exceptions.InvalidTopLevelConfigKeyError):
            logger.info(
                "Profiler store is not configured; omitting it from active stores"
            )

        return [
            store
            for store in self.list_stores()
            if store.get("name") in active_store_names  # type: ignore[arg-type,operator]
        ]

    def list_checkpoints(self) -> Union[List[str], List[ConfigurationIdentifier]]:
        return self.checkpoint_store.list_checkpoints()

    def list_profilers(self) -> Union[List[str], List[ConfigurationIdentifier]]:
        return RuleBasedProfiler.list_profilers(self.profiler_store)

    def save_profiler(
        self,
        profiler: RuleBasedProfiler,
    ) -> RuleBasedProfiler:
        name = profiler.name
        ge_cloud_id = profiler.ge_cloud_id
        key = self._determine_key_for_profiler_save(name=name, id=ge_cloud_id)

        response = self.profiler_store.set(key=key, value=profiler.config)  # type: ignore[func-returns-value]
        if isinstance(response, GXCloudResourceRef):
            ge_cloud_id = response.cloud_id

        # If an id is present, we want to prioritize that as our key for object retrieval
        if ge_cloud_id:
            name = None  # type: ignore[assignment]

        profiler = self.get_profiler(name=name, ge_cloud_id=ge_cloud_id)
        return profiler

    def _determine_key_for_profiler_save(
        self, name: str, id: Optional[str]
    ) -> Union[ConfigurationIdentifier, GXCloudIdentifier]:
        return ConfigurationIdentifier(configuration_key=name)

    def get_datasource(
        self, datasource_name: str = "default"
    ) -> Optional[Union[LegacyDatasource, BaseDatasource]]:
        """Get the named datasource

        Args:
            datasource_name (str): the name of the datasource from the configuration

        Returns:
            datasource (Datasource)
        """
        if datasource_name is None:
            raise ValueError(
                "Must provide a datasource_name to retrieve an existing Datasource"
            )

        if datasource_name in self._cached_datasources:
            return self._cached_datasources[datasource_name]

        datasource_config: DatasourceConfig = self._datasource_store.retrieve_by_name(  # type: ignore[attr-defined]
            datasource_name=datasource_name
        )

        raw_config_dict: dict = dict(datasourceConfigSchema.dump(datasource_config))
        raw_config = datasourceConfigSchema.load(raw_config_dict)

        substituted_config = self.config_provider.substitute_config(raw_config_dict)

        # Instantiate the datasource and add to our in-memory cache of datasources, this does not persist:
        datasource_config = datasourceConfigSchema.load(substituted_config)
        datasource: Optional[
            Union[LegacyDatasource, BaseDatasource]
        ] = self._instantiate_datasource_from_config(
            raw_config=raw_config, substituted_config=substituted_config
        )
        self._cached_datasources[datasource_name] = datasource
        return datasource

    def _serialize_substitute_and_sanitize_datasource_config(
        self, serializer: AbstractConfigSerializer, datasource_config: DatasourceConfig
    ) -> dict:
        """Serialize, then make substitutions and sanitize config (mask passwords), return as dict.

        Args:
            serializer: Serializer to use when converting config to dict for substitutions.
            datasource_config: Datasource config to process.

        Returns:
            Dict of config with substitutions and sanitizations applied.
        """
        datasource_dict: dict = serializer.serialize(datasource_config)

        substituted_config = cast(
            dict, self.config_provider.substitute_config(datasource_dict)
        )
        masked_config: dict = PasswordMasker.sanitize_config(substituted_config)
        return masked_config

    def add_store(self, store_name: str, store_config: dict) -> Optional[Store]:
        """Add a new Store to the DataContext and (for convenience) return the instantiated Store object.

        Args:
            store_name (str): a key for the new Store in in self._stores
            store_config (dict): a config for the Store to add

        Returns:
            store (Store)
        """

        self.config.stores[store_name] = store_config  # type: ignore[index]
        return self._build_store_from_config(store_name, store_config)

    def list_datasources(self) -> List[dict]:
        """List currently-configured datasources on this context. Masks passwords.

        Returns:
            List(dict): each dictionary includes "name", "class_name", and "module_name" keys
        """
        datasources: List[dict] = []

        datasource_name: str
        datasource_config: Union[dict, DatasourceConfig]
        serializer = NamedDatasourceSerializer(schema=datasourceConfigSchema)

        for datasource_name, datasource_config in self.config.datasources.items():  # type: ignore[union-attr]
            if isinstance(datasource_config, dict):
                datasource_config = DatasourceConfig(**datasource_config)
            datasource_config.name = datasource_name

            masked_config: dict = (
                self._serialize_substitute_and_sanitize_datasource_config(
                    serializer, datasource_config
                )
            )
            datasources.append(masked_config)
        return datasources

    def delete_datasource(
        self, datasource_name: Optional[str], save_changes: Optional[bool] = None
    ) -> None:
        """Delete a datasource
        Args:
            datasource_name: The name of the datasource to delete.

        Raises:
            ValueError: If the datasource name isn't provided or cannot be found.
        """
        save_changes = self._determine_save_changes_flag(save_changes)

        if not datasource_name:
            raise ValueError("Datasource names must be a datasource name")

        datasource = self.get_datasource(datasource_name=datasource_name)

        if datasource is None:
            raise ValueError(f"Datasource {datasource_name} not found")

        if save_changes:
            datasource_config = datasourceConfigSchema.load(datasource.config)
            self._datasource_store.delete(datasource_config)  # type: ignore[attr-defined]
        self._cached_datasources.pop(datasource_name, None)
        self.config.datasources.pop(datasource_name, None)  # type: ignore[union-attr]

    def add_checkpoint(
        self,
        name: str,
        config_version: Optional[Union[int, float]] = None,
        template_name: Optional[str] = None,
        module_name: Optional[str] = None,
        class_name: Optional[str] = None,
        run_name_template: Optional[str] = None,
        expectation_suite_name: Optional[str] = None,
        batch_request: Optional[dict] = None,
        action_list: Optional[List[dict]] = None,
        evaluation_parameters: Optional[dict] = None,
        runtime_configuration: Optional[dict] = None,
        validations: Optional[List[dict]] = None,
        profilers: Optional[List[dict]] = None,
        # Next two fields are for LegacyCheckpoint configuration
        validation_operator_name: Optional[str] = None,
        batches: Optional[List[dict]] = None,
        # the following four arguments are used by SimpleCheckpoint
        site_names: Optional[Union[str, List[str]]] = None,
        slack_webhook: Optional[str] = None,
        notify_on: Optional[str] = None,
        notify_with: Optional[Union[str, List[str]]] = None,
        ge_cloud_id: Optional[str] = None,
        expectation_suite_ge_cloud_id: Optional[str] = None,
        default_validation_id: Optional[str] = None,
    ) -> Checkpoint:

        from great_expectations.checkpoint.checkpoint import Checkpoint

        checkpoint: Checkpoint = Checkpoint.construct_from_config_args(
            data_context=self,
            checkpoint_store_name=self.checkpoint_store_name,  # type: ignore[arg-type]
            name=name,
            config_version=config_version,
            template_name=template_name,
            module_name=module_name,
            class_name=class_name,
            run_name_template=run_name_template,
            expectation_suite_name=expectation_suite_name,
            batch_request=batch_request,
            action_list=action_list,
            evaluation_parameters=evaluation_parameters,
            runtime_configuration=runtime_configuration,
            validations=validations,
            profilers=profilers,
            # Next two fields are for LegacyCheckpoint configuration
            validation_operator_name=validation_operator_name,
            batches=batches,
            # the following four arguments are used by SimpleCheckpoint
            site_names=site_names,
            slack_webhook=slack_webhook,
            notify_on=notify_on,
            notify_with=notify_with,
            ge_cloud_id=ge_cloud_id,
            expectation_suite_ge_cloud_id=expectation_suite_ge_cloud_id,
            default_validation_id=default_validation_id,
        )

        self.checkpoint_store.add_checkpoint(checkpoint, name, ge_cloud_id)
        return checkpoint

    def get_checkpoint(
        self,
        name: Optional[str] = None,
        ge_cloud_id: Optional[str] = None,
    ) -> Checkpoint:
        from great_expectations.checkpoint.checkpoint import Checkpoint

        checkpoint_config: CheckpointConfig = self.checkpoint_store.get_checkpoint(
            name=name, ge_cloud_id=ge_cloud_id
        )
        checkpoint: Checkpoint = Checkpoint.instantiate_from_config_with_runtime_args(
            checkpoint_config=checkpoint_config,
            data_context=self,
            name=name,
        )

        return checkpoint

    def delete_checkpoint(
        self,
        name: Optional[str] = None,
        ge_cloud_id: Optional[str] = None,
    ) -> None:
        return self.checkpoint_store.delete_checkpoint(
            name=name, ge_cloud_id=ge_cloud_id
        )

    @usage_statistics_enabled_method(
        event_name=UsageStatsEvents.DATA_CONTEXT_RUN_CHECKPOINT,
    )
    def run_checkpoint(
        self,
        checkpoint_name: Optional[str] = None,
        ge_cloud_id: Optional[str] = None,
        template_name: Optional[str] = None,
        run_name_template: Optional[str] = None,
        expectation_suite_name: Optional[str] = None,
        batch_request: Optional[BatchRequestBase] = None,
        action_list: Optional[List[dict]] = None,
        evaluation_parameters: Optional[dict] = None,
        runtime_configuration: Optional[dict] = None,
        validations: Optional[List[dict]] = None,
        profilers: Optional[List[dict]] = None,
        run_id: Optional[Union[str, int, float]] = None,
        run_name: Optional[str] = None,
        run_time: Optional[datetime.datetime] = None,
        result_format: Optional[str] = None,
        expectation_suite_ge_cloud_id: Optional[str] = None,
        **kwargs,
    ) -> CheckpointResult:
        """
        Validate against a pre-defined Checkpoint. (Experimental)

        Args:
            checkpoint_name: The name of a Checkpoint defined via the CLI or by manually creating a yml file
            template_name: The name of a Checkpoint template to retrieve from the CheckpointStore
            run_name_template: The template to use for run_name
            expectation_suite_name: Expectation suite to be used by Checkpoint run
            batch_request: Batch request to be used by Checkpoint run
            action_list: List of actions to be performed by the Checkpoint
            evaluation_parameters: $parameter_name syntax references to be evaluated at runtime
            runtime_configuration: Runtime configuration override parameters
            validations: Validations to be performed by the Checkpoint run
            profilers: Profilers to be used by the Checkpoint run
            run_id: The run_id for the validation; if None, a default value will be used
            run_name: The run_name for the validation; if None, a default value will be used
            run_time: The date/time of the run
            result_format: One of several supported formatting directives for expectation validation results
            ge_cloud_id: Great Expectations Cloud id for the checkpoint
            expectation_suite_ge_cloud_id: Great Expectations Cloud id for the expectation suite
            **kwargs: Additional kwargs to pass to the validation operator

        Returns:
            CheckpointResult
        """
        checkpoint: Checkpoint = self.get_checkpoint(
            name=checkpoint_name,
            ge_cloud_id=ge_cloud_id,
        )
        result: CheckpointResult = checkpoint.run_with_runtime_args(
            template_name=template_name,
            run_name_template=run_name_template,
            expectation_suite_name=expectation_suite_name,
            batch_request=batch_request,
            action_list=action_list,
            evaluation_parameters=evaluation_parameters,
            runtime_configuration=runtime_configuration,
            validations=validations,
            profilers=profilers,
            run_id=run_id,
            run_name=run_name,
            run_time=run_time,
            result_format=result_format,
            expectation_suite_ge_cloud_id=expectation_suite_ge_cloud_id,
            **kwargs,
        )
        return result

    def store_evaluation_parameters(
        self, validation_results, target_store_name=None
    ) -> None:
        """
        Stores ValidationResult EvaluationParameters to defined store
        """
        if not self._evaluation_parameter_dependencies_compiled:
            self._compile_evaluation_parameter_dependencies()

        if target_store_name is None:
            target_store_name = self.evaluation_parameter_store_name

        self._store_metrics(
            self._evaluation_parameter_dependencies,
            validation_results,
            target_store_name,
        )

    def list_expectation_suite_names(self) -> List[str]:
        """
        Lists the available expectation suite names.
        """
        sorted_expectation_suite_names = [
            i.expectation_suite_name for i in self.list_expectation_suites()  # type: ignore[union-attr]
        ]
        sorted_expectation_suite_names.sort()
        return sorted_expectation_suite_names

    def list_expectation_suites(
        self,
    ) -> Optional[Union[List[str], List[GXCloudIdentifier]]]:
        """Return a list of available expectation suite keys."""
        try:
            keys = self.expectations_store.list_keys()
        except KeyError as e:
            raise ge_exceptions.InvalidConfigError(
                f"Unable to find configured store: {str(e)}"
            )
        return keys  # type: ignore[return-value]

    def get_validator(
        self,
        datasource_name: Optional[str] = None,
        data_connector_name: Optional[str] = None,
        data_asset_name: Optional[str] = None,
        batch: Optional[Batch] = None,
        batch_list: Optional[List[Batch]] = None,
        batch_request: Optional[BatchRequestBase] = None,
        batch_request_list: Optional[List[BatchRequestBase]] = None,
        batch_data: Optional[Any] = None,
        data_connector_query: Optional[Union[IDDict, dict]] = None,
        batch_identifiers: Optional[dict] = None,
        limit: Optional[int] = None,
        index: Optional[Union[int, list, tuple, slice, str]] = None,
        custom_filter_function: Optional[Callable] = None,
        sampling_method: Optional[str] = None,
        sampling_kwargs: Optional[dict] = None,
        splitter_method: Optional[str] = None,
        splitter_kwargs: Optional[dict] = None,
        runtime_parameters: Optional[dict] = None,
        query: Optional[str] = None,
        path: Optional[str] = None,
        batch_filter_parameters: Optional[dict] = None,
        expectation_suite_ge_cloud_id: Optional[str] = None,
        batch_spec_passthrough: Optional[dict] = None,
        expectation_suite_name: Optional[str] = None,
        expectation_suite: Optional[ExpectationSuite] = None,
        create_expectation_suite_with_name: Optional[str] = None,
        include_rendered_content: Optional[bool] = None,
        **kwargs: Optional[dict],
    ) -> Validator:
        """
        This method applies only to the new (V3) Datasource schema.
        """

        include_rendered_content = (
            self._determine_if_expectation_validation_result_include_rendered_content(
                include_rendered_content=include_rendered_content
            )
        )

        if (
            sum(
                bool(x)
                for x in [
                    expectation_suite is not None,
                    expectation_suite_name is not None,
                    create_expectation_suite_with_name is not None,
                    expectation_suite_ge_cloud_id is not None,
                ]
            )
            > 1
        ):
            ge_cloud_mode = getattr(  # attr not on AbstractDataContext
                self, "ge_cloud_mode"
            )
            raise ValueError(
                "No more than one of expectation_suite_name,"
                f"{'expectation_suite_ge_cloud_id,' if ge_cloud_mode else ''}"
                " expectation_suite, or create_expectation_suite_with_name can be specified"
            )

        if expectation_suite_ge_cloud_id is not None:
            expectation_suite = self.get_expectation_suite(
                include_rendered_content=include_rendered_content,
                ge_cloud_id=expectation_suite_ge_cloud_id,
            )
        if expectation_suite_name is not None:
            expectation_suite = self.get_expectation_suite(
                expectation_suite_name,
                include_rendered_content=include_rendered_content,
            )
        if create_expectation_suite_with_name is not None:
            expectation_suite = self.create_expectation_suite(
                expectation_suite_name=create_expectation_suite_with_name,
            )

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
            raise ValueError(
                "No more than one of batch, batch_list, batch_request, or batch_request_list can be specified"
            )

        if batch_list:
            pass

        elif batch:
            batch_list = [batch]

        else:
            batch_list = []
            if not batch_request_list:
                batch_request_list = [batch_request]  # type: ignore[list-item]

            for batch_request in batch_request_list:
                batch_list.extend(
                    self.get_batch_list(
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
                        splitter_method=splitter_method,
                        splitter_kwargs=splitter_kwargs,
                        runtime_parameters=runtime_parameters,
                        query=query,
                        path=path,
                        batch_filter_parameters=batch_filter_parameters,
                        batch_spec_passthrough=batch_spec_passthrough,
                        **kwargs,
                    )
                )

        return self.get_validator_using_batch_list(
            expectation_suite=expectation_suite,  # type: ignore[arg-type]
            batch_list=batch_list,
            include_rendered_content=include_rendered_content,
        )

    # noinspection PyUnusedLocal
    def get_validator_using_batch_list(
        self,
        expectation_suite: ExpectationSuite,
        batch_list: Sequence[Union[Batch, XBatch]],
        include_rendered_content: Optional[bool] = None,
        **kwargs: Optional[dict],
    ) -> Validator:
        """

        Args:
            expectation_suite ():
            batch_list ():
            include_rendered_content ():
            **kwargs ():

        Returns:

        """
        if len(batch_list) == 0:
            raise ge_exceptions.InvalidBatchRequestError(
                """Validator could not be created because BatchRequest returned an empty batch_list.
                Please check your parameters and try again."""
            )

        include_rendered_content = (
            self._determine_if_expectation_validation_result_include_rendered_content(
                include_rendered_content=include_rendered_content
            )
        )

        # We get a single batch_definition so we can get the execution_engine here. All batches will share the same one
        # So the batch itself doesn't matter. But we use -1 because that will be the latest batch loaded.
        execution_engine: ExecutionEngine
        if hasattr(batch_list[-1], "execution_engine"):
            # 'XBatch's are execution engine aware. We just checked for this attr so we ignore the following
            # attr defined mypy error
            execution_engine = batch_list[-1].execution_engine
        else:
            execution_engine = self.datasources[  # type: ignore[union-attr]
                batch_list[-1].batch_definition.datasource_name
            ].execution_engine

        validator = Validator(
            execution_engine=execution_engine,
            interactive_evaluation=True,
            expectation_suite=expectation_suite,
            data_context=self,
            batches=batch_list,
            include_rendered_content=include_rendered_content,
        )

        return validator

    @usage_statistics_enabled_method(
        event_name=UsageStatsEvents.DATA_CONTEXT_GET_BATCH_LIST,
        args_payload_fn=get_batch_list_usage_statistics,
    )
    def get_batch_list(
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
        splitter_method: Optional[str] = None,
        splitter_kwargs: Optional[dict] = None,
        runtime_parameters: Optional[dict] = None,
        query: Optional[str] = None,
        path: Optional[str] = None,
        batch_filter_parameters: Optional[dict] = None,
        batch_spec_passthrough: Optional[dict] = None,
        **kwargs: Optional[dict],
    ) -> List[Batch]:
        """Get the list of zero or more batches, based on a variety of flexible input types.
        This method applies only to the new (V3) Datasource schema.

        Args:
            batch_request

            datasource_name
            data_connector_name
            data_asset_name

            batch_request
            batch_data
            query
            path
            runtime_parameters
            data_connector_query
            batch_identifiers
            batch_filter_parameters

            limit
            index
            custom_filter_function

            sampling_method
            sampling_kwargs

            splitter_method
            splitter_kwargs

            batch_spec_passthrough

            **kwargs

        Returns:
            (Batch) The requested batch

        `get_batch` is the main user-facing API for getting batches.
        In contrast to virtually all other methods in the class, it does not require typed or nested inputs.
        Instead, this method is intended to help the user pick the right parameters

        This method attempts to return any number of batches, including an empty list.
        """

        batch_request = get_batch_request_from_acceptable_arguments(
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
            splitter_method=splitter_method,
            splitter_kwargs=splitter_kwargs,
            runtime_parameters=runtime_parameters,
            query=query,
            path=path,
            batch_filter_parameters=batch_filter_parameters,
            batch_spec_passthrough=batch_spec_passthrough,
            **kwargs,
        )
        datasource_name = batch_request.datasource_name
        if datasource_name in self.datasources:
            datasource: Datasource = cast(Datasource, self.datasources[datasource_name])
        else:
            raise ge_exceptions.DatasourceError(
                datasource_name,
                "The given datasource could not be retrieved from the DataContext; "
                "please confirm that your configuration is accurate.",
            )
        return datasource.get_batch_list_from_batch_request(batch_request=batch_request)

    def create_expectation_suite(
        self,
        expectation_suite_name: str,
        overwrite_existing: bool = False,
        **kwargs: Optional[dict],
    ) -> ExpectationSuite:
        """Build a new expectation suite and save it into the data_context expectation store.

        Args:
            expectation_suite_name: The name of the expectation_suite to create
            overwrite_existing (boolean): Whether to overwrite expectation suite if expectation suite with given name
                already exists.

        Returns:
            A new (empty) expectation suite.
        """
        if not isinstance(overwrite_existing, bool):
            raise ValueError("Parameter overwrite_existing must be of type BOOL")

        expectation_suite = ExpectationSuite(
            expectation_suite_name=expectation_suite_name, data_context=self
        )
        key = ExpectationSuiteIdentifier(expectation_suite_name=expectation_suite_name)
        if (
            self.expectations_store.has_key(key)  # noqa: W601
            and not overwrite_existing
        ):
            raise ge_exceptions.DataContextError(
                "expectation_suite with name {} already exists. If you would like to overwrite this "
                "expectation_suite, set overwrite_existing=True.".format(
                    expectation_suite_name
                )
            )
        self.expectations_store.set(key, expectation_suite, **kwargs)
        return expectation_suite

    def delete_expectation_suite(
        self,
        expectation_suite_name: Optional[str] = None,
        ge_cloud_id: Optional[str] = None,
    ) -> bool:
        """Delete specified expectation suite from data_context expectation store.

        Args:
            expectation_suite_name: The name of the expectation_suite to create

        Returns:
            True for Success and False for Failure.
        """
        key = ExpectationSuiteIdentifier(expectation_suite_name)  # type: ignore[arg-type]
        if not self.expectations_store.has_key(key):  # noqa: W601
            raise ge_exceptions.DataContextError(
                "expectation_suite with name {} does not exist."
            )
        else:
            self.expectations_store.remove_key(key)
            return True

    def get_expectation_suite(
        self,
        expectation_suite_name: Optional[str] = None,
        include_rendered_content: Optional[bool] = None,
        ge_cloud_id: Optional[str] = None,
    ) -> ExpectationSuite:
        """Get an Expectation Suite by name or GX Cloud ID
        Args:
            expectation_suite_name (str): The name of the Expectation Suite
            include_rendered_content (bool): Whether or not to re-populate rendered_content for each
                ExpectationConfiguration.
            ge_cloud_id (str): The GX Cloud ID for the Expectation Suite.

        Returns:
            An existing ExpectationSuite
        """
        key: Optional[ExpectationSuiteIdentifier] = ExpectationSuiteIdentifier(
            expectation_suite_name=expectation_suite_name  # type: ignore[arg-type]
        )

        if include_rendered_content is None:
            include_rendered_content = (
                self._determine_if_expectation_suite_include_rendered_content()
            )

        if self.expectations_store.has_key(key):  # type: ignore[arg-type] # noqa: W601
            expectations_schema_dict: dict = cast(
                dict, self.expectations_store.get(key)
            )
            # create the ExpectationSuite from constructor
            expectation_suite = ExpectationSuite(
                **expectations_schema_dict, data_context=self
            )
            if include_rendered_content:
                expectation_suite.render()
            return expectation_suite

        else:
            raise ge_exceptions.DataContextError(
                f"expectation_suite {expectation_suite_name} not found"
            )

    def add_profiler(
        self,
        name: str,
        config_version: float,
        rules: Dict[str, dict],
        variables: Optional[dict] = None,
    ) -> RuleBasedProfiler:
        config_data = {
            "name": name,
            "config_version": config_version,
            "rules": rules,
            "variables": variables,
        }

        # Roundtrip through schema validation to remove any illegal fields add/or restore any missing fields.
        validated_config: dict = ruleBasedProfilerConfigSchema.load(config_data)
        profiler_config: dict = ruleBasedProfilerConfigSchema.dump(validated_config)
        profiler_config.pop("class_name")
        profiler_config.pop("module_name")

        config = RuleBasedProfilerConfig(**profiler_config)

        profiler = RuleBasedProfiler.add_profiler(
            config=config,
            data_context=self,
            profiler_store=self.profiler_store,
        )
        return profiler

    def get_profiler(
        self,
        name: Optional[str] = None,
        ge_cloud_id: Optional[str] = None,
    ) -> RuleBasedProfiler:
        return RuleBasedProfiler.get_profiler(
            data_context=self,
            profiler_store=self.profiler_store,
            name=name,
            ge_cloud_id=ge_cloud_id,
        )

    def delete_profiler(
        self,
        name: Optional[str] = None,
        ge_cloud_id: Optional[str] = None,
    ) -> None:
        RuleBasedProfiler.delete_profiler(
            profiler_store=self.profiler_store,
            name=name,
            ge_cloud_id=ge_cloud_id,
        )

    @usage_statistics_enabled_method(
        event_name=UsageStatsEvents.DATA_CONTEXT_RUN_RULE_BASED_PROFILER_WITH_DYNAMIC_ARGUMENTS,
    )
    def run_profiler_with_dynamic_arguments(
        self,
        batch_list: Optional[List[Batch]] = None,
        batch_request: Optional[Union[BatchRequestBase, dict]] = None,
        name: Optional[str] = None,
        ge_cloud_id: Optional[str] = None,
        variables: Optional[dict] = None,
        rules: Optional[dict] = None,
    ) -> RuleBasedProfilerResult:
        """Retrieve a RuleBasedProfiler from a ProfilerStore and run it with rules/variables supplied at runtime.

        Args:
            batch_list: Explicit list of Batch objects to supply data at runtime
            batch_request: Explicit batch_request used to supply data at runtime
            name: Identifier used to retrieve the profiler from a store.
            ge_cloud_id: Identifier used to retrieve the profiler from a store (GX Cloud specific).
            variables: Attribute name/value pairs (overrides)
            rules: Key-value pairs of name/configuration-dictionary (overrides)

        Returns:
            Set of rule evaluation results in the form of an RuleBasedProfilerResult

        Raises:
            AssertionError if both a `name` and `ge_cloud_id` are provided.
            AssertionError if both an `expectation_suite` and `expectation_suite_name` are provided.
        """
        return RuleBasedProfiler.run_profiler(
            data_context=self,
            profiler_store=self.profiler_store,
            batch_list=batch_list,
            batch_request=batch_request,
            name=name,
            ge_cloud_id=ge_cloud_id,
            variables=variables,
            rules=rules,
        )

    @usage_statistics_enabled_method(
        event_name=UsageStatsEvents.DATA_CONTEXT_RUN_RULE_BASED_PROFILER_ON_DATA,
    )
    def run_profiler_on_data(
        self,
        batch_list: Optional[List[Batch]] = None,
        batch_request: Optional[BatchRequestBase] = None,
        name: Optional[str] = None,
        ge_cloud_id: Optional[str] = None,
    ) -> RuleBasedProfilerResult:
        """Retrieve a RuleBasedProfiler from a ProfilerStore and run it with a batch request supplied at runtime.

        Args:
            batch_list: Explicit list of Batch objects to supply data at runtime.
            batch_request: Explicit batch_request used to supply data at runtime.
            name: Identifier used to retrieve the profiler from a store.
            ge_cloud_id: Identifier used to retrieve the profiler from a store (GX Cloud specific).

        Returns:
            Set of rule evaluation results in the form of an RuleBasedProfilerResult

        Raises:
            ProfilerConfigurationError is both "batch_list" and "batch_request" arguments are specified.
            AssertionError if both a `name` and `ge_cloud_id` are provided.
            AssertionError if both an `expectation_suite` and `expectation_suite_name` are provided.
        """
        return RuleBasedProfiler.run_profiler_on_data(
            data_context=self,
            profiler_store=self.profiler_store,
            batch_list=batch_list,
            batch_request=batch_request,
            name=name,
            ge_cloud_id=ge_cloud_id,
        )

    def add_validation_operator(
        self, validation_operator_name: str, validation_operator_config: dict
    ) -> ValidationOperator:
        """Add a new ValidationOperator to the DataContext and (for convenience) return the instantiated object.

        Args:
            validation_operator_name (str): a key for the new ValidationOperator in in self._validation_operators
            validation_operator_config (dict): a config for the ValidationOperator to add

        Returns:
            validation_operator (ValidationOperator)
        """

        self.config.validation_operators[
            validation_operator_name
        ] = validation_operator_config
        config = self.variables.validation_operators[validation_operator_name]  # type: ignore[index]
        module_name = "great_expectations.validation_operators"
        new_validation_operator = instantiate_class_from_config(
            config=config,
            runtime_environment={
                "data_context": self,
                "name": validation_operator_name,
            },
            config_defaults={"module_name": module_name},
        )
        if not new_validation_operator:
            raise ge_exceptions.ClassInstantiationError(
                module_name=module_name,
                package_name=None,
                class_name=config["class_name"],
            )
        self.validation_operators[validation_operator_name] = new_validation_operator
        return new_validation_operator

    @usage_statistics_enabled_method(
        event_name=UsageStatsEvents.DATA_CONTEXT_RUN_VALIDATION_OPERATOR,
        args_payload_fn=run_validation_operator_usage_statistics,
    )
    def run_validation_operator(
        self,
        validation_operator_name: str,
        assets_to_validate: List,
        run_id: Optional[Union[str, RunIdentifier]] = None,
        evaluation_parameters: Optional[dict] = None,
        run_name: Optional[str] = None,
        run_time: Optional[Union[str, datetime.datetime]] = None,
        result_format: Optional[Union[str, dict]] = None,
        **kwargs,
    ):
        """
        Run a validation operator to validate data assets and to perform the business logic around
        validation that the operator implements.

        Args:
            validation_operator_name: name of the operator, as appears in the context's config file
            assets_to_validate: a list that specifies the data assets that the operator will validate. The members of
                the list can be either batches, or a tuple that will allow the operator to fetch the batch:
                (batch_kwargs, expectation_suite_name)
            evaluation_parameters: $parameter_name syntax references to be evaluated at runtime
            run_id: The run_id for the validation; if None, a default value will be used
            run_name: The run_name for the validation; if None, a default value will be used
            run_time: The date/time of the run
            result_format: one of several supported formatting directives for expectation validation results
            **kwargs: Additional kwargs to pass to the validation operator

        Returns:
            ValidationOperatorResult
        """
        result_format = result_format or {"result_format": "SUMMARY"}

        if not assets_to_validate:
            raise ge_exceptions.DataContextError(
                "No batches of data were passed in. These are required"
            )

        for batch in assets_to_validate:
            if not isinstance(batch, (tuple, DataAsset, Validator)):
                raise ge_exceptions.DataContextError(
                    "Batches are required to be of type DataAsset or Validator"
                )
        try:
            validation_operator = self.validation_operators[validation_operator_name]
        except KeyError:
            raise ge_exceptions.DataContextError(
                f"No validation operator `{validation_operator_name}` was found in your project. Please verify this in your great_expectations.yml"
            )

        if run_id is None and run_name is None:
            run_name = datetime.datetime.now(datetime.timezone.utc).strftime(
                "%Y%m%dT%H%M%S.%fZ"
            )
            logger.info(f"Setting run_name to: {run_name}")
        if evaluation_parameters is None:
            return validation_operator.run(
                assets_to_validate=assets_to_validate,
                run_id=run_id,
                run_name=run_name,
                run_time=run_time,
                result_format=result_format,
                **kwargs,
            )
        else:
            return validation_operator.run(
                assets_to_validate=assets_to_validate,
                run_id=run_id,
                evaluation_parameters=evaluation_parameters,
                run_name=run_name,
                run_time=run_time,
                result_format=result_format,
                **kwargs,
            )

    def list_validation_operators(self):
        """List currently-configured Validation Operators on this context"""

        validation_operators = []
        for (
            name,
            value,
        ) in self.variables.validation_operators.items():
            value["name"] = name
            validation_operators.append(value)
        return validation_operators

    def list_validation_operator_names(self):
        if not self.validation_operators:
            return []

        return list(self.validation_operators.keys())

    def profile_data_asset(  # noqa: C901 - complexity 16
        self,
        datasource_name,
        batch_kwargs_generator_name=None,
        data_asset_name=None,
        batch_kwargs=None,
        expectation_suite_name=None,
        profiler=BasicDatasetProfiler,
        profiler_configuration=None,
        run_id=None,
        additional_batch_kwargs=None,
        run_name=None,
        run_time=None,
    ):
        """
        Profile a data asset

        :param datasource_name: the name of the datasource to which the profiled data asset belongs
        :param batch_kwargs_generator_name: the name of the batch kwargs generator to use to get batches (only if batch_kwargs are not provided)
        :param data_asset_name: the name of the profiled data asset
        :param batch_kwargs: optional - if set, the method will use the value to fetch the batch to be profiled. If not passed, the batch kwargs generator (generator_name arg) will choose a batch
        :param profiler: the profiler class to use
        :param profiler_configuration: Optional profiler configuration dict
        :param run_name: optional - if set, the validation result created by the profiler will be under the provided run_name
        :param additional_batch_kwargs:
        :returns
            A dictionary::

                {
                    "success": True/False,
                    "results": List of (expectation_suite, EVR) tuples for each of the data_assets found in the datasource
                }

            When success = False, the error details are under "error" key
        """

        assert not (run_id and run_name) and not (
            run_id and run_time
        ), "Please provide either a run_id or run_name and/or run_time."
        if isinstance(run_id, str) and not run_name:
            # deprecated-v0.11.0
            warnings.warn(
                "String run_ids are deprecated as of v0.11.0 and support will be removed in v0.16. Please provide a run_id of type "
                "RunIdentifier(run_name=None, run_time=None), or a dictionary containing run_name "
                "and run_time (both optional). Instead of providing a run_id, you may also provide"
                "run_name and run_time separately.",
                DeprecationWarning,
            )
            try:
                run_time = parse(run_id)
            except (ValueError, TypeError):
                pass
            run_id = RunIdentifier(run_name=run_id, run_time=run_time)
        elif isinstance(run_id, dict):
            run_id = RunIdentifier(**run_id)
        elif not isinstance(run_id, RunIdentifier):
            run_name = run_name or "profiling"
            run_id = RunIdentifier(run_name=run_name, run_time=run_time)

        logger.info(f"Profiling '{datasource_name}' with '{profiler.__name__}'")

        if not additional_batch_kwargs:
            additional_batch_kwargs = {}

        if batch_kwargs is None:
            try:
                generator = self.get_datasource(
                    datasource_name=datasource_name
                ).get_batch_kwargs_generator(name=batch_kwargs_generator_name)
                batch_kwargs = generator.build_batch_kwargs(
                    data_asset_name, **additional_batch_kwargs
                )
            except ge_exceptions.BatchKwargsError:
                raise ge_exceptions.ProfilerError(
                    "Unable to build batch_kwargs for datasource {}, using batch kwargs generator {} for name {}".format(
                        datasource_name, batch_kwargs_generator_name, data_asset_name
                    )
                )
            except ValueError:
                raise ge_exceptions.ProfilerError(
                    "Unable to find datasource {} or batch kwargs generator {}.".format(
                        datasource_name, batch_kwargs_generator_name
                    )
                )
        else:
            batch_kwargs.update(additional_batch_kwargs)

        profiling_results = {"success": False, "results": []}

        total_columns, total_expectations, total_rows = 0, 0, 0
        total_start_time = datetime.datetime.now()

        name = data_asset_name
        # logger.info("\tProfiling '%s'..." % name)

        start_time = datetime.datetime.now()

        if expectation_suite_name is None:
            if batch_kwargs_generator_name is None and data_asset_name is None:
                expectation_suite_name = (
                    datasource_name
                    + "."
                    + profiler.__name__
                    + "."
                    + BatchKwargs(batch_kwargs).to_id()
                )
            else:
                expectation_suite_name = (
                    datasource_name
                    + "."
                    + batch_kwargs_generator_name
                    + "."
                    + data_asset_name
                    + "."
                    + profiler.__name__
                )

        self.create_expectation_suite(
            expectation_suite_name=expectation_suite_name, overwrite_existing=True
        )

        # TODO: Add batch_parameters
        batch = self.get_batch(
            expectation_suite_name=expectation_suite_name,
            batch_kwargs=batch_kwargs,
        )

        if not profiler.validate(batch):
            raise ge_exceptions.ProfilerError(
                f"batch '{name}' is not a valid batch for the '{profiler.__name__}' profiler"
            )

        # Note: This logic is specific to DatasetProfilers, which profile a single batch. Multi-batch profilers
        # will have more to unpack.
        expectation_suite, validation_results = profiler.profile(
            batch, run_id=run_id, profiler_configuration=profiler_configuration
        )
        profiling_results["results"].append((expectation_suite, validation_results))

        validation_ref = self.validations_store.set(
            key=ValidationResultIdentifier(
                expectation_suite_identifier=ExpectationSuiteIdentifier(
                    expectation_suite_name=expectation_suite_name
                ),
                run_id=run_id,
                batch_identifier=batch.batch_id,
            ),
            value=validation_results,
        )

        if isinstance(validation_ref, GXCloudIDAwareRef):
            ge_cloud_id = validation_ref.ge_cloud_id
            validation_results.ge_cloud_id = uuid.UUID(ge_cloud_id)

        if isinstance(batch, Dataset):
            # For datasets, we can produce some more detailed statistics
            row_count = batch.get_row_count()
            total_rows += row_count
            new_column_count = len(
                {
                    exp.kwargs["column"]
                    for exp in expectation_suite.expectations
                    if "column" in exp.kwargs
                }
            )
            total_columns += new_column_count

        new_expectation_count = len(expectation_suite.expectations)
        total_expectations += new_expectation_count

        self.save_expectation_suite(expectation_suite)
        duration = (datetime.datetime.now() - start_time).total_seconds()
        # noinspection PyUnboundLocalVariable
        logger.info(
            f"\tProfiled {new_column_count} columns using {row_count} rows from {name} ({duration:.3f} sec)"
        )

        total_duration = (datetime.datetime.now() - total_start_time).total_seconds()
        logger.info(
            f"""
Profiled the data asset, with {total_rows} total rows and {total_columns} columns in {total_duration:.2f} seconds.
Generated, evaluated, and stored {total_expectations} Expectations during profiling. Please review results using data-docs."""
        )

        profiling_results["success"] = True
        return profiling_results

    def add_batch_kwargs_generator(
        self, datasource_name, batch_kwargs_generator_name, class_name, **kwargs
    ):
        """
        Add a batch kwargs generator to the named datasource, using the provided
        configuration.

        Args:
            datasource_name: name of datasource to which to add the new batch kwargs generator
            batch_kwargs_generator_name: name of the generator to add
            class_name: class of the batch kwargs generator to add
            **kwargs: batch kwargs generator configuration, provided as kwargs

        Returns:

        """
        datasource_obj = self.get_datasource(datasource_name)
        generator = datasource_obj.add_batch_kwargs_generator(
            name=batch_kwargs_generator_name, class_name=class_name, **kwargs
        )
        return generator

    def get_available_data_asset_names(
        self, datasource_names=None, batch_kwargs_generator_names=None
    ):
        """Inspect datasource and batch kwargs generators to provide available data_asset objects.

        Args:
            datasource_names: list of datasources for which to provide available data_asset_name objects. If None, \
            return available data assets for all datasources.
            batch_kwargs_generator_names: list of batch kwargs generators for which to provide available
            data_asset_name objects.

        Returns:
            data_asset_names (dict): Dictionary describing available data assets
            ::

                {
                  datasource_name: {
                    batch_kwargs_generator_name: [ data_asset_1, data_asset_2, ... ]
                    ...
                  }
                  ...
                }

        """
        data_asset_names = {}
        if datasource_names is None:
            datasource_names = [
                datasource["name"] for datasource in self.list_datasources()
            ]
        elif isinstance(datasource_names, str):
            datasource_names = [datasource_names]
        elif not isinstance(datasource_names, list):
            raise ValueError(
                "Datasource names must be a datasource name, list of datasource names or None (to list all datasources)"
            )

        if batch_kwargs_generator_names is not None:
            if isinstance(batch_kwargs_generator_names, str):
                batch_kwargs_generator_names = [batch_kwargs_generator_names]
            if len(batch_kwargs_generator_names) == len(
                datasource_names
            ):  # Iterate over both together
                for idx, datasource_name in enumerate(datasource_names):
                    datasource = self.get_datasource(datasource_name)
                    data_asset_names[
                        datasource_name
                    ] = datasource.get_available_data_asset_names(
                        batch_kwargs_generator_names[idx]
                    )

            elif len(batch_kwargs_generator_names) == 1:
                datasource = self.get_datasource(datasource_names[0])
                datasource_names[
                    datasource_names[0]
                ] = datasource.get_available_data_asset_names(
                    batch_kwargs_generator_names
                )

            else:
                raise ValueError(
                    "If providing batch kwargs generator, you must either specify one for each datasource or only "
                    "one datasource."
                )
        else:  # generator_names is None
            for datasource_name in datasource_names:
                try:
                    datasource = self.get_datasource(datasource_name)
                    data_asset_names[
                        datasource_name
                    ] = datasource.get_available_data_asset_names()
                except ValueError:
                    # handle the edge case of a non-existent datasource
                    data_asset_names[datasource_name] = {}

        return data_asset_names

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

        """
        if kwargs.get("name"):
            if data_asset_name:
                raise ValueError(
                    "Cannot provide both 'name' and 'data_asset_name'. Please use 'data_asset_name' only."
                )
            # deprecated-v0.11.2
            warnings.warn(
                "name is deprecated as a batch_parameter as of v0.11.2 and will be removed in v0.16. Please use data_asset_name instead.",
                DeprecationWarning,
            )
            data_asset_name = kwargs.pop("name")
        datasource_obj = self.get_datasource(datasource)
        batch_kwargs = datasource_obj.build_batch_kwargs(
            batch_kwargs_generator=batch_kwargs_generator,
            data_asset_name=data_asset_name,
            partition_id=partition_id,
            **kwargs,
        )
        return batch_kwargs

    @usage_statistics_enabled_method(
        event_name=UsageStatsEvents.DATA_CONTEXT_OPEN_DATA_DOCS,
    )
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
        data_docs_urls: List[Dict[str, str]] = self.get_docs_sites_urls(
            resource_identifier=resource_identifier,
            site_name=site_name,
            only_if_exists=only_if_exists,
        )
        urls_to_open: List[str] = [site["site_url"] for site in data_docs_urls]

        for url in urls_to_open:
            if url is not None:
                logger.debug(f"Opening Data Docs found here: {url}")
                webbrowser.open(url)

    def get_docs_sites_urls(
        self,
        resource_identifier=None,
        site_name: Optional[str] = None,
        only_if_exists=True,
        site_names: Optional[List[str]] = None,
    ) -> List[Dict[str, str]]:
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
            if site_name not in sites.keys():
                raise ge_exceptions.DataContextError(
                    f"Could not find site named {site_name}. Please check your configurations"
                )
            site = sites[site_name]
            site_builder = self._load_site_builder_from_site_config(site)
            url = site_builder.get_resource_url(
                resource_identifier=resource_identifier, only_if_exists=only_if_exists
            )
            return [{"site_name": site_name, "site_url": url}]

        site_urls = []
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
            raise ge_exceptions.ClassInstantiationError(
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
            raise ge_exceptions.DataContextError(
                "No data docs sites were found on this DataContext, therefore no sites will be cleaned.",
            )

        data_docs_site_names = list(data_docs_sites.keys())
        if site_name:
            if site_name not in data_docs_site_names:
                raise ge_exceptions.DataContextError(
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
            config_defaults={
                "module_name": "great_expectations.render.renderer.site_builder"
            },
        )
        site_builder.clean_site()
        return True

    @staticmethod
    def _default_profilers_exist(directory_path: Optional[str]) -> bool:
        """
        Helper method. Do default profilers exist in directory_path?
        """
        if not directory_path:
            return False

        profiler_directory_path: str = os.path.join(
            directory_path,
            DataContextConfigDefaults.DEFAULT_PROFILER_STORE_BASE_DIRECTORY_RELATIVE_NAME.value,
        )
        return os.path.isdir(profiler_directory_path)

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
        if environment_variable and os.environ.get(environment_variable, ""):
            return os.environ.get(environment_variable)
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
    def _get_metric_configuration_tuples(
        metric_configuration: Union[str, dict], base_kwargs: Optional[dict] = None
    ) -> List[Tuple[str, Union[dict, Any]]]:
        if base_kwargs is None:
            base_kwargs = {}

        if isinstance(metric_configuration, str):
            return [(metric_configuration, base_kwargs)]

        metric_configurations_list = []
        for kwarg_name in metric_configuration.keys():
            if not isinstance(metric_configuration[kwarg_name], dict):
                raise ge_exceptions.DataContextError(
                    "Invalid metric_configuration: each key must contain a "
                    "dictionary."
                )
            if (
                kwarg_name == "metric_kwargs_id"
            ):  # this special case allows a hash of multiple kwargs
                for metric_kwargs_id in metric_configuration[kwarg_name].keys():
                    if base_kwargs != {}:
                        raise ge_exceptions.DataContextError(
                            "Invalid metric_configuration: when specifying "
                            "metric_kwargs_id, no other keys or values may be defined."
                        )
                    if not isinstance(
                        metric_configuration[kwarg_name][metric_kwargs_id], list
                    ):
                        raise ge_exceptions.DataContextError(
                            "Invalid metric_configuration: each value must contain a "
                            "list."
                        )
                    metric_configurations_list += [
                        (metric_name, {"metric_kwargs_id": metric_kwargs_id})
                        for metric_name in metric_configuration[kwarg_name][
                            metric_kwargs_id
                        ]
                    ]
            else:
                for kwarg_value in metric_configuration[kwarg_name].keys():
                    base_kwargs.update({kwarg_name: kwarg_value})
                    if not isinstance(
                        metric_configuration[kwarg_name][kwarg_value], list
                    ):
                        raise ge_exceptions.DataContextError(
                            "Invalid metric_configuration: each value must contain a "
                            "list."
                        )
                    for nested_configuration in metric_configuration[kwarg_name][
                        kwarg_value
                    ]:
                        metric_configurations_list += (
                            AbstractDataContext._get_metric_configuration_tuples(
                                nested_configuration, base_kwargs=base_kwargs
                            )
                        )

        return metric_configurations_list

    @classmethod
    def get_or_create_data_context_config(
        cls, project_config: Union[DataContextConfig, Mapping]
    ) -> DataContextConfig:
        if isinstance(project_config, DataContextConfig):
            return project_config
        try:
            # Roundtrip through schema validation to remove any illegal fields add/or restore any missing fields.
            project_config_dict = dataContextConfigSchema.dump(project_config)
            project_config_dict = dataContextConfigSchema.load(project_config_dict)
            context_config: DataContextConfig = DataContextConfig(**project_config_dict)
            return context_config
        except ValidationError:
            raise

    def _normalize_absolute_or_relative_path(
        self, path: Optional[str]
    ) -> Optional[str]:
        """
        Why does this exist in AbstractDataContext? CloudDataContext and FileDataContext both use it
        """
        if path is None:
            return None
        if os.path.isabs(path):
            return path
        else:
            return os.path.join(self.root_directory, path)  # type: ignore[arg-type]

    def _apply_global_config_overrides(
        self, config: DataContextConfig
    ) -> DataContextConfig:

        """
        Applies global configuration overrides for
            - usage_statistics being enabled
            - data_context_id for usage_statistics
            - global_usage_statistics_url

        Args:
            config (DataContextConfig): Config that is passed into the DataContext constructor

        Returns:
            DataContextConfig with the appropriate overrides
        """
        validation_errors: dict = {}
        config_with_global_config_overrides: DataContextConfig = copy.deepcopy(config)
        usage_stats_enabled: bool = self._is_usage_stats_enabled()
        if not usage_stats_enabled:
            logger.info(
                "Usage statistics is disabled globally. Applying override to project_config."
            )
            config_with_global_config_overrides.anonymous_usage_statistics.enabled = (
                False
            )
        global_data_context_id: Optional[str] = self._get_data_context_id_override()
        # data_context_id
        if global_data_context_id:
            data_context_id_errors = anonymizedUsageStatisticsSchema.validate(
                {"data_context_id": global_data_context_id}
            )
            if not data_context_id_errors:
                logger.info(
                    "data_context_id is defined globally. Applying override to project_config."
                )
                config_with_global_config_overrides.anonymous_usage_statistics.data_context_id = (
                    global_data_context_id
                )
            else:
                validation_errors.update(data_context_id_errors)

        # usage statistics url
        global_usage_statistics_url: Optional[
            str
        ] = self._get_usage_stats_url_override()
        if global_usage_statistics_url:
            usage_statistics_url_errors = anonymizedUsageStatisticsSchema.validate(
                {"usage_statistics_url": global_usage_statistics_url}
            )
            if not usage_statistics_url_errors:
                logger.info(
                    "usage_statistics_url is defined globally. Applying override to project_config."
                )
                config_with_global_config_overrides.anonymous_usage_statistics.usage_statistics_url = (
                    global_usage_statistics_url
                )
            else:
                validation_errors.update(usage_statistics_url_errors)
        if validation_errors:
            logger.warning(
                "The following globally-defined config variables failed validation:\n{}\n\n"
                "Please fix the variables if you would like to apply global values to project_config.".format(
                    json.dumps(validation_errors, indent=2)
                )
            )

        return config_with_global_config_overrides

    def _load_config_variables(self) -> Dict:
        config_var_provider = self.config_provider.get_provider(
            _ConfigurationVariablesConfigurationProvider
        )
        if config_var_provider:
            return config_var_provider.get_values()
        return {}

    @staticmethod
    def _is_usage_stats_enabled() -> bool:
        """
        Checks the following locations to see if usage_statistics is disabled in any of the following locations:
            - GE_USAGE_STATS, which is an environment_variable
            - GLOBAL_CONFIG_PATHS
        If GE_USAGE_STATS exists AND its value is one of the FALSEY_STRINGS, usage_statistics is disabled (return False)
        Also checks GLOBAL_CONFIG_PATHS to see if config file contains override for anonymous_usage_statistics
        Returns True otherwise

        Returns:
            bool that tells you whether usage_statistics is on or off
        """
        usage_statistics_enabled: bool = True
        if os.environ.get("GE_USAGE_STATS", False):
            ge_usage_stats = os.environ.get("GE_USAGE_STATS")
            if ge_usage_stats in AbstractDataContext.FALSEY_STRINGS:
                usage_statistics_enabled = False
            else:
                logger.warning(
                    "GE_USAGE_STATS environment variable must be one of: {}".format(
                        AbstractDataContext.FALSEY_STRINGS
                    )
                )
        for config_path in AbstractDataContext.GLOBAL_CONFIG_PATHS:
            config = configparser.ConfigParser()
            states = config.BOOLEAN_STATES
            for falsey_string in AbstractDataContext.FALSEY_STRINGS:
                states[falsey_string] = False  # type: ignore[index]

            states["TRUE"] = True  # type: ignore[index]
            states["True"] = True  # type: ignore[index]
            config.BOOLEAN_STATES = states  # type: ignore[misc] # Cannot assign to class variable via instance
            config.read(config_path)
            try:
                if not config.getboolean("anonymous_usage_statistics", "enabled"):
                    usage_statistics_enabled = False

            except (ValueError, configparser.Error):
                pass
        return usage_statistics_enabled

    def _get_data_context_id_override(self) -> Optional[str]:
        """
        Return data_context_id from environment variable.

        Returns:
            Optional string that represents data_context_id
        """
        return self._get_global_config_value(
            environment_variable="GE_DATA_CONTEXT_ID",
            conf_file_section="anonymous_usage_statistics",
            conf_file_option="data_context_id",
        )

    def _get_usage_stats_url_override(self) -> Optional[str]:
        """
        Return GE_USAGE_STATISTICS_URL from environment variable if it exists

        Returns:
            Optional string that represents GE_USAGE_STATISTICS_URL
        """
        return self._get_global_config_value(
            environment_variable="GE_USAGE_STATISTICS_URL",
            conf_file_section="anonymous_usage_statistics",
            conf_file_option="usage_statistics_url",
        )

    def _build_store_from_config(
        self, store_name: str, store_config: dict
    ) -> Optional[Store]:
        module_name = "great_expectations.data_context.store"
        # Set expectations_store.store_backend_id to the data_context_id from the project_config if
        # the expectations_store does not yet exist by:
        # adding the data_context_id from the project_config
        # to the store_config under the key manually_initialize_store_backend_id
        if (store_name == self.expectations_store_name) and store_config.get(
            "store_backend"
        ):
            store_config["store_backend"].update(
                {
                    "manually_initialize_store_backend_id": self.variables.anonymous_usage_statistics.data_context_id  # type: ignore[union-attr]
                }
            )

        # Set suppress_store_backend_id = True if store is inactive and has a store_backend.
        if (
            store_name not in [store["name"] for store in self.list_active_stores()]  # type: ignore[index]
            and store_config.get("store_backend") is not None
        ):
            store_config["store_backend"].update({"suppress_store_backend_id": True})

        new_store = build_store_from_config(
            store_name=store_name,
            store_config=store_config,
            module_name=module_name,
            runtime_environment={
                "root_directory": self.root_directory,
            },
        )
        self._stores[store_name] = new_store
        return new_store

    # properties
    @property
    def variables(self) -> DataContextVariables:
        if self._variables is None:
            self._variables = self._init_variables()
        return self._variables

    @property
    def usage_statistics_handler(self) -> Optional[UsageStatisticsHandler]:
        return self._usage_statistics_handler

    @property
    def anonymous_usage_statistics(self) -> AnonymizedUsageStatisticsConfig:
        return self.variables.anonymous_usage_statistics  # type: ignore[return-value]

    @property
    def progress_bars(self) -> Optional[ProgressBarsConfig]:
        return self.variables.progress_bars

    @property
    def include_rendered_content(self) -> IncludeRenderedContentConfig:
        return self.variables.include_rendered_content

    @property
    def notebooks(self) -> NotebookConfig:
        return self.variables.notebooks  # type: ignore[return-value]

    @property
    def datasources(
        self,
    ) -> Dict[str, Union[LegacyDatasource, BaseDatasource, XDatasource]]:
        """A single holder for all Datasources in this context"""
        return self._cached_datasources

    @property
    def data_context_id(self) -> str:
        return self.variables.anonymous_usage_statistics.data_context_id  # type: ignore[union-attr]

    def _init_stores(self, store_configs: Dict[str, dict]) -> None:
        """Initialize all Stores for this DataContext.

        Stores are a good fit for reading/writing objects that:
            1. follow a clear key-value pattern, and
            2. are usually edited programmatically, using the Context

        Note that stores do NOT manage plugins.
        """
        for store_name, store_config in store_configs.items():
            self._build_store_from_config(store_name, store_config)

        # The DatasourceStore is inherent to all DataContexts but is not an explicit part of the project config.
        # As such, it must be instantiated separately.
        self._init_datasource_store()

    @abstractmethod
    def _init_datasource_store(self) -> None:
        """Internal utility responsible for creating a DatasourceStore to persist and manage a user's Datasources.

        Please note that the DatasourceStore lacks the same extensibility that other analagous Stores do; a default
        implementation is provided based on the user's environment but is not customizable.
        """
        raise NotImplementedError

    def _update_config_variables(self) -> None:
        """Updates config_variables cache by re-calling _load_config_variables().
        Necessary after running methods that modify config AND could contain config_variables for credentials
        (example is add_datasource())
        """
        self._config_variables = self._load_config_variables()

    def _initialize_usage_statistics(
        self, usage_statistics_config: AnonymizedUsageStatisticsConfig
    ) -> None:
        """Initialize the usage statistics system."""
        if not usage_statistics_config.enabled:
            logger.info("Usage statistics is disabled; skipping initialization.")
            self._usage_statistics_handler = None
            return

        self._usage_statistics_handler = UsageStatisticsHandler(
            data_context=self,
            data_context_id=self._data_context_id,
            usage_statistics_url=usage_statistics_config.usage_statistics_url,
        )

    def _init_datasources(self) -> None:
        """Initialize the datasources in store"""
        config: DataContextConfig = self.config
        datasources: Dict[str, DatasourceConfig] = cast(
            Dict[str, DatasourceConfig], config.datasources
        )

        for datasource_name, datasource_config in datasources.items():
            try:
                config = copy.deepcopy(datasource_config)  # type: ignore[assignment]

                raw_config_dict = dict(datasourceConfigSchema.dump(config))
                substituted_config_dict: dict = self.config_provider.substitute_config(
                    raw_config_dict
                )

                raw_datasource_config = datasourceConfigSchema.load(raw_config_dict)
                substituted_datasource_config = datasourceConfigSchema.load(
                    substituted_config_dict
                )
                substituted_datasource_config.name = datasource_name

                datasource = self._instantiate_datasource_from_config(
                    raw_config=raw_datasource_config,
                    substituted_config=substituted_datasource_config,
                )
                self._cached_datasources[datasource_name] = datasource
            except ge_exceptions.DatasourceInitializationError as e:
                logger.warning(f"Cannot initialize datasource {datasource_name}: {e}")
                # this error will happen if our configuration contains datasources that GX can no longer connect to.
                # this is ok, as long as we don't use it to retrieve a batch. If we try to do that, the error will be
                # caught at the context.get_batch() step. So we just pass here.
                pass

    def _instantiate_datasource_from_config(
        self,
        raw_config: DatasourceConfig,
        substituted_config: DatasourceConfig,
    ) -> Datasource:
        """Instantiate a new datasource.
        Args:
            config: Datasource config.

        Returns:
            Datasource instantiated from config.

        Raises:
            DatasourceInitializationError
        """
        try:
            datasource: Datasource = self._build_datasource_from_config(
                raw_config=raw_config, substituted_config=substituted_config
            )
        except Exception as e:
            raise ge_exceptions.DatasourceInitializationError(
                datasource_name=substituted_config.name, message=str(e)
            )
        return datasource

    def _build_datasource_from_config(
        self, raw_config: DatasourceConfig, substituted_config: DatasourceConfig
    ) -> Datasource:
        """Instantiate a Datasource from a config.

        Args:
            config: DatasourceConfig object defining the datsource to instantiate.

        Returns:
            Datasource instantiated from config.

        Raises:
            ClassInstantiationError
        """
        # We convert from the type back to a dictionary for purposes of instantiation
        serializer = DictConfigSerializer(schema=datasourceConfigSchema)
        substituted_config_dict: dict = serializer.serialize(substituted_config)

        # While the new Datasource classes accept "data_context_root_directory", the Legacy Datasource classes do not.
        if substituted_config_dict["class_name"] in [
            "BaseDatasource",
            "Datasource",
        ]:
            substituted_config_dict.update(
                {"data_context_root_directory": self.root_directory}
            )
        module_name: str = "great_expectations.datasource"
        datasource: Datasource = instantiate_class_from_config(
            config=substituted_config_dict,
            runtime_environment={"data_context": self, "concurrency": self.concurrency},
            config_defaults={"module_name": module_name},
        )
        if not datasource:
            raise ge_exceptions.ClassInstantiationError(
                module_name=module_name,
                package_name=None,
                class_name=substituted_config_dict["class_name"],
            )

        # Chetan - 20221103 - Directly accessing private attr in order to patch security vulnerabiliy around credential leakage.
        # This is to be removed once substitution logic is migrated from the context to the individual object level.
        raw_config_dict: dict = serializer.serialize(raw_config)
        datasource._raw_config = raw_config_dict

        return datasource

    def _perform_substitutions_on_datasource_config(
        self, config: DatasourceConfig
    ) -> DatasourceConfig:
        """Substitute variables in a datasource config e.g. from env vars, config_vars.yml

        Config must be persisted with ${VARIABLES} syntax but hydrated at time of use.

        Args:
            config: Datasource Config

        Returns:
            Datasource Config with substitutions performed.
        """
        substitution_serializer = DictConfigSerializer(schema=datasourceConfigSchema)
        raw_config: dict = substitution_serializer.serialize(config)

        substituted_config_dict: dict = self.config_provider.substitute_config(
            raw_config
        )

        substituted_config: DatasourceConfig = datasourceConfigSchema.load(
            substituted_config_dict
        )

        return substituted_config

    def _instantiate_datasource_from_config_and_update_project_config(
        self,
        config: DatasourceConfig,
        initialize: bool,
        save_changes: bool,
    ) -> Optional[Datasource]:
        """Perform substitutions and optionally initialize the Datasource and/or store the config.

        Args:
            config: Datasource Config to initialize and/or store.
            initialize: Whether to initialize the datasource, alternatively you can store without initializing.
            save_changes: Whether to store the configuration in your configuration store (GX cloud or great_expectations.yml)

        Returns:
            Datasource object if initialized.

        Raises:
            DatasourceInitializationError
        """
        # Note that the call to `DatasourcStore.set` may alter the config object's state
        # As such, we invoke it at the top of our function so any changes are reflected downstream
        if save_changes:
            config = self._datasource_store.set(key=None, value=config)  # type: ignore[attr-defined]

        datasource: Optional[Datasource] = None
        if initialize:
            try:
                substituted_config = self._perform_substitutions_on_datasource_config(
                    config
                )
                datasource = self._instantiate_datasource_from_config(
                    raw_config=config, substituted_config=substituted_config
                )
                self._cached_datasources[config.name] = datasource
            except ge_exceptions.DatasourceInitializationError as e:
                if save_changes:
                    self._datasource_store.delete(config)  # type: ignore[attr-defined]
                raise e

        self.config.datasources[config.name] = config  # type: ignore[index,assignment]

        return datasource

    def _construct_data_context_id(self) -> str:
        # Choose the id of the currently-configured expectations store, if it is a persistent store
        expectations_store = self._stores[self.variables.expectations_store_name]
        if isinstance(expectations_store.store_backend, TupleStoreBackend):
            # suppress_warnings since a warning will already have been issued during the store creation
            # if there was an invalid store config
            return expectations_store.store_backend_id_warnings_suppressed

        # Otherwise choose the id stored in the project_config
        else:
            return self.variables.anonymous_usage_statistics.data_context_id  # type: ignore[union-attr]

    def _compile_evaluation_parameter_dependencies(self) -> None:
        self._evaluation_parameter_dependencies = {}
        # NOTE: Chetan - 20211118: This iteration is reverting the behavior performed here:
        # https://github.com/great-expectations/great_expectations/pull/3377
        # This revision was necessary due to breaking changes but will need to be brought back in a future ticket.
        for key in self.expectations_store.list_keys():
            expectation_suite_dict: dict = cast(dict, self.expectations_store.get(key))
            if not expectation_suite_dict:
                continue
            expectation_suite = ExpectationSuite(
                **expectation_suite_dict, data_context=self
            )

            dependencies: dict = (
                expectation_suite.get_evaluation_parameter_dependencies()
            )
            if len(dependencies) > 0:
                nested_update(self._evaluation_parameter_dependencies, dependencies)

        self._evaluation_parameter_dependencies_compiled = True

    def get_validation_result(
        self,
        expectation_suite_name,
        run_id=None,
        batch_identifier=None,
        validations_store_name=None,
        failed_only=False,
        include_rendered_content=None,
    ):
        """Get validation results from a configured store.

        Args:
            expectation_suite_name: expectation_suite name for which to get validation result (default: "default")
            run_id: run_id for which to get validation result (if None, fetch the latest result by alphanumeric sort)
            validations_store_name: the name of the store from which to get validation results
            failed_only: if True, filter the result to return only failed expectations
            include_rendered_content: whether to re-populate the validation_result rendered_content

        Returns:
            validation_result

        """
        if validations_store_name is None:
            validations_store_name = self.validations_store_name
        selected_store = self.stores[validations_store_name]

        if run_id is None or batch_identifier is None:
            # Get most recent run id
            # NOTE : This method requires a (potentially very inefficient) list_keys call.
            # It should probably move to live in an appropriate Store class,
            # but when we do so, that Store will need to function as more than just a key-value Store.
            key_list = selected_store.list_keys()
            filtered_key_list = []
            for key in key_list:
                if run_id is not None and key.run_id != run_id:
                    continue
                if (
                    batch_identifier is not None
                    and key.batch_identifier != batch_identifier
                ):
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

        if include_rendered_content is None:
            include_rendered_content = (
                self._determine_if_expectation_validation_result_include_rendered_content()
            )

        key = ValidationResultIdentifier(
            expectation_suite_identifier=ExpectationSuiteIdentifier(
                expectation_suite_name=expectation_suite_name
            ),
            run_id=run_id,
            batch_identifier=batch_identifier,
        )
        results_dict = selected_store.get(key)

        validation_result = (
            results_dict.get_failed_validation_results()
            if failed_only
            else results_dict
        )

        if include_rendered_content:
            for expectation_validation_result in validation_result.results:
                expectation_validation_result.render()
                expectation_validation_result.expectation_config.render()

        return validation_result

    def store_validation_result_metrics(
        self, requested_metrics, validation_results, target_store_name
    ) -> None:
        self._store_metrics(requested_metrics, validation_results, target_store_name)

    def _store_metrics(
        self, requested_metrics, validation_results, target_store_name
    ) -> None:
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
        data_asset_name = validation_results.meta.get("batch_kwargs", {}).get(
            "data_asset_name"
        )

        for expectation_suite_dependency, metrics_list in requested_metrics.items():
            if (expectation_suite_dependency != "*") and (
                expectation_suite_dependency != expectation_suite_name
            ):
                continue

            if not isinstance(metrics_list, list):
                raise ge_exceptions.DataContextError(
                    "Invalid requested_metrics configuration: metrics requested for "
                    "each expectation suite must be a list."
                )

            for metric_configuration in metrics_list:
                metric_configurations = (
                    AbstractDataContext._get_metric_configuration_tuples(
                        metric_configuration
                    )
                )
                for metric_name, metric_kwargs in metric_configurations:
                    try:
                        metric_value = validation_results.get_metric(
                            metric_name, **metric_kwargs
                        )
                        self.stores[target_store_name].set(
                            ValidationMetricIdentifier(
                                run_id=run_id,
                                data_asset_name=data_asset_name,
                                expectation_suite_identifier=ExpectationSuiteIdentifier(
                                    expectation_suite_name
                                ),
                                metric_name=metric_name,
                                metric_kwargs_id=get_metric_kwargs_id(
                                    metric_name, metric_kwargs
                                ),
                            ),
                            metric_value,
                        )
                    except ge_exceptions.UnavailableMetricError:
                        # This will happen frequently in larger pipelines
                        logger.debug(
                            "metric {} was requested by another expectation suite but is not available in "
                            "this validation result.".format(metric_name)
                        )

    def send_usage_message(
        self, event: str, event_payload: Optional[dict], success: Optional[bool] = None
    ) -> None:
        """helper method to send a usage method using DataContext. Used when sending usage events from
            classes like ExpectationSuite.
            event
        Args:
            event (str): str representation of event
            event_payload (dict): optional event payload
            success (bool): optional success param
        Returns:
            None
        """
        send_usage_message(self, event, event_payload, success)

    def _determine_if_expectation_suite_include_rendered_content(
        self, include_rendered_content: Optional[bool] = None
    ) -> bool:
        if include_rendered_content is None:
            if (
                self.include_rendered_content.expectation_suite is True
                or self.include_rendered_content.globally is True
            ):
                return True
            else:
                return False
        return include_rendered_content

    def _determine_if_expectation_validation_result_include_rendered_content(
        self, include_rendered_content: Optional[bool] = None
    ) -> bool:
        if include_rendered_content is None:
            if (
                self.include_rendered_content.expectation_validation_result is True
                or self.include_rendered_content.globally is True
            ):
                return True
            else:
                return False
        return include_rendered_content

    @staticmethod
    def _determine_save_changes_flag(save_changes: Optional[bool]) -> bool:
        """
        This method is meant to enable the gradual deprecation of the `save_changes` boolean
        flag on various Datasource CRUD methods. Moving forward, we will always persist changes
        made by these CRUD methods (a.k.a. the behavior created by save_changes=True).

        As part of this effort, `save_changes` has been set to `None` as a default value
        and will be automatically converted to `True` within this method. If a user passes in a boolean
        value (thereby bypassing the default arg of `None`), a deprecation warning will be raised.
        """
        if save_changes is not None:
            # deprecated-v0.15.32
            warnings.warn(
                'The parameter "save_changes" is deprecated as of v0.15.32; moving forward, '
                "changes made to Datasources will always be persisted by Store implementations. "
                "As support will be removed in v0.18, please omit the argument moving forward.",
                DeprecationWarning,
            )
            return save_changes
        return True

    @public_api
    def test_yaml_config(  # noqa: C901 - complexity 17
        self,
        yaml_config: str,
        name: Optional[str] = None,
        class_name: Optional[str] = None,
        runtime_environment: Optional[dict] = None,
        pretty_print: bool = True,
        return_mode: Literal[
            "instantiated_class", "report_object"
        ] = "instantiated_class",
        shorten_tracebacks: bool = False,
    ):
        """Convenience method for testing yaml configs

        test_yaml_config is a convenience method for configuring the moving
        parts of a Great Expectations deployment. It allows you to quickly
        test out configs for system components, especially Datasources,
        Checkpoints, and Stores.

        For many deployments of Great Expectations, these components (plus
        Expectations) are the only ones you'll need.

        `test_yaml_config` is mainly intended for use within notebooks and tests.

        --Documentation--
            - https://docs.greatexpectations.io/docs/terms/data_context
            - https://docs.greatexpectations.io/docs/guides/validation/checkpoints/how_to_configure_a_new_checkpoint_using_test_yaml_config

        Args:
            yaml_config: A string containing the yaml config to be tested
            name: (Optional) A string containing the name of the component to instantiate
            pretty_print: Determines whether to print human-readable output
            return_mode: Determines what type of object test_yaml_config will return.
                Valid modes are "instantiated_class" and "report_object"
            shorten_tracebacks: If true, catch any errors during instantiation and print only the
                last element of the traceback stack. This can be helpful for
                rapid iteration on configs in a notebook, because it can remove
                the need to scroll up and down a lot.

        Returns:
            The instantiated component (e.g. a Datasource)
            OR
            a json object containing metadata from the component's self_check method.
            The returned object is determined by return_mode.

        """
        yaml_config_validator = _YamlConfigValidator(
            data_context=self,
        )
        return yaml_config_validator.test_yaml_config(
            yaml_config=yaml_config,
            name=name,
            class_name=class_name,
            runtime_environment=runtime_environment,
            pretty_print=pretty_print,
            return_mode=return_mode,
            shorten_tracebacks=shorten_tracebacks,
        )

    def profile_datasource(  # noqa: C901 - complexity 25
        self,
        datasource_name,
        batch_kwargs_generator_name=None,
        data_assets=None,
        max_data_assets=20,
        profile_all_data_assets=True,
        profiler=BasicDatasetProfiler,
        profiler_configuration=None,
        dry_run=False,
        run_id=None,
        additional_batch_kwargs=None,
        run_name=None,
        run_time=None,
    ):
        """Profile the named datasource using the named profiler.

        Args:
            datasource_name: the name of the datasource for which to profile data_assets
            batch_kwargs_generator_name: the name of the batch kwargs generator to use to get batches
            data_assets: list of data asset names to profile
            max_data_assets: if the number of data assets the batch kwargs generator yields is greater than this max_data_assets,
                profile_all_data_assets=True is required to profile all
            profile_all_data_assets: when True, all data assets are profiled, regardless of their number
            profiler: the profiler class to use
            profiler_configuration: Optional profiler configuration dict
            dry_run: when true, the method checks arguments and reports if can profile or specifies the arguments that are missing
            additional_batch_kwargs: Additional keyword arguments to be provided to get_batch when loading the data asset.
        Returns:
            A dictionary::

                {
                    "success": True/False,
                    "results": List of (expectation_suite, EVR) tuples for each of the data_assets found in the datasource
                }

            When success = False, the error details are under "error" key
        """

        # We don't need the datasource object, but this line serves to check if the datasource by the name passed as
        # an arg exists and raise an error if it does not.
        datasource = self.get_datasource(datasource_name)
        assert datasource

        if not dry_run:
            logger.info(f"Profiling '{datasource_name}' with '{profiler.__name__}'")

        profiling_results = {}

        # Build the list of available data asset names (each item a tuple of name and type)

        data_asset_names_dict = self.get_available_data_asset_names(datasource_name)

        available_data_asset_name_list = []
        try:
            datasource_data_asset_names_dict = data_asset_names_dict[datasource_name]
        except KeyError:
            # KeyError will happen if there is not datasource
            raise ge_exceptions.ProfilerError(f"No datasource {datasource_name} found.")

        if batch_kwargs_generator_name is None:
            # if no generator name is passed as an arg and the datasource has only
            # one generator with data asset names, use it.
            # if ambiguous, raise an exception
            for name in datasource_data_asset_names_dict.keys():
                if batch_kwargs_generator_name is not None:
                    profiling_results = {
                        "success": False,
                        "error": {
                            "code": self.PROFILING_ERROR_CODE_MULTIPLE_BATCH_KWARGS_GENERATORS_FOUND
                        },
                    }
                    return profiling_results

                if len(datasource_data_asset_names_dict[name]["names"]) > 0:
                    available_data_asset_name_list = datasource_data_asset_names_dict[
                        name
                    ]["names"]
                    batch_kwargs_generator_name = name

            if batch_kwargs_generator_name is None:
                profiling_results = {
                    "success": False,
                    "error": {
                        "code": self.PROFILING_ERROR_CODE_NO_BATCH_KWARGS_GENERATORS_FOUND
                    },
                }
                return profiling_results
        else:
            # if the generator name is passed as an arg, get this generator's available data asset names
            try:
                available_data_asset_name_list = datasource_data_asset_names_dict[
                    batch_kwargs_generator_name
                ]["names"]
            except KeyError:
                raise ge_exceptions.ProfilerError(
                    "batch kwargs Generator {} not found. Specify the name of a generator configured in this datasource".format(
                        batch_kwargs_generator_name
                    )
                )

        available_data_asset_name_list = sorted(
            available_data_asset_name_list, key=lambda x: x[0]
        )

        if len(available_data_asset_name_list) == 0:
            raise ge_exceptions.ProfilerError(
                "No Data Assets found in Datasource {}. Used batch kwargs generator: {}.".format(
                    datasource_name, batch_kwargs_generator_name
                )
            )
        total_data_assets = len(available_data_asset_name_list)

        if isinstance(data_assets, list) and len(data_assets) > 0:
            not_found_data_assets = [
                name
                for name in data_assets
                if name not in [da[0] for da in available_data_asset_name_list]
            ]
            if len(not_found_data_assets) > 0:
                profiling_results = {
                    "success": False,
                    "error": {
                        "code": self.PROFILING_ERROR_CODE_SPECIFIED_DATA_ASSETS_NOT_FOUND,
                        "not_found_data_assets": not_found_data_assets,
                        "data_assets": available_data_asset_name_list,
                    },
                }
                return profiling_results

            data_assets.sort()
            data_asset_names_to_profiled = data_assets
            total_data_assets = len(available_data_asset_name_list)
            if not dry_run:
                logger.info(
                    f"Profiling the white-listed data assets: {','.join(data_assets)}, alphabetically."
                )
        else:
            if not profile_all_data_assets:
                if total_data_assets > max_data_assets:
                    profiling_results = {
                        "success": False,
                        "error": {
                            "code": self.PROFILING_ERROR_CODE_TOO_MANY_DATA_ASSETS,
                            "num_data_assets": total_data_assets,
                            "data_assets": available_data_asset_name_list,
                        },
                    }
                    return profiling_results

            data_asset_names_to_profiled = [
                name[0] for name in available_data_asset_name_list
            ]
        if not dry_run:
            logger.info(
                f"Profiling all {len(available_data_asset_name_list)} data assets from batch kwargs generator {batch_kwargs_generator_name}"
            )
        else:
            logger.info(
                f"Found {len(available_data_asset_name_list)} data assets from batch kwargs generator {batch_kwargs_generator_name}"
            )

        profiling_results["success"] = True

        if not dry_run:
            profiling_results["results"] = []
            total_columns, total_expectations, total_rows, skipped_data_assets = (
                0,
                0,
                0,
                0,
            )
            total_start_time = datetime.datetime.now()

            for name in data_asset_names_to_profiled:
                logger.info(f"\tProfiling '{name}'...")
                try:
                    profiling_results["results"].append(
                        self.profile_data_asset(
                            datasource_name=datasource_name,
                            batch_kwargs_generator_name=batch_kwargs_generator_name,
                            data_asset_name=name,
                            profiler=profiler,
                            profiler_configuration=profiler_configuration,
                            run_id=run_id,
                            additional_batch_kwargs=additional_batch_kwargs,
                            run_name=run_name,
                            run_time=run_time,
                        )["results"][0]
                    )

                except ge_exceptions.ProfilerError as err:
                    logger.warning(err.message)
                except OSError as err:
                    logger.warning(
                        f"IOError while profiling {name[1]}. (Perhaps a loading error?) Skipping."
                    )
                    logger.debug(str(err))
                    skipped_data_assets += 1
                except SQLAlchemyError as e:
                    logger.warning(
                        f"SqlAlchemyError while profiling {name[1]}. Skipping."
                    )
                    logger.debug(str(e))
                    skipped_data_assets += 1

            total_duration = (
                datetime.datetime.now() - total_start_time
            ).total_seconds()
            logger.info(
                f"""
    Profiled {len(data_asset_names_to_profiled)} of {total_data_assets} named data assets, with {total_rows} total rows and {total_columns} columns in {total_duration:.2f} seconds.
    Generated, evaluated, and stored {total_expectations} Expectations during profiling. Please review results using data-docs."""
            )
            if skipped_data_assets > 0:
                logger.warning(
                    f"Skipped {skipped_data_assets} data assets due to errors."
                )

        profiling_results["success"] = True
        return profiling_results

    @usage_statistics_enabled_method(
        event_name=UsageStatsEvents.DATA_CONTEXT_BUILD_DATA_DOCS,
    )
    def build_data_docs(
        self,
        site_names=None,
        resource_identifiers=None,
        dry_run=False,
        build_index: bool = True,
    ):
        """
        Build Data Docs for your project.

        These make it simple to visualize data quality in your project. These
        include Expectations, Validations & Profiles. The are built for all
        Datasources from JSON artifacts in the local repo including validations
        & profiles from the uncommitted directory.

        :param site_names: if specified, build data docs only for these sites, otherwise,
                            build all the sites specified in the context's config
        :param resource_identifiers: a list of resource identifiers (ExpectationSuiteIdentifier,
                            ValidationResultIdentifier). If specified, rebuild HTML
                            (or other views the data docs sites are rendering) only for
                            the resources in this list. This supports incremental build
                            of data docs sites (e.g., when a new validation result is created)
                            and avoids full rebuild.
        :param dry_run: a flag, if True, the method returns a structure containing the
                            URLs of the sites that *would* be built, but it does not build
                            these sites. The motivation for adding this flag was to allow
                            the CLI to display the the URLs before building and to let users
                            confirm.

        :param build_index: a flag if False, skips building the index page

        Returns:
            A dictionary with the names of the updated data documentation sites as keys and the the location info
            of their index.html files as values
        """
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
                    site_builder: SiteBuilder = (
                        self._init_site_builder_for_data_docs_site_creation(
                            site_name=site_name,
                            site_config=site_config,
                        )
                    )
                    if not site_builder:
                        raise ge_exceptions.ClassInstantiationError(
                            module_name=module_name,
                            package_name=None,
                            class_name=complete_site_config["class_name"],
                        )
                    if dry_run:
                        index_page_locator_infos[
                            site_name
                        ] = site_builder.get_resource_url(only_if_exists=False)
                    else:
                        index_page_resource_identifier_tuple = site_builder.build(
                            resource_identifiers,
                            build_index=build_index,
                        )
                        if index_page_resource_identifier_tuple:
                            index_page_locator_infos[
                                site_name
                            ] = index_page_resource_identifier_tuple[0]

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
                "module_name": "great_expectations.render.renderer.site_builder"
            },
        )
        return site_builder

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
        if isinstance(value, dict) or isinstance(value, OrderedDict):
            return {  # type: ignore[return-value] # recursive call expects str
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
        config_variable_name: str,
        value: Any,
        skip_if_substitution_variable: bool = True,
    ) -> None:
        r"""Save config variable value
        Escapes $ unless they are used in substitution variables e.g. the $ characters in ${SOME_VAR} or $SOME_VAR are not escaped

        Args:
            config_variable_name: name of the property
            value: the value to save for the property
            skip_if_substitution_variable: set to False to escape $ in values in substitution variable form e.g. ${SOME_VAR} -> r"\${SOME_VAR}" or $SOME_VAR -> r"\$SOME_VAR"

        Returns:
            None
        """
        config_variables = self.config_variables
        value = self.escape_all_config_variables(
            value,
            self.DOLLAR_SIGN_ESCAPE_STRING,
            skip_if_substitution_variable=skip_if_substitution_variable,
        )
        config_variables[config_variable_name] = value
        # Required to call _variables instead of variables property because we don't want to trigger substitutions
        config = self._variables.config
        config_variables_filepath = config.config_variables_file_path
        if not config_variables_filepath:
            raise ge_exceptions.InvalidConfigError(
                "'config_variables_file_path' property is not found in config - setting it is required to use this feature"
            )

        config_variables_filepath = os.path.join(
            self.root_directory, config_variables_filepath  # type: ignore[arg-type]
        )

        os.makedirs(os.path.dirname(config_variables_filepath), exist_ok=True)
        if not os.path.isfile(config_variables_filepath):
            logger.info(
                "Creating new substitution_variables file at {config_variables_filepath}".format(
                    config_variables_filepath=config_variables_filepath
                )
            )
            with open(config_variables_filepath, "w") as template:
                template.write(CONFIG_VARIABLES_TEMPLATE)

        with open(config_variables_filepath, "w") as config_variables_file:
            yaml.dump(config_variables, config_variables_file)
