import configparser
import copy
import errno
import json
import logging
import os
import sys
import uuid
from abc import ABC, abstractmethod
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Mapping,
    Optional,
    Tuple,
    Union,
    cast,
)

from great_expectations.core.serializer import (
    AbstractConfigSerializer,
    DictConfigSerializer,
)
from great_expectations.datasource.datasource_serializer import (
    NamedDatasourceSerializer,
)

if TYPE_CHECKING:
    from great_expectations.data_context.store import EvaluationParameterStore
    from great_expectations.checkpoint import Checkpoint
    from great_expectations.data_context.store import CheckpointStore

from ruamel.yaml.comments import CommentedMap

import great_expectations.exceptions as ge_exceptions
from great_expectations.core import ExpectationSuite
from great_expectations.core.batch import (
    Batch,
    BatchDefinition,
    BatchRequestBase,
    IDDict,
    get_batch_request_from_acceptable_arguments,
)
from great_expectations.core.expectation_validation_result import get_metric_kwargs_id
from great_expectations.core.metric import ValidationMetricIdentifier
from great_expectations.core.util import nested_update
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context.data_context_variables import DataContextVariables
from great_expectations.data_context.store import Store, TupleStoreBackend
from great_expectations.data_context.store.expectations_store import ExpectationsStore
from great_expectations.data_context.store.profiler_store import ProfilerStore
from great_expectations.data_context.store.validations_store import ValidationsStore
from great_expectations.data_context.types.base import (
    CURRENT_GE_CONFIG_VERSION,
    AnonymizedUsageStatisticsConfig,
    ConcurrencyConfig,
    DataContextConfig,
    DataContextConfigDefaults,
    DatasourceConfig,
    IncludeRenderedContentConfig,
    NotebookConfig,
    ProgressBarsConfig,
    anonymizedUsageStatisticsSchema,
    datasourceConfigSchema,
)
from great_expectations.data_context.types.resource_identifiers import (
    ExpectationSuiteIdentifier,
)
from great_expectations.data_context.util import (
    PasswordMasker,
    build_store_from_config,
    instantiate_class_from_config,
    substitute_all_config_variables,
    substitute_config_variable,
)
from great_expectations.datasource import LegacyDatasource
from great_expectations.datasource.new_datasource import BaseDatasource, Datasource
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.rule_based_profiler.data_assistant.data_assistant_dispatcher import (
    DataAssistantDispatcher,
)
from great_expectations.util import load_class, verify_dynamic_loading_support
from great_expectations.validator.validator import Validator

from great_expectations.core.usage_statistics.usage_statistics import (  # isort: skip
    UsageStatisticsHandler,
    send_usage_message,
)

logger = logging.getLogger(__name__)
yaml = YAMLHandler()


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
    TEST_YAML_CONFIG_SUPPORTED_STORE_TYPES = [
        "ExpectationsStore",
        "ValidationsStore",
        "HtmlSiteStore",
        "EvaluationParameterStore",
        "MetricStore",
        "SqlAlchemyQueryStore",
        "CheckpointStore",
        "ProfilerStore",
    ]
    TEST_YAML_CONFIG_SUPPORTED_DATASOURCE_TYPES = [
        "Datasource",
        "SimpleSqlalchemyDatasource",
    ]
    TEST_YAML_CONFIG_SUPPORTED_DATA_CONNECTOR_TYPES = [
        "InferredAssetFilesystemDataConnector",
        "ConfiguredAssetFilesystemDataConnector",
        "InferredAssetS3DataConnector",
        "ConfiguredAssetS3DataConnector",
        "InferredAssetAzureDataConnector",
        "ConfiguredAssetAzureDataConnector",
        "InferredAssetGCSDataConnector",
        "ConfiguredAssetGCSDataConnector",
        "InferredAssetSqlDataConnector",
        "ConfiguredAssetSqlDataConnector",
    ]
    TEST_YAML_CONFIG_SUPPORTED_CHECKPOINT_TYPES = [
        "Checkpoint",
        "SimpleCheckpoint",
    ]
    TEST_YAML_CONFIG_SUPPORTED_PROFILER_TYPES = [
        "RuleBasedProfiler",
    ]
    ALL_TEST_YAML_CONFIG_DIAGNOSTIC_INFO_TYPES = [
        "__substitution_error__",
        "__yaml_parse_error__",
        "__custom_subclass_not_core_ge__",
        "__class_name_not_provided__",
    ]
    ALL_TEST_YAML_CONFIG_SUPPORTED_TYPES = (
        TEST_YAML_CONFIG_SUPPORTED_STORE_TYPES
        + TEST_YAML_CONFIG_SUPPORTED_DATASOURCE_TYPES
        + TEST_YAML_CONFIG_SUPPORTED_DATA_CONNECTOR_TYPES
        + TEST_YAML_CONFIG_SUPPORTED_CHECKPOINT_TYPES
        + TEST_YAML_CONFIG_SUPPORTED_PROFILER_TYPES
    )
    MIGRATION_WEBSITE: str = "https://docs.greatexpectations.io/docs/guides/miscellaneous/migration_guide#migrating-to-the-batch-request-v3-api"

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

        # These attributes that are set downstream.
        self._variables: Optional[DataContextVariables] = None
        self._config_variables: Optional[dict] = None

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

    @abstractmethod
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
        raise NotImplementedError

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
    def evaluation_parameter_store(self) -> "EvaluationParameterStore":
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
                    f"to the new version number {float(CURRENT_GE_CONFIG_VERSION)}.\n  "
                    f"Visit {AbstractDataContext.MIGRATION_WEBSITE} "
                    f"to learn more about the upgrade process."
                )
            else:
                error_message = (
                    f"Attempted to access the 'checkpoint_store_name' field "
                    f"with no `checkpoints` directory.\n  "
                    f"Please create a `checkpoints` directory in your Great Expectations directory."
                    f"To use the new 'Checkpoint Store' feature, please update your configuration "
                    f"to the new version number {float(CURRENT_GE_CONFIG_VERSION)}.\n  "
                    f"Visit {AbstractDataContext.MIGRATION_WEBSITE} "
                    f"to learn more about the upgrade process."
                )

            raise ge_exceptions.InvalidTopLevelConfigKeyError(error_message)

    @property
    def checkpoint_store(self) -> "CheckpointStore":
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
                    f"to the new version number {float(CURRENT_GE_CONFIG_VERSION)} in order to use the new "
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
                    f"to the new version number {float(CURRENT_GE_CONFIG_VERSION)}.\n  "
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
                    f"to the new version number {float(CURRENT_GE_CONFIG_VERSION)}.\n  "
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
                    f"{float(CURRENT_GE_CONFIG_VERSION)} in order to use the new 'Profiler Store' feature.\n  "
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

        datasource_config_dict: dict = datasourceConfigSchema.dump(datasource.config)
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
        ] = self._instantiate_datasource_from_config(config=substituted_config)
        self._cached_datasources[datasource_name] = updated_datasource
        return updated_datasource

    def add_datasource(
        self,
        name: str,
        initialize: bool = True,
        save_changes: bool = False,
        **kwargs: Optional[dict],
    ) -> Optional[Union[LegacyDatasource, BaseDatasource]]:
        """Add a new datasource to the data context, with configuration provided as kwargs.
        Args:
            name: the name for the new datasource to add
            initialize: if False, add the datasource to the config, but do not
                initialize it, for example if a user needs to debug database connectivity.
            save_changes (bool): should GE save the Datasource config?
            kwargs (keyword arguments): the configuration for the new datasource

        Returns:
            datasource (Datasource)
        """
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

    def get_config_with_variables_substituted(
        self, config: Optional[DataContextConfig] = None
    ) -> DataContextConfig:
        """
        Substitute vars in config of form ${var} or $(var) with values found in the following places,
        in order of precedence: ge_cloud_config (for Data Contexts in GE Cloud mode), runtime_environment,
        environment variables, config_variables, or ge_cloud_config_variable_defaults (allows certain variables to
        be optional in GE Cloud mode).
        """
        if not config:
            config = self._project_config

        substitutions: dict = self._determine_substitutions()

        return DataContextConfig(
            **substitute_all_config_variables(
                config, substitutions, self.DOLLAR_SIGN_ESCAPE_STRING
            )
        )

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

        config: dict = dict(datasourceConfigSchema.dump(datasource_config))
        substitutions: dict = self._determine_substitutions()
        config = substitute_all_config_variables(
            config, substitutions, self.DOLLAR_SIGN_ESCAPE_STRING
        )

        # Instantiate the datasource and add to our in-memory cache of datasources, this does not persist:
        datasource_config = datasourceConfigSchema.load(config)
        datasource: Optional[
            Union[LegacyDatasource, BaseDatasource]
        ] = self._instantiate_datasource_from_config(config=datasource_config)
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
        substitutions: dict = self._determine_substitutions()
        datasource_dict: dict = serializer.serialize(datasource_config)

        substituted_config: dict = cast(
            dict,
            substitute_all_config_variables(
                datasource_dict, substitutions, self.DOLLAR_SIGN_ESCAPE_STRING
            ),
        )
        masked_config: dict = PasswordMasker.sanitize_config(substituted_config)
        return masked_config

    def list_datasources(self) -> List[dict]:
        """List currently-configured datasources on this context. Masks passwords.

        Returns:
            List(dict): each dictionary includes "name", "class_name", and "module_name" keys
        """
        datasources: List[dict] = []

        datasource_name: str
        datasource_config: Union[dict, DatasourceConfig]
        serializer = NamedDatasourceSerializer(schema=datasourceConfigSchema)

        for datasource_name, datasource_config in self.config.datasources.items():  # type: ignore[union-attr,assignment]
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
        self, datasource_name: Optional[str], save_changes: bool = False
    ) -> None:
        """Delete a datasource
        Args:
            datasource_name: The name of the datasource to delete.

        Raises:
            ValueError: If the datasource name isn't provided or cannot be found.
        """
        if datasource_name is None:
            raise ValueError("Datasource names must be a datasource name")

        datasource = self.get_datasource(datasource_name=datasource_name)

        if datasource is None:
            raise ValueError(f"Datasource {datasource_name} not found")

        if save_changes:
            self._datasource_store.delete_by_name(datasource_name)  # type: ignore[attr-defined]
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
    ) -> "Checkpoint":

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

    def list_expectation_suites(self) -> Optional[List[str]]:
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
            raise ValueError(
                "No more than one of expectation_suite_name,"  # type: ignore[attr-defined]
                f"{'expectation_suite_ge_cloud_id,' if self.ge_cloud_mode else ''}"
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
                batch_list.extend(  # type: ignore[union-attr]
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
            batch_list=batch_list,  # type: ignore[arg-type]
            include_rendered_content=include_rendered_content,
        )

    def get_validator_using_batch_list(
        self,
        expectation_suite: ExpectationSuite,
        batch_list: List[Batch],
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
        batch_definition: BatchDefinition = batch_list[-1].batch_definition
        execution_engine: ExecutionEngine = self.datasources[  # type: ignore[union-attr]
            batch_definition.datasource_name
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
        self.expectations_store.set(key, expectation_suite, **kwargs)  # type: ignore[arg-type]
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
        """Get an Expectation Suite by name or GE Cloud ID
        Args:
            expectation_suite_name (str): The name of the Expectation Suite
            include_rendered_content (bool): Whether or not to re-populate rendered_content for each
                ExpectationConfiguration.
            ge_cloud_id (str): The GE Cloud ID for the Expectation Suite.

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

    def store_validation_result_metrics(
        self, requested_metrics, validation_results, target_store_name
    ) -> None:
        """

        Args:
            requested_metrics ():
            validation_results ():
            target_store_name ():

        Returns:

        """
        self._store_metrics(requested_metrics, validation_results, target_store_name)

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
        self, config: Union[DataContextConfig, Mapping]
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
        config_with_global_config_overrides: DataContextConfig = copy.deepcopy(config)  # type: ignore[assignment]
        usage_stats_opted_out: bool = self._check_global_usage_statistics_opt_out()
        # if usage_stats_opted_out then usage_statistics is false
        # NOTE: <DataContextRefactor> 202207 Refactor so that this becomes usage_stats_enabled
        # (and we don't have to flip a boolean in our minds)
        if usage_stats_opted_out:
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
        """
        Get all config variables from the default location. For Data Contexts in GE Cloud mode, config variables
        have already been interpolated before being sent from the Cloud API.

        """
        config_variables_file_path = self._project_config.config_variables_file_path
        if config_variables_file_path:
            try:
                # If the user specifies the config variable path with an environment variable, we want to substitute it
                defined_path: str = substitute_config_variable(  # type: ignore[assignment]
                    config_variables_file_path, dict(os.environ)
                )
                if not os.path.isabs(defined_path) and hasattr(self, "root_directory"):
                    # A BaseDataContext will not have a root directory; in that case use the current directory
                    # for any non-absolute path
                    root_directory: str = self.root_directory or os.curdir
                else:
                    root_directory = ""
                var_path = os.path.join(root_directory, defined_path)
                with open(var_path) as config_variables_file:
                    res = dict(yaml.load(config_variables_file.read()))
                    return res or {}
            except OSError as e:
                if e.errno != errno.ENOENT:
                    raise
                return {}
        else:
            return {}

    def _check_global_usage_statistics_opt_out(self) -> bool:
        """
        Method to retrieve config value.
        This method can be overridden in child classes (like FileDataContext) when we need to look for
        config values in other locations like config files.

        Returns:
            bool that tells you whether usage_statistics is opted out
        """
        # NOTE: <DataContextRefactor> Refactor so that opt_out is no longer used, and we don't have to flip boolean in
        # our minds.
        if os.environ.get("GE_USAGE_STATS", False):
            ge_usage_stats = os.environ.get("GE_USAGE_STATS")
            if ge_usage_stats in AbstractDataContext.FALSEY_STRINGS:
                return True
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
                if config.getboolean("anonymous_usage_statistics", "enabled") is False:
                    # If stats are disabled, then opt out is true
                    return True
            except (ValueError, configparser.Error):
                pass
        return False

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

        # By always recalculating substitutions with each call, we ensure we stay up-to-date
        # with the latest changes to env vars and config vars
        substitutions: dict = self._determine_substitutions()
        self._variables.substitutions = substitutions

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
    def datasources(self) -> Dict[str, Union[LegacyDatasource, BaseDatasource]]:
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

    def _determine_substitutions(self) -> Dict:
        """Aggregates substitutions from the project's config variables file, any environment variables, and
        the runtime environment.

        Returns: A dictionary containing all possible substitutions that can be applied to a given object
                 using `substitute_all_config_variables`.
        """
        substituted_config_variables: dict = substitute_all_config_variables(
            self.config_variables,
            dict(os.environ),
            self.DOLLAR_SIGN_ESCAPE_STRING,
        )

        substitutions: dict = {
            **substituted_config_variables,
            **dict(os.environ),
            **self.runtime_environment,
        }

        return substitutions

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
        config: DataContextConfig = self.get_config_with_variables_substituted(
            self.config
        )
        datasources: Dict[str, DatasourceConfig] = cast(
            Dict[str, DatasourceConfig], config.datasources
        )

        for datasource_name, datasource_config in datasources.items():
            try:
                config = copy.deepcopy(datasource_config)  # type: ignore[assignment]
                config_dict = dict(datasourceConfigSchema.dump(config))
                datasource_config = datasourceConfigSchema.load(config_dict)
                datasource_config.name = datasource_name
                datasource = self._instantiate_datasource_from_config(
                    config=datasource_config
                )
                self._cached_datasources[datasource_name] = datasource
            except ge_exceptions.DatasourceInitializationError as e:
                logger.warning(f"Cannot initialize datasource {datasource_name}: {e}")
                # this error will happen if our configuration contains datasources that GE can no longer connect to.
                # this is ok, as long as we don't use it to retrieve a batch. If we try to do that, the error will be
                # caught at the context.get_batch() step. So we just pass here.
                pass

    def _instantiate_datasource_from_config(
        self, config: DatasourceConfig
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
            datasource: Datasource = self._build_datasource_from_config(config=config)
        except Exception as e:
            raise ge_exceptions.DatasourceInitializationError(
                datasource_name=config.name, message=str(e)
            )
        return datasource

    def _build_datasource_from_config(self, config: DatasourceConfig) -> Datasource:
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
        config_dict: dict = serializer.serialize(config)

        # While the new Datasource classes accept "data_context_root_directory", the Legacy Datasource classes do not.
        if config_dict["class_name"] in [
            "BaseDatasource",
            "Datasource",
        ]:
            config_dict.update({"data_context_root_directory": self.root_directory})  # type: ignore[union-attr]
        module_name: str = "great_expectations.datasource"
        datasource: Datasource = instantiate_class_from_config(
            config=config_dict,
            runtime_environment={"data_context": self, "concurrency": self.concurrency},
            config_defaults={"module_name": module_name},
        )
        if not datasource:
            raise ge_exceptions.ClassInstantiationError(
                module_name=module_name,
                package_name=None,
                class_name=config["class_name"],
            )
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
        substitutions: dict = self._determine_substitutions()

        substitution_serializer = DictConfigSerializer(schema=datasourceConfigSchema)
        raw_config: dict = substitution_serializer.serialize(config)

        substituted_config_dict: dict = substitute_all_config_variables(
            raw_config, substitutions, self.DOLLAR_SIGN_ESCAPE_STRING
        )

        substituted_config: DatasourceConfig = datasourceConfigSchema.load(
            substituted_config_dict
        )

        return substituted_config

    def _instantiate_datasource_from_config_and_update_project_config(
        self,
        config: DatasourceConfig,
        initialize: bool = True,
        save_changes: bool = False,
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
        if save_changes:
            config = self._datasource_store.set(key=None, value=config)  # type: ignore[attr-defined]

        self.config.datasources[config.name] = config  # type: ignore[index,assignment]

        substituted_config = self._perform_substitutions_on_datasource_config(config)

        datasource: Optional[Datasource] = None
        if initialize:
            try:
                datasource = self._instantiate_datasource_from_config(
                    config=substituted_config
                )
                self._cached_datasources[config.name] = datasource
            except ge_exceptions.DatasourceInitializationError as e:
                # Do not keep configuration that could not be instantiated.
                if save_changes:
                    self._datasource_store.delete(config)  # type: ignore[attr-defined]
                # If the DatasourceStore uses an InlineStoreBackend, the config may already be updated
                self.config.datasources.pop(config.name, None)  # type: ignore[union-attr,arg-type]
                raise e

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
