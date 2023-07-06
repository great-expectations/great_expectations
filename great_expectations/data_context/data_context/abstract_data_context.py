from __future__ import annotations

import configparser
import copy
import datetime
import json
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
    Literal,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
    overload,
)

from marshmallow import ValidationError
from ruamel.yaml.comments import CommentedMap

import great_expectations.exceptions as gx_exceptions
from great_expectations.compatibility import sqlalchemy
from great_expectations.core import ExpectationSuite
from great_expectations.core._docs_decorators import (
    deprecated_argument,
    deprecated_method_or_class,
    new_argument,
    new_method_or_class,
    public_api,
)
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
from great_expectations.core.id_dict import BatchKwargs
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
from great_expectations.data_context.store import Store, TupleStoreBackend
from great_expectations.data_context.store.profiler_store import ProfilerStore
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
    ValidationMetricIdentifier,
    ValidationResultIdentifier,
)
from great_expectations.data_context.util import (
    PasswordMasker,
    instantiate_class_from_config,
    parse_substitution_variable,
)
from great_expectations.dataset.dataset import Dataset
from great_expectations.datasource import LegacyDatasource
from great_expectations.datasource.datasource_serializer import (
    NamedDatasourceSerializer,
)
from great_expectations.datasource.fluent.config import GxConfig
from great_expectations.datasource.fluent.interfaces import Batch as FluentBatch
from great_expectations.datasource.fluent.interfaces import (
    Datasource as FluentDatasource,
)
from great_expectations.datasource.fluent.sources import _SourceFactories
from great_expectations.datasource.new_datasource import BaseDatasource, Datasource
from great_expectations.profile.basic_dataset_profiler import BasicDatasetProfiler
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
from great_expectations.checkpoint import Checkpoint

SQLAlchemyError = sqlalchemy.SQLAlchemyError
if not SQLAlchemyError:
    # We'll redefine this error in code below to catch ProfilerError, which is caught above, so SA errors will
    # just fall through
    SQLAlchemyError = gx_exceptions.ProfilerError


if TYPE_CHECKING:
    from typing_extensions import TypeAlias

    from great_expectations.checkpoint.configurator import ActionDict
    from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
    from great_expectations.core.expectation_configuration import (
        ExpectationConfiguration,
    )
    from great_expectations.data_context.data_context_variables import (
        DataContextVariables,
    )
    from great_expectations.data_context.store import (
        CheckpointStore,
        EvaluationParameterStore,
    )
    from great_expectations.data_context.store.datasource_store import DatasourceStore
    from great_expectations.data_context.store.expectations_store import (
        ExpectationsStore,
    )
    from great_expectations.data_context.store.store import (
        DataDocsSiteConfigTypedDict,
        StoreConfigTypedDict,
    )
    from great_expectations.data_context.store.validations_store import ValidationsStore
    from great_expectations.data_context.types.resource_identifiers import (
        GXCloudIdentifier,
    )
    from great_expectations.datasource.fluent.interfaces import (
        BatchRequest as FluentBatchRequest,
    )
    from great_expectations.datasource.fluent.interfaces import (
        BatchRequestOptions,
    )
    from great_expectations.execution_engine import ExecutionEngine
    from great_expectations.render.renderer.site_builder import SiteBuilder
    from great_expectations.rule_based_profiler import RuleBasedProfilerResult
    from great_expectations.validation_operators.validation_operators import (
        ValidationOperator,
    )

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
       :widths: 10 18 18 18 18 18
       :header-rows: 1

       * -
         - Stores
         - Datasources
         - ExpectationSuites
         - Checkpoints
         - Profilers
       * - `get`
         - ❌
         - ✅
         - ✅
         - ✅
         - ✅
       * - `add`
         - ✅
         - ✅
         - ✅
         - ✅
         - ✅
       * - `update`
         - ❌
         - ✅
         - ✅
         - ✅
         - ✅
       * - `add_or_update`
         - ❌
         - ✅
         - ✅
         - ✅
         - ✅
       * - `delete`
         - ✅
         - ✅
         - ✅
         - ✅
         - ✅
    """

    # NOTE: <DataContextRefactor> These can become a property like ExpectationsStore.__name__ or placed in a separate
    # test_yml_config module so AbstractDataContext is not so cluttered.
    FALSEY_STRINGS = ["FALSE", "false", "False", "f", "F", "0"]
    _ROOT_CONF_DIR = pathlib.Path.home() / ".great_expectations"
    _ROOT_CONF_FILE = _ROOT_CONF_DIR / "great_expectations.conf"
    _ETC_CONF_DIR = pathlib.Path("/etc")
    _ETC_CONF_FILE = _ETC_CONF_DIR / "great_expectations.conf"
    GLOBAL_CONFIG_PATHS = [_ROOT_CONF_FILE, _ETC_CONF_FILE]
    DOLLAR_SIGN_ESCAPE_STRING = r"\$"
    MIGRATION_WEBSITE: str = "https://docs.greatexpectations.io/docs/guides/miscellaneous/migration_guide#migrating-to-the-batch-request-v3-api"

    PROFILING_ERROR_CODE_TOO_MANY_DATA_ASSETS = 2
    PROFILING_ERROR_CODE_SPECIFIED_DATA_ASSETS_NOT_FOUND = 3
    PROFILING_ERROR_CODE_NO_BATCH_KWARGS_GENERATORS_FOUND = 4
    PROFILING_ERROR_CODE_MULTIPLE_BATCH_KWARGS_GENERATORS_FOUND = 5

    # instance attribute type annotations
    fluent_config: GxConfig

    @usage_statistics_enabled_method(
        event_name=UsageStatsEvents.DATA_CONTEXT___INIT__,
    )
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

        # config providers must be provisioned before loading zep_config
        self.fluent_config = self._load_fluent_config(self._config_provider)

        # Init plugin support
        if self.plugins_directory is not None and os.path.exists(  # noqa: PTH110
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
        self._init_primary_stores(self.project_config_with_variables_substituted.stores)
        # The DatasourceStore is inherent to all DataContexts but is not an explicit part of the project config.
        # As such, it must be instantiated separately.
        self._datasource_store = self._init_datasource_store()

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

        self._sources: _SourceFactories = _SourceFactories(self)

        # NOTE - 20210112 - Alex Sherstinsky - Validation Operators are planned to be deprecated.
        self.validation_operators: dict = {}
        if (
            "validation_operators" in self.get_config().commented_map  # type: ignore[union-attr]
            and self.config.validation_operators
        ):
            for (
                validation_operator_name,
                validation_operator_config,
            ) in self.config.validation_operators.items():
                self.add_validation_operator(
                    validation_operator_name,
                    validation_operator_config,
                )

        self._attach_fluent_config_datasources_and_build_data_connectors(
            self.fluent_config
        )

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
    def _init_project_config(
        self, project_config: DataContextConfig | Mapping
    ) -> DataContextConfig:
        raise NotImplementedError

    @abstractmethod
    def _init_variables(self) -> DataContextVariables:
        raise NotImplementedError

    def _save_project_config(
        self, _fds_datasource: FluentDatasource | None = None
    ) -> None:
        """
        Each DataContext will define how its project_config will be saved through its internal 'variables'.
            - FileDataContext : Filesystem.
            - CloudDataContext : Cloud endpoint
            - Ephemeral : not saved, and logging message outputted
        """
        self.variables.save_config()

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

    @public_api
    @deprecated_method_or_class(
        version="0.15.48", message="Part of the deprecated DataContext CRUD API"
    )
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
        """Save the provided ExpectationSuite into the DataContext using the configured ExpectationStore.

        Args:
            expectation_suite: The ExpectationSuite to save.
            expectation_suite_name: The name of this ExpectationSuite. If no name is provided, the name will be read
                from the suite.
            overwrite_existing: Whether to overwrite the suite if it already exists.
            include_rendered_content: Whether to save the prescriptive rendered content for each expectation.
            kwargs: Additional parameters, unused

        Returns:
            None

        Raises:
            DataContextError: If a suite with the same name exists and `overwrite_existing` is set to `False`.
        """
        # deprecated-v0.15.48
        warnings.warn(
            "save_expectation_suite is deprecated as of v0.15.48 and will be removed in v0.18. "
            "Please use update_expectation_suite or add_or_update_expectation_suite instead.",
            DeprecationWarning,
        )
        return self._save_expectation_suite(
            expectation_suite=expectation_suite,
            expectation_suite_name=expectation_suite_name,
            overwrite_existing=overwrite_existing,
            include_rendered_content=include_rendered_content,
            **kwargs,
        )

    def _save_expectation_suite(
        self,
        expectation_suite: ExpectationSuite,
        expectation_suite_name: Optional[str] = None,
        overwrite_existing: bool = True,
        include_rendered_content: Optional[bool] = None,
        **kwargs: Optional[dict],
    ) -> None:
        if expectation_suite_name is None:
            key = ExpectationSuiteIdentifier(
                expectation_suite_name=expectation_suite.expectation_suite_name
            )
        else:
            expectation_suite.expectation_suite_name = expectation_suite_name
            key = ExpectationSuiteIdentifier(
                expectation_suite_name=expectation_suite_name
            )
        if self.expectations_store.has_key(key) and not overwrite_existing:  # : @601
            raise gx_exceptions.DataContextError(
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
    def root_directory(self) -> Optional[str]:  # TODO: This should be a `pathlib.Path`
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

    @expectations_store_name.setter
    @public_api
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
    def evaluation_parameter_store_name(self) -> Optional[str]:
        return self.variables.evaluation_parameter_store_name

    @property
    def evaluation_parameter_store(self) -> EvaluationParameterStore:
        return self.stores[self.evaluation_parameter_store_name]

    @property
    def validations_store_name(self) -> Optional[str]:
        return self.variables.validations_store_name

    @validations_store_name.setter
    @public_api
    @new_method_or_class(version="0.17.2")
    def validations_store_name(self, value: str) -> None:
        """Set the name of the validations store.

        Args:
            value: New value for the validations store name.
        """
        self.variables.validations_store_name = value
        self._save_project_config()

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
                checkpoint_store_directory: str = os.path.join(  # noqa: PTH118
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

            raise gx_exceptions.InvalidTopLevelConfigKeyError(error_message)

    @checkpoint_store_name.setter
    @public_api
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
            raise gx_exceptions.StoreConfigurationError(
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
                checkpoint_store_directory: str = os.path.join(  # noqa: PTH118
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

            raise gx_exceptions.InvalidTopLevelConfigKeyError(error_message)

    @profiler_store_name.setter
    @public_api
    @new_method_or_class(version="0.17.2")
    def profiler_store_name(self, value: str) -> None:
        """Set the name of the profiler store.

        Args:
            value: New value for the profiler store name.
        """
        self.variables.profiler_store_name = value
        self._save_project_config()

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
                built_store: Store = self._build_store_from_config(
                    profiler_store_name,  # type: ignore[arg-type]
                    DataContextConfigDefaults.DEFAULT_STORES.value[profiler_store_name],  # type: ignore[index,arg-type]
                )
                return cast(ProfilerStore, built_store)

            raise gx_exceptions.StoreConfigurationError(
                f"Attempted to access the Profiler store: '{profiler_store_name}'. It is not a configured store."
            )

    @property
    def concurrency(self) -> Optional[ConcurrencyConfig]:
        return self.variables.concurrency

    @property
    def assistants(self) -> DataAssistantDispatcher:
        return self._assistants

    @property
    def sources(self) -> _SourceFactories:
        return self._sources

    def _add_fluent_datasource(
        self, datasource: Optional[FluentDatasource] = None, **kwargs
    ) -> FluentDatasource:
        if datasource:
            from_config: bool = False
            datasource_name = datasource.name
        else:
            from_config = True
            datasource_name = kwargs.get("name", "")

        if not datasource_name:
            raise gx_exceptions.DataContextError(
                "Can not write the fluent datasource, because no name was provided."
            )

        # We currently don't allow one to overwrite a datasource with this internal method
        if datasource_name in self.datasources:
            raise gx_exceptions.DataContextError(
                f"Can not write the fluent datasource {datasource_name} because a datasource of that "
                "name already exists in the data context."
            )

        if not datasource:
            ds_type = _SourceFactories.type_lookup[kwargs["type"]]
            datasource = ds_type(**kwargs)

        assert isinstance(datasource, FluentDatasource)

        datasource._data_context = self

        datasource._data_context._save_project_config()

        # temporary workaround while we update stores to work better with Fluent Datasources for all contexts
        # Without this we end up with duplicate entries for datasources in both
        # "fluent_datasources" and "datasources" config/yaml entries.
        if self._datasource_store.cloud_mode and not from_config:
            set_datasource = self._datasource_store.set(key=None, value=datasource)
            if set_datasource.id:
                logger.debug(f"Assigning `id` to '{datasource_name}'")
                datasource.id = set_datasource.id
        self.datasources[datasource_name] = datasource
        return datasource

    def _update_fluent_datasource(
        self, datasource: Optional[FluentDatasource] = None, **kwargs
    ) -> None:
        if datasource:
            datasource_name = datasource.name
        else:
            datasource_name = kwargs.get("name", "")

        if not datasource_name:
            raise gx_exceptions.DataContextError(
                "Can not write the fluent datasource, because no name was provided."
            )

        if not datasource:
            ds_type = _SourceFactories.type_lookup[kwargs["type"]]
            update_datasource = ds_type(**kwargs)
        else:
            update_datasource = datasource

        update_datasource._data_context = self

        update_datasource._rebuild_asset_data_connectors()

        update_datasource.test_connection()
        update_datasource._data_context._save_project_config()

        self.datasources[datasource_name] = update_datasource

    def _delete_fluent_datasource(
        self, datasource_name: str, _call_store: bool = True
    ) -> None:
        """
        _call_store = False allows for local deletes without deleting the persisted storage datasource.
        This should generally be avoided.
        """
        self.fluent_config.pop(datasource_name, None)
        datasource = self.datasources.get(datasource_name)
        if datasource:
            if self._datasource_store.cloud_mode and _call_store:
                self._datasource_store.delete(datasource)  # type: ignore[arg-type] # Could be a LegacyDatasource
        else:
            # Raise key error instead?
            logger.info(f"No Datasource '{datasource_name}' to delete")
        self.datasources.pop(datasource_name, None)

    def set_config(self, project_config: DataContextConfig) -> None:
        self._project_config = project_config
        self.variables.config = project_config

    @deprecated_method_or_class(
        version="0.15.48", message="Part of the deprecated DataContext CRUD API"
    )
    def save_datasource(
        self, datasource: BaseDatasource | FluentDatasource | LegacyDatasource
    ) -> BaseDatasource | FluentDatasource | LegacyDatasource:
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
        # deprecated-v0.15.48
        warnings.warn(
            "save_datasource is deprecated as of v0.15.48 and will be removed in v0.18. "
            "Please use update_datasource or add_or_update_datasource instead.",
            DeprecationWarning,
        )
        return self.add_or_update_datasource(datasource=datasource)

    @overload
    def add_datasource(
        self,
        name: str = ...,
        initialize: bool = ...,
        save_changes: bool | None = ...,
        datasource: None = ...,
        **kwargs,
    ) -> BaseDatasource | FluentDatasource | LegacyDatasource | None:
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
        save_changes: bool | None = ...,
        datasource: BaseDatasource | FluentDatasource | LegacyDatasource = ...,
        **kwargs,
    ) -> BaseDatasource | FluentDatasource | LegacyDatasource | None:
        """
        A `datasource` is provided.
        `name` should not be provided.
        """
        ...

    @public_api
    @deprecated_argument(argument_name="save_changes", version="0.15.32")
    @new_argument(
        argument_name="datasource",
        version="0.15.49",
        message="Pass in an existing Datasource instead of individual constructor arguments",
    )
    @usage_statistics_enabled_method(
        event_name=UsageStatsEvents.DATA_CONTEXT_ADD_DATASOURCE,
        args_payload_fn=add_datasource_usage_statistics,
    )
    def add_datasource(
        self,
        name: str | None = None,
        initialize: bool = True,
        save_changes: bool | None = None,
        datasource: BaseDatasource | FluentDatasource | LegacyDatasource | None = None,
        **kwargs,
    ) -> BaseDatasource | FluentDatasource | LegacyDatasource | None:
        """Add a new Datasource to the data context, with configuration provided as kwargs.

        --Documentation--
            - https://docs.greatexpectations.io/docs/terms/datasource

        Args:
            name: the name of the new Datasource to add
            initialize: if False, add the Datasource to the config, but do not
                initialize it, for example if a user needs to debug database connectivity.
            save_changes: should GX save the Datasource config?
            datasource: an existing Datasource you wish to persist
            kwargs: the configuration for the new Datasource

        Returns:
            Datasource instance added.
        """
        return self._add_datasource(
            name=name,
            initialize=initialize,
            save_changes=save_changes,
            datasource=datasource,
            **kwargs,
        )

    @staticmethod
    def _validate_add_datasource_args(
        name: str | None,
        datasource: BaseDatasource | FluentDatasource | LegacyDatasource | None,
    ) -> None:
        if not ((datasource is None) ^ (name is None)):
            raise ValueError(
                "Must either pass in an existing datasource or individual constructor arguments (but not both)"
            )

    def _add_datasource(
        self,
        name: str | None = None,
        initialize: bool = True,
        save_changes: bool | None = None,
        datasource: BaseDatasource | FluentDatasource | LegacyDatasource | None = None,
        **kwargs,
    ) -> BaseDatasource | FluentDatasource | LegacyDatasource | None:
        self._validate_add_datasource_args(name=name, datasource=datasource)
        if isinstance(datasource, FluentDatasource):
            self._add_fluent_datasource(
                datasource=datasource,
            )
        else:
            datasource = self._add_block_config_datasource(
                name=name,
                initialize=initialize,
                save_changes=save_changes,
                datasource=datasource,
                **kwargs,
            )
        return datasource

    def _add_block_config_datasource(
        self,
        name: str | None = None,
        initialize: bool = True,
        save_changes: bool | None = None,
        datasource: BaseDatasource | LegacyDatasource | None = None,
        **kwargs,
    ) -> BaseDatasource | LegacyDatasource | None:
        save_changes = self._determine_save_changes_flag(save_changes)

        logger.debug(f"Starting AbstractDataContext.add_datasource for {name}")

        if datasource:
            config = datasource.config
        else:
            module_name: str = kwargs.get(
                "module_name", "great_expectations.datasource"
            )
            verify_dynamic_loading_support(module_name=module_name)
            class_name = kwargs.get("class_name", "Datasource")
            datasource_class = load_class(
                class_name=class_name, module_name=module_name
            )

            # For any class that should be loaded, it may control its configuration construction
            # by implementing a classmethod called build_configuration
            if hasattr(datasource_class, "build_configuration"):
                config = datasource_class.build_configuration(**kwargs)
            else:
                config = kwargs

        datasource_config: DatasourceConfig = datasourceConfigSchema.load(
            CommentedMap(**config)
        )
        datasource_config.name = name or datasource_config.name

        return self._instantiate_datasource_from_config_and_update_project_config(
            config=datasource_config,
            initialize=initialize,
            save_changes=save_changes,
        )

    @public_api
    def update_datasource(
        self,
        datasource: BaseDatasource | FluentDatasource | LegacyDatasource,
        save_changes: bool | None = None,
    ) -> BaseDatasource | FluentDatasource | LegacyDatasource:
        """Updates a Datasource that already exists in the store.

        Args:
            datasource: The Datasource object to update.
            save_changes: do I save changes to disk?

        Returns:
            The updated Datasource.
        """
        save_changes = self._determine_save_changes_flag(save_changes)
        if isinstance(datasource, FluentDatasource):
            self._update_fluent_datasource(datasource=datasource)
        else:
            datasource = self._update_block_config_datasource(
                datasource=datasource, save_changes=save_changes
            )
        return datasource

    def _update_block_config_datasource(
        self,
        datasource: LegacyDatasource | BaseDatasource,
        save_changes: bool,
    ) -> BaseDatasource | LegacyDatasource:
        name = datasource.name
        config = datasource.config
        # `instantiate_class_from_config` requires `class_name`
        config["class_name"] = datasource.__class__.__name__

        datasource_config_dict: dict = datasourceConfigSchema.dump(config)
        datasource_config = DatasourceConfig(**datasource_config_dict)

        if save_changes:
            self._datasource_store.update_by_name(
                datasource_name=name, datasource_config=datasource_config
            )

        updated_datasource = (
            self._instantiate_datasource_from_config_and_update_project_config(
                config=datasource_config,
                initialize=True,
                save_changes=False,
            )
        )

        # Invariant based on `initalize=True` above
        assert updated_datasource is not None

        return updated_datasource

    @overload
    def add_or_update_datasource(
        self,
        name: str = ...,
        datasource: None = ...,
        **kwargs,
    ) -> BaseDatasource | FluentDatasource | LegacyDatasource:
        """
        A `name` is provided.
        `datasource` should not be provided.
        """
        ...

    @overload
    def add_or_update_datasource(
        self,
        name: None = ...,
        datasource: BaseDatasource | FluentDatasource | LegacyDatasource = ...,
        **kwargs,
    ) -> BaseDatasource | FluentDatasource | LegacyDatasource:
        """
        A `datasource` is provided.
        `name` should not be provided.
        """
        ...

    @public_api
    @new_method_or_class(version="0.15.48")
    def add_or_update_datasource(
        self,
        name: str | None = None,
        datasource: BaseDatasource | FluentDatasource | LegacyDatasource | None = None,
        **kwargs,
    ) -> BaseDatasource | FluentDatasource | LegacyDatasource:
        """Add a new Datasource or update an existing one on the context depending on whether
        it already exists or not. The configuration is provided as kwargs.

        Args:
            name: The name of the Datasource to add or update.
            datasource: an existing Datasource you wish to persist.
            kwargs: Any relevant keyword args to use when adding or updating the target Datasource named `name`.

        Returns:
            The Datasource added or updated by the input `kwargs`.
        """
        self._validate_add_datasource_args(name=name, datasource=datasource)
        return_datasource: BaseDatasource | FluentDatasource | LegacyDatasource
        if "type" in kwargs:
            assert name, 'Fluent Datasource kwargs must include the keyword "name"'
            kwargs["name"] = name
            if name in self.datasources:
                self._update_fluent_datasource(**kwargs)
            else:
                self._add_fluent_datasource(**kwargs)
            return_datasource = self.datasources[name]
        elif isinstance(datasource, FluentDatasource):
            if datasource.name in self.datasources:
                self._update_fluent_datasource(datasource=datasource)
            else:
                self._add_fluent_datasource(datasource=datasource)
            return_datasource = self.datasources[datasource.name]
        else:
            block_config_datasource = self._add_block_config_datasource(
                name=name,
                datasource=datasource,
                initialize=True,
                **kwargs,
            )
            assert block_config_datasource is not None
            return_datasource = block_config_datasource

        return return_datasource

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

    @public_api
    def get_batch(  # noqa: PLR0912
        self, arg1: Any = None, arg2: Any = None, arg3: Any = None, **kwargs
    ) -> Union[Batch, DataAsset]:
        """Get exactly one batch, based on a variety of flexible input types.

        The method `get_batch` is the main user-facing method for getting batches; it supports both the new (V3) and the
        Legacy (V2) Datasource schemas.  The version-specific implementations are contained in "_get_batch_v2()" and
        "_get_batch_v3()", respectively, both of which are in the present module.

        For the V3 API parameters, please refer to the signature and parameter description of method "_get_batch_v3()".
        For the Legacy usage, please refer to the signature and parameter description of the method "_get_batch_v2()".

        Processing Steps:
            1. Determine the version (possible values are "v3" or "v2").
            2. Convert the positional arguments to the appropriate named arguments, based on the version.
            3. Package the remaining arguments as variable keyword arguments (applies only to V3).
            4. Call the version-specific method ("_get_batch_v3()" or "_get_batch_v2()") with the appropriate arguments.

        Args:
            arg1: the first positional argument (can take on various types)
            arg2: the second positional argument (can take on various types)
            arg3: the third positional argument (can take on various types)

            **kwargs: variable arguments

        Returns:
            Batch (V3) or DataAsset (V2) -- the requested batch
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

    def _get_data_context_version(  # noqa: PLR0912
        self, arg1: Any, **kwargs
    ) -> Optional[str]:
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
            datasource: LegacyDatasource | BaseDatasource | FluentDatasource = (
                self.get_datasource(datasource_name=datasource_name)
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
            raise gx_exceptions.BatchKwargsError(
                "BatchKwargs must be a BatchKwargs object or dictionary."
            )

        if not isinstance(
            expectation_suite_name, (ExpectationSuite, ExpectationSuiteIdentifier, str)
        ):
            raise gx_exceptions.DataContextError(
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

        datasource_name: Optional[Any] = batch_kwargs.get("datasource")
        datasource: LegacyDatasource | BaseDatasource | FluentDatasource
        if isinstance(datasource_name, str):
            datasource = self.get_datasource(datasource_name)
        else:
            datasource = self.get_datasource(None)  #  type: ignore[arg-type]
        assert not isinstance(
            datasource, FluentDatasource
        ), "Fluent Datasource cannot be built from batch_kwargs"
        batch = datasource.get_batch(  #  type: ignore[union-attr]
            batch_kwargs=batch_kwargs, batch_parameters=batch_parameters
        )
        if data_asset_type is None:
            data_asset_type = datasource.config.get("data_asset_type")

        validator = BridgeValidator(
            batch=batch,
            expectation_suite=expectation_suite,
            expectation_engine=data_asset_type,
        )
        return validator.get_dataset()

    def _get_batch_v3(  # noqa: PLR0913
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
        except (AttributeError, gx_exceptions.InvalidTopLevelConfigKeyError):
            logger.info(
                "Checkpoint store is not configured; omitting it from active stores"
            )

        try:
            active_store_names.append(self.profiler_store_name)  # type: ignore[arg-type]
        except (AttributeError, gx_exceptions.InvalidTopLevelConfigKeyError):
            logger.info(
                "Profiler store is not configured; omitting it from active stores"
            )

        return [
            store
            for store in self.list_stores()
            if store.get("name") in active_store_names  # type: ignore[arg-type,operator]
        ]

    @public_api
    def list_checkpoints(self) -> Union[List[str], List[ConfigurationIdentifier]]:
        """List existing Checkpoint identifiers on this context.

        Returns:
            Either a list of strings or ConfigurationIdentifiers depending on the environment and context type.
        """
        return self.checkpoint_store.list_checkpoints()

    def list_profilers(self) -> Union[List[str], List[ConfigurationIdentifier]]:
        """List existing Profiler identifiers on this context.

        Returns:
            Either a list of strings or ConfigurationIdentifiers depending on the environment and context type.
        """
        return RuleBasedProfiler.list_profilers(self.profiler_store)

    @deprecated_method_or_class(
        version="0.15.48", message="Part of the deprecated DataContext CRUD API"
    )
    def save_profiler(
        self,
        profiler: RuleBasedProfiler,
    ) -> RuleBasedProfiler:
        """Save an existing Profiler object utilizing the context's underlying ProfilerStore.

        Args:
            profiler: The Profiler object to persist.

        Returns:
            The input profiler - may be modified with an id depending on the backing store.
        """
        # deprecated-v0.15.48
        warnings.warn(
            "save_profiler is deprecated as of v0.15.48 and will be removed in v0.18. "
            "Please use update_profiler or add_or_update_profiler instead.",
            DeprecationWarning,
        )
        name = profiler.name
        ge_cloud_id = profiler.ge_cloud_id
        key = self._determine_key_for_profiler_save(name=name, id=ge_cloud_id)

        response = self.profiler_store.set(key=key, value=profiler.config)  # type: ignore[func-returns-value]
        if isinstance(response, GXCloudResourceRef):
            ge_cloud_id = response.id

        # If an id is present, we want to prioritize that as our key for object retrieval
        if ge_cloud_id:
            name = None  # type: ignore[assignment]

        profiler = self.get_profiler(name=name, ge_cloud_id=ge_cloud_id)
        return profiler

    def _determine_key_for_profiler_save(
        self, name: str, id: Optional[str]
    ) -> Union[ConfigurationIdentifier, GXCloudIdentifier]:
        return ConfigurationIdentifier(configuration_key=name)

    @public_api
    def get_datasource(
        self, datasource_name: str = "default"
    ) -> BaseDatasource | FluentDatasource | LegacyDatasource:
        """Retrieve a given Datasource by name from the context's underlying DatasourceStore.

        Args:
            datasource_name: The name of the target datasource.

        Returns:
            The target datasource.

        Raises:
            ValueError: The input `datasource_name` is None.
        """
        if datasource_name is None:
            raise ValueError(
                "Must provide a datasource_name to retrieve an existing Datasource"
            )

        if datasource_name in self._cached_datasources:
            self._cached_datasources[datasource_name]._data_context = self
            return self._cached_datasources[datasource_name]

        datasource_config: DatasourceConfig | FluentDatasource = (
            self._datasource_store.retrieve_by_name(datasource_name=datasource_name)
        )

        datasource: BaseDatasource | LegacyDatasource | FluentDatasource
        if isinstance(datasource_config, FluentDatasource):
            datasource = datasource_config
            datasource_config._data_context = self
            # Fluent Datasource has already been hydrated into runtime model and uses lazy config substitution
        else:
            raw_config_dict: dict = dict(datasourceConfigSchema.dump(datasource_config))
            raw_config = datasourceConfigSchema.load(raw_config_dict)

            substituted_config = self.config_provider.substitute_config(raw_config_dict)

            # Instantiate the datasource and add to our in-memory cache of datasources, this does not persist:
            datasource = self._instantiate_datasource_from_config(
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

    @public_api
    def add_store(self, store_name: str, store_config: StoreConfigTypedDict) -> Store:
        """Add a new Store to the DataContext.

        Args:
            store_name: the name to associate with the created store.
            store_config: the config to use to construct the store.

        Returns:
            The instantiated Store.
        """
        store = self._build_store_from_config(store_name, store_config)

        # Both the config and the actual stores need to be kept in sync
        self.config.stores[store_name] = store_config
        self._stores[store_name] = store

        self._save_project_config()
        return store

    @public_api
    @new_method_or_class(version="0.17.2")
    def add_data_docs_site(
        self, site_name: str, site_config: DataDocsSiteConfigTypedDict
    ) -> None:
        """Add a new Data Docs Site to the DataContext.

        Example site config dicts can be found in our "Host and share Data Docs" guides.

        Args:
            site_name: New site name to add.
            site_config: Config dict for the new site.
        """
        if self.config.data_docs_sites is not None:
            if site_name in self.config.data_docs_sites:
                raise gx_exceptions.InvalidKeyError(
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
                raise gx_exceptions.InvalidKeyError(
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
                raise gx_exceptions.InvalidKeyError(
                    f"Data Docs Site `{site_name}` does not already exist in the Data Context."
                )

            sites = self.config.data_docs_sites
            sites.pop(site_name)
            self.variables.data_docs_sites = sites
            self._save_project_config()

    @public_api
    @new_method_or_class(version="0.15.48")
    def delete_store(self, store_name: str) -> None:
        """Delete an existing Store from the DataContext.

        Args:
            store_name: The name of the Store to be deleted.

        Raises:
            StoreConfigurationError if the target Store is not found.
        """
        if store_name not in self.config.stores and store_name not in self._stores:
            raise gx_exceptions.StoreConfigurationError(
                f'Attempted to delete a store named: "{store_name}". It is not a configured store.'
            )

        # Both the config and the actual stores need to be kept in sync
        self.config.stores.pop(store_name, None)
        self._stores.pop(store_name, None)

        self._save_project_config()

    @public_api
    def list_datasources(self) -> List[dict]:
        """List the configurations of the datasources associated with this context.

        Note that any sensitive values are obfuscated before being returned.

        Returns:
            A list of dictionaries representing datasource configurations.
        """
        datasources: List[dict] = []

        datasource_name: str
        datasource_config: Union[dict, DatasourceConfig]
        serializer = NamedDatasourceSerializer(schema=datasourceConfigSchema)

        for datasource_name, datasource_config in self.config.datasources.items():  # type: ignore[union-attr]
            if isinstance(datasource_config, dict):
                datasource_config = DatasourceConfig(  # noqa: PLW2901
                    **datasource_config
                )
            datasource_config.name = datasource_name

            masked_config: dict = (
                self._serialize_substitute_and_sanitize_datasource_config(
                    serializer, datasource_config
                )
            )
            datasources.append(masked_config)

        for (
            datasource_name,
            fluent_datasource_config,
        ) in self.fluent_datasources.items():
            datasources.append(fluent_datasource_config.dict())
        return datasources

    @public_api
    @deprecated_argument(argument_name="save_changes", version="0.15.32")
    def delete_datasource(
        self, datasource_name: Optional[str], save_changes: Optional[bool] = None
    ) -> None:
        """Delete a given Datasource by name.

        Note that this method causes deletion from the underlying DatasourceStore.
        This can be overridden to only impact the Datasource cache through the deprecated
        `save_changes` argument.

        Args:
            datasource_name: The name of the target datasource.
            save_changes: Should this change be persisted by the DatasourceStore?

        Raises:
            ValueError: The `datasource_name` isn't provided or cannot be found.
        """
        save_changes = self._determine_save_changes_flag(save_changes)

        if not datasource_name:
            raise ValueError("Datasource names must be a datasource name")

        datasource = self.get_datasource(datasource_name=datasource_name)

        if isinstance(datasource, FluentDatasource):
            # Note: this results in some unnecessary dict lookups
            self._delete_fluent_datasource(datasource_name)
        elif save_changes:
            datasource_config = datasourceConfigSchema.load(datasource.config)
            self._datasource_store.delete(datasource_config)
        self._cached_datasources.pop(datasource_name, None)
        self.config.datasources.pop(datasource_name, None)  # type: ignore[union-attr]

        if save_changes:
            self._save_project_config()

    @overload
    def add_checkpoint(  # noqa: PLR0913
        self,
        name: str = ...,
        config_version: int | float = ...,
        template_name: str | None = ...,
        module_name: str = ...,
        class_name: str = ...,
        run_name_template: str | None = ...,
        expectation_suite_name: str | None = ...,
        batch_request: dict | None = ...,
        action_list: Sequence[ActionDict] | None = ...,
        evaluation_parameters: dict | None = ...,
        runtime_configuration: dict | None = ...,
        validations: list[dict] | None = ...,
        profilers: list[dict] | None = ...,
        # the following four arguments are used by SimpleCheckpoint
        site_names: str | list[str] | None = ...,
        slack_webhook: str | None = ...,
        notify_on: str | None = ...,
        notify_with: str | list[str] | None = ...,
        ge_cloud_id: str | None = ...,
        expectation_suite_ge_cloud_id: str | None = ...,
        default_validation_id: str | None = ...,
        id: str | None = ...,
        expectation_suite_id: str | None = ...,
        validator: Validator | None = ...,
        checkpoint: None = ...,
    ) -> Checkpoint:
        """
        Individual constructor arguments are provided.
        `checkpoint` should not be provided.
        """
        ...

    @overload
    def add_checkpoint(  # noqa: PLR0913
        self,
        name: None = ...,
        config_version: int | float = ...,
        template_name: None = ...,
        module_name: str = ...,
        class_name: str = ...,
        run_name_template: None = ...,
        expectation_suite_name: None = ...,
        batch_request: None = ...,
        action_list: Sequence[ActionDict] | None = ...,
        evaluation_parameters: None = ...,
        runtime_configuration: None = ...,
        validations: None = ...,
        profilers: None = ...,
        site_names: None = ...,
        slack_webhook: None = ...,
        notify_on: None = ...,
        notify_with: None = ...,
        ge_cloud_id: None = ...,
        expectation_suite_ge_cloud_id: None = ...,
        default_validation_id: None = ...,
        id: None = ...,
        expectation_suite_id: None = ...,
        validator: Validator | None = ...,
        checkpoint: Checkpoint = ...,
    ) -> Checkpoint:
        """
        A `checkpoint` is provided.
        Individual constructor arguments should not be provided.
        """
        ...

    @public_api
    @new_argument(
        argument_name="id",
        version="0.15.48",
        message="To be used in place of `ge_cloud_id`",
    )
    @new_argument(
        argument_name="expectation_suite_id",
        version="0.15.48",
        message="To be used in place of `expectation_suite_ge_cloud_id`",
    )
    @new_argument(
        argument_name="checkpoint",
        version="0.15.48",
        message="Pass in an existing checkpoint instead of individual constructor args",
    )
    @new_argument(
        argument_name="validator",
        version="0.16.15",
        message="Pass in an existing validator instead of individual validations",
    )
    def add_checkpoint(  # noqa: PLR0913
        self,
        name: str | None = None,
        config_version: int | float = 1.0,
        template_name: str | None = None,
        module_name: str = "great_expectations.checkpoint",
        class_name: str = "Checkpoint",
        run_name_template: str | None = None,
        expectation_suite_name: str | None = None,
        batch_request: dict | None = None,
        action_list: Sequence[ActionDict] | None = None,
        evaluation_parameters: dict | None = None,
        runtime_configuration: dict | None = None,
        validations: list[dict] | None = None,
        profilers: list[dict] | None = None,
        # the following four arguments are used by SimpleCheckpoint
        site_names: str | list[str] | None = None,
        slack_webhook: str | None = None,
        notify_on: str | None = None,
        notify_with: str | list[str] | None = None,
        ge_cloud_id: str | None = None,
        expectation_suite_ge_cloud_id: str | None = None,
        default_validation_id: str | None = None,
        id: str | None = None,
        expectation_suite_id: str | None = None,
        validator: Validator | None = None,
        checkpoint: Checkpoint | None = None,
    ) -> Checkpoint:
        """Add a Checkpoint to the DataContext.

        ---Documentation---
            - https://docs.greatexpectations.io/docs/terms/checkpoint/

        Args:
            name: The name to give the checkpoint.
            config_version: The config version of this checkpoint.
            template_name: The template to use in generating this checkpoint.
            module_name: The module name to use in generating this checkpoint.
            class_name: The class name to use in generating this checkpoint.
            run_name_template: The run name template to use in generating this checkpoint.
            expectation_suite_name: The expectation suite name to use in generating this checkpoint.
            batch_request: The batch request to use in generating this checkpoint.
            action_list: The action list to use in generating this checkpoint.
            evaluation_parameters: The evaluation parameters to use in generating this checkpoint.
            runtime_configuration: The runtime configuration to use in generating this checkpoint.
            validations: The validations to use in generating this checkpoint.
            profilers: The profilers to use in generating this checkpoint.
            site_names: The site names to use in generating this checkpoint. This is only used for SimpleCheckpoint configuration.
            slack_webhook: The slack webhook to use in generating this checkpoint. This is only used for SimpleCheckpoint configuration.
            notify_on: The notify on setting to use in generating this checkpoint. This is only used for SimpleCheckpoint configuration.
            notify_with: The notify with setting to use in generating this checkpoint. This is only used for SimpleCheckpoint configuration.
            ge_cloud_id: The GE Cloud ID to use in generating this checkpoint.
            expectation_suite_ge_cloud_id: The expectation suite GE Cloud ID to use in generating this checkpoint.
            default_validation_id: The default validation ID to use in generating this checkpoint.
            id: The ID to use in generating this checkpoint (preferred over `ge_cloud_id`).
            expectation_suite_id: The expectation suite ID to use in generating this checkpoint (preferred over `expectation_suite_ge_cloud_id`).
            validator: An existing validator used to generate a validations list.
            checkpoint: An existing checkpoint you wish to persist.

        Returns:
            The Checkpoint object created.
        """
        # <GX_RENAME>
        id = self._resolve_id_and_ge_cloud_id(id=id, ge_cloud_id=ge_cloud_id)
        expectation_suite_id = self._resolve_id_and_ge_cloud_id(
            id=expectation_suite_id, ge_cloud_id=expectation_suite_ge_cloud_id
        )
        del ge_cloud_id
        del expectation_suite_ge_cloud_id

        checkpoint = self._resolve_add_checkpoint_args(
            name=name,
            id=id,
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
            site_names=site_names,
            slack_webhook=slack_webhook,
            notify_on=notify_on,
            notify_with=notify_with,
            expectation_suite_id=expectation_suite_id,
            default_validation_id=default_validation_id,
            validator=validator,
            checkpoint=checkpoint,
        )

        result: Checkpoint | CheckpointConfig
        try:
            result = self.checkpoint_store.add_checkpoint(checkpoint)
        except gx_exceptions.CheckpointError as e:
            # deprecated-v0.15.50
            warnings.warn(
                f"{e.message}; using add_checkpoint to overwrite an existing value is deprecated as of v0.15.50 "
                "and will be removed in v0.18. Please use add_or_update_checkpoint instead.",
                DeprecationWarning,
            )
            result = self.checkpoint_store.add_or_update_checkpoint(checkpoint)

        if isinstance(result, CheckpointConfig):
            result = Checkpoint.instantiate_from_config_with_runtime_args(
                checkpoint_config=result,
                data_context=self,
                name=name,
            )
        return result

    @public_api
    @new_method_or_class(version="0.15.48")
    def update_checkpoint(self, checkpoint: Checkpoint) -> Checkpoint:
        """Update a Checkpoint that already exists.

        Args:
            checkpoint: The checkpoint to use to update.

        Raises:
            DataContextError: A suite with the given name does not already exist.

        Returns:
            The updated Checkpoint.
        """
        result: Checkpoint | CheckpointConfig = self.checkpoint_store.update_checkpoint(
            checkpoint
        )
        if isinstance(result, CheckpointConfig):
            result = Checkpoint.instantiate_from_config_with_runtime_args(
                checkpoint_config=result,
                data_context=self,
                name=result.name,
            )
        return result

    @overload
    def add_or_update_checkpoint(  # noqa: PLR0913
        self,
        name: str = ...,
        id: str | None = ...,
        config_version: int | float = ...,
        template_name: str | None = ...,
        module_name: str = ...,
        class_name: str = ...,
        run_name_template: str | None = ...,
        expectation_suite_name: str | None = ...,
        batch_request: dict | None = ...,
        action_list: Sequence[ActionDict] | None = ...,
        evaluation_parameters: dict | None = ...,
        runtime_configuration: dict | None = ...,
        validations: list[dict] | None = ...,
        profilers: list[dict] | None = ...,
        site_names: str | list[str] | None = ...,
        slack_webhook: str | None = ...,
        notify_on: str | None = ...,
        notify_with: str | list[str] | None = ...,
        expectation_suite_id: str | None = ...,
        default_validation_id: str | None = ...,
        validator: Validator | None = ...,
        checkpoint: None = ...,
    ) -> Checkpoint:
        """
        Individual constructor arguments are provided.
        `checkpoint` should not be provided.
        """
        ...

    @overload
    def add_or_update_checkpoint(  # noqa: PLR0913
        self,
        name: None = ...,
        id: None = ...,
        config_version: int | float = ...,
        template_name: None = ...,
        module_name: str = ...,
        class_name: str = ...,
        run_name_template: None = ...,
        expectation_suite_name: None = ...,
        batch_request: None = ...,
        action_list: Sequence[ActionDict] | None = ...,
        evaluation_parameters: None = ...,
        runtime_configuration: None = ...,
        validations: None = ...,
        profilers: None = ...,
        site_names: None = ...,
        slack_webhook: None = ...,
        notify_on: None = ...,
        notify_with: None = ...,
        expectation_suite_id: None = ...,
        default_validation_id: None = ...,
        validator: Validator | None = ...,
        checkpoint: Checkpoint = ...,
    ) -> Checkpoint:
        """
        A `checkpoint` is provided.
        Individual constructor arguments should not be provided.
        """
        ...

    @public_api
    @new_method_or_class(version="0.15.48")
    @new_argument(
        argument_name="validator",
        version="0.16.15",
        message="Pass in an existing validator instead of individual validations",
    )
    def add_or_update_checkpoint(  # noqa: PLR0913
        self,
        name: str | None = None,
        id: str | None = None,
        config_version: int | float = 1.0,
        template_name: str | None = None,
        module_name: str = "great_expectations.checkpoint",
        class_name: str = "Checkpoint",
        run_name_template: str | None = None,
        expectation_suite_name: str | None = None,
        batch_request: dict | None = None,
        action_list: Sequence[ActionDict] | None = None,
        evaluation_parameters: dict | None = None,
        runtime_configuration: dict | None = None,
        validations: list[dict] | None = None,
        profilers: list[dict] | None = None,
        # the following four arguments are used by SimpleCheckpoint
        site_names: str | list[str] | None = None,
        slack_webhook: str | None = None,
        notify_on: str | None = None,
        notify_with: str | list[str] | None = None,
        expectation_suite_id: str | None = None,
        default_validation_id: str | None = None,
        validator: Validator | None = None,
        checkpoint: Checkpoint | None = None,
    ) -> Checkpoint:
        """Add a new Checkpoint or update an existing one on the context depending on whether it already exists or not.

        Args:
            name: The name to give the checkpoint.
            id: The ID to associate with this checkpoint.
            config_version: The config version of this checkpoint.
            template_name: The template to use in generating this checkpoint.
            module_name: The module name to use in generating this checkpoint.
            class_name: The class name to use in generating this checkpoint.
            run_name_template: The run name template to use in generating this checkpoint.
            expectation_suite_name: The expectation suite name to use in generating this checkpoint.
            batch_request: The batch request to use in generating this checkpoint.
            action_list: The action list to use in generating this checkpoint.
            evaluation_parameters: The evaluation parameters to use in generating this checkpoint.
            runtime_configuration: The runtime configuration to use in generating this checkpoint.
            validations: The validations to use in generating this checkpoint.
            profilers: The profilers to use in generating this checkpoint.
            site_names: The site names to use in generating this checkpoint. This is only used for SimpleCheckpoint configuration.
            slack_webhook: The slack webhook to use in generating this checkpoint. This is only used for SimpleCheckpoint configuration.
            notify_on: The notify on setting to use in generating this checkpoint. This is only used for SimpleCheckpoint configuration.
            notify_with: The notify with setting to use in generating this checkpoint. This is only used for SimpleCheckpoint configuration.
            expectation_suite_id: The expectation suite GE Cloud ID to use in generating this checkpoint.
            default_validation_id: The default validation ID to use in generating this checkpoint.
            validator: An existing validator used to generate a validations list.
            checkpoint: An existing checkpoint you wish to persist.

        Returns:
            A new Checkpoint or an updated once (depending on whether or not it existed before this method call).
        """
        checkpoint = self._resolve_add_checkpoint_args(
            name=name,
            id=id,
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
            site_names=site_names,
            slack_webhook=slack_webhook,
            notify_on=notify_on,
            notify_with=notify_with,
            expectation_suite_id=expectation_suite_id,
            default_validation_id=default_validation_id,
            validator=validator,
            checkpoint=checkpoint,
        )

        result: Checkpoint | CheckpointConfig = (
            self.checkpoint_store.add_or_update_checkpoint(checkpoint)
        )
        if isinstance(result, CheckpointConfig):
            result = Checkpoint.instantiate_from_config_with_runtime_args(
                checkpoint_config=result,
                data_context=self,
                name=name,
            )
        return result

    def _resolve_add_checkpoint_args(  # noqa: PLR0913
        self,
        name: str | None = None,
        id: str | None = None,
        config_version: int | float = 1.0,
        template_name: str | None = None,
        module_name: str = "great_expectations.checkpoint",
        class_name: str = "Checkpoint",
        run_name_template: str | None = None,
        expectation_suite_name: str | None = None,
        batch_request: dict | None = None,
        action_list: Sequence[ActionDict] | None = None,
        evaluation_parameters: dict | None = None,
        runtime_configuration: dict | None = None,
        validations: list[dict] | None = None,
        profilers: list[dict] | None = None,
        site_names: str | list[str] | None = None,
        slack_webhook: str | None = None,
        notify_on: str | None = None,
        notify_with: str | list[str] | None = None,
        expectation_suite_id: str | None = None,
        default_validation_id: str | None = None,
        validator: Validator | None = None,
        checkpoint: Checkpoint | None = None,
    ) -> Checkpoint:
        from great_expectations.checkpoint.checkpoint import Checkpoint

        if not ((checkpoint is None) ^ (name is None)):
            raise ValueError(
                "Must either pass in an existing checkpoint or individual constructor arguments (but not both)"
            )

        action_list = action_list or self._determine_default_action_list()

        if not checkpoint:
            assert (
                name
            ), "Guaranteed to have a non-null name if constructing Checkpoint with individual args"
            checkpoint = Checkpoint.construct_from_config_args(
                data_context=self,
                checkpoint_store_name=self.checkpoint_store_name,  # type: ignore[arg-type]
                name=name,
                config_version=config_version,
                template_name=template_name,
                module_name=module_name,
                class_name=class_name,  # type: ignore[arg-type] # should be specific Literal str.
                run_name_template=run_name_template,
                expectation_suite_name=expectation_suite_name,
                batch_request=batch_request,
                action_list=action_list,
                evaluation_parameters=evaluation_parameters,
                runtime_configuration=runtime_configuration,
                validations=validations,
                profilers=profilers,
                site_names=site_names,
                slack_webhook=slack_webhook,
                notify_on=notify_on,
                notify_with=notify_with,
                ge_cloud_id=id,
                expectation_suite_ge_cloud_id=expectation_suite_id,
                default_validation_id=default_validation_id,
                validator=validator,
            )

        return checkpoint

    def _determine_default_action_list(self) -> Sequence[ActionDict]:
        from great_expectations.checkpoint.checkpoint import Checkpoint

        return Checkpoint.DEFAULT_ACTION_LIST

    @public_api
    @new_argument(
        argument_name="id",
        version="0.15.48",
        message="To be used in place of `ge_cloud_id`",
    )
    def get_checkpoint(
        self,
        name: str | None = None,
        ge_cloud_id: str | None = None,
        id: str | None = None,
    ) -> Checkpoint:
        """Retrieves a given Checkpoint by either name or id.

        Args:
            name: The name of the target Checkpoint.
            ge_cloud_id: The id associated with the target Checkpoint.
            id: The id associated with the target Checkpoint (preferred over `ge_cloud_id`).

        Returns:
            The requested Checkpoint.

        Raises:
            CheckpointNotFoundError: If the requested Checkpoint does not exist.
        """
        # <GX_RENAME>
        id = self._resolve_id_and_ge_cloud_id(id=id, ge_cloud_id=ge_cloud_id)

        if not name and not id:
            raise ValueError("name and id cannot both be None")

        del ge_cloud_id

        from great_expectations.checkpoint.checkpoint import Checkpoint

        checkpoint_config: CheckpointConfig = self.checkpoint_store.get_checkpoint(
            name=name, id=id
        )
        checkpoint: Checkpoint = Checkpoint.instantiate_from_config_with_runtime_args(
            checkpoint_config=checkpoint_config,
            data_context=self,
            name=name,
        )

        return checkpoint

    @public_api
    @new_argument(
        argument_name="id",
        version="0.15.48",
        message="To be used in place of `ge_cloud_id`",
    )
    def delete_checkpoint(
        self,
        name: str | None = None,
        ge_cloud_id: str | None = None,
        id: str | None = None,
    ) -> None:
        """Deletes a given Checkpoint by either name or id.

        Args:
            name: The name of the target Checkpoint.
            ge_cloud_id: The id associated with the target Checkpoint.
            id: The id associated with the target Checkpoint (preferred over `ge_cloud_id`).

        Raises:
            CheckpointNotFoundError: If the requested Checkpoint does not exist.
        """
        # <GX_RENAME>
        id = self._resolve_id_and_ge_cloud_id(id=id, ge_cloud_id=ge_cloud_id)
        del ge_cloud_id

        return self.checkpoint_store.delete_checkpoint(name=name, id=id)

    @public_api
    @new_argument(
        argument_name="id",
        version="0.15.48",
        message="To be used in place of `ge_cloud_id`",
    )
    @new_argument(
        argument_name="expectation_suite_id",
        version="0.15.48",
        message="To be used in place of `expectation_suite_ge_cloud_id`",
    )
    @usage_statistics_enabled_method(
        event_name=UsageStatsEvents.DATA_CONTEXT_RUN_CHECKPOINT,
    )
    def run_checkpoint(  # noqa: PLR0913
        self,
        checkpoint_name: str | None = None,
        ge_cloud_id: str | None = None,
        template_name: str | None = None,
        run_name_template: str | None = None,
        expectation_suite_name: str | None = None,
        batch_request: BatchRequestBase | FluentBatchRequest | dict | None = None,
        action_list: Sequence[ActionDict] | None = None,
        evaluation_parameters: dict | None = None,
        runtime_configuration: dict | None = None,
        validations: list[dict] | None = None,
        profilers: list[dict] | None = None,
        run_id: str | int | float | None = None,
        run_name: str | None = None,
        run_time: datetime.datetime | None = None,
        result_format: str | None = None,
        expectation_suite_ge_cloud_id: str | None = None,
        id: str | None = None,
        expectation_suite_id: str | None = None,
        **kwargs,
    ) -> CheckpointResult:
        """Validate using an existing Checkpoint.

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
            id: Great Expectations Cloud id for the checkpoint (preferred over `ge_cloud_id`)
            expectation_suite_id: Great Expectations Cloud id for the expectation suite (preferred over `expectation_suite_ge_cloud_id`)
            **kwargs: Additional kwargs to pass to the validation operator

        Returns:
            CheckpointResult
        """
        # <GX_RENAME>
        id = self._resolve_id_and_ge_cloud_id(id=id, ge_cloud_id=ge_cloud_id)
        expectation_suite_id = self._resolve_id_and_ge_cloud_id(
            id=expectation_suite_id, ge_cloud_id=expectation_suite_ge_cloud_id
        )
        del ge_cloud_id
        del expectation_suite_ge_cloud_id

        return self._run_checkpoint(
            checkpoint_name=checkpoint_name,
            id=id,
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
            expectation_suite_ge_cloud_id=expectation_suite_id,
            **kwargs,
        )

    def _run_checkpoint(  # noqa: PLR0913
        self,
        checkpoint_name: str | None = None,
        id: str | None = None,
        template_name: str | None = None,
        run_name_template: str | None = None,
        expectation_suite_name: str | None = None,
        batch_request: BatchRequestBase | FluentBatchRequest | dict | None = None,
        action_list: Sequence[ActionDict] | None = None,
        evaluation_parameters: dict | None = None,
        runtime_configuration: dict | None = None,
        validations: list[dict] | None = None,
        profilers: list[dict] | None = None,
        run_id: str | int | float | None = None,
        run_name: str | None = None,
        run_time: datetime.datetime | None = None,
        result_format: str | None = None,
        expectation_suite_ge_cloud_id: str | None = None,
        **kwargs,
    ) -> CheckpointResult:
        checkpoint: Checkpoint = self.get_checkpoint(
            name=checkpoint_name,
            id=id,
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

    @public_api
    def list_expectation_suite_names(self) -> List[str]:
        """Lists the available expectation suite names.

        Returns:
            A list of suite names (sorted in alphabetic order).
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
            raise gx_exceptions.InvalidConfigError(
                f"Unable to find configured store: {str(e)}"
            )
        return keys  # type: ignore[return-value]

    @public_api
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
        expectation_suite_id: Optional[str] = None,
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
            sampling_method: The method used to sample Batch data (see: Splitting and Sampling)
            sampling_kwargs: Arguments for the sampling method
            splitter_method: The method used to split the Data Asset into Batches
            splitter_kwargs: Arguments for the splitting method
            batch_spec_passthrough: Arguments specific to the `ExecutionEngine` that aid in Batch retrieval
            expectation_suite_ge_cloud_id: The identifier of the ExpectationSuite to retrieve from the DataContext
                (can be used in place of `expectation_suite_name`)
            expectation_suite_name: The name of the ExpectationSuite to retrieve from the DataContext
            expectation_suite: The ExpectationSuite to use with the validator
            create_expectation_suite_with_name: Creates a Validator with a new ExpectationSuite with the provided name
            include_rendered_content: If `True` the ExpectationSuite will include rendered content when saved
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
        """
        # <GX_RENAME>
        expectation_suite_id = self._resolve_id_and_ge_cloud_id(
            id=expectation_suite_id, ge_cloud_id=expectation_suite_ge_cloud_id
        )
        del expectation_suite_ge_cloud_id

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
                    expectation_suite_id is not None,
                ]
            )
            > 1
        ):
            ge_cloud_mode = getattr(  # attr not on AbstractDataContext
                self, "ge_cloud_mode"
            )
            raise ValueError(
                "No more than one of expectation_suite_name,"
                f"{'expectation_suite_id,' if ge_cloud_mode else ''}"
                " expectation_suite, or create_expectation_suite_with_name can be specified"
            )

        if expectation_suite_id is not None:
            expectation_suite = self.get_expectation_suite(
                include_rendered_content=include_rendered_content,
                ge_cloud_id=expectation_suite_id,
            )
        if expectation_suite_name is not None:
            expectation_suite = self.get_expectation_suite(
                expectation_suite_name,
                include_rendered_content=include_rendered_content,
            )
        if create_expectation_suite_with_name is not None:
            expectation_suite = self.add_expectation_suite(
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
        batch_list: Sequence[Union[Batch, FluentBatch]],
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
            raise gx_exceptions.InvalidBatchRequestError(
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
        datasource_name: str = batch_list[-1].batch_definition.datasource_name
        datasource: LegacyDatasource | BaseDatasource | FluentDatasource = (
            self.datasources[datasource_name]
        )
        execution_engine: ExecutionEngine
        if isinstance(datasource, FluentDatasource):
            batch = batch_list[-1]
            assert isinstance(batch, FluentBatch)
            execution_engine = batch.data.execution_engine
        elif isinstance(datasource, BaseDatasource):
            execution_engine = datasource.execution_engine
        else:
            raise gx_exceptions.DatasourceError(
                message="LegacyDatasource cannot be used to create a Validator",
                datasource_name=datasource_name,
            )

        validator = Validator(
            execution_engine=execution_engine,
            interactive_evaluation=True,
            expectation_suite=expectation_suite,
            data_context=self,
            batches=batch_list,
            include_rendered_content=include_rendered_content,
        )

        return validator

    @public_api
    @usage_statistics_enabled_method(
        event_name=UsageStatsEvents.DATA_CONTEXT_GET_BATCH_LIST,
        args_payload_fn=get_batch_list_usage_statistics,
    )
    def get_batch_list(  # noqa: PLR0913
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
        batch_request_options: Optional[Union[dict, BatchRequestOptions]] = None,
        **kwargs: Optional[dict],
    ) -> List[Batch]:
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
            sampling_method: The method used to sample Batch data (see: Splitting and Sampling)
            sampling_kwargs: Arguments for the sampling method
            splitter_method: The method used to split the Data Asset into Batches
            splitter_kwargs: Arguments for the splitting method
            batch_spec_passthrough: Arguments specific to the `ExecutionEngine` that aid in Batch retrieval
            batch_request_options: Options for `FluentBatchRequest`
            **kwargs: Used to specify either `batch_identifiers` or `batch_filter_parameters`

        Returns:
            (Batch) The `list` of requested Batch instances

        Raises:
            DatasourceError: If the specified `datasource_name` does not exist in the DataContext
            TypeError: If the specified types of the `batch_request` are not supported, or if the
                `datasource_name` is not a `str`
            ValueError: If more than one exclusive parameter is specified (ex: specifing more than one
                of `batch_data`, `query` or `path`)

        """
        return self._get_batch_list(
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
            batch_request_options=batch_request_options,
            **kwargs,
        )

    def _get_batch_list(  # noqa: PLR0913
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
        batch_request_options: Optional[Union[dict, BatchRequestOptions]] = None,
        **kwargs: Optional[dict],
    ) -> List[Batch]:
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
            splitter_method=splitter_method,
            splitter_kwargs=splitter_kwargs,
            runtime_parameters=runtime_parameters,
            query=query,
            path=path,
            batch_filter_parameters=batch_filter_parameters,
            batch_spec_passthrough=batch_spec_passthrough,
            batch_request_options=batch_request_options,
            **kwargs,
        )
        datasource_name = result.datasource_name
        if datasource_name not in self.datasources:
            raise gx_exceptions.DatasourceError(
                datasource_name,
                "The given datasource could not be retrieved from the DataContext; "
                "please confirm that your configuration is accurate.",
            )

        datasource = self.datasources[
            datasource_name
        ]  # this can return one of three datasource types, including Fluent datasource types
        return datasource.get_batch_list_from_batch_request(batch_request=result)  # type: ignore[union-attr, return-value, arg-type]

    @public_api
    @deprecated_method_or_class(
        version="0.15.48", message="Part of the deprecated DataContext CRUD API"
    )
    def create_expectation_suite(
        self,
        expectation_suite_name: str,
        overwrite_existing: bool = False,
        **kwargs: Optional[dict],
    ) -> ExpectationSuite:
        """Build a new ExpectationSuite and save it utilizing the context's underlying ExpectationsStore.

        Note that this method can be called by itself or run within the get_validator workflow.

        When run with create_expectation_suite():

        ```python
        expectation_suite_name = "genres_movies.fkey"
        context.create_expectation_suite(expectation_suite_name, overwrite_existing=True)
        batch = context.get_batch(
            expectation_suite_name=expectation_suite_name
        )
        ```

        When run as part of get_validator():

        ```python
        validator = context.get_validator(
            datasource_name="my_datasource",
            data_connector_name="whole_table",
            data_asset_name="my_table",
            create_expectation_suite_with_name="my_expectation_suite",
        )
        validator.expect_column_values_to_be_in_set("c1", [4,5,6])
        ```

        Args:
            expectation_suite_name: The name of the suite to create.
            overwrite_existing: Whether to overwrite if a suite with the given name already exists.
            **kwargs: Any key-value arguments to pass to the store when persisting.

        Returns:
            A new (empty) ExpectationSuite.

        Raises:
            ValueError: The input `overwrite_existing` is of the wrong type.
            DataContextError: A suite with the same name already exists (and `overwrite_existing` is not enabled).
        """
        # deprecated-v0.15.48
        warnings.warn(
            "create_expectation_suite is deprecated as of v0.15.48 and will be removed in v0.18. "
            "Please use add_expectation_suite or add_or_update_expectation_suite instead.",
            DeprecationWarning,
        )
        return self._add_expectation_suite(
            expectation_suite_name=expectation_suite_name,
            overwrite_existing=overwrite_existing,
        )

    @overload
    def add_expectation_suite(  # noqa: PLR0913
        self,
        expectation_suite_name: str,
        id: str | None = ...,
        expectations: list[dict | ExpectationConfiguration] | None = ...,
        evaluation_parameters: dict | None = ...,
        data_asset_type: str | None = ...,
        execution_engine_type: Type[ExecutionEngine] | None = ...,
        meta: dict | None = ...,
        expectation_suite: None = ...,
    ) -> ExpectationSuite:
        """
        An `expectation_suite_name` is provided.
        `expectation_suite` should not be provided.
        """
        ...

    @overload
    def add_expectation_suite(  # noqa: PLR0913
        self,
        expectation_suite_name: None = ...,
        id: str | None = ...,
        expectations: list[dict | ExpectationConfiguration] | None = ...,
        evaluation_parameters: dict | None = ...,
        data_asset_type: str | None = ...,
        execution_engine_type: Type[ExecutionEngine] | None = ...,
        meta: dict | None = ...,
        expectation_suite: ExpectationSuite = ...,
    ) -> ExpectationSuite:
        """
        An `expectation_suite` is provided.
        `expectation_suite_name` should not be provided.
        """
        ...

    @public_api
    @new_method_or_class(version="0.15.48")
    def add_expectation_suite(  # noqa: PLR0913
        self,
        expectation_suite_name: str | None = None,
        id: str | None = None,
        expectations: list[dict | ExpectationConfiguration] | None = None,
        evaluation_parameters: dict | None = None,
        data_asset_type: str | None = None,
        execution_engine_type: Type[ExecutionEngine] | None = None,
        meta: dict | None = None,
        expectation_suite: ExpectationSuite | None = None,
    ) -> ExpectationSuite:
        """Build a new ExpectationSuite and save it utilizing the context's underlying ExpectationsStore.

        Note that this method can be called by itself or run within the get_validator workflow.

        When run with create_expectation_suite()::

            expectation_suite_name = "genres_movies.fkey"
            context.create_expectation_suite(expectation_suite_name, overwrite_existing=True)
            batch = context.get_batch(
                expectation_suite_name=expectation_suite_name
            )


        When run as part of get_validator()::

            validator = context.get_validator(
                datasource_name="my_datasource",
                data_connector_name="whole_table",
                data_asset_name="my_table",
                create_expectation_suite_with_name="my_expectation_suite",
            )
            validator.expect_column_values_to_be_in_set("c1", [4,5,6])


        Args:
            expectation_suite_name: The name of the suite to create.
            id: Identifier to associate with this suite.
            expectations: Expectation Configurations to associate with this suite.
            evaluation_parameters: Evaluation parameters to be substituted when evaluating Expectations.
            data_asset_type: Type of data asset to associate with this suite.
            execution_engine_type: Name of the execution engine type.
            meta: Metadata related to the suite.

        Returns:
            A new ExpectationSuite built with provided input args.

        Raises:
            DataContextError: A suite with the same name already exists (and `overwrite_existing` is not enabled).
            ValueError: The arguments provided are invalid.
        """
        return self._add_expectation_suite(
            expectation_suite_name=expectation_suite_name,
            id=id,
            expectations=expectations,
            evaluation_parameters=evaluation_parameters,
            data_asset_type=data_asset_type,
            execution_engine_type=execution_engine_type,
            meta=meta,
            expectation_suite=expectation_suite,
            overwrite_existing=False,  # `add` does not resolve collisions
        )

    def _add_expectation_suite(  # noqa: PLR0913
        self,
        expectation_suite_name: str | None = None,
        id: str | None = None,
        expectations: Sequence[dict | ExpectationConfiguration] | None = None,
        evaluation_parameters: dict | None = None,
        data_asset_type: str | None = None,
        execution_engine_type: Type[ExecutionEngine] | None = None,
        meta: dict | None = None,
        overwrite_existing: bool = False,
        expectation_suite: ExpectationSuite | None = None,
        **kwargs,
    ) -> ExpectationSuite:
        if not isinstance(overwrite_existing, bool):
            raise ValueError("overwrite_existing must be of type bool.")

        self._validate_expectation_suite_xor_expectation_suite_name(
            expectation_suite, expectation_suite_name
        )

        if not expectation_suite:
            # type narrowing
            assert isinstance(
                expectation_suite_name, str
            ), "expectation_suite_name must be specified."

            expectation_suite = ExpectationSuite(
                expectation_suite_name=expectation_suite_name,
                data_context=self,
                ge_cloud_id=id,
                expectations=expectations,
                evaluation_parameters=evaluation_parameters,
                data_asset_type=data_asset_type,
                execution_engine_type=execution_engine_type,
                meta=meta,
            )

        return self._persist_suite_with_store(
            expectation_suite=expectation_suite,
            overwrite_existing=overwrite_existing,
            **kwargs,
        )

    def _persist_suite_with_store(
        self,
        expectation_suite: ExpectationSuite,
        overwrite_existing: bool,
        **kwargs,
    ) -> ExpectationSuite:
        key = ExpectationSuiteIdentifier(
            expectation_suite_name=expectation_suite.expectation_suite_name
        )

        persistence_fn: Callable
        if overwrite_existing:
            persistence_fn = self.expectations_store.add_or_update
        else:
            persistence_fn = self.expectations_store.add

        persistence_fn(key=key, value=expectation_suite, **kwargs)
        return expectation_suite

    @public_api
    @new_method_or_class(version="0.15.48")
    # 20230216 - Chetan - Decorating with save to ensure no gaps in usage stats collection
    # A future effort will add more granular event names
    @usage_statistics_enabled_method(
        event_name=UsageStatsEvents.DATA_CONTEXT_SAVE_EXPECTATION_SUITE,
        args_payload_fn=save_expectation_suite_usage_statistics,
    )
    def update_expectation_suite(
        self,
        expectation_suite: ExpectationSuite,
    ) -> ExpectationSuite:
        """Update an ExpectationSuite that already exists.

        Args:
            expectation_suite: The suite to use to update.

        Raises:
            DataContextError: A suite with the given name does not already exist.
        """
        return self._update_expectation_suite(expectation_suite=expectation_suite)

    def _update_expectation_suite(
        self,
        expectation_suite: ExpectationSuite,
    ) -> ExpectationSuite:
        """
        Like `update_expectation_suite` but without the usage statistics logging.
        """
        name = expectation_suite.expectation_suite_name
        id = expectation_suite.ge_cloud_id
        key = self._determine_key_for_suite_update(name=name, id=id)
        self.expectations_store.update(key=key, value=expectation_suite)
        return expectation_suite

    def _determine_key_for_suite_update(
        self, name: str, id: str | None
    ) -> Union[ExpectationSuiteIdentifier, GXCloudIdentifier]:
        return ExpectationSuiteIdentifier(name)

    @overload
    def add_or_update_expectation_suite(  # noqa: PLR0913
        self,
        expectation_suite_name: str,
        id: str | None = ...,
        expectations: list[dict | ExpectationConfiguration] | None = ...,
        evaluation_parameters: dict | None = ...,
        data_asset_type: str | None = ...,
        execution_engine_type: Type[ExecutionEngine] | None = ...,
        meta: dict | None = ...,
        expectation_suite: None = ...,
    ) -> ExpectationSuite:
        """
        Two possible patterns:
            - An expectation_suite_name and optional constructor args
            - An expectation_suite
        """
        ...

    @overload
    def add_or_update_expectation_suite(  # noqa: PLR0913
        self,
        expectation_suite_name: None = ...,
        id: str | None = ...,
        expectations: list[dict | ExpectationConfiguration] | None = ...,
        evaluation_parameters: dict | None = ...,
        data_asset_type: str | None = ...,
        execution_engine_type: Type[ExecutionEngine] | None = ...,
        meta: dict | None = ...,
        expectation_suite: ExpectationSuite = ...,
    ) -> ExpectationSuite:
        """
        Two possible patterns:
            - An expectation_suite_name and optional constructor args
            - An expectation_suite
        """
        ...

    @public_api
    @new_method_or_class(version="0.15.48")
    # 20230216 - Chetan - Decorating with save to ensure no gaps in usage stats collection
    # A future effort will add more granular event names
    @usage_statistics_enabled_method(
        event_name=UsageStatsEvents.DATA_CONTEXT_SAVE_EXPECTATION_SUITE,
        args_payload_fn=save_expectation_suite_usage_statistics,
    )
    def add_or_update_expectation_suite(  # noqa: PLR0913
        self,
        expectation_suite_name: str | None = None,
        id: str | None = None,
        expectations: list[dict | ExpectationConfiguration] | None = None,
        evaluation_parameters: dict | None = None,
        data_asset_type: str | None = None,
        execution_engine_type: Type[ExecutionEngine] | None = None,
        meta: dict | None = None,
        expectation_suite: ExpectationSuite | None = None,
    ) -> ExpectationSuite:
        """Add a new ExpectationSuite or update an existing one on the context depending on whether it already exists or not.

        Args:
            expectation_suite_name: The name of the suite to create or replace.
            id: Identifier to associate with this suite (ignored if updating existing suite).
            expectations: Expectation Configurations to associate with this suite.
            evaluation_parameters: Evaluation parameters to be substituted when evaluating Expectations.
            data_asset_type: Type of Data Asset to associate with this suite.
            execution_engine_type: Name of the Execution Engine type.
            meta: Metadata related to the suite.
            expectation_suite: The `ExpectationSuite` object you wish to persist.

        Returns:
            The persisted `ExpectationSuite`.
        """

        self._validate_expectation_suite_xor_expectation_suite_name(
            expectation_suite, expectation_suite_name
        )

        if not expectation_suite:
            # type narrowing
            assert isinstance(
                expectation_suite_name, str
            ), "expectation_suite_name must be specified."

            expectation_suite = ExpectationSuite(
                expectation_suite_name=expectation_suite_name,
                data_context=self,
                ge_cloud_id=id,
                expectations=expectations,
                evaluation_parameters=evaluation_parameters,
                data_asset_type=data_asset_type,
                execution_engine_type=execution_engine_type,
                meta=meta,
            )

        try:
            existing = self.get_expectation_suite(
                expectation_suite_name=expectation_suite.name
            )
        except gx_exceptions.DataContextError:
            # not found
            return self._add_expectation_suite(expectation_suite=expectation_suite)

        # The suite object must have an ID in order to request a PUT to GX Cloud.
        expectation_suite.ge_cloud_id = existing.ge_cloud_id
        return self._update_expectation_suite(expectation_suite=expectation_suite)

    @public_api
    @new_argument(
        argument_name="id",
        version="0.15.48",
        message="To be used in place of `ge_cloud_id`",
    )
    def delete_expectation_suite(
        self,
        expectation_suite_name: str | None = None,
        ge_cloud_id: str | None = None,
        id: str | None = None,
    ) -> None:
        """Delete specified expectation suite from data_context expectation store.

        Args:
            expectation_suite_name: The name of the expectation suite to delete
            ge_cloud_id: The identifier of the expectation suite to delete
            id: The identifier of the expectation suite to delete (preferred over `ge_cloud_id`)

        Returns:
            True for Success and False for Failure.
        """
        key = ExpectationSuiteIdentifier(expectation_suite_name)  # type: ignore[arg-type]
        if not self.expectations_store.has_key(key):
            raise gx_exceptions.DataContextError(
                f"expectation_suite with name {expectation_suite_name} does not exist."
            )
        self.expectations_store.remove_key(key)

    @public_api
    @deprecated_argument(argument_name="ge_cloud_id", version="0.15.45")
    def get_expectation_suite(
        self,
        expectation_suite_name: str | None = None,
        include_rendered_content: bool | None = None,
        ge_cloud_id: str | None = None,
    ) -> ExpectationSuite:
        """Get an Expectation Suite by name.

        Args:
            expectation_suite_name (str): The name of the Expectation Suite
            include_rendered_content (bool): Whether to re-populate rendered_content for each
                ExpectationConfiguration.
            ge_cloud_id (str): The GX Cloud ID for the Expectation Suite (unused)

        Returns:
            An existing ExpectationSuite

        Raises:
            DataContextError: There is no expectation suite with the name provided
        """
        if ge_cloud_id is not None:
            # deprecated-v0.15.45
            warnings.warn(
                "ge_cloud_id is deprecated as of v0.15.45 and will be removed in v0.16. Please use"
                "expectation_suite_name instead",
                DeprecationWarning,
            )

        if expectation_suite_name:
            key = ExpectationSuiteIdentifier(
                expectation_suite_name=expectation_suite_name
            )
        else:
            raise ValueError("expectation_suite_name must be provided")

        if include_rendered_content is None:
            include_rendered_content = (
                self._determine_if_expectation_suite_include_rendered_content()
            )

        if self.expectations_store.has_key(key):
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
            raise gx_exceptions.DataContextError(
                f"expectation_suite {expectation_suite_name} not found"
            )

    @overload
    def add_profiler(  # noqa: PLR0913
        self,
        name: str,
        config_version: float,
        rules: dict[str, dict],
        variables: dict | None = ...,
        profiler: None = ...,
    ) -> RuleBasedProfiler:
        """
        Individual constructors args (`name`, `config_version`, and `rules`) are provided.
        `profiler` should not be provided.
        """
        ...

    @overload
    def add_profiler(  # noqa: PLR0913
        self,
        name: None = ...,
        config_version: None = ...,
        rules: None = ...,
        variables: None = ...,
        profiler: RuleBasedProfiler = ...,
    ) -> RuleBasedProfiler:
        """
        `profiler` is provided.
        Individual constructors args (`name`, `config_version`, and `rules`) should not be provided.
        """
        ...

    @public_api
    @new_argument(
        argument_name="profiler",
        version="0.15.48",
        message="Pass in an existing profiler instead of individual constructor args",
    )
    def add_profiler(  # noqa: PLR0913
        self,
        name: str | None = None,
        config_version: float | None = None,
        rules: dict[str, dict] | None = None,
        variables: dict | None = None,
        profiler: RuleBasedProfiler | None = None,
    ) -> RuleBasedProfiler:
        """
        Constructs a Profiler, persists it utilizing the context's underlying ProfilerStore,
        and returns it to the user for subsequent usage.

        Args:
            name: The name of the RBP instance.
            config_version: The version of the RBP (currently only 1.0 is supported).
            rules: A set of dictionaries, each of which contains its own domain_builder, parameter_builders, and expectation_configuration_builders.
            variables: Any variables to be substituted within the rules.
            profiler: An existing RuleBasedProfiler to persist.

        Returns:
            The persisted Profiler constructed by the input arguments.
        """
        try:
            return RuleBasedProfiler.add_profiler(
                data_context=self,
                profiler_store=self.profiler_store,
                name=name,
                config_version=config_version,
                rules=rules,
                variables=variables,
                profiler=profiler,
            )
        except gx_exceptions.ProfilerError as e:
            # deprecated-v0.15.50
            warnings.warn(
                f"{e.message}; using add_profiler to overwrite an existing value is deprecated as of v0.15.50 "
                "and will be removed in v0.18. Please use add_or_update_profiler instead.",
                DeprecationWarning,
            )
            return RuleBasedProfiler.add_or_update_profiler(
                data_context=self,
                profiler_store=self.profiler_store,
                name=name,
                config_version=config_version,
                rules=rules,
                variables=variables,
                profiler=profiler,
            )

    @public_api
    @new_argument(
        argument_name="id",
        version="0.15.48",
        message="To be used in place of `ge_cloud_id`",
    )
    def get_profiler(
        self,
        name: str | None = None,
        ge_cloud_id: str | None = None,
        id: str | None = None,
    ) -> RuleBasedProfiler:
        """Retrieves a given Profiler by either name or id.

        Args:
            name: The name of the target Profiler.
            ge_cloud_id: The id associated with the target Profiler.
            id: The id associated with the target Profiler (preferred over `ge_cloud_id`).

        Returns:
            The requested Profiler.

        Raises:
            ProfilerNotFoundError: If the requested Profiler does not exists.
        """
        # <GX_RENAME>
        id = self._resolve_id_and_ge_cloud_id(id=id, ge_cloud_id=ge_cloud_id)
        del ge_cloud_id

        return RuleBasedProfiler.get_profiler(
            data_context=self,
            profiler_store=self.profiler_store,
            name=name,
            id=id,
        )

    @public_api
    @new_argument(
        argument_name="id",
        version="0.15.48",
        message="To be used in place of `ge_cloud_id`",
    )
    def delete_profiler(
        self,
        name: str | None = None,
        ge_cloud_id: str | None = None,
        id: str | None = None,
    ) -> None:
        """Deletes a given Profiler by either name or id.

        Args:
            name: The name of the target Profiler.
            ge_cloud_id: The id associated with the target Profiler.
            id: The id associated with the target Profiler (preferred over `ge_cloud_id`).

        Raises:
            ProfilerNotFoundError: If the requested Profiler does not exists.
        """
        # <GX_RENAME>
        id = self._resolve_id_and_ge_cloud_id(id=id, ge_cloud_id=ge_cloud_id)
        del ge_cloud_id

        RuleBasedProfiler.delete_profiler(
            profiler_store=self.profiler_store,
            name=name,
            id=id,
        )

    @public_api
    @new_method_or_class(version="0.15.48")
    def update_profiler(self, profiler: RuleBasedProfiler) -> RuleBasedProfiler:
        """Update a Profiler that already exists.

        Args:
            profiler: The profiler to use to update.

        Raises:
            ProfilerNotFoundError: A profiler with the given name/id does not already exist.
        """
        return RuleBasedProfiler.update_profiler(
            profiler_store=self.profiler_store,
            data_context=self,
            profiler=profiler,
        )

    @overload
    def add_or_update_profiler(  # noqa: PLR0913
        self,
        name: str,
        config_version: float,
        rules: dict[str, dict],
        variables: dict | None = ...,
        profiler: None = ...,
    ) -> RuleBasedProfiler:
        """
        Individual constructors args (`name`, `config_version`, and `rules`) are provided.
        `profiler` should not be provided.
        """
        ...

    @overload
    def add_or_update_profiler(  # noqa: PLR0913
        self,
        name: None = ...,
        config_version: None = ...,
        rules: None = ...,
        variables: None = ...,
        profiler: RuleBasedProfiler = ...,
    ) -> RuleBasedProfiler:
        """
        `profiler` is provided.
        Individual constructors args (`name`, `config_version`, and `rules`) should not be provided.
        """
        ...

    @public_api
    @new_method_or_class(version="0.15.48")
    def add_or_update_profiler(  # noqa: PLR0913
        self,
        name: str | None = None,
        id: str | None = None,
        config_version: float | None = None,
        rules: dict[str, dict] | None = None,
        variables: dict | None = None,
        profiler: RuleBasedProfiler | None = None,
    ) -> RuleBasedProfiler:
        """Add a new Profiler or update an existing one on the context depending on whether it already exists or not.

        Args:
            name: The name of the RBP instance.
            config_version: The version of the RBP (currently only 1.0 is supported).
            rules: A set of dictionaries, each of which contains its own domain_builder, parameter_builders, and expectation_configuration_builders.
            variables: Any variables to be substituted within the rules.
            id: The id associated with the RBP instance (if applicable).
            profiler: An existing RuleBasedProfiler to persist.

        Returns:
            A new Profiler or an updated one (depending on whether or not it existed before this method call).
        """
        return RuleBasedProfiler.add_or_update_profiler(
            data_context=self,
            profiler_store=self.profiler_store,
            name=name,
            id=id,
            config_version=config_version,
            rules=rules,
            variables=variables,
            profiler=profiler,
        )

    @usage_statistics_enabled_method(
        event_name=UsageStatsEvents.DATA_CONTEXT_RUN_RULE_BASED_PROFILER_WITH_DYNAMIC_ARGUMENTS,
    )
    def run_profiler_with_dynamic_arguments(  # noqa: PLR0913
        self,
        batch_list: list[Batch] | None = None,
        batch_request: BatchRequestBase | dict | None = None,
        name: str | None = None,
        ge_cloud_id: str | None = None,
        variables: dict | None = None,
        rules: dict | None = None,
        id: str | None = None,
    ) -> RuleBasedProfilerResult:
        """Retrieve a RuleBasedProfiler from a ProfilerStore and run it with rules/variables supplied at runtime.

        Args:
            batch_list: Explicit list of Batch objects to supply data at runtime
            batch_request: Explicit batch_request used to supply data at runtime
            name: Identifier used to retrieve the profiler from a store.
            ge_cloud_id: Identifier used to retrieve the profiler from a store (GX Cloud specific).
            variables: Attribute name/value pairs (overrides)
            rules: Key-value pairs of name/configuration-dictionary (overrides)
            id: Identifier used to retrieve the profiler from a store (preferred over `ge_cloud_id`).

        Returns:
            Set of rule evaluation results in the form of an RuleBasedProfilerResult

        Raises:
            AssertionError if both a `name` and `id` are provided.
            AssertionError if both an `expectation_suite` and `expectation_suite_name` are provided.
        """
        # <GX_RENAME>
        id = self._resolve_id_and_ge_cloud_id(id=id, ge_cloud_id=ge_cloud_id)
        del ge_cloud_id

        return self._run_profiler_with_dynamic_arguments(
            batch_list=batch_list,
            batch_request=batch_request,
            name=name,
            id=id,
            variables=variables,
            rules=rules,
        )

    def _run_profiler_with_dynamic_arguments(  # noqa: PLR0913
        self,
        batch_list: Optional[List[Batch]] = None,
        batch_request: Optional[Union[BatchRequestBase, dict]] = None,
        name: Optional[str] = None,
        id: Optional[str] = None,
        variables: Optional[dict] = None,
        rules: Optional[dict] = None,
    ) -> RuleBasedProfilerResult:
        return RuleBasedProfiler.run_profiler(
            data_context=self,
            profiler_store=self.profiler_store,
            batch_list=batch_list,
            batch_request=batch_request,
            name=name,
            id=id,
            variables=variables,
            rules=rules,
        )

    @usage_statistics_enabled_method(
        event_name=UsageStatsEvents.DATA_CONTEXT_RUN_RULE_BASED_PROFILER_ON_DATA,
    )
    def run_profiler_on_data(  # noqa: PLR0913
        self,
        batch_list: list[Batch] | None = None,
        batch_request: BatchRequestBase | None = None,
        name: str | None = None,
        ge_cloud_id: str | None = None,
        id: str | None = None,
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
        # <GX_RENAME>
        id = self._resolve_id_and_ge_cloud_id(id=id, ge_cloud_id=ge_cloud_id)
        del ge_cloud_id

        return self._run_profiler_on_data(
            batch_list=batch_list,
            batch_request=batch_request,
            name=name,
            id=id,
        )

    def _run_profiler_on_data(
        self,
        batch_list: list[Batch] | None = None,
        batch_request: BatchRequestBase | None = None,
        name: str | None = None,
        id: str | None = None,
    ) -> RuleBasedProfilerResult:
        return RuleBasedProfiler.run_profiler_on_data(
            data_context=self,
            profiler_store=self.profiler_store,
            batch_list=batch_list,
            batch_request=batch_request,
            name=name,
            id=id,
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
            raise gx_exceptions.ClassInstantiationError(
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
    def run_validation_operator(  # noqa: PLR0913
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
        return self._run_validation_operator(
            validation_operator_name=validation_operator_name,
            assets_to_validate=assets_to_validate,
            run_id=run_id,
            evaluation_parameters=evaluation_parameters,
            run_name=run_name,
            run_time=run_time,
            result_format=result_format,
            **kwargs,
        )

    def _run_validation_operator(  # noqa: PLR0913
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
        result_format = result_format or {"result_format": "SUMMARY"}

        if not assets_to_validate:
            raise gx_exceptions.DataContextError(
                "No batches of data were passed in. These are required"
            )

        for batch in assets_to_validate:
            if not isinstance(batch, (tuple, DataAsset, Validator)):
                raise gx_exceptions.DataContextError(
                    "Batches are required to be of type DataAsset or Validator"
                )
        try:
            validation_operator = self.validation_operators[validation_operator_name]
        except KeyError:
            raise gx_exceptions.DataContextError(
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
        """List the names of currently-configured Validation Operators on this context"""
        if not self.validation_operators:
            return []

        return list(self.validation_operators.keys())

    def profile_data_asset(  # noqa: PLR0912, PLR0913, PLR0915
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
        if isinstance(run_id, dict):
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
            except gx_exceptions.BatchKwargsError:
                raise gx_exceptions.ProfilerError(
                    "Unable to build batch_kwargs for datasource {}, using batch kwargs generator {} for name {}".format(
                        datasource_name, batch_kwargs_generator_name, data_asset_name
                    )
                )
            except ValueError:
                raise gx_exceptions.ProfilerError(
                    "Unable to find datasource {} or batch kwargs generator {}.".format(
                        datasource_name, batch_kwargs_generator_name
                    )
                )
        else:
            batch_kwargs.update(additional_batch_kwargs)

        profiling_results = {"success": False, "results": []}

        total_columns, total_expectations, total_rows = 0, 0, 0
        total_start_time = datetime.datetime.now()  # noqa: DTZ005

        name = data_asset_name
        # logger.info("\tProfiling '%s'..." % name)

        start_time = datetime.datetime.now()  # noqa: DTZ005

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

        self.add_or_update_expectation_suite(
            expectation_suite_name=expectation_suite_name
        )

        # TODO: Add batch_parameters
        batch = self.get_batch(
            expectation_suite_name=expectation_suite_name,
            batch_kwargs=batch_kwargs,
        )

        if not profiler.validate(batch):
            raise gx_exceptions.ProfilerError(
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
        duration = (
            datetime.datetime.now() - start_time  # noqa: DTZ005
        ).total_seconds()
        # noinspection PyUnboundLocalVariable
        logger.info(
            f"\tProfiled {new_column_count} columns using {row_count} rows from {name} ({duration:.3f} sec)"
        )

        total_duration = (
            datetime.datetime.now() - total_start_time  # noqa: DTZ005
        ).total_seconds()
        logger.info(
            f"""
Profiled the data asset, with {total_rows} total rows and {total_columns} columns in {total_duration:.2f} seconds.
Generated, evaluated, and stored {total_expectations} Expectations during profiling. Please review results using data-docs."""
        )

        profiling_results["success"] = True
        return profiling_results

    @deprecated_method_or_class(
        version="0.14.0", message="Part of the deprecated v2 API"
    )
    def add_batch_kwargs_generator(
        self, datasource_name, batch_kwargs_generator_name, class_name, **kwargs
    ):
        """Add a batch kwargs generator to the named datasource, using the provided
        configuration.

        Args:
            datasource_name: name of datasource to which to add the new batch kwargs generator
            batch_kwargs_generator_name: name of the generator to add
            class_name: class of the batch kwargs generator to add
            **kwargs: batch kwargs generator configuration, provided as kwargs

        Returns:
            The batch_kwargs_generator
        """
        datasource_obj = self.get_datasource(datasource_name)
        generator = datasource_obj.add_batch_kwargs_generator(
            name=batch_kwargs_generator_name, class_name=class_name, **kwargs
        )
        return generator

    BlockConfigDataAssetNames: TypeAlias = Dict[str, List[str]]
    FluentDataAssetNames: TypeAlias = List[str]

    @public_api
    def get_available_data_asset_names(  # noqa: PLR0912
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
        """
        data_asset_names = {}
        fluent_data_asset_names = {}
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
                    if isinstance(datasource, FluentDatasource):
                        fluent_data_asset_names[datasource_name] = sorted(
                            datasource.get_asset_names()
                        )
                    else:
                        data_asset_names[
                            datasource_name
                        ] = datasource.get_available_data_asset_names(
                            batch_kwargs_generator_names[idx]
                        )

            elif len(batch_kwargs_generator_names) == 1:
                datasource = self.get_datasource(datasource_names[0])
                if isinstance(datasource, FluentDatasource):
                    fluent_data_asset_names[datasource_names[0]] = sorted(
                        datasource.get_asset_names()
                    )

                else:
                    data_asset_names[
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
                    if isinstance(datasource, FluentDatasource):
                        fluent_data_asset_names[datasource_name] = sorted(
                            datasource.get_asset_names()
                        )

                    else:
                        data_asset_names[
                            datasource_name
                        ] = datasource.get_available_data_asset_names()

                except ValueError:
                    # handle the edge case of a non-existent datasource
                    data_asset_names[datasource_name] = {}

        fluent_and_config_data_asset_names = {
            **data_asset_names,
            **fluent_data_asset_names,
        }
        return fluent_and_config_data_asset_names

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
        data_docs_urls: List[Dict[str, str]] = self.get_docs_sites_urls(
            resource_identifier=resource_identifier,
            site_name=site_name,
            only_if_exists=only_if_exists,
        )
        urls_to_open: List[str] = [site["site_url"] for site in data_docs_urls]

        for url in urls_to_open:
            if url is not None:
                logger.debug(f"Opening Data Docs found here: {url}")
                self._open_url_in_browser(url)

    @staticmethod
    def _open_url_in_browser(url: str) -> None:
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
                raise gx_exceptions.DataContextError(
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
            raise gx_exceptions.DataContextError(
                "No data docs sites were found on this DataContext, therefore no sites will be cleaned.",
            )

        data_docs_site_names = list(data_docs_sites.keys())
        if site_name:
            if site_name not in data_docs_site_names:
                raise gx_exceptions.DataContextError(
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

        profiler_directory_path: str = os.path.join(  # noqa: PTH118
            directory_path,
            DataContextConfigDefaults.DEFAULT_PROFILER_STORE_BASE_DIRECTORY_RELATIVE_NAME.value,
        )
        return os.path.isdir(profiler_directory_path)  # noqa: PTH112

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
                raise gx_exceptions.DataContextError(
                    "Invalid metric_configuration: each key must contain a "
                    "dictionary."
                )
            if (
                kwarg_name == "metric_kwargs_id"
            ):  # this special case allows a hash of multiple kwargs
                for metric_kwargs_id in metric_configuration[kwarg_name].keys():
                    if base_kwargs != {}:
                        raise gx_exceptions.DataContextError(
                            "Invalid metric_configuration: when specifying "
                            "metric_kwargs_id, no other keys or values may be defined."
                        )
                    if not isinstance(
                        metric_configuration[kwarg_name][metric_kwargs_id], list
                    ):
                        raise gx_exceptions.DataContextError(
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
                        raise gx_exceptions.DataContextError(
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
        """
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
        if os.path.isabs(path):  # noqa: PTH117
            return path
        else:
            return os.path.join(self.root_directory, path)  # type: ignore[arg-type]  # noqa: PTH118

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
        self, store_name: str, store_config: dict | StoreConfigTypedDict
    ) -> Store:
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

        new_store = Store.build_store_from_config(
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
    ) -> Dict[str, Union[LegacyDatasource, BaseDatasource, FluentDatasource]]:
        """A single holder for all Datasources in this context"""
        return self._cached_datasources

    @property
    def fluent_datasources(self) -> Dict[str, FluentDatasource]:
        return {
            name: ds
            for (name, ds) in self.datasources.items()
            if isinstance(ds, FluentDatasource)
        }

    @property
    def data_context_id(self) -> str:
        return self.variables.anonymous_usage_statistics.data_context_id  # type: ignore[union-attr]

    def _init_primary_stores(
        self, store_configs: Dict[str, StoreConfigTypedDict]
    ) -> None:
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
            oss_id=self._get_oss_id(),
        )

    @classmethod
    def _get_oss_id(cls) -> uuid.UUID | None:
        """
        Retrieves a user's `oss_id` from disk ($HOME/.great_expectations/great_expectations.conf).

        If no such value is present, a new UUID is generated and written to disk for subsequent usage.
        If there is an error when reading from / writing to disk, we default to a NoneType.
        """
        config = configparser.ConfigParser()

        if not cls._ROOT_CONF_FILE.exists():
            success = cls._scaffold_root_conf()
            if not success:
                return None
            return cls._set_oss_id(config)

        try:
            config.read(cls._ROOT_CONF_FILE)
        except OSError as e:
            logger.info(
                f"Something went wrong when trying to read from the user's conf file: {e}"
            )
            return None

        oss_id = config.get("anonymous_usage_statistics", "oss_id", fallback=None)
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
        config["anonymous_usage_statistics"] = {}
        config["anonymous_usage_statistics"]["oss_id"] = str(oss_id)

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
        config: DataContextConfig = self.config

        if self._datasource_store.cloud_mode:
            for fds in config.fluent_datasources.values():
                self._add_fluent_datasource(**fds)._rebuild_asset_data_connectors()

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
            except gx_exceptions.DatasourceInitializationError as e:
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
            name = getattr(substituted_config, "name", None) or ""
            raise gx_exceptions.DatasourceInitializationError(
                datasource_name=name, message=str(e)
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
            raise gx_exceptions.ClassInstantiationError(
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
        # If attempting to override an existing value, ensure that the id persists
        name = config.name
        if not config.id and name in self._cached_datasources:
            existing_datasource = self._cached_datasources[name]
            if isinstance(existing_datasource, BaseDatasource):
                config.id = existing_datasource.id

        # Note that the call to `DatasourceStore.set` may alter the config object's state
        # As such, we invoke it at the top of our function so any changes are reflected downstream
        if save_changes:
            config = self._datasource_store.set(key=None, value=config)

        datasource: Optional[Datasource] = None
        if initialize:
            try:
                substituted_config = self._perform_substitutions_on_datasource_config(
                    config
                )

                datasource = self._instantiate_datasource_from_config(
                    raw_config=config, substituted_config=substituted_config
                )
                self._cached_datasources[name] = datasource
            except gx_exceptions.DatasourceInitializationError as e:
                if save_changes:
                    self._datasource_store.delete(config)
                raise e

        self.config.datasources[name] = config  # type: ignore[index,assignment]

        return datasource

    def _construct_data_context_id(self) -> str:
        # Choose the id of the currently-configured expectations store, if it is a persistent store
        expectations_store = self.stores[self.expectations_store_name]
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

    def get_validation_result(  # noqa: PLR0913
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

        return validation_result

    def store_validation_result_metrics(
        self, requested_metrics, validation_results, target_store_name
    ) -> None:
        self._store_metrics(
            requested_metrics=requested_metrics,
            validation_results=validation_results,
            target_store_name=target_store_name,
        )

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
        data_asset_name = validation_results.meta.get(
            "active_batch_definition", {}
        ).get("data_asset_name")

        for expectation_suite_dependency, metrics_list in requested_metrics.items():
            if (expectation_suite_dependency != "*") and (
                expectation_suite_dependency != expectation_suite_name
            ):
                continue

            if not isinstance(metrics_list, list):
                raise gx_exceptions.DataContextError(
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
                                    metric_kwargs=metric_kwargs
                                ),
                            ),
                            metric_value,
                        )
                    except gx_exceptions.UnavailableMetricError:
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
    def test_yaml_config(  # noqa: PLR0913
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
        """Convenience method for testing yaml configs.

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
            name: Optional name of the component to instantiate
            class_name: Optional, overridden if provided in the config
            runtime_environment: Optional override for config items
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

    def profile_datasource(  # noqa: C901, PLR0912, PLR0913, PLR0915
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
            raise gx_exceptions.ProfilerError(f"No datasource {datasource_name} found.")

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
                raise gx_exceptions.ProfilerError(
                    "batch kwargs Generator {} not found. Specify the name of a generator configured in this datasource".format(
                        batch_kwargs_generator_name
                    )
                )

        available_data_asset_name_list = sorted(
            available_data_asset_name_list, key=lambda x: x[0]
        )

        if len(available_data_asset_name_list) == 0:
            raise gx_exceptions.ProfilerError(
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
            total_start_time = datetime.datetime.now()  # noqa: DTZ005

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

                except gx_exceptions.ProfilerError as err:
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
                datetime.datetime.now() - total_start_time  # noqa: DTZ005
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
    @public_api
    def build_data_docs(
        self,
        site_names=None,
        resource_identifiers=None,
        dry_run=False,
        build_index: bool = True,
    ):
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
        """
        return self._build_data_docs(
            site_names=site_names,
            resource_identifiers=resource_identifiers,
            dry_run=dry_run,
            build_index=build_index,
        )

    def _build_data_docs(
        self,
        site_names=None,
        resource_identifiers=None,
        dry_run=False,
        build_index: bool = True,
    ):
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
                        raise gx_exceptions.ClassInstantiationError(
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
        validation_result_identifier = result.list_validation_result_identifiers()[0]
        self.open_data_docs(resource_identifier=validation_result_identifier)

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
        if isinstance(value, dict) or isinstance(value, OrderedDict):  # noqa: PLR1701
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
            raise gx_exceptions.InvalidConfigError(
                "'config_variables_file_path' property is not found in config - setting it is required to use this feature"
            )

        config_variables_filepath = os.path.join(  # noqa: PTH118
            self.root_directory, config_variables_filepath  # type: ignore[arg-type]
        )

        os.makedirs(  # noqa: PTH103
            os.path.dirname(config_variables_filepath), exist_ok=True  # noqa: PTH120
        )
        if not os.path.isfile(config_variables_filepath):  # noqa: PTH113
            logger.info(
                "Creating new substitution_variables file at {config_variables_filepath}".format(
                    config_variables_filepath=config_variables_filepath
                )
            )
            with open(config_variables_filepath, "w") as template:
                template.write(CONFIG_VARIABLES_TEMPLATE)

        with open(config_variables_filepath, "w") as config_variables_file:
            yaml.dump(config_variables, config_variables_file)

    def _load_fluent_config(self, config_provider: _ConfigurationProvider) -> GxConfig:
        """Called at beginning of DataContext __init__ after config_providers init."""
        logger.info(
            f"{self.__class__.__name__} has not implemented `_load_fluent_config()` returning empty `GxConfig`"
        )
        return GxConfig(fluent_datasources=[])

    def _attach_fluent_config_datasources_and_build_data_connectors(
        self, config: GxConfig
    ):
        """Called at end of __init__"""
        for datasource in config.datasources:
            ds_name = datasource.name
            logger.info(f"Loaded '{ds_name}' from fluent config")

            datasource._rebuild_asset_data_connectors()

            self._add_fluent_datasource(datasource=datasource)

    def _synchronize_fluent_datasources(self) -> Dict[str, FluentDatasource]:
        """
        Update `self.fluent_config.fluent_datasources` with any newly added datasources.
        Should be called before serializing `fluent_config`.
        """
        fluent_datasources = self.fluent_datasources
        if fluent_datasources:
            self.fluent_config.update_datasources(datasources=fluent_datasources)

        return self.fluent_config.get_datasources_as_dict()

    @staticmethod
    def _resolve_id_and_ge_cloud_id(
        id: str | None, ge_cloud_id: str | None
    ) -> str | None:
        if id and ge_cloud_id:
            raise ValueError("Please only pass in either id or ge_cloud_id (not both)")
        return id or ge_cloud_id

    @staticmethod
    def _validate_expectation_suite_xor_expectation_suite_name(
        expectation_suite: Optional[ExpectationSuite] = None,
        expectation_suite_name: Optional[str] = None,
    ) -> None:
        """
        Validate that only one of expectation_suite or expectation_suite_name is specified.

        Raises:
            ValueError: Invalid arguments.
        """
        if expectation_suite_name is not None and expectation_suite is not None:
            raise ValueError(
                "Only one of expectation_suite_name or expectation_suite may be specified."
            )
        if expectation_suite_name is None and expectation_suite is None:
            raise ValueError(
                "One of expectation_suite_name or expectation_suite must be specified."
            )
