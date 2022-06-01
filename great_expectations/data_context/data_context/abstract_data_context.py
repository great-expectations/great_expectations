import configparser
import copy
import json
import logging
import os
from abc import ABC
from typing import Dict, List, Mapping, Optional, Union

import great_expectations.exceptions.exceptions as ge_exceptions
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context.types.base import (
    DataContextConfig,
    DataContextConfigDefaults,
    anonymizedUsageStatisticsSchema,
)
from great_expectations.data_context.util import (
    PasswordMasker,
    build_store_from_config,
    substitute_all_config_variables,
)
from great_expectations.rule_based_profiler.data_assistant.data_assistant_dispatcher import (
    DataAssistantDispatcher,
)

from great_expectations.data_context.store import (  # isort:skip
    ExpectationsStore,
    ValidationsStore,
)

from great_expectations.data_context.store import Store  # isort:skip
from great_expectations.data_context.store import TupleStoreBackend  # isort:skip

# this is here because of circular imports
# from great_expectations.rule_based_profiler import RuleBasedProfiler  # isort:skip
# from great_expectations.rule_based_profiler import RuleBasedProfilerResult  # isort:skip
#

logger = logging.getLogger(__name__)

yaml: YAMLHandler = YAMLHandler()


class AbstractDataContext(ABC):
    """
    Base class for all DataContexts that contain all context-agnostic data context operations.

    The class encapsulates most store / core components and convenience methods used to access them, meaning the
    majority of DataContext functionality lives here.

    TODO: eventually the dependency on ConfigPeer will be removed and this will become a pure ABC.
    """

    PROFILING_ERROR_CODE_TOO_MANY_DATA_ASSETS = 2
    PROFILING_ERROR_CODE_SPECIFIED_DATA_ASSETS_NOT_FOUND = 3
    PROFILING_ERROR_CODE_NO_BATCH_KWARGS_GENERATORS_FOUND = 4
    PROFILING_ERROR_CODE_MULTIPLE_BATCH_KWARGS_GENERATORS_FOUND = 5

    DOLLAR_SIGN_ESCAPE_STRING = r"\$"
    FALSEY_STRINGS = ["FALSE", "false", "False", "f", "F", "0"]

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

    UNCOMMITTED_DIRECTORIES = ["data_docs", "validations"]
    GE_UNCOMMITTED_DIR = "uncommitted"
    BASE_DIRECTORIES = [
        DataContextConfigDefaults.CHECKPOINTS_BASE_DIRECTORY.value,
        DataContextConfigDefaults.EXPECTATIONS_BASE_DIRECTORY.value,
        DataContextConfigDefaults.PLUGINS_BASE_DIRECTORY.value,
        DataContextConfigDefaults.PROFILERS_BASE_DIRECTORY.value,
        GE_UNCOMMITTED_DIR,
    ]
    GE_DIR = "great_expectations"
    GE_YML = "great_expectations.yml"
    GE_EDIT_NOTEBOOK_DIR = GE_UNCOMMITTED_DIR

    GLOBAL_CONFIG_PATHS = [
        os.path.expanduser("~/.great_expectations/great_expectations.conf"),
        "/etc/great_expectations.conf",
    ]

    _data_context = None

    def __init__(
        self,
        project_config: Union[DataContextConfig, Mapping],
        runtime_environment: Optional[dict] = None
        # context_root_dir do we keep this?
    ):
        self._project_config = project_config
        # is this the property that we want?
        self._project_config_with_variables_substituted: Optional[
            DataContextConfig
        ] = None

        self.runtime_environment = runtime_environment or {}
        self._apply_global_config_overrides()

        # Init stores
        self._stores = {}
        self._init_stores(self.project_config_with_variables_substituted.stores)

        # TODO : Datasources
        self._evaluation_parameter_dependencies_compiled = False
        self._evaluation_parameter_dependencies = {}

        self._assistants = DataAssistantDispatcher(data_context=self)

    ### properties

    @property
    def config(self) -> DataContextConfig:
        return self._project_config

    @property
    def stores(self):
        """A single holder for all Stores in this context"""
        return self._stores

    @property
    def expectations_store_name(self) -> Optional[str]:
        return self.project_config_with_variables_substituted.expectations_store_name

    @property
    def expectations_store(self) -> ExpectationsStore:
        return self.stores[self.expectations_store_name]

    @property
    def project_config_with_variables_substituted(self) -> DataContextConfig:
        return self.get_config_with_variables_substituted()

    @property
    def validations_store_name(self):
        return self.project_config_with_variables_substituted.validations_store_name

    @property
    def validations_store(self) -> ValidationsStore:
        return self.stores[self.validations_store_name]

    @property
    def checkpoint_store_name(self):
        return self.project_config_with_variables_substituted.checkpoint_store_name

    @property
    def profiler_store_name(self) -> str:
        return self.project_config_with_variables_substituted.profiler_store_name

    @property
    def config_variables(self):
        # Note Abe 20121114 : We should probably cache config_variables instead of loading them from disk every time.
        return dict(self._load_config_variables_file())

    @property
    def evaluation_parameter_store_name(self):
        return (
            self.project_config_with_variables_substituted.evaluation_parameter_store_name
        )

    ### public methods

    def list_stores(self):
        """List currently-configured Stores on this context"""

        stores = []
        for (
            name,
            value,
        ) in self.project_config_with_variables_substituted.stores.items():
            store_config = copy.deepcopy(value)
            store_config["name"] = name
            masked_config = PasswordMasker.sanitize_config(store_config)
            stores.append(masked_config)
        return stores

    def add_store(self, store_name: str, store_config: dict) -> Optional[Store]:
        """Add a new Store to the DataContext and (for convenience) return the instantiated Store object.

        Args:
            store_name (str): a key for the new Store in in self._stores
            store_config (dict): a config for the Store to add

        Returns:
            store (Store)
        """

        self.config["stores"][store_name] = store_config
        return self._build_store_from_config(store_name, store_config)

    def list_active_stores(self):
        """
        List active Stores on this context. Active stores are identified by setting the following parameters:
            expectations_store_name,
            validations_store_name,
            evaluation_parameter_store_name,
            checkpoint_store_name
            profiler_store_name
        """
        active_store_names: List[str] = [
            self.expectations_store_name,
            self.validations_store_name,
            self.evaluation_parameter_store_name,
        ]

        try:
            active_store_names.append(self.checkpoint_store_name)
        except (AttributeError, ge_exceptions.InvalidTopLevelConfigKeyError):
            logger.info(
                "Checkpoint store is not configured; omitting it from active stores"
            )

        try:
            active_store_names.append(self.profiler_store_name)
        except (AttributeError, ge_exceptions.InvalidTopLevelConfigKeyError):
            logger.info(
                "Profiler store is not configured; omitting it from active stores"
            )

        return [
            store for store in self.list_stores() if store["name"] in active_store_names
        ]

    ### private methods

    def _build_store_from_config(
        self,
        store_name: str,
        store_config: dict,
        runtime_environment: Optional[dict] = None,
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
                    "manually_initialize_store_backend_id": self.project_config_with_variables_substituted.anonymous_usage_statistics.data_context_id
                }
            )

        # Set suppress_store_backend_id = True if store is inactive and has a store_backend.
        if (
            store_name not in [store["name"] for store in self.list_active_stores()]
            and store_config.get("store_backend") is not None
        ):
            store_config["store_backend"].update({"suppress_store_backend_id": True})

        new_store = build_store_from_config(
            store_name=store_name,
            store_config=store_config,
            module_name=module_name,
            runtime_environment=runtime_environment,
        )
        self._stores[store_name] = new_store
        return new_store

    def _construct_data_context_id(self) -> str:
        """
        Choose the id of the currently-configured expectations store, if available and a persistent store.
        If not, it should choose the id stored in DataContextConfig.
        Returns:
            UUID to use as the data_context_id
        """
        # Choose the id of the currently-configured expectations store, if it is a persistent store
        expectations_store = self._stores[
            self.project_config_with_variables_substituted.expectations_store_name
        ]
        if isinstance(expectations_store.store_backend, TupleStoreBackend):
            # suppress_warnings since a warning will already have been issued during the store creation if there was an invalid store config
            return expectations_store.store_backend_id_warnings_suppressed

        # Otherwise choose the id stored in the project_config
        else:
            return (
                self.project_config_with_variables_substituted.anonymous_usage_statistics.data_context_id
            )

    def _load_config_variables_file(self):
        return {}

    def _apply_global_config_overrides(self) -> None:
        # check for global usage statistics opt out
        validation_errors = {}

        if self._check_global_usage_statistics_opt_out():
            logger.info(
                "Usage statistics is disabled globally. Applying override to project_config."
            )
            self.config.anonymous_usage_statistics.enabled = False

        # check for global data_context_id
        global_data_context_id = self._get_global_config_value(
            environment_variable="GE_DATA_CONTEXT_ID",
            conf_file_section="anonymous_usage_statistics",
            conf_file_option="data_context_id",
        )
        if global_data_context_id:
            data_context_id_errors = anonymizedUsageStatisticsSchema.validate(
                {"data_context_id": global_data_context_id}
            )
            if not data_context_id_errors:
                logger.info(
                    "data_context_id is defined globally. Applying override to project_config."
                )
                self.config.anonymous_usage_statistics.data_context_id = (
                    global_data_context_id
                )
            else:
                validation_errors.update(data_context_id_errors)
        # check for global usage_statistics url
        global_usage_statistics_url = self._get_global_config_value(
            environment_variable="GE_USAGE_STATISTICS_URL",
            conf_file_section="anonymous_usage_statistics",
            conf_file_option="usage_statistics_url",
        )
        if global_usage_statistics_url:
            usage_statistics_url_errors = anonymizedUsageStatisticsSchema.validate(
                {"usage_statistics_url": global_usage_statistics_url}
            )
            if not usage_statistics_url_errors:
                logger.info(
                    "usage_statistics_url is defined globally. Applying override to project_config."
                )
                self.config.anonymous_usage_statistics.usage_statistics_url = (
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

    # static methods

    @staticmethod
    def _check_global_usage_statistics_opt_out() -> bool:
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
                states[falsey_string] = False
            states["TRUE"] = True
            config.BOOLEAN_STATES = states
            config.read(config_path)
            try:
                if config.getboolean("anonymous_usage_statistics", "enabled") is False:
                    # If stats are disabled, then opt out is true
                    return True
            except (ValueError, configparser.Error):
                pass

    def _init_stores(self, store_configs: Dict[str, dict]) -> None:
        """Initialize all Stores for this DataContext.

        Stores are a good fit for reading/writing objects that:
            1. follow a clear key-value pattern, and
            2. are usually edited programmatically, using the Context

        Note that stores do NOT manage plugins.
        """
        for store_name, store_config in store_configs.items():
            self._build_store_from_config(store_name, store_config)

    #### class method
    @classmethod
    def _get_global_config_value(
        cls,
        environment_variable: Optional[str] = None,
        conf_file_section=None,
        conf_file_option=None,
    ) -> Optional[str]:
        assert (conf_file_section and conf_file_option) or (
            not conf_file_section and not conf_file_option
        ), "Must pass both 'conf_file_section' and 'conf_file_option' or neither."
        if environment_variable and os.environ.get(environment_variable, False):
            return os.environ.get(environment_variable)
        if conf_file_section and conf_file_option:
            for config_path in AbstractDataContext.GLOBAL_CONFIG_PATHS:
                config = configparser.ConfigParser()
                config.read(config_path)
                config_value = config.get(
                    conf_file_section, conf_file_option, fallback=None
                )
                if config_value:
                    return config_value
        return None

    def get_config_with_variables_substituted(self, config=None) -> DataContextConfig:
        """

        Args:
            config ():

        Returns:

        """
        substituted_config_variables = substitute_all_config_variables(
            # so this is actually a file that is loaded?
            self.config_variables,
            dict(os.environ),
            self.DOLLAR_SIGN_ESCAPE_STRING,
        )

        # Substitutions should have already occurred for GE Cloud configs at this point
        substitutions = {
            **substituted_config_variables,
            **dict(os.environ),
            **self.runtime_environment,
        }
        return DataContextConfig(
            **substitute_all_config_variables(
                config, substitutions, self.DOLLAR_SIGN_ESCAPE_STRING
            )
        )
