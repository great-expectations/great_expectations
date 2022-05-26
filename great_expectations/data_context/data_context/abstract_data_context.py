import logging
import os
import sys
from typing import Mapping, Optional, Union

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.config_peer import ConfigPeer

# usage statistics causing circular import
from great_expectations.core.usage_statistics.usage_statistics import (
    usage_statistics_enabled_method,
)
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context.types.base import (
    BaseYamlConfig,
    DataContextConfig,
    dataContextConfigSchema,
)
from great_expectations.marshmallow__shade import ValidationError
from great_expectations.rule_based_profiler.data_assistant.data_assistant_dispatcher import (
    DataAssistantDispatcher,
)

# from great_expectations.core.usage_statistics.events import UsageStatsEvents


logger = logging.getLogger(__name__)
yaml: YAMLHandler = YAMLHandler()


class AbstractDataContext(ConfigPeer):
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

    FALSEY_STRINGS = ["FALSE", "false", "False", "f", "F", "0"]

    # DOLLAR_SIGN_ESCAPE_STRING = r"\$"
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

    @property
    def config(self) -> BaseYamlConfig:
        return super().config

    @classmethod
    def validate_config(cls, project_config: Union[DataContextConfig, Mapping]) -> bool:
        if isinstance(project_config, DataContextConfig):
            return True
        try:
            dataContextConfigSchema.load(project_config)
        except ValidationError:
            raise
        return True

    @usage_statistics_enabled_method(
        event_name=UsageStatsEvents.DATA_CONTEXT___INIT__.value,
    )
    def __init__(
        self,
        project_config: Union[DataContextConfig, Mapping],
        context_root_dir: Optional[str] = None,
        runtime_environment: Optional[dict] = None,
    ) -> None:
        """DataContext constructor

        Args:
            context_root_dir: location to look for the ``great_expectations.yml`` file. If None, searches for the file \
            based on conventions for project subdirectories.
            runtime_environment: a dictionary of config variables that
            override both those set in config_variables.yml and the environment

        Returns:
            None
        """
        if not AbstractDataContext.validate_config(project_config):
            raise ge_exceptions.InvalidConfigError(
                "Your project_config is not valid. Try using the CLI check-config command."
            )
        # self._ge_cloud_mode = ge_cloud_mode
        # self._ge_cloud_config = ge_cloud_config
        self._project_config = project_config
        # self._apply_global_config_overrides()

        if context_root_dir is not None:
            context_root_dir = os.path.abspath(context_root_dir)
        self._context_root_directory = context_root_dir

        self.runtime_environment = runtime_environment or {}

        # Init plugin support
        # if self.plugins_directory is not None and os.path.exists(
        #        self.plugins_directory
        # ):
        #    sys.path.append(self.plugins_directory)

        # We want to have directories set up before initializing usage statistics so that we can obtain a context instance id
        self._in_memory_instance_id = (
            None  # This variable *may* be used in case we cannot save an instance id
        )

        # Init stores
        self._stores = {}
        # self._init_stores(self.project_config_with_variables_substituted.stores)

        # Init data_context_id
        # self._data_context_id = self._construct_data_context_id()

        # Override the project_config data_context_id if an expectations_store was already set up
        # self.config.anonymous_usage_statistics.data_context_id = self._data_context_id
        # self._initialize_usage_statistics(
        #     self.project_config_with_variables_substituted.anonymous_usage_statistics
        # )

        # Store cached datasources but don't init them
        self._cached_datasources = {}

        # Build the datasources we know about and have access to
        # self._init_datasources(self.project_config_with_variables_substituted)

        # Init validation operators
        # NOTE - 20200522 - JPC - A consistent approach to lazy loading for plugins will be useful here, harmonizing
        # the way that execution environments (AKA datasources), validation operators, site builders and other
        # plugins are built.
        self.validation_operators = {}
        # NOTE - 20210112 - Alex Sherstinsky - Validation Operators are planned to be deprecated.
        # if (
        #         "validation_operators" in self.get_config().commented_map
        #         and self.config.validation_operators
        # ):
        #     for (
        #             validation_operator_name,
        #             validation_operator_config,
        #     ) in self.config.validation_operators.items():
        #         self.add_validation_operator(
        #             validation_operator_name,
        #             validation_operator_config,
        #         )

        self._evaluation_parameter_dependencies_compiled = False
        self._evaluation_parameter_dependencies = {}

        self._assistants = DataAssistantDispatcher(data_context=self)

    def delete_datasource(self, datasource_name: str) -> None:
        """Delete a data source
        Args:
            datasource_name: The name of the datasource to delete.

        Raises:
            ValueError: If the datasource name isn't provided or cannot be found.
        """
        if datasource_name is None:
            raise ValueError("Datasource names must be a datasource name")
        else:
            datasource = self.get_datasource(datasource_name=datasource_name)
            if datasource:
                # remove key until we have a delete method on project_config
                # self.project_config_with_variables_substituted.datasources[
                # datasource_name].remove()
                del self.config["datasources"][datasource_name]
                del self._cached_datasources[datasource_name]
            else:
                raise ValueError(f"Datasource {datasource_name} not found")
