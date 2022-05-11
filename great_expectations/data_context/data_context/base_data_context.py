import configparser
import copy
import datetime
import errno
import json
import logging
import os
import sys
import traceback
import uuid
import warnings
import webbrowser
from collections import OrderedDict
from typing import Any, Callable, Dict, List, Mapping, Optional, Tuple, Union, cast

from dateutil.parser import parse
from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap

from great_expectations.core.config_peer import ConfigPeer
from great_expectations.core.usage_statistics.events import UsageStatsEvents
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.rule_based_profiler.config.base import (
    ruleBasedProfilerConfigSchema,
)
from great_expectations.rule_based_profiler.data_assistant.data_assistant_dispatcher import (
    DataAssistantDispatcher,
)

try:
    from typing import Literal
except ImportError:
    from typing_extensions import Literal

import great_expectations.exceptions as ge_exceptions
from great_expectations.checkpoint import Checkpoint, SimpleCheckpoint
from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
from great_expectations.core.batch import (
    Batch,
    BatchDefinition,
    BatchRequestBase,
    IDDict,
    get_batch_request_from_acceptable_arguments,
)
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.expectation_validation_result import get_metric_kwargs_id
from great_expectations.core.id_dict import BatchKwargs
from great_expectations.core.metric import ValidationMetricIdentifier
from great_expectations.core.run_identifier import RunIdentifier
from great_expectations.core.usage_statistics.anonymizers.anonymizer import Anonymizer
from great_expectations.core.usage_statistics.anonymizers.datasource_anonymizer import (
    DatasourceAnonymizer,
)
from great_expectations.core.usage_statistics.usage_statistics import (
    UsageStatisticsHandler,
    add_datasource_usage_statistics,
    get_batch_list_usage_statistics,
    run_validation_operator_usage_statistics,
    save_expectation_suite_usage_statistics,
    send_usage_message,
    usage_statistics_enabled_method,
)
from great_expectations.core.util import nested_update
from great_expectations.data_asset import DataAsset
from great_expectations.data_context.store import Store, TupleStoreBackend
from great_expectations.data_context.store.expectations_store import ExpectationsStore
from great_expectations.data_context.store.profiler_store import ProfilerStore
from great_expectations.data_context.store.validations_store import ValidationsStore
from great_expectations.data_context.templates import CONFIG_VARIABLES_TEMPLATE
from great_expectations.data_context.types.base import (
    CURRENT_GE_CONFIG_VERSION,
    DEFAULT_USAGE_STATISTICS_URL,
    AnonymizedUsageStatisticsConfig,
    CheckpointConfig,
    ConcurrencyConfig,
    DataContextConfig,
    DataContextConfigDefaults,
    DatasourceConfig,
    GeCloudConfig,
    ProgressBarsConfig,
    anonymizedUsageStatisticsSchema,
    dataContextConfigSchema,
    datasourceConfigSchema,
)
from great_expectations.data_context.types.refs import GeCloudIdAwareRef
from great_expectations.data_context.types.resource_identifiers import (
    ConfigurationIdentifier,
    ExpectationSuiteIdentifier,
    GeCloudIdentifier,
    ValidationResultIdentifier,
)
from great_expectations.data_context.util import (
    PasswordMasker,
    build_store_from_config,
    instantiate_class_from_config,
    load_class,
    parse_substitution_variable,
    substitute_all_config_variables,
    substitute_config_variable,
)
from great_expectations.dataset import Dataset
from great_expectations.datasource import LegacyDatasource
from great_expectations.datasource.data_connector.data_connector import DataConnector
from great_expectations.datasource.new_datasource import BaseDatasource, Datasource
from great_expectations.marshmallow__shade import ValidationError
from great_expectations.profile.basic_dataset_profiler import BasicDatasetProfiler
from great_expectations.render.renderer.site_builder import SiteBuilder
from great_expectations.rule_based_profiler import (
    RuleBasedProfiler,
    RuleBasedProfilerResult,
)
from great_expectations.rule_based_profiler.config import RuleBasedProfilerConfig
from great_expectations.util import (
    filter_properties_dict,
    verify_dynamic_loading_support,
)
from great_expectations.validator.validator import BridgeValidator, Validator

try:
    from sqlalchemy.exc import SQLAlchemyError
except ImportError:
    SQLAlchemyError = ge_exceptions.ProfilerError
logger = logging.getLogger(__name__)
yaml = YAML()
yaml.indent(mapping=2, sequence=4, offset=2)
yaml.default_flow_style = False


class BaseDataContext(ConfigPeer):
    '\n        This class implements most of the functionality of DataContext, with a few exceptions.\n\n        1. BaseDataContext does not attempt to keep its project_config in sync with a file on disc.\n        2. BaseDataContext doesn\'t attempt to "guess" paths or objects types. Instead, that logic is pushed\n            into DataContext class.\n\n        Together, these changes make BaseDataContext class more testable.\n\n    --ge-feature-maturity-info--\n\n        id: os_linux\n        title: OS - Linux\n        icon:\n        short_description:\n        description:\n        how_to_guide_url:\n        maturity: Production\n        maturity_details:\n            api_stability: N/A\n            implementation_completeness: N/A\n            unit_test_coverage: Complete\n            integration_infrastructure_test_coverage: Complete\n            documentation_completeness: Complete\n            bug_risk: Low\n\n        id: os_macos\n        title: OS - MacOS\n        icon:\n        short_description:\n        description:\n        how_to_guide_url:\n        maturity: Production\n        maturity_details:\n            api_stability: N/A\n            implementation_completeness: N/A\n            unit_test_coverage: Complete (local only)\n            integration_infrastructure_test_coverage: Complete (local only)\n            documentation_completeness: Complete\n            bug_risk: Low\n\n        id: os_windows\n        title: OS - Windows\n        icon:\n        short_description:\n        description:\n        how_to_guide_url:\n        maturity: Beta\n        maturity_details:\n            api_stability: N/A\n            implementation_completeness: N/A\n            unit_test_coverage: Minimal\n            integration_infrastructure_test_coverage: Minimal\n            documentation_completeness: Complete\n            bug_risk: Moderate\n    ------------------------------------------------------------\n        id: workflow_create_edit_expectations_cli_scaffold\n        title: Create and Edit Expectations - suite scaffold\n        icon:\n        short_description: Creating a new Expectation Suite using suite scaffold\n        description: Creating Expectation Suites through an interactive development loop using suite scaffold\n        how_to_guide_url: https://docs.greatexpectations.io/en/latest/how_to_guides/creating_and_editing_expectations/how_to_automatically_create_a_new_expectation_suite.html\n        maturity: Experimental (expect exciting changes to Profiler capability)\n        maturity_details:\n            api_stability: N/A\n            implementation_completeness: N/A\n            unit_test_coverage: N/A\n            integration_infrastructure_test_coverage: Partial\n            documentation_completeness: Complete\n            bug_risk: Low\n\n        id: workflow_create_edit_expectations_cli_edit\n        title: Create and Edit Expectations - CLI\n        icon:\n        short_description: Creating a new Expectation Suite using the CLI\n        description: Creating a Expectation Suite great_expectations suite new command\n        how_to_guide_url: https://docs.greatexpectations.io/en/latest/how_to_guides/creating_and_editing_expectations/how_to_create_a_new_expectation_suite_using_the_cli.html\n        maturity: Experimental (expect exciting changes to Profiler and Suite Renderer capability)\n        maturity_details:\n            api_stability: N/A\n            implementation_completeness: N/A\n            unit_test_coverage: N/A\n            integration_infrastructure_test_coverage: Partial\n            documentation_completeness: Complete\n            bug_risk: Low\n\n        id: workflow_create_edit_expectations_json_schema\n        title: Create and Edit Expectations - Json schema\n        icon:\n        short_description: Creating a new Expectation Suite from a json schema file\n        description: Creating a new Expectation Suite using JsonSchemaProfiler function and json schema file\n        how_to_guide_url: https://docs.greatexpectations.io/en/latest/how_to_guides/creating_and_editing_expectations/how_to_create_a_suite_from_a_json_schema_file.html\n        maturity: Experimental (expect exciting changes to Profiler capability)\n        maturity_details:\n            api_stability: N/A\n            implementation_completeness: N/A\n            unit_test_coverage: N/A\n            integration_infrastructure_test_coverage: Partial\n            documentation_completeness: Complete\n            bug_risk: Low\n\n    --ge-feature-maturity-info--\n'
    PROFILING_ERROR_CODE_TOO_MANY_DATA_ASSETS = 2
    PROFILING_ERROR_CODE_SPECIFIED_DATA_ASSETS_NOT_FOUND = 3
    PROFILING_ERROR_CODE_NO_BATCH_KWARGS_GENERATORS_FOUND = 4
    PROFILING_ERROR_CODE_MULTIPLE_BATCH_KWARGS_GENERATORS_FOUND = 5
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
    FALSEY_STRINGS = ["FALSE", "false", "False", "f", "F", "0"]
    GLOBAL_CONFIG_PATHS = [
        os.path.expanduser("~/.great_expectations/great_expectations.conf"),
        "/etc/great_expectations.conf",
    ]
    DOLLAR_SIGN_ESCAPE_STRING = "\\$"
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
    TEST_YAML_CONFIG_SUPPORTED_CHECKPOINT_TYPES = ["Checkpoint", "SimpleCheckpoint"]
    TEST_YAML_CONFIG_SUPPORTED_PROFILER_TYPES = ["RuleBasedProfiler"]
    ALL_TEST_YAML_CONFIG_DIAGNOSTIC_INFO_TYPES = [
        "__substitution_error__",
        "__yaml_parse_error__",
        "__custom_subclass_not_core_ge__",
        "__class_name_not_provided__",
    ]
    ALL_TEST_YAML_CONFIG_SUPPORTED_TYPES = (
        (
            (
                TEST_YAML_CONFIG_SUPPORTED_STORE_TYPES
                + TEST_YAML_CONFIG_SUPPORTED_DATASOURCE_TYPES
            )
            + TEST_YAML_CONFIG_SUPPORTED_DATA_CONNECTOR_TYPES
        )
        + TEST_YAML_CONFIG_SUPPORTED_CHECKPOINT_TYPES
    ) + TEST_YAML_CONFIG_SUPPORTED_PROFILER_TYPES
    _data_context = None

    @classmethod
    def validate_config(
        cls, project_config: Union[(DataContextConfig, Mapping)]
    ) -> bool:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if isinstance(project_config, DataContextConfig):
            return True
        try:
            dataContextConfigSchema.load(project_config)
        except ValidationError:
            raise
        return True

    @usage_statistics_enabled_method(
        event_name=UsageStatsEvents.DATA_CONTEXT___INIT__.value
    )
    def __init__(
        self,
        project_config: Union[(DataContextConfig, Mapping)],
        context_root_dir: Optional[str] = None,
        runtime_environment: Optional[dict] = None,
        ge_cloud_mode: bool = False,
        ge_cloud_config: Optional[GeCloudConfig] = None,
    ) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "DataContext constructor\n\n        Args:\n            context_root_dir: location to look for the ``great_expectations.yml`` file. If None, searches for the file             based on conventions for project subdirectories.\n            runtime_environment: a dictionary of config variables that\n            override both those set in config_variables.yml and the environment\n\n        Returns:\n            None\n        "
        if not BaseDataContext.validate_config(project_config):
            raise ge_exceptions.InvalidConfigError(
                "Your project_config is not valid. Try using the CLI check-config command."
            )
        self._ge_cloud_mode = ge_cloud_mode
        self._ge_cloud_config = ge_cloud_config
        self._project_config = project_config
        self._apply_global_config_overrides()
        if context_root_dir is not None:
            context_root_dir = os.path.abspath(context_root_dir)
        self._context_root_directory = context_root_dir
        self.runtime_environment = runtime_environment or {}
        if (self.plugins_directory is not None) and os.path.exists(
            self.plugins_directory
        ):
            sys.path.append(self.plugins_directory)
        self._in_memory_instance_id = None
        self._stores = {}
        self._init_stores(self.project_config_with_variables_substituted.stores)
        self._data_context_id = self._construct_data_context_id()
        self.config.anonymous_usage_statistics.data_context_id = self._data_context_id
        self._initialize_usage_statistics(
            self.project_config_with_variables_substituted.anonymous_usage_statistics
        )
        self._cached_datasources = {}
        self._init_datasources(self.project_config_with_variables_substituted)
        self.validation_operators = {}
        if (
            "validation_operators" in self.get_config().commented_map
        ) and self.config.validation_operators:
            for (
                validation_operator_name,
                validation_operator_config,
            ) in self.config.validation_operators.items():
                self.add_validation_operator(
                    validation_operator_name, validation_operator_config
                )
        self._evaluation_parameter_dependencies_compiled = False
        self._evaluation_parameter_dependencies = {}
        self._assistants = DataAssistantDispatcher(data_context=self)

    @property
    def ge_cloud_config(self) -> Optional[GeCloudConfig]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return self._ge_cloud_config

    @property
    def ge_cloud_mode(self) -> bool:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return self._ge_cloud_mode

    def _build_store_from_config(
        self, store_name: str, store_config: dict
    ) -> Optional[Store]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        module_name = "great_expectations.data_context.store"
        if (store_name == self.expectations_store_name) and store_config.get(
            "store_backend"
        ):
            store_config["store_backend"].update(
                {
                    "manually_initialize_store_backend_id": self.project_config_with_variables_substituted.anonymous_usage_statistics.data_context_id
                }
            )
        if (
            store_name not in [store["name"] for store in self.list_active_stores()]
        ) and (store_config.get("store_backend") is not None):
            store_config["store_backend"].update({"suppress_store_backend_id": True})
        new_store = build_store_from_config(
            store_name=store_name,
            store_config=store_config,
            module_name=module_name,
            runtime_environment={"root_directory": self.root_directory},
        )
        self._stores[store_name] = new_store
        return new_store

    def _init_stores(self, store_configs: Dict[(str, dict)]) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Initialize all Stores for this DataContext.\n\n        Stores are a good fit for reading/writing objects that:\n            1. follow a clear key-value pattern, and\n            2. are usually edited programmatically, using the Context\n\n        Note that stores do NOT manage plugins.\n        "
        for (store_name, store_config) in store_configs.items():
            self._build_store_from_config(store_name, store_config)

    def _init_datasources(self, config: DataContextConfig) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if not config.datasources:
            return
        for datasource_name in config.datasources:
            try:
                self._cached_datasources[datasource_name] = self.get_datasource(
                    datasource_name=datasource_name
                )
            except ge_exceptions.DatasourceInitializationError as e:
                logger.warning(f"Cannot initialize datasource {datasource_name}: {e}")
                pass

    def _apply_global_config_overrides(self) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        validation_errors = {}
        if self._check_global_usage_statistics_opt_out():
            logger.info(
                "Usage statistics is disabled globally. Applying override to project_config."
            )
            self.config.anonymous_usage_statistics.enabled = False
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
                "The following globally-defined config variables failed validation:\n{}\n\nPlease fix the variables if you would like to apply global values to project_config.".format(
                    json.dumps(validation_errors, indent=2)
                )
            )

    @classmethod
    def _get_global_config_value(
        cls,
        environment_variable: Optional[str] = None,
        conf_file_section=None,
        conf_file_option=None,
    ) -> Optional[str]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        assert (conf_file_section and conf_file_option) or (
            (not conf_file_section) and (not conf_file_option)
        ), "Must pass both 'conf_file_section' and 'conf_file_option' or neither."
        if environment_variable and os.environ.get(environment_variable, False):
            return os.environ.get(environment_variable)
        if conf_file_section and conf_file_option:
            for config_path in BaseDataContext.GLOBAL_CONFIG_PATHS:
                config = configparser.ConfigParser()
                config.read(config_path)
                config_value = config.get(
                    conf_file_section, conf_file_option, fallback=None
                )
                if config_value:
                    return config_value
        return None

    @staticmethod
    def _check_global_usage_statistics_opt_out() -> bool:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if os.environ.get("GE_USAGE_STATS", False):
            ge_usage_stats = os.environ.get("GE_USAGE_STATS")
            if ge_usage_stats in BaseDataContext.FALSEY_STRINGS:
                return True
            else:
                logger.warning(
                    "GE_USAGE_STATS environment variable must be one of: {}".format(
                        BaseDataContext.FALSEY_STRINGS
                    )
                )
        for config_path in BaseDataContext.GLOBAL_CONFIG_PATHS:
            config = configparser.ConfigParser()
            states = config.BOOLEAN_STATES
            for falsey_string in BaseDataContext.FALSEY_STRINGS:
                states[falsey_string] = False
            states["TRUE"] = True
            states["True"] = True
            config.BOOLEAN_STATES = states
            config.read(config_path)
            try:
                if config.getboolean("anonymous_usage_statistics", "enabled") is False:
                    return True
            except (ValueError, configparser.Error):
                pass
        return False

    def _construct_data_context_id(self) -> str:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        Choose the id of the currently-configured expectations store, if available and a persistent store.\n        If not, it should choose the id stored in DataContextConfig.\n        Returns:\n            UUID to use as the data_context_id\n        "
        if self.ge_cloud_mode:
            return self.ge_cloud_config.organization_id
        expectations_store = self._stores[
            self.project_config_with_variables_substituted.expectations_store_name
        ]
        if isinstance(expectations_store.store_backend, TupleStoreBackend):
            return expectations_store.store_backend_id_warnings_suppressed
        else:
            return (
                self.project_config_with_variables_substituted.anonymous_usage_statistics.data_context_id
            )

    def _initialize_usage_statistics(
        self, usage_statistics_config: AnonymizedUsageStatisticsConfig
    ) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Initialize the usage statistics system."
        if not usage_statistics_config.enabled:
            logger.info("Usage statistics is disabled; skipping initialization.")
            self._usage_statistics_handler = None
            return
        self._usage_statistics_handler = UsageStatisticsHandler(
            data_context=self,
            data_context_id=self._data_context_id,
            usage_statistics_url=usage_statistics_config.usage_statistics_url,
        )

    def add_store(self, store_name: str, store_config: dict) -> Optional[Store]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Add a new Store to the DataContext and (for convenience) return the instantiated Store object.\n\n        Args:\n            store_name (str): a key for the new Store in in self._stores\n            store_config (dict): a config for the Store to add\n\n        Returns:\n            store (Store)\n        "
        self.config["stores"][store_name] = store_config
        return self._build_store_from_config(store_name, store_config)

    def add_validation_operator(
        self, validation_operator_name: str, validation_operator_config: dict
    ) -> "ValidationOperator":
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Add a new ValidationOperator to the DataContext and (for convenience) return the instantiated object.\n\n        Args:\n            validation_operator_name (str): a key for the new ValidationOperator in in self._validation_operators\n            validation_operator_config (dict): a config for the ValidationOperator to add\n\n        Returns:\n            validation_operator (ValidationOperator)\n        "
        self.config["validation_operators"][
            validation_operator_name
        ] = validation_operator_config
        config = self.project_config_with_variables_substituted.validation_operators[
            validation_operator_name
        ]
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

    def _normalize_absolute_or_relative_path(
        self, path: Optional[str]
    ) -> Optional[str]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if path is None:
            return
        if os.path.isabs(path):
            return path
        else:
            return os.path.join(self.root_directory, path)

    def _normalize_store_path(self, resource_store):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if resource_store["type"] == "filesystem":
            if not os.path.isabs(resource_store["base_directory"]):
                resource_store["base_directory"] = os.path.join(
                    self.root_directory, resource_store["base_directory"]
                )
        return resource_store

    def get_site_names(self) -> List[str]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Get a list of configured site names."
        return list(
            self.project_config_with_variables_substituted.data_docs_sites.keys()
        )

    def get_docs_sites_urls(
        self,
        resource_identifier=None,
        site_name: Optional[str] = None,
        only_if_exists=True,
        site_names: Optional[List[str]] = None,
    ) -> List[Dict[(str, str)]]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        Get URLs for a resource for all data docs sites.\n\n        This function will return URLs for any configured site even if the sites\n        have not been built yet.\n\n        Args:\n            resource_identifier (object): optional. It can be an identifier of\n                ExpectationSuite's, ValidationResults and other resources that\n                have typed identifiers. If not provided, the method will return\n                the URLs of the index page.\n            site_name: Optionally specify which site to open. If not specified,\n                return all urls in the project.\n            site_names: Optionally specify which sites are active. Sites not in\n                this list are not processed, even if specified in site_name.\n\n        Returns:\n            list: a list of URLs. Each item is the URL for the resource for a\n                data docs site\n        "
        unfiltered_sites = (
            self.project_config_with_variables_substituted.data_docs_sites
        )
        sites = (
            {k: v for (k, v) in unfiltered_sites.items() if (k in site_names)}
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
        for (_site_name, site_config) in sites.items():
            site_builder = self._load_site_builder_from_site_config(site_config)
            url = site_builder.get_resource_url(
                resource_identifier=resource_identifier, only_if_exists=only_if_exists
            )
            site_urls.append({"site_name": _site_name, "site_url": url})
        return site_urls

    def _load_site_builder_from_site_config(self, site_config) -> SiteBuilder:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
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

    @usage_statistics_enabled_method(
        event_name=UsageStatsEvents.DATA_CONTEXT_OPEN_DATA_DOCS.value
    )
    def open_data_docs(
        self,
        resource_identifier: Optional[str] = None,
        site_name: Optional[str] = None,
        only_if_exists: bool = True,
    ) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        '\n        A stdlib cross-platform way to open a file in a browser.\n\n        Args:\n            resource_identifier: ExpectationSuiteIdentifier,\n                ValidationResultIdentifier or any other type\'s identifier. The\n                argument is optional - when not supplied, the method returns the\n                URL of the index page.\n            site_name: Optionally specify which site to open. If not specified,\n                open all docs found in the project.\n            only_if_exists: Optionally specify flag to pass to "self.get_docs_sites_urls()".\n        '
        data_docs_urls: List[Dict[(str, str)]] = self.get_docs_sites_urls(
            resource_identifier=resource_identifier,
            site_name=site_name,
            only_if_exists=only_if_exists,
        )
        urls_to_open: List[str] = [site["site_url"] for site in data_docs_urls]
        for url in urls_to_open:
            if url is not None:
                logger.debug(f"Opening Data Docs found here: {url}")
                webbrowser.open(url)

    @property
    def root_directory(self):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "The root directory for configuration objects in the data context; the location in which\n        ``great_expectations.yml`` is located."
        return self._context_root_directory

    @property
    def plugins_directory(self):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "The directory in which custom plugin modules should be placed."
        return self._normalize_absolute_or_relative_path(
            self.project_config_with_variables_substituted.plugins_directory
        )

    @property
    def usage_statistics_handler(self) -> Optional[UsageStatisticsHandler]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return self._usage_statistics_handler

    @property
    def project_config_with_variables_substituted(self) -> DataContextConfig:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return self.get_config_with_variables_substituted()

    @property
    def anonymous_usage_statistics(self):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return self.project_config_with_variables_substituted.anonymous_usage_statistics

    @property
    def concurrency(self) -> Optional[ConcurrencyConfig]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return self.project_config_with_variables_substituted.concurrency

    @property
    def progress_bars(self) -> Optional[ProgressBarsConfig]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return self.project_config_with_variables_substituted.progress_bars

    @property
    def notebooks(self):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return self.project_config_with_variables_substituted.notebooks

    @property
    def stores(self):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "A single holder for all Stores in this context"
        return self._stores

    @property
    def datasources(self) -> Dict[(str, Union[(LegacyDatasource, BaseDatasource)])]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "A single holder for all Datasources in this context"
        return self._cached_datasources

    @property
    def checkpoint_store_name(self):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        try:
            return self.project_config_with_variables_substituted.checkpoint_store_name
        except AttributeError:
            from great_expectations.data_context.store.checkpoint_store import (
                CheckpointStore,
            )

            if CheckpointStore.default_checkpoints_exist(
                directory_path=self.root_directory
            ):
                return DataContextConfigDefaults.DEFAULT_CHECKPOINT_STORE_NAME.value
            if self.root_directory:
                error_message: str = f"""Attempted to access the "checkpoint_store_name" field with no `checkpoints` directory.
  Please create the following directory: {os.path.join(self.root_directory, DataContextConfigDefaults.DEFAULT_CHECKPOINT_STORE_BASE_DIRECTORY_RELATIVE_NAME.value)}
  To use the new "Checkpoint Store" feature, please update your configuration to the new version number {float(CURRENT_GE_CONFIG_VERSION)}.
  Visit https://docs.greatexpectations.io/docs/guides/miscellaneous/migration_guide#migrating-to-the-batch-request-v3-api to learn more about the upgrade process."""
            else:
                error_message: str = f"""Attempted to access the "checkpoint_store_name" field with no `checkpoints` directory.
  Please create a `checkpoints` directory in your Great Expectations project " f"directory.
  To use the new "Checkpoint Store" feature, please update your configuration to the new version number {float(CURRENT_GE_CONFIG_VERSION)}.
  Visit https://docs.greatexpectations.io/docs/guides/miscellaneous/migration_guide#migrating-to-the-batch-request-v3-api to learn more about the upgrade process."""
            raise ge_exceptions.InvalidTopLevelConfigKeyError(error_message)

    @property
    def checkpoint_store(self) -> "CheckpointStore":
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        checkpoint_store_name: str = self.checkpoint_store_name
        try:
            return self.stores[checkpoint_store_name]
        except KeyError:
            from great_expectations.data_context.store.checkpoint_store import (
                CheckpointStore,
            )

            if CheckpointStore.default_checkpoints_exist(
                directory_path=self.root_directory
            ):
                logger.warning(
                    f"""Checkpoint store named "{checkpoint_store_name}" is not a configured store, so will try to use default Checkpoint store.
  Please update your configuration to the new version number {float(CURRENT_GE_CONFIG_VERSION)} in order to use the new "Checkpoint Store" feature.
  Visit https://docs.greatexpectations.io/docs/guides/miscellaneous/migration_guide#migrating-to-the-batch-request-v3-api to learn more about the upgrade process."""
                )
                return self._build_store_from_config(
                    checkpoint_store_name,
                    DataContextConfigDefaults.DEFAULT_STORES.value[
                        checkpoint_store_name
                    ],
                )
            raise ge_exceptions.StoreConfigurationError(
                f'Attempted to access the Checkpoint store named "{checkpoint_store_name}", which is not a configured store.'
            )

    @property
    def profiler_store_name(self) -> str:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        try:
            return self.project_config_with_variables_substituted.profiler_store_name
        except AttributeError:
            if BaseDataContext._default_profilers_exist(
                directory_path=self.root_directory
            ):
                return DataContextConfigDefaults.DEFAULT_PROFILER_STORE_NAME.value
            if self.root_directory:
                error_message: str = f"""Attempted to access the "profiler_store_name" field with no `profilers` directory.
  Please create the following directory: {os.path.join(self.root_directory, DataContextConfigDefaults.DEFAULT_PROFILER_STORE_BASE_DIRECTORY_RELATIVE_NAME.value)}
  To use the new "Profiler Store" feature, please update your configuration to the new version number {float(CURRENT_GE_CONFIG_VERSION)}.
  Visit https://docs.greatexpectations.io/docs/guides/miscellaneous/migration_guide#migrating-to-the-batch-request-v3-api to learn more about the upgrade process."""
            else:
                error_message: str = f"""Attempted to access the "profiler_store_name" field with no `profilers` directory.
  Please create a `profilers` directory in your Great Expectations project " f"directory.
  To use the new "Profiler Store" feature, please update your configuration to the new version number {float(CURRENT_GE_CONFIG_VERSION)}.
  Visit https://docs.greatexpectations.io/docs/guides/miscellaneous/migration_guide#migrating-to-the-batch-request-v3-api to learn more about the upgrade process."""
            raise ge_exceptions.InvalidTopLevelConfigKeyError(error_message)

    @property
    def profiler_store(self) -> ProfilerStore:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        profiler_store_name: str = self.profiler_store_name
        try:
            return self.stores[profiler_store_name]
        except KeyError:
            if BaseDataContext._default_profilers_exist(
                directory_path=self.root_directory
            ):
                logger.warning(
                    f"""Profiler store named "{profiler_store_name}" is not a configured store, so will try to use default Profiler store.
  Please update your configuration to the new version number {float(CURRENT_GE_CONFIG_VERSION)} in order to use the new "Profiler Store" feature.
  Visit https://docs.greatexpectations.io/docs/guides/miscellaneous/migration_guide#migrating-to-the-batch-request-v3-api to learn more about the upgrade process."""
                )
                return self._build_store_from_config(
                    profiler_store_name,
                    DataContextConfigDefaults.DEFAULT_STORES.value[profiler_store_name],
                )
            raise ge_exceptions.StoreConfigurationError(
                f'Attempted to access the Profiler store named "{profiler_store_name}", which is not a configured store.'
            )

    @staticmethod
    def _default_profilers_exist(directory_path: Optional[str]) -> bool:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if not directory_path:
            return False
        profiler_directory_path: str = os.path.join(
            directory_path,
            DataContextConfigDefaults.DEFAULT_PROFILER_STORE_BASE_DIRECTORY_RELATIVE_NAME.value,
        )
        return os.path.isdir(profiler_directory_path)

    @property
    def expectations_store_name(self) -> Optional[str]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return self.project_config_with_variables_substituted.expectations_store_name

    @property
    def expectations_store(self) -> ExpectationsStore:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return self.stores[self.expectations_store_name]

    @property
    def data_context_id(self):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return (
            self.project_config_with_variables_substituted.anonymous_usage_statistics.data_context_id
        )

    @property
    def instance_id(self):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        instance_id = self._load_config_variables_file().get("instance_id")
        if instance_id is None:
            if self._in_memory_instance_id is not None:
                return self._in_memory_instance_id
            instance_id = str(uuid.uuid4())
            self._in_memory_instance_id = instance_id
        return instance_id

    @property
    def config_variables(self):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return dict(self._load_config_variables_file())

    @property
    def config(self) -> DataContextConfig:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return self._project_config

    def _load_config_variables_file(self):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        Get all config variables from the default location. For Data Contexts in GE Cloud mode, config variables\n        have already been interpolated before being sent from the Cloud API.\n        "
        if self.ge_cloud_mode:
            return {}
        config_variables_file_path = cast(
            DataContextConfig, self.get_config()
        ).config_variables_file_path
        if config_variables_file_path:
            try:
                defined_path = substitute_config_variable(
                    config_variables_file_path, dict(os.environ)
                )
                if not os.path.isabs(defined_path):
                    root_directory = self.root_directory or os.curdir
                else:
                    root_directory = ""
                var_path = os.path.join(root_directory, defined_path)
                with open(var_path) as config_variables_file:
                    return yaml.load(config_variables_file) or {}
            except OSError as e:
                if e.errno != errno.ENOENT:
                    raise
                logger.debug("Generating empty config variables file.")
                return {}
        else:
            return {}

    def get_config_with_variables_substituted(self, config=None) -> DataContextConfig:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        Substitute vars in config of form ${var} or $(var) with values found in the following places,\n        in order of precedence: ge_cloud_config (for Data Contexts in GE Cloud mode), runtime_environment,\n        environment variables, config_variables, or ge_cloud_config_variable_defaults (allows certain variables to\n        be optional in GE Cloud mode).\n        "
        if not config:
            config = self.config
        substituted_config_variables = substitute_all_config_variables(
            self.config_variables, dict(os.environ), self.DOLLAR_SIGN_ESCAPE_STRING
        )
        substitutions = {
            **substituted_config_variables,
            **dict(os.environ),
            **self.runtime_environment,
        }
        if self.ge_cloud_mode:
            ge_cloud_config_variable_defaults = {
                "plugins_directory": self._normalize_absolute_or_relative_path(
                    DataContextConfigDefaults.DEFAULT_PLUGINS_DIRECTORY.value
                ),
                "usage_statistics_url": DEFAULT_USAGE_STATISTICS_URL,
            }
            for (config_variable, value) in ge_cloud_config_variable_defaults.items():
                if substitutions.get(config_variable) is None:
                    logger.info(
                        f'Config variable "{config_variable}" was not found in environment or global config ({self.GLOBAL_CONFIG_PATHS}). Using default value "{value}" instead. If you would like to use a different value, please specify it in an environment variable or in a great_expectations.conf file located at one of the above paths, in a section named "ge_cloud_config".'
                    )
                    substitutions[config_variable] = value
        return DataContextConfig(
            **substitute_all_config_variables(
                config, substitutions, self.DOLLAR_SIGN_ESCAPE_STRING
            )
        )

    def escape_all_config_variables(
        self,
        value: Union[(str, dict, list)],
        dollar_sign_escape_string: str = DOLLAR_SIGN_ESCAPE_STRING,
        skip_if_substitution_variable: bool = True,
    ) -> Union[(str, dict, list)]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        Replace all `$` characters with the DOLLAR_SIGN_ESCAPE_STRING\n\n        Args:\n            value: config variable value\n            dollar_sign_escape_string: replaces instances of `$`\n            skip_if_substitution_variable: skip if the value is of the form ${MYVAR} or $MYVAR\n\n        Returns:\n            input value with all `$` characters replaced with the escape string\n        "
        if isinstance(value, dict) or isinstance(value, OrderedDict):
            return {
                k: self.escape_all_config_variables(
                    v, dollar_sign_escape_string, skip_if_substitution_variable
                )
                for (k, v) in value.items()
            }
        elif isinstance(value, list):
            return [
                self.escape_all_config_variables(
                    v, dollar_sign_escape_string, skip_if_substitution_variable
                )
                for v in value
            ]
        if skip_if_substitution_variable:
            if parse_substitution_variable(value) is None:
                return value.replace("$", dollar_sign_escape_string)
            else:
                return value
        else:
            return value.replace("$", dollar_sign_escape_string)

    def save_config_variable(
        self, config_variable_name, value, skip_if_substitution_variable: bool = True
    ) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        'Save config variable value\n        Escapes $ unless they are used in substitution variables e.g. the $ characters in ${SOME_VAR} or $SOME_VAR are not escaped\n\n        Args:\n            config_variable_name: name of the property\n            value: the value to save for the property\n            skip_if_substitution_variable: set to False to escape $ in values in substitution variable form e.g. ${SOME_VAR} -> r"\\${SOME_VAR}" or $SOME_VAR -> r"\\$SOME_VAR"\n\n        Returns:\n            None\n        '
        config_variables = self._load_config_variables_file()
        value = self.escape_all_config_variables(
            value,
            self.DOLLAR_SIGN_ESCAPE_STRING,
            skip_if_substitution_variable=skip_if_substitution_variable,
        )
        config_variables[config_variable_name] = value
        config_variables_filepath = cast(
            DataContextConfig, self.get_config()
        ).config_variables_file_path
        if not config_variables_filepath:
            raise ge_exceptions.InvalidConfigError(
                "'config_variables_file_path' property is not found in config - setting it is required to use this feature"
            )
        config_variables_filepath = os.path.join(
            self.root_directory, config_variables_filepath
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

    def delete_datasource(self, datasource_name: str) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Delete a data source\n        Args:\n            datasource_name: The name of the datasource to delete.\n\n        Raises:\n            ValueError: If the datasource name isn't provided or cannot be found.\n        "
        if datasource_name is None:
            raise ValueError("Datasource names must be a datasource name")
        else:
            datasource = self.get_datasource(datasource_name=datasource_name)
            if datasource:
                del self.config["datasources"][datasource_name]
                del self._cached_datasources[datasource_name]
            else:
                raise ValueError(f"Datasource {datasource_name} not found")

    def get_available_data_asset_names(
        self, datasource_names=None, batch_kwargs_generator_names=None
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Inspect datasource and batch kwargs generators to provide available data_asset objects.\n\n        Args:\n            datasource_names: list of datasources for which to provide available data_asset_name objects. If None,             return available data assets for all datasources.\n            batch_kwargs_generator_names: list of batch kwargs generators for which to provide available\n            data_asset_name objects.\n\n        Returns:\n            data_asset_names (dict): Dictionary describing available data assets\n            ::\n\n                {\n                  datasource_name: {\n                    batch_kwargs_generator_name: [ data_asset_1, data_asset_2, ... ]\n                    ...\n                  }\n                  ...\n                }\n\n        "
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
            if len(batch_kwargs_generator_names) == len(datasource_names):
                for (idx, datasource_name) in enumerate(datasource_names):
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
                    "If providing batch kwargs generator, you must either specify one for each datasource or only one datasource."
                )
        else:
            for datasource_name in datasource_names:
                try:
                    datasource = self.get_datasource(datasource_name)
                    data_asset_names[
                        datasource_name
                    ] = datasource.get_available_data_asset_names()
                except ValueError:
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
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Builds batch kwargs using the provided datasource, batch kwargs generator, and batch_parameters.\n\n        Args:\n            datasource (str): the name of the datasource for which to build batch_kwargs\n            batch_kwargs_generator (str): the name of the batch kwargs generator to use to build batch_kwargs\n            data_asset_name (str): an optional name batch_parameter\n            **kwargs: additional batch_parameters\n\n        Returns:\n            BatchKwargs\n\n        "
        if kwargs.get("name"):
            if data_asset_name:
                raise ValueError(
                    "Cannot provide both 'name' and 'data_asset_name'. Please use 'data_asset_name' only."
                )
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

    def _get_batch_v2(
        self,
        batch_kwargs: Union[(dict, BatchKwargs)],
        expectation_suite_name: Union[(str, ExpectationSuite)],
        data_asset_type=None,
        batch_parameters=None,
    ) -> DataAsset:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Build a batch of data using batch_kwargs, and return a DataAsset with expectation_suite_name attached. If\n        batch_parameters are included, they will be available as attributes of the batch.\n        Args:\n            batch_kwargs: the batch_kwargs to use; must include a datasource key\n            expectation_suite_name: The ExpectationSuite or the name of the expectation_suite to get\n            data_asset_type: the type of data_asset to build, with associated expectation implementations. This can\n                generally be inferred from the datasource.\n            batch_parameters: optional parameters to store as the reference description of the batch. They should\n                reflect parameters that would provide the passed BatchKwargs.\n        Returns:\n            DataAsset\n        "
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
                "expectation_suite_name must be an ExpectationSuite, ExpectationSuiteIdentifier or string."
            )
        if isinstance(expectation_suite_name, ExpectationSuite):
            expectation_suite = expectation_suite_name
        elif isinstance(expectation_suite_name, ExpectationSuiteIdentifier):
            expectation_suite = self.get_expectation_suite(
                expectation_suite_name.expectation_suite_name
            )
        else:
            expectation_suite = self.get_expectation_suite(expectation_suite_name)
        datasource = self.get_datasource(batch_kwargs.get("datasource"))
        batch = datasource.get_batch(
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

    def _get_batch_v3(
        self,
        datasource_name: Optional[str] = None,
        data_connector_name: Optional[str] = None,
        data_asset_name: Optional[str] = None,
        *,
        batch_request: Optional[BatchRequestBase] = None,
        batch_data: Optional[Any] = None,
        data_connector_query: Optional[Union[(IDDict, dict)]] = None,
        batch_identifiers: Optional[dict] = None,
        limit: Optional[int] = None,
        index: Optional[Union[(int, list, tuple, slice, str)]] = None,
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
    ) -> Union[(Batch, DataAsset)]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Get exactly one batch, based on a variety of flexible input types.\n\n        Args:\n            datasource_name\n            data_connector_name\n            data_asset_name\n\n            batch_request\n            batch_data\n            data_connector_query\n            batch_identifiers\n            batch_filter_parameters\n\n            limit\n            index\n            custom_filter_function\n\n            batch_spec_passthrough\n\n            sampling_method\n            sampling_kwargs\n\n            splitter_method\n            splitter_kwargs\n\n            **kwargs\n\n        Returns:\n            (Batch) The requested batch\n\n        This method does not require typed or nested inputs.\n        Instead, it is intended to help the user pick the right parameters.\n\n        This method attempts to return exactly one batch.\n        If 0 or more than 1 batches would be returned, it raises an error.\n        "
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
        warnings.warn(
            "get_batch is deprecated for the V3 Batch Request API as of v0.13.20 and will be removed in v0.16. Please use get_batch_list instead.",
            DeprecationWarning,
        )
        if len(batch_list) != 1:
            raise ValueError(
                f"Got {len(batch_list)} batches instead of a single batch. If you would like to use a BatchRequest to return multiple batches, please use get_batch_list directly instead of calling get_batch"
            )
        return batch_list[0]

    @usage_statistics_enabled_method(
        event_name=UsageStatsEvents.DATA_CONTEXT_RUN_VALIDATION_OPERATOR.value,
        args_payload_fn=run_validation_operator_usage_statistics,
    )
    def run_validation_operator(
        self,
        validation_operator_name: str,
        assets_to_validate: List,
        run_id: Optional[Union[(str, RunIdentifier)]] = None,
        evaluation_parameters: Optional[dict] = None,
        run_name: Optional[str] = None,
        run_time: Optional[Union[(str, datetime.datetime)]] = None,
        result_format: Optional[Union[(str, dict)]] = None,
        **kwargs,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        Run a validation operator to validate data assets and to perform the business logic around\n        validation that the operator implements.\n\n        Args:\n            validation_operator_name: name of the operator, as appears in the context's config file\n            assets_to_validate: a list that specifies the data assets that the operator will validate. The members of\n                the list can be either batches, or a tuple that will allow the operator to fetch the batch:\n                (batch_kwargs, expectation_suite_name)\n            evaluation_parameters: $parameter_name syntax references to be evaluated at runtime\n            run_id: The run_id for the validation; if None, a default value will be used\n            run_name: The run_name for the validation; if None, a default value will be used\n            run_time: The date/time of the run\n            result_format: one of several supported formatting directives for expectation validation results\n            **kwargs: Additional kwargs to pass to the validation operator\n\n        Returns:\n            ValidationOperatorResult\n        "
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
        if (run_id is None) and (run_name is None):
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

    def _get_data_context_version(self, arg1: Any, **kwargs) -> Optional[str]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        '\n        arg1: the first positional argument (can take on various types)\n\n        **kwargs: variable arguments\n\n        First check:\n        Returns "v3" if the "0.13" entities are specified in the **kwargs.\n\n        Otherwise:\n        Returns None if no datasources have been configured (or if there is an exception while getting the datasource).\n        Returns "v3" if the datasource is a subclass of the BaseDatasource class.\n        Returns "v2" if the datasource is an instance of the LegacyDatasource class.\n        '
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
            datasource: Union[(LegacyDatasource, BaseDatasource)] = self.get_datasource(
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
                        datasource: Union[
                            (LegacyDatasource, BaseDatasource)
                        ] = self.get_datasource(datasource_name=datasource_name)
                        if isinstance(datasource, LegacyDatasource):
                            api_version = "v2"
                    except (ValueError, TypeError):
                        pass
        return api_version

    def get_batch(
        self, arg1: Any = None, arg2: Any = None, arg3: Any = None, **kwargs
    ) -> Union[(Batch, DataAsset)]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        'Get exactly one batch, based on a variety of flexible input types.\n        The method `get_batch` is the main user-facing method for getting batches; it supports both the new (V3) and the\n        Legacy (V2) Datasource schemas.  The version-specific implementations are contained in "_get_batch_v2()" and\n        "_get_batch_v3()", respectively, both of which are in the present module.\n\n        For the V3 API parameters, please refer to the signature and parameter description of method "_get_batch_v3()".\n        For the Legacy usage, please refer to the signature and parameter description of the method "_get_batch_v2()".\n\n        Args:\n            arg1: the first positional argument (can take on various types)\n            arg2: the second positional argument (can take on various types)\n            arg3: the third positional argument (can take on various types)\n\n            **kwargs: variable arguments\n\n        Returns:\n            Batch (V3) or DataAsset (V2) -- the requested batch\n\n        Processing Steps:\n        1. Determine the version (possible values are "v3" or "v2").\n        2. Convert the positional arguments to the appropriate named arguments, based on the version.\n        3. Package the remaining arguments as variable keyword arguments (applies only to V3).\n        4. Call the version-specific method ("_get_batch_v3()" or "_get_batch_v2()") with the appropriate arguments.\n        '
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

    @usage_statistics_enabled_method(
        event_name=UsageStatsEvents.DATA_CONTEXT_GET_BATCH_LIST.value,
        args_payload_fn=get_batch_list_usage_statistics,
    )
    def get_batch_list(
        self,
        datasource_name: Optional[str] = None,
        data_connector_name: Optional[str] = None,
        data_asset_name: Optional[str] = None,
        *,
        batch_request: Optional[BatchRequestBase] = None,
        batch_data: Optional[Any] = None,
        data_connector_query: Optional[dict] = None,
        batch_identifiers: Optional[dict] = None,
        limit: Optional[int] = None,
        index: Optional[Union[(int, list, tuple, slice, str)]] = None,
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
        **kwargs,
    ) -> List[Batch]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Get the list of zero or more batches, based on a variety of flexible input types.\n        This method applies only to the new (V3) Datasource schema.\n\n        Args:\n            batch_request\n\n            datasource_name\n            data_connector_name\n            data_asset_name\n\n            batch_request\n            batch_data\n            query\n            path\n            runtime_parameters\n            data_connector_query\n            batch_identifiers\n            batch_filter_parameters\n\n            limit\n            index\n            custom_filter_function\n\n            sampling_method\n            sampling_kwargs\n\n            splitter_method\n            splitter_kwargs\n\n            batch_spec_passthrough\n\n            **kwargs\n\n        Returns:\n            (Batch) The requested batch\n\n        `get_batch` is the main user-facing API for getting batches.\n        In contrast to virtually all other methods in the class, it does not require typed or nested inputs.\n        Instead, this method is intended to help the user pick the right parameters\n\n        This method attempts to return any number of batches, including an empty list.\n        "
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
                "The given datasource could not be retrieved from the DataContext; please confirm that your configuration is accurate.",
            )
        return datasource.get_batch_list_from_batch_request(batch_request=batch_request)

    def get_validator(
        self,
        datasource_name: Optional[str] = None,
        data_connector_name: Optional[str] = None,
        data_asset_name: Optional[str] = None,
        *,
        batch: Optional[Batch] = None,
        batch_list: Optional[List[Batch]] = None,
        batch_request: Optional[BatchRequestBase] = None,
        batch_request_list: List[Optional[BatchRequestBase]] = None,
        batch_data: Optional[Any] = None,
        data_connector_query: Optional[Union[(IDDict, dict)]] = None,
        batch_identifiers: Optional[dict] = None,
        limit: Optional[int] = None,
        index: Optional[Union[(int, list, tuple, slice, str)]] = None,
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
        **kwargs,
    ) -> Validator:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        This method applies only to the new (V3) Datasource schema.\n        "
        if (
            sum(
                bool(x)
                for x in [
                    (expectation_suite is not None),
                    (expectation_suite_name is not None),
                    (create_expectation_suite_with_name is not None),
                    (expectation_suite_ge_cloud_id is not None),
                ]
            )
            > 1
        ):
            raise ValueError(
                f"No more than one of expectation_suite_name,{('expectation_suite_ge_cloud_id,' if self.ge_cloud_mode else '')} expectation_suite, or create_expectation_suite_with_name can be specified"
            )
        if expectation_suite_ge_cloud_id is not None:
            expectation_suite = self.get_expectation_suite(
                ge_cloud_id=expectation_suite_ge_cloud_id
            )
        if expectation_suite_name is not None:
            expectation_suite = self.get_expectation_suite(expectation_suite_name)
        if create_expectation_suite_with_name is not None:
            expectation_suite = self.create_expectation_suite(
                expectation_suite_name=create_expectation_suite_with_name
            )
        if (
            sum(
                bool(x)
                for x in [
                    (batch is not None),
                    (batch_list is not None),
                    (batch_request is not None),
                    (batch_request_list is not None),
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
            batch_list: List = [batch]
        else:
            batch_list: List = []
            if not batch_request_list:
                batch_request_list = [batch_request]
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
            expectation_suite=expectation_suite, batch_list=batch_list
        )

    def get_validator_using_batch_list(
        self, expectation_suite: ExpectationSuite, batch_list: List[Batch]
    ) -> Validator:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        batch_definition: BatchDefinition = batch_list[(-1)].batch_definition
        execution_engine: ExecutionEngine = self.datasources[
            batch_definition.datasource_name
        ].execution_engine
        validator: Validator = Validator(
            execution_engine=execution_engine,
            interactive_evaluation=True,
            expectation_suite=expectation_suite,
            data_context=self,
            batches=batch_list,
        )
        return validator

    def list_validation_operator_names(self):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if not self.validation_operators:
            return []
        return list(self.validation_operators.keys())

    @usage_statistics_enabled_method(
        event_name=UsageStatsEvents.DATA_CONTEXT_ADD_DATASOURCE.value,
        args_payload_fn=add_datasource_usage_statistics,
    )
    def add_datasource(
        self, name, initialize=True, **kwargs
    ) -> Optional[Dict[(str, Union[(LegacyDatasource, BaseDatasource)])]]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Add a new datasource to the data context, with configuration provided as kwargs.\n        Args:\n            name: the name for the new datasource to add\n            initialize: if False, add the datasource to the config, but do not\n                initialize it, for example if a user needs to debug database connectivity.\n            kwargs (keyword arguments): the configuration for the new datasource\n\n        Returns:\n            datasource (Datasource)\n        "
        logger.debug(f"Starting BaseDataContext.add_datasource for {name}")
        module_name = kwargs.get("module_name", "great_expectations.datasource")
        verify_dynamic_loading_support(module_name=module_name)
        class_name = kwargs.get("class_name")
        datasource_class = load_class(module_name=module_name, class_name=class_name)
        config: Union[(CommentedMap, dict)]
        if hasattr(datasource_class, "build_configuration"):
            config = datasource_class.build_configuration(**kwargs)
        else:
            config = kwargs
        return self._instantiate_datasource_from_config_and_update_project_config(
            name=name, config=config, initialize=initialize
        )

    def _instantiate_datasource_from_config_and_update_project_config(
        self, name: str, config: dict, initialize: bool = True
    ) -> Optional[Union[(LegacyDatasource, BaseDatasource)]]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        datasource_config: DatasourceConfig = datasourceConfigSchema.load(
            CommentedMap(**config)
        )
        self.config["datasources"][name] = datasource_config
        datasource_config = self.project_config_with_variables_substituted.datasources[
            name
        ]
        config = dict(datasourceConfigSchema.dump(datasource_config))
        datasource: Optional[Union[(LegacyDatasource, BaseDatasource)]]
        if initialize:
            try:
                datasource = self._instantiate_datasource_from_config(
                    name=name, config=config
                )
                self._cached_datasources[name] = datasource
            except ge_exceptions.DatasourceInitializationError as e:
                del self.config["datasources"][name]
                raise e
        else:
            datasource = None
        return datasource

    def _instantiate_datasource_from_config(
        self, name: str, config: dict
    ) -> Union[(LegacyDatasource, BaseDatasource)]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Instantiate a new datasource to the data context, with configuration provided as kwargs.\n        Args:\n            name(str): name of datasource\n            config(dict): dictionary of configuration\n\n        Returns:\n            datasource (Datasource)\n        "
        try:
            datasource: Union[
                (LegacyDatasource, BaseDatasource)
            ] = self._build_datasource_from_config(name=name, config=config)
        except Exception as e:
            raise ge_exceptions.DatasourceInitializationError(
                datasource_name=name, message=str(e)
            )
        return datasource

    def add_batch_kwargs_generator(
        self, datasource_name, batch_kwargs_generator_name, class_name, **kwargs
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        Add a batch kwargs generator to the named datasource, using the provided\n        configuration.\n\n        Args:\n            datasource_name: name of datasource to which to add the new batch kwargs generator\n            batch_kwargs_generator_name: name of the generator to add\n            class_name: class of the batch kwargs generator to add\n            **kwargs: batch kwargs generator configuration, provided as kwargs\n\n        Returns:\n\n        "
        datasource_obj = self.get_datasource(datasource_name)
        generator = datasource_obj.add_batch_kwargs_generator(
            name=batch_kwargs_generator_name, class_name=class_name, **kwargs
        )
        return generator

    def set_config(self, project_config: DataContextConfig) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        self._project_config = project_config

    def _build_datasource_from_config(
        self, name: str, config: Union[(dict, DatasourceConfig)]
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if isinstance(config, DatasourceConfig):
            config = datasourceConfigSchema.dump(config)
        config.update({"name": name})
        if config["class_name"] in ["BaseDatasource", "Datasource"]:
            config.update({"data_context_root_directory": self.root_directory})
        module_name = "great_expectations.datasource"
        datasource = instantiate_class_from_config(
            config=config,
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

    def get_datasource(
        self, datasource_name: str = "default"
    ) -> Optional[Union[(LegacyDatasource, BaseDatasource)]]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Get the named datasource\n\n        Args:\n            datasource_name (str): the name of the datasource from the configuration\n\n        Returns:\n            datasource (Datasource)\n        "
        if datasource_name in self._cached_datasources:
            return self._cached_datasources[datasource_name]
        if (
            datasource_name
            in self.project_config_with_variables_substituted.datasources
        ):
            datasource_config: DatasourceConfig = copy.deepcopy(
                self.project_config_with_variables_substituted.datasources[
                    datasource_name
                ]
            )
        else:
            raise ValueError(
                f"Unable to load datasource `{datasource_name}` -- no configuration found or invalid configuration."
            )
        config: dict = dict(datasourceConfigSchema.dump(datasource_config))
        datasource: Optional[
            Union[(LegacyDatasource, BaseDatasource)]
        ] = self._instantiate_datasource_from_config(
            name=datasource_name, config=config
        )
        self._cached_datasources[datasource_name] = datasource
        return datasource

    def list_expectation_suites(self):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Return a list of available expectation suite keys."
        try:
            keys = self.expectations_store.list_keys()
        except KeyError as e:
            raise ge_exceptions.InvalidConfigError(
                f"Unable to find configured store: {str(e)}"
            )
        return keys

    def list_datasources(self):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        'List currently-configured datasources on this context. Masks passwords.\n\n        Returns:\n            List(dict): each dictionary includes "name", "class_name", and "module_name" keys\n        '
        datasources = []
        for (
            name,
            value,
        ) in self.project_config_with_variables_substituted.datasources.items():
            datasource_config = copy.deepcopy(value)
            datasource_config["name"] = name
            masked_config = PasswordMasker.sanitize_config(datasource_config)
            datasources.append(masked_config)
        return datasources

    def list_stores(self):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "List currently-configured Stores on this context"
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

    def list_active_stores(self):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        List active Stores on this context. Active stores are identified by setting the following parameters:\n            expectations_store_name,\n            validations_store_name,\n            evaluation_parameter_store_name,\n            checkpoint_store_name\n            profiler_store_name\n        "
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
            store
            for store in self.list_stores()
            if (store["name"] in active_store_names)
        ]

    def list_validation_operators(self):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "List currently-configured Validation Operators on this context"
        validation_operators = []
        for (
            name,
            value,
        ) in (
            self.project_config_with_variables_substituted.validation_operators.items()
        ):
            value["name"] = name
            validation_operators.append(value)
        return validation_operators

    def send_usage_message(
        self, event: str, event_payload: Optional[dict], success: Optional[bool] = None
    ) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "helper method to send a usage method using DataContext. Used when sending usage events from\n            classes like ExpectationSuite.\n            event\n        Args:\n            event (str): str representation of event\n            event_payload (dict): optional event payload\n            success (bool): optional success param\n        Returns:\n            None\n        "
        send_usage_message(self, event, event_payload, success)

    def create_expectation_suite(
        self,
        expectation_suite_name: str,
        overwrite_existing: bool = False,
        ge_cloud_id: Optional[str] = None,
        **kwargs,
    ) -> ExpectationSuite:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Build a new expectation suite and save it into the data_context expectation store.\n\n        Args:\n            expectation_suite_name: The name of the expectation_suite to create\n            overwrite_existing (boolean): Whether to overwrite expectation suite if expectation suite with given name\n                already exists.\n\n        Returns:\n            A new (empty) expectation suite.\n        "
        if not isinstance(overwrite_existing, bool):
            raise ValueError("Parameter overwrite_existing must be of type BOOL")
        expectation_suite: ExpectationSuite = ExpectationSuite(
            expectation_suite_name=expectation_suite_name, data_context=self
        )
        if self.ge_cloud_mode:
            key: GeCloudIdentifier = GeCloudIdentifier(
                resource_type="expectation_suite", ge_cloud_id=ge_cloud_id
            )
            if self.expectations_store.has_key(key) and (not overwrite_existing):
                raise ge_exceptions.DataContextError(
                    "expectation_suite with GE Cloud ID {} already exists. If you would like to overwrite this expectation_suite, set overwrite_existing=True.".format(
                        ge_cloud_id
                    )
                )
        else:
            key: ExpectationSuiteIdentifier = ExpectationSuiteIdentifier(
                expectation_suite_name=expectation_suite_name
            )
            if self.expectations_store.has_key(key) and (not overwrite_existing):
                raise ge_exceptions.DataContextError(
                    "expectation_suite with name {} already exists. If you would like to overwrite this expectation_suite, set overwrite_existing=True.".format(
                        expectation_suite_name
                    )
                )
        self.expectations_store.set(key, expectation_suite, **kwargs)
        return expectation_suite

    def delete_expectation_suite(
        self,
        expectation_suite_name: Optional[str] = None,
        ge_cloud_id: Optional[str] = None,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Delete specified expectation suite from data_context expectation store.\n\n        Args:\n            expectation_suite_name: The name of the expectation_suite to create\n\n        Returns:\n            True for Success and False for Failure.\n        "
        if self.ge_cloud_mode:
            key: GeCloudIdentifier = GeCloudIdentifier(
                resource_type="expectation_suite", ge_cloud_id=ge_cloud_id
            )
        else:
            key: ExpectationSuiteIdentifier = ExpectationSuiteIdentifier(
                expectation_suite_name
            )
        if not self.expectations_store.has_key(key):
            raise ge_exceptions.DataContextError(
                "expectation_suite with name {} does not exist."
            )
        else:
            self.expectations_store.remove_key(key)
            return True

    def get_expectation_suite(
        self,
        expectation_suite_name: Optional[str] = None,
        ge_cloud_id: Optional[str] = None,
    ) -> ExpectationSuite:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Get an Expectation Suite by name or GE Cloud ID\n        Args:\n            expectation_suite_name (str): the name for the Expectation Suite\n            ge_cloud_id (str): the GE Cloud ID for the Expectation Suite\n\n        Returns:\n            expectation_suite\n        "
        if self.ge_cloud_mode:
            key: GeCloudIdentifier = GeCloudIdentifier(
                resource_type="expectation_suite", ge_cloud_id=ge_cloud_id
            )
        else:
            key: Optional[ExpectationSuiteIdentifier] = ExpectationSuiteIdentifier(
                expectation_suite_name=expectation_suite_name
            )
        if self.expectations_store.has_key(key):
            expectations_schema_dict: dict = cast(
                dict, self.expectations_store.get(key)
            )
            return ExpectationSuite(**expectations_schema_dict, data_context=self)
        else:
            raise ge_exceptions.DataContextError(
                f"expectation_suite {expectation_suite_name} not found"
            )

    def list_expectation_suite_names(self) -> List[str]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        Lists the available expectation suite names. If in ge_cloud_mode, a list of\n        GE Cloud ids is returned instead.\n        "
        if self.ge_cloud_mode:
            return [
                suite_key.ge_cloud_id for suite_key in self.list_expectation_suites()
            ]
        sorted_expectation_suite_names = [
            i.expectation_suite_name for i in self.list_expectation_suites()
        ]
        sorted_expectation_suite_names.sort()
        return sorted_expectation_suite_names

    @usage_statistics_enabled_method(
        event_name=UsageStatsEvents.DATA_CONTEXT_SAVE_EXPECTATION_SUITE.value,
        args_payload_fn=save_expectation_suite_usage_statistics,
    )
    def save_expectation_suite(
        self,
        expectation_suite: ExpectationSuite,
        expectation_suite_name: Optional[str] = None,
        overwrite_existing: bool = True,
        ge_cloud_id: Optional[str] = None,
        **kwargs,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Save the provided expectation suite into the DataContext.\n\n        Args:\n            expectation_suite: the suite to save\n            expectation_suite_name: the name of this expectation suite. If no name is provided the name will                 be read from the suite\n\n        Returns:\n            None\n        "
        if self.ge_cloud_mode:
            key: GeCloudIdentifier = GeCloudIdentifier(
                resource_type="expectation_suite",
                ge_cloud_id=(
                    ge_cloud_id
                    if (ge_cloud_id is not None)
                    else str(expectation_suite.ge_cloud_id)
                ),
            )
            if self.expectations_store.has_key(key) and (not overwrite_existing):
                raise ge_exceptions.DataContextError(
                    "expectation_suite with GE Cloud ID {} already exists. If you would like to overwrite this expectation_suite, set overwrite_existing=True.".format(
                        ge_cloud_id
                    )
                )
        else:
            if expectation_suite_name is None:
                key: ExpectationSuiteIdentifier = ExpectationSuiteIdentifier(
                    expectation_suite_name=expectation_suite.expectation_suite_name
                )
            else:
                expectation_suite.expectation_suite_name = expectation_suite_name
                key: ExpectationSuiteIdentifier = ExpectationSuiteIdentifier(
                    expectation_suite_name=expectation_suite_name
                )
            if self.expectations_store.has_key(key) and (not overwrite_existing):
                raise ge_exceptions.DataContextError(
                    "expectation_suite with name {} already exists. If you would like to overwrite this expectation_suite, set overwrite_existing=True.".format(
                        expectation_suite_name
                    )
                )
        self._evaluation_parameter_dependencies_compiled = False
        return self.expectations_store.set(key, expectation_suite, **kwargs)

    def _store_metrics(
        self, requested_metrics, validation_results, target_store_name
    ) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        requested_metrics is a dictionary like this:\n\n              requested_metrics:\n                *:  # The asterisk here matches *any* expectation suite name\n                  # use the 'kwargs' key to request metrics that are defined by kwargs,\n                  # for example because they are defined only for a particular column\n                  # - column:\n                  #     Age:\n                  #        - expect_column_min_to_be_between.result.observed_value\n                    - statistics.evaluated_expectations\n                    - statistics.successful_expectations\n\n        Args:\n            requested_metrics:\n            validation_results:\n            target_store_name:\n\n        Returns:\n\n        "
        expectation_suite_name = validation_results.meta["expectation_suite_name"]
        run_id = validation_results.meta["run_id"]
        data_asset_name = validation_results.meta.get("batch_kwargs", {}).get(
            "data_asset_name"
        )
        for (expectation_suite_dependency, metrics_list) in requested_metrics.items():
            if (expectation_suite_dependency != "*") and (
                expectation_suite_dependency != expectation_suite_name
            ):
                continue
            if not isinstance(metrics_list, list):
                raise ge_exceptions.DataContextError(
                    "Invalid requested_metrics configuration: metrics requested for each expectation suite must be a list."
                )
            for metric_configuration in metrics_list:
                metric_configurations = (
                    BaseDataContext._get_metric_configuration_tuples(
                        metric_configuration
                    )
                )
                for (metric_name, metric_kwargs) in metric_configurations:
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
                        logger.debug(
                            "metric {} was requested by another expectation suite but is not available in this validation result.".format(
                                metric_name
                            )
                        )

    def store_validation_result_metrics(
        self, requested_metrics, validation_results, target_store_name
    ) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        self._store_metrics(requested_metrics, validation_results, target_store_name)

    def store_evaluation_parameters(
        self, validation_results, target_store_name=None
    ) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if not self._evaluation_parameter_dependencies_compiled:
            self._compile_evaluation_parameter_dependencies()
        if target_store_name is None:
            target_store_name = self.evaluation_parameter_store_name
        self._store_metrics(
            self._evaluation_parameter_dependencies,
            validation_results,
            target_store_name,
        )

    @property
    def evaluation_parameter_store(self):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return self.stores[self.evaluation_parameter_store_name]

    @property
    def evaluation_parameter_store_name(self):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return (
            self.project_config_with_variables_substituted.evaluation_parameter_store_name
        )

    @property
    def validations_store_name(self):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return self.project_config_with_variables_substituted.validations_store_name

    @property
    def validations_store(self) -> ValidationsStore:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return self.stores[self.validations_store_name]

    @property
    def assistants(self) -> DataAssistantDispatcher:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return self._assistants

    def _compile_evaluation_parameter_dependencies(self) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        self._evaluation_parameter_dependencies = {}
        for key in self.expectations_store.list_keys():
            expectation_suite_dict: dict = cast(dict, self.expectations_store.get(key))
            if not expectation_suite_dict:
                continue
            expectation_suite: ExpectationSuite = ExpectationSuite(
                **expectation_suite_dict, data_context=self
            )
            dependencies = expectation_suite.get_evaluation_parameter_dependencies()
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
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        'Get validation results from a configured store.\n\n        Args:\n            expectation_suite_name: expectation_suite name for which to get validation result (default: "default")\n            run_id: run_id for which to get validation result (if None, fetch the latest result by alphanumeric sort)\n            validations_store_name: the name of the store from which to get validation results\n            failed_only: if True, filter the result to return only failed expectations\n\n        Returns:\n            validation_result\n\n        '
        if validations_store_name is None:
            validations_store_name = self.validations_store_name
        selected_store = self.stores[validations_store_name]
        if (run_id is None) or (batch_identifier is None):
            key_list = selected_store.list_keys()
            filtered_key_list = []
            for key in key_list:
                if (run_id is not None) and (key.run_id != run_id):
                    continue
                if (batch_identifier is not None) and (
                    key.batch_identifier != batch_identifier
                ):
                    continue
                filtered_key_list.append(key)
            if len(filtered_key_list) == 0:
                logger.warning("No valid run_id values found.")
                return {}
            filtered_key_list = sorted(filtered_key_list, key=(lambda x: x.run_id))
            if run_id is None:
                run_id = filtered_key_list[(-1)].run_id
            if batch_identifier is None:
                batch_identifier = filtered_key_list[(-1)].batch_identifier
        key = ValidationResultIdentifier(
            expectation_suite_identifier=ExpectationSuiteIdentifier(
                expectation_suite_name=expectation_suite_name
            ),
            run_id=run_id,
            batch_identifier=batch_identifier,
        )
        results_dict = selected_store.get(key)
        return (
            results_dict.get_failed_validation_results()
            if failed_only
            else results_dict
        )

    def update_return_obj(self, data_asset, return_obj):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Helper called by data_asset.\n\n        Args:\n            data_asset: The data_asset whose validation produced the current return object\n            return_obj: the return object to update\n\n        Returns:\n            return_obj: the return object, potentially changed into a widget by the configured expectation explorer\n        "
        return return_obj

    @usage_statistics_enabled_method(
        event_name=UsageStatsEvents.DATA_CONTEXT_BUILD_DATA_DOCS.value
    )
    def build_data_docs(
        self,
        site_names=None,
        resource_identifiers=None,
        dry_run=False,
        build_index: bool = True,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        Build Data Docs for your project.\n\n        These make it simple to visualize data quality in your project. These\n        include Expectations, Validations & Profiles. The are built for all\n        Datasources from JSON artifacts in the local repo including validations\n        & profiles from the uncommitted directory.\n\n        :param site_names: if specified, build data docs only for these sites, otherwise,\n                            build all the sites specified in the context's config\n        :param resource_identifiers: a list of resource identifiers (ExpectationSuiteIdentifier,\n                            ValidationResultIdentifier). If specified, rebuild HTML\n                            (or other views the data docs sites are rendering) only for\n                            the resources in this list. This supports incremental build\n                            of data docs sites (e.g., when a new validation result is created)\n                            and avoids full rebuild.\n        :param dry_run: a flag, if True, the method returns a structure containing the\n                            URLs of the sites that *would* be built, but it does not build\n                            these sites. The motivation for adding this flag was to allow\n                            the CLI to display the the URLs before building and to let users\n                            confirm.\n\n        :param build_index: a flag if False, skips building the index page\n\n        Returns:\n            A dictionary with the names of the updated data documentation sites as keys and the the location info\n            of their index.html files as values\n        "
        logger.debug("Starting DataContext.build_data_docs")
        index_page_locator_infos = {}
        sites = self.project_config_with_variables_substituted.data_docs_sites
        if sites:
            logger.debug("Found data_docs_sites. Building sites...")
            for (site_name, site_config) in sites.items():
                logger.debug(f"Building Data Docs Site {site_name}")
                if (site_names and (site_name in site_names)) or (not site_names):
                    complete_site_config = site_config
                    module_name = "great_expectations.render.renderer.site_builder"
                    site_builder: SiteBuilder = instantiate_class_from_config(
                        config=complete_site_config,
                        runtime_environment={
                            "data_context": self,
                            "root_directory": self.root_directory,
                            "site_name": site_name,
                            "ge_cloud_mode": self.ge_cloud_mode,
                        },
                        config_defaults={"module_name": module_name},
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
                            build_index=(build_index and (not self.ge_cloud_mode)),
                        )
                        if index_page_resource_identifier_tuple:
                            index_page_locator_infos[
                                site_name
                            ] = index_page_resource_identifier_tuple[0]
        else:
            logger.debug("No data_docs_config found. No site(s) built.")
        return index_page_locator_infos

    def clean_data_docs(self, site_name=None) -> bool:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        Clean a given data docs site.\n\n        This removes all files from the configured Store.\n\n        Args:\n            site_name (str): Optional, the name of the site to clean. If not\n            specified, all sites will be cleaned.\n        "
        data_docs_sites = self.project_config_with_variables_substituted.data_docs_sites
        if not data_docs_sites:
            raise ge_exceptions.DataContextError(
                "No data docs sites were found on this DataContext, therefore no sites will be cleaned."
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
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        sites = self.project_config_with_variables_substituted.data_docs_sites
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

    def profile_datasource(
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
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        'Profile the named datasource using the named profiler.\n\n        Args:\n            datasource_name: the name of the datasource for which to profile data_assets\n            batch_kwargs_generator_name: the name of the batch kwargs generator to use to get batches\n            data_assets: list of data asset names to profile\n            max_data_assets: if the number of data assets the batch kwargs generator yields is greater than this max_data_assets,\n                profile_all_data_assets=True is required to profile all\n            profile_all_data_assets: when True, all data assets are profiled, regardless of their number\n            profiler: the profiler class to use\n            profiler_configuration: Optional profiler configuration dict\n            dry_run: when true, the method checks arguments and reports if can profile or specifies the arguments that are missing\n            additional_batch_kwargs: Additional keyword arguments to be provided to get_batch when loading the data asset.\n        Returns:\n            A dictionary::\n\n                {\n                    "success": True/False,\n                    "results": List of (expectation_suite, EVR) tuples for each of the data_assets found in the datasource\n                }\n\n            When success = False, the error details are under "error" key\n        '
        datasource = self.get_datasource(datasource_name)
        if not dry_run:
            logger.info(f"Profiling '{datasource_name}' with '{profiler.__name__}'")
        profiling_results = {}
        data_asset_names_dict = self.get_available_data_asset_names(datasource_name)
        available_data_asset_name_list = []
        try:
            datasource_data_asset_names_dict = data_asset_names_dict[datasource_name]
        except KeyError:
            raise ge_exceptions.ProfilerError(f"No datasource {datasource_name} found.")
        if batch_kwargs_generator_name is None:
            for name in datasource_data_asset_names_dict.keys():
                if batch_kwargs_generator_name is not None:
                    profiling_results = {
                        "success": False,
                        "error": {
                            "code": BaseDataContext.PROFILING_ERROR_CODE_MULTIPLE_BATCH_KWARGS_GENERATORS_FOUND
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
                        "code": BaseDataContext.PROFILING_ERROR_CODE_NO_BATCH_KWARGS_GENERATORS_FOUND
                    },
                }
                return profiling_results
        else:
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
            available_data_asset_name_list, key=(lambda x: x[0])
        )
        if len(available_data_asset_name_list) == 0:
            raise ge_exceptions.ProfilerError(
                "No Data Assets found in Datasource {}. Used batch kwargs generator: {}.".format(
                    datasource_name, batch_kwargs_generator_name
                )
            )
        total_data_assets = len(available_data_asset_name_list)
        if isinstance(data_assets, list) and (len(data_assets) > 0):
            not_found_data_assets = [
                name
                for name in data_assets
                if (name not in [da[0] for da in available_data_asset_name_list])
            ]
            if len(not_found_data_assets) > 0:
                profiling_results = {
                    "success": False,
                    "error": {
                        "code": BaseDataContext.PROFILING_ERROR_CODE_SPECIFIED_DATA_ASSETS_NOT_FOUND,
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
                    "Profiling the white-listed data assets: %s, alphabetically."
                    % ",".join(data_assets)
                )
        else:
            if not profile_all_data_assets:
                if total_data_assets > max_data_assets:
                    profiling_results = {
                        "success": False,
                        "error": {
                            "code": BaseDataContext.PROFILING_ERROR_CODE_TOO_MANY_DATA_ASSETS,
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
                "Profiling all %d data assets from batch kwargs generator %s"
                % (len(available_data_asset_name_list), batch_kwargs_generator_name)
            )
        else:
            logger.info(
                "Found %d data assets from batch kwargs generator %s"
                % (len(available_data_asset_name_list), batch_kwargs_generator_name)
            )
        profiling_results["success"] = True
        if not dry_run:
            profiling_results["results"] = []
            (total_columns, total_expectations, total_rows, skipped_data_assets) = (
                0,
                0,
                0,
                0,
            )
            total_start_time = datetime.datetime.now()
            for name in data_asset_names_to_profiled:
                logger.info(f"	Profiling '{name}'...")
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
                        "IOError while profiling %s. (Perhaps a loading error?) Skipping."
                        % name[1]
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
                "\n    Profiled %d of %d named data assets, with %d total rows and %d columns in %.2f seconds.\n    Generated, evaluated, and stored %d Expectations during profiling. Please review results using data-docs."
                % (
                    len(data_asset_names_to_profiled),
                    total_data_assets,
                    total_rows,
                    total_columns,
                    total_duration,
                    total_expectations,
                )
            )
            if skipped_data_assets > 0:
                logger.warning(
                    "Skipped %d data assets due to errors." % skipped_data_assets
                )
        profiling_results["success"] = True
        return profiling_results

    def profile_data_asset(
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
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        '\n        Profile a data asset\n\n        :param datasource_name: the name of the datasource to which the profiled data asset belongs\n        :param batch_kwargs_generator_name: the name of the batch kwargs generator to use to get batches (only if batch_kwargs are not provided)\n        :param data_asset_name: the name of the profiled data asset\n        :param batch_kwargs: optional - if set, the method will use the value to fetch the batch to be profiled. If not passed, the batch kwargs generator (generator_name arg) will choose a batch\n        :param profiler: the profiler class to use\n        :param profiler_configuration: Optional profiler configuration dict\n        :param run_name: optional - if set, the validation result created by the profiler will be under the provided run_name\n        :param additional_batch_kwargs:\n        :returns\n            A dictionary::\n\n                {\n                    "success": True/False,\n                    "results": List of (expectation_suite, EVR) tuples for each of the data_assets found in the datasource\n                }\n\n            When success = False, the error details are under "error" key\n        '
        assert (not (run_id and run_name)) and (
            not (run_id and run_time)
        ), "Please provide either a run_id or run_name and/or run_time."
        if isinstance(run_id, str) and (not run_name):
            warnings.warn(
                "String run_ids are deprecated as of v0.11.0 and support will be removed in v0.16. Please provide a run_id of type RunIdentifier(run_name=None, run_time=None), or a dictionary containing run_name and run_time (both optional). Instead of providing a run_id, you may also providerun_name and run_time separately.",
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
        (total_columns, total_expectations, total_rows, skipped_data_assets) = (
            0,
            0,
            0,
            0,
        )
        total_start_time = datetime.datetime.now()
        name = data_asset_name
        start_time = datetime.datetime.now()
        if expectation_suite_name is None:
            if (batch_kwargs_generator_name is None) and (data_asset_name is None):
                expectation_suite_name = (
                    ((datasource_name + ".") + profiler.__name__) + "."
                ) + BatchKwargs(batch_kwargs).to_id()
            else:
                expectation_suite_name = (
                    (
                        (((datasource_name + ".") + batch_kwargs_generator_name) + ".")
                        + data_asset_name
                    )
                    + "."
                ) + profiler.__name__
        self.create_expectation_suite(
            expectation_suite_name=expectation_suite_name, overwrite_existing=True
        )
        batch = self.get_batch(
            expectation_suite_name=expectation_suite_name, batch_kwargs=batch_kwargs
        )
        if not profiler.validate(batch):
            raise ge_exceptions.ProfilerError(
                "batch '%s' is not a valid batch for the '%s' profiler"
                % (name, profiler.__name__)
            )
        (expectation_suite, validation_results) = profiler.profile(
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
        if isinstance(validation_ref, GeCloudIdAwareRef):
            ge_cloud_id = validation_ref.ge_cloud_id
            validation_results.ge_cloud_id = uuid.UUID(ge_cloud_id)
        if isinstance(batch, Dataset):
            row_count = batch.get_row_count()
            total_rows += row_count
            new_column_count = len(
                {
                    exp.kwargs["column"]
                    for exp in expectation_suite.expectations
                    if ("column" in exp.kwargs)
                }
            )
            total_columns += new_column_count
        new_expectation_count = len(expectation_suite.expectations)
        total_expectations += new_expectation_count
        self.save_expectation_suite(expectation_suite)
        duration = (datetime.datetime.now() - start_time).total_seconds()
        logger.info(
            "\tProfiled %d columns using %d rows from %s (%.3f sec)"
            % (new_column_count, row_count, name, duration)
        )
        total_duration = (datetime.datetime.now() - total_start_time).total_seconds()
        logger.info(
            "\nProfiled the data asset, with %d total rows and %d columns in %.2f seconds.\nGenerated, evaluated, and stored %d Expectations during profiling. Please review results using data-docs."
            % (total_rows, total_columns, total_duration, total_expectations)
        )
        profiling_results["success"] = True
        return profiling_results

    def list_checkpoints(self) -> List[str]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return self.checkpoint_store.list_checkpoints(ge_cloud_mode=self.ge_cloud_mode)

    def add_checkpoint(
        self,
        name: str,
        config_version: Optional[Union[(int, float)]] = None,
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
        validation_operator_name: Optional[str] = None,
        batches: Optional[List[dict]] = None,
        site_names: Optional[Union[(str, List[str])]] = None,
        slack_webhook: Optional[str] = None,
        notify_on: Optional[str] = None,
        notify_with: Optional[Union[(str, List[str])]] = None,
        ge_cloud_id: Optional[str] = None,
        expectation_suite_ge_cloud_id: Optional[str] = None,
    ) -> Checkpoint:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        checkpoint: Checkpoint = Checkpoint.construct_from_config_args(
            data_context=self,
            checkpoint_store_name=self.checkpoint_store_name,
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
            validation_operator_name=validation_operator_name,
            batches=batches,
            site_names=site_names,
            slack_webhook=slack_webhook,
            notify_on=notify_on,
            notify_with=notify_with,
            ge_cloud_id=ge_cloud_id,
            expectation_suite_ge_cloud_id=expectation_suite_ge_cloud_id,
        )
        self.checkpoint_store.add_checkpoint(checkpoint, name, ge_cloud_id)
        return checkpoint

    def get_checkpoint(
        self, name: Optional[str] = None, ge_cloud_id: Optional[str] = None
    ) -> Checkpoint:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        checkpoint_config: CheckpointConfig = self.checkpoint_store.get_checkpoint(
            name=name, ge_cloud_id=ge_cloud_id
        )
        checkpoint: Checkpoint = Checkpoint.instantiate_from_config_with_runtime_args(
            checkpoint_config=checkpoint_config, data_context=self, name=name
        )
        return checkpoint

    def delete_checkpoint(
        self, name: Optional[str] = None, ge_cloud_id: Optional[str] = None
    ) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return self.checkpoint_store.delete_checkpoint(
            name=name, ge_cloud_id=ge_cloud_id
        )

    @usage_statistics_enabled_method(
        event_name=UsageStatsEvents.DATA_CONTEXT_RUN_CHECKPOINT.value
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
        run_id: Optional[Union[(str, int, float)]] = None,
        run_name: Optional[str] = None,
        run_time: Optional[datetime.datetime] = None,
        result_format: Optional[str] = None,
        expectation_suite_ge_cloud_id: Optional[str] = None,
        **kwargs,
    ) -> CheckpointResult:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        Validate against a pre-defined Checkpoint. (Experimental)\n        Args:\n            checkpoint_name: The name of a Checkpoint defined via the CLI or by manually creating a yml file\n            template_name: The name of a Checkpoint template to retrieve from the CheckpointStore\n            run_name_template: The template to use for run_name\n            expectation_suite_name: Expectation suite to be used by Checkpoint run\n            batch_request: Batch request to be used by Checkpoint run\n            action_list: List of actions to be performed by the Checkpoint\n            evaluation_parameters: $parameter_name syntax references to be evaluated at runtime\n            runtime_configuration: Runtime configuration override parameters\n            validations: Validations to be performed by the Checkpoint run\n            profilers: Profilers to be used by the Checkpoint run\n            run_id: The run_id for the validation; if None, a default value will be used\n            run_name: The run_name for the validation; if None, a default value will be used\n            run_time: The date/time of the run\n            result_format: One of several supported formatting directives for expectation validation results\n            ge_cloud_id: Great Expectations Cloud id for the checkpoint\n            expectation_suite_ge_cloud_id: Great Expectations Cloud id for the expectation suite\n            **kwargs: Additional kwargs to pass to the validation operator\n\n        Returns:\n            CheckpointResult\n        "
        checkpoint: Checkpoint = self.get_checkpoint(
            name=checkpoint_name, ge_cloud_id=ge_cloud_id
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

    def add_profiler(
        self,
        name: str,
        config_version: float,
        rules: Dict[(str, dict)],
        variables: Optional[dict] = None,
        ge_cloud_id: Optional[str] = None,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        config_data = {
            "name": name,
            "config_version": config_version,
            "rules": rules,
            "variables": variables,
        }
        validated_config: dict = ruleBasedProfilerConfigSchema.load(config_data)
        profiler_config: dict = ruleBasedProfilerConfigSchema.dump(validated_config)
        profiler_config.pop("class_name")
        profiler_config.pop("module_name")
        config: RuleBasedProfilerConfig = RuleBasedProfilerConfig(**profiler_config)
        return RuleBasedProfiler.add_profiler(
            config=config,
            data_context=self,
            profiler_store=self.profiler_store,
            ge_cloud_id=ge_cloud_id,
        )

    def save_profiler(
        self,
        profiler: RuleBasedProfiler,
        name: Optional[str] = None,
        ge_cloud_id: Optional[str] = None,
    ) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        key: Union[
            (GeCloudIdentifier, ConfigurationIdentifier)
        ] = self.profiler_store.determine_key(name=name, ge_cloud_id=ge_cloud_id)
        self.profiler_store.set(key=key, value=profiler.config)

    def get_profiler(
        self, name: Optional[str] = None, ge_cloud_id: Optional[str] = None
    ) -> RuleBasedProfiler:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return RuleBasedProfiler.get_profiler(
            data_context=self,
            profiler_store=self.profiler_store,
            name=name,
            ge_cloud_id=ge_cloud_id,
        )

    def delete_profiler(
        self, name: Optional[str] = None, ge_cloud_id: Optional[str] = None
    ) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        RuleBasedProfiler.delete_profiler(
            profiler_store=self.profiler_store, name=name, ge_cloud_id=ge_cloud_id
        )

    def list_profilers(self) -> List[str]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if self.profiler_store is None:
            raise ge_exceptions.StoreConfigurationError(
                "Attempted to list profilers from a Profiler Store, which is not a configured store."
            )
        return RuleBasedProfiler.list_profilers(
            profiler_store=self.profiler_store, ge_cloud_mode=self.ge_cloud_mode
        )

    @usage_statistics_enabled_method(
        event_name=UsageStatsEvents.DATA_CONTEXT_RUN_PROFILER_WITH_DYNAMIC_ARGUMENTS.value
    )
    def run_profiler_with_dynamic_arguments(
        self,
        name: Optional[str] = None,
        ge_cloud_id: Optional[str] = None,
        variables: Optional[dict] = None,
        rules: Optional[dict] = None,
    ) -> RuleBasedProfilerResult:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Retrieve a RuleBasedProfiler from a ProfilerStore and run it with rules/variables supplied at runtime.\n\n        Args:\n            name: Identifier used to retrieve the profiler from a store.\n            ge_cloud_id: Identifier used to retrieve the profiler from a store (GE Cloud specific).\n            variables: Attribute name/value pairs (overrides)\n            rules: Key-value pairs of name/configuration-dictionary (overrides)\n\n        Returns:\n            Set of rule evaluation results in the form of an RuleBasedProfilerResult\n\n        Raises:\n            AssertionError if both a `name` and `ge_cloud_id` are provided.\n            AssertionError if both an `expectation_suite` and `expectation_suite_name` are provided.\n        "
        return RuleBasedProfiler.run_profiler(
            data_context=self,
            profiler_store=self.profiler_store,
            name=name,
            ge_cloud_id=ge_cloud_id,
            variables=variables,
            rules=rules,
        )

    @usage_statistics_enabled_method(
        event_name=UsageStatsEvents.DATA_CONTEXT_RUN_PROFILER_ON_DATA.value
    )
    def run_profiler_on_data(
        self,
        batch_list: Optional[List[Batch]] = None,
        batch_request: Optional[BatchRequestBase] = None,
        name: Optional[str] = None,
        ge_cloud_id: Optional[str] = None,
    ) -> RuleBasedProfilerResult:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        'Retrieve a RuleBasedProfiler from a ProfilerStore and run it with a batch request supplied at runtime.\n\n        Args:\n            batch_list: Explicit list of Batch objects to supply data at runtime.\n            batch_request: Explicit batch_request used to supply data at runtime.\n            name: Identifier used to retrieve the profiler from a store.\n            ge_cloud_id: Identifier used to retrieve the profiler from a store (GE Cloud specific).\n\n        Returns:\n            Set of rule evaluation results in the form of an RuleBasedProfilerResult\n\n        Raises:\n            ProfilerConfigurationError is both "batch_list" and "batch_request" arguments are specified.\n            AssertionError if both a `name` and `ge_cloud_id` are provided.\n            AssertionError if both an `expectation_suite` and `expectation_suite_name` are provided.\n        '
        return RuleBasedProfiler.run_profiler_on_data(
            data_context=self,
            profiler_store=self.profiler_store,
            batch_list=batch_list,
            batch_request=batch_request,
            name=name,
            ge_cloud_id=ge_cloud_id,
        )

    def test_yaml_config(
        self,
        yaml_config: str,
        name: Optional[str] = None,
        class_name: Optional[str] = None,
        runtime_environment: Optional[dict] = None,
        pretty_print: bool = True,
        return_mode: Union[
            (Literal["instantiated_class"], Literal["report_object"])
        ] = "instantiated_class",
        shorten_tracebacks: bool = False,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        'Convenience method for testing yaml configs\n\n        test_yaml_config is a convenience method for configuring the moving\n        parts of a Great Expectations deployment. It allows you to quickly\n        test out configs for system components, especially Datasources,\n        Checkpoints, and Stores.\n\n        For many deployments of Great Expectations, these components (plus\n        Expectations) are the only ones you\'ll need.\n\n        test_yaml_config is mainly intended for use within notebooks and tests.\n\n        Parameters\n        ----------\n        yaml_config : str\n            A string containing the yaml config to be tested\n\n        name: str\n            (Optional) A string containing the name of the component to instantiate\n\n        pretty_print : bool\n            Determines whether to print human-readable output\n\n        return_mode : str\n            Determines what type of object test_yaml_config will return\n            Valid modes are "instantiated_class" and "report_object"\n\n        shorten_tracebacks : bool\n            If true, catch any errors during instantiation and print only the\n            last element of the traceback stack. This can be helpful for\n            rapid iteration on configs in a notebook, because it can remove\n            the need to scroll up and down a lot.\n\n        Returns\n        -------\n        The instantiated component (e.g. a Datasource)\n        OR\n        a json object containing metadata from the component\'s self_check method\n\n        The returned object is determined by return_mode.\n        '
        if return_mode not in ["instantiated_class", "report_object"]:
            raise ValueError(f"Unknown return_mode: {return_mode}.")
        if runtime_environment is None:
            runtime_environment = {}
        runtime_environment = {**runtime_environment, **self.runtime_environment}
        usage_stats_event_name: str = "data_context.test_yaml_config"
        config = self._test_yaml_config_prepare_config(
            yaml_config, runtime_environment, usage_stats_event_name
        )
        if "class_name" in config:
            class_name = config["class_name"]
        instantiated_class: Any = None
        usage_stats_event_payload: Dict[(str, Union[(str, List[str])])] = {}
        if pretty_print:
            print("Attempting to instantiate class from config...")
        try:
            if class_name in self.TEST_YAML_CONFIG_SUPPORTED_STORE_TYPES:
                (
                    instantiated_class,
                    usage_stats_event_payload,
                ) = self._test_instantiation_of_store_from_yaml_config(
                    name, class_name, config
                )
            elif class_name in self.TEST_YAML_CONFIG_SUPPORTED_DATASOURCE_TYPES:
                (
                    instantiated_class,
                    usage_stats_event_payload,
                ) = self._test_instantiation_of_datasource_from_yaml_config(
                    name, class_name, config
                )
            elif class_name in self.TEST_YAML_CONFIG_SUPPORTED_CHECKPOINT_TYPES:
                (
                    instantiated_class,
                    usage_stats_event_payload,
                ) = self._test_instantiation_of_checkpoint_from_yaml_config(
                    name, class_name, config
                )
            elif class_name in self.TEST_YAML_CONFIG_SUPPORTED_DATA_CONNECTOR_TYPES:
                (
                    instantiated_class,
                    usage_stats_event_payload,
                ) = self._test_instantiation_of_data_connector_from_yaml_config(
                    name, class_name, config, runtime_environment
                )
            elif class_name in self.TEST_YAML_CONFIG_SUPPORTED_PROFILER_TYPES:
                (
                    instantiated_class,
                    usage_stats_event_payload,
                ) = self._test_instantiation_of_profiler_from_yaml_config(
                    name, class_name, config
                )
            else:
                (
                    instantiated_class,
                    usage_stats_event_payload,
                ) = self._test_instantiation_of_misc_class_from_yaml_config(
                    name, config, runtime_environment, usage_stats_event_payload
                )
            send_usage_message(
                data_context=self,
                event=usage_stats_event_name,
                event_payload=usage_stats_event_payload,
                success=True,
            )
            if pretty_print:
                print(
                    f"""	Successfully instantiated {instantiated_class.__class__.__name__}
"""
                )
            report_object: dict = instantiated_class.self_check(
                pretty_print=pretty_print
            )
            if return_mode == "instantiated_class":
                return instantiated_class
            return report_object
        except Exception as e:
            if class_name is None:
                usage_stats_event_payload[
                    "diagnostic_info"
                ] = usage_stats_event_payload.get("diagnostic_info", []) + [
                    "__class_name_not_provided__"
                ]
            elif (usage_stats_event_payload.get("parent_class") is None) and (
                class_name in self.ALL_TEST_YAML_CONFIG_SUPPORTED_TYPES
            ):
                usage_stats_event_payload["parent_class"] = class_name
            send_usage_message(
                data_context=self,
                event=usage_stats_event_name,
                event_payload=usage_stats_event_payload,
                success=False,
            )
            if shorten_tracebacks:
                traceback.print_exc(limit=1)
            else:
                raise e

    def _test_yaml_config_prepare_config(
        self, yaml_config: str, runtime_environment: dict, usage_stats_event_name: str
    ) -> CommentedMap:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        Performs variable substitution and conversion from YAML to CommentedMap.\n        See `test_yaml_config` for more details.\n        "
        try:
            substituted_config_variables: Union[
                (DataContextConfig, dict)
            ] = substitute_all_config_variables(self.config_variables, dict(os.environ))
            substitutions: dict = {
                **substituted_config_variables,
                **dict(os.environ),
                **runtime_environment,
            }
            config_str_with_substituted_variables: Union[
                (DataContextConfig, dict)
            ] = substitute_all_config_variables(yaml_config, substitutions)
        except Exception as e:
            usage_stats_event_payload: dict = {
                "diagnostic_info": ["__substitution_error__"]
            }
            send_usage_message(
                data_context=self,
                event=usage_stats_event_name,
                event_payload=usage_stats_event_payload,
                success=False,
            )
            raise e
        try:
            config: CommentedMap = yaml.load(config_str_with_substituted_variables)
            return config
        except Exception as e:
            usage_stats_event_payload: dict = {
                "diagnostic_info": ["__yaml_parse_error__"]
            }
            send_usage_message(
                data_context=self,
                event=usage_stats_event_name,
                event_payload=usage_stats_event_payload,
                success=False,
            )
            raise e

    def _test_instantiation_of_store_from_yaml_config(
        self, name: Optional[str], class_name: str, config: CommentedMap
    ) -> Tuple[(Store, dict)]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        Helper to create store instance and update usage stats payload.\n        See `test_yaml_config` for more details.\n        "
        print(f"	Instantiating as a Store, since class_name is {class_name}")
        store_name: str = name or config.get("name") or "my_temp_store"
        instantiated_class = cast(
            Store,
            self._build_store_from_config(store_name=store_name, store_config=config),
        )
        store_name = instantiated_class.store_name or store_name
        self.config["stores"][store_name] = config
        anonymizer = Anonymizer(self.data_context_id)
        usage_stats_event_payload = anonymizer.anonymize(
            store_name=store_name, store_obj=instantiated_class
        )
        return (instantiated_class, usage_stats_event_payload)

    def _test_instantiation_of_datasource_from_yaml_config(
        self, name: Optional[str], class_name: str, config: CommentedMap
    ) -> Tuple[(Datasource, dict)]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        Helper to create datasource instance and update usage stats payload.\n        See `test_yaml_config` for more details.\n        "
        print(f"	Instantiating as a Datasource, since class_name is {class_name}")
        datasource_name: str = name or config.get("name") or "my_temp_datasource"
        instantiated_class = cast(
            Datasource,
            self._instantiate_datasource_from_config_and_update_project_config(
                name=datasource_name, config=config, initialize=True
            ),
        )
        anonymizer = Anonymizer(self.data_context_id)
        if class_name == "SimpleSqlalchemyDatasource":
            usage_stats_event_payload = anonymizer.anonymize(
                obj=instantiated_class, name=datasource_name, config=config
            )
        else:
            datasource_config = datasourceConfigSchema.load(instantiated_class.config)
            full_datasource_config = datasourceConfigSchema.dump(datasource_config)
            usage_stats_event_payload = anonymizer.anonymize(
                obj=instantiated_class,
                name=datasource_name,
                config=full_datasource_config,
            )
        return (instantiated_class, usage_stats_event_payload)

    def _test_instantiation_of_checkpoint_from_yaml_config(
        self, name: Optional[str], class_name: str, config: CommentedMap
    ) -> Tuple[(Checkpoint, dict)]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        Helper to create checkpoint instance and update usage stats payload.\n        See `test_yaml_config` for more details.\n        "
        print(f"	Instantiating as a {class_name}, since class_name is {class_name}")
        checkpoint_name: str = name or config.get("name") or "my_temp_checkpoint"
        checkpoint_config: Union[(CheckpointConfig, dict)]
        checkpoint_config = CheckpointConfig.from_commented_map(commented_map=config)
        checkpoint_config = checkpoint_config.to_json_dict()
        checkpoint_config.update({"name": checkpoint_name})
        checkpoint_class_args: dict = filter_properties_dict(
            properties=checkpoint_config,
            delete_fields={"class_name", "module_name"},
            clean_falsy=True,
        )
        if class_name == "Checkpoint":
            instantiated_class = Checkpoint(data_context=self, **checkpoint_class_args)
        elif class_name == "SimpleCheckpoint":
            instantiated_class = SimpleCheckpoint(
                data_context=self, **checkpoint_class_args
            )
        else:
            raise ValueError(f'Unknown Checkpoint class_name: "{class_name}".')
        anonymizer: Anonymizer = Anonymizer(self.data_context_id)
        usage_stats_event_payload = anonymizer.anonymize(
            obj=instantiated_class, name=checkpoint_name, config=checkpoint_config
        )
        return (instantiated_class, usage_stats_event_payload)

    def _test_instantiation_of_data_connector_from_yaml_config(
        self,
        name: Optional[str],
        class_name: str,
        config: CommentedMap,
        runtime_environment: dict,
    ) -> Tuple[(DataConnector, dict)]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        Helper to create data connector instance and update usage stats payload.\n        See `test_yaml_config` for more details.\n        "
        print(f"	Instantiating as a DataConnector, since class_name is {class_name}")
        data_connector_name: str = (
            name or config.get("name") or "my_temp_data_connector"
        )
        instantiated_class = instantiate_class_from_config(
            config=config,
            runtime_environment={
                **runtime_environment,
                **{"root_directory": self.root_directory},
            },
            config_defaults={},
        )
        anonymizer = Anonymizer(self.data_context_id)
        usage_stats_event_payload = anonymizer.anonymize(
            obj=instantiated_class, name=data_connector_name, config=config
        )
        return (instantiated_class, usage_stats_event_payload)

    def _test_instantiation_of_profiler_from_yaml_config(
        self, name: Optional[str], class_name: str, config: CommentedMap
    ) -> Tuple[(RuleBasedProfiler, dict)]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        Helper to create profiler instance and update usage stats payload.\n        See `test_yaml_config` for more details.\n        "
        print(f"	Instantiating as a {class_name}, since class_name is {class_name}")
        profiler_name: str = name or config.get("name") or "my_temp_profiler"
        profiler_config: Union[
            (RuleBasedProfilerConfig, dict)
        ] = RuleBasedProfilerConfig.from_commented_map(commented_map=config)
        profiler_config = profiler_config.to_json_dict()
        profiler_config.update({"name": profiler_name})
        instantiated_class = instantiate_class_from_config(
            config=profiler_config,
            runtime_environment={"data_context": self},
            config_defaults={
                "module_name": "great_expectations.rule_based_profiler",
                "class_name": "RuleBasedProfiler",
            },
        )
        anonymizer: Anonymizer = Anonymizer(self.data_context_id)
        usage_stats_event_payload: dict = anonymizer.anonymize(
            obj=instantiated_class, name=profiler_name, config=profiler_config
        )
        return (instantiated_class, usage_stats_event_payload)

    def _test_instantiation_of_misc_class_from_yaml_config(
        self,
        name: Optional[str],
        config: CommentedMap,
        runtime_environment: dict,
        usage_stats_event_payload: dict,
    ) -> Tuple[(Any, dict)]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        Catch-all to cover all classes not covered in other `_test_instantiation` methods.\n        Attempts to match config to the relevant class/parent and update usage stats payload.\n        See `test_yaml_config` for more details.\n        "
        print(
            "\tNo matching class found. Attempting to instantiate class from the raw config..."
        )
        instantiated_class = instantiate_class_from_config(
            config=config,
            runtime_environment={
                **runtime_environment,
                **{"root_directory": self.root_directory},
            },
            config_defaults={},
        )
        anonymizer: Anonymizer = Anonymizer(self.data_context_id)
        parent_class_from_object = anonymizer.get_parent_class(
            object_=instantiated_class
        )
        parent_class_from_config = anonymizer.get_parent_class(object_config=config)
        if (parent_class_from_object is not None) and parent_class_from_object.endswith(
            "Store"
        ):
            store_name: str = name or config.get("name") or "my_temp_store"
            store_name = instantiated_class.store_name or store_name
            usage_stats_event_payload = anonymizer.anonymize(
                store_name=store_name, store_obj=instantiated_class
            )
        elif (
            parent_class_from_config is not None
        ) and parent_class_from_config.endswith("Datasource"):
            datasource_name: str = name or config.get("name") or "my_temp_datasource"
            if DatasourceAnonymizer.get_parent_class_v3_api(config=config):
                datasource_config = datasourceConfigSchema.load(
                    instantiated_class.config
                )
                full_datasource_config = datasourceConfigSchema.dump(datasource_config)
            else:
                full_datasource_config = config
            if parent_class_from_config == "SimpleSqlalchemyDatasource":
                usage_stats_event_payload = anonymizer.anonymize(
                    obj=instantiated_class, name=datasource_name, config=config
                )
            else:
                usage_stats_event_payload = anonymizer.anonymize(
                    obj=instantiated_class,
                    name=datasource_name,
                    config=full_datasource_config,
                )
        elif (
            parent_class_from_config is not None
        ) and parent_class_from_config.endswith("Checkpoint"):
            checkpoint_name: str = name or config.get("name") or "my_temp_checkpoint"
            checkpoint_config: Union[(CheckpointConfig, dict)]
            checkpoint_config = CheckpointConfig.from_commented_map(
                commented_map=config
            )
            checkpoint_config = checkpoint_config.to_json_dict()
            checkpoint_config.update({"name": checkpoint_name})
            usage_stats_event_payload = anonymizer.anonymize(
                obj=checkpoint_config, name=checkpoint_name, config=checkpoint_config
            )
        elif (
            parent_class_from_config is not None
        ) and parent_class_from_config.endswith("DataConnector"):
            data_connector_name: str = (
                name or config.get("name") or "my_temp_data_connector"
            )
            usage_stats_event_payload = anonymizer.anonymize(
                obj=instantiated_class, name=data_connector_name, config=config
            )
        else:
            usage_stats_event_payload[
                "diagnostic_info"
            ] = usage_stats_event_payload.get("diagnostic_info", []) + [
                "__custom_subclass_not_core_ge__"
            ]
        return (instantiated_class, usage_stats_event_payload)

    @staticmethod
    def _get_metric_configuration_tuples(metric_configuration, base_kwargs=None):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if base_kwargs is None:
            base_kwargs = {}
        if isinstance(metric_configuration, str):
            return [(metric_configuration, base_kwargs)]
        metric_configurations_list = []
        for kwarg_name in metric_configuration.keys():
            if not isinstance(metric_configuration[kwarg_name], dict):
                raise ge_exceptions.DataContextError(
                    "Invalid metric_configuration: each key must contain a dictionary."
                )
            if kwarg_name == "metric_kwargs_id":
                for metric_kwargs_id in metric_configuration[kwarg_name].keys():
                    if base_kwargs != {}:
                        raise ge_exceptions.DataContextError(
                            "Invalid metric_configuration: when specifying metric_kwargs_id, no other keys or values may be defined."
                        )
                    if not isinstance(
                        metric_configuration[kwarg_name][metric_kwargs_id], list
                    ):
                        raise ge_exceptions.DataContextError(
                            "Invalid metric_configuration: each value must contain a list."
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
                            "Invalid metric_configuration: each value must contain a list."
                        )
                    for nested_configuration in metric_configuration[kwarg_name][
                        kwarg_value
                    ]:
                        metric_configurations_list += (
                            BaseDataContext._get_metric_configuration_tuples(
                                nested_configuration, base_kwargs=base_kwargs
                            )
                        )
        return metric_configurations_list
