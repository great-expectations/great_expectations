import configparser
import copy
import datetime
import errno
import glob
import json
import logging
import os
import shutil
import sys
import uuid
import warnings
import webbrowser
from collections import OrderedDict
from typing import Dict, List, Optional, Union

from dateutil.parser import parse
from ruamel.yaml import YAML, YAMLError
from ruamel.yaml.constructor import DuplicateKeyError

import great_expectations.exceptions as ge_exceptions
from great_expectations.core import (
    ExpectationSuite,
    RunIdentifier,
    get_metric_kwargs_id,
)
from great_expectations.core.id_dict import BatchKwargs
from great_expectations.core.metric import ValidationMetricIdentifier
from great_expectations.core.usage_statistics.usage_statistics import (
    UsageStatisticsHandler,
    add_datasource_usage_statistics,
    run_validation_operator_usage_statistics,
    save_expectation_suite_usage_statistics,
    usage_statistics_enabled_method,
)
from great_expectations.core.util import nested_update
from great_expectations.data_asset import DataAsset
from great_expectations.data_context.templates import (
    CONFIG_VARIABLES_TEMPLATE,
    PROJECT_TEMPLATE_USAGE_STATISTICS_DISABLED,
    PROJECT_TEMPLATE_USAGE_STATISTICS_ENABLED,
)
from great_expectations.data_context.types.base import (
    CURRENT_CONFIG_VERSION,
    MINIMUM_SUPPORTED_CONFIG_VERSION,
    AnonymizedUsageStatisticsConfig,
    DataContextConfig,
    DatasourceConfig,
    anonymizedUsageStatisticsSchema,
    dataContextConfigSchema,
    datasourceConfigSchema,
)
from great_expectations.data_context.types.resource_identifiers import (
    ExpectationSuiteIdentifier,
    ValidationResultIdentifier,
)
from great_expectations.data_context.util import (
    file_relative_path,
    instantiate_class_from_config,
    load_class,
    substitute_all_config_variables,
    substitute_config_variable,
)
from great_expectations.dataset import Dataset
from great_expectations.datasource import Datasource
from great_expectations.marshmallow__shade import ValidationError
from great_expectations.profile.basic_dataset_profiler import BasicDatasetProfiler
from great_expectations.render.renderer.site_builder import SiteBuilder
from great_expectations.util import verify_dynamic_loading_support
from great_expectations.validator.validator import Validator

try:
    from sqlalchemy.exc import SQLAlchemyError
except ImportError:
    # We'll redefine this error in code below to catch ProfilerError, which is caught above, so SA errors will
    # just fall through
    SQLAlchemyError = ge_exceptions.ProfilerError

logger = logging.getLogger(__name__)
yaml = YAML()
yaml.indent(mapping=2, sequence=4, offset=2)
yaml.default_flow_style = False


class BaseDataContext:
    """
    This class implements most of the functionality of DataContext, with a few exceptions.

    1. BaseDataContext does not attempt to keep its project_config in sync with a file on disc.
    2. BaseDataContext doesn't attempt to "guess" paths or objects types. Instead, that logic is pushed
        into DataContext class.

    Together, these changes make BaseDataContext class more testable.

--ge-feature-maturity-info--

    id: os_linux
    title: OS - Linux
    icon:
    short_description:
    description:
    how_to_guide_url:
    maturity: Production
    maturity_details:
        api_stability: N/A
        implementation_completeness: N/A
        unit_test_coverage: Complete
        integration_infrastructure_test_coverage: Complete
        documentation_completeness: Complete
        bug_risk: Low

    id: os_macos
    title: OS - MacOS
    icon:
    short_description:
    description:
    how_to_guide_url:
    maturity: Production
    maturity_details:
        api_stability: N/A
        implementation_completeness: N/A
        unit_test_coverage: Complete (local only)
        integration_infrastructure_test_coverage: Complete (local only)
        documentation_completeness: Complete
        bug_risk: Low

    id: os_windows
    title: OS - Windows
    icon:
    short_description:
    description:
    how_to_guide_url:
    maturity: Beta
    maturity_details:
        api_stability: N/A
        implementation_completeness: N/A
        unit_test_coverage: Minimal
        integration_infrastructure_test_coverage: Minimal
        documentation_completeness: Complete
        bug_risk: Moderate
------------------------------------------------------------
    id: workflow_create_edit_expectations_cli_scaffold
    title: Create and Edit Expectations - suite scaffold
    icon:
    short_description: Creating a new Expectation Suite using suite scaffold
    description: Creating Expectation Suites through an interactive development loop using suite scaffold
    how_to_guide_url: https://docs.greatexpectations.io/en/latest/how_to_guides/creating_and_editing_expectations/how_to_create_a_new_expectation_suite_using_suite_scaffold.html
    maturity: Experimental (expect exciting changes to Profiler capability)
    maturity_details:
        api_stability: N/A
        implementation_completeness: N/A
        unit_test_coverage: N/A
        integration_infrastructure_test_coverage: Partial
        documentation_completeness: Complete
        bug_risk: Low

    id: workflow_create_edit_expectations_cli_edit
    title: Create and Edit Expectations - CLI
    icon:
    short_description: Creating a new Expectation Suite using the CLI
    description: Creating a Expectation Suite great_expectations suite new command
    how_to_guide_url: https://docs.greatexpectations.io/en/latest/how_to_guides/creating_and_editing_expectations/how_to_create_a_new_expectation_suite_using_the_cli.html
    maturity: Experimental (expect exciting changes to Profiler and Suite Renderer capability)
    maturity_details:
        api_stability: N/A
        implementation_completeness: N/A
        unit_test_coverage: N/A
        integration_infrastructure_test_coverage: Partial
        documentation_completeness: Complete
        bug_risk: Low

    id: workflow_create_edit_expectations_json_schema
    title: Create and Edit Expectations - Json schema
    icon:
    short_description: Creating a new Expectation Suite from a json schema file
    description: Creating a new Expectation Suite using JsonSchemaProfiler function and json schema file
    how_to_guide_url: https://docs.greatexpectations.io/en/latest/how_to_guides/creating_and_editing_expectations/how_to_create_a_suite_from_a_json_schema_file.html
    maturity: Experimental (expect exciting changes to Profiler capability)
    maturity_details:
        api_stability: N/A
        implementation_completeness: N/A
        unit_test_coverage: N/A
        integration_infrastructure_test_coverage: Partial
        documentation_completeness: Complete
        bug_risk: Low

--ge-feature-maturity-info--
    """

    PROFILING_ERROR_CODE_TOO_MANY_DATA_ASSETS = 2
    PROFILING_ERROR_CODE_SPECIFIED_DATA_ASSETS_NOT_FOUND = 3
    PROFILING_ERROR_CODE_NO_BATCH_KWARGS_GENERATORS_FOUND = 4
    PROFILING_ERROR_CODE_MULTIPLE_BATCH_KWARGS_GENERATORS_FOUND = 5
    UNCOMMITTED_DIRECTORIES = ["data_docs", "validations"]
    GE_UNCOMMITTED_DIR = "uncommitted"
    CHECKPOINTS_DIR = "checkpoints"
    BASE_DIRECTORIES = [
        CHECKPOINTS_DIR,
        "expectations",
        "notebooks",
        "plugins",
        GE_UNCOMMITTED_DIR,
    ]
    NOTEBOOK_SUBDIRECTORIES = ["pandas", "spark", "sql"]
    GE_DIR = "great_expectations"
    GE_YML = "great_expectations.yml"
    GE_EDIT_NOTEBOOK_DIR = GE_UNCOMMITTED_DIR
    FALSEY_STRINGS = ["FALSE", "false", "False", "f", "F", "0"]
    GLOBAL_CONFIG_PATHS = [
        os.path.expanduser("~/.great_expectations/great_expectations.conf"),
        "/etc/great_expectations.conf",
    ]
    DOLLAR_SIGN_ESCAPE_STRING = r"\$"

    @classmethod
    def validate_config(cls, project_config):
        if isinstance(project_config, DataContextConfig):
            return True
        try:
            dataContextConfigSchema.load(project_config)
        except ValidationError:
            raise
        return True

    @usage_statistics_enabled_method(event_name="data_context.__init__",)
    def __init__(self, project_config, context_root_dir=None, runtime_environment=None):
        """DataContext constructor

        Args:
            context_root_dir: location to look for the ``great_expectations.yml`` file. If None, searches for the file \
            based on conventions for project subdirectories.
            runtime_environment: a dictionary of config variables that
            override both those set in config_variables.yml and the environment

        Returns:
            None
        """
        if not BaseDataContext.validate_config(project_config):
            raise ge_exceptions.InvalidConfigError(
                "Your project_config is not valid. Try using the CLI check-config command."
            )
        self._project_config = project_config
        self._apply_global_config_overrides()
        if context_root_dir is not None:
            self._context_root_directory = os.path.abspath(context_root_dir)
        else:
            self._context_root_directory = context_root_dir
        self.runtime_environment = runtime_environment or {}

        # Init plugin support
        if self.plugins_directory is not None:
            sys.path.append(self.plugins_directory)

        # We want to have directories set up before initializing usage statistics so that we can obtain a context instance id
        self._in_memory_instance_id = (
            None  # This variable *may* be used in case we cannot save an instance id
        )
        self._initialize_usage_statistics(project_config.anonymous_usage_statistics)

        # Store cached datasources but don't init them
        self._cached_datasources = {}

        # Init stores
        self._stores = dict()
        self._init_stores(self._project_config_with_variables_substituted.stores)

        # Init validation operators
        # NOTE - 20200522 - JPC - A consistent approach to lazy loading for plugins will be useful here, harmonizing
        # the way that datasources, validation operators, site builders and other plugins are built.
        self.validation_operators = {}
        for (
            validation_operator_name,
            validation_operator_config,
        ) in self._project_config.validation_operators.items():
            self.add_validation_operator(
                validation_operator_name, validation_operator_config,
            )

        self._evaluation_parameter_dependencies_compiled = False
        self._evaluation_parameter_dependencies = {}

    def _build_store(self, store_name, store_config):
        module_name = "great_expectations.data_context.store"
        try:
            new_store = instantiate_class_from_config(
                config=store_config,
                runtime_environment={"root_directory": self.root_directory,},
                config_defaults={"module_name": module_name},
            )
        except ge_exceptions.DataContextError as e:
            new_store = None
            logger.critical(
                f"While attempting to instantiate the store named {store_name} an error occurred: {e}"
            )
        if not new_store:
            raise ge_exceptions.ClassInstantiationError(
                module_name=module_name,
                package_name=None,
                class_name=store_config["class_name"],
            )
        self._stores[store_name] = new_store
        return new_store

    def _init_stores(self, store_configs):
        """Initialize all Stores for this DataContext.

        Stores are a good fit for reading/writing objects that:
            1. follow a clear key-value pattern, and
            2. are usually edited programmatically, using the Context

        In general, Stores should take over most of the reading and writing to disk that DataContext had previously done.
        As of 9/21/2019, the following Stores had not yet been implemented
            * great_expectations.yml
            * expectations
            * data documentation
            * config_variables
            * anything accessed via write_resource

        Note that stores do NOT manage plugins.
        """

        for store_name, store_config in store_configs.items():
            self._build_store(store_name, store_config)

    def _apply_global_config_overrides(self):
        # check for global usage statistics opt out
        validation_errors = {}

        if self._check_global_usage_statistics_opt_out():
            logger.info(
                "Usage statistics is disabled globally. Applying override to project_config."
            )
            self._project_config.anonymous_usage_statistics.enabled = False

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
                self._project_config.anonymous_usage_statistics.data_context_id = (
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
                self._project_config.anonymous_usage_statistics.usage_statistics_url = (
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

    def _get_global_config_value(
        self, environment_variable=None, conf_file_section=None, conf_file_option=None
    ):
        assert (conf_file_section and conf_file_option) or (
            not conf_file_section and not conf_file_option
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

    def _check_global_usage_statistics_opt_out(self):
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
                    # If stats are disabled, then opt out is true
                    return True
            except (ValueError, configparser.Error):
                pass
        return False

    def _initialize_usage_statistics(
        self, usage_statistics_config: AnonymizedUsageStatisticsConfig
    ):
        """Initialize the usage statistics system."""
        if not usage_statistics_config.enabled:
            logger.info("Usage statistics is disabled; skipping initialization.")
            self._usage_statistics_handler = None
            return

        self._usage_statistics_handler = UsageStatisticsHandler(
            data_context=self,
            data_context_id=usage_statistics_config.data_context_id,
            usage_statistics_url=usage_statistics_config.usage_statistics_url,
        )

    def add_store(self, store_name, store_config):
        """Add a new Store to the DataContext and (for convenience) return the instantiated Store object.

        Args:
            store_name (str): a key for the new Store in in self._stores
            store_config (dict): a config for the Store to add

        Returns:
            store (Store)
        """

        self._project_config["stores"][store_name] = store_config
        return self._build_store(store_name, store_config)

    def add_validation_operator(
        self, validation_operator_name, validation_operator_config
    ):
        """Add a new ValidationOperator to the DataContext and (for convenience) return the instantiated object.

        Args:
            validation_operator_name (str): a key for the new ValidationOperator in in self._validation_operators
            validation_operator_config (dict): a config for the ValidationOperator to add

        Returns:
            validation_operator (ValidationOperator)
        """

        self._project_config["validation_operators"][
            validation_operator_name
        ] = validation_operator_config
        config = self._project_config_with_variables_substituted.validation_operators[
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

    def _normalize_absolute_or_relative_path(self, path):
        if path is None:
            return
        if os.path.isabs(path):
            return path
        else:
            return os.path.join(self.root_directory, path)

    def _normalize_store_path(self, resource_store):
        if resource_store["type"] == "filesystem":
            if not os.path.isabs(resource_store["base_directory"]):
                resource_store["base_directory"] = os.path.join(
                    self.root_directory, resource_store["base_directory"]
                )
        return resource_store

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
        unfiltered_sites = (
            self._project_config_with_variables_substituted.data_docs_sites
        )

        # Filter out sites that are not in site_names
        sites = (
            {k: v for k, v in unfiltered_sites.items() if k in site_names}
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

    @usage_statistics_enabled_method(event_name="data_context.open_data_docs",)
    def open_data_docs(
        self,
        resource_identifier: Optional[str] = None,
        site_name: Optional[str] = None,
        only_if_exists=True,
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
        """
        data_docs_urls = self.get_docs_sites_urls(
            resource_identifier=resource_identifier,
            site_name=site_name,
            only_if_exists=only_if_exists,
        )
        urls_to_open = [site["site_url"] for site in data_docs_urls]

        for url in urls_to_open:
            if url is not None:
                logger.debug(f"Opening Data Docs found here: {url}")
                webbrowser.open(url)

    @property
    def root_directory(self):
        """The root directory for configuration objects in the data context; the location in which
        ``great_expectations.yml`` is located."""
        return self._context_root_directory

    @property
    def plugins_directory(self):
        """The directory in which custom plugin modules should be placed."""
        return self._normalize_absolute_or_relative_path(
            self._project_config_with_variables_substituted.plugins_directory
        )

    @property
    def _project_config_with_variables_substituted(self):
        return self.get_config_with_variables_substituted()

    @property
    def anonymous_usage_statistics(self):
        return (
            self._project_config_with_variables_substituted.anonymous_usage_statistics
        )

    @property
    def notebooks(self):
        return self._project_config_with_variables_substituted.notebooks

    @property
    def stores(self):
        """A single holder for all Stores in this context"""
        return self._stores

    @property
    def datasources(self):
        """A single holder for all Datasources in this context"""
        return {
            datasource: self.get_datasource(datasource)
            for datasource in self._project_config_with_variables_substituted.datasources
        }

    @property
    def expectations_store_name(self):
        return self._project_config_with_variables_substituted.expectations_store_name

    @property
    def data_context_id(self):
        return (
            self._project_config_with_variables_substituted.anonymous_usage_statistics.data_context_id
        )

    @property
    def instance_id(self):
        instance_id = self._load_config_variables_file().get("instance_id")
        if instance_id is None:
            if self._in_memory_instance_id is not None:
                return self._in_memory_instance_id
            instance_id = str(uuid.uuid4())
            self._in_memory_instance_id = instance_id
        return instance_id

    #####
    #
    # Internal helper methods
    #
    #####

    def _load_config_variables_file(self):
        """Get all config variables from the default location."""
        config_variables_file_path = self.get_config().config_variables_file_path
        if config_variables_file_path:
            try:
                # If the user specifies the config variable path with an environment variable, we want to substitute it
                defined_path = substitute_config_variable(
                    config_variables_file_path, dict(os.environ)
                )
                if not os.path.isabs(defined_path):
                    # A BaseDataContext will not have a root directory; in that case use the current directory
                    # for any non-absolute path
                    root_directory = self.root_directory or os.curdir()
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

    def get_config_with_variables_substituted(self, config=None):

        if not config:
            config = self._project_config

        substituted_config_variables = substitute_all_config_variables(
            dict(self._load_config_variables_file()),
            dict(os.environ),
            self.DOLLAR_SIGN_ESCAPE_STRING,
        )

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

    def escape_all_config_variables(
        self,
        value: Union[str, dict, list],
        dollar_sign_escape_string: str = DOLLAR_SIGN_ESCAPE_STRING,
    ) -> Union[str, dict, list]:
        """
        Replace all `$` characters with the DOLLAR_SIGN_ESCAPE_STRING

        Args:
            value: config variable value
            dollar_sign_escape_string: replaces instances of `$`

        Returns:
            input value with all `$` characters replaced with the escape string
        """

        if isinstance(value, dict) or isinstance(value, OrderedDict):
            return {
                k: self.escape_all_config_variables(v, dollar_sign_escape_string)
                for k, v in value.items()
            }

        elif isinstance(value, list):
            return [
                self.escape_all_config_variables(v, dollar_sign_escape_string)
                for v in value
            ]
        return value.replace("$", dollar_sign_escape_string)

    def save_config_variable(self, config_variable_name, value):
        """Save config variable value

        Args:
            config_variable_name: name of the property
            value: the value to save for the property

        Returns:
            None
        """
        config_variables = self._load_config_variables_file()
        value = self.escape_all_config_variables(value, self.DOLLAR_SIGN_ESCAPE_STRING)
        config_variables[config_variable_name] = value
        config_variables_filepath = self.get_config().config_variables_file_path
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

    def delete_datasource(self, datasource_name=None):
        """Delete a data source
        Args:
            datasource_name: The name of the datasource to delete.

        Raises:
            ValueError: If the datasource name isn't provided or cannot be found.
        """
        if datasource_name is None:
            raise ValueError("Datasource names must be a datasource name")
        else:
            datasource = self.get_datasource(datasource_name)
            if datasource:
                # delete datasources project config
                del self._project_config_with_variables_substituted.datasources[
                    datasource_name
                ]
                del self._project_config.datasources[datasource_name]
                del self._cached_datasources[datasource_name]
            else:
                raise ValueError("Datasource {} not found".format(datasource_name))

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
            warnings.warn(
                "name is being deprecated as a batch_parameter. Please use data_asset_name instead.",
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

    def get_batch(
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

        datasource = self.get_datasource(batch_kwargs.get("datasource"))
        batch = datasource.get_batch(
            batch_kwargs=batch_kwargs, batch_parameters=batch_parameters
        )
        if data_asset_type is None:
            data_asset_type = datasource.config.get("data_asset_type")
        validator = Validator(
            batch=batch,
            expectation_suite=expectation_suite,
            expectation_engine=data_asset_type,
        )
        return validator.get_dataset()

    @usage_statistics_enabled_method(
        event_name="data_context.run_validation_operator",
        args_payload_fn=run_validation_operator_usage_statistics,
    )
    def run_validation_operator(
        self,
        validation_operator_name,
        assets_to_validate,
        run_id=None,
        evaluation_parameters=None,
        run_name=None,
        run_time=None,
        result_format={"result_format": "SUMMARY"},
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
            run_name: The run_name for the validation; if None, a default value will be used
            **kwargs: Additional kwargs to pass to the validation operator

        Returns:
            ValidationOperatorResult
        """
        if not assets_to_validate:
            raise ge_exceptions.DataContextError(
                "No batches of data were passed in. These are required"
            )

        for batch in assets_to_validate:
            if not isinstance(batch, (tuple, DataAsset)):
                raise ge_exceptions.DataContextError(
                    "Batches are required to be of type DataAsset"
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
            logger.info("Setting run_name to: {}".format(run_name))
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

    def list_validation_operator_names(self):
        if not self.validation_operators:
            return []
        return list(self.validation_operators.keys())

    @usage_statistics_enabled_method(
        event_name="data_context.add_datasource",
        args_payload_fn=add_datasource_usage_statistics,
    )
    def add_datasource(self, name, initialize=True, **kwargs):
        """Add a new datasource to the data context, with configuration provided as kwargs.
        Args:
            name: the name for the new datasource to add
            initialize: if False, add the datasource to the config, but do not
                initialize it, for example if a user needs to debug database connectivity.
            kwargs (keyword arguments): the configuration for the new datasource

        Returns:
            datasource (Datasource)
        """
        logger.debug("Starting BaseDataContext.add_datasource for %s" % name)
        module_name = kwargs.get("module_name", "great_expectations.datasource")
        verify_dynamic_loading_support(module_name=module_name)
        class_name = kwargs.get("class_name")
        datasource_class = load_class(module_name=module_name, class_name=class_name)

        # For any class that should be loaded, it may control its configuration construction
        # by implementing a classmethod called build_configuration
        if hasattr(datasource_class, "build_configuration"):
            config = datasource_class.build_configuration(**kwargs)
        else:
            config = kwargs

        config = datasourceConfigSchema.load(config)
        self._project_config["datasources"][name] = config

        # We perform variable substitution in the datasource's config here before using the config
        # to instantiate the datasource object. Variable substitution is a service that the data
        # context provides. Datasources should not see unsubstituted variables in their config.
        if initialize:
            datasource = self._build_datasource_from_config(
                name, self._project_config_with_variables_substituted.datasources[name]
            )
            self._cached_datasources[name] = datasource
        else:
            datasource = None

        return datasource

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

    def get_config(self):
        return self._project_config

    def _build_datasource_from_config(self, name, config):
        # We convert from the type back to a dictionary for purposes of instantiation
        if isinstance(config, DatasourceConfig):
            config = datasourceConfigSchema.dump(config)
        config.update({"name": name})
        module_name = "great_expectations.datasource"
        datasource = instantiate_class_from_config(
            config=config,
            runtime_environment={"data_context": self},
            config_defaults={"module_name": module_name},
        )
        if not datasource:
            raise ge_exceptions.ClassInstantiationError(
                module_name=module_name,
                package_name=None,
                class_name=config["class_name"],
            )
        return datasource

    def get_datasource(self, datasource_name: str = "default") -> Datasource:
        """Get the named datasource

        Args:
            datasource_name (str): the name of the datasource from the configuration

        Returns:
            datasource (Datasource)
        """
        if datasource_name in self._cached_datasources:
            return self._cached_datasources[datasource_name]
        if (
            datasource_name
            in self._project_config_with_variables_substituted.datasources
        ):
            datasource_config = copy.deepcopy(
                self._project_config_with_variables_substituted.datasources[
                    datasource_name
                ]
            )
        else:
            raise ValueError(
                f"Unable to load datasource `{datasource_name}` -- no configuration found or invalid configuration."
            )
        datasource_config = datasourceConfigSchema.load(datasource_config)
        datasource = self._build_datasource_from_config(
            datasource_name, datasource_config
        )
        self._cached_datasources[datasource_name] = datasource
        return datasource

    def list_expectation_suites(self):
        """Return a list of available expectation suite names."""
        try:
            keys = self.stores[self.expectations_store_name].list_keys()
        except KeyError as e:
            raise ge_exceptions.InvalidConfigError(
                "Unable to find configured store: %s" % str(e)
            )
        return keys

    def list_datasources(self):
        """List currently-configured datasources on this context.

        Returns:
            List(dict): each dictionary includes "name", "class_name", and "module_name" keys
        """
        datasources = []
        for (
            key,
            value,
        ) in self._project_config_with_variables_substituted.datasources.items():
            value["name"] = key
            datasources.append(value)
        return datasources

    def list_stores(self):
        """List currently-configured Stores on this context"""

        stores = []
        for (
            name,
            value,
        ) in self._project_config_with_variables_substituted.stores.items():
            value["name"] = name
            stores.append(value)
        return stores

    def list_validation_operators(self):
        """List currently-configured Validation Operators on this context"""

        validation_operators = []
        for (
            name,
            value,
        ) in (
            self._project_config_with_variables_substituted.validation_operators.items()
        ):
            value["name"] = name
            validation_operators.append(value)
        return validation_operators

    def create_expectation_suite(
        self, expectation_suite_name, overwrite_existing=False
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
            expectation_suite_name=expectation_suite_name
        )
        key = ExpectationSuiteIdentifier(expectation_suite_name=expectation_suite_name)

        if (
            self._stores[self.expectations_store_name].has_key(key)
            and not overwrite_existing
        ):
            raise ge_exceptions.DataContextError(
                "expectation_suite with name {} already exists. If you would like to overwrite this "
                "expectation_suite, set overwrite_existing=True.".format(
                    expectation_suite_name
                )
            )
        else:
            self._stores[self.expectations_store_name].set(key, expectation_suite)

        return expectation_suite

    def delete_expectation_suite(self, expectation_suite_name):
        """Delete specified expectation suite from data_context expectation store.

        Args:
            expectation_suite_name: The name of the expectation_suite to create

        Returns:
            True for Success and False for Failure.
        """
        key = ExpectationSuiteIdentifier(expectation_suite_name)
        if not self._stores[self.expectations_store_name].has_key(key):
            raise ge_exceptions.DataContextError(
                "expectation_suite with name {} does not exist."
            )
        else:
            self._stores[self.expectations_store_name].remove_key(key)
            return True
        return False

    def get_expectation_suite(self, expectation_suite_name):
        """Get a named expectation suite for the provided data_asset_name.

        Args:
            expectation_suite_name (str): the name for the expectation suite

        Returns:
            expectation_suite
        """
        key = ExpectationSuiteIdentifier(expectation_suite_name=expectation_suite_name)

        if self.stores[self.expectations_store_name].has_key(key):
            return self.stores[self.expectations_store_name].get(key)
        else:
            raise ge_exceptions.DataContextError(
                "expectation_suite %s not found" % expectation_suite_name
            )

    def list_expectation_suite_names(self):
        """Lists the available expectation suite names"""
        sorted_expectation_suite_names = [
            i.expectation_suite_name for i in self.list_expectation_suites()
        ]
        sorted_expectation_suite_names.sort()
        return sorted_expectation_suite_names

    @usage_statistics_enabled_method(
        event_name="data_context.save_expectation_suite",
        args_payload_fn=save_expectation_suite_usage_statistics,
    )
    def save_expectation_suite(self, expectation_suite, expectation_suite_name=None):
        """Save the provided expectation suite into the DataContext.

        Args:
            expectation_suite: the suite to save
            expectation_suite_name: the name of this expectation suite. If no name is provided the name will \
                be read from the suite

        Returns:
            None
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

        self.stores[self.expectations_store_name].set(key, expectation_suite)
        self._evaluation_parameter_dependencies_compiled = False

    def _store_metrics(self, requested_metrics, validation_results, target_store_name):
        """
        requested_metrics is a dictionary like this:

              requested_metrics:
                *:  # The asterisk here matches *any* expectation suite name
                  # use the 'kwargs' key to request metrics that are defined by kwargs,
                  # for example because they are defined only for a particular column
                  # - column:
                  #     Age:
                  #        - expect_column_min_to_be_between.result.observed_value
                    - statistics.evaluated_expectations
                    - statistics.successful_expectations

        Args:
            requested_metrics:
            validation_results:
            target_store_name:

        Returns:

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
                metric_configurations = _get_metric_configuration_tuples(
                    metric_configuration
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

    def store_validation_result_metrics(
        self, requested_metrics, validation_results, target_store_name
    ):
        self._store_metrics(requested_metrics, validation_results, target_store_name)

    def store_evaluation_parameters(self, validation_results, target_store_name=None):
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
        return self.stores[self.evaluation_parameter_store_name]

    @property
    def evaluation_parameter_store_name(self):
        return (
            self._project_config_with_variables_substituted.evaluation_parameter_store_name
        )

    @property
    def validations_store_name(self):
        return self._project_config_with_variables_substituted.validations_store_name

    @property
    def validations_store(self):
        return self.stores[self.validations_store_name]

    def _compile_evaluation_parameter_dependencies(self):
        self._evaluation_parameter_dependencies = {}
        for key in self.stores[self.expectations_store_name].list_keys():
            expectation_suite = self.stores[self.expectations_store_name].get(key)
            if not expectation_suite:
                continue

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
        """Get validation results from a configured store.

        Args:
            data_asset_name: name of data asset for which to get validation result
            expectation_suite_name: expectation_suite name for which to get validation result (default: "default")
            run_id: run_id for which to get validation result (if None, fetch the latest result by alphanumeric sort)
            validations_store_name: the name of the store from which to get validation results
            failed_only: if True, filter the result to return only failed expectations

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

        key = ValidationResultIdentifier(
            expectation_suite_identifier=ExpectationSuiteIdentifier(
                expectation_suite_name=expectation_suite_name
            ),
            run_id=run_id,
            batch_identifier=batch_identifier,
        )
        results_dict = selected_store.get(key)

        # TODO: This should be a convenience method of ValidationResultSuite
        if failed_only:
            failed_results_list = [
                result for result in results_dict.results if not result.success
            ]
            results_dict.results = failed_results_list
            return results_dict
        else:
            return results_dict

    def update_return_obj(self, data_asset, return_obj):
        """Helper called by data_asset.

        Args:
            data_asset: The data_asset whose validation produced the current return object
            return_obj: the return object to update

        Returns:
            return_obj: the return object, potentially changed into a widget by the configured expectation explorer
        """
        return return_obj

    @usage_statistics_enabled_method(event_name="data_context.build_data_docs")
    def build_data_docs(
        self, site_names=None, resource_identifiers=None, dry_run=False
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

        Returns:
            A dictionary with the names of the updated data documentation sites as keys and the the location info
            of their index.html files as values
        """
        logger.debug("Starting DataContext.build_data_docs")

        index_page_locator_infos = {}

        sites = self._project_config_with_variables_substituted.data_docs_sites
        if sites:
            logger.debug("Found data_docs_sites. Building sites...")

            for site_name, site_config in sites.items():
                logger.debug("Building Data Docs Site %s" % site_name,)

                if (site_names and (site_name in site_names)) or not site_names:
                    complete_site_config = site_config
                    module_name = "great_expectations.render.renderer.site_builder"
                    site_builder = instantiate_class_from_config(
                        config=complete_site_config,
                        runtime_environment={
                            "data_context": self,
                            "root_directory": self.root_directory,
                            "site_name": site_name,
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
                            resource_identifiers
                        )
                        if index_page_resource_identifier_tuple:
                            index_page_locator_infos[
                                site_name
                            ] = index_page_resource_identifier_tuple[0]

        else:
            logger.debug("No data_docs_config found. No site(s) built.")

        return index_page_locator_infos

    def clean_data_docs(self, site_name=None):
        sites123 = self._project_config_with_variables_substituted.data_docs_sites
        cleaned = False
        for sname, site_config in sites123.items():
            if site_name is None:
                cleaned = False
                complete_site_config = site_config
                module_name = "great_expectations.render.renderer.site_builder"
                site_builder = instantiate_class_from_config(
                    config=complete_site_config,
                    runtime_environment={
                        "data_context": self,
                        "root_directory": self.root_directory,
                    },
                    config_defaults={"module_name": module_name},
                )
                site_builder.clean_site()
                cleaned = True
            else:
                if site_name == sname:
                    complete_site_config = site_config
                    module_name = "great_expectations.render.renderer.site_builder"
                    site_builder = instantiate_class_from_config(
                        config=complete_site_config,
                        runtime_environment={
                            "data_context": self,
                            "root_directory": self.root_directory,
                        },
                        config_defaults={"module_name": module_name},
                    )
                    site_builder.clean_site()
                    return True
        return cleaned

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

        if not dry_run:
            logger.info(
                "Profiling '{}' with '{}'".format(datasource_name, profiler.__name__)
            )

        profiling_results = {}

        # Build the list of available data asset names (each item a tuple of name and type)

        data_asset_names_dict = self.get_available_data_asset_names(datasource_name)

        available_data_asset_name_list = []
        try:
            datasource_data_asset_names_dict = data_asset_names_dict[datasource_name]
        except KeyError:
            # KeyError will happen if there is not datasource
            raise ge_exceptions.ProfilerError(
                "No datasource {} found.".format(datasource_name)
            )

        if batch_kwargs_generator_name is None:
            # if no generator name is passed as an arg and the datasource has only
            # one generator with data asset names, use it.
            # if ambiguous, raise an exception
            for name in datasource_data_asset_names_dict.keys():
                if batch_kwargs_generator_name is not None:
                    profiling_results = {
                        "success": False,
                        "error": {
                            "code": DataContext.PROFILING_ERROR_CODE_MULTIPLE_BATCH_KWARGS_GENERATORS_FOUND
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
                        "code": DataContext.PROFILING_ERROR_CODE_NO_BATCH_KWARGS_GENERATORS_FOUND
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

        data_asset_names_to_profiled = None

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
                        "code": DataContext.PROFILING_ERROR_CODE_SPECIFIED_DATA_ASSETS_NOT_FOUND,
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
                    % (",".join(data_assets))
                )
        else:
            if not profile_all_data_assets:
                if total_data_assets > max_data_assets:
                    profiling_results = {
                        "success": False,
                        "error": {
                            "code": DataContext.PROFILING_ERROR_CODE_TOO_MANY_DATA_ASSETS,
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
            total_columns, total_expectations, total_rows, skipped_data_assets = (
                0,
                0,
                0,
                0,
            )
            total_start_time = datetime.datetime.now()

            for name in data_asset_names_to_profiled:
                logger.info("\tProfiling '%s'..." % name)
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
                        "SqlAlchemyError while profiling %s. Skipping." % name[1]
                    )
                    logger.debug(str(e))
                    skipped_data_assets += 1

            total_duration = (
                datetime.datetime.now() - total_start_time
            ).total_seconds()
            logger.info(
                """
    Profiled %d of %d named data assets, with %d total rows and %d columns in %.2f seconds.
    Generated, evaluated, and stored %d Expectations during profiling. Please review results using data-docs."""
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
            warnings.warn(
                "String run_ids will be deprecated in the future. Please provide a run_id of type "
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

        logger.info(
            "Profiling '{}' with '{}'".format(datasource_name, profiler.__name__)
        )

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

        total_columns, total_expectations, total_rows, skipped_data_assets = 0, 0, 0, 0
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
            expectation_suite_name=expectation_suite_name, batch_kwargs=batch_kwargs,
        )

        if not profiler.validate(batch):
            raise ge_exceptions.ProfilerError(
                "batch '%s' is not a valid batch for the '%s' profiler"
                % (name, profiler.__name__)
            )

        # Note: This logic is specific to DatasetProfilers, which profile a single batch. Multi-batch profilers
        # will have more to unpack.
        expectation_suite, validation_results = profiler.profile(
            batch, run_id=run_id, profiler_configuration=profiler_configuration
        )
        profiling_results["results"].append((expectation_suite, validation_results))

        self.validations_store.set(
            key=ValidationResultIdentifier(
                expectation_suite_identifier=ExpectationSuiteIdentifier(
                    expectation_suite_name=expectation_suite_name
                ),
                run_id=run_id,
                batch_identifier=batch.batch_id,
            ),
            value=validation_results,
        )

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
        logger.info(
            "\tProfiled %d columns using %d rows from %s (%.3f sec)"
            % (new_column_count, row_count, name, duration)
        )

        total_duration = (datetime.datetime.now() - total_start_time).total_seconds()
        logger.info(
            """
Profiled the data asset, with %d total rows and %d columns in %.2f seconds.
Generated, evaluated, and stored %d Expectations during profiling. Please review results using data-docs."""
            % (total_rows, total_columns, total_duration, total_expectations,)
        )

        profiling_results["success"] = True
        return profiling_results


class DataContext(BaseDataContext):
    """A DataContext represents a Great Expectations project. It organizes storage and access for
    expectation suites, datasources, notification settings, and data fixtures.

    The DataContext is configured via a yml file stored in a directory called great_expectations; the configuration file
    as well as managed expectation suites should be stored in version control.

    Use the `create` classmethod to create a new empty config, or instantiate the DataContext
    by passing the path to an existing data context root directory.

    DataContexts use data sources you're already familiar with. BatchKwargGenerators help introspect data stores and data execution
    frameworks (such as airflow, Nifi, dbt, or dagster) to describe and produce batches of data ready for analysis. This
    enables fetching, validation, profiling, and documentation of  your data in a way that is meaningful within your
    existing infrastructure and work environment.

    DataContexts use a datasource-based namespace, where each accessible type of data has a three-part
    normalized *data_asset_name*, consisting of *datasource/generator/data_asset_name*.

    - The datasource actually connects to a source of materialized data and returns Great Expectations DataAssets \
      connected to a compute environment and ready for validation.

    - The BatchKwargGenerator knows how to introspect datasources and produce identifying "batch_kwargs" that define \
      particular slices of data.

    - The data_asset_name is a specific name -- often a table name or other name familiar to users -- that \
      batch kwargs generators can slice into batches.

    An expectation suite is a collection of expectations ready to be applied to a batch of data. Since
    in many projects it is useful to have different expectations evaluate in different contexts--profiling
    vs. testing; warning vs. error; high vs. low compute; ML model or dashboard--suites provide a namespace
    option for selecting which expectations a DataContext returns.

    In many simple projects, the datasource or batch kwargs generator name may be omitted and the DataContext will infer
    the correct name when there is no ambiguity.

    Similarly, if no expectation suite name is provided, the DataContext will assume the name "default".
    """

    @classmethod
    def create(
        cls,
        project_root_dir=None,
        usage_statistics_enabled=True,
        runtime_environment=None,
    ):
        """
        Build a new great_expectations directory and DataContext object in the provided project_root_dir.

        `create` will not create a new "great_expectations" directory in the provided folder, provided one does not
        already exist. Then, it will initialize a new DataContext in that folder and write the resulting config.

        Args:
            project_root_dir: path to the root directory in which to create a new great_expectations directory
            runtime_environment: a dictionary of config variables that
            override both those set in config_variables.yml and the environment

        Returns:
            DataContext
        """

        if not os.path.isdir(project_root_dir):
            raise ge_exceptions.DataContextError(
                "The project_root_dir must be an existing directory in which "
                "to initialize a new DataContext"
            )

        ge_dir = os.path.join(project_root_dir, cls.GE_DIR)
        os.makedirs(ge_dir, exist_ok=True)
        cls.scaffold_directories(ge_dir)

        if os.path.isfile(os.path.join(ge_dir, cls.GE_YML)):
            message = """Warning. An existing `{}` was found here: {}.
    - No action was taken.""".format(
                cls.GE_YML, ge_dir
            )
            warnings.warn(message)
        else:
            cls.write_project_template_to_disk(ge_dir, usage_statistics_enabled)

        if os.path.isfile(os.path.join(ge_dir, "notebooks")):
            message = """Warning. An existing `notebooks` directory was found here: {}.
    - No action was taken.""".format(
                ge_dir
            )
            warnings.warn(message)
        else:
            cls.scaffold_notebooks(ge_dir)

        uncommitted_dir = os.path.join(ge_dir, cls.GE_UNCOMMITTED_DIR)
        if os.path.isfile(os.path.join(uncommitted_dir, "config_variables.yml")):
            message = """Warning. An existing `config_variables.yml` was found here: {}.
    - No action was taken.""".format(
                uncommitted_dir
            )
            warnings.warn(message)
        else:
            cls.write_config_variables_template_to_disk(uncommitted_dir)

        return cls(ge_dir, runtime_environment=runtime_environment)

    @classmethod
    def all_uncommitted_directories_exist(cls, ge_dir):
        """Check if all uncommitted direcotries exist."""
        uncommitted_dir = os.path.join(ge_dir, cls.GE_UNCOMMITTED_DIR)
        for directory in cls.UNCOMMITTED_DIRECTORIES:
            if not os.path.isdir(os.path.join(uncommitted_dir, directory)):
                return False

        return True

    @classmethod
    def config_variables_yml_exist(cls, ge_dir):
        """Check if all config_variables.yml exists."""
        path_to_yml = os.path.join(ge_dir, cls.GE_YML)

        # TODO this is so brittle and gross
        with open(path_to_yml) as f:
            config = yaml.load(f)
        config_var_path = config.get("config_variables_file_path")
        config_var_path = os.path.join(ge_dir, config_var_path)
        return os.path.isfile(config_var_path)

    @classmethod
    def write_config_variables_template_to_disk(cls, uncommitted_dir):
        os.makedirs(uncommitted_dir, exist_ok=True)
        config_var_file = os.path.join(uncommitted_dir, "config_variables.yml")
        with open(config_var_file, "w") as template:
            template.write(CONFIG_VARIABLES_TEMPLATE)

    @classmethod
    def write_project_template_to_disk(cls, ge_dir, usage_statistics_enabled=True):
        file_path = os.path.join(ge_dir, cls.GE_YML)
        with open(file_path, "w") as template:
            if usage_statistics_enabled:
                template.write(PROJECT_TEMPLATE_USAGE_STATISTICS_ENABLED)
            else:
                template.write(PROJECT_TEMPLATE_USAGE_STATISTICS_DISABLED)

    @classmethod
    def scaffold_directories(cls, base_dir):
        """Safely create GE directories for a new project."""
        os.makedirs(base_dir, exist_ok=True)
        open(os.path.join(base_dir, ".gitignore"), "w").write("uncommitted/")

        for directory in cls.BASE_DIRECTORIES:
            if directory == "plugins":
                plugins_dir = os.path.join(base_dir, directory)
                os.makedirs(plugins_dir, exist_ok=True)
                os.makedirs(
                    os.path.join(plugins_dir, "custom_data_docs"), exist_ok=True
                )
                os.makedirs(
                    os.path.join(plugins_dir, "custom_data_docs", "views"),
                    exist_ok=True,
                )
                os.makedirs(
                    os.path.join(plugins_dir, "custom_data_docs", "renderers"),
                    exist_ok=True,
                )
                os.makedirs(
                    os.path.join(plugins_dir, "custom_data_docs", "styles"),
                    exist_ok=True,
                )
                cls.scaffold_custom_data_docs(plugins_dir)
            else:
                os.makedirs(os.path.join(base_dir, directory), exist_ok=True)

        uncommitted_dir = os.path.join(base_dir, cls.GE_UNCOMMITTED_DIR)

        for new_directory in cls.UNCOMMITTED_DIRECTORIES:
            new_directory_path = os.path.join(uncommitted_dir, new_directory)
            os.makedirs(new_directory_path, exist_ok=True)

        notebook_path = os.path.join(base_dir, "notebooks")
        for subdir in cls.NOTEBOOK_SUBDIRECTORIES:
            os.makedirs(os.path.join(notebook_path, subdir), exist_ok=True)

    @classmethod
    def scaffold_custom_data_docs(cls, plugins_dir):
        """Copy custom data docs templates"""
        styles_template = file_relative_path(
            __file__,
            "../render/view/static/styles/data_docs_custom_styles_template.css",
        )
        styles_destination_path = os.path.join(
            plugins_dir, "custom_data_docs", "styles", "data_docs_custom_styles.css"
        )
        shutil.copyfile(styles_template, styles_destination_path)

    @classmethod
    def scaffold_notebooks(cls, base_dir):
        """Copy template notebooks into the notebooks directory for a project."""
        template_dir = file_relative_path(__file__, "../init_notebooks/")
        notebook_dir = os.path.join(base_dir, "notebooks/")
        for subdir in cls.NOTEBOOK_SUBDIRECTORIES:
            subdir_path = os.path.join(notebook_dir, subdir)
            for notebook in glob.glob(os.path.join(template_dir, subdir, "*.ipynb")):
                notebook_name = os.path.basename(notebook)
                destination_path = os.path.join(subdir_path, notebook_name)
                shutil.copyfile(notebook, destination_path)

    def __init__(self, context_root_dir=None, runtime_environment=None):

        # Determine the "context root directory" - this is the parent of "great_expectations" dir
        if context_root_dir is None:
            context_root_dir = self.find_context_root_dir()
        context_root_directory = os.path.abspath(os.path.expanduser(context_root_dir))
        self._context_root_directory = context_root_directory

        project_config = self._load_project_config()
        project_config_dict = dataContextConfigSchema.dump(project_config)
        super().__init__(project_config, context_root_directory, runtime_environment)

        # save project config if data_context_id auto-generated or global config values applied
        if (
            project_config.anonymous_usage_statistics.explicit_id is False
            or project_config_dict != dataContextConfigSchema.dump(self._project_config)
        ):
            self._save_project_config()

    def _load_project_config(self):
        """
        Reads the project configuration from the project configuration file.
        The file may contain ${SOME_VARIABLE} variables - see self._project_config_with_variables_substituted
        for how these are substituted.

        :return: the configuration object read from the file
        """
        path_to_yml = os.path.join(self.root_directory, self.GE_YML)
        try:
            with open(path_to_yml) as data:
                config_dict = yaml.load(data)

        except YAMLError as err:
            raise ge_exceptions.InvalidConfigurationYamlError(
                "Your configuration file is not a valid yml file likely due to a yml syntax error:\n\n{}".format(
                    err
                )
            )
        except DuplicateKeyError:
            raise ge_exceptions.InvalidConfigurationYamlError(
                "Error: duplicate key found in project YAML file."
            )
        except OSError:
            raise ge_exceptions.ConfigNotFoundError()

        try:
            return DataContextConfig.from_commented_map(config_dict)
        except ge_exceptions.InvalidDataContextConfigError:
            # Just to be explicit about what we intended to catch
            raise

    def list_checkpoints(self) -> List[str]:
        """List checkpoints. (Experimental)"""
        # TODO mark experimental
        files = self._list_ymls_in_checkpoints_directory()
        return [
            os.path.basename(f)[:-4]
            for f in files
            if os.path.basename(f).endswith(".yml")
        ]

    def get_checkpoint(self, checkpoint_name: str) -> dict:
        """Load a checkpoint. (Experimental)"""
        # TODO mark experimental
        yaml = YAML(typ="safe")
        # TODO make a serializable class with a schema
        checkpoint_path = os.path.join(
            self.root_directory, self.CHECKPOINTS_DIR, f"{checkpoint_name}.yml"
        )
        try:
            with open(checkpoint_path) as f:
                checkpoint = yaml.load(f.read())
                return self._validate_checkpoint(checkpoint, checkpoint_name)
        except FileNotFoundError:
            raise ge_exceptions.CheckpointNotFoundError(
                f"Could not find checkpoint `{checkpoint_name}`."
            )

    def _list_ymls_in_checkpoints_directory(self):
        checkpoints_dir = os.path.join(self.root_directory, self.CHECKPOINTS_DIR)
        files = glob.glob(os.path.join(checkpoints_dir, "*.yml"), recursive=False)
        return files

    def _save_project_config(self):
        """Save the current project to disk."""
        logger.debug("Starting DataContext._save_project_config")

        config_filepath = os.path.join(self.root_directory, self.GE_YML)
        with open(config_filepath, "w") as outfile:
            self._project_config.to_yaml(outfile)

    def add_store(self, store_name, store_config):
        logger.debug("Starting DataContext.add_store for store %s" % store_name)

        new_store = super().add_store(store_name, store_config)
        self._save_project_config()
        return new_store

    def add_datasource(self, name, **kwargs):
        logger.debug("Starting DataContext.add_datasource for datasource %s" % name)

        new_datasource = super().add_datasource(name, **kwargs)
        self._save_project_config()

        return new_datasource

    def delete_datasource(self, name, **kwargs):
        logger.debug("Starting DataContext.delete_datasource for datasource %s" % name)

        delete_datasource = super().delete_datasource(name, **kwargs)
        self._save_project_config()

        return delete_datasource

    @classmethod
    def find_context_root_dir(cls):
        result = None
        yml_path = None
        ge_home_environment = os.getenv("GE_HOME")
        if ge_home_environment:
            ge_home_environment = os.path.expanduser(ge_home_environment)
            if os.path.isdir(ge_home_environment) and os.path.isfile(
                os.path.join(ge_home_environment, "great_expectations.yml")
            ):
                result = ge_home_environment
        else:
            yml_path = cls.find_context_yml_file()
            if yml_path:
                result = os.path.dirname(yml_path)

        if result is None:
            raise ge_exceptions.ConfigNotFoundError()

        logger.debug("Using project config: {}".format(yml_path))
        return result

    @classmethod
    def get_ge_config_version(cls, context_root_dir=None):
        yml_path = cls.find_context_yml_file(search_start_dir=context_root_dir)
        if yml_path is None:
            return

        with open(yml_path) as f:
            config_dict = yaml.load(f)

        config_version = config_dict.get("config_version")
        return float(config_version) if config_version else None

    @classmethod
    def set_ge_config_version(
        cls, config_version, context_root_dir=None, validate_config_version=True
    ):
        if not isinstance(config_version, (int, float)):
            raise ge_exceptions.UnsupportedConfigVersionError(
                "The argument `config_version` must be a number.",
            )

        if validate_config_version:
            if config_version < MINIMUM_SUPPORTED_CONFIG_VERSION:
                raise ge_exceptions.UnsupportedConfigVersionError(
                    "Invalid config version ({}).\n    The version number must be at least {}. ".format(
                        config_version, MINIMUM_SUPPORTED_CONFIG_VERSION
                    ),
                )
            elif config_version > CURRENT_CONFIG_VERSION:
                raise ge_exceptions.UnsupportedConfigVersionError(
                    "Invalid config version ({}).\n    The maximum valid version is {}.".format(
                        config_version, CURRENT_CONFIG_VERSION
                    ),
                )

        yml_path = cls.find_context_yml_file(search_start_dir=context_root_dir)
        if yml_path is None:
            return False

        with open(yml_path) as f:
            config_dict = yaml.load(f)
            config_dict["config_version"] = config_version

        with open(yml_path, "w") as f:
            yaml.dump(config_dict, f)

        return True

    @classmethod
    def find_context_yml_file(cls, search_start_dir=None):
        """Search for the yml file starting here and moving upward."""
        yml_path = None
        if search_start_dir is None:
            search_start_dir = os.getcwd()

        for i in range(4):
            logger.debug(
                "Searching for config file {} ({} layer deep)".format(
                    search_start_dir, i
                )
            )

            potential_ge_dir = os.path.join(search_start_dir, cls.GE_DIR)

            if os.path.isdir(potential_ge_dir):
                potential_yml = os.path.join(potential_ge_dir, cls.GE_YML)
                if os.path.isfile(potential_yml):
                    yml_path = potential_yml
                    logger.debug("Found config file at " + str(yml_path))
                    break
            # move up one directory
            search_start_dir = os.path.dirname(search_start_dir)

        return yml_path

    @classmethod
    def does_config_exist_on_disk(cls, context_root_dir):
        """Return True if the great_expectations.yml exists on disk."""
        return os.path.isfile(os.path.join(context_root_dir, cls.GE_YML))

    @classmethod
    def is_project_initialized(cls, ge_dir):
        """
        Return True if the project is initialized.

        To be considered initialized, all of the following must be true:
        - all project directories exist (including uncommitted directories)
        - a valid great_expectations.yml is on disk
        - a config_variables.yml is on disk
        - the project has at least one datasource
        - the project has at least one suite
        """
        return (
            cls.does_config_exist_on_disk(ge_dir)
            and cls.all_uncommitted_directories_exist(ge_dir)
            and cls.config_variables_yml_exist(ge_dir)
            and cls._does_context_have_at_least_one_datasource(ge_dir)
            and cls._does_context_have_at_least_one_suite(ge_dir)
        )

    @classmethod
    def does_project_have_a_datasource_in_config_file(cls, ge_dir):
        if not cls.does_config_exist_on_disk(ge_dir):
            return False
        return cls._does_context_have_at_least_one_datasource(ge_dir)

    @classmethod
    def _does_context_have_at_least_one_datasource(cls, ge_dir):
        context = cls._attempt_context_instantiation(ge_dir)
        if not isinstance(context, DataContext):
            return False
        return len(context.list_datasources()) >= 1

    @classmethod
    def _does_context_have_at_least_one_suite(cls, ge_dir):
        context = cls._attempt_context_instantiation(ge_dir)
        if not isinstance(context, DataContext):
            return False
        return len(context.list_expectation_suites()) >= 1

    @classmethod
    def _attempt_context_instantiation(cls, ge_dir):
        try:
            context = DataContext(ge_dir)
            return context
        except (
            ge_exceptions.DataContextError,
            ge_exceptions.InvalidDataContextConfigError,
        ) as e:
            logger.debug(e)

    @staticmethod
    def _validate_checkpoint(checkpoint: dict, checkpoint_name: str) -> dict:
        if checkpoint is None:
            raise ge_exceptions.CheckpointError(
                f"Checkpoint `{checkpoint_name}` has no contents. Please fix this."
            )
        if "validation_operator_name" not in checkpoint:
            checkpoint["validation_operator_name"] = "action_list_operator"

        if "batches" not in checkpoint:
            raise ge_exceptions.CheckpointError(
                f"Checkpoint `{checkpoint_name}` is missing required key: `batches`."
            )
        batches = checkpoint["batches"]
        if not isinstance(batches, list):
            raise ge_exceptions.CheckpointError(
                f"In the checkpoint `{checkpoint_name}`, the key `batches` must be a list"
            )

        for batch in batches:
            for required in ["expectation_suite_names", "batch_kwargs"]:
                if required not in batch:
                    raise ge_exceptions.CheckpointError(
                        f"Items in `batches` must have a key `{required}`"
                    )

        return checkpoint


class ExplorerDataContext(DataContext):
    def __init__(self, context_root_dir=None, expectation_explorer=True):
        """
            expectation_explorer: If True, load the expectation explorer manager, which will modify GE return objects \
            to include ipython notebook widgets.
        """

        super().__init__(context_root_dir)

        self._expectation_explorer = expectation_explorer
        if expectation_explorer:
            from great_expectations.jupyter_ux.expectation_explorer import (
                ExpectationExplorer,
            )

            self._expectation_explorer_manager = ExpectationExplorer()

    def update_return_obj(self, data_asset, return_obj):
        """Helper called by data_asset.

        Args:
            data_asset: The data_asset whose validation produced the current return object
            return_obj: the return object to update

        Returns:
            return_obj: the return object, potentially changed into a widget by the configured expectation explorer
        """
        if self._expectation_explorer:
            return self._expectation_explorer_manager.create_expectation_widget(
                data_asset, return_obj
            )
        else:
            return return_obj


def _get_metric_configuration_tuples(metric_configuration, base_kwargs=None):
    if base_kwargs is None:
        base_kwargs = {}

    if isinstance(metric_configuration, str):
        return [(metric_configuration, base_kwargs)]

    metric_configurations_list = []
    for kwarg_name in metric_configuration.keys():
        if not isinstance(metric_configuration[kwarg_name], dict):
            raise ge_exceptions.DataContextError(
                "Invalid metric_configuration: each key must contain a " "dictionary."
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
                if not isinstance(metric_configuration[kwarg_name][kwarg_value], list):
                    raise ge_exceptions.DataContextError(
                        "Invalid metric_configuration: each value must contain a "
                        "list."
                    )
                for nested_configuration in metric_configuration[kwarg_name][
                    kwarg_value
                ]:
                    metric_configurations_list += _get_metric_configuration_tuples(
                        nested_configuration, base_kwargs=base_kwargs
                    )

    return metric_configurations_list
