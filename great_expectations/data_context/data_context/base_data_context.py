import datetime
import logging
import os
import traceback
import uuid
import warnings
import webbrowser
from collections import OrderedDict
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

if TYPE_CHECKING:
    from great_expectations.validation_operators.validation_operators import (
        ValidationOperator,
    )

from dateutil.parser import parse
from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap

from great_expectations.core.config_peer import ConfigPeer
from great_expectations.core.usage_statistics.events import UsageStatsEvents
from great_expectations.data_context.store.ge_cloud_store_backend import (
    GeCloudRESTResource,
)
from great_expectations.rule_based_profiler.config.base import (
    ruleBasedProfilerConfigSchema,
)

try:
    from typing import Literal  # type: ignore[attr-defined]
except ImportError:
    # Fallback for python < 3.8
    from typing_extensions import Literal  # type: ignore[misc]

from marshmallow import ValidationError

import great_expectations.exceptions as ge_exceptions
from great_expectations.checkpoint import Checkpoint, SimpleCheckpoint
from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
from great_expectations.core.batch import Batch, BatchRequestBase, IDDict
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.id_dict import BatchKwargs
from great_expectations.core.run_identifier import RunIdentifier
from great_expectations.core.usage_statistics.anonymizers.anonymizer import Anonymizer
from great_expectations.core.usage_statistics.anonymizers.datasource_anonymizer import (
    DatasourceAnonymizer,
)
from great_expectations.core.usage_statistics.usage_statistics import (
    add_datasource_usage_statistics,
    get_batch_list_usage_statistics,
    run_validation_operator_usage_statistics,
    save_expectation_suite_usage_statistics,
    send_usage_message,
    usage_statistics_enabled_method,
)
from great_expectations.data_asset import DataAsset
from great_expectations.data_context.data_context.cloud_data_context import (
    CloudDataContext,
)
from great_expectations.data_context.data_context.ephemeral_data_context import (
    EphemeralDataContext,
)
from great_expectations.data_context.data_context.file_data_context import (
    FileDataContext,
)
from great_expectations.data_context.store import Store
from great_expectations.data_context.templates import CONFIG_VARIABLES_TEMPLATE
from great_expectations.data_context.types.base import (
    CheckpointConfig,
    DataContextConfig,
    DataContextConfigDefaults,
    DatasourceConfig,
    GeCloudConfig,
    dataContextConfigSchema,
    datasourceConfigSchema,
)
from great_expectations.data_context.types.refs import (
    GeCloudIdAwareRef,
    GeCloudResourceRef,
)
from great_expectations.data_context.types.resource_identifiers import (
    ConfigurationIdentifier,
    ExpectationSuiteIdentifier,
    GeCloudIdentifier,
    ValidationResultIdentifier,
)
from great_expectations.data_context.util import (
    instantiate_class_from_config,
    parse_substitution_variable,
    substitute_all_config_variables,
)
from great_expectations.dataset import Dataset
from great_expectations.datasource import LegacyDatasource
from great_expectations.datasource.data_connector.data_connector import DataConnector
from great_expectations.datasource.new_datasource import BaseDatasource, Datasource
from great_expectations.profile.basic_dataset_profiler import BasicDatasetProfiler
from great_expectations.render.renderer.site_builder import SiteBuilder
from great_expectations.rule_based_profiler import (
    RuleBasedProfiler,
    RuleBasedProfilerResult,
)
from great_expectations.rule_based_profiler.config import RuleBasedProfilerConfig
from great_expectations.util import filter_properties_dict
from great_expectations.validator.validator import BridgeValidator, Validator

try:
    from sqlalchemy.exc import SQLAlchemyError
except ImportError:
    # We'll redefine this error in code below to catch ProfilerError, which is caught above, so SA errors will
    # just fall through
    SQLAlchemyError = ge_exceptions.ProfilerError

logger = logging.getLogger(__name__)

# TODO: check if this can be refactored to use YAMLHandler class
yaml = YAML()
yaml.indent(mapping=2, sequence=4, offset=2)
yaml.default_flow_style = False


# TODO: <WILL> Most of the logic here will be migrated to EphemeralDataContext
class BaseDataContext(EphemeralDataContext, ConfigPeer):
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
        how_to_guide_url: https://docs.greatexpectations.io/en/latest/how_to_guides/creating_and_editing_expectations/how_to_automatically_create_a_new_expectation_suite.html
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
    BASE_DIRECTORIES = [
        DataContextConfigDefaults.CHECKPOINTS_BASE_DIRECTORY.value,
        DataContextConfigDefaults.EXPECTATIONS_BASE_DIRECTORY.value,
        DataContextConfigDefaults.PLUGINS_BASE_DIRECTORY.value,
        DataContextConfigDefaults.PROFILERS_BASE_DIRECTORY.value,
        GE_UNCOMMITTED_DIR,
    ]
    GE_DIR = "great_expectations"
    GE_YML = "great_expectations.yml"  # TODO: migrate this to FileDataContext. Still needed by DataContext
    GE_EDIT_NOTEBOOK_DIR = GE_UNCOMMITTED_DIR
    DOLLAR_SIGN_ESCAPE_STRING = r"\$"

    _data_context = None

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
        ge_cloud_mode: bool = False,
        ge_cloud_config: Optional[GeCloudConfig] = None,
    ) -> None:
        """DataContext constructor

        Args:
            context_root_dir: location to look for the ``great_expectations.yml`` file. If None, searches for the file
                based on conventions for project subdirectories.
            runtime_environment: a dictionary of config variables that
                override both those set in config_variables.yml and the environment
            ge_cloud_mode: boolean flag that describe whether DataContext is being instantiated by ge_cloud
           ge_cloud_config: config for ge_cloud
        Returns:
            None
        """
        if not BaseDataContext.validate_config(project_config):
            raise ge_exceptions.InvalidConfigError(
                "Your project_config is not valid. Try using the CLI check-config command."
            )
        self._ge_cloud_mode = ge_cloud_mode
        self._ge_cloud_config = ge_cloud_config
        if context_root_dir is not None:
            context_root_dir = os.path.abspath(context_root_dir)
        self._context_root_directory = context_root_dir
        # initialize runtime_environment as empty dict if None
        runtime_environment = runtime_environment or {}
        if self._ge_cloud_mode:
            ge_cloud_base_url: Optional[str] = None
            ge_cloud_access_token: Optional[str] = None
            ge_cloud_organization_id: Optional[str] = None
            if ge_cloud_config:
                ge_cloud_base_url = ge_cloud_config.base_url
                ge_cloud_access_token = ge_cloud_config.access_token
                ge_cloud_organization_id = ge_cloud_config.organization_id
            self._data_context = CloudDataContext(
                project_config=project_config,
                runtime_environment=runtime_environment,
                context_root_dir=context_root_dir,  # type: ignore[arg-type]
                ge_cloud_base_url=ge_cloud_base_url,
                ge_cloud_access_token=ge_cloud_access_token,
                ge_cloud_organization_id=ge_cloud_organization_id,
            )
        elif self._context_root_directory:
            self._data_context = FileDataContext(  # type: ignore[assignment]
                project_config=project_config,
                context_root_dir=context_root_dir,  # type: ignore[arg-type]
                runtime_environment=runtime_environment,
            )
        else:
            self._data_context = EphemeralDataContext(  # type: ignore[assignment]
                project_config=project_config, runtime_environment=runtime_environment
            )

        # NOTE: <DataContextRefactor> This will ensure that parameters set in _data_context are persisted to self.
        # It is rather clunkly and we should explore other ways of ensuring that BaseDataContext has all of the
        # necessary properties / overrides
        self._synchronize_self_with_underlying_data_context()

        self._variables = self._data_context.variables  # type: ignore[assignment,union-attr]

        # Init validation operators
        # NOTE - 20200522 - JPC - A consistent approach to lazy loading for plugins will be useful here, harmonizing
        # the way that execution environments (AKA datasources), validation operators, site builders and other
        # plugins are built.
        self.validation_operators: Dict = {}
        # NOTE - 20210112 - Alex Sherstinsky - Validation Operators are planned to be deprecated.
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

    @property
    def ge_cloud_config(self) -> Optional[GeCloudConfig]:
        return self._ge_cloud_config

    @property
    def ge_cloud_mode(self) -> bool:
        return self._ge_cloud_mode

    def _synchronize_self_with_underlying_data_context(self) -> None:
        """
        This is a helper method that only exists during the DataContext refactor that is occurring 202206.

        Until the composition-pattern is complete for BaseDataContext, we have to load the private properties from the
        private self._data_context object into properties in self

        This is a helper method that performs this loading.
        """
        # NOTE: <DataContextRefactor> This remains a rather clunky way of ensuring that all necessary parameters and
        # values from self._data_context are persisted to self.
        self._project_config = self._data_context._project_config  # type: ignore[union-attr]
        self.runtime_environment = self._data_context.runtime_environment or {}  # type: ignore[union-attr]
        self._config_variables = self._data_context.config_variables  # type: ignore[union-attr]
        self._in_memory_instance_id = self._data_context._in_memory_instance_id  # type: ignore[union-attr]
        self._stores = self._data_context._stores  # type: ignore[union-attr]
        self._datasource_store = self._data_context._datasource_store  # type: ignore[union-attr]
        self._data_context_id = self._data_context._data_context_id  # type: ignore[union-attr]
        self._usage_statistics_handler = self._data_context._usage_statistics_handler  # type: ignore[union-attr]
        self._cached_datasources = self._data_context._cached_datasources  # type: ignore[union-attr]
        self._evaluation_parameter_dependencies_compiled = (
            self._data_context._evaluation_parameter_dependencies_compiled  # type: ignore[union-attr]
        )
        self._evaluation_parameter_dependencies = (
            self._data_context._evaluation_parameter_dependencies  # type: ignore[union-attr]
        )
        self._assistants = self._data_context._assistants  # type: ignore[union-attr]

    def _save_project_config(self) -> None:
        """Save the current project to disk."""
        logger.debug("Starting DataContext._save_project_config")

        config_filepath = os.path.join(self.root_directory, self.GE_YML)  # type: ignore[arg-type]

        try:
            with open(config_filepath, "w") as outfile:
                self.config.to_yaml(outfile)
        except PermissionError as e:
            logger.warning(f"Could not save project config to disk: {e}")

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

    def add_validation_operator(
        self, validation_operator_name: str, validation_operator_config: dict
    ) -> "ValidationOperator":
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

    def _normalize_store_path(self, resource_store):
        if resource_store["type"] == "filesystem":
            if not os.path.isabs(resource_store["base_directory"]):
                resource_store["base_directory"] = os.path.join(
                    self.root_directory, resource_store["base_directory"]
                )
        return resource_store

    def get_site_names(self) -> List[str]:
        """Get a list of configured site names."""
        return list(self.variables.data_docs_sites.keys())  # type: ignore[union-attr]

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

    @usage_statistics_enabled_method(
        event_name=UsageStatsEvents.DATA_CONTEXT_OPEN_DATA_DOCS.value,
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

    #####
    #
    # Internal helper methods
    #
    #####

    def escape_all_config_variables(
        self,
        value: Union[str, dict, list],
        dollar_sign_escape_string: str = DOLLAR_SIGN_ESCAPE_STRING,
        skip_if_substitution_variable: bool = True,
    ) -> Union[str, dict, list]:
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
            return {
                k: self.escape_all_config_variables(
                    v, dollar_sign_escape_string, skip_if_substitution_variable
                )
                for k, v in value.items()
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

    def delete_datasource(  # type: ignore[override]
        self, datasource_name: str, save_changes: bool = False
    ) -> None:
        """Delete a data source
        Args:
            datasource_name: The name of the datasource to delete.
            save_changes: Whether or not to save changes to disk.

        Raises:
            ValueError: If the datasource name isn't provided or cannot be found.
        """
        super().delete_datasource(datasource_name, save_changes=save_changes)
        self._synchronize_self_with_underlying_data_context()

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

        datasource = self.get_datasource(batch_kwargs.get("datasource"))  # type: ignore[union-attr,arg-type]
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

    @usage_statistics_enabled_method(
        event_name=UsageStatsEvents.DATA_CONTEXT_RUN_VALIDATION_OPERATOR.value,
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

    @usage_statistics_enabled_method(
        event_name=UsageStatsEvents.DATA_CONTEXT_GET_BATCH_LIST.value,
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
        **kwargs,
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
        return super().get_batch_list(
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

    def list_validation_operator_names(self):
        if not self.validation_operators:
            return []

        return list(self.validation_operators.keys())

    @usage_statistics_enabled_method(
        event_name=UsageStatsEvents.DATA_CONTEXT_ADD_DATASOURCE.value,
        args_payload_fn=add_datasource_usage_statistics,
    )
    def add_datasource(
        self,
        name: str,
        initialize: bool = True,
        save_changes: bool = False,
        **kwargs: dict,
    ) -> Optional[Union[LegacyDatasource, BaseDatasource]]:
        """
        Add named datasource, with options to initialize (and return) the datasource and save_config.

        Current version will call super(), which preserves the `usage_statistics` decorator in the current method.
        A subsequence refactor will migrate the `usage_statistics` to parent and sibling classes.

        Args:
            name (str): Name of Datasource
            initialize (bool): Should GE add and initialize the Datasource? If true then current
                method will return initialized Datasource
            save_changes (bool): should GE save the Datasource config?
            **kwargs Optional[dict]: Additional kwargs that define Datasource initialization kwargs

        Returns:
            Datasource that was added

        """
        new_datasource = super().add_datasource(
            name=name, initialize=initialize, save_changes=save_changes, **kwargs
        )
        self._synchronize_self_with_underlying_data_context()
        return new_datasource

    def update_datasource(
        self,
        datasource: Union[LegacyDatasource, BaseDatasource],
        save_changes: bool = False,
    ) -> None:
        """
        Updates a DatasourceConfig that already exists in the store.

        Args:
            datasource_config: The config object to persist using the DatasourceStore.
            save_changes: do I save changes to disk?
        """
        datasource_config_dict: dict = datasourceConfigSchema.dump(datasource.config)
        datasource_config = DatasourceConfig(**datasource_config_dict)
        datasource_name: str = datasource.name

        if save_changes:
            self._datasource_store.update_by_name(
                datasource_name=datasource_name, datasource_config=datasource_config
            )
        self.config.datasources[datasource_name] = datasource_config  # type: ignore[assignment,index]
        self._cached_datasources[datasource_name] = datasource_config

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

    def create_expectation_suite(
        self,
        expectation_suite_name: str,
        overwrite_existing: bool = False,
        **kwargs,
    ) -> ExpectationSuite:
        """
        See `AbstractDataContext.create_expectation_suite` for more information.
        """
        suite = self._data_context.create_expectation_suite(  # type: ignore[union-attr]
            expectation_suite_name,
            overwrite_existing=overwrite_existing,
            **kwargs,
        )
        self._synchronize_self_with_underlying_data_context()
        return suite

    def get_expectation_suite(
        self,
        expectation_suite_name: Optional[str] = None,
        include_rendered_content: Optional[bool] = None,
        ge_cloud_id: Optional[str] = None,
    ) -> ExpectationSuite:
        """
        Args:
            expectation_suite_name (str): The name of the Expectation Suite
            include_rendered_content (bool): Whether or not to re-populate rendered_content for each
                ExpectationConfiguration.
            ge_cloud_id (str): The GE Cloud ID for the Expectation Suite.

        Returns:
            An existing ExpectationSuite
        """
        if include_rendered_content is None:
            include_rendered_content = (
                self._determine_if_expectation_suite_include_rendered_content()
            )

        res = self._data_context.get_expectation_suite(  # type: ignore[union-attr]
            expectation_suite_name=expectation_suite_name,
            include_rendered_content=include_rendered_content,
            ge_cloud_id=ge_cloud_id,
        )
        return res

    def delete_expectation_suite(  # type: ignore[override]
        self,
        expectation_suite_name: Optional[str] = None,
        ge_cloud_id: Optional[str] = None,
    ) -> bool:
        """
        See `AbstractDataContext.delete_expectation_suite` for more information.
        """
        res = self._data_context.delete_expectation_suite(  # type: ignore[union-attr]
            expectation_suite_name=expectation_suite_name, ge_cloud_id=ge_cloud_id
        )
        self._synchronize_self_with_underlying_data_context()
        return res

    @usage_statistics_enabled_method(
        event_name=UsageStatsEvents.DATA_CONTEXT_SAVE_EXPECTATION_SUITE.value,
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
        """Save the provided expectation suite into the DataContext.

        Args:
            expectation_suite: The suite to save.
            expectation_suite_name: The name of this Expectation Suite. If no name is provided, the name will be read
                from the suite.
            overwrite_existing: Whether to overwrite the suite if it already exists.
            include_rendered_content: Whether to save the prescriptive rendered content for each expectation.

        Returns:
            None
        """
        include_rendered_content = (
            self._determine_if_expectation_suite_include_rendered_content(
                include_rendered_content=include_rendered_content
            )
        )

        self._data_context.save_expectation_suite(  # type: ignore[union-attr]
            expectation_suite,
            expectation_suite_name,
            overwrite_existing,
            include_rendered_content,
            **kwargs,
        )
        self._synchronize_self_with_underlying_data_context()

    def store_validation_result_metrics(
        self, requested_metrics, validation_results, target_store_name
    ) -> None:
        self._store_metrics(requested_metrics, validation_results, target_store_name)

    @property
    def root_directory(self) -> Optional[str]:
        if hasattr(self._data_context, "_context_root_directory"):
            return self._data_context._context_root_directory  # type: ignore[union-attr]
        return None

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

    @usage_statistics_enabled_method(
        event_name=UsageStatsEvents.DATA_CONTEXT_BUILD_DATA_DOCS.value,
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
                            build_index=(build_index and not self.ge_cloud_mode),
                        )
                        if index_page_resource_identifier_tuple:
                            index_page_locator_infos[
                                site_name
                            ] = index_page_resource_identifier_tuple[0]

        else:
            logger.debug("No data_docs_config found. No site(s) built.")

        return index_page_locator_infos

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
                    f"Profiling the white-listed data assets: {','.join(data_assets)}, alphabetically."
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

        if isinstance(validation_ref, GeCloudIdAwareRef):
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

    def list_checkpoints(self) -> Union[List[str], List[ConfigurationIdentifier]]:
        return self.checkpoint_store.list_checkpoints(ge_cloud_mode=self.ge_cloud_mode)

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
        """
        See parent 'AbstractDataContext.add_checkpoint()' for more information
        """
        checkpoint = self._data_context.add_checkpoint(  # type: ignore[union-attr]
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
            default_validation_id=default_validation_id,
        )
        # <TODO> Remove this after BaseDataContext refactor is complete.
        # currently this can cause problems if the Checkpoint is instantiated with
        # EphemeralDataContext, which does not (yet) have full functionality.
        checkpoint._data_context = self

        self._synchronize_self_with_underlying_data_context()
        return checkpoint

    def get_checkpoint(
        self,
        name: Optional[str] = None,
        ge_cloud_id: Optional[str] = None,
    ) -> Checkpoint:
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
        event_name=UsageStatsEvents.DATA_CONTEXT_RUN_CHECKPOINT.value,
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

    def save_profiler(
        self,
        profiler: RuleBasedProfiler,
    ) -> RuleBasedProfiler:
        name = profiler.name
        ge_cloud_id = profiler.ge_cloud_id

        key: Union[GeCloudIdentifier, ConfigurationIdentifier]
        if self.ge_cloud_mode:
            key = GeCloudIdentifier(
                resource_type=GeCloudRESTResource.PROFILER, ge_cloud_id=ge_cloud_id
            )
        else:
            key = ConfigurationIdentifier(configuration_key=name)

        response = self.profiler_store.set(key=key, value=profiler.config)  # type: ignore[func-returns-value]
        if isinstance(response, GeCloudResourceRef):
            ge_cloud_id = response.ge_cloud_id

        # If an id is present, we want to prioritize that as our key for object retrieval
        if ge_cloud_id:
            name = None  # type: ignore[assignment]

        profiler = self.get_profiler(name=name, ge_cloud_id=ge_cloud_id)
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

    def list_profilers(self) -> List[str]:
        if self.profiler_store is None:
            raise ge_exceptions.StoreConfigurationError(
                "Attempted to list profilers from a Profiler Store, which is not a configured store."
            )
        return RuleBasedProfiler.list_profilers(
            profiler_store=self.profiler_store,
            ge_cloud_mode=self.ge_cloud_mode,
        )

    @usage_statistics_enabled_method(
        event_name=UsageStatsEvents.DATA_CONTEXT_RUN_RULE_BASED_PROFILER_WITH_DYNAMIC_ARGUMENTS.value,
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
            ge_cloud_id: Identifier used to retrieve the profiler from a store (GE Cloud specific).
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
        event_name=UsageStatsEvents.DATA_CONTEXT_RUN_RULE_BASED_PROFILER_ON_DATA.value,
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
            ge_cloud_id: Identifier used to retrieve the profiler from a store (GE Cloud specific).

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

    def list_expectation_suites(self) -> Optional[List[str]]:
        """
        See parent 'AbstractDataContext.list_expectation_suites()` for more information.
        """
        return self._data_context.list_expectation_suites()  # type: ignore[union-attr]

    def list_expectation_suite_names(self) -> List[str]:
        """
        See parent 'AbstractDataContext.list_expectation_suite_names()` for more information.
        """
        return self._data_context.list_expectation_suite_names()  # type: ignore[union-attr]

    def test_yaml_config(  # noqa: C901 - complexity 17
        self,
        yaml_config: str,
        name: Optional[str] = None,
        class_name: Optional[str] = None,
        runtime_environment: Optional[dict] = None,
        pretty_print: bool = True,
        return_mode: Union[  # type: ignore[name-defined]
            Literal["instantiated_class"], Literal["report_object"]
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

        test_yaml_config is mainly intended for use within notebooks and tests.

        --Public API--

        --Documentation--
            https://docs.greatexpectations.io/docs/terms/data_context
            https://docs.greatexpectations.io/docs/guides/validation/checkpoints/how_to_configure_a_new_checkpoint_using_test_yaml_config

        Args:
            yaml_config: A string containing the yaml config to be tested
            name: (Optional) A string containing the name of the component to instantiate
            pretty_print: Determines whether to print human-readable output
            return_mode: Determines what type of object test_yaml_config will return.
                Valid modes are "instantiated_class" and "report_object"
            shorten_tracebacks:If true, catch any errors during instantiation and print only the
                last element of the traceback stack. This can be helpful for
                rapid iteration on configs in a notebook, because it can remove
                the need to scroll up and down a lot.

        Returns:
            The instantiated component (e.g. a Datasource)
            OR
            a json object containing metadata from the component's self_check method.
            The returned object is determined by return_mode.
        """
        if return_mode not in ["instantiated_class", "report_object"]:
            raise ValueError(f"Unknown return_mode: {return_mode}.")

        if runtime_environment is None:
            runtime_environment = {}

        runtime_environment = {
            **runtime_environment,
            **self.runtime_environment,
        }

        usage_stats_event_name: str = "data_context.test_yaml_config"

        config = self._test_yaml_config_prepare_config(
            yaml_config, runtime_environment, usage_stats_event_name
        )

        if "class_name" in config:
            class_name = config["class_name"]

        instantiated_class: Any = None
        usage_stats_event_payload: Dict[str, Union[str, List[str]]] = {}

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
                    f"\tSuccessfully instantiated {instantiated_class.__class__.__name__}\n"
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
                ] = usage_stats_event_payload.get(
                    "diagnostic_info", []
                ) + [  # type: ignore[operator]
                    "__class_name_not_provided__"
                ]
            elif (
                usage_stats_event_payload.get("parent_class") is None
                and class_name in self.ALL_TEST_YAML_CONFIG_SUPPORTED_TYPES
            ):
                # add parent_class if it doesn't exist and class_name is one of our supported core GE types
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
        """
        Performs variable substitution and conversion from YAML to CommentedMap.
        See `test_yaml_config` for more details.
        """
        try:
            substituted_config_variables: Union[
                DataContextConfig, dict
            ] = substitute_all_config_variables(
                self.config_variables,
                dict(os.environ),
            )

            substitutions: dict = {
                **substituted_config_variables,  # type: ignore[list-item]
                **dict(os.environ),
                **runtime_environment,
            }

            config_str_with_substituted_variables: Union[
                DataContextConfig, dict
            ] = substitute_all_config_variables(
                yaml_config,
                substitutions,
            )
        except Exception as e:
            usage_stats_event_payload: dict = {
                "diagnostic_info": ["__substitution_error__"],
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
            usage_stats_event_payload = {
                "diagnostic_info": ["__yaml_parse_error__"],
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
    ) -> Tuple[Store, dict]:
        """
        Helper to create store instance and update usage stats payload.
        See `test_yaml_config` for more details.
        """
        print(f"\tInstantiating as a Store, since class_name is {class_name}")
        store_name: str = name or config.get("name") or "my_temp_store"
        instantiated_class = cast(
            Store,
            self._build_store_from_config(
                store_name=store_name,
                store_config=config,
            ),
        )
        store_name = instantiated_class.store_name or store_name
        self.config.stores[store_name] = config  # type: ignore[index]

        anonymizer = Anonymizer(self.data_context_id)
        usage_stats_event_payload = anonymizer.anonymize(
            store_name=store_name, store_obj=instantiated_class  # type: ignore[arg-type]
        )
        return instantiated_class, usage_stats_event_payload

    def _test_instantiation_of_datasource_from_yaml_config(
        self, name: Optional[str], class_name: str, config: CommentedMap
    ) -> Tuple[Datasource, dict]:
        """
        Helper to create datasource instance and update usage stats payload.
        See `test_yaml_config` for more details.
        """
        print(f"\tInstantiating as a Datasource, since class_name is {class_name}")
        datasource_name: str = name or config.get("name") or "my_temp_datasource"
        datasource_config = datasourceConfigSchema.load(config)
        datasource_config.name = datasource_name
        instantiated_class = cast(
            Datasource,
            self._instantiate_datasource_from_config_and_update_project_config(
                config=datasource_config,
                initialize=True,
                save_changes=False,
            ),
        )

        anonymizer = Anonymizer(self.data_context_id)

        if class_name == "SimpleSqlalchemyDatasource":
            # Use the raw config here, defaults will be added in the anonymizer
            usage_stats_event_payload = anonymizer.anonymize(
                obj=instantiated_class, name=datasource_name, config=config  # type: ignore[arg-type]
            )
        else:
            # Roundtrip through schema validation to remove any illegal fields add/or restore any missing fields.
            datasource_config = datasourceConfigSchema.load(instantiated_class.config)
            full_datasource_config = datasourceConfigSchema.dump(datasource_config)
            usage_stats_event_payload = anonymizer.anonymize(
                obj=instantiated_class,
                name=datasource_name,  # type: ignore[arg-type]
                config=full_datasource_config,
            )
        return instantiated_class, usage_stats_event_payload

    def _test_instantiation_of_checkpoint_from_yaml_config(
        self, name: Optional[str], class_name: str, config: CommentedMap
    ) -> Tuple[Checkpoint, dict]:
        """
        Helper to create checkpoint instance and update usage stats payload.
        See `test_yaml_config` for more details.
        """
        print(f"\tInstantiating as a {class_name}, since class_name is {class_name}")

        checkpoint_name: str = name or config.get("name") or "my_temp_checkpoint"

        checkpoint_config: Union[CheckpointConfig, dict]

        checkpoint_config = CheckpointConfig.from_commented_map(commented_map=config)  # type: ignore[assignment]
        checkpoint_config = checkpoint_config.to_json_dict()  # type: ignore[union-attr]
        checkpoint_config.update({"name": checkpoint_name})

        checkpoint_class_args: dict = filter_properties_dict(  # type: ignore[assignment]
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

        anonymizer = Anonymizer(self.data_context_id)

        usage_stats_event_payload = anonymizer.anonymize(
            obj=instantiated_class, name=checkpoint_name, config=checkpoint_config  # type: ignore[arg-type]
        )

        return instantiated_class, usage_stats_event_payload

    def _test_instantiation_of_data_connector_from_yaml_config(
        self,
        name: Optional[str],
        class_name: str,
        config: CommentedMap,
        runtime_environment: dict,
    ) -> Tuple[DataConnector, dict]:
        """
        Helper to create data connector instance and update usage stats payload.
        See `test_yaml_config` for more details.
        """
        print(f"\tInstantiating as a DataConnector, since class_name is {class_name}")
        data_connector_name: str = (
            name or config.get("name") or "my_temp_data_connector"
        )
        instantiated_class = instantiate_class_from_config(
            config=config,
            runtime_environment={
                **runtime_environment,
                **{
                    "root_directory": self.root_directory,
                },
            },
            config_defaults={},
        )

        anonymizer = Anonymizer(self.data_context_id)

        usage_stats_event_payload = anonymizer.anonymize(
            obj=instantiated_class, name=data_connector_name, config=config  # type: ignore[arg-type]
        )
        return instantiated_class, usage_stats_event_payload

    def _test_instantiation_of_profiler_from_yaml_config(
        self, name: Optional[str], class_name: str, config: CommentedMap
    ) -> Tuple[RuleBasedProfiler, dict]:
        """
        Helper to create profiler instance and update usage stats payload.
        See `test_yaml_config` for more details.
        """
        print(f"\tInstantiating as a {class_name}, since class_name is {class_name}")

        profiler_name: str = name or config.get("name") or "my_temp_profiler"

        profiler_config: Union[
            RuleBasedProfilerConfig, dict
        ] = RuleBasedProfilerConfig.from_commented_map(commented_map=config)
        profiler_config = profiler_config.to_json_dict()  # type: ignore[union-attr]
        profiler_config.update({"name": profiler_name})

        instantiated_class = instantiate_class_from_config(
            config=profiler_config,
            runtime_environment={"data_context": self},
            config_defaults={
                "module_name": "great_expectations.rule_based_profiler",
                "class_name": "RuleBasedProfiler",
            },
        )

        anonymizer = Anonymizer(self.data_context_id)

        usage_stats_event_payload: dict = anonymizer.anonymize(
            obj=instantiated_class, name=profiler_name, config=profiler_config  # type: ignore[arg-type]
        )

        return instantiated_class, usage_stats_event_payload

    def _test_instantiation_of_misc_class_from_yaml_config(
        self,
        name: Optional[str],
        config: CommentedMap,
        runtime_environment: dict,
        usage_stats_event_payload: dict,
    ) -> Tuple[Any, dict]:
        """
        Catch-all to cover all classes not covered in other `_test_instantiation` methods.
        Attempts to match config to the relevant class/parent and update usage stats payload.
        See `test_yaml_config` for more details.
        """
        print(
            "\tNo matching class found. Attempting to instantiate class from the raw config..."
        )
        instantiated_class = instantiate_class_from_config(
            config=config,
            runtime_environment={
                **runtime_environment,
                **{
                    "root_directory": self.root_directory,
                },
            },
            config_defaults={},
        )

        # If a subclass of a supported type, find the parent class and anonymize
        anonymizer = Anonymizer(self.data_context_id)

        parent_class_from_object = anonymizer.get_parent_class(
            object_=instantiated_class
        )
        parent_class_from_config = anonymizer.get_parent_class(object_config=config)

        if parent_class_from_object is not None and parent_class_from_object.endswith(
            "Store"
        ):
            store_name: str = name or config.get("name") or "my_temp_store"
            store_name = instantiated_class.store_name or store_name
            usage_stats_event_payload = anonymizer.anonymize(
                store_name=store_name, store_obj=instantiated_class  # type: ignore[arg-type]
            )
        elif parent_class_from_config is not None and parent_class_from_config.endswith(
            "Datasource"
        ):
            datasource_name: str = name or config.get("name") or "my_temp_datasource"
            if DatasourceAnonymizer.get_parent_class_v3_api(config=config):
                # Roundtrip through schema validation to remove any illegal fields add/or restore any missing fields.
                datasource_config = datasourceConfigSchema.load(
                    instantiated_class.config
                )
                full_datasource_config = datasourceConfigSchema.dump(datasource_config)
            else:
                # for v2 api
                full_datasource_config = config
            if parent_class_from_config == "SimpleSqlalchemyDatasource":
                # Use the raw config here, defaults will be added in the anonymizer
                usage_stats_event_payload = anonymizer.anonymize(
                    obj=instantiated_class, name=datasource_name, config=config  # type: ignore[arg-type]
                )
            else:
                usage_stats_event_payload = anonymizer.anonymize(
                    obj=instantiated_class,
                    name=datasource_name,  # type: ignore[arg-type]
                    config=full_datasource_config,
                )

        elif parent_class_from_config is not None and parent_class_from_config.endswith(
            "Checkpoint"
        ):
            checkpoint_name: str = name or config.get("name") or "my_temp_checkpoint"
            # Roundtrip through schema validation to remove any illegal fields add/or restore any missing fields.
            checkpoint_config: Union[CheckpointConfig, dict]
            checkpoint_config = CheckpointConfig.from_commented_map(  # type: ignore[assignment]
                commented_map=config
            )
            checkpoint_config = checkpoint_config.to_json_dict()  # type: ignore[union-attr]
            checkpoint_config.update({"name": checkpoint_name})
            usage_stats_event_payload = anonymizer.anonymize(
                obj=checkpoint_config, name=checkpoint_name, config=checkpoint_config  # type: ignore[arg-type]
            )

        elif parent_class_from_config is not None and parent_class_from_config.endswith(
            "DataConnector"
        ):
            data_connector_name: str = (
                name or config.get("name") or "my_temp_data_connector"
            )
            usage_stats_event_payload = anonymizer.anonymize(
                obj=instantiated_class, name=data_connector_name, config=config  # type: ignore[arg-type]
            )

        else:
            # If class_name is not a supported type or subclass of a supported type,
            # mark it as custom with no additional information since we can't anonymize
            usage_stats_event_payload[
                "diagnostic_info"
            ] = usage_stats_event_payload.get("diagnostic_info", []) + [
                "__custom_subclass_not_core_ge__"
            ]

        return instantiated_class, usage_stats_event_payload

    def _instantiate_datasource_from_config_and_update_project_config(
        self,
        config: DatasourceConfig,
        initialize: bool = True,
        save_changes: bool = False,
    ) -> Optional[Datasource]:
        """Instantiate datasource and optionally persist datasource config to store and/or initialize datasource for use.

        Args:
            config: Config for the datasource.
            initialize: Whether to initialize the datasource or return None.
            save_changes: Whether to save the datasource config to the configured Datasource store.

        Returns:
            If initialize=True return an instantiated Datasource object, else None.
        """
        datasource: Datasource = self._data_context._instantiate_datasource_from_config_and_update_project_config(  # type: ignore[assignment,union-attr,arg-type]
            config=config,
            initialize=initialize,
            save_changes=save_changes,
        )
        self._synchronize_self_with_underlying_data_context()
        return datasource
