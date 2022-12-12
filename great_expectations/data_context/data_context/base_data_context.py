from __future__ import annotations

import logging
import os
from typing import List, Mapping, Optional, Union

from ruamel.yaml import YAML

from great_expectations.checkpoint import Checkpoint
from great_expectations.core.config_peer import ConfigPeer
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.usage_statistics.events import UsageStatsEvents
from great_expectations.core.usage_statistics.usage_statistics import (
    usage_statistics_enabled_method,
)
from great_expectations.data_context.data_context.cloud_data_context import (
    CloudDataContext,
)
from great_expectations.data_context.data_context.ephemeral_data_context import (
    EphemeralDataContext,
)
from great_expectations.data_context.data_context.file_data_context import (
    FileDataContext,
)
from great_expectations.data_context.types.base import (
    DataContextConfig,
    DataContextConfigDefaults,
    DatasourceConfig,
    GXCloudConfig,
)
from great_expectations.data_context.types.resource_identifiers import (
    ConfigurationIdentifier,
    GXCloudIdentifier,
)
from great_expectations.datasource import LegacyDatasource
from great_expectations.datasource.new_datasource import BaseDatasource, Datasource

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

    UNCOMMITTED_DIRECTORIES = ["data_docs", "validations"]
    GX_UNCOMMITTED_DIR = "uncommitted"
    BASE_DIRECTORIES = [
        DataContextConfigDefaults.CHECKPOINTS_BASE_DIRECTORY.value,
        DataContextConfigDefaults.EXPECTATIONS_BASE_DIRECTORY.value,
        DataContextConfigDefaults.PLUGINS_BASE_DIRECTORY.value,
        DataContextConfigDefaults.PROFILERS_BASE_DIRECTORY.value,
        GX_UNCOMMITTED_DIR,
    ]
    GX_DIR = "great_expectations"
    GX_YML = "great_expectations.yml"  # TODO: migrate this to FileDataContext. Still needed by DataContext
    GX_EDIT_NOTEBOOK_DIR = GX_UNCOMMITTED_DIR
    DOLLAR_SIGN_ESCAPE_STRING = r"\$"

    @usage_statistics_enabled_method(
        event_name=UsageStatsEvents.DATA_CONTEXT___INIT__,
    )
    def __init__(
        self,
        project_config: Union[DataContextConfig, Mapping],
        context_root_dir: Optional[str] = None,
        runtime_environment: Optional[dict] = None,
        cloud_mode: bool = False,
        cloud_config: Optional[GXCloudConfig] = None,
        # Deprecated as of 0.15.37
        ge_cloud_mode: bool = False,
        ge_cloud_config: Optional[GXCloudConfig] = None,
    ) -> None:
        """DataContext constructor

        Args:
            context_root_dir: location to look for the ``great_expectations.yml`` file. If None, searches for the file
                based on conventions for project subdirectories.
            runtime_environment: a dictionary of config variables that
                override both those set in config_variables.yml and the environment
            cloud_mode: boolean flag that describe whether DataContext is being instantiated by Cloud
            cloud_config: config for Cloud
        Returns:
            None
        """
        # Chetan - 20221208 - not formally deprecating these values until a future date
        cloud_config = cloud_config if cloud_config is not None else ge_cloud_config
        cloud_mode = True if cloud_mode or ge_cloud_mode else False

        project_data_context_config: DataContextConfig = (
            BaseDataContext.get_or_create_data_context_config(project_config)
        )

        self._cloud_mode = cloud_mode
        self._cloud_config = cloud_config
        if context_root_dir is not None:
            context_root_dir = os.path.abspath(context_root_dir)
        self._context_root_directory = context_root_dir
        # initialize runtime_environment as empty dict if None
        runtime_environment = runtime_environment or {}
        if self._cloud_mode:
            cloud_base_url: Optional[str] = None
            cloud_access_token: Optional[str] = None
            cloud_organization_id: Optional[str] = None
            if cloud_config:
                cloud_base_url = cloud_config.base_url
                cloud_access_token = cloud_config.access_token
                cloud_organization_id = cloud_config.organization_id
            self._data_context = CloudDataContext(
                project_config=project_data_context_config,
                runtime_environment=runtime_environment,
                context_root_dir=context_root_dir,
                cloud_base_url=cloud_base_url,
                cloud_access_token=cloud_access_token,
                cloud_organization_id=cloud_organization_id,
            )
        elif self._context_root_directory:
            self._data_context = FileDataContext(  # type: ignore[assignment]
                project_config=project_data_context_config,
                context_root_dir=context_root_dir,  # type: ignore[arg-type]
                runtime_environment=runtime_environment,
            )
        else:
            self._data_context = EphemeralDataContext(  # type: ignore[assignment]
                project_config=project_data_context_config,
                runtime_environment=runtime_environment,
            )

        # NOTE: <DataContextRefactor> This will ensure that parameters set in _data_context are persisted to self.
        # It is rather clunky and we should explore other ways of ensuring that BaseDataContext has all of the
        # necessary properties / overrides
        self._synchronize_self_with_underlying_data_context()

        self._config_provider = self._data_context.config_provider
        self._variables = self._data_context.variables

        # Init validation operators
        # NOTE - 20200522 - JPC - A consistent approach to lazy loading for plugins will be useful here, harmonizing
        # the way that execution environments (AKA datasources), validation operators, site builders and other
        # plugins are built.

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

    @property
    def ge_cloud_config(self) -> Optional[GXCloudConfig]:
        return self._cloud_config

    @property
    def cloud_mode(self) -> bool:
        return self._cloud_mode

    @property
    def ge_cloud_mode(self) -> bool:
        # Deprecated 0.15.37
        return self.cloud_mode

    def _synchronize_self_with_underlying_data_context(self) -> None:
        """
        This is a helper method that only exists during the DataContext refactor that is occurring 202206.

        Until the composition-pattern is complete for BaseDataContext, we have to load the private properties from the
        private self._data_context object into properties in self

        This is a helper method that performs this loading.
        """
        # NOTE: <DataContextRefactor> This remains a rather clunky way of ensuring that all necessary parameters and
        # values from self._data_context are persisted to self.

        assert self._data_context is not None

        self._project_config = self._data_context._project_config
        self.runtime_environment = self._data_context.runtime_environment or {}
        self._config_variables = self._data_context.config_variables
        self._in_memory_instance_id = self._data_context._in_memory_instance_id
        self._stores = self._data_context._stores
        self._datasource_store = self._data_context._datasource_store
        self._data_context_id = self._data_context._data_context_id
        self._usage_statistics_handler = self._data_context._usage_statistics_handler
        self._cached_datasources = self._data_context._cached_datasources
        self._evaluation_parameter_dependencies_compiled = (
            self._data_context._evaluation_parameter_dependencies_compiled
        )
        self._evaluation_parameter_dependencies = (
            self._data_context._evaluation_parameter_dependencies
        )
        self._assistants = self._data_context._assistants

    #####
    #
    # Internal helper methods
    #
    #####

    def delete_datasource(  # type: ignore[override]
        self, datasource_name: str, save_changes: Optional[bool] = None
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

    def add_datasource(
        self,
        name: str,
        initialize: bool = True,
        save_changes: Optional[bool] = None,
        **kwargs: dict,
    ) -> Optional[Union[LegacyDatasource, BaseDatasource]]:
        """
        Add named datasource, with options to initialize (and return) the datasource and save_config.

        Current version will call super(), which preserves the `usage_statistics` decorator in the current method.
        A subsequence refactor will migrate the `usage_statistics` to parent and sibling classes.

        Args:
            name (str): Name of Datasource
            initialize (bool): Should GX add and initialize the Datasource? If true then current
                method will return initialized Datasource
            save_changes (Optional[bool]): should GX save the Datasource config?
            **kwargs Optional[dict]: Additional kwargs that define Datasource initialization kwargs

        Returns:
            Datasource that was added

        """
        new_datasource = super().add_datasource(
            name=name, initialize=initialize, save_changes=save_changes, **kwargs
        )
        self._synchronize_self_with_underlying_data_context()
        return new_datasource

    def create_expectation_suite(
        self,
        expectation_suite_name: str,
        overwrite_existing: bool = False,
        **kwargs,
    ) -> ExpectationSuite:
        """
        See `AbstractDataContext.create_expectation_suite` for more information.
        """
        suite = self._data_context.create_expectation_suite(
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
            ge_cloud_id (str): The GX Cloud ID for the Expectation Suite.

        Returns:
            An existing ExpectationSuite
        """
        if include_rendered_content is None:
            include_rendered_content = (
                self._determine_if_expectation_suite_include_rendered_content()
            )

        res = self._data_context.get_expectation_suite(
            expectation_suite_name=expectation_suite_name,
            include_rendered_content=include_rendered_content,
            ge_cloud_id=ge_cloud_id,
        )
        return res

    def delete_expectation_suite(
        self,
        expectation_suite_name: Optional[str] = None,
        ge_cloud_id: Optional[str] = None,
    ) -> bool:
        """
        See `AbstractDataContext.delete_expectation_suite` for more information.
        """
        res = self._data_context.delete_expectation_suite(
            expectation_suite_name=expectation_suite_name, ge_cloud_id=ge_cloud_id
        )
        self._synchronize_self_with_underlying_data_context()
        return res

    @property
    def root_directory(self) -> Optional[str]:
        if hasattr(self._data_context, "_context_root_directory"):
            return self._data_context._context_root_directory
        return None

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
        checkpoint = self._data_context.add_checkpoint(
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
        checkpoint._data_context = self  # type: ignore[assignment]

        self._synchronize_self_with_underlying_data_context()
        return checkpoint

    def save_expectation_suite(
        self,
        expectation_suite: ExpectationSuite,
        expectation_suite_name: Optional[str] = None,
        overwrite_existing: bool = True,
        include_rendered_content: Optional[bool] = None,
        **kwargs: Optional[dict],
    ) -> None:
        self._data_context.save_expectation_suite(
            expectation_suite,
            expectation_suite_name=expectation_suite_name,
            overwrite_existing=overwrite_existing,
            include_rendered_content=include_rendered_content,
            **kwargs,
        )

    def list_checkpoints(self) -> Union[List[str], List[ConfigurationIdentifier]]:
        return self._data_context.list_checkpoints()

    def list_profilers(self) -> Union[List[str], List[ConfigurationIdentifier]]:
        return self._data_context.list_profilers()

    def list_expectation_suites(
        self,
    ) -> Optional[Union[List[str], List[GXCloudIdentifier]]]:
        """
        See parent 'AbstractDataContext.list_expectation_suites()` for more information.
        """
        return self._data_context.list_expectation_suites()

    def list_expectation_suite_names(self) -> List[str]:
        """
        See parent 'AbstractDataContext.list_expectation_suite_names()` for more information.
        """
        return self._data_context.list_expectation_suite_names()

    def _instantiate_datasource_from_config_and_update_project_config(
        self,
        config: DatasourceConfig,
        initialize: bool,
        save_changes: bool,
    ) -> Optional[Datasource]:
        """Instantiate datasource and optionally persist datasource config to store and/or initialize datasource for use.

        Args:
            config: Config for the datasource.
            initialize: Whether to initialize the datasource or return None.
            save_changes: Whether to save the datasource config to the configured Datasource store.

        Returns:
            If initialize=True return an instantiated Datasource object, else None.
        """
        datasource = self._data_context._instantiate_datasource_from_config_and_update_project_config(
            config=config,
            initialize=initialize,
            save_changes=save_changes,
        )
        self._synchronize_self_with_underlying_data_context()
        return datasource

    def _determine_key_for_profiler_save(
        self, name: str, id: Optional[str]
    ) -> Union[ConfigurationIdentifier, GXCloudIdentifier]:
        return self._data_context._determine_key_for_profiler_save(name=name, id=id)
