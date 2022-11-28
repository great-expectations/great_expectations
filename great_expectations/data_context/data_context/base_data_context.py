from __future__ import annotations

import os
from typing import Any, Dict, Mapping, Optional, Union

from great_expectations.core.config_peer import ConfigPeer
from great_expectations.core.usage_statistics.events import UsageStatsEvents
from great_expectations.core.usage_statistics.usage_statistics import (
    usage_statistics_enabled_method,
)
from great_expectations.data_context.data_context.abstract_data_context import (
    AbstractDataContext,
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
    GXCloudConfig,
)


class BaseDataContext(AbstractDataContext, ConfigPeer):
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

    @usage_statistics_enabled_method(
        event_name=UsageStatsEvents.DATA_CONTEXT___INIT__,
    )
    def __init__(
        self,
        project_config: Union[DataContextConfig, Mapping],
        context_root_dir: Optional[str] = None,
        runtime_environment: Optional[dict] = None,
        ge_cloud_mode: bool = False,
        ge_cloud_config: Optional[GXCloudConfig] = None,
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

        project_data_context_config: DataContextConfig = (
            AbstractDataContext.get_or_create_data_context_config(project_config)
        )

        self._ge_cloud_mode = ge_cloud_mode
        self._ge_cloud_config = ge_cloud_config
        if context_root_dir is not None:
            context_root_dir = os.path.abspath(context_root_dir)
        # initialize runtime_environment as empty dict if None
        runtime_environment = runtime_environment or {}

        # This should be replaced by a call to `get_context()` once that returns Union[EphemeralDataContext, FileDataContext, CloudDataContext]:
        self._data_context = self._init_inner_data_context(
            project_config=project_data_context_config,
            runtime_environment=runtime_environment,
            context_root_dir=context_root_dir,
        )

    def __getstate__(self) -> dict:
        """
        Necessary override to make `BaseDataContext` work with copy/pickle.
        """
        return self.__dict__

    def __setstate__(self, state: dict) -> None:
        """
        Necessary override to make `BaseDataContext` work with copy/pickle.
        """
        self.__dict__.update(state)

    def __getattr__(self, attr: str) -> Any:
        """
        Necessary to enable facade design pattern.

        If a requested attribute is a part of `BaseDataContext`, we want to leverage it.
        Otherwise, we should utilize the underlying `self._data_context` instance.
        """
        if attr in self.__class__.__dict__:
            return getattr(self, attr)
        return getattr(self._data_context, attr)

    # Chetan - 20221128 - This is temporary and will be deleted in favor of using `gx.get_context()`
    def _init_inner_data_context(
        self,
        project_config: DataContextConfig,
        runtime_environment: Dict[str, str],
        context_root_dir: Optional[str],
    ) -> Union[EphemeralDataContext, FileDataContext, CloudDataContext]:
        if self.ge_cloud_mode:
            ge_cloud_base_url: Optional[str] = None
            ge_cloud_access_token: Optional[str] = None
            ge_cloud_organization_id: Optional[str] = None
            if self.ge_cloud_config:
                ge_cloud_base_url = self.ge_cloud_config.base_url
                ge_cloud_access_token = self.ge_cloud_config.access_token
                ge_cloud_organization_id = self.ge_cloud_config.organization_id
            return CloudDataContext(
                project_config=project_config,
                runtime_environment=runtime_environment,
                context_root_dir=context_root_dir,
                ge_cloud_base_url=ge_cloud_base_url,
                ge_cloud_access_token=ge_cloud_access_token,
                ge_cloud_organization_id=ge_cloud_organization_id,
            )
        elif context_root_dir:
            return FileDataContext(
                project_config=project_config,
                context_root_dir=context_root_dir,  # type: ignore[arg-type]
                runtime_environment=runtime_environment,
            )
        return EphemeralDataContext(
            project_config=project_config,
            runtime_environment=runtime_environment,
        )

    def _init_datasource_store(self):
        """
        Only required to fulfill contract set by parent `AbstractDataContext`.

        This should never actually be called since this class is leveraging the
        underlying `self._data_context` instance for all actual behavior.
        """
        raise NotImplementedError

    def _init_variables(self):
        """
        Only required to fulfill contract set by parent `AbstractDataContext`.

        This should never actually be called since this class is leveraging the
        underlying `self._data_context` instance for all actual behavior.
        """
        raise NotImplementedError

    @property
    def config(self) -> DataContextConfig:
        """
        Required to fulfill contract set by parent `ConfigPeer`.
        """
        return self._data_context.config

    @property
    def ge_cloud_config(self) -> Optional[GXCloudConfig]:
        return self._ge_cloud_config

    @property
    def ge_cloud_mode(self) -> bool:
        return self._ge_cloud_mode
