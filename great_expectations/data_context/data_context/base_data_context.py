from __future__ import annotations

import os
import warnings
from typing import TYPE_CHECKING, Mapping

from great_expectations.core._docs_decorators import deprecated_method_or_class
from great_expectations.data_context.data_context.abstract_data_context import (
    AbstractDataContext,
)
from great_expectations.data_context.data_context.context_factory import get_context

if TYPE_CHECKING:
    from great_expectations.alias_types import PathStr
    from great_expectations.data_context.types.base import (
        DataContextConfig,
        GXCloudConfig,
    )


@deprecated_method_or_class(
    version="0.17.10", message="Deprecated in favor of get_context"
)
def BaseDataContext(
    project_config: DataContextConfig | Mapping,
    context_root_dir: PathStr | None = None,
    runtime_environment: dict | None = None,
    cloud_mode: bool = False,
    cloud_config: GXCloudConfig | None = None,
) -> AbstractDataContext:
    """A lightweight wrapper around `get_context()`.

    While this used to be the canonical method of instantiating a DataContext before 0.15.40,
    it is now recommended to use `get_context()`.

    Usage:
        `import BaseDataContext from great_expectations.data_context`

        `my_context = BaseDataContext(<insert_your_parameters>)`

    This class implements most of the functionality of DataContext, with a few exceptions.


    Args:
        project_config: In-memory configuration for Data Context.
        context_root_dir (str or pathlib.Path): Path to directory that contains great_expectations.yml file
        runtime_environment: A dictionary of values can be passed to a DataContext when it is instantiated.
            These values will override both values from the config variables file and
            from environment variables.
        cloud_config: GX Cloud credentials (base URL, access token, and org id)
        cloud_mode: Whether to run GX in Cloud mode (default is None).
            If None, cloud mode is assumed if Cloud credentials are set up. Set to False to override.

    Returns:
        A Data Context. Either a FileDataContext, EphemeralDataContext, or
        CloudDataContext depending on environment and/or
        parameters.

    Raises:
        GXCloudConfigurationError: Cloud mode enabled, but missing configuration.


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
    # deprecated-v0.17.10
    warnings.warn(
        "DataContext and BaseDataContext are deprecated as of v0.17.10 and will be removed in v0.20. "
        "Please use gx.get_context instead.",
        DeprecationWarning,
    )

    project_data_context_config: DataContextConfig = (
        AbstractDataContext.get_or_create_data_context_config(project_config)
    )

    if context_root_dir is not None:
        context_root_dir = os.path.abspath(context_root_dir)  # noqa: PTH100
    # initialize runtime_environment as empty dict if None
    runtime_environment = runtime_environment or {}

    cloud_base_url: str | None = None
    cloud_access_token: str | None = None
    cloud_organization_id: str | None = None
    if cloud_config:
        cloud_base_url = cloud_config.base_url
        cloud_access_token = cloud_config.access_token
        cloud_organization_id = cloud_config.organization_id

    return get_context(
        project_config=project_data_context_config,
        context_root_dir=context_root_dir,
        runtime_environment=runtime_environment,
        cloud_base_url=cloud_base_url,
        cloud_access_token=cloud_access_token,
        cloud_organization_id=cloud_organization_id,
        cloud_mode=cloud_mode,
    )
