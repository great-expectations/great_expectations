from __future__ import annotations

import logging
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Literal,
    Mapping,
    Optional,
    Type,
    overload,
)

import great_expectations.exceptions as gx_exceptions
from great_expectations._docs_decorators import public_api
from great_expectations.exceptions import (
    GXCloudConfigurationError,
)
from great_expectations.exceptions.exceptions import DataContextRequiredError

logger = logging.getLogger(__name__)


if TYPE_CHECKING:
    # needed until numpy min version 1.20
    from typing_extensions import TypeAlias

    from great_expectations.alias_types import PathStr
    from great_expectations.core.config_provider import _ConfigurationProvider
    from great_expectations.data_context import (
        AbstractDataContext,
        CloudDataContext,
        EphemeralDataContext,
        FileDataContext,
    )
    from great_expectations.data_context.store import (
        ExpectationsStore,
        ValidationResultsStore,
    )
    from great_expectations.data_context.store.checkpoint_store import CheckpointStore
    from great_expectations.data_context.store.validation_definition_store import (
        ValidationDefinitionStore,
    )
    from great_expectations.data_context.types.base import DataContextConfig
    from great_expectations.datasource.datasource_dict import DatasourceDict
    from great_expectations.datasource.fluent.batch_request import BatchRequest
    from great_expectations.validator.validator import Validator

ContextModes: TypeAlias = Literal["file", "cloud", "ephemeral"]


class ProjectManager:
    """Singleton class to manage projects in the global namespace."""

    __project: AbstractDataContext | None

    def __init__(self):
        self.__project = None

    def get_project(  # noqa: PLR0913
        self,
        project_config: DataContextConfig | Mapping | None = None,
        context_root_dir: PathStr | None = None,
        project_root_dir: PathStr | None = None,
        runtime_environment: dict | None = None,
        cloud_base_url: str | None = None,
        cloud_access_token: str | None = None,
        cloud_organization_id: str | None = None,
        cloud_mode: bool | None = None,
        mode: ContextModes | None = None,
    ) -> AbstractDataContext:
        self.__project = self._build_context(
            project_config=project_config,
            context_root_dir=context_root_dir,
            project_root_dir=project_root_dir,
            runtime_environment=runtime_environment,
            cloud_base_url=cloud_base_url,
            cloud_access_token=cloud_access_token,
            cloud_organization_id=cloud_organization_id,
            cloud_mode=cloud_mode,
            mode=mode,
        )
        return self.__project

    def set_project(self, project: AbstractDataContext | None) -> None:
        self.__project = project

    @property
    def _project(self) -> AbstractDataContext:
        if not self.__project:
            raise DataContextRequiredError()
        return self.__project

    def get_expectations_store(self) -> ExpectationsStore:
        return self._project.expectations_store

    def get_checkpoints_store(self) -> CheckpointStore:
        return self._project.checkpoint_store

    def get_validation_results_store(self) -> ValidationResultsStore:
        return self._project.validation_results_store

    def get_validation_definition_store(self) -> ValidationDefinitionStore:
        return self._project.validation_definition_store

    def get_datasources(self) -> DatasourceDict:
        return self._project.data_sources.all()

    def get_validator(self, batch_request: BatchRequest) -> Validator:
        return self._project.get_validator(batch_request=batch_request)

    def is_using_cloud(self) -> bool:
        from great_expectations.data_context import CloudDataContext

        return isinstance(self._project, CloudDataContext)

    def build_data_docs(
        self,
        site_names: list[str] | None = None,
        resource_identifiers: list | None = None,
        dry_run: bool = False,
        build_index: bool = True,
    ) -> dict:
        return self._project.build_data_docs(
            site_names=site_names,
            resource_identifiers=resource_identifiers,
            dry_run=dry_run,
            build_index=build_index,
        )

    def get_docs_sites_urls(
        self,
        resource_identifier: Any | None = None,
        site_name: str | None = None,
        only_if_exists: bool = True,
        site_names: list[str] | None = None,
    ) -> list[dict[str, Optional[str]]]:
        return self._project.get_docs_sites_urls(
            resource_identifier=resource_identifier,
            site_name=site_name,
            only_if_exists=only_if_exists,
            site_names=site_names,
        )

    def get_config_provider(self) -> _ConfigurationProvider:
        return self._project.config_provider

    def _build_context(  # noqa: PLR0913
        self,
        project_config: DataContextConfig | Mapping | None = None,
        context_root_dir: PathStr | None = None,
        project_root_dir: PathStr | None = None,
        runtime_environment: dict | None = None,
        cloud_base_url: str | None = None,
        cloud_access_token: str | None = None,
        cloud_organization_id: str | None = None,
        cloud_mode: bool | None = None,
        mode: ContextModes | None = None,
    ) -> AbstractDataContext:
        project_config = self._prepare_project_config(project_config)

        param_lookup: dict[ContextModes | None, dict] = {
            "ephemeral": dict(
                project_config=project_config,
                runtime_environment=runtime_environment,
            ),
            "file": dict(
                project_config=project_config,
                context_root_dir=context_root_dir,
                project_root_dir=project_root_dir or Path.cwd(),
                runtime_environment=runtime_environment,
            ),
            "cloud": dict(
                project_config=project_config,
                context_root_dir=context_root_dir,
                project_root_dir=project_root_dir,
                runtime_environment=runtime_environment,
                cloud_base_url=cloud_base_url,
                cloud_access_token=cloud_access_token,
                cloud_organization_id=cloud_organization_id,
            ),
            None: dict(
                project_config=project_config,
                context_root_dir=context_root_dir,
                project_root_dir=project_root_dir,
                runtime_environment=runtime_environment,
                cloud_base_url=cloud_base_url,
                cloud_access_token=cloud_access_token,
                cloud_organization_id=cloud_organization_id,
                cloud_mode=cloud_mode,
            ),
        }
        try:
            kwargs = param_lookup[mode]
        except KeyError:
            raise ValueError(f"Unknown mode {mode}. Please choose one of: ephemeral, file, cloud.")  # noqa: TRY003

        from great_expectations.data_context.data_context import (
            AbstractDataContext,
            CloudDataContext,
            EphemeralDataContext,
            FileDataContext,
        )

        expected_ctx_types: dict[
            ContextModes | None,
            Type[CloudDataContext]
            | Type[EphemeralDataContext]
            | Type[FileDataContext]
            | Type[AbstractDataContext],
        ] = {
            "ephemeral": EphemeralDataContext,
            "file": FileDataContext,
            "cloud": CloudDataContext,
            None: AbstractDataContext,  # type: ignore[type-abstract]
        }

        context_fn_map: dict[ContextModes | None, Callable] = {
            "ephemeral": self._get_ephemeral_context,
            "file": self._get_file_context,
            "cloud": self._get_cloud_context,
            None: self._get_default_context,
        }

        context_fn = context_fn_map[mode]
        context = context_fn(**kwargs)

        expected_type = expected_ctx_types[mode]
        if not isinstance(context, expected_type):
            # example I want an ephemeral context but the presence of a GX_CLOUD env var gives me a cloud context  # noqa: E501
            # this kind of thing should not be possible but there may be some edge cases
            raise ValueError(  # noqa: TRY003, TRY004
                f"Provided mode {mode} returned context of type {type(context).__name__} instead of {expected_type.__name__}; please check your input arguments."  # noqa: E501
            )

        return context

    def _get_default_context(  # noqa: PLR0913
        self,
        project_config: DataContextConfig | None = None,
        context_root_dir: PathStr | None = None,
        project_root_dir: PathStr | None = None,
        runtime_environment: dict | None = None,
        cloud_base_url: str | None = None,
        cloud_access_token: str | None = None,
        cloud_organization_id: str | None = None,
        cloud_mode: bool | None = None,
    ) -> AbstractDataContext:
        """Infer which type of DataContext a user wants based on available parameters."""
        # First, check for GX Cloud conditions
        cloud_context = self._get_cloud_context(
            project_config=project_config,
            context_root_dir=context_root_dir,
            project_root_dir=project_root_dir,
            runtime_environment=runtime_environment,
            cloud_mode=cloud_mode,
            cloud_base_url=cloud_base_url,
            cloud_access_token=cloud_access_token,
            cloud_organization_id=cloud_organization_id,
        )

        if cloud_context:
            return cloud_context

        # Second, check for a context_root_dir to determine if using a filesystem
        file_context = self._get_file_context(
            project_config=project_config,
            context_root_dir=context_root_dir,
            project_root_dir=project_root_dir,
            runtime_environment=runtime_environment,
        )
        if file_context:
            return file_context

        # Finally, default to ephemeral
        return self._get_ephemeral_context(
            project_config=project_config,
            runtime_environment=runtime_environment,
        )

    def _prepare_project_config(
        self,
        project_config: DataContextConfig | Mapping | None,
    ) -> DataContextConfig | None:
        from great_expectations.data_context.data_context import AbstractDataContext
        from great_expectations.data_context.types.base import DataContextConfig

        # If available and applicable, convert project_config mapping into a rich config type
        if project_config:
            project_config = AbstractDataContext.get_or_create_data_context_config(project_config)
        assert project_config is None or isinstance(
            project_config, DataContextConfig
        ), "project_config must be of type Optional[DataContextConfig]"

        return project_config

    def _get_cloud_context(  # noqa: PLR0913
        self,
        project_config: DataContextConfig | Mapping | None = None,
        context_root_dir: PathStr | None = None,
        project_root_dir: PathStr | None = None,
        runtime_environment: dict | None = None,
        cloud_base_url: str | None = None,
        cloud_access_token: str | None = None,
        cloud_organization_id: str | None = None,
        cloud_mode: bool | None = None,
    ) -> CloudDataContext | None:
        from great_expectations.data_context.data_context import CloudDataContext

        config_available = CloudDataContext.is_cloud_config_available(
            cloud_base_url=cloud_base_url,
            cloud_access_token=cloud_access_token,
            cloud_organization_id=cloud_organization_id,
        )

        # If config available and not explicitly disabled
        if config_available and cloud_mode is not False:
            return CloudDataContext(
                project_config=project_config,
                runtime_environment=runtime_environment,
                context_root_dir=context_root_dir,
                project_root_dir=project_root_dir,
                cloud_base_url=cloud_base_url,
                cloud_access_token=cloud_access_token,
                cloud_organization_id=cloud_organization_id,
            )

        if cloud_mode and not config_available:
            raise GXCloudConfigurationError(  # noqa: TRY003
                "GX Cloud Mode enabled, but missing env vars: GX_CLOUD_ORGANIZATION_ID, GX_CLOUD_ACCESS_TOKEN"  # noqa: E501
            )

        return None

    def _get_file_context(
        self,
        project_config: DataContextConfig | None = None,
        context_root_dir: PathStr | None = None,
        project_root_dir: PathStr | None = None,
        runtime_environment: dict | None = None,
    ) -> FileDataContext | None:
        from great_expectations.data_context.data_context import FileDataContext

        try:
            return FileDataContext(
                project_config=project_config,
                context_root_dir=context_root_dir,
                project_root_dir=project_root_dir,
                runtime_environment=runtime_environment,
            )
        except gx_exceptions.ConfigNotFoundError:
            logger.info("Could not find local file-backed GX project")
            return None

    def _get_ephemeral_context(
        self,
        project_config: DataContextConfig | None = None,
        runtime_environment: dict | None = None,
    ) -> EphemeralDataContext:
        from great_expectations.data_context.data_context import EphemeralDataContext
        from great_expectations.data_context.types.base import (
            DataContextConfig,
            InMemoryStoreBackendDefaults,
        )

        if not project_config:
            project_config = DataContextConfig(
                store_backend_defaults=InMemoryStoreBackendDefaults(init_temp_docs_sites=True)
            )

        return EphemeralDataContext(
            project_config=project_config,
            runtime_environment=runtime_environment,
        )


# global singleton
project_manager = ProjectManager()


@overload
def get_context(
    project_config: DataContextConfig | Mapping | None = ...,
    context_root_dir: None = ...,
    project_root_dir: None = ...,
    runtime_environment: dict | None = ...,
    cloud_base_url: None = ...,
    cloud_access_token: None = ...,
    cloud_organization_id: None = ...,
    cloud_mode: Literal[False] | None = ...,
    mode: Literal["ephemeral"] = ...,
) -> EphemeralDataContext: ...


@overload
def get_context(
    project_config: DataContextConfig | Mapping | None = ...,
    context_root_dir: PathStr = ...,  # If context_root_dir is provided, project_root_dir shouldn't be  # noqa: E501
    project_root_dir: None = ...,
    runtime_environment: dict | None = ...,
    cloud_base_url: None = ...,
    cloud_access_token: None = ...,
    cloud_organization_id: None = ...,
    cloud_mode: Literal[False] | None = ...,
) -> FileDataContext: ...


@overload
def get_context(
    project_config: DataContextConfig | Mapping | None = ...,
    context_root_dir: None = ...,
    project_root_dir: PathStr = ...,  # If project_root_dir is provided, context_root_dir shouldn't be  # noqa: E501
    runtime_environment: dict | None = ...,
    cloud_base_url: None = ...,
    cloud_access_token: None = ...,
    cloud_organization_id: None = ...,
    cloud_mode: Literal[False] | None = ...,
    mode: Literal["file"] | None = ...,
) -> FileDataContext: ...


@overload
def get_context(
    project_config: DataContextConfig | Mapping | None = ...,
    context_root_dir: None = ...,
    project_root_dir: None = ...,
    runtime_environment: dict | None = ...,
    cloud_base_url: str | None = ...,
    cloud_access_token: str | None = ...,
    cloud_organization_id: str | None = ...,
    cloud_mode: Literal[True] = ...,
    mode: Literal["cloud"] | None = ...,
) -> CloudDataContext: ...


@overload
def get_context(
    project_config: DataContextConfig | Mapping | None = ...,
    context_root_dir: PathStr | None = ...,
    project_root_dir: PathStr | None = ...,
    runtime_environment: dict | None = ...,
    cloud_base_url: str | None = ...,
    cloud_access_token: str | None = ...,
    cloud_organization_id: str | None = ...,
    cloud_mode: bool | None = ...,
    mode: None = ...,
) -> EphemeralDataContext | FileDataContext | CloudDataContext: ...


@public_api
def get_context(  # noqa: PLR0913
    project_config: DataContextConfig | Mapping | None = None,
    context_root_dir: PathStr | None = None,
    project_root_dir: PathStr | None = None,
    runtime_environment: dict | None = None,
    cloud_base_url: str | None = None,
    cloud_access_token: str | None = None,
    cloud_organization_id: str | None = None,
    cloud_mode: bool | None = None,
    mode: ContextModes | None = None,
) -> AbstractDataContext:
    """Method to return the appropriate Data Context depending on parameters and environment.

    Usage:
        `import great_expectations as gx`

        `my_context = gx.get_context(<insert_your_parameters>)`

    This method returns the appropriate Data Context based on which parameters you've passed and / or your environment configuration:

    - FileDataContext: Configuration stored in a file.
    - EphemeralDataContext: Configuration passed in at runtime.
    - CloudDataContext: Configuration stored in Great Expectations Cloud.

    Read on for more details about each of the Data Context types:

    **FileDataContext:** A Data Context configured via a yaml file. Returned by default if you have no cloud configuration set up and pass no parameters. If you pass context_root_dir, we will look for a great_expectations.yml configuration there. If not we will look at the following locations:

    - Path defined in a GX_HOME environment variable.
    - The current directory.
    - Parent directories of the current directory.

    Relevant parameters

    - project_root_dir: Provide the project root where a GX project exists or will be scaffolded (contains the context root).
    - context_root_dir: Provide an alternative directory to look for GX config.
    - project_config: Optionally override the configuration on disk - only if `context_root_dir` is also provided.
    - runtime_environment: Optionally override specific configuration values.

    **EphemeralDataContext:** A temporary, in-memory Data Context typically used in a pipeline. The default if you pass in only a project_config and have no cloud configuration set up.

    Relevant parameters

    - project_config: Used to configure the Data Context.
    - runtime_environment: Optionally override specific configuration values.

    **CloudDataContext:** A Data Context whose configuration comes from Great Expectations Cloud. The default if you have a cloud configuration set up. Pass `cloud_mode=False` if you have a cloud configuration set up and you do not wish to create a CloudDataContext.

    Cloud configuration can be set up by passing `cloud_*` parameters to `gx.get_context()`, configuring cloud environment variables, or in a great_expectations.conf file.

    Relevant parameters

    - cloud_base_url: Override env var or great_expectations.conf file.
    - cloud_access_token: Override env var or great_expectations.conf file.
    - cloud_organization_id: Override env var or great_expectations.conf file.
    - cloud_mode: Set to True or False to explicitly enable/disable cloud mode.
    - project_config: Optionally override the cloud configuration.
    - runtime_environment: Optionally override specific configuration values.

    Args:
        project_config: In-memory configuration for Data Context.
        context_root_dir (str or pathlib.Path): Path to directory that contains great_expectations.yml file
        project_root_dir (str or pathlib.Path): Path to project root (contains context root with GX config)
        runtime_environment: A dictionary of values can be passed to a DataContext when it is instantiated.
            These values will override both values from the config variables file and
            from environment variables.
        cloud_base_url: url for GX Cloud endpoint.
        cloud_access_token: access_token for GX Cloud account.
        cloud_organization_id: org_id for GX Cloud account.
        cloud_mode: whether to run GX in Cloud mode (default is None).
            If None, cloud mode is assumed if cloud credentials are set up. Set to False to override.
        mode: which mode to use. One of: ephemeral, file, cloud.
            Note: if mode is specified, cloud_mode is ignored.

    Returns:
        A Data Context. Either a FileDataContext, EphemeralDataContext, or
        CloudDataContext depending on environment and/or
        parameters.

    Raises:
        GXCloudConfigurationError: Cloud mode enabled, but missing configuration.
    """  # noqa: E501
    return project_manager.get_project(
        project_config=project_config,
        context_root_dir=context_root_dir,
        project_root_dir=project_root_dir,
        runtime_environment=runtime_environment,
        cloud_base_url=cloud_base_url,
        cloud_access_token=cloud_access_token,
        cloud_organization_id=cloud_organization_id,
        cloud_mode=cloud_mode,
        mode=mode,
    )


set_context = project_manager.set_project
