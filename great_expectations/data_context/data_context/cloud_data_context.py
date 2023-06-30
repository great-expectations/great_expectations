from __future__ import annotations

import logging
import os
import warnings
from typing import (
    TYPE_CHECKING,
    Callable,
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Union,
    cast,
    overload,
)

import great_expectations.exceptions as gx_exceptions
from great_expectations import __version__
from great_expectations.checkpoint.checkpoint import Checkpoint
from great_expectations.core import ExpectationSuite
from great_expectations.core._docs_decorators import public_api
from great_expectations.core.config_provider import (
    _CloudConfigurationProvider,
    _ConfigurationProvider,
)
from great_expectations.core.http import create_session
from great_expectations.core.serializer import JsonConfigSerializer
from great_expectations.data_context._version_checker import _VersionChecker
from great_expectations.data_context.cloud_constants import (
    CLOUD_DEFAULT_BASE_URL,
    GXCloudEnvironmentVariable,
    GXCloudRESTResource,
)
from great_expectations.data_context.data_context.serializable_data_context import (
    SerializableDataContext,
)
from great_expectations.data_context.data_context_variables import (
    CloudDataContextVariables,
)
from great_expectations.data_context.types.base import (
    DEFAULT_USAGE_STATISTICS_URL,
    CheckpointConfig,
    DataContextConfig,
    DataContextConfigDefaults,
    GXCloudConfig,
    datasourceConfigSchema,
)
from great_expectations.data_context.types.refs import GXCloudResourceRef
from great_expectations.data_context.types.resource_identifiers import GXCloudIdentifier
from great_expectations.data_context.util import instantiate_class_from_config
from great_expectations.datasource.fluent import Datasource as FluentDatasource
from great_expectations.exceptions.exceptions import DataContextError, StoreBackendError
from great_expectations.rule_based_profiler.rule_based_profiler import RuleBasedProfiler

if TYPE_CHECKING:
    from great_expectations.alias_types import PathStr
    from great_expectations.checkpoint.configurator import ActionDict
    from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
    from great_expectations.data_context.store.datasource_store import DatasourceStore
    from great_expectations.data_context.types.resource_identifiers import (
        ConfigurationIdentifier,
        ExpectationSuiteIdentifier,
    )
    from great_expectations.datasource import LegacyDatasource
    from great_expectations.datasource.new_datasource import BaseDatasource
    from great_expectations.render.renderer.site_builder import SiteBuilder
    from great_expectations.validator.validator import Validator

logger = logging.getLogger(__name__)


def _extract_fluent_datasources(config_dict: dict) -> dict:
    """
    When pulling from cloud config, FDS and BSD are nested under the `"datasources" key`.
    We need to extract the fluent datasources otherwise the data context will attempt eager config
    substitutions and other inappropriate operations.
    """
    datasources = config_dict.get("datasources", {})
    fds_names: list[str] = []
    for ds_name, ds in datasources.items():
        if "type" in ds:
            fds_names.append(ds_name)
    return {name: datasources.pop(name) for name in fds_names}


@public_api
class CloudDataContext(SerializableDataContext):
    """Subclass of AbstractDataContext that contains functionality necessary to work in a GX Cloud-backed environment."""

    def __init__(  # noqa: PLR0913
        self,
        project_config: Optional[Union[DataContextConfig, Mapping]] = None,
        context_root_dir: Optional[PathStr] = None,
        runtime_environment: Optional[dict] = None,
        cloud_base_url: Optional[str] = None,
        cloud_access_token: Optional[str] = None,
        cloud_organization_id: Optional[str] = None,
        # <GX_RENAME> Deprecated as of 0.15.37
        ge_cloud_base_url: Optional[str] = None,
        ge_cloud_access_token: Optional[str] = None,
        ge_cloud_organization_id: Optional[str] = None,
    ) -> None:
        """
        CloudDataContext constructor

        Args:
            project_config (DataContextConfig): config for CloudDataContext
            runtime_environment (dict):  a dictionary of config variables that override both those set in
                config_variables.yml and the environment
            cloud_config (GXCloudConfig): GXCloudConfig corresponding to current CloudDataContext
        """
        # Chetan - 20221208 - not formally deprecating these values until a future date
        (
            cloud_base_url,
            cloud_access_token,
            cloud_organization_id,
        ) = CloudDataContext._resolve_cloud_args(
            cloud_base_url=cloud_base_url,
            cloud_access_token=cloud_access_token,
            cloud_organization_id=cloud_organization_id,
            ge_cloud_base_url=ge_cloud_base_url,
            ge_cloud_access_token=ge_cloud_access_token,
            ge_cloud_organization_id=ge_cloud_organization_id,
        )

        self._check_if_latest_version()
        self._cloud_config = self.get_cloud_config(
            cloud_base_url=cloud_base_url,
            cloud_access_token=cloud_access_token,
            cloud_organization_id=cloud_organization_id,
        )
        self._context_root_directory = self.determine_context_root_directory(
            context_root_dir
        )
        self._project_config = self._init_project_config(project_config)

        super().__init__(
            context_root_dir=self._context_root_directory,
            runtime_environment=runtime_environment,
        )

    def _check_if_latest_version(self) -> None:
        checker = _VersionChecker(__version__)
        checker.check_if_using_latest_gx()

    def _init_project_config(
        self, project_config: Optional[Union[DataContextConfig, Mapping]]
    ) -> DataContextConfig:
        if project_config is None:
            project_config = self.retrieve_data_context_config_from_cloud(
                cloud_config=self._cloud_config,
            )

        project_data_context_config = (
            CloudDataContext.get_or_create_data_context_config(project_config)
        )

        return self._apply_global_config_overrides(config=project_data_context_config)

    @staticmethod
    def _resolve_cloud_args(  # noqa: PLR0913
        cloud_base_url: Optional[str] = None,
        cloud_access_token: Optional[str] = None,
        cloud_organization_id: Optional[str] = None,
        # <GX_RENAME> Deprecated as of 0.15.37
        ge_cloud_base_url: Optional[str] = None,
        ge_cloud_access_token: Optional[str] = None,
        ge_cloud_organization_id: Optional[str] = None,
    ) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        cloud_base_url = (
            cloud_base_url if cloud_base_url is not None else ge_cloud_base_url
        )
        cloud_access_token = (
            cloud_access_token
            if cloud_access_token is not None
            else ge_cloud_access_token
        )
        cloud_organization_id = (
            cloud_organization_id
            if cloud_organization_id is not None
            else ge_cloud_organization_id
        )
        return cloud_base_url, cloud_access_token, cloud_organization_id

    def _register_providers(self, config_provider: _ConfigurationProvider) -> None:
        """
        To ensure that Cloud credentials are accessible downstream, we want to ensure that
        we register a CloudConfigurationProvider.

        Note that it is registered last as it takes the highest precedence.
        """
        super()._register_providers(config_provider)
        config_provider.register_provider(
            _CloudConfigurationProvider(self._cloud_config)
        )

    @classmethod
    def is_cloud_config_available(
        cls,
        cloud_base_url: Optional[str] = None,
        cloud_access_token: Optional[str] = None,
        cloud_organization_id: Optional[str] = None,
    ) -> bool:
        """
        Helper method called by gx.get_context() method to determine whether all the information needed
        to build a cloud_config is available.

        If provided as explicit arguments, cloud_base_url, cloud_access_token and
        cloud_organization_id will use runtime values instead of environment variables or conf files.

        If any of the values are missing, the method will return False. It will return True otherwise.

        Args:
            cloud_base_url: Optional, you may provide this alternatively via
                environment variable GX_CLOUD_BASE_URL or within a config file.
            cloud_access_token: Optional, you may provide this alternatively
                via environment variable GX_CLOUD_ACCESS_TOKEN or within a config file.
            cloud_organization_id: Optional, you may provide this alternatively
                via environment variable GX_CLOUD_ORGANIZATION_ID or within a config file.

        Returns:
            bool: Is all the information needed to build a cloud_config is available?
        """
        cloud_config_dict = cls._get_cloud_config_dict(
            cloud_base_url=cloud_base_url,
            cloud_access_token=cloud_access_token,
            cloud_organization_id=cloud_organization_id,
        )
        return all(val for val in cloud_config_dict.values())

    @classmethod
    def determine_context_root_directory(
        cls, context_root_dir: Optional[PathStr]
    ) -> str:
        if context_root_dir is None:
            context_root_dir = os.getcwd()  # noqa: PTH109
            logger.info(
                f'context_root_dir was not provided - defaulting to current working directory "'
                f'{context_root_dir}".'
            )
        return os.path.abspath(  # noqa: PTH100
            os.path.expanduser(context_root_dir)  # noqa: PTH111
        )

    @classmethod
    def retrieve_data_context_config_from_cloud(
        cls, cloud_config: GXCloudConfig
    ) -> DataContextConfig:
        """
        Utilizes the GXCloudConfig instantiated in the constructor to create a request to the Cloud API.
        Given proper authorization, the request retrieves a data context config that is pre-populated with
        GX objects specific to the user's Cloud environment (datasources, data connectors, etc).

        Please note that substitution for ${VAR} variables is performed in GX Cloud before being sent
        over the wire.

        :return: the configuration object retrieved from the Cloud API
        """
        base_url = cloud_config.base_url
        organization_id = cloud_config.organization_id
        cloud_url = (
            f"{base_url}/organizations/{organization_id}/data-context-configuration"
        )

        session = create_session(access_token=cloud_config.access_token)

        response = session.get(cloud_url)
        if response.status_code != 200:  # noqa: PLR2004
            raise gx_exceptions.GXCloudError(
                f"Bad request made to GX Cloud; {response.text}", response=response
            )
        config = response.json()
        config["fluent_datasources"] = _extract_fluent_datasources(config)
        return DataContextConfig(**config)

    @classmethod
    def get_cloud_config(
        cls,
        cloud_base_url: Optional[str] = None,
        cloud_access_token: Optional[str] = None,
        cloud_organization_id: Optional[str] = None,
    ) -> GXCloudConfig:
        """
        Build a GXCloudConfig object. Config attributes are collected from any combination of args passed in at
        runtime, environment variables, or a global great_expectations.conf file (in order of precedence).

        If provided as explicit arguments, cloud_base_url, cloud_access_token and
        cloud_organization_id will use runtime values instead of environment variables or conf files.

        Args:
            cloud_base_url: Optional, you may provide this alternatively via
                environment variable GX_CLOUD_BASE_URL or within a config file.
            cloud_access_token: Optional, you may provide this alternatively
                via environment variable GX_CLOUD_ACCESS_TOKEN or within a config file.
            cloud_organization_id: Optional, you may provide this alternatively
                via environment variable GX_CLOUD_ORGANIZATION_ID or within a config file.

        Returns:
            GXCloudConfig

        Raises:
            GXCloudError if a GX Cloud variable is missing
        """
        cloud_config_dict = cls._get_cloud_config_dict(
            cloud_base_url=cloud_base_url,
            cloud_access_token=cloud_access_token,
            cloud_organization_id=cloud_organization_id,
        )

        missing_keys = []
        for key, val in cloud_config_dict.items():
            if not val:
                missing_keys.append(key)
        if len(missing_keys) > 0:
            missing_keys_str = [f'"{key}"' for key in missing_keys]
            global_config_path_str = [
                f'"{path}"' for path in super().GLOBAL_CONFIG_PATHS
            ]
            raise DataContextError(
                f"{(', ').join(missing_keys_str)} arg(s) required for ge_cloud_mode but neither provided nor found in "
                f"environment or in global configs ({(', ').join(global_config_path_str)})."
            )

        base_url = cloud_config_dict[GXCloudEnvironmentVariable.BASE_URL]
        assert base_url is not None
        access_token = cloud_config_dict[GXCloudEnvironmentVariable.ACCESS_TOKEN]
        organization_id = cloud_config_dict[GXCloudEnvironmentVariable.ORGANIZATION_ID]

        return GXCloudConfig(
            base_url=base_url,
            access_token=access_token,
            organization_id=organization_id,
        )

    @classmethod
    def _get_cloud_config_dict(
        cls,
        cloud_base_url: Optional[str] = None,
        cloud_access_token: Optional[str] = None,
        cloud_organization_id: Optional[str] = None,
    ) -> Dict[GXCloudEnvironmentVariable, Optional[str]]:
        cloud_base_url = (
            cloud_base_url
            or cls._get_cloud_env_var(
                primary_environment_variable=GXCloudEnvironmentVariable.BASE_URL,
                deprecated_environment_variable=GXCloudEnvironmentVariable._OLD_BASE_URL,
                conf_file_section="ge_cloud_config",
                conf_file_option="base_url",
            )
            or CLOUD_DEFAULT_BASE_URL
        )
        cloud_organization_id = cloud_organization_id or cls._get_cloud_env_var(
            primary_environment_variable=GXCloudEnvironmentVariable.ORGANIZATION_ID,
            deprecated_environment_variable=GXCloudEnvironmentVariable._OLD_ORGANIZATION_ID,
            conf_file_section="ge_cloud_config",
            conf_file_option="organization_id",
        )
        cloud_access_token = cloud_access_token or cls._get_cloud_env_var(
            primary_environment_variable=GXCloudEnvironmentVariable.ACCESS_TOKEN,
            deprecated_environment_variable=GXCloudEnvironmentVariable._OLD_ACCESS_TOKEN,
            conf_file_section="ge_cloud_config",
            conf_file_option="access_token",
        )
        return {
            GXCloudEnvironmentVariable.BASE_URL: cloud_base_url,
            GXCloudEnvironmentVariable.ORGANIZATION_ID: cloud_organization_id,
            GXCloudEnvironmentVariable.ACCESS_TOKEN: cloud_access_token,
        }

    @classmethod
    def _get_cloud_env_var(
        cls,
        primary_environment_variable: GXCloudEnvironmentVariable,
        deprecated_environment_variable: GXCloudEnvironmentVariable,
        conf_file_section: str,
        conf_file_option: str,
    ) -> Optional[str]:
        """
        Utility to gradually deprecate environment variables prefixed with `GE`.

        This method is aimed to initially attempt retrieval with the `GX` prefix
        and only attempt to grab the deprecated value if unsuccessful.
        """
        val = cls._get_global_config_value(
            environment_variable=primary_environment_variable,
            conf_file_section=conf_file_section,
            conf_file_option=conf_file_option,
        )
        if val:
            return val
        return cls._get_global_config_value(
            environment_variable=deprecated_environment_variable,
            conf_file_section=conf_file_section,
            conf_file_option=conf_file_option,
        )

    def _init_datasource_store(self) -> DatasourceStore:
        from great_expectations.data_context.store.datasource_store import (
            DatasourceStore,
        )
        from great_expectations.data_context.store.gx_cloud_store_backend import (
            GXCloudStoreBackend,
        )

        # Never explicitly referenced but adheres
        # to the convention set by other internal Stores
        store_name = DataContextConfigDefaults.DEFAULT_DATASOURCE_STORE_NAME.value
        store_backend: dict = {"class_name": GXCloudStoreBackend.__name__}
        runtime_environment: dict = {
            "root_directory": self.root_directory,
            "ge_cloud_credentials": self.ge_cloud_config.to_dict(),  # type: ignore[union-attr]
            "ge_cloud_resource_type": GXCloudRESTResource.DATASOURCE,
            "ge_cloud_base_url": self.ge_cloud_config.base_url,  # type: ignore[union-attr]
        }

        datasource_store = DatasourceStore(
            store_name=store_name,
            store_backend=store_backend,
            runtime_environment=runtime_environment,
            serializer=JsonConfigSerializer(schema=datasourceConfigSchema),
        )
        return datasource_store

    def list_expectation_suite_names(self) -> List[str]:
        """
        Lists the available expectation suite names. If in ge_cloud_mode, a list of
        GX Cloud ids is returned instead.
        """
        return [suite_key.resource_name for suite_key in self.list_expectation_suites() if suite_key.resource_name]  # type: ignore[union-attr]

    @property
    def ge_cloud_config(self) -> Optional[GXCloudConfig]:
        return self._cloud_config

    def _init_variables(self) -> CloudDataContextVariables:
        ge_cloud_base_url: str = self._cloud_config.base_url
        ge_cloud_organization_id: str = self._cloud_config.organization_id  # type: ignore[assignment]
        ge_cloud_access_token: str = self._cloud_config.access_token

        variables = CloudDataContextVariables(
            config=self._project_config,
            config_provider=self.config_provider,
            ge_cloud_base_url=ge_cloud_base_url,
            ge_cloud_organization_id=ge_cloud_organization_id,
            ge_cloud_access_token=ge_cloud_access_token,
        )
        return variables

    def _construct_data_context_id(self) -> str:
        """
        Choose the id of the currently-configured expectations store, if available and a persistent store.
        If not, it should choose the id stored in DataContextConfig.
        Returns:
            UUID to use as the data_context_id
        """
        return self.ge_cloud_config.organization_id  # type: ignore[return-value,union-attr]

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
            config = self.config

        substitutions: dict = self.config_provider.get_values()

        cloud_config_variable_defaults = {
            "plugins_directory": self._normalize_absolute_or_relative_path(
                path=DataContextConfigDefaults.DEFAULT_PLUGINS_DIRECTORY.value
            ),
            "usage_statistics_url": DEFAULT_USAGE_STATISTICS_URL,
        }
        for config_variable, value in cloud_config_variable_defaults.items():
            if substitutions.get(config_variable) is None:
                logger.info(
                    f'Config variable "{config_variable}" was not found in environment or global config ('
                    f'{self.GLOBAL_CONFIG_PATHS}). Using default value "{value}" instead. If you would '
                    f"like to "
                    f"use a different value, please specify it in an environment variable or in a "
                    f"great_expectations.conf file located at one of the above paths, in a section named "
                    f'"ge_cloud_config".'
                )
                substitutions[config_variable] = value

        return DataContextConfig(**self.config_provider.substitute_config(config))

    def create_expectation_suite(
        self,
        expectation_suite_name: str,
        overwrite_existing: bool = False,
        **kwargs,
    ) -> ExpectationSuite:
        """Build a new expectation suite and save it into the data_context expectation store.

        Args:
            expectation_suite_name: The name of the expectation_suite to create
            overwrite_existing (boolean): Whether to overwrite expectation suite if expectation suite with given name
                already exists.

        Returns:
            A new (empty) expectation suite.
        """
        # deprecated-v0.15.48
        warnings.warn(
            "create_expectation_suite is deprecated as of v0.15.49 and will be removed in v0.18. "
            "Please use add_expectation_suite or add_or_update_expectation_suite instead.",
            DeprecationWarning,
        )
        if not isinstance(overwrite_existing, bool):
            raise ValueError("overwrite_existing must be of type bool.")

        expectation_suite = ExpectationSuite(
            expectation_suite_name=expectation_suite_name, data_context=self
        )

        existing_suite_names = self.list_expectation_suite_names()
        cloud_id: Optional[str] = None
        if expectation_suite_name in existing_suite_names and not overwrite_existing:
            raise gx_exceptions.DataContextError(
                f"expectation_suite '{expectation_suite_name}' already exists. If you would like to overwrite this "
                "expectation_suite, set overwrite_existing=True."
            )
        elif expectation_suite_name in existing_suite_names and overwrite_existing:
            identifiers: Optional[
                Union[List[str], List[GXCloudIdentifier]]
            ] = self.list_expectation_suites()
            if identifiers:
                for cloud_identifier in identifiers:
                    if isinstance(cloud_identifier, GXCloudIdentifier):
                        cloud_identifier_tuple = cloud_identifier.to_tuple()
                        name: str = cloud_identifier_tuple[2]
                        if name == expectation_suite_name:
                            cloud_id = cloud_identifier_tuple[1]
                            expectation_suite.ge_cloud_id = cloud_id

        key = GXCloudIdentifier(
            resource_type=GXCloudRESTResource.EXPECTATION_SUITE,
            id=cloud_id,
            resource_name=expectation_suite_name,
        )

        response: Union[bool, GXCloudResourceRef] = self.expectations_store.set(key, expectation_suite, **kwargs)  # type: ignore[func-returns-value]
        if isinstance(response, GXCloudResourceRef):
            expectation_suite.ge_cloud_id = response.id

        return expectation_suite

    @overload
    def delete_expectation_suite(
        self,
        expectation_suite_name: str = ...,
        ge_cloud_id: None = ...,
        id: None = ...,
    ) -> bool:
        ...

    @overload
    def delete_expectation_suite(
        self,
        expectation_suite_name: None = ...,
        ge_cloud_id: str = ...,
        id: None = ...,
    ) -> bool:
        ...

    @overload
    def delete_expectation_suite(
        self,
        expectation_suite_name: None = ...,
        ge_cloud_id: None = ...,
        id: str = ...,
    ) -> bool:
        ...

    def delete_expectation_suite(
        self,
        expectation_suite_name: str | None = None,
        ge_cloud_id: str | None = None,
        id: str | None = None,
    ) -> bool:
        """Delete specified expectation suite from data_context expectation store.

        Args:
            expectation_suite_name: The name of the expectation_suite to create

        Returns:
            True for Success and False for Failure.
        """
        # <GX_RENAME>
        id = self._resolve_id_and_ge_cloud_id(id=id, ge_cloud_id=ge_cloud_id)
        del ge_cloud_id

        key = GXCloudIdentifier(
            resource_type=GXCloudRESTResource.EXPECTATION_SUITE,
            id=id,
            resource_name=expectation_suite_name,
        )

        return self.expectations_store.remove_key(key)

    def get_expectation_suite(
        self,
        expectation_suite_name: Optional[str] = None,
        include_rendered_content: Optional[bool] = None,
        ge_cloud_id: Optional[str] = None,
    ) -> ExpectationSuite:
        """Get an Expectation Suite by name or GX Cloud ID
        Args:
            expectation_suite_name (str): The name of the Expectation Suite
            include_rendered_content (bool): Whether or not to re-populate rendered_content for each
                ExpectationConfiguration.
            ge_cloud_id (str): The GX Cloud ID for the Expectation Suite.

        Returns:
            An existing ExpectationSuite
        """
        if ge_cloud_id is None and expectation_suite_name is None:
            raise ValueError(
                "ge_cloud_id and expectation_suite_name cannot both be None"
            )

        key = GXCloudIdentifier(
            resource_type=GXCloudRESTResource.EXPECTATION_SUITE,
            id=ge_cloud_id,
            resource_name=expectation_suite_name,
        )

        try:
            expectations_schema_dict: dict = cast(
                dict, self.expectations_store.get(key)
            )
        except StoreBackendError:
            raise ValueError(
                f"Unable to load Expectation Suite {key.resource_name or key.id}"
            )

        if include_rendered_content is None:
            include_rendered_content = (
                self._determine_if_expectation_suite_include_rendered_content()
            )

        # create the ExpectationSuite from constructor
        expectation_suite = ExpectationSuite(
            **expectations_schema_dict, data_context=self
        )
        if include_rendered_content:
            expectation_suite.render()
        return expectation_suite

    def _save_expectation_suite(
        self,
        expectation_suite: ExpectationSuite,
        expectation_suite_name: Optional[str] = None,
        overwrite_existing: bool = True,
        include_rendered_content: Optional[bool] = None,
        **kwargs: Optional[dict],
    ) -> None:
        id = expectation_suite.ge_cloud_id
        key = GXCloudIdentifier(
            resource_type=GXCloudRESTResource.EXPECTATION_SUITE,
            id=id,
            resource_name=expectation_suite.expectation_suite_name,
        )

        if not overwrite_existing:
            self._validate_suite_unique_constaints_before_save(key)

        self._evaluation_parameter_dependencies_compiled = False
        include_rendered_content = (
            self._determine_if_expectation_suite_include_rendered_content(
                include_rendered_content=include_rendered_content
            )
        )
        if include_rendered_content:
            expectation_suite.render()

        response = self.expectations_store.set(key, expectation_suite, **kwargs)  # type: ignore[func-returns-value]
        if isinstance(response, GXCloudResourceRef):
            expectation_suite.ge_cloud_id = response.id

    def _validate_suite_unique_constaints_before_save(
        self, key: GXCloudIdentifier
    ) -> None:
        ge_cloud_id = key.id
        if ge_cloud_id:
            if self.expectations_store.has_key(key):
                raise gx_exceptions.DataContextError(
                    f"expectation_suite with GX Cloud ID {ge_cloud_id} already exists. "
                    f"If you would like to overwrite this expectation_suite, set overwrite_existing=True."
                )

        suite_name = key.resource_name
        existing_suite_names = self.list_expectation_suite_names()
        if suite_name in existing_suite_names:
            raise gx_exceptions.DataContextError(
                f"expectation_suite '{suite_name}' already exists. If you would like to overwrite this "
                "expectation_suite, set overwrite_existing=True."
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
        """
        See `AbstractDataContext.add_checkpoint` for more information.
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
            # deprecated-v0.16.16
            warnings.warn(
                f"{e.message}; using add_checkpoint to overwrite an existing value is deprecated as of v0.16.16 "
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

    def _determine_default_action_list(self) -> Sequence[ActionDict]:
        default_actions = super()._determine_default_action_list()

        # Data docs are not relevant to Cloud and should be excluded
        return [
            action
            for action in default_actions
            if action["action"]["class_name"] != "UpdateDataDocsAction"
        ]

    def list_checkpoints(self) -> Union[List[str], List[ConfigurationIdentifier]]:
        return self.checkpoint_store.list_checkpoints(ge_cloud_mode=True)

    def list_profilers(self) -> Union[List[str], List[ConfigurationIdentifier]]:
        return RuleBasedProfiler.list_profilers(
            profiler_store=self.profiler_store,
            ge_cloud_mode=True,
        )

    def _init_site_builder_for_data_docs_site_creation(
        self, site_name: str, site_config: dict
    ) -> SiteBuilder:
        """
        Note that this explicitly overriding the `AbstractDataContext` helper method called
        in `self.build_data_docs()`.

        The only difference here is the inclusion of `ge_cloud_mode` in the `runtime_environment`
        used in `SiteBuilder` instantiation.
        """
        site_builder: SiteBuilder = instantiate_class_from_config(
            config=site_config,
            runtime_environment={
                "data_context": self,
                "root_directory": self.root_directory,
                "site_name": site_name,
                "cloud_mode": True,
            },
            config_defaults={
                "class_name": "SiteBuilder",
                "module_name": "great_expectations.render.renderer.site_builder",
            },
        )
        return site_builder

    def _determine_key_for_suite_update(
        self, name: str, id: str | None
    ) -> Union[ExpectationSuiteIdentifier, GXCloudIdentifier]:
        """
        Note that this explicitly overriding the `AbstractDataContext` helper method called
        in `self.update_expectation_suite()`.

        The only difference here is the creation of a Cloud-specific `GXCloudIdentifier`
        instead of the usual `ExpectationSuiteIdentifier` for `Store` interaction.
        """
        return GXCloudIdentifier(
            resource_type=GXCloudRESTResource.EXPECTATION_SUITE,
            id=id,
            resource_name=name,
        )

    def _determine_key_for_profiler_save(
        self, name: str, id: Optional[str]
    ) -> Union[ConfigurationIdentifier, GXCloudIdentifier]:
        """
        Note that this explicitly overriding the `AbstractDataContext` helper method called
        in `self.save_profiler()`.

        The only difference here is the creation of a Cloud-specific `GXCloudIdentifier`
        instead of the usual `ConfigurationIdentifier` for `Store` interaction.
        """
        return GXCloudIdentifier(resource_type=GXCloudRESTResource.PROFILER, id=id)

    @classmethod
    def _load_cloud_backed_project_config(
        cls,
        cloud_config: Optional[GXCloudConfig],
    ):
        assert cloud_config is not None
        config = cls.retrieve_data_context_config_from_cloud(cloud_config=cloud_config)
        return config

    def _persist_suite_with_store(
        self,
        expectation_suite: ExpectationSuite,
        overwrite_existing: bool,
        **kwargs,
    ) -> ExpectationSuite:
        cloud_id: str | None
        if expectation_suite.ge_cloud_id:
            cloud_id = expectation_suite.ge_cloud_id
        else:
            cloud_id = None

        key = GXCloudIdentifier(
            resource_type=GXCloudRESTResource.EXPECTATION_SUITE,
            resource_name=expectation_suite.expectation_suite_name,
            id=cloud_id,
        )

        persistence_fn: Callable
        if overwrite_existing:
            persistence_fn = self.expectations_store.add_or_update
        else:
            persistence_fn = self.expectations_store.add

        response = persistence_fn(key=key, value=expectation_suite, **kwargs)
        if isinstance(response, GXCloudResourceRef):
            expectation_suite.ge_cloud_id = response.id

        return expectation_suite

    def _save_project_config(
        self, _fds_datasource: FluentDatasource | None = None
    ) -> None:
        """
        See parent 'AbstractDataContext._save_project_config()` for more information.

        Explicitly override base class implementation to retain legacy behavior.
        """
        # 042723 kilo59
        # Currently CloudDataContext and FileDataContext diverge in how FDS are persisted.
        # FileDataContexts don't use the DatasourceStore at all to save or hydrate FDS configs.
        # CloudDataContext does use DatasourceStore in order to make use of the Cloud http clients.
        # The intended future state is for a new FluentDatasourceStore that can fully encapsulate
        # the different requirements for FDS vs BDS.
        # At which time `_save_project_config` will revert to being a no-op operation on the CloudDataContext.
        if _fds_datasource:
            self._datasource_store.set(key=None, value=_fds_datasource)
        else:
            logger.debug(
                "CloudDataContext._save_project_config() has no `fds_datasource` to update"
            )

    def _view_validation_result(self, result: CheckpointResult) -> None:
        url = result.validation_result_url
        assert (
            url
        ), "Guaranteed to have a validation_result_url if generating a CheckpointResult in a Cloud-backed environment"
        self._open_url_in_browser(url)

    def _add_datasource(
        self,
        name: str | None = None,
        initialize: bool = True,
        save_changes: bool | None = None,
        datasource: BaseDatasource | FluentDatasource | LegacyDatasource | None = None,
        **kwargs,
    ) -> BaseDatasource | FluentDatasource | LegacyDatasource | None:
        result = super()._add_datasource(
            name=name,
            initialize=initialize,
            save_changes=save_changes,
            datasource=datasource,
            **kwargs,
        )
        if result and not isinstance(result, FluentDatasource):
            # deprecated-v0.17.2
            warnings.warn(
                "Adding block-style or legacy datasources in a Cloud-backed environment is deprecated as of v0.17.2 and will be removed in a future version. "
                "Please migrate to fluent-style datasources moving forward.",
                DeprecationWarning,
            )
        return result
