from __future__ import annotations

import logging
import os
import uuid
from typing import (
    TYPE_CHECKING,
    Dict,
    Mapping,
    Optional,
    Union,
)

from requests import HTTPError, Response

import great_expectations.exceptions as gx_exceptions
from great_expectations import __version__
from great_expectations._docs_decorators import public_api
from great_expectations.analytics.client import init as init_analytics
from great_expectations.analytics.config import ENV_CONFIG
from great_expectations.compatibility.typing_extensions import override
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
    DataContextVariableSchema,
)
from great_expectations.data_context.store import DataAssetStore
from great_expectations.data_context.store.datasource_store import (
    DatasourceStore,
)
from great_expectations.data_context.store.gx_cloud_store_backend import (
    GXCloudStoreBackend,
)
from great_expectations.data_context.store.validation_results_store import (
    ValidationResultsStore,
)
from great_expectations.data_context.types.base import (
    DataContextConfig,
    DataContextConfigDefaults,
    GXCloudConfig,
    assetConfigSchema,
)
from great_expectations.data_context.types.resource_identifiers import GXCloudIdentifier
from great_expectations.data_context.util import instantiate_class_from_config
from great_expectations.datasource.datasource_dict import DatasourceDict
from great_expectations.datasource.fluent import Datasource as FluentDatasource
from great_expectations.exceptions.exceptions import DataContextError

if TYPE_CHECKING:
    from great_expectations.alias_types import PathStr
    from great_expectations.checkpoint.checkpoint import CheckpointResult
    from great_expectations.datasource.fluent import Datasource as FluentDatasource
    from great_expectations.render.renderer.site_builder import SiteBuilder

logger = logging.getLogger(__name__)


class NoUserIdError(Exception):
    def __init__(self):
        super().__init__("No user id in /account/me response")


class OrganizationIdNotSpecifiedError(Exception):
    def __init__(self):
        super().__init__(
            "A request to GX Cloud is being attempted without an organization id configured. "
            "Maybe you need to set the environment variable GX_CLOUD_ORGANIZATION_ID?"
        )


@public_api
class CloudDataContext(SerializableDataContext):
    """Subclass of AbstractDataContext that contains functionality necessary to work in a GX Cloud-backed environment."""  # noqa: E501

    def __init__(  # noqa: PLR0913
        self,
        project_config: Optional[Union[DataContextConfig, Mapping]] = None,
        context_root_dir: Optional[PathStr] = None,
        project_root_dir: Optional[PathStr] = None,
        runtime_environment: Optional[dict] = None,
        cloud_base_url: Optional[str] = None,
        cloud_access_token: Optional[str] = None,
        cloud_organization_id: Optional[str] = None,
    ) -> None:
        """
        CloudDataContext constructor

        Args:
            project_config (DataContextConfig): config for CloudDataContext
            runtime_environment (dict):  a dictionary of config variables that override both those set in
                config_variables.yml and the environment
            cloud_config (GXCloudConfig): GXCloudConfig corresponding to current CloudDataContext
        """  # noqa: E501
        self._check_if_latest_version()
        self._cloud_config = self.get_cloud_config(
            cloud_base_url=cloud_base_url,
            cloud_access_token=cloud_access_token,
            cloud_organization_id=cloud_organization_id,
        )
        self._context_root_directory = self.determine_context_root_directory(
            context_root_dir=context_root_dir,
            project_root_dir=project_root_dir,
        )
        self._project_config = self._init_project_config(project_config)

        # The DataAssetStore is relevant only for CloudDataContexts and is not an explicit part of the project config.  # noqa: E501
        # As such, it must be instantiated separately.
        self._data_asset_store = self._init_data_asset_store()

        super().__init__(
            context_root_dir=self._context_root_directory,
            runtime_environment=runtime_environment,
        )

    def _check_if_latest_version(self) -> None:
        checker = _VersionChecker(__version__)
        checker.check_if_using_latest_gx()

    @override
    def _init_analytics(self) -> None:
        organization_id = self.ge_cloud_config.organization_id
        init_analytics(
            enable=self._determine_analytics_enabled(),
            user_id=self._get_cloud_user_id(),
            data_context_id=self._data_context_id,
            organization_id=uuid.UUID(organization_id) if organization_id else None,
            oss_id=self._get_oss_id(),
            cloud_mode=True,
        )

    def _get_cloud_user_id(self) -> uuid.UUID | None:
        if not ENV_CONFIG.gx_analytics_enabled:
            return None

        response = self._request_cloud_backend(
            cloud_config=self.ge_cloud_config, resource="accounts/me"
        )
        data = response.json()
        user_id = data.get("user_id") or data.get("id")
        if not user_id:
            raise NoUserIdError()
        return uuid.UUID(user_id)

    @override
    def _init_project_config(
        self, project_config: Optional[Union[DataContextConfig, Mapping]]
    ) -> DataContextConfig:
        if project_config is None:
            project_config = self.retrieve_data_context_config_from_cloud(
                cloud_config=self.ge_cloud_config,
            )

        return CloudDataContext.get_or_create_data_context_config(project_config)

    @override
    def _register_providers(self, config_provider: _ConfigurationProvider) -> None:
        """
        To ensure that Cloud credentials are accessible downstream, we want to ensure that
        we register a CloudConfigurationProvider.

        Note that it is registered last as it takes the highest precedence.
        """
        super()._register_providers(config_provider)
        config_provider.register_provider(_CloudConfigurationProvider(self.ge_cloud_config))

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
        """  # noqa: E501
        cloud_config_dict = cls._get_cloud_config_dict(
            cloud_base_url=cloud_base_url,
            cloud_access_token=cloud_access_token,
            cloud_organization_id=cloud_organization_id,
        )
        return all(val for val in cloud_config_dict.values())

    @classmethod
    def determine_context_root_directory(
        cls,
        context_root_dir: Optional[PathStr],
        project_root_dir: Optional[PathStr],
    ) -> str:
        context_root_dir = cls._resolve_context_root_dir_and_project_root_dir(
            context_root_dir=context_root_dir, project_root_dir=project_root_dir
        )
        if context_root_dir is None:
            context_root_dir = os.getcwd()  # noqa: PTH109
            logger.debug(
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
        """  # noqa: E501
        response = cls._request_cloud_backend(
            cloud_config=cloud_config, resource="data_context_configuration"
        )
        config = cls._prepare_v1_config(config=response.json())
        return DataContextConfig(**config)

    @classmethod
    def _prepare_v1_config(cls, config: dict) -> dict:
        # FluentDatasources are nested under the "datasources" key and need to be separated
        # to prevent downstream issues
        # This should be done before datasources are popped from the config below until
        # fluent_datasourcse are renamed datasourcess ()
        v1_data_sources = config.pop("data_sources", [])
        config["fluent_datasources"] = {ds["name"]: ds for ds in v1_data_sources}

        # Various context variables are no longer top-level keys in V1
        for var in (
            "datasources",
            "notebooks",
            "concurrency",
            "include_rendered_content",
            "profiler_store_name",
            "anonymous_usage_statistics",
            "evaluation_parameter_store_name",
            "suite_parameter_store_name",
        ):
            val = config.pop(var, None)
            if val:
                logger.info(f"Removed {var} from DataContextConfig while preparing V1 config")

        # V1 renamed Validations to ValidationResults
        # so this is a temporary patch until Cloud implements a V1 endpoint for DataContextConfig
        cls._change_key_from_v0_to_v1(
            config,
            "validations_store_name",
            DataContextVariableSchema.VALIDATIONS_STORE_NAME,
        )

        config = cls._prepare_stores_config(config=config)

        return config

    @classmethod
    def _prepare_stores_config(cls, config) -> dict:
        stores = config.get("stores")
        if not stores:
            return config

        to_delete: list[str] = []
        for name, store in stores.items():
            # Certain stores have been renamed in V1
            cls._change_value_from_v0_to_v1(
                store, "class_name", "ValidationsStore", ValidationResultsStore.__name__
            )

            # Profiler stores are no longer supported in V1
            if store.get("class_name") in [
                "ProfilerStore",
                "EvaluationParameterStore",
                "SuiteParameterStore",
            ]:
                to_delete.append(name)

        for name in to_delete:
            config["stores"].pop(name)

        return config

    @staticmethod
    def _change_key_from_v0_to_v1(config: dict, v0_key: str, v1_key: str) -> Optional[dict]:
        """Update the key if we have a V0 key and no V1 key in the config.

        Mutates the config object and returns the value that was renamed
        """
        value = config.pop(v0_key, None)
        if value and v1_key not in config:
            config[v1_key] = value
        return config.get(v1_key)

    @staticmethod
    def _change_value_from_v0_to_v1(config: dict, key: str, v0_value: str, v1_value: str) -> dict:
        if config.get(key) == v0_value:
            config[key] = v1_value
        return config

    @classmethod
    def _request_cloud_backend(cls, cloud_config: GXCloudConfig, resource: str) -> Response:
        access_token = cloud_config.access_token
        base_url = cloud_config.base_url
        organization_id = cloud_config.organization_id
        if not organization_id:
            raise OrganizationIdNotSpecifiedError()

        with create_session(access_token=access_token) as session:
            url = GXCloudStoreBackend.construct_versioned_url(base_url, organization_id, resource)
            response = session.get(url)

        try:
            response.raise_for_status()
        except HTTPError:
            raise gx_exceptions.GXCloudError(  # noqa: TRY003
                f"Bad request made to GX Cloud; {response.text}", response=response
            )

        return response

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
        """  # noqa: E501
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
            global_config_path_str = [f'"{path}"' for path in super().GLOBAL_CONFIG_PATHS]
            raise DataContextError(  # noqa: TRY003
                f"{(', ').join(missing_keys_str)} arg(s) required for ge_cloud_mode but neither provided nor found in "  # noqa: E501
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
            or cls._get_global_config_value(
                environment_variable=GXCloudEnvironmentVariable.BASE_URL,
                conf_file_section="ge_cloud_config",
                conf_file_option="base_url",
            )
            or CLOUD_DEFAULT_BASE_URL
        )
        cloud_organization_id = cloud_organization_id or cls._get_global_config_value(
            environment_variable=GXCloudEnvironmentVariable.ORGANIZATION_ID,
            conf_file_section="ge_cloud_config",
            conf_file_option="organization_id",
        )
        cloud_access_token = cloud_access_token or cls._get_global_config_value(
            environment_variable=GXCloudEnvironmentVariable.ACCESS_TOKEN,
            conf_file_section="ge_cloud_config",
            conf_file_option="access_token",
        )
        return {
            GXCloudEnvironmentVariable.BASE_URL: cloud_base_url,
            GXCloudEnvironmentVariable.ORGANIZATION_ID: cloud_organization_id,
            GXCloudEnvironmentVariable.ACCESS_TOKEN: cloud_access_token,
        }

    @override
    def _init_datasources(self) -> None:
        # Note that Cloud does NOT populate self._datasources with existing objects on init.
        # Objects are retrieved only when requested and are NOT cached (this differs in ephemeral/file-backed contexts).  # noqa: E501
        self._datasources = DatasourceDict(
            context=self,
            datasource_store=self._datasource_store,
        )

    @override
    def _init_datasource_store(self) -> DatasourceStore:
        # Never explicitly referenced but adheres
        # to the convention set by other internal Stores
        store_name = DataContextConfigDefaults.DEFAULT_DATASOURCE_STORE_NAME.value
        store_backend: dict = {"class_name": GXCloudStoreBackend.__name__}
        runtime_environment: dict = {
            "root_directory": self.root_directory,
            "ge_cloud_credentials": self.ge_cloud_config.to_dict(),
            "ge_cloud_resource_type": GXCloudRESTResource.DATASOURCE,
            "ge_cloud_base_url": self.ge_cloud_config.base_url,
        }

        datasource_store = DatasourceStore(
            store_name=store_name,
            store_backend=store_backend,
            runtime_environment=runtime_environment,
        )
        return datasource_store

    def _init_data_asset_store(self) -> DataAssetStore:
        # Never explicitly referenced but adheres
        # to the convention set by other internal Stores
        store_name = DataContextConfigDefaults.DEFAULT_DATA_ASSET_STORE_NAME.value
        store_backend: dict = {"class_name": GXCloudStoreBackend.__name__}
        runtime_environment: dict = {
            "root_directory": self.root_directory,
            "ge_cloud_credentials": self.ge_cloud_config.to_dict(),
            "ge_cloud_resource_type": GXCloudRESTResource.DATA_ASSET,
            "ge_cloud_base_url": self.ge_cloud_config.base_url,
        }

        data_asset_store = DataAssetStore(
            store_name=store_name,
            store_backend=store_backend,
            runtime_environment=runtime_environment,
            serializer=JsonConfigSerializer(schema=assetConfigSchema),
        )
        return data_asset_store

    def _delete_asset(self, id: str) -> bool:
        """Delete a DataAsset. Cloud will also update the corresponding Datasource."""
        key = GXCloudIdentifier(
            resource_type=GXCloudRESTResource.DATA_ASSET,
            id=id,
        )

        return self._data_asset_store.remove_key(key)

    @property
    def ge_cloud_config(self) -> GXCloudConfig:
        return self._cloud_config

    @override
    @property
    def _include_rendered_content(self) -> bool:
        # Cloud contexts always want rendered content
        return True

    @override
    def _init_variables(self) -> CloudDataContextVariables:
        ge_cloud_base_url: str = self.ge_cloud_config.base_url
        ge_cloud_organization_id: str = self.ge_cloud_config.organization_id  # type: ignore[assignment]
        ge_cloud_access_token: str = self.ge_cloud_config.access_token

        variables = CloudDataContextVariables(
            config=self._project_config,
            config_provider=self.config_provider,
            ge_cloud_base_url=ge_cloud_base_url,
            ge_cloud_organization_id=ge_cloud_organization_id,
            ge_cloud_access_token=ge_cloud_access_token,
        )
        return variables

    @override
    def _construct_data_context_id(self) -> uuid.UUID | None:
        """
        Choose the id of the currently-configured expectations store, if available and a persistent store.
        If not, it should choose the id stored in DataContextConfig.
        Returns:
            UUID to use as the data_context_id
        """  # noqa: E501
        org_id = self.ge_cloud_config.organization_id
        if org_id:
            return uuid.UUID(org_id)
        return None

    @override
    def get_config_with_variables_substituted(
        self, config: Optional[DataContextConfig] = None
    ) -> DataContextConfig:
        """
        Substitute vars in config of form ${var} or $(var) with values found in the following places,
        in order of precedence: cloud_config (for Data Contexts in GX Cloud mode), runtime_environment,
        environment variables, config_variables, or ge_cloud_config_variable_defaults (allows certain variables to
        be optional in GX Cloud mode).
        """  # noqa: E501
        if not config:
            config = self.config

        substitutions: dict = self.config_provider.get_values()

        cloud_config_variable_defaults = {
            "plugins_directory": self._normalize_absolute_or_relative_path(
                path=DataContextConfigDefaults.DEFAULT_PLUGINS_DIRECTORY.value
            ),
        }
        missing_config_vars_and_subs: list[tuple[str, str]] = []
        for config_variable, value in cloud_config_variable_defaults.items():
            if substitutions.get(config_variable) is None:
                substitutions[config_variable] = value
                missing_config_vars_and_subs.append((config_variable, value))

        if missing_config_vars_and_subs:
            missing_config_var_repr = ", ".join(
                [f"{var}={sub}" for var, sub in missing_config_vars_and_subs]
            )
            logger.info(
                "Config variables were not found in environment or global config ("
                f"{self.GLOBAL_CONFIG_PATHS}). Using default values instead. {missing_config_var_repr} ;"  # noqa: E501
                " If you would like to "
                "use a different value, please specify it in an environment variable or in a "
                "great_expectations.conf file located at one of the above paths, in a section named "  # noqa: E501
                '"ge_cloud_config".'
            )

        return DataContextConfig(**self.config_provider.substitute_config(config))

    @override
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

    @classmethod
    def _load_cloud_backed_project_config(
        cls,
        cloud_config: Optional[GXCloudConfig],
    ):
        assert cloud_config is not None
        config = cls.retrieve_data_context_config_from_cloud(cloud_config=cloud_config)
        return config

    @override
    def _save_project_config(self) -> None:
        """
        See parent 'AbstractDataContext._save_project_config()` for more information.

        Explicitly override base class implementation to retain legacy behavior.
        """
        logger.debug(
            "CloudDataContext._save_project_config() was called. Base class impl was override to be no-op to retain "  # noqa: E501
            "legacy behavior."
        )

    @override
    def _view_validation_result(self, result: CheckpointResult) -> None:
        for validation_result in result.run_results.values():
            url = validation_result.result_url
            if url:
                self._open_url_in_browser(url)

    @override
    def _add_datasource(
        self,
        name: str | None = None,
        initialize: bool = True,
        datasource: FluentDatasource | None = None,
        **kwargs,
    ) -> FluentDatasource | None:
        return super()._add_datasource(
            name=name,
            initialize=initialize,
            datasource=datasource,
            **kwargs,
        )
