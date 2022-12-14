from __future__ import annotations

import logging
import os
import shutil
import warnings
from typing import Optional, Tuple, Union

from ruamel.yaml import YAML, YAMLError
from ruamel.yaml.constructor import DuplicateKeyError

import great_expectations.exceptions as ge_exceptions
from great_expectations.core._docs_decorators import public_api
from great_expectations.data_context.data_context.base_data_context import (
    BaseDataContext,
)
from great_expectations.data_context.data_context.cloud_data_context import (
    CloudDataContext,
)
from great_expectations.data_context.templates import (
    CONFIG_VARIABLES_TEMPLATE,
    PROJECT_TEMPLATE_USAGE_STATISTICS_DISABLED,
    PROJECT_TEMPLATE_USAGE_STATISTICS_ENABLED,
)
from great_expectations.data_context.types.base import (
    CURRENT_GX_CONFIG_VERSION,
    MINIMUM_SUPPORTED_CONFIG_VERSION,
    AnonymizedUsageStatisticsConfig,
    DataContextConfig,
    GXCloudConfig,
)
from great_expectations.data_context.util import file_relative_path
from great_expectations.datasource import LegacyDatasource
from great_expectations.datasource.new_datasource import BaseDatasource
from great_expectations.experimental.datasources.interfaces import (
    Datasource as XDatasource,
)
from great_expectations.experimental.datasources.sources import _SourceFactories

logger = logging.getLogger(__name__)
yaml = YAML()
yaml.indent(mapping=2, sequence=4, offset=2)
yaml.default_flow_style = False


# TODO: <WILL> Most of the logic here will be migrated to FileDataContext
class DataContext(BaseDataContext):
    """A DataContext represents a Great Expectations project. It is the primary entry point for a Great Expectations
    deployment, with configurations and methods for all supporting components.

    The DataContext is configured via a yml file stored in a directory called great_expectations; this configuration
    file as well as managed Expectation Suites should be stored in version control. There are other ways to create a
    Data Context that may be better suited for your particular deployment e.g. ephemerally or backed by GX Cloud
    (coming soon). Please refer to our documentation for more details.

    You can Validate data or generate Expectations using Execution Engines including:

     * SQL (multiple dialects supported)
     * Spark
     * Pandas

    Your data can be stored in common locations including:

     * databases / data warehouses
     * files in s3, GCS, Azure, local storage
     * dataframes (spark and pandas) loaded into memory

    Please see our documentation for examples on how to set up Great Expectations, connect to your data,
    create Expectations, and Validate data.

    Other configuration options you can apply to a DataContext besides how to access data include things like where to
    store Expectations, Profilers, Checkpoints, Metrics, Validation Results and Data Docs and how those Stores are
    configured. Take a look at our documentation for more configuration options.

    --Public API--

    --Documentation--
        - https://docs.greatexpectations.io/docs/terms/data_context

    """

    @classmethod
    @public_api
    def create(
        cls,
        project_root_dir: Optional[str] = None,
        usage_statistics_enabled: bool = True,
        runtime_environment: Optional[dict] = None,
    ) -> DataContext:
        """Build a new great_expectations directory and DataContext object in the provided project_root_dir.

        `create` will create a new "great_expectations" directory in the provided folder, provided one does not
        already exist. Then, it will initialize a new DataContext in that folder and write the resulting config.

        --Documentation--
            - https://docs.greatexpectations.io/docs/terms/data_context

        Args:
            project_root_dir: path to the root directory in which to create a new great_expectations directory
            usage_statistics_enabled: boolean directive specifying whether or not to gather usage statistics
            runtime_environment: a dictionary of config variables that override both those set in
                config_variables.yml and the environment

        Returns:
            DataContext
        """

        if not os.path.isdir(project_root_dir):  # type: ignore[arg-type]
            raise ge_exceptions.DataContextError(
                "The project_root_dir must be an existing directory in which "
                "to initialize a new DataContext"
            )

        ge_dir = os.path.join(project_root_dir, cls.GX_DIR)  # type: ignore[arg-type]
        os.makedirs(ge_dir, exist_ok=True)
        cls.scaffold_directories(ge_dir)

        if os.path.isfile(os.path.join(ge_dir, cls.GX_YML)):
            message = f"""Warning. An existing `{cls.GX_YML}` was found here: {ge_dir}.
    - No action was taken."""
            warnings.warn(message)
        else:
            cls.write_project_template_to_disk(ge_dir, usage_statistics_enabled)

        uncommitted_dir = os.path.join(ge_dir, cls.GX_UNCOMMITTED_DIR)
        if os.path.isfile(os.path.join(uncommitted_dir, "config_variables.yml")):
            message = """Warning. An existing `config_variables.yml` was found here: {}.
    - No action was taken.""".format(
                uncommitted_dir
            )
            warnings.warn(message)
        else:
            cls.write_config_variables_template_to_disk(uncommitted_dir)

        return cls(context_root_dir=ge_dir, runtime_environment=runtime_environment)

    @classmethod
    def all_uncommitted_directories_exist(cls, ge_dir: str) -> bool:
        """Check if all uncommitted directories exist."""
        uncommitted_dir = os.path.join(ge_dir, cls.GX_UNCOMMITTED_DIR)
        for directory in cls.UNCOMMITTED_DIRECTORIES:
            if not os.path.isdir(os.path.join(uncommitted_dir, directory)):
                return False

        return True

    @classmethod
    def config_variables_yml_exist(cls, ge_dir: str) -> bool:
        """Check if all config_variables.yml exists."""
        path_to_yml = os.path.join(ge_dir, cls.GX_YML)

        # TODO this is so brittle and gross
        with open(path_to_yml) as f:
            config = yaml.load(f)
        config_var_path = config.get("config_variables_file_path")
        config_var_path = os.path.join(ge_dir, config_var_path)
        return os.path.isfile(config_var_path)

    @classmethod
    def write_config_variables_template_to_disk(cls, uncommitted_dir: str) -> None:
        os.makedirs(uncommitted_dir, exist_ok=True)
        config_var_file = os.path.join(uncommitted_dir, "config_variables.yml")
        with open(config_var_file, "w") as template:
            template.write(CONFIG_VARIABLES_TEMPLATE)

    @classmethod
    def write_project_template_to_disk(
        cls, ge_dir: str, usage_statistics_enabled: bool = True
    ) -> None:
        file_path = os.path.join(ge_dir, cls.GX_YML)
        with open(file_path, "w") as template:
            if usage_statistics_enabled:
                template.write(PROJECT_TEMPLATE_USAGE_STATISTICS_ENABLED)
            else:
                template.write(PROJECT_TEMPLATE_USAGE_STATISTICS_DISABLED)

    @classmethod
    def scaffold_directories(cls, base_dir: str) -> None:
        """Safely create GX directories for a new project."""
        os.makedirs(base_dir, exist_ok=True)
        with open(os.path.join(base_dir, ".gitignore"), "w") as f:
            f.write("uncommitted/")

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

        uncommitted_dir = os.path.join(base_dir, cls.GX_UNCOMMITTED_DIR)

        for new_directory in cls.UNCOMMITTED_DIRECTORIES:
            new_directory_path = os.path.join(uncommitted_dir, new_directory)
            os.makedirs(new_directory_path, exist_ok=True)

    @classmethod
    def scaffold_custom_data_docs(cls, plugins_dir: str) -> None:
        """Copy custom data docs templates"""
        styles_template = file_relative_path(
            __file__,
            "../../render/view/static/styles/data_docs_custom_styles_template.css",
        )
        styles_destination_path = os.path.join(
            plugins_dir, "custom_data_docs", "styles", "data_docs_custom_styles.css"
        )
        shutil.copyfile(styles_template, styles_destination_path)

    def __init__(
        self,
        context_root_dir: Optional[str] = None,
        runtime_environment: Optional[dict] = None,
        cloud_mode: bool = False,
        cloud_base_url: Optional[str] = None,
        cloud_access_token: Optional[str] = None,
        cloud_organization_id: Optional[str] = None,
        # <GX_RENAME> Deprecated as of 0.15.37
        ge_cloud_mode: bool = False,
        ge_cloud_base_url: Optional[str] = None,
        ge_cloud_access_token: Optional[str] = None,
        ge_cloud_organization_id: Optional[str] = None,
    ) -> None:
        # Chetan - 20221208 - not formally deprecating these values until a future date
        (
            cloud_base_url,
            cloud_access_token,
            cloud_organization_id,
            cloud_mode,
        ) = DataContext._resolve_cloud_args(
            cloud_mode=cloud_mode,
            cloud_base_url=cloud_base_url,
            cloud_access_token=cloud_access_token,
            cloud_organization_id=cloud_organization_id,
            ge_cloud_mode=ge_cloud_mode,
            ge_cloud_base_url=ge_cloud_base_url,
            ge_cloud_access_token=ge_cloud_access_token,
            ge_cloud_organization_id=ge_cloud_organization_id,
        )

        self._sources: _SourceFactories = _SourceFactories(self)
        self._cloud_mode = cloud_mode
        self._cloud_config = self._init_cloud_config(
            cloud_mode=cloud_mode,
            cloud_base_url=cloud_base_url,
            cloud_access_token=cloud_access_token,
            cloud_organization_id=cloud_organization_id,
        )

        self._context_root_directory = self._init_context_root_directory(
            context_root_dir=context_root_dir,
        )

        project_config = self._load_project_config()

        super().__init__(
            project_config=project_config,
            context_root_dir=self._context_root_directory,
            runtime_environment=runtime_environment,
            cloud_mode=self._cloud_mode,
            cloud_config=self._cloud_config,
        )

        # Save project config if data_context_id auto-generated
        if self._check_for_usage_stats_sync(project_config):
            self._save_project_config()

    @staticmethod
    def _resolve_cloud_args(  # type: ignore[override]
        cloud_mode: bool = False,
        cloud_base_url: Optional[str] = None,
        cloud_access_token: Optional[str] = None,
        cloud_organization_id: Optional[str] = None,
        # <GX_RENAME> Deprecated as of 0.15.37
        ge_cloud_mode: bool = False,
        ge_cloud_base_url: Optional[str] = None,
        ge_cloud_access_token: Optional[str] = None,
        ge_cloud_organization_id: Optional[str] = None,
    ) -> Tuple[Optional[str], Optional[str], Optional[str], bool]:
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
        cloud_mode = True if cloud_mode or ge_cloud_mode else False
        return cloud_base_url, cloud_access_token, cloud_organization_id, cloud_mode

    def _save_project_config(self) -> None:
        """
        See parent 'AbstractDataContext._save_project_config()` for more information.

        Explicitly override base class implementation to retain legacy behavior.
        """
        logger.debug("Starting DataContext._save_project_config")

        config_filepath = os.path.join(self.root_directory, self.GX_YML)  # type: ignore[arg-type]

        try:
            with open(config_filepath, "w") as outfile:
                self.config.to_yaml(outfile)
        except PermissionError as e:
            logger.warning(f"Could not save project config to disk: {e}")

    def _attach_datasource_to_context(self, datasource: XDatasource):
        # We currently don't allow one to overwrite a datasource with this internal method
        if datasource.name in self.datasources:
            raise ge_exceptions.DataContextError(
                f"Can not write the experimental datasource {datasource.name} because a datasource of that "
                "name already exists in the data context."
            )
        self.datasources[datasource.name] = datasource

    @property
    def sources(self) -> _SourceFactories:
        return self._sources

    def _init_cloud_config(
        self,
        cloud_mode: bool,
        cloud_base_url: Optional[str],
        cloud_access_token: Optional[str],
        cloud_organization_id: Optional[str],
    ) -> Optional[GXCloudConfig]:
        if not cloud_mode:
            return None

        cloud_config = CloudDataContext.get_cloud_config(
            cloud_base_url=cloud_base_url,
            cloud_access_token=cloud_access_token,
            cloud_organization_id=cloud_organization_id,
        )
        return cloud_config

    def _init_context_root_directory(self, context_root_dir: Optional[str]) -> str:
        if self.cloud_mode and context_root_dir is None:
            context_root_dir = CloudDataContext.determine_context_root_directory(
                context_root_dir
            )
        else:
            context_root_dir = (
                self.find_context_root_dir()
                if context_root_dir is None
                else context_root_dir
            )

        return os.path.abspath(os.path.expanduser(context_root_dir))

    def _check_for_usage_stats_sync(self, project_config: DataContextConfig) -> bool:
        """
        If there are differences between the DataContextConfig used to instantiate
        the DataContext and the DataContextConfig assigned to `self.config`, we want
        to save those changes to disk so that subsequent instantiations will utilize
        the same values.

        A small caveat is that if that difference stems from a global override (env var
        or conf file), we don't want to write to disk. This is due to the fact that
        those mechanisms allow for dynamic values and saving them will make them static.

        Args:
            project_config: The DataContextConfig used to instantiate the DataContext.

        Returns:
            A boolean signifying whether or not the current DataContext's config needs
            to be persisted in order to recognize changes made to usage statistics.
        """
        project_config_usage_stats: Optional[
            AnonymizedUsageStatisticsConfig
        ] = project_config.anonymous_usage_statistics
        context_config_usage_stats: Optional[
            AnonymizedUsageStatisticsConfig
        ] = self.config.anonymous_usage_statistics

        if (
            project_config_usage_stats.enabled is False  # type: ignore[union-attr]
            or context_config_usage_stats.enabled is False  # type: ignore[union-attr]
        ):
            return False

        if project_config_usage_stats.explicit_id is False:  # type: ignore[union-attr]
            return True

        if project_config_usage_stats == context_config_usage_stats:
            return False

        if project_config_usage_stats is None or context_config_usage_stats is None:
            return True

        # If the data_context_id differs and that difference is not a result of a global override, a sync is necessary.
        global_data_context_id: Optional[str] = self._get_data_context_id_override()
        if (
            project_config_usage_stats.data_context_id
            != context_config_usage_stats.data_context_id
            and context_config_usage_stats.data_context_id != global_data_context_id
        ):
            return True

        # If the usage_statistics_url differs and that difference is not a result of a global override, a sync is necessary.
        global_usage_stats_url: Optional[str] = self._get_usage_stats_url_override()
        if (
            project_config_usage_stats.usage_statistics_url
            != context_config_usage_stats.usage_statistics_url
            and context_config_usage_stats.usage_statistics_url
            != global_usage_stats_url
        ):
            return True

        return False

    def _load_project_config(self):
        """
        Reads the project configuration from the project configuration file.
        The file may contain ${SOME_VARIABLE} variables - see self.project_config_with_variables_substituted
        for how these are substituted.

        For Data Contexts in GX Cloud mode, a user-specific template is retrieved from the Cloud API
        - see CloudDataContext.retrieve_data_context_config_from_cloud for more details.

        :return: the configuration object read from the file or template
        """
        if self.cloud_mode:
            cloud_config = self.ge_cloud_config
            assert cloud_config is not None
            config = CloudDataContext.retrieve_data_context_config_from_cloud(
                cloud_config=cloud_config
            )
            return config

        path_to_yml = os.path.join(self._context_root_directory, self.GX_YML)
        try:
            with open(path_to_yml) as data:
                config_commented_map_from_yaml = yaml.load(data)

        except DuplicateKeyError:
            raise ge_exceptions.InvalidConfigurationYamlError(
                "Error: duplicate key found in project YAML file."
            )
        except YAMLError as err:
            raise ge_exceptions.InvalidConfigurationYamlError(
                "Your configuration file is not a valid yml file likely due to a yml syntax error:\n\n{}".format(
                    err
                )
            )
        except OSError:
            raise ge_exceptions.ConfigNotFoundError()

        try:
            return DataContextConfig.from_commented_map(
                commented_map=config_commented_map_from_yaml
            )
        except ge_exceptions.InvalidDataContextConfigError:
            # Just to be explicit about what we intended to catch
            raise

    def add_store(self, store_name, store_config):
        logger.debug(f"Starting DataContext.add_store for store {store_name}")

        new_store = super().add_store(store_name, store_config)
        self._save_project_config()
        return new_store

    def add_datasource(  # type: ignore[override]
        self, name: str, **kwargs: dict
    ) -> Optional[Union[LegacyDatasource, BaseDatasource]]:
        logger.debug(f"Starting DataContext.add_datasource for datasource {name}")

        new_datasource: Optional[
            Union[LegacyDatasource, BaseDatasource]
        ] = super().add_datasource(
            name=name, **kwargs  # type: ignore[arg-type]
        )
        return new_datasource

    def update_datasource(  # type: ignore[override]
        self,
        datasource: Union[LegacyDatasource, BaseDatasource],
    ) -> None:
        """
        See parent `BaseDataContext.update_datasource` for more details.
        Note that this method persists changes using an underlying Store.
        """
        logger.debug(
            f"Starting DataContext.update_datasource for datasource {datasource.name}"
        )

        super().update_datasource(
            datasource=datasource,
        )

    def delete_datasource(self, name: str) -> None:  # type: ignore[override]
        logger.debug(f"Starting DataContext.delete_datasource for datasource {name}")
        super().delete_datasource(datasource_name=name)
        self._save_project_config()

    @classmethod
    def find_context_root_dir(cls) -> str:
        result = None
        yml_path = None
        ge_home_environment = os.getenv("GX_HOME")
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

        logger.debug(f"Using project config: {yml_path}")
        return result

    @classmethod
    def get_ge_config_version(
        cls, context_root_dir: Optional[str] = None
    ) -> Optional[float]:
        yml_path = cls.find_context_yml_file(search_start_dir=context_root_dir)
        if yml_path is None:
            return None

        with open(yml_path) as f:
            config_commented_map_from_yaml = yaml.load(f)

        config_version = config_commented_map_from_yaml.get("config_version")
        return float(config_version) if config_version else None

    @classmethod
    def set_ge_config_version(
        cls,
        config_version: Union[int, float],
        context_root_dir: Optional[str] = None,
        validate_config_version: bool = True,
    ) -> bool:
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
            elif config_version > CURRENT_GX_CONFIG_VERSION:
                raise ge_exceptions.UnsupportedConfigVersionError(
                    "Invalid config version ({}).\n    The maximum valid version is {}.".format(
                        config_version, CURRENT_GX_CONFIG_VERSION
                    ),
                )

        yml_path = cls.find_context_yml_file(search_start_dir=context_root_dir)
        if yml_path is None:
            return False

        with open(yml_path) as f:
            config_commented_map_from_yaml = yaml.load(f)
            config_commented_map_from_yaml["config_version"] = float(config_version)

        with open(yml_path, "w") as f:
            yaml.dump(config_commented_map_from_yaml, f)

        return True

    @classmethod
    def find_context_yml_file(
        cls, search_start_dir: Optional[str] = None
    ) -> Optional[str]:
        """Search for the yml file starting here and moving upward."""
        yml_path = None
        if search_start_dir is None:
            search_start_dir = os.getcwd()

        for i in range(4):
            logger.debug(
                f"Searching for config file {search_start_dir} ({i} layer deep)"
            )

            potential_ge_dir = os.path.join(search_start_dir, cls.GX_DIR)

            if os.path.isdir(potential_ge_dir):
                potential_yml = os.path.join(potential_ge_dir, cls.GX_YML)
                if os.path.isfile(potential_yml):
                    yml_path = potential_yml
                    logger.debug(f"Found config file at {str(yml_path)}")
                    break
            # move up one directory
            search_start_dir = os.path.dirname(search_start_dir)

        return yml_path

    @classmethod
    def does_config_exist_on_disk(cls, context_root_dir: str) -> bool:
        """Return True if the great_expectations.yml exists on disk."""
        return os.path.isfile(os.path.join(context_root_dir, cls.GX_YML))

    @classmethod
    def is_project_initialized(cls, ge_dir: str) -> bool:
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
    def does_project_have_a_datasource_in_config_file(cls, ge_dir: str) -> bool:
        if not cls.does_config_exist_on_disk(ge_dir):
            return False
        return cls._does_context_have_at_least_one_datasource(ge_dir)

    @classmethod
    def _does_context_have_at_least_one_datasource(cls, ge_dir: str) -> bool:
        context = cls._attempt_context_instantiation(ge_dir)
        if not isinstance(context, DataContext):
            return False
        return len(context.list_datasources()) >= 1

    @classmethod
    def _does_context_have_at_least_one_suite(cls, ge_dir: str) -> bool:
        context = cls._attempt_context_instantiation(ge_dir)
        if not isinstance(context, DataContext):
            return False
        return bool(context.list_expectation_suites())

    @classmethod
    def _attempt_context_instantiation(cls, ge_dir: str) -> Optional[DataContext]:
        try:
            context = DataContext(ge_dir)
            return context
        except (
            ge_exceptions.DataContextError,
            ge_exceptions.InvalidDataContextConfigError,
        ) as e:
            logger.debug(e)
        return None
