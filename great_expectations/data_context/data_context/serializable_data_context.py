from __future__ import annotations

import logging
import os
import shutil
import warnings
from typing import Optional, Union

import requests
from ruamel.yaml import YAML, YAMLError
from ruamel.yaml.constructor import DuplicateKeyError

import great_expectations.exceptions as ge_exceptions
from great_expectations import __version__
from great_expectations.data_context.data_context.abstract_data_context import (
    AbstractDataContext,
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
    DataContextConfigDefaults,
    GXCloudConfig,
)
from great_expectations.data_context.util import file_relative_path

logger = logging.getLogger(__name__)
yaml = YAML()
yaml.indent(mapping=2, sequence=4, offset=2)
yaml.default_flow_style = False


class SerializableDataContext(AbstractDataContext):
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

    def __init__(
        self, context_root_dir: str, runtime_environment: Optional[dict] = None
    ) -> None:
        self.context_root_dir = context_root_dir
        super().__init__(runtime_environment=runtime_environment)

    def _init_datasource_store(self):
        pass

    def _init_variables(self):
        pass

    def _save_project_config(self) -> None:
        """
        See parent 'AbstractDataContext._save_project_config()` for more information.

        Explicitly override base class implementation to retain legacy behavior.
        """
        logger.debug("Starting DataContext._save_project_config")

        config_filepath = os.path.join(self.root_directory, self.GE_YML)  # type: ignore[arg-type]

        try:
            with open(config_filepath, "w") as outfile:
                self.config.to_yaml(outfile)
        except PermissionError as e:
            logger.warning(f"Could not save project config to disk: {e}")

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

    @classmethod
    def create(
        cls,
        project_root_dir: Optional[str] = None,
        usage_statistics_enabled: bool = True,
        runtime_environment: Optional[dict] = None,
    ) -> SerializableDataContext:
        """
        Build a new great_expectations directory and DataContext object in the provided project_root_dir.

        `create` will create a new "great_expectations" directory in the provided folder, provided one does not
        already exist. Then, it will initialize a new DataContext in that folder and write the resulting config.

        --Public API--

        --Documentation--
            https://docs.greatexpectations.io/docs/terms/data_context

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

        ge_dir = os.path.join(project_root_dir, cls.GE_DIR)  # type: ignore[arg-type]
        os.makedirs(ge_dir, exist_ok=True)
        cls.scaffold_directories(ge_dir)

        if os.path.isfile(os.path.join(ge_dir, cls.GE_YML)):
            message = f"""Warning. An existing `{cls.GE_YML}` was found here: {ge_dir}.
    - No action was taken."""
            warnings.warn(message)
        else:
            cls.write_project_template_to_disk(ge_dir, usage_statistics_enabled)

        uncommitted_dir = os.path.join(ge_dir, cls.GE_UNCOMMITTED_DIR)
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
    def _load_project_config(
        cls,
        context_root_directory: Optional[str],
        ge_cloud_mode: bool,
        ge_cloud_config: Optional[GXCloudConfig],
    ):
        """
        Reads the project configuration from the project configuration file.
        The file may contain ${SOME_VARIABLE} variables - see self.project_config_with_variables_substituted
        for how these are substituted.

        For Data Contexts in GE Cloud mode, a user-specific template is retrieved from the Cloud API
        - see CloudDataContext.retrieve_data_context_config_from_ge_cloud for more details.

        :return: the configuration object read from the file or template
        """
        if ge_cloud_mode:
            assert ge_cloud_config is not None
            config = cls.retrieve_data_context_config_from_ge_cloud(
                ge_cloud_config=ge_cloud_config
            )
            return config

        path_to_yml = os.path.join(context_root_directory, cls.GE_YML)
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

    @classmethod
    def retrieve_data_context_config_from_ge_cloud(
        cls, ge_cloud_config: GXCloudConfig
    ) -> DataContextConfig:
        """
        Utilizes the GeCloudConfig instantiated in the constructor to create a request to the Cloud API.
        Given proper authorization, the request retrieves a data context config that is pre-populated with
        GE objects specific to the user's Cloud environment (datasources, data connectors, etc).

        Please note that substitution for ${VAR} variables is performed in GE Cloud before being sent
        over the wire.

        :return: the configuration object retrieved from the Cloud API
        """
        base_url = ge_cloud_config.base_url
        organization_id = ge_cloud_config.organization_id
        ge_cloud_url = (
            f"{base_url}/organizations/{organization_id}/data-context-configuration"
        )
        headers = {
            "Content-Type": "application/vnd.api+json",
            "Authorization": f"Bearer {ge_cloud_config.access_token}",
            "Gx-Version": __version__,
        }

        response = requests.get(ge_cloud_url, headers=headers)
        if response.status_code != 200:
            raise ge_exceptions.GXCloudError(
                f"Bad request made to GE Cloud; {response.text}"
            )
        config = response.json()
        return DataContextConfig(**config)

    @classmethod
    def all_uncommitted_directories_exist(cls, ge_dir: str) -> bool:
        """Check if all uncommitted directories exist."""
        uncommitted_dir = os.path.join(ge_dir, cls.GE_UNCOMMITTED_DIR)
        for directory in cls.UNCOMMITTED_DIRECTORIES:
            if not os.path.isdir(os.path.join(uncommitted_dir, directory)):
                return False

        return True

    @classmethod
    def config_variables_yml_exist(cls, ge_dir: str) -> bool:
        """Check if all config_variables.yml exists."""
        path_to_yml = os.path.join(ge_dir, cls.GE_YML)

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
        file_path = os.path.join(ge_dir, cls.GE_YML)
        with open(file_path, "w") as template:
            if usage_statistics_enabled:
                template.write(PROJECT_TEMPLATE_USAGE_STATISTICS_ENABLED)
            else:
                template.write(PROJECT_TEMPLATE_USAGE_STATISTICS_DISABLED)

    @classmethod
    def scaffold_directories(cls, base_dir: str) -> None:
        """Safely create GE directories for a new project."""
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

        uncommitted_dir = os.path.join(base_dir, cls.GE_UNCOMMITTED_DIR)

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

    @classmethod
    def find_context_root_dir(cls) -> str:
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

            potential_ge_dir = os.path.join(search_start_dir, cls.GE_DIR)

            if os.path.isdir(potential_ge_dir):
                potential_yml = os.path.join(potential_ge_dir, cls.GE_YML)
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
        return os.path.isfile(os.path.join(context_root_dir, cls.GE_YML))

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
        if not context:
            return False
        return len(context.list_datasources()) >= 1

    @classmethod
    def _does_context_have_at_least_one_suite(cls, ge_dir: str) -> bool:
        context = cls._attempt_context_instantiation(ge_dir)
        if not context:
            return False
        return bool(context.list_expectation_suites())

    @classmethod
    def _attempt_context_instantiation(
        cls, ge_dir: str
    ) -> Optional[SerializableDataContext]:
        try:
            context = cls(ge_dir)
            return context
        except (
            ge_exceptions.DataContextError,
            ge_exceptions.InvalidDataContextConfigError,
        ) as e:
            logger.debug(e)
        return None
