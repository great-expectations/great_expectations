from __future__ import annotations

import logging
import os
import pathlib
import shutil
import warnings
from typing import TYPE_CHECKING, ClassVar, Optional, Union

from ruamel.yaml import YAML

import great_expectations.exceptions as gx_exceptions
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
)
from great_expectations.data_context.util import file_relative_path

logger = logging.getLogger(__name__)
yaml = YAML()
yaml.indent(mapping=2, sequence=4, offset=2)
yaml.default_flow_style = False

if TYPE_CHECKING:
    from great_expectations.alias_types import JSONValues, PathStr


class SerializableDataContext(AbstractDataContext):
    """
    TODO - write docstring!
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
    GX_DIR: ClassVar[str] = "great_expectations"
    GX_YML: ClassVar[str] = "great_expectations.yml"
    GX_EDIT_NOTEBOOK_DIR = GX_UNCOMMITTED_DIR
    DOLLAR_SIGN_ESCAPE_STRING = r"\$"

    def __init__(
        self,
        context_root_dir: PathStr,
        runtime_environment: Optional[dict] = None,
    ) -> None:
        if isinstance(context_root_dir, pathlib.Path):
            # TODO: (kilo59) 122022 should be saving and passing around `pathlib.Path` not str
            context_root_dir = str(context_root_dir)
        self._context_root_directory = context_root_dir
        super().__init__(runtime_environment=runtime_environment)

    def _init_datasource_store(self):
        raise NotImplementedError  # Required by parent ABC but this class is never instantiated

    def _init_variables(self):
        raise NotImplementedError  # Required by parent ABC but this class is never instantiated

    def _save_project_config(self) -> None:
        """
        See parent 'AbstractDataContext._save_project_config()` for more information.

        Explicitly override base class implementation to retain legacy behavior.
        """
        config_filepath = os.path.join(self.root_directory, self.GX_YML)  # type: ignore[arg-type]

        logger.debug(
            f"Starting DataContext._save_project_config; attempting to update {config_filepath}"
        )

        try:
            with open(config_filepath, "w") as outfile:

                zep_datasources = self._synchronize_zep_datasources()
                if zep_datasources:
                    self.zep_config.datasources.update(zep_datasources)
                    logger.info(
                        f"Saving {len(self.zep_config.datasources)} ZEP Datasources to {config_filepath}"
                    )
                    zep_json_dict: dict[str, JSONValues] = self.zep_config._json_dict()
                    self.config._commented_map.update(zep_json_dict)

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
        project_root_dir: Optional[PathStr] = None,
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
            raise gx_exceptions.DataContextError(
                "The project_root_dir must be an existing directory in which "
                "to initialize a new DataContext"
            )

        gx_dir = os.path.join(project_root_dir, cls.GX_DIR)  # type: ignore[arg-type]
        os.makedirs(gx_dir, exist_ok=True)
        cls._scaffold_directories(gx_dir)

        if os.path.isfile(os.path.join(gx_dir, cls.GX_YML)):
            message = f"""Warning. An existing `{cls.GX_YML}` was found here: {gx_dir}.
    - No action was taken."""
            warnings.warn(message)
        else:
            cls._write_project_template_to_disk(gx_dir, usage_statistics_enabled)

        uncommitted_dir = os.path.join(gx_dir, cls.GX_UNCOMMITTED_DIR)
        if os.path.isfile(os.path.join(uncommitted_dir, "config_variables.yml")):
            message = """Warning. An existing `config_variables.yml` was found here: {}.
    - No action was taken.""".format(
                uncommitted_dir
            )
            warnings.warn(message)
        else:
            cls._write_config_variables_template_to_disk(uncommitted_dir)

        return cls(context_root_dir=gx_dir, runtime_environment=runtime_environment)

    @classmethod
    def all_uncommitted_directories_exist(cls, gx_dir: PathStr) -> bool:
        """Check if all uncommitted directories exist."""
        uncommitted_dir = os.path.join(gx_dir, cls.GX_UNCOMMITTED_DIR)
        for directory in cls.UNCOMMITTED_DIRECTORIES:
            if not os.path.isdir(os.path.join(uncommitted_dir, directory)):
                return False

        return True

    @classmethod
    def config_variables_yml_exist(cls, gx_dir: PathStr) -> bool:
        """Check if all config_variables.yml exists."""
        path_to_yml = os.path.join(gx_dir, cls.GX_YML)

        # TODO this is so brittle and gross
        with open(path_to_yml) as f:
            config = yaml.load(f)
        config_var_path = config.get("config_variables_file_path")
        config_var_path = os.path.join(gx_dir, config_var_path)
        return os.path.isfile(config_var_path)

    @classmethod
    def _write_config_variables_template_to_disk(cls, uncommitted_dir: str) -> None:
        os.makedirs(uncommitted_dir, exist_ok=True)
        config_var_file = os.path.join(uncommitted_dir, "config_variables.yml")
        with open(config_var_file, "w") as template:
            template.write(CONFIG_VARIABLES_TEMPLATE)

    @classmethod
    def _write_project_template_to_disk(
        cls, gx_dir: PathStr, usage_statistics_enabled: bool = True
    ) -> None:
        file_path = os.path.join(gx_dir, cls.GX_YML)
        with open(file_path, "w") as template:
            if usage_statistics_enabled:
                template.write(PROJECT_TEMPLATE_USAGE_STATISTICS_ENABLED)
            else:
                template.write(PROJECT_TEMPLATE_USAGE_STATISTICS_DISABLED)

    @classmethod
    def _scaffold_directories(cls, base_dir: PathStr) -> None:
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
                cls._scaffold_custom_data_docs(plugins_dir)
            else:
                os.makedirs(os.path.join(base_dir, directory), exist_ok=True)

        uncommitted_dir = os.path.join(base_dir, cls.GX_UNCOMMITTED_DIR)

        for new_directory in cls.UNCOMMITTED_DIRECTORIES:
            new_directory_path = os.path.join(uncommitted_dir, new_directory)
            os.makedirs(new_directory_path, exist_ok=True)

    @classmethod
    def _scaffold_custom_data_docs(cls, plugins_dir: PathStr) -> None:
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
        """
        TODO
        """
        result = None
        yml_path = None
        gx_home_environment = os.getenv("GX_HOME")
        if gx_home_environment:
            gx_home_environment = os.path.expanduser(gx_home_environment)
            if os.path.isdir(gx_home_environment) and os.path.isfile(
                os.path.join(gx_home_environment, "great_expectations.yml")
            ):
                result = gx_home_environment
        else:
            yml_path = cls._find_context_yml_file()
            if yml_path:
                result = os.path.dirname(yml_path)

        if result is None:
            raise gx_exceptions.ConfigNotFoundError()

        logger.debug(f"Using project config: {yml_path}")
        return result

    @classmethod
    def get_ge_config_version(
        cls, context_root_dir: Optional[PathStr] = None
    ) -> Optional[float]:
        """
        TODO
        """
        yml_path = cls._find_context_yml_file(search_start_dir=context_root_dir)
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
        """
        TODO
        """
        if not isinstance(config_version, (int, float)):
            raise gx_exceptions.UnsupportedConfigVersionError(
                "The argument `config_version` must be a number.",
            )

        if validate_config_version:
            if config_version < MINIMUM_SUPPORTED_CONFIG_VERSION:
                raise gx_exceptions.UnsupportedConfigVersionError(
                    "Invalid config version ({}).\n    The version number must be at least {}. ".format(
                        config_version, MINIMUM_SUPPORTED_CONFIG_VERSION
                    ),
                )
            elif config_version > CURRENT_GX_CONFIG_VERSION:
                raise gx_exceptions.UnsupportedConfigVersionError(
                    "Invalid config version ({}).\n    The maximum valid version is {}.".format(
                        config_version, CURRENT_GX_CONFIG_VERSION
                    ),
                )

        yml_path = cls._find_context_yml_file(search_start_dir=context_root_dir)
        if yml_path is None:
            return False

        with open(yml_path) as f:
            config_commented_map_from_yaml = yaml.load(f)
            config_commented_map_from_yaml["config_version"] = float(config_version)

        with open(yml_path, "w") as f:
            yaml.dump(config_commented_map_from_yaml, f)

        return True

    @classmethod
    def _find_context_yml_file(
        cls, search_start_dir: Optional[PathStr] = None
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
    def does_config_exist_on_disk(cls, context_root_dir: PathStr) -> bool:
        """Return True if the great_expectations.yml exists on disk."""
        return os.path.isfile(os.path.join(context_root_dir, cls.GX_YML))

    @classmethod
    def is_project_initialized(cls, ge_dir: PathStr) -> bool:
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
    def _does_project_have_a_datasource_in_config_file(cls, ge_dir: PathStr) -> bool:
        if not cls.does_config_exist_on_disk(ge_dir):
            return False
        return cls._does_context_have_at_least_one_datasource(ge_dir)

    @classmethod
    def _does_context_have_at_least_one_datasource(cls, ge_dir: PathStr) -> bool:
        context = cls._attempt_context_instantiation(ge_dir)
        if not context:
            return False
        return len(context.list_datasources()) >= 1

    @classmethod
    def _does_context_have_at_least_one_suite(cls, ge_dir: PathStr) -> bool:
        context = cls._attempt_context_instantiation(ge_dir)
        if not context:
            return False
        return bool(context.list_expectation_suites())

    @classmethod
    def _attempt_context_instantiation(
        cls, ge_dir: PathStr
    ) -> Optional[SerializableDataContext]:
        try:
            context = cls(context_root_dir=ge_dir)
            return context
        except (
            gx_exceptions.DataContextError,
            gx_exceptions.InvalidDataContextConfigError,
        ) as e:
            logger.debug(e)
        return None
