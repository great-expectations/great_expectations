import logging
import os
import shutil
import warnings
from typing import Dict, Optional, Union

import requests
from ruamel.yaml import YAML, YAMLError
from ruamel.yaml.constructor import DuplicateKeyError

import great_expectations.exceptions as ge_exceptions
from great_expectations.data_context.data_context.base_data_context import (
    BaseDataContext,
)
from great_expectations.data_context.templates import (
    CONFIG_VARIABLES_TEMPLATE,
    PROJECT_TEMPLATE_USAGE_STATISTICS_DISABLED,
    PROJECT_TEMPLATE_USAGE_STATISTICS_ENABLED,
)
from great_expectations.data_context.types.base import (
    CURRENT_GE_CONFIG_VERSION,
    MINIMUM_SUPPORTED_CONFIG_VERSION,
    DataContextConfig,
    GeCloudConfig,
    dataContextConfigSchema,
)
from great_expectations.data_context.util import file_relative_path
from great_expectations.datasource import LegacyDatasource
from great_expectations.datasource.new_datasource import BaseDatasource
from great_expectations.exceptions import DataContextError

logger = logging.getLogger(__name__)
yaml = YAML()
yaml.indent(mapping=2, sequence=4, offset=2)
yaml.default_flow_style = False


# TODO: <WILL> Most of the logic here will be migrated to FileDataContext
class DataContext(BaseDataContext):
    """A DataContext represents a Great Expectations project. It organizes storage and access for
    expectation suites, datasources, notification settings, and data fixtures.

    The DataContext is configured via a yml file stored in a directory called great_expectations; the configuration file
    as well as managed expectation suites should be stored in version control.

    Use the `create` classmethod to create a new empty config, or instantiate the DataContext
    by passing the path to an existing data context root directory.

    DataContexts use data sources you're already familiar with. BatchKwargGenerators help introspect data stores and data execution
    frameworks (such as airflow, Nifi, dbt, or dagster) to describe and produce batches of data ready for analysis. This
    enables fetching, validation, profiling, and documentation of  your data in a way that is meaningful within your
    existing infrastructure and work environment.

    DataContexts use a datasource-based namespace, where each accessible type of data has a three-part
    normalized *data_asset_name*, consisting of *datasource/generator/data_asset_name*.

    - The datasource actually connects to a source of materialized data and returns Great Expectations DataAssets \
      connected to a compute environment and ready for validation.

    - The BatchKwargGenerator knows how to introspect datasources and produce identifying "batch_kwargs" that define \
      particular slices of data.

    - The data_asset_name is a specific name -- often a table name or other name familiar to users -- that \
      batch kwargs generators can slice into batches.

    An expectation suite is a collection of expectations ready to be applied to a batch of data. Since
    in many projects it is useful to have different expectations evaluate in different contexts--profiling
    vs. testing; warning vs. error; high vs. low compute; ML model or dashboard--suites provide a namespace
    option for selecting which expectations a DataContext returns.

    In many simple projects, the datasource or batch kwargs generator name may be omitted and the DataContext will infer
    the correct name when there is no ambiguity.

    Similarly, if no expectation suite name is provided, the DataContext will assume the name "default".
    """

    @classmethod
    def create(
        cls,
        project_root_dir: Optional[str] = None,
        usage_statistics_enabled: bool = True,
        runtime_environment: Optional[dict] = None,
    ) -> "DataContext":
        """
        Build a new great_expectations directory and DataContext object in the provided project_root_dir.

        `create` will not create a new "great_expectations" directory in the provided folder, provided one does not
        already exist. Then, it will initialize a new DataContext in that folder and write the resulting config.

        Args:
            project_root_dir: path to the root directory in which to create a new great_expectations directory
            usage_statistics_enabled: boolean directive specifying whether or not to gather usage statistics
            runtime_environment: a dictionary of config variables that
            override both those set in config_variables.yml and the environment

        Returns:
            DataContext
        """

        if not os.path.isdir(project_root_dir):
            raise ge_exceptions.DataContextError(
                "The project_root_dir must be an existing directory in which "
                "to initialize a new DataContext"
            )

        ge_dir = os.path.join(project_root_dir, cls.GE_DIR)
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

        return cls(ge_dir, runtime_environment=runtime_environment)

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

    # TODO: deprecate ge_cloud_account_id
    @classmethod
    def _get_ge_cloud_config_dict(
        cls,
        ge_cloud_base_url: Optional[str] = None,
        ge_cloud_account_id: Optional[str] = None,
        ge_cloud_access_token: Optional[str] = None,
        ge_cloud_organization_id: Optional[str] = None,
    ) -> Dict[str, Optional[str]]:
        ge_cloud_base_url = (
            ge_cloud_base_url
            or super()._get_global_config_value(
                environment_variable="GE_CLOUD_BASE_URL",
                conf_file_section="ge_cloud_config",
                conf_file_option="base_url",
            )
            or "https://app.greatexpectations.io/"
        )

        # TODO: remove if/else block when ge_cloud_account_id is deprecated.
        if ge_cloud_account_id is not None:
            logger.warning(
                'The "ge_cloud_account_id" argument has been renamed "ge_cloud_organization_id" and will be '
                "deprecated in the next major release."
            )
        else:
            ge_cloud_account_id = super()._get_global_config_value(
                environment_variable="GE_CLOUD_ACCOUNT_ID",
                conf_file_section="ge_cloud_config",
                conf_file_option="account_id",
            )

        if ge_cloud_organization_id is None:
            ge_cloud_organization_id = super()._get_global_config_value(
                environment_variable="GE_CLOUD_ORGANIZATION_ID",
                conf_file_section="ge_cloud_config",
                conf_file_option="organization_id",
            )

        ge_cloud_organization_id = ge_cloud_organization_id or ge_cloud_account_id
        ge_cloud_access_token = (
            ge_cloud_access_token
            or super()._get_global_config_value(
                environment_variable="GE_CLOUD_ACCESS_TOKEN",
                conf_file_section="ge_cloud_config",
                conf_file_option="access_token",
            )
        )
        return {
            "base_url": ge_cloud_base_url,
            "organization_id": ge_cloud_organization_id,
            "access_token": ge_cloud_access_token,
        }

    # TODO: deprecate ge_cloud_account_id
    def get_ge_cloud_config(
        self,
        ge_cloud_base_url: Optional[str] = None,
        ge_cloud_account_id: Optional[str] = None,
        ge_cloud_access_token: Optional[str] = None,
        ge_cloud_organization_id: Optional[str] = None,
    ) -> GeCloudConfig:
        """
        Build a GeCloudConfig object. Config attributes are collected from any combination of args passed in at
        runtime, environment variables, or a global great_expectations.conf file (in order of precedence)
        """
        ge_cloud_config_dict = self._get_ge_cloud_config_dict(
            ge_cloud_base_url=ge_cloud_base_url,
            ge_cloud_account_id=ge_cloud_account_id,
            ge_cloud_access_token=ge_cloud_access_token,
            ge_cloud_organization_id=ge_cloud_organization_id,
        )

        missing_keys = []
        for key, val in ge_cloud_config_dict.items():
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

        return GeCloudConfig(**ge_cloud_config_dict)

    # TODO: deprecate ge_cloud_account_id
    def __init__(
        self,
        context_root_dir: Optional[str] = None,
        runtime_environment: Optional[dict] = None,
        ge_cloud_mode: bool = False,
        ge_cloud_base_url: Optional[str] = None,
        ge_cloud_account_id: Optional[str] = None,
        ge_cloud_access_token: Optional[str] = None,
        ge_cloud_organization_id: Optional[str] = None,
    ) -> None:
        self._ge_cloud_mode = ge_cloud_mode
        self._ge_cloud_config = None
        ge_cloud_config = None

        if ge_cloud_mode:
            ge_cloud_config = self.get_ge_cloud_config(
                ge_cloud_base_url=ge_cloud_base_url,
                ge_cloud_account_id=ge_cloud_account_id,
                ge_cloud_access_token=ge_cloud_access_token,
                ge_cloud_organization_id=ge_cloud_organization_id,
            )
            self._ge_cloud_config = ge_cloud_config
            # in ge_cloud_mode, if not provided, set context_root_dir to cwd
            if context_root_dir is None:
                context_root_dir = os.getcwd()
                logger.info(
                    f'context_root_dir was not provided - defaulting to current working directory "'
                    f'{context_root_dir}".'
                )
        else:
            # Determine the "context root directory" - this is the parent of "great_expectations" dir
            context_root_dir = (
                self.find_context_root_dir()
                if context_root_dir is None
                else context_root_dir
            )

        context_root_directory = os.path.abspath(os.path.expanduser(context_root_dir))
        self._context_root_directory = context_root_directory

        project_config = self._load_project_config()
        super().__init__(
            project_config,
            context_root_directory,
            runtime_environment,
            ge_cloud_mode=ge_cloud_mode,
            ge_cloud_config=ge_cloud_config,
        )

        # save project config if data_context_id auto-generated or global config values applied
        project_config_dict = dataContextConfigSchema.dump(project_config)
        if (
            project_config.anonymous_usage_statistics.explicit_id is False
            or project_config_dict != dataContextConfigSchema.dump(self.config)
        ):
            self._save_project_config()

    def _retrieve_data_context_config_from_ge_cloud(self) -> DataContextConfig:
        """
        Utilizes the GeCloudConfig instantiated in the constructor to create a request to the Cloud API.
        Given proper authorization, the request retrieves a data context config that is pre-populated with
        GE objects specific to the user's Cloud environment (datasources, data connectors, etc).

        Please note that substitution for ${VAR} variables is performed in GE Cloud before being sent
        over the wire.

        :return: the configuration object retrieved from the Cloud API
        """
        ge_cloud_url = (
            self.ge_cloud_config.base_url
            + f"/organizations/{self.ge_cloud_config.organization_id}/data-context-configuration"
        )
        auth_headers = {
            "Content-Type": "application/vnd.api+json",
            "Authorization": f"Bearer {self.ge_cloud_config.access_token}",
        }

        response = requests.get(ge_cloud_url, headers=auth_headers)
        if response.status_code != 200:
            raise ge_exceptions.GeCloudError(
                f"Bad request made to GE Cloud; {response.text}"
            )
        config = response.json()
        return DataContextConfig(**config)

    def _load_project_config(self):
        """
        Reads the project configuration from the project configuration file.
        The file may contain ${SOME_VARIABLE} variables - see self.project_config_with_variables_substituted
        for how these are substituted.

        For Data Contexts in GE Cloud mode, a user-specific template is retrieved from the Cloud API
        - see self._retrieve_data_context_config_from_ge_cloud for more details.

        :return: the configuration object read from the file or template
        """
        if self.ge_cloud_mode:
            config = self._retrieve_data_context_config_from_ge_cloud()
            return config

        path_to_yml = os.path.join(self.root_directory, self.GE_YML)
        try:
            with open(path_to_yml) as data:
                config_commented_map_from_yaml = yaml.load(data)

        except YAMLError as err:
            raise ge_exceptions.InvalidConfigurationYamlError(
                "Your configuration file is not a valid yml file likely due to a yml syntax error:\n\n{}".format(
                    err
                )
            )
        except DuplicateKeyError:
            raise ge_exceptions.InvalidConfigurationYamlError(
                "Error: duplicate key found in project YAML file."
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

    def _save_project_config(self):
        """Save the current project to disk."""
        if self.ge_cloud_mode:
            logger.debug(
                "ge_cloud_mode detected - skipping DataContext._save_project_config"
            )
            return
        logger.debug("Starting DataContext._save_project_config")

        config_filepath = os.path.join(self.root_directory, self.GE_YML)
        with open(config_filepath, "w") as outfile:
            self.config.to_yaml(outfile)

    def add_store(self, store_name, store_config):
        logger.debug(f"Starting DataContext.add_store for store {store_name}")

        new_store = super().add_store(store_name, store_config)
        self._save_project_config()
        return new_store

    def add_datasource(
        self, name, **kwargs
    ) -> Optional[Union[LegacyDatasource, BaseDatasource]]:
        logger.debug(f"Starting DataContext.add_datasource for datasource {name}")

        new_datasource: Optional[
            Union[LegacyDatasource, BaseDatasource]
        ] = super().add_datasource(name=name, **kwargs)
        self._save_project_config()

        return new_datasource

    def delete_datasource(self, name: str) -> None:
        logger.debug(f"Starting DataContext.delete_datasource for datasource {name}")

        super().delete_datasource(datasource_name=name)
        self._save_project_config()

    @classmethod
    def find_context_root_dir(cls):
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
    def get_ge_config_version(cls, context_root_dir=None):
        yml_path = cls.find_context_yml_file(search_start_dir=context_root_dir)
        if yml_path is None:
            return

        with open(yml_path) as f:
            config_commented_map_from_yaml = yaml.load(f)

        config_version = config_commented_map_from_yaml.get("config_version")
        return float(config_version) if config_version else None

    @classmethod
    def set_ge_config_version(
        cls, config_version, context_root_dir=None, validate_config_version=True
    ):
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
            elif config_version > CURRENT_GE_CONFIG_VERSION:
                raise ge_exceptions.UnsupportedConfigVersionError(
                    "Invalid config version ({}).\n    The maximum valid version is {}.".format(
                        config_version, CURRENT_GE_CONFIG_VERSION
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
    def find_context_yml_file(cls, search_start_dir=None):
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
    def does_config_exist_on_disk(cls, context_root_dir):
        """Return True if the great_expectations.yml exists on disk."""
        return os.path.isfile(os.path.join(context_root_dir, cls.GE_YML))

    @classmethod
    def is_project_initialized(cls, ge_dir):
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
    def does_project_have_a_datasource_in_config_file(cls, ge_dir):
        if not cls.does_config_exist_on_disk(ge_dir):
            return False
        return cls._does_context_have_at_least_one_datasource(ge_dir)

    @classmethod
    def _does_context_have_at_least_one_datasource(cls, ge_dir):
        context = cls._attempt_context_instantiation(ge_dir)
        if not isinstance(context, DataContext):
            return False
        return len(context.list_datasources()) >= 1

    @classmethod
    def _does_context_have_at_least_one_suite(cls, ge_dir):
        context = cls._attempt_context_instantiation(ge_dir)
        if not isinstance(context, DataContext):
            return False
        return len(context.list_expectation_suites()) >= 1

    @classmethod
    def _attempt_context_instantiation(cls, ge_dir):
        try:
            context = DataContext(ge_dir)
            return context
        except (
            ge_exceptions.DataContextError,
            ge_exceptions.InvalidDataContextConfigError,
        ) as e:
            logger.debug(e)
