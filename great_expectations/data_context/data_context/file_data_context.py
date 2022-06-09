import configparser
import errno
import json
import logging
import os
import sys
import uuid
from typing import Mapping, Optional, Union, cast

from ruamel.yaml import YAML

from great_expectations.data_context.data_context.abstract_data_context import (
    AbstractDataContext,
)
from great_expectations.data_context.store import Store
from great_expectations.data_context.types.base import (
    DataContextConfig,
    DataContextConfigDefaults,
    anonymizedUsageStatisticsSchema,
)
from great_expectations.data_context.util import (
    substitute_all_config_variables,
    substitute_config_variable,
)

logger = logging.getLogger(__name__)

yaml = YAML()


class FileDataContext(AbstractDataContext):
    """
    Extends AbstractDataContext, contains only functionality necessary to hydrate state from disk.

    TODO: Most of the functionality in DataContext will be refactored into this class, and the current DataContext
    class will exist only for backwards-compatibility reasons.
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
    GE_YML = "great_expectations.yml"
    GE_EDIT_NOTEBOOK_DIR = GE_UNCOMMITTED_DIR

    def __init__(
        self,
        project_config: Union[DataContextConfig, Mapping],
        context_root_dir: Optional[str] = None,
        runtime_environment: Optional[dict] = None,
    ) -> None:

        # since default is None
        if context_root_dir:
            context_root_dir = os.path.abspath(context_root_dir)

        self._context_root_directory = context_root_dir
        self._context_root_dir = context_root_dir
        self.runtime_environment = runtime_environment or {}

        self._project_config = project_config
        self._apply_global_config_overrides()

        # Init plugin support
        if self.plugins_directory is not None and os.path.exists(
            self.plugins_directory
        ):
            sys.path.append(self.plugins_directory)

        super().__init__(
            project_config=project_config, runtime_environment=runtime_environment
        )

    def _apply_global_config_overrides(self) -> None:
        # check for global usage statistics opt out
        validation_errors = {}

        if self._check_global_usage_statistics_opt_out():
            logger.info(
                "Usage statistics is disabled globally. Applying override to project_config."
            )
            self.config.anonymous_usage_statistics.enabled = False

        global_data_context_id = self._get_global_config_value(
            environment_variable="GE_DATA_CONTEXT_ID",
            conf_file_section="anonymous_usage_statistics",
            conf_file_option="data_context_id",
        )
        if global_data_context_id:
            data_context_id_errors = anonymizedUsageStatisticsSchema.validate(
                {"data_context_id": global_data_context_id}
            )
            if not data_context_id_errors:
                logger.info(
                    "data_context_id is defined globally. Applying override to project_config."
                )
                # this is the key line
                # TODO: what is the global_data_context_id and usage_statistics_url before this step?
                self.config.anonymous_usage_statistics.data_context_id = (
                    global_data_context_id
                )
            else:
                validation_errors.update(data_context_id_errors)
        # check for global usage_statistics url
        global_usage_statistics_url = self._get_global_config_value(
            environment_variable="GE_USAGE_STATISTICS_URL",
            conf_file_section="anonymous_usage_statistics",
            conf_file_option="usage_statistics_url",
        )
        if global_usage_statistics_url:
            usage_statistics_url_errors = anonymizedUsageStatisticsSchema.validate(
                {"usage_statistics_url": global_usage_statistics_url}
            )
            if not usage_statistics_url_errors:
                logger.info(
                    "usage_statistics_url is defined globally. Applying override to project_config."
                )
                # this is the key line
                self.config.anonymous_usage_statistics.usage_statistics_url = (
                    global_usage_statistics_url
                )
            else:
                validation_errors.update(usage_statistics_url_errors)
        if validation_errors:
            logger.warning(
                "The following globally-defined config variables failed validation:\n{}\n\n"
                "Please fix the variables if you would like to apply global values to project_config.".format(
                    json.dumps(validation_errors, indent=2)
                )
            )

    @property
    def instance_id(self):
        instance_id = self._load_config_variables_file().get("instance_id")
        if instance_id is None:
            if self._in_memory_instance_id is not None:
                return self._in_memory_instance_id
            instance_id = str(uuid.uuid4())
            self._in_memory_instance_id = instance_id
        return instance_id

    @classmethod
    def _get_global_config_value(
        cls,
        environment_variable: Optional[str] = None,
        conf_file_section=None,
        conf_file_option=None,
    ) -> Optional[str]:
        """
        Returns global config value from environment variable or None
        Args:
            environment_variable (str): variable to return
        Returns:
            value of env variable or None
        """
        assert (conf_file_section and conf_file_option) or (
            not conf_file_section and not conf_file_option
        ), "Must pass both 'conf_file_section' and 'conf_file_option' or neither."
        if environment_variable and os.environ.get(environment_variable, False):
            return os.environ.get(environment_variable)
        if conf_file_section and conf_file_option:
            for config_path in AbstractDataContext.GLOBAL_CONFIG_PATHS:
                config = configparser.ConfigParser()
                config.read(config_path)
                config_value = config.get(
                    conf_file_section, conf_file_option, fallback=None
                )
                if config_value:
                    return config_value
        return None

    def _build_store_from_config(
        self,
        store_name: str,
        store_config: dict,
        runtime_environment: Optional[dict] = None,
    ) -> Optional[Store]:
        runtime_environment: dict = {
            "root_directory": self.root_directory,
        }
        return super()._build_store_from_config(
            store_name=store_name,
            store_config=store_config,
            runtime_environment=runtime_environment,
        )

    def _load_config_variables_file(self):
        """
        Get all config variables from the default location. For Data Contexts in GE Cloud mode, config variables
        have already been interpolated before being sent from the Cloud API.
        """
        # can this become get_config() --> config?
        config: DataContextConfig = cast(DataContextConfig, self.config)
        if (
            hasattr(config, "config_variables_file_path")
            and config.config_variables_file_path
        ):
            try:
                # If the user specifies the config variable path with an environment variable, we want to substitute it
                defined_path = substitute_config_variable(
                    config.config_variables_file_path, dict(os.environ)
                )
                if not os.path.isabs(defined_path):
                    # A BaseDataContext will not have a root directory; in that case use the current directory
                    # for any non-absolute path
                    root_directory = self.root_directory or os.curdir
                else:
                    root_directory = ""
                var_path = os.path.join(root_directory, defined_path)
                with open(var_path) as config_variables_file:
                    return yaml.load(config_variables_file) or {}
            except OSError as e:
                if e.errno != errno.ENOENT:
                    raise
                logger.debug("Generating empty config variables file.")
                return {}
        else:
            return super()._load_config_variables_file()

    # properties
    @property
    def root_directory(self):
        """The root directory for configuration objects in the data context; the location in which
        ``great_expectations.yml`` is located."""
        return self._context_root_directory

    @property
    def plugins_directory(self):
        """The directory in which custom plugin modules should be placed."""
        return self._normalize_absolute_or_relative_path(
            self.project_config_with_variables_substituted.plugins_directory
        )

    @property
    def config_variables(self):
        # Note Abe 20121114 : We should probably cache config_variables instead of loading them from disk every time.
        return dict(self._load_config_variables_file())

    # private methods
    def _normalize_absolute_or_relative_path(
        self, path: Optional[str]
    ) -> Optional[str]:
        if path is None:
            return
        if os.path.isabs(path):
            return path
        else:
            return os.path.join(self.root_directory, path)

    def get_config_with_variables_substituted(self, config=None) -> DataContextConfig:
        """
        Substitute vars in config of form ${var} or $(var) with values found in the following places,
        in order of precedence: ge_cloud_config (for Data Contexts in GE Cloud mode), runtime_environment,
        environment variables, config_variables, or ge_cloud_config_variable_defaults (allows certain variables to
        be optional in GE Cloud mode).
        """
        if not config:
            config = self.config

        substituted_config_variables = substitute_all_config_variables(
            self.config_variables,
            dict(os.environ),
            self.DOLLAR_SIGN_ESCAPE_STRING,
        )

        # Substitutions should have already occurred for GE Cloud configs at this point
        substitutions = {
            **substituted_config_variables,
            **dict(os.environ),
            **self.runtime_environment,
        }

        return DataContextConfig(
            **substitute_all_config_variables(
                config, substitutions, self.DOLLAR_SIGN_ESCAPE_STRING
            )
        )
