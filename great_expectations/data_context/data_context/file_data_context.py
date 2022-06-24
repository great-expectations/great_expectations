import errno
import logging
import os
from typing import Mapping, Optional, Union, cast

from ruamel.yaml import YAML

from great_expectations.data_context.data_context.abstract_data_context import (
    AbstractDataContext,
)
from great_expectations.data_context.types.base import DataContextConfig
from great_expectations.data_context.types.data_context_variables import (
    FileDataContextVariables,
)
from great_expectations.data_context.util import substitute_config_variable

logger = logging.getLogger(__name__)

# TODO: check if this can be refactored to use YAMLHandler class
yaml = YAML()
yaml.indent(mapping=2, sequence=4, offset=2)
yaml.default_flow_style = False


class FileDataContext(AbstractDataContext):
    """
    Extends AbstractDataContext, contains only functionality necessary to hydrate state from disk.

    TODO: Most of the functionality in DataContext will be refactored into this class, and the current DataContext
    class will exist only for backwards-compatibility reasons.
    """

    def __init__(
        self,
        project_config: Union[DataContextConfig, Mapping],
        context_root_dir: Optional[str] = None,
        runtime_environment: Optional[dict] = None,
    ) -> None:
        """FileDataContext constructor

        Args:
            project_config (DataContextConfig):  Config for current DataContext
            context_root_dir (Optional[str]): location to look for the ``great_expectations.yml`` file. If None,
                searches for the file based on conventions for project subdirectories.
            runtime_environment (Optional[dict]): a dictionary of config variables that override both those set in
                config_variables.yml and the environment
        """
        super().__init__(runtime_environment=runtime_environment)
        # move this down here too
        if context_root_dir is not None:
            context_root_dir = os.path.abspath(context_root_dir)
        self._context_root_directory = context_root_dir

        self._context_root_dir = context_root_dir
        self._project_config = self._apply_global_config_overrides(
            config=project_config
        )

    def _init_variables(self) -> FileDataContextVariables:
        raise NotImplementedError

    def _load_config_variables(self) -> dict:
        """
        Get all config variables from the default location. For Data Contexts in GE Cloud mode, config variables
        have already been interpolated before being sent from the Cloud API.
        """
        print("runningg me!!!")
        help = self.get_config()
        print(f"help me : {help}")
        config_variables_file_path = cast(
            DataContextConfig, self.get_config()
        ).config_variables_file_path
        print(f"running me :{config_variables_file_path}")
        if config_variables_file_path:
            try:
                # If the user specifies the config variable path with an environment variable, we want to substitute it
                defined_path = substitute_config_variable(
                    config_variables_file_path, dict(os.environ)
                )
                if not os.path.isabs(defined_path):
                    # TODO: is this something that needs to be revisited?
                    # A BaseDataContext will not have a root directory; in that case use the current directory
                    # for any non-absolute path
                    root_directory = self.root_directory or os.curdir
                else:
                    root_directory = ""
                var_path = os.path.join(root_directory, defined_path)
                with open(var_path) as config_variables_file:
                    res = yaml.load(config_variables_file)
                    print(f"this is res: {res}")
                    return res or {}
            except OSError as e:
                if e.errno != errno.ENOENT:
                    raise
                logger.debug("Generating empty config variables file.")
                return {}
        else:
            return {}

    @property
    def root_directory(self) -> str:
        """The root directory for configuration objects in the data context; the location in which
        ``great_expectations.yml`` is located."""
        return self._context_root_directory
