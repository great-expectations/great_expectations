import logging
from typing import Mapping, Optional, Union

from great_expectations.data_context.data_context.abstract_data_context import (
    AbstractDataContext,
)
from great_expectations.data_context.data_context_variables import (
    FileDataContextVariables,
)
from great_expectations.data_context.types.base import DataContextConfig

logger = logging.getLogger(__name__)


class FileDataContext(AbstractDataContext):
    """
    Extends AbstractDataContext, contains only functionality necessary to hydrate state from disk.

    TODO: Most of the functionality in DataContext will be refactored into this class, and the current DataContext
    class will exist only for backwards-compatibility reasons.
    """

    GE_YML = "great_expectations.yml"

    def __init__(
        self,
        project_config: Union[DataContextConfig, Mapping],
        context_root_dir: str,
        runtime_environment: dict,
    ) -> None:
        """FileDataContext constructor

        Args:
            project_config (DataContextConfig):  Config for current DataContext
            context_root_dir (Optional[str]): location to look for the ``great_expectations.yml`` file. If None,
                searches for the file based on conventions for project subdirectories.
            runtime_environment (Optional[dict]): a dictionary of config variables that override both those set in
                config_variables.yml and the environment
        """
        self._context_root_directory: str = context_root_dir
        self._project_config: DataContextConfig = self._apply_global_config_overrides(
            config=project_config
        )
        super().__init__(runtime_environment=runtime_environment)

    def _init_variables(self) -> FileDataContextVariables:
        raise NotImplementedError

    def _save_project_config(self) -> None:
        """Save the current project to disk."""
        logger.debug("Starting FileDataContext._save_project_config")
        config_filepath: str = os.path.join(self.root_directory, self.GE_YML)
        with open(config_filepath, "w") as outfile:
            self.config.to_yaml(outfile)

    @property
    def root_directory(self) -> Optional[str]:
        """The root directory for configuration objects in the data context; the location in which
        ``great_expectations.yml`` is located.

        Why does this exist in AbstractDataContext? CloudDataContext and FileDataContext both use it

        """
        return self._context_root_directory
