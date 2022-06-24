import logging
import os
import sys
from typing import Mapping, Optional, Union

from great_expectations.data_context.data_context.abstract_data_context import (
    AbstractDataContext,
)
from great_expectations.data_context.types.base import DataContextConfig
from great_expectations.data_context.types.data_context_variables import (
    FileDataContextVariables,
)

logger = logging.getLogger(__name__)


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
        self._context_root_directory = context_root_dir
        self._project_config = self._apply_global_config_overrides(
            config=project_config
        )
        if self.plugins_directory is not None and os.path.exists(
            self.plugins_directory
        ):
            sys.path.append(self.plugins_directory)

    # private methods

    def _init_variables(self) -> FileDataContextVariables:
        raise NotImplementedError

    def _normalize_absolute_or_relative_path(
        self, path: Optional[str]
    ) -> Optional[str]:
        if path is None:
            return
        if os.path.isabs(path):
            return path
        else:
            return os.path.join(self.root_directory, path)

    # properties
    @property
    def plugins_directory(self):
        """The directory in which custom plugin modules should be placed."""
        return self._normalize_absolute_or_relative_path(
            self.project_config_with_variables_substituted.plugins_directory
        )

    @property
    def root_directory(self):
        """The root directory for configuration objects in the data context; the location in which
        ``great_expectations.yml`` is located."""
        return self._context_root_directory
