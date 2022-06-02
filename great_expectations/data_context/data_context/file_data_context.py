import os
from typing import Mapping, Optional, Union

from great_expectations.data_context.data_context.abstract_data_context import (
    AbstractDataContext,
)
from great_expectations.data_context.types.base import (
    DataContextConfig,
    DataContextConfigDefaults,
)


class FileDataContext(AbstractDataContext):
    """
    Extends AbstractDataContext, contains only functionality necessary to hydrate state from disk.

    TODO: Most of the functionality in DataContext will be refactored into this class, and the current DataContext
    class will exist only for backwards-compatibility reasons.
    """

    GLOBAL_CONFIG_PATHS = [
        os.path.expanduser("~/.great_expectations/great_expectations.conf"),
        "/etc/great_expectations.conf",
    ]
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
        self._context_root_dir = context_root_dir
        super().__init__(
            project_config=project_config, runtime_environment=runtime_environment
        )
