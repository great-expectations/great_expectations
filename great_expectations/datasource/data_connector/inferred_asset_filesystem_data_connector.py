import logging
from pathlib import Path
from typing import List, Optional

from great_expectations.datasource.data_connector.inferred_asset_file_path_data_connector import (
    InferredAssetFilePathDataConnector,
)
from great_expectations.datasource.data_connector.util import (
    get_filesystem_one_level_directory_glob_path_list,
    normalize_directory_path,
)
from great_expectations.execution_engine import ExecutionEngine

logger = logging.getLogger(__name__)


class InferredAssetFilesystemDataConnector(InferredAssetFilePathDataConnector):
    "\n    Extension of InferredAssetFilePathDataConnector used to connect to data on a filesystem.\n\n    The InferredAssetFilesystemDataConnector is one of two classes (ConfiguredAssetFilesystemDataConnector being the\n    other one) designed for connecting to data on a filesystem. It connects to assets\n    inferred from directory and file name by default_regex and glob_directive.\n\n    InferredAssetFilesystemDataConnector that operates on file paths and determines\n    the data_asset_name implicitly (e.g., through the combination of the regular expressions pattern and group names)\n\n"

    def __init__(
        self,
        name: str,
        datasource_name: str,
        base_directory: str,
        execution_engine: Optional[ExecutionEngine] = None,
        default_regex: Optional[dict] = None,
        glob_directive: str = "*",
        sorters: Optional[list] = None,
        batch_spec_passthrough: Optional[dict] = None,
    ) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        Base class for DataConnectors that connect to filesystem-like data. This class supports the configuration of default_regex\n        and sorters for filtering and sorting data_references.\n\n        Args:\n            name (str): name of InferredAssetFilesystemDataConnector\n            datasource_name (str): Name of datasource that this DataConnector is connected to\n            base_directory(str): base_directory for DataConnector to begin reading files\n            execution_engine (ExecutionEngine): ExecutionEngine object to actually read the data\n            default_regex (dict): Optional dict the filter and organize the data_references.\n            glob_directive (str): glob for selecting files in directory (defaults to *) or nested directories (e.g. */*.csv)\n            sorters (list): Optional list if you want to sort the data_references\n            batch_spec_passthrough (dict): dictionary with keys that will be added directly to batch_spec\n        "
        logger.debug(f'Constructing InferredAssetFilesystemDataConnector "{name}".')
        super().__init__(
            name=name,
            datasource_name=datasource_name,
            execution_engine=execution_engine,
            default_regex=default_regex,
            sorters=sorters,
            batch_spec_passthrough=batch_spec_passthrough,
        )
        self._base_directory = base_directory
        self._glob_directive = glob_directive

    def _get_data_reference_list(
        self, data_asset_name: Optional[str] = None
    ) -> List[str]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        List objects in the underlying data store to create a list of data_references.\n\n        This method is used to refresh the cache.\n        "
        path_list: List[str] = get_filesystem_one_level_directory_glob_path_list(
            base_directory_path=self.base_directory, glob_directive=self._glob_directive
        )
        return sorted(path_list)

    def _get_full_file_path(
        self, path: str, data_asset_name: Optional[str] = None
    ) -> str:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return str(Path(self.base_directory).joinpath(path))

    @property
    def base_directory(self):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        Accessor method for base_directory. If directory is a relative path, interpret it as relative to the\n        root directory. If it is absolute, then keep as-is.\n        "
        return normalize_directory_path(
            dir_path=self._base_directory,
            root_directory_path=self.data_context_root_directory,
        )
