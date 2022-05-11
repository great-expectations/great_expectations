import logging
from typing import Optional

from great_expectations.datasource.data_connector import (
    InferredAssetFilesystemDataConnector,
)
from great_expectations.execution_engine import ExecutionEngine

logger = logging.getLogger(__name__)


class InferredAssetDBFSDataConnector(InferredAssetFilesystemDataConnector):
    "\n    Extension of InferredAssetFilesystemDataConnector used to connect to data on a DBFS filesystem.\n    Note: This works for the current implementation of DBFS. If in the future DBFS diverges from a Filesystem-like implementation, we should instead inherit from InferredAssetFilePathDataConnector or another DataConnector.\n\n    The InferredAssetDBFSDataConnector is one of two classes (ConfiguredAssetDBFSDataConnector being the\n    other one) designed for connecting to data on a DBFS filesystem. It connects to assets\n    inferred from directory and file name by default_regex and glob_directive.\n\n    InferredAssetDBFSDataConnector that operates on file paths and determines\n    the data_asset_name implicitly (e.g., through the combination of the regular expressions pattern and group names)\n"

    def __init__(
        self,
        name: str,
        datasource_name: str,
        base_directory: str,
        execution_engine: ExecutionEngine,
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
        "\n        Base class for DataConnectors that connect to filesystem-like data. This class supports the configuration of default_regex\n        and sorters for filtering and sorting data_references.\n\n        Args:\n            name (str): name of InferredAssetDBFSDataConnector\n            datasource_name (str): Name of datasource that this DataConnector is connected to\n            base_directory(str): base_directory for DataConnector to begin reading files\n            execution_engine (ExecutionEngine): ExecutionEngine object to actually read the data\n            default_regex (dict): Optional dict the filter and organize the data_references.\n            glob_directive (str): glob for selecting files in directory (defaults to *) or nested directories (e.g. */*.csv)\n            sorters (list): Optional list if you want to sort the data_references\n            batch_spec_passthrough (dict): dictionary with keys that will be added directly to batch_spec\n        "
        logger.debug(f'Constructing InferredAssetDBFSDataConnector "{name}".')
        super().__init__(
            name=name,
            datasource_name=datasource_name,
            base_directory=base_directory,
            execution_engine=execution_engine,
            default_regex=default_regex,
            glob_directive=glob_directive,
            sorters=sorters,
            batch_spec_passthrough=batch_spec_passthrough,
        )

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
        full_path = super()._get_full_file_path(
            path=path, data_asset_name=data_asset_name
        )
        template_arguments: dict = {"path": full_path}
        return self.execution_engine.resolve_data_reference(
            data_connector_name=self.__class__.__name__,
            template_arguments=template_arguments,
        )
