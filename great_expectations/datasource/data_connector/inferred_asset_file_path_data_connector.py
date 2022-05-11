import copy
import logging
from typing import List, Optional

from great_expectations.core.batch import BatchDefinition, BatchRequestBase
from great_expectations.core.batch_spec import BatchSpec, PathBatchSpec
from great_expectations.datasource.data_connector.file_path_data_connector import (
    FilePathDataConnector,
)
from great_expectations.execution_engine import ExecutionEngine

logger = logging.getLogger(__name__)


class InferredAssetFilePathDataConnector(FilePathDataConnector):
    "\n    The InferredAssetFilePathDataConnector is one of two classes (ConfiguredAssetFilePathDataConnector being the\n    other one) designed for connecting to filesystem-like data. This includes files on disk, but also things\n    like S3 object stores, etc:\n\n    InferredAssetFilePathDataConnector is a base class that operates on file paths and determines\n    the data_asset_name implicitly (e.g., through the combination of the regular expressions pattern and group names)\n\n    *Note*: InferredAssetFilePathDataConnector is not meant to be used on its own, but extended. Currently\n    InferredAssetFilesystemDataConnector, InferredAssetS3DataConnector, InferredAssetAzureDataConnector, and\n    InferredAssetGCSDataConnector are subclasses of InferredAssetFilePathDataConnector.\n"

    def __init__(
        self,
        name: str,
        datasource_name: str,
        execution_engine: Optional[ExecutionEngine] = None,
        default_regex: Optional[dict] = None,
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
        "\n        Base class for DataConnectors that connect to filesystem-like data. This class supports the configuration of default_regex\n        and sorters for filtering and sorting data_references.\n\n        Args:\n            name (str): name of ConfiguredAssetFilePathDataConnector\n            datasource_name (str): Name of datasource that this DataConnector is connected to\n            execution_engine (ExecutionEngine): ExecutionEngine object to actually read the data\n            default_regex (dict): Optional dict the filter and organize the data_references.\n            sorters (list): Optional list if you want to sort the data_references\n            batch_spec_passthrough (dict): dictionary with keys that will be added directly to batch_spec\n        "
        logger.debug(f'Constructing InferredAssetFilePathDataConnector "{name}".')
        super().__init__(
            name=name,
            datasource_name=datasource_name,
            execution_engine=execution_engine,
            default_regex=default_regex,
            sorters=sorters,
            batch_spec_passthrough=batch_spec_passthrough,
        )

    def _refresh_data_references_cache(self) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "refreshes data_reference cache"
        self._data_references_cache = {}
        for data_reference in self._get_data_reference_list():
            mapped_batch_definition_list: List[
                BatchDefinition
            ] = self._map_data_reference_to_batch_definition_list(
                data_reference=data_reference, data_asset_name=None
            )
            self._data_references_cache[data_reference] = mapped_batch_definition_list

    def get_data_reference_list_count(self) -> int:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        Returns the list of data_references known by this DataConnector by looping over all data_asset_names in\n        _data_references_cache\n\n        Returns:\n            number of data_references known by this DataConnector\n        "
        return len(self._data_references_cache)

    def get_unmatched_data_references(self) -> List[str]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        Returns the list of data_references unmatched by configuration by looping through items in _data_references_cache\n        and returning data_references that do not have an associated data_asset.\n\n        Returns:\n            list of data_references that are not matched by configuration.\n        "
        return [k for (k, v) in self._data_references_cache.items() if (v is None)]

    def get_available_data_asset_names(self) -> List[str]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        Return the list of asset names known by this DataConnector\n\n        Returns:\n            A list of available names\n        "
        if len(self._data_references_cache) == 0:
            self._refresh_data_references_cache()
        batch_definition_list: List[
            BatchDefinition
        ] = self._get_batch_definition_list_from_batch_request(
            batch_request=BatchRequestBase(
                datasource_name=self.datasource_name,
                data_connector_name=self.name,
                data_asset_name="",
            )
        )
        data_asset_names: List[str] = [
            batch_definition.data_asset_name
            for batch_definition in batch_definition_list
        ]
        return list(set(data_asset_names))

    def build_batch_spec(self, batch_definition: BatchDefinition) -> PathBatchSpec:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        Build BatchSpec from batch_definition by calling DataConnector's build_batch_spec function.\n\n        Args:\n            batch_definition (BatchDefinition): to be used to build batch_spec\n\n        Returns:\n            BatchSpec built from batch_definition\n        "
        batch_spec: BatchSpec = super().build_batch_spec(
            batch_definition=batch_definition
        )
        return PathBatchSpec(batch_spec)

    def _get_batch_definition_list_from_cache(self) -> List[BatchDefinition]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        batch_definition_list: List[BatchDefinition] = [
            batch_definitions[0]
            for batch_definitions in self._data_references_cache.values()
            if (batch_definitions is not None)
        ]
        return batch_definition_list

    def _get_regex_config(self, data_asset_name: Optional[str] = None) -> dict:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        regex_config: dict = copy.deepcopy(self._default_regex)
        return regex_config
