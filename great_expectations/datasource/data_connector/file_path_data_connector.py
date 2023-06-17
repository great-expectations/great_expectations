import logging
import re
from typing import Callable, Dict, Iterator, List, Optional, Tuple, cast

import great_expectations.exceptions as gx_exceptions
from great_expectations.core._docs_decorators import public_api
from great_expectations.core.batch import (
    BatchDefinition,
    BatchRequest,
    BatchRequestBase,
    BatchSpec,
)
from great_expectations.core.batch_spec import PathBatchSpec
from great_expectations.core.util import AzureUrl, DBFSPath, GCSUrl, S3Url
from great_expectations.datasource.data_connector.batch_filter import (
    BatchFilter,
    build_batch_filter,
)
from great_expectations.datasource.data_connector.data_connector import DataConnector
from great_expectations.datasource.data_connector.sorter import Sorter  # noqa: TCH001
from great_expectations.datasource.data_connector.util import (
    batch_definition_matches_batch_request,
    build_sorters_from_config,
    map_batch_definition_to_data_reference_string_using_regex,
    map_data_reference_string_to_batch_definition_list_using_regex,
)
from great_expectations.execution_engine import ExecutionEngine

logger = logging.getLogger(__name__)


class DataConnectorStorageDataReferenceResolver:
    DATA_CONNECTOR_NAME_TO_STORAGE_NAME_MAP: Dict[str, str] = {
        "InferredAssetS3DataConnector": "S3",
        "ConfiguredAssetS3DataConnector": "S3",
        "InferredAssetGCSDataConnector": "GCS",
        "ConfiguredAssetGCSDataConnector": "GCS",
        "InferredAssetAzureDataConnector": "ABS",
        "ConfiguredAssetAzureDataConnector": "ABS",
        "InferredAssetDBFSDataConnector": "DBFS",
        "ConfiguredAssetDBFSDataConnector": "DBFS",
    }
    STORAGE_NAME_EXECUTION_ENGINE_NAME_PATH_RESOLVERS: Dict[
        Tuple[str, str], Callable
    ] = {
        (
            "S3",
            "PandasExecutionEngine",
        ): lambda template_arguments: S3Url.OBJECT_URL_TEMPLATE.format(
            **template_arguments
        ),
        (
            "S3",
            "SparkDFExecutionEngine",
        ): lambda template_arguments: S3Url.OBJECT_URL_TEMPLATE.format(
            **template_arguments
        ),
        (
            "GCS",
            "PandasExecutionEngine",
        ): lambda template_arguments: GCSUrl.OBJECT_URL_TEMPLATE.format(
            **template_arguments
        ),
        (
            "GCS",
            "SparkDFExecutionEngine",
        ): lambda template_arguments: GCSUrl.OBJECT_URL_TEMPLATE.format(
            **template_arguments
        ),
        (
            "ABS",
            "PandasExecutionEngine",
        ): lambda template_arguments: AzureUrl.AZURE_BLOB_STORAGE_HTTPS_URL_TEMPLATE.format(
            **template_arguments
        ),
        (
            "ABS",
            "SparkDFExecutionEngine",
        ): lambda template_arguments: AzureUrl.AZURE_BLOB_STORAGE_WASBS_URL_TEMPLATE.format(
            **template_arguments
        ),
        (
            "DBFS",
            "PandasExecutionEngine",
        ): lambda template_arguments: DBFSPath.convert_to_file_semantics_version(
            **template_arguments
        ),
        (
            "DBFS",
            "SparkDFExecutionEngine",
        ): lambda template_arguments: DBFSPath.convert_to_protocol_version(
            **template_arguments
        ),
    }

    @staticmethod
    def resolve_data_reference(
        data_connector_name: str,
        execution_engine_name: str,
        template_arguments: dict,
    ):
        """Resolve file path for a (data_connector_name, execution_engine_name) combination."""
        storage_name: str = DataConnectorStorageDataReferenceResolver.DATA_CONNECTOR_NAME_TO_STORAGE_NAME_MAP[
            data_connector_name
        ]
        return DataConnectorStorageDataReferenceResolver.STORAGE_NAME_EXECUTION_ENGINE_NAME_PATH_RESOLVERS[
            (storage_name, execution_engine_name)
        ](
            template_arguments
        )


@public_api
class FilePathDataConnector(DataConnector):
    """The base class for Data Connectors designed to access filesystem-like data.

    This can include traditional, disk-based filesystems or object stores such as S3, GCS, or Azure Blob Storage (ABS).
    This class supports the configuration of a default regular expression and sorters for filtering and sorting
    Data Assets.

    See the `DataConnector` base class for more information on the role of Data Connectors.

    Note that `FilePathDataConnector` is not meant to be used on its own, but extended.

    Args:
        name: The name of the Data Connector.
        datasource_name: The name of this Data Connector's Datasource.
        execution_engine: The Execution Engine object to used by this Data Connector to read the data.
        default_regex: A regex configuration for filtering data references. The dict can include a regex `pattern` and
            a list of `group_names` for capture groups.
        sorters: A list of sorters for sorting data references.
        batch_spec_passthrough: Dictionary with keys that will be added directly to the batch spec.
        id: The unique identifier for this Data Connector used when running in cloud mode.
    """

    def __init__(  # noqa: PLR0913
        self,
        name: str,
        datasource_name: str,
        execution_engine: Optional[ExecutionEngine] = None,
        default_regex: Optional[dict] = None,
        sorters: Optional[list] = None,
        batch_spec_passthrough: Optional[dict] = None,
        id: Optional[str] = None,
    ) -> None:
        logger.debug(f'Constructing FilePathDataConnector "{name}".')

        super().__init__(
            name=name,
            id=id,
            datasource_name=datasource_name,
            execution_engine=execution_engine,  # type: ignore[arg-type] # execution_engine cannot be None
            batch_spec_passthrough=batch_spec_passthrough,
        )

        if default_regex is None:
            default_regex = {}

        self._default_regex = default_regex

        self._sorters = build_sorters_from_config(config_list=sorters)  # type: ignore[arg-type]
        self._validate_sorters_configuration()

    @property
    def sorters(self) -> Optional[dict]:
        return self._sorters

    def _get_data_reference_list_from_cache_by_data_asset_name(
        self, data_asset_name: str
    ) -> List[str]:
        """
        Fetch data_references corresponding to data_asset_name from the cache.
        """
        regex_config: dict = self._get_regex_config(data_asset_name=data_asset_name)
        pattern: str = regex_config["pattern"]
        group_names: List[str] = regex_config["group_names"]

        batch_definition_list = self._get_batch_definition_list_from_batch_request(
            batch_request=BatchRequestBase(
                datasource_name=self.datasource_name,
                data_connector_name=self.name,
                data_asset_name=data_asset_name,
            )
        )

        if self.sorters:
            batch_definition_list = self._sort_batch_definition_list(
                batch_definition_list=batch_definition_list
            )

        path_list: List[str] = [
            map_batch_definition_to_data_reference_string_using_regex(
                batch_definition=batch_definition,
                regex_pattern=re.compile(pattern),
                group_names=group_names,
            )
            for batch_definition in batch_definition_list
        ]

        return path_list

    def get_batch_definition_list_from_batch_request(  # type: ignore[override] # BaseBatchRequest
        self,
        batch_request: BatchRequest,
    ) -> List[BatchDefinition]:
        """
        Retrieve batch_definitions and that match batch_request.

        First retrieves all batch_definitions that match batch_request
            - if batch_request also has a batch_filter, then select batch_definitions that match batch_filter.
            - if data_connector has sorters configured, then sort the batch_definition list before returning.

        Args:
            batch_request (BatchRequest): BatchRequest (containing previously validated attributes) to process

        Returns:
            A list of BatchDefinition objects that match BatchRequest

        """
        batch_request_base: BatchRequestBase = cast(BatchRequestBase, batch_request)
        return self._get_batch_definition_list_from_batch_request(
            batch_request=batch_request_base
        )

    def _get_batch_definition_list_from_batch_request(
        self,
        batch_request: BatchRequestBase,
    ) -> List[BatchDefinition]:
        """
        Retrieve batch_definitions that match batch_request.

        First retrieves all batch_definitions that match batch_request
            - if batch_request also has a batch_filter, then select batch_definitions that match batch_filter.
            - if data_connector has sorters configured, then sort the batch_definition list before returning.

        Args:
            batch_request (BatchRequestBase): BatchRequestBase (BatchRequest without attribute validation) to process

        Returns:
            A list of BatchDefinition objects that match BatchRequest

        """
        self._validate_batch_request(batch_request=batch_request)

        if len(self._data_references_cache) == 0:
            self._refresh_data_references_cache()

        # Use a combination of a list and set to preserve iteration order
        batch_definition_list: List[BatchDefinition] = list()
        batch_definition_set = set()
        for batch_definition in self._get_batch_definition_list_from_cache():
            if (
                batch_definition_matches_batch_request(
                    batch_definition=batch_definition, batch_request=batch_request
                )
                and batch_definition not in batch_definition_set
            ):
                batch_definition_list.append(batch_definition)
                batch_definition_set.add(batch_definition)

        if self.sorters:
            batch_definition_list = self._sort_batch_definition_list(
                batch_definition_list=batch_definition_list
            )

        if batch_request.data_connector_query is not None:
            data_connector_query_dict = batch_request.data_connector_query.copy()
            if (
                batch_request.limit is not None
                and data_connector_query_dict.get("limit") is None
            ):
                data_connector_query_dict["limit"] = batch_request.limit

            batch_filter_obj: BatchFilter = build_batch_filter(
                data_connector_query_dict=data_connector_query_dict
            )
            batch_definition_list = batch_filter_obj.select_from_data_connector_query(
                batch_definition_list=batch_definition_list
            )

        return batch_definition_list

    def _sort_batch_definition_list(
        self, batch_definition_list: List[BatchDefinition]
    ) -> List[BatchDefinition]:
        """
        Use configured sorters to sort batch_definition

        Args:
            batch_definition_list (list): list of batch_definitions to sort

        Returns:
            sorted list of batch_definitions

        """
        sorters: Iterator[Sorter] = reversed(list(self.sorters.values()))  # type: ignore[union-attr]
        for sorter in sorters:
            batch_definition_list = sorter.get_sorted_batch_definitions(
                batch_definitions=batch_definition_list
            )

        return batch_definition_list

    def _map_data_reference_to_batch_definition_list(
        self, data_reference: str, data_asset_name: Optional[str] = None
    ) -> Optional[List[BatchDefinition]]:
        regex_config: dict = self._get_regex_config(data_asset_name=data_asset_name)
        pattern: str = regex_config["pattern"]
        group_names: List[str] = regex_config["group_names"]
        return map_data_reference_string_to_batch_definition_list_using_regex(
            datasource_name=self.datasource_name,
            data_connector_name=self.name,
            data_asset_name=data_asset_name,
            data_reference=data_reference,
            regex_pattern=pattern,
            group_names=group_names,
        )

    def _map_batch_definition_to_data_reference(
        self, batch_definition: BatchDefinition
    ) -> str:
        data_asset_name: str = batch_definition.data_asset_name
        regex_config: dict = self._get_regex_config(data_asset_name=data_asset_name)
        pattern: str = regex_config["pattern"]
        group_names: List[str] = regex_config["group_names"]
        return map_batch_definition_to_data_reference_string_using_regex(
            batch_definition=batch_definition,
            regex_pattern=re.compile(pattern),
            group_names=group_names,
        )

    def build_batch_spec(self, batch_definition: BatchDefinition) -> PathBatchSpec:
        """
        Build BatchSpec from batch_definition by calling DataConnector's build_batch_spec function.

        Args:
            batch_definition (BatchDefinition): to be used to build batch_spec

        Returns:
            BatchSpec built from batch_definition
        """
        batch_spec: BatchSpec = super().build_batch_spec(
            batch_definition=batch_definition
        )
        return PathBatchSpec(batch_spec)

    def resolve_data_reference(self, template_arguments: dict):
        """Resolve file path for a (data_connector_name, execution_engine_name) combination."""
        return DataConnectorStorageDataReferenceResolver.resolve_data_reference(
            data_connector_name=self.__class__.__name__,
            execution_engine_name=self._execution_engine.__class__.__name__,
            template_arguments=template_arguments,
        )

    def _generate_batch_spec_parameters_from_batch_definition(
        self, batch_definition: BatchDefinition
    ) -> dict:
        path: str = self._map_batch_definition_to_data_reference(
            batch_definition=batch_definition
        )
        if not path:
            raise ValueError(
                f"""No data reference for data asset name "{batch_definition.data_asset_name}" matches the given
batch identifiers {batch_definition.batch_identifiers} from batch definition {batch_definition}.
"""
            )

        path = self._get_full_file_path(
            path=path, data_asset_name=batch_definition.data_asset_name
        )

        return {"path": path}

    def _validate_batch_request(self, batch_request: BatchRequestBase) -> None:
        super()._validate_batch_request(batch_request=batch_request)
        self._validate_sorters_configuration(
            data_asset_name=batch_request.data_asset_name
        )

    def _validate_sorters_configuration(
        self, data_asset_name: Optional[str] = None
    ) -> None:
        if self.sorters is not None and len(self.sorters) > 0:
            # data_asset_name: str = batch_request.data_asset_name
            regex_config: dict = self._get_regex_config(data_asset_name=data_asset_name)
            group_names: List[str] = regex_config["group_names"]
            if any(
                sorter_name not in group_names for sorter_name in self.sorters.keys()
            ):
                raise gx_exceptions.DataConnectorError(
                    f"""DataConnector "{self.name}" specifies one or more sort keys that do not appear among the
configured group_name.
                    """
                )

            if len(group_names) < len(self.sorters):
                raise gx_exceptions.DataConnectorError(
                    f"""DataConnector "{self.name}" is configured with {len(group_names)} group names;
this is fewer than number of sorters specified, which is {len(self.sorters)}.
                    """
                )

    def _get_batch_definition_list_from_cache(self) -> List[BatchDefinition]:
        raise NotImplementedError

    def _get_regex_config(self, data_asset_name: Optional[str] = None) -> dict:
        raise NotImplementedError

    def _get_full_file_path(
        self, path: str, data_asset_name: Optional[str] = None
    ) -> str:
        raise NotImplementedError
