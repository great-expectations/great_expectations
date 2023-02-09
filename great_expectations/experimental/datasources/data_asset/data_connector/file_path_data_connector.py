from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Callable, Dict, List, Optional

from great_expectations.core._docs_decorators import public_api
from great_expectations.core.batch_spec import BatchSpec, PathBatchSpec
from great_expectations.core.util import AzureUrl, DBFSPath, GCSUrl, S3Url
from great_expectations.experimental.datasources.data_asset.data_connector.batch_filter import (
    BatchFilter,
    build_batch_filter,
)
from great_expectations.experimental.datasources.data_asset.data_connector.data_connector import (
    DataConnector,
)
from great_expectations.experimental.datasources.data_asset.data_connector.regex_parser import (
    RegExParser,
)

# TODO: <Alex>ALEX_INCLUDE_SORTERS_FUNCTIONALITY_UNDER_PYDANTIC-MAKE_SURE_SORTER_CONFIGURATIONS_ARE_VALIDATED</Alex>
# TODO: <Alex>ALEX</Alex>
# from great_expectations.experimental.data_asset.data_connector.sorter import Sorter
# TODO: <Alex>ALEX</Alex>
from great_expectations.experimental.datasources.data_asset.data_connector.util import (
    batch_definition_matches_batch_request,
    map_batch_definition_to_data_reference_string_using_regex,
    map_data_reference_string_to_batch_definition_list_using_regex,
)

if TYPE_CHECKING:
    from great_expectations.core.batch import BatchDefinition
    from great_expectations.experimental.datasources.interfaces import BatchRequest


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
        tuple[str, str], Callable
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
            "SparkDFExecutionEngine",
        ): lambda template_arguments: DBFSPath.convert_to_protocol_version(
            **template_arguments
        ),
        (
            "DBFS",
            "PandasExecutionEngine",
        ): lambda template_arguments: DBFSPath.convert_to_file_semantics_version(
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

    This can include traditional, disk-based filesystems or object stores such as S3, GCS, or Azure Blob Store.
    # TODO: <Alex>ALEX_INCLUDE_SORTERS_FUNCTIONALITY_UNDER_PYDANTIC-MAKE_SURE_SORTER_CONFIGURATIONS_ARE_VALIDATED</Alex>
    # TODO: <Alex>ALEX</Alex>
    # This class supports a regular expression and sorters for filtering and sorting data references.
    # TODO: <Alex>ALEX</Alex>

    See the `DataConnector` base class for more information on the role of Data Connectors.

    Note that `FilePathDataConnector` is not meant to be used on its own, but extended.

    Args:
        name: The name of the Data Connector.
        regex: A regex pattern for filtering data references
        # TODO: <Alex>ALEX_INCLUDE_SORTERS_FUNCTIONALITY_UNDER_PYDANTIC-MAKE_SURE_SORTER_CONFIGURATIONS_ARE_VALIDATED</Alex>
        # TODO: <Alex>ALEX</Alex>
        # sorters: A list of sorters for sorting data references.
        # TODO: <Alex>ALEX</Alex>
    """

    def __init__(
        self,
        name: str,
        datasource_name: str,
        data_asset_name: str,
        execution_engine_name: str,
        regex: re.Pattern,
        unnamed_regex_group_prefix: str = "batch_request_param_",
        # TODO: <Alex>ALEX_INCLUDE_SORTERS_FUNCTIONALITY_UNDER_PYDANTIC-MAKE_SURE_SORTER_CONFIGURATIONS_ARE_VALIDATED</Alex>
        # TODO: <Alex>ALEX</Alex>
        # sorters: Optional[list] = None,
        # TODO: <Alex>ALEX</Alex>
    ) -> None:
        super().__init__(
            name=name,
            datasource_name=datasource_name,
            data_asset_name=data_asset_name,
            execution_engine_name=execution_engine_name,
        )

        self._regex: re.Pattern = regex
        self._regex_parser: RegExParser = RegExParser(
            regex_pattern=regex, unnamed_regex_group_prefix=unnamed_regex_group_prefix
        )

    # TODO: <Alex>ALEX_INCLUDE_SORTERS_FUNCTIONALITY_UNDER_PYDANTIC-MAKE_SURE_SORTER_CONFIGURATIONS_ARE_VALIDATED</Alex>
    # TODO: <Alex>ALEX</Alex>
    # @property
    # def sorters(self) -> Optional[dict]:
    #     return self._sorters
    # TODO: <Alex>ALEX</Alex>

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

    # Interface Method
    def get_batch_definition_list_from_batch_request(
        self, batch_request: BatchRequest
    ) -> List[BatchDefinition]:
        """
        Retrieve batch_definitions and that match batch_request.

        First retrieves all batch_definitions that match batch_request
            - if batch_request also has a batch_filter, then select batch_definitions that match batch_filter.
            # TODO: <Alex>ALEX_INCLUDE_SORTERS_FUNCTIONALITY_UNDER_PYDANTIC-MAKE_SURE_SORTER_CONFIGURATIONS_ARE_VALIDATED</Alex>
            # TODO: <Alex>ALEX</Alex>
            # - if data_connector has sorters configured, then sort the batch_definition list before returning.
            # TODO: <Alex>ALEX</Alex>

        Args:
            batch_request (BatchRequest): BatchRequest (containing previously validated attributes) to process

        Returns:
            A list of BatchDefinition objects that match BatchRequest

        """
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

        # TODO: <Alex>ALEX_INCLUDE_SORTERS_FUNCTIONALITY_UNDER_PYDANTIC-MAKE_SURE_SORTER_CONFIGURATIONS_ARE_VALIDATED</Alex>
        # TODO: <Alex>ALEX</Alex>
        # if self.sorters:
        #     batch_definition_list = self._sort_batch_definition_list(
        #         batch_definition_list=batch_definition_list
        #     )
        # TODO: <Alex>ALEX</Alex>

        if batch_request.options is not None:
            data_connector_query_dict = {
                "batch_filter_parameters": batch_request.options.copy()
            }
            # TODO: <Alex>ALEX-SUPPORT_LIMIT_BATCH_QUERY_OPTION_DIRECTIVE_LATER</Alex>
            # TODO: <Alex>ALEX</Alex>
            # if (
            #     batch_request.limit is not None
            #     and data_connector_query_dict.get("limit") is None
            # ):
            #     data_connector_query_dict["limit"] = batch_request.limit
            # TODO: <Alex>ALEX</Alex>

            batch_filter_obj: BatchFilter = build_batch_filter(
                data_connector_query_dict=data_connector_query_dict  # type: ignore[arg-type]
            )
            batch_definition_list = batch_filter_obj.select_from_data_connector_query(
                batch_definition_list=batch_definition_list
            )

        return batch_definition_list

    # Interface Method
    def get_data_reference_count(self) -> int:
        """
        Returns the list of data_references known by this DataConnector from its _data_references_cache

        Returns:
            number of data_references known by this DataConnector.
        """
        total_references: int = len(self._data_references_cache)
        return total_references

    # Interface Method
    def get_unmatched_data_references(self) -> List[str]:
        """
        Returns the list of data_references unmatched by configuration by looping through items in
        _data_references_cache and returning data_references that do not have an associated data_asset.

        Returns:
            list of data_references that are not matched by configuration.
        """

        # noinspection PyTypeChecker
        unmatched_data_references: List[str] = list(
            dict(
                filter(
                    lambda element: element[1] is None,
                    self._data_references_cache.items(),
                )
            ).keys()
        )
        return unmatched_data_references

    # Interface Method
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

        path = self._get_full_file_path(path=path)

        return {"path": path}

    # Interface Method
    def _refresh_data_references_cache(self) -> None:
        # Map data_references to batch_definitions
        self._data_references_cache = {}

        for data_reference in self._get_data_reference_list():
            mapped_batch_definition_list: Optional[
                List[BatchDefinition]
            ] = self._map_data_reference_to_batch_definition_list(
                data_reference=data_reference
            )
            self._data_references_cache[data_reference] = mapped_batch_definition_list

    # Interface Method
    def _map_data_reference_to_batch_definition_list(
        self, data_reference: str
    ) -> Optional[List[BatchDefinition]]:
        return map_data_reference_string_to_batch_definition_list_using_regex(
            datasource_name=self.datasource_name,
            data_connector_name=self.name,
            data_asset_name=self.data_asset_name,
            data_reference=data_reference,
            regex_pattern=self._regex,
        )

    # Interface Method
    def _map_batch_definition_to_data_reference(
        self, batch_definition: BatchDefinition
    ) -> str:
        group_names: List[str] = self._regex_parser.get_all_group_names()
        return map_batch_definition_to_data_reference_string_using_regex(
            batch_definition=batch_definition,
            regex_pattern=self._regex,
            group_names=group_names,
        )

    # TODO: <Alex>ALEX_INCLUDE_SORTERS_FUNCTIONALITY_UNDER_PYDANTIC-MAKE_SURE_SORTER_CONFIGURATIONS_ARE_VALIDATED</Alex>
    # TODO: <Alex>ALEX</Alex>
    # def _sort_batch_definition_list(
    #     self, batch_definition_list: List[BatchDefinition]
    # ) -> List[BatchDefinition]:
    #     """
    #     Use configured sorters to sort batch_definition
    #
    #     Args:
    #         batch_definition_list (list): list of batch_definitions to sort
    #
    #     Returns:
    #         sorted list of batch_definitions
    #
    #     """
    #     sorters: Iterator[Sorter] = reversed(list(self.sorters.values()))
    #     for sorter in sorters:
    #         batch_definition_list = sorter.get_sorted_batch_definitions(
    #             batch_definitions=batch_definition_list
    #         )
    #
    #     return batch_definition_list
    # TODO: <Alex>ALEX</Alex>

    def _get_batch_definition_list_from_cache(self) -> List[BatchDefinition]:
        batch_definition_list: List[BatchDefinition] = [
            batch_definitions[0]
            for batch_definitions in self._data_references_cache.values()
            if batch_definitions is not None
        ]
        return batch_definition_list

    def resolve_data_reference(self, template_arguments: dict):
        """Resolve file path for a (data_connector_name, execution_engine_name) combination."""
        return DataConnectorStorageDataReferenceResolver.resolve_data_reference(
            data_connector_name=self.__class__.__name__,
            execution_engine_name=self._execution_engine_name,
            template_arguments=template_arguments,
        )

    # Interface Method
    def _get_data_reference_list(self) -> List[str]:
        """
        List objects in the underlying data store to create a list of data_references.
        This method is used to refresh the cache by classes that extend this base DataConnector class
        """
        raise NotImplementedError

    def _get_full_file_path(self, path: str) -> str:
        raise NotImplementedError
