from __future__ import annotations

# TODO: <Alex>ALEX</Alex>
# import copy
# TODO: <Alex>ALEX</Alex>
import logging
import re

# TODO: <Alex>ALEX</Alex>
# from typing import Iterator, List, Optional, cast
# TODO: <Alex>ALEX</Alex>
# TODO: <Alex>ALEX</Alex>
from typing import TYPE_CHECKING, List, Optional

# TODO: <Alex>ALEX</Alex>
# import great_expectations.exceptions as gx_exceptions
# TODO: <Alex>ALEX</Alex>
from great_expectations.core._docs_decorators import public_api

# TODO: <Alex>ALEX</Alex>
# from great_expectations.core.batch import (
#     BatchDefinition,
#     BatchRequest,
#     BatchRequestBase,
# )
# TODO: <Alex>ALEX</Alex>
# TODO: <Alex>ALEX</Alex>
from great_expectations.core.batch_spec import BatchSpec, PathBatchSpec

# TODO: <Alex>ALEX</Alex>
# TODO: <Alex>ALEX</Alex>
from great_expectations.experimental.datasources.data_asset.data_connector.batch_filter import (
    BatchFilter,
    build_batch_filter,
)

# TODO: <Alex>ALEX</Alex>
from great_expectations.experimental.datasources.data_asset.data_connector.data_connector import (
    DataConnector,
)
from great_expectations.experimental.datasources.data_asset.data_connector.regex_parser import (
    RegExParser,
)

# TODO: <Alex>ALEX</Alex>
# from great_expectations.experimental.data_asset.data_connector.sorter import Sorter
# TODO: <Alex>ALEX</Alex>
from great_expectations.experimental.datasources.data_asset.data_connector.util import (  # TODO: <Alex>ALEX</Alex>; build_sorters_from_config,
    batch_definition_matches_batch_request,
    map_batch_definition_to_data_reference_string_using_regex,
    map_data_reference_string_to_batch_definition_list_using_regex,
)

# TODO: <Alex>ALEX</Alex>

if TYPE_CHECKING:
    from great_expectations.core.batch import (
        BatchDefinition,
        BatchRequest,
    )


logger = logging.getLogger(__name__)


@public_api
class FilePathDataConnector(DataConnector):
    """The base class for Data Connectors designed to access filesystem-like data.

    This can include traditional, disk-based filesystems or object stores such as S3, GCS, or Azure Blob Store.
    # TODO: <Alex>ALEX</Alex>
    # This class supports the configuration of a default regular expression and sorters for filtering and sorting Data Assets.
    # TODO: <Alex>ALEX</Alex>

    See the `DataConnector` base class for more information on the role of Data Connectors.

    Note that `FilePathDataConnector` is not meant to be used on its own, but extended.

    Args:
        name: The name of the Data Connector.
        regex: A regex pattern for filtering data references
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
        regex: Optional[re.Pattern] = None,
        unnamed_regex_group_prefix: str = "batch_request_param_",
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

        # TODO: <Alex>ALEX</Alex>
        self._regex: Optional[re.Pattern] = regex
        # TODO: <Alex>ALEX</Alex>
        # TODO: <Alex>ALEX</Alex>
        self._regex_parser: RegExParser = RegExParser(
            regex_pattern=regex, unnamed_regex_group_prefix=unnamed_regex_group_prefix
        )
        # TODO: <Alex>ALEX</Alex>

        # TODO: <Alex>ALEX</Alex>
        # self._sorters = build_sorters_from_config(config_list=sorters)
        # self._validate_sorters_configuration()
        # TODO: <Alex>ALEX</Alex>

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
            # TODO: <Alex>ALEX</Alex>
            # - if data_connector has sorters configured, then sort the batch_definition list before returning.
            # TODO: <Alex>ALEX</Alex>

        Args:
            batch_request (BatchRequest): BatchRequest (containing previously validated attributes) to process

        Returns:
            A list of BatchDefinition objects that match BatchRequest

        """
        # TODO: <Alex>ALEX</Alex>
        return self._get_batch_definition_list_from_batch_request(
            batch_request=batch_request
        )

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
            mapped_batch_definition_list: List[
                BatchDefinition
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

    def _get_batch_definition_list_from_batch_request(
        self, batch_request: BatchRequest
    ) -> List[BatchDefinition]:
        """
        Retrieve batch_definitions that match batch_request.

        First retrieves all batch_definitions that match batch_request
            - if batch_request also has a batch_filter, then select batch_definitions that match batch_filter.
            # TODO: <Alex>ALEX</Alex>
            # - if data_connector has sorters configured, then sort the batch_definition list before returning.
            # TODO: <Alex>ALEX</Alex>

        Args:
            batch_request (BatchRequest): BatchRequest to process

        Returns:
            A list of BatchDefinition objects that match BatchRequest

        """
        # TODO: <Alex>ALEX</Alex>
        # self._validate_batch_request(batch_request=batch_request)
        # TODO: <Alex>ALEX</Alex>

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

        # TODO: <Alex>ALEX</Alex>
        # if self.sorters:
        #     batch_definition_list = self._sort_batch_definition_list(
        #         batch_definition_list=batch_definition_list
        #     )
        # TODO: <Alex>ALEX</Alex>

        # TODO: <Alex>ALEX</Alex>
        # if batch_request.data_connector_query is not None:
        #     data_connector_query_dict = batch_request.data_connector_query.copy()
        #     if (
        #         batch_request.limit is not None
        #         and data_connector_query_dict.get("limit") is None
        #     ):
        #         data_connector_query_dict["limit"] = batch_request.limit
        #
        #     batch_filter_obj: BatchFilter = build_batch_filter(
        #         data_connector_query_dict=data_connector_query_dict
        #     )
        #     batch_definition_list = batch_filter_obj.select_from_data_connector_query(
        #         batch_definition_list=batch_definition_list
        #     )
        # TODO: <Alex>ALEX</Alex>
        # TODO: <Alex>ALEX</Alex>
        if batch_request.options is not None:
            # TODO: <Alex>ALEX</Alex>
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
            # TODO: <Alex>ALEX</Alex>

            batch_filter_obj: BatchFilter = build_batch_filter(
                data_connector_query_dict=data_connector_query_dict
            )
            batch_definition_list = batch_filter_obj.select_from_data_connector_query(
                batch_definition_list=batch_definition_list
            )
        # TODO: <Alex>ALEX</Alex>

        return batch_definition_list

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

    # TODO: <Alex>ALEX</Alex>
    #     def _validate_batch_request(self, batch_request: BatchRequestBase) -> None:
    #         self._validate_sorters_configuration(
    #             data_asset_name=batch_request.data_asset_name
    #         )
    #
    #     def _validate_sorters_configuration(
    #         self, data_asset_name: Optional[str] = None
    #     ) -> None:
    #         if self.sorters is not None and len(self.sorters) > 0:
    #             # data_asset_name: str = batch_request.data_asset_name
    #             regex_config: dict = self._get_regex_config(data_asset_name=data_asset_name)
    #             group_names: List[str] = regex_config["group_names"]
    #             if any(
    #                 [sorter_name not in group_names for sorter_name in self.sorters.keys()]
    #             ):
    #                 raise gx_exceptions.DataConnectorError(
    #                     f"""DataConnector "{self.name}" specifies one or more sort keys that do not appear among the
    # configured group_name.
    #                     """
    #                 )
    #
    #             if len(group_names) < len(self.sorters):
    #                 raise gx_exceptions.DataConnectorError(
    #                     f"""DataConnector "{self.name}" is configured with {len(group_names)} group names;
    # this is fewer than number of sorters specified, which is {len(self.sorters)}.
    #                     """
    #                 )
    # TODO: <Alex>ALEX</Alex>

    # Interface Method
    def _get_data_reference_list(self) -> List[str]:
        """
        List objects in the underlying data store to create a list of data_references.
        This method is used to refresh the cache by classes that extend this base DataConnector class
        """
        raise NotImplementedError

    def _get_full_file_path(self, path: str) -> str:
        raise NotImplementedError
