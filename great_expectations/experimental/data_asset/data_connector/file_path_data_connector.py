import copy
import logging
from pathlib import Path
from typing import Iterator, List, Optional, cast

import great_expectations.exceptions as gx_exceptions
from great_expectations.core._docs_decorators import public_api
from great_expectations.core.batch import (
    BatchDefinition,
    BatchRequest,
    BatchRequestBase,
    BatchSpec,
)
from great_expectations.core.batch_spec import PathBatchSpec
from great_expectations.experimental.data_asset.data_connector.asset.asset import Asset
from great_expectations.experimental.data_asset.data_connector.batch_filter import (
    BatchFilter,
    build_batch_filter,
)
from great_expectations.experimental.data_asset.data_connector.data_connector import (
    DataConnector,
)
from great_expectations.experimental.data_asset.data_connector.sorter import Sorter
from great_expectations.experimental.data_asset.data_connector.util import (
    batch_definition_matches_batch_request,
    build_sorters_from_config,
    map_batch_definition_to_data_reference_string_using_regex,
    map_data_reference_string_to_batch_definition_list_using_regex,
)

logger = logging.getLogger(__name__)


@public_api
class FilePathDataConnector(DataConnector):
    """The base class for Data Connectors designed to access filesystem-like data.

    This can include traditional, disk-based filesystems or object stores such as S3, GCS, or Azure Blob Store.
    This class supports the configuration of a default regular expression and sorters for filtering and sorting
    Data Assets.

    See the `DataConnector` base class for more information on the role of Data Connectors.

    Note that `FilePathDataConnector` is not meant to be used on its own, but extended.

    Args:
        name: The name of the Data Connector.
        default_regex: A regex configuration for filtering data references. The dict can include a regex `pattern` and
            a list of `group_names` for capture groups.
        sorters: A list of sorters for sorting data references.
        batch_spec_passthrough: Dictionary with keys that will be added directly to the batch spec.
    """

    def __init__(
        self,
        name: str,
        default_regex: Optional[dict] = None,
        sorters: Optional[list] = None,
        batch_spec_passthrough: Optional[dict] = None,
    ) -> None:
        super().__init__(
            name=name,
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

    def build_batch_spec(self, batch_definition: BatchDefinition) -> PathBatchSpec:
        """
        Build BatchSpec from batch_definition by calling DataConnector's build_batch_spec function.

        Args:
            batch_definition (BatchDefinition): to be used to build batch_spec

        Returns:
            BatchSpec built from batch_definition
        """

        # TODO: <Alex>ALEX</Alex>
        # TODO: <Alex>ALEX</Alex>
        # data_asset_name: str = batch_definition.data_asset_name
        # batch_spec_passthrough: dict = self.asset.batch_spec_passthrough
        # TODO: <Alex>ALEX</Alex>
        batch_spec_passthrough: dict = {}
        # TODO: <Alex>ALEX</Alex>
        # batch_spec_passthrough from data_asset
        batch_spec_passthrough = copy.deepcopy(batch_spec_passthrough)
        batch_definition_batch_spec_passthrough = (
            copy.deepcopy(batch_definition.batch_spec_passthrough) or {}
        )
        # batch_spec_passthrough from Batch Definition supersedes batch_spec_passthrough from data_asset
        batch_spec_passthrough.update(batch_definition_batch_spec_passthrough)
        batch_definition.batch_spec_passthrough = batch_spec_passthrough

        batch_spec: BatchSpec = super().build_batch_spec(
            batch_definition=batch_definition
        )
        return PathBatchSpec(batch_spec)
        # TODO: <Alex>ALEX</Alex>

    def _refresh_data_references_cache(self) -> None:
        # Map data_references to batch_definitions
        self._data_references_cache = {}

        # TODO: <Alex>ALEX</Alex>
        for data_reference in self._get_data_reference_list():
            mapped_batch_definition_list: List[BatchDefinition] = self._map_data_reference_to_batch_definition_list(  # type: ignore[assignment]
                data_reference=data_reference,
                # TODO: <Alex>ALEX</Alex>
                data_asset_name=None,
                # TODO: <Alex>ALEX</Alex>
            )
            self._data_references_cache[data_reference] = mapped_batch_definition_list
        # TODO: <Alex>ALEX</Alex>

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
                regex_pattern=pattern,
                group_names=group_names,
            )
            for batch_definition in batch_definition_list
        ]

        return path_list

    def get_data_reference_list_count(self) -> int:
        """
        Returns the list of data_references known by this DataConnector from its _data_references_cache

        Returns:
            number of data_references known by this DataConnector.
        """
        total_references: int = len(self._data_references_cache)
        return total_references

    def get_unmatched_data_references(self) -> List[str]:
        """
        Returns the list of data_references unmatched by configuration by looping through items in _data_references_cache
        and returning data_reference that do not have an associated data_asset.

        Returns:
            list of data_references that are not matched by configuration.
        """

        # TODO: <Alex>ALEX</Alex>
        # noinspection PyTypeChecker
        unmatched_data_references: List[str] = list(
            dict(
                filter(
                    lambda element: element[1] is None,
                    self._data_references_cache.items(),
                )
            ).values()
        )
        # TODO: <Alex>ALEX</Alex>
        return unmatched_data_references

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
            regex_pattern=pattern,
            group_names=group_names,
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

        path = self._get_full_file_path(path=path)

        return {"path": path}

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

    def _get_batch_definition_list_from_cache(self) -> List[BatchDefinition]:
        batch_definition_list: List[BatchDefinition] = [
            batch_definitions[0]
            for batch_definitions in self._data_references_cache.values()
            if batch_definitions is not None
        ]
        return batch_definition_list

    def _validate_batch_request(self, batch_request: BatchRequestBase) -> None:
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
                [sorter_name not in group_names for sorter_name in self.sorters.keys()]
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

    def _get_regex_config(self, data_asset_name: Optional[str] = None) -> dict:
        regex_config: dict = copy.deepcopy(self._default_regex)
        asset: Optional[Asset] = None
        if data_asset_name:
            asset = self._get_asset(data_asset_name=data_asset_name)

        if asset is not None:
            # Override the defaults
            if asset.pattern:
                regex_config["pattern"] = asset.pattern
            if asset.group_names:
                regex_config["group_names"] = asset.group_names

        return regex_config

    def _get_full_file_path(self, path: str) -> str:
        base_directory: str = self.base_directory
        return str(Path(base_directory).joinpath(path))
