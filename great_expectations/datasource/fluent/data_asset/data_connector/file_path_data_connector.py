from __future__ import annotations

import copy
import logging
import re
from abc import abstractmethod
from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Set, Tuple, Union

from great_expectations.core import IDDict
from great_expectations.core.batch import BatchDefinition
from great_expectations.core.batch_spec import BatchSpec, PathBatchSpec
from great_expectations.datasource.data_connector.batch_filter import (
    BatchFilter,
    build_batch_filter,
)
from great_expectations.datasource.data_connector.util import (
    map_batch_definition_to_data_reference_string_using_regex,
)
from great_expectations.datasource.fluent.constants import _DATA_CONNECTOR_NAME
from great_expectations.datasource.fluent.data_asset.data_connector import (
    DataConnector,
)
from great_expectations.datasource.fluent.data_asset.data_connector.regex_parser import (
    RegExParser,
)

# TODO: <Alex>ALEX_INCLUDE_SORTERS_FUNCTIONALITY_UNDER_PYDANTIC-MAKE_SURE_SORTER_CONFIGURATIONS_ARE_VALIDATED</Alex>
# TODO: <Alex>ALEX</Alex>
# from great_expectations.datasource.fluent.data_asset.data_connector.sorter import Sorter
# TODO: <Alex>ALEX</Alex>

if TYPE_CHECKING:
    from great_expectations.alias_types import PathStr
    from great_expectations.datasource.fluent import BatchRequest

logger = logging.getLogger(__name__)


def file_get_unfiltered_batch_definition_list_fn(
    data_connector: FilePathDataConnector, batch_request: BatchRequest
) -> list[BatchDefinition]:
    """Get all batch definitions for all files from a data connector using the supplied batch request.

    Args:
        data_connector: Used to get batch definitions.
        batch_request: Specifies which batch definitions to get from data connector.

    Returns:
        A list of batch definitions from the data connector based on the batch request.
    """

    # Use a combination of a list and set to preserve iteration order
    batch_definition_list: list[BatchDefinition] = list()
    batch_definition_set = set()
    for (
        batch_definition
    ) in data_connector._get_batch_definition_list_from_data_references_cache():
        if (
            data_connector._batch_definition_matches_batch_request(
                batch_definition=batch_definition, batch_request=batch_request
            )
            and batch_definition not in batch_definition_set
        ):
            batch_definition_list.append(batch_definition)
            batch_definition_set.add(batch_definition)

    return batch_definition_list


def make_directory_get_unfiltered_batch_definition_list_fn(
    data_directory: PathStr,
) -> Callable[[FilePathDataConnector, BatchRequest], list[BatchDefinition]]:
    def directory_get_unfiltered_batch_definition_list_fn(
        data_connector: FilePathDataConnector,
        batch_request: BatchRequest,
    ) -> list[BatchDefinition]:
        """Get a single batch definition for the directory supplied in data_directory.

        Args:
            data_connector: Data connector containing information for the batch definition.
            batch_request: Unused, but included for consistency.
            data_directory: Directory pointing to data to reference in the batch definition.

        Returns:
            List containing a single batch definition referencing the directory of interest.
        """
        batch_definition = BatchDefinition(
            datasource_name=data_connector.datasource_name,
            data_connector_name=_DATA_CONNECTOR_NAME,
            data_asset_name=data_connector.data_asset_name,
            batch_identifiers=IDDict({"path": data_directory}),
        )
        return [batch_definition]

    return directory_get_unfiltered_batch_definition_list_fn


class FilePathDataConnector(DataConnector):
    """The base class for Data Connectors designed to access filesystem-like data.

    This can include traditional, disk-based filesystems or object stores such as S3, GCS, or ABS.
    # TODO: <Alex>ALEX_INCLUDE_SORTERS_FUNCTIONALITY_UNDER_PYDANTIC-MAKE_SURE_SORTER_CONFIGURATIONS_ARE_VALIDATED</Alex>
    # TODO: <Alex>ALEX</Alex>
    # This class supports a regular expression and sorters for filtering and sorting data references.
    # TODO: <Alex>ALEX</Alex>

    See the `DataConnector` base class for more information on the role of Data Connectors.

    Note that `FilePathDataConnector` is not meant to be used on its own, but extended.

    Args:
        datasource_name: The name of the Datasource associated with this DataConnector instance
        data_asset_name: The name of the DataAsset using this DataConnector instance
        batching_regex: A regex pattern for partitioning data references
        # TODO: <Alex>ALEX_INCLUDE_SORTERS_FUNCTIONALITY_UNDER_PYDANTIC-MAKE_SURE_SORTER_CONFIGURATIONS_ARE_VALIDATED</Alex>
        # TODO: <Alex>ALEX</Alex>
        # sorters: A list of sorters for sorting data references.
        file_path_template_map_fn: Format function mapping path to fully-qualified resource on network file storage
        # TODO: <Alex>ALEX</Alex>
    """

    FILE_PATH_BATCH_SPEC_KEY = "path"

    def __init__(  # noqa: PLR0913
        self,
        datasource_name: str,
        data_asset_name: str,
        batching_regex: re.Pattern,
        unnamed_regex_group_prefix: str = "batch_request_param_",
        # TODO: <Alex>ALEX_INCLUDE_SORTERS_FUNCTIONALITY_UNDER_PYDANTIC-MAKE_SURE_SORTER_CONFIGURATIONS_ARE_VALIDATED</Alex>
        # TODO: <Alex>ALEX</Alex>
        # sorters: Optional[list] = None,
        # TODO: <Alex>ALEX</Alex>
        file_path_template_map_fn: Optional[Callable] = None,
        get_unfiltered_batch_definition_list_fn: Callable[
            [FilePathDataConnector, BatchRequest], list[BatchDefinition]
        ] = file_get_unfiltered_batch_definition_list_fn,
    ) -> None:
        super().__init__(
            datasource_name=datasource_name,
            data_asset_name=data_asset_name,
        )

        self._unnamed_regex_group_prefix: str = unnamed_regex_group_prefix
        self._batching_regex: re.Pattern = (
            self._ensure_regex_groups_include_data_reference_key(regex=batching_regex)
        )
        self._regex_parser: RegExParser = RegExParser(
            regex_pattern=self._batching_regex,
            unnamed_regex_group_prefix=self._unnamed_regex_group_prefix,
        )

        self._file_path_template_map_fn: Optional[Callable] = file_path_template_map_fn

        self._get_unfiltered_batch_definition_list_fn = (
            get_unfiltered_batch_definition_list_fn
        )

        # This is a dictionary which maps data_references onto batch_requests.
        self._data_references_cache: Dict[str, List[BatchDefinition] | None] = {}

    # TODO: <Alex>ALEX_INCLUDE_SORTERS_FUNCTIONALITY_UNDER_PYDANTIC-MAKE_SURE_SORTER_CONFIGURATIONS_ARE_VALIDATED</Alex>
    # TODO: <Alex>ALEX</Alex>
    # @property
    # def sorters(self) -> Optional[dict]:
    #     return self._sorters
    # TODO: <Alex>ALEX</Alex>

    # Interface Method
    def get_batch_definition_list(
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
        batch_definition_list: List[
            BatchDefinition
        ] = self._get_unfiltered_batch_definition_list_fn(self, batch_request)

        # TODO: <Alex>ALEX_INCLUDE_SORTERS_FUNCTIONALITY_UNDER_PYDANTIC-MAKE_SURE_SORTER_CONFIGURATIONS_ARE_VALIDATED</Alex>
        # TODO: <Alex>ALEX</Alex>
        # if self.sorters:
        #     batch_definition_list = self._sort_batch_definition_list(
        #         batch_definition_list=batch_definition_list
        #     )
        # TODO: <Alex>ALEX</Alex>

        data_connector_query_dict: dict[str, dict | slice] = {}
        if batch_request.options:
            data_connector_query_dict.update(
                {
                    "batch_filter_parameters": {
                        key: value
                        for key, value in batch_request.options.items()
                        if value is not None
                    }
                }
            )

        data_connector_query_dict.update({"index": batch_request.batch_slice})

        batch_filter_obj: BatchFilter = build_batch_filter(
            data_connector_query_dict=data_connector_query_dict  # type: ignore[arg-type]
        )
        batch_definition_list = batch_filter_obj.select_from_data_connector_query(
            batch_definition_list=batch_definition_list
        )

        return batch_definition_list

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
    def get_data_reference_count(self) -> int:
        """
        Returns the list of data_references known by this DataConnector from its _data_references_cache

        Returns:
            number of data_references known by this DataConnector.
        """
        total_references: int = len(self._get_data_references_cache())
        return total_references

    # Interface Method
    def get_matched_data_references(self) -> List[str]:
        """
        Returns the list of data_references matched by configuration by looping through items in
        _data_references_cache and returning data_references that have an associated data_asset.

        Returns:
            list of data_references that are matched by configuration.
        """
        return self._get_data_references(matched=True)

    # Interface Method
    def get_matched_data_reference_count(self) -> int:
        """
        Returns the list of matched data_references known by this DataConnector from its _data_references_cache

        Returns:
            number of matched data_references known by this DataConnector.
        """
        return len(self.get_matched_data_references())

    # Interface Method
    def get_unmatched_data_references(self) -> List[str]:
        """
        Returns the list of data_references unmatched by configuration by looping through items in
        _data_references_cache and returning data_references that do not have an associated data_asset.

        Returns:
            list of data_references that are not matched by configuration.
        """
        return self._get_data_references(matched=False)

    # Interface Method
    def get_unmatched_data_reference_count(self) -> int:
        """
        Returns the list of unmatched data_references known by this DataConnector from its _data_references_cache

        Returns:
            number of unmached data_references known by this DataConnector.
        """
        return len(self.get_unmatched_data_references())

    def _get_data_references(self, matched: bool) -> List[str]:
        """
        Returns the list of data_references unmatched by configuration by looping through items in
        _data_references_cache and returning data_references that do not have an associated data_asset.

        Returns:
            list of data_references that are not matched by configuration.
        """

        def _matching_criterion(
            batch_definition_list: Union[List[BatchDefinition], None]
        ) -> bool:
            return (
                (batch_definition_list is not None)
                if matched
                else (batch_definition_list is None)
            )

        data_reference_mapped_element: Tuple[str, Union[List[BatchDefinition], None]]
        # noinspection PyTypeChecker
        unmatched_data_references: List[str] = list(
            dict(
                filter(
                    lambda data_reference_mapped_element: _matching_criterion(
                        batch_definition_list=data_reference_mapped_element[1]
                    ),
                    self._get_data_references_cache().items(),
                )
            ).keys()
        )
        return unmatched_data_references

    # Interface Method
    def _generate_batch_spec_parameters_from_batch_definition(
        self, batch_definition: BatchDefinition
    ) -> dict:
        """
        This interface method examines "BatchDefinition" object and converts it to exactly one "data_reference" handle,
        based on partitioning behavior of given subclass (e.g., Regular Expressions for file path based DataConnector
        implementations).  Type of "data_reference" is storage dependent.  This method is then used to create storage
        system specific "BatchSpec" parameters for retrieving "Batch" of data.

        Args:
            batch_definition: input "BatchDefinition" object

        Returns:
            dict -- dictionary of "BatchSpec" properties
        """
        group_names: List[str] = self._regex_parser.get_all_group_names()
        path: str = map_batch_definition_to_data_reference_string_using_regex(
            batch_definition=batch_definition,
            regex_pattern=self._batching_regex,
            group_names=group_names,
        )
        if not path:
            raise ValueError(
                f"""No data reference for data asset name "{batch_definition.data_asset_name}" matches the given
batch identifiers {batch_definition.batch_identifiers} from batch definition {batch_definition}.
"""
            )

        path = self._get_full_file_path(path=path)

        return {FilePathDataConnector.FILE_PATH_BATCH_SPEC_KEY: path}

    def _ensure_regex_groups_include_data_reference_key(
        self, regex: re.Pattern
    ) -> re.Pattern:
        """
        Args:
            regex: regex pattern for filtering data references; if reserved group name "path" (FILE_PATH_BATCH_SPEC_KEY)
            is absent, then it is added to enclose original regex pattern, supplied on input.

        Returns:
            Potentially modified Regular Expression pattern (with enclosing FILE_PATH_BATCH_SPEC_KEY reserved group)
        """
        regex_parser = RegExParser(
            regex_pattern=regex,
            unnamed_regex_group_prefix=self._unnamed_regex_group_prefix,
        )
        group_names: List[str] = regex_parser.get_all_group_names()
        if FilePathDataConnector.FILE_PATH_BATCH_SPEC_KEY not in group_names:
            pattern: str = regex.pattern
            pattern = f"(?P<{FilePathDataConnector.FILE_PATH_BATCH_SPEC_KEY}>{pattern})"
            regex = re.compile(pattern)

        return regex

    def _get_data_references_cache(self) -> Dict[str, List[BatchDefinition] | None]:
        """
        This prototypical method populates cache, whose keys are data references and values are "BatchDefinition"
        objects.  Subsequently, "BatchDefinition" objects generated are amenable to flexible querying and sorting.

        It examines every "data_reference" handle and converts it to zero or more "BatchDefinition" objects, based on
        partitioning behavior of given subclass (e.g., Regular Expressions for file path based DataConnector
        implementations).  Type of each "data_reference" is storage dependent.
        """
        if len(self._data_references_cache) == 0:
            # Map data_references to batch_definitions.
            for data_reference in self.get_data_references():
                mapped_batch_definition_list: List[
                    BatchDefinition
                ] | None = self._map_data_reference_string_to_batch_definition_list_using_regex(
                    data_reference=data_reference
                )
                self._data_references_cache[
                    data_reference
                ] = mapped_batch_definition_list

        return self._data_references_cache

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

    def _get_batch_definition_list_from_data_references_cache(
        self,
    ) -> List[BatchDefinition]:
        batch_definition_list: List[BatchDefinition] = [
            batch_definitions[0]
            for batch_definitions in self._get_data_references_cache().values()
            if batch_definitions is not None
        ]
        return batch_definition_list

    def _map_data_reference_string_to_batch_definition_list_using_regex(
        self, data_reference: str
    ) -> List[BatchDefinition] | None:
        batch_identifiers: Optional[
            IDDict
        ] = self._convert_data_reference_string_to_batch_identifiers_using_regex(
            data_reference=data_reference
        )
        if batch_identifiers is None:
            return None

        # Importing at module level causes circular dependencies.
        from great_expectations.core.batch import BatchDefinition

        return [
            BatchDefinition(
                datasource_name=self._datasource_name,
                data_connector_name=_DATA_CONNECTOR_NAME,
                data_asset_name=self._data_asset_name,
                batch_identifiers=IDDict(batch_identifiers),
            )
        ]

    def _convert_data_reference_string_to_batch_identifiers_using_regex(
        self, data_reference: str
    ) -> Optional[IDDict]:
        # noinspection PyUnresolvedReferences
        matches: Optional[re.Match] = self._regex_parser.get_matches(
            target=data_reference
        )
        if matches is None:
            return None

        num_all_matched_group_values: int = (
            self._regex_parser.get_num_all_matched_group_values()
        )

        # Check for `(?P<name>)` named group syntax
        defined_group_name_to_group_index_mapping: Dict[
            str, int
        ] = self._regex_parser.get_named_group_name_to_group_index_mapping()
        defined_group_name_indexes: Set[int] = set(
            defined_group_name_to_group_index_mapping.values()
        )
        defined_group_name_to_group_value_mapping: Dict[str, str] = matches.groupdict()

        all_matched_group_values: List[str] = list(matches.groups())

        assert len(all_matched_group_values) == num_all_matched_group_values

        group_name_to_group_value_mapping: Dict[str, str] = copy.deepcopy(
            defined_group_name_to_group_value_mapping
        )

        idx: int
        group_idx: int
        matched_group_value: str
        for idx, matched_group_value in enumerate(all_matched_group_values):
            group_idx = idx + 1
            if group_idx not in defined_group_name_indexes:
                group_name: str = f"{self._unnamed_regex_group_prefix}{group_idx}"
                group_name_to_group_value_mapping[group_name] = matched_group_value

        batch_identifiers = IDDict(group_name_to_group_value_mapping)

        return batch_identifiers

    @abstractmethod
    def _get_full_file_path(self, path: str) -> str:
        pass
