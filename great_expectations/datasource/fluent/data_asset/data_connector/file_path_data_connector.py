from __future__ import annotations

import copy
import logging
import re
from abc import abstractmethod
from collections import defaultdict
from typing import TYPE_CHECKING, Callable, Dict, Final, List, Optional, Set, Tuple, Union

from great_expectations.compatibility.typing_extensions import override
from great_expectations.core import IDDict
from great_expectations.core.batch import LegacyBatchDefinition
from great_expectations.core.batch_spec import BatchSpec, PathBatchSpec
from great_expectations.datasource.data_connector.batch_filter import (
    BatchFilter,
    build_batch_filter,
)
from great_expectations.datasource.data_connector.util import (
    map_batch_definition_to_data_reference_string_using_regex,
)
from great_expectations.datasource.fluent.constants import _DATA_CONNECTOR_NAME, MATCH_ALL_PATTERN
from great_expectations.datasource.fluent.data_asset.data_connector import (
    DataConnector,
)
from great_expectations.datasource.fluent.data_asset.data_connector.regex_parser import (
    RegExParser,
)

if TYPE_CHECKING:
    from typing import DefaultDict

    from great_expectations.alias_types import PathStr
    from great_expectations.datasource.fluent import BatchRequest

logger = logging.getLogger(__name__)


class MissingBatchingRegex(Exception):
    def __init__(self, parent_name: str | None = None):
        message = "Parameter batching_regex is required but missing"
        if parent_name:
            message += f" from {parent_name}"
        super().__init__(message)
        self.class_name = parent_name


class FilePathDataConnector(DataConnector):
    """The base class for Data Connectors designed to access filesystem-like data.

    This can include traditional, disk-based filesystems or object stores such as S3, GCS, or ABS.

    See the `DataConnector` base class for more information on the role of Data Connectors.

    Note that `FilePathDataConnector` is not meant to be used on its own, but extended.

    Args:
        datasource_name: The name of the Datasource associated with this DataConnector instance
        data_asset_name: The name of the DataAsset using this DataConnector instance
    """

    FILE_PATH_BATCH_SPEC_KEY = "path"

    def __init__(  # noqa: PLR0913
        self,
        datasource_name: str,
        data_asset_name: str,
        unnamed_regex_group_prefix: str = "batch_request_param_",
        file_path_template_map_fn: Optional[Callable] = None,
        whole_directory_path_override: PathStr | None = None,
    ) -> None:
        super().__init__(
            datasource_name=datasource_name,
            data_asset_name=data_asset_name,
        )

        self._unnamed_regex_group_prefix: str = unnamed_regex_group_prefix

        # todo: remove this when DataConnector.test_connection supports passing in a batch request
        self._default_batching_regex: Final[re.Pattern] = self._build_batching_regex(
            regex=MATCH_ALL_PATTERN
        )

        self._file_path_template_map_fn: Optional[Callable] = file_path_template_map_fn

        # allow callers to always treat entire directory as single asset
        self._whole_directory_path_override = whole_directory_path_override

        # This is a dictionary which maps data_references onto batch_requests.
        self._data_references_cache: DefaultDict[
            re.Pattern, Dict[str, List[LegacyBatchDefinition] | None]
        ] = defaultdict(dict)

    # Interface Method
    @override
    def get_batch_definition_list(self, batch_request: BatchRequest) -> List[LegacyBatchDefinition]:
        """
        Retrieve batch_definitions and that match batch_request.

        First retrieves all batch_definitions that match batch_request
            - if batch_request also has a batch_filter, then select batch_definitions that match batch_filter.

        Args:
            batch_request (BatchRequest): BatchRequest (containing previously validated attributes) to process

        Returns:
            A list of BatchDefinition objects that match BatchRequest

        """  # noqa: E501
        batch_definition_list: List[LegacyBatchDefinition] = (
            self._get_unfiltered_batch_definition_list(batch_request=batch_request)
        )

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

    @override
    def build_batch_spec(self, batch_definition: LegacyBatchDefinition) -> PathBatchSpec:
        """
        Build BatchSpec from batch_definition by calling DataConnector's build_batch_spec function.

        Args:
            batch_definition (LegacyBatchDefinition): to be used to build batch_spec

        Returns:
            BatchSpec built from batch_definition
        """
        batch_spec: BatchSpec = super().build_batch_spec(batch_definition=batch_definition)
        return PathBatchSpec(batch_spec)

    # Interface Method
    @override
    def get_data_reference_count(self) -> int:
        data_references = self._get_data_references_cache(
            batching_regex=self._default_batching_regex
        )
        return len(data_references)

    # Interface Method
    @override
    def get_matched_data_references(self) -> List[str]:
        """
        Returns the list of data_references matched by configuration by looping through items in
        _data_references_cache and returning data_references that have an associated data_asset.

        Returns:
            list of data_references that are matched by configuration.
        """
        return self._get_data_references(matched=True)

    # Interface Method
    @override
    def get_matched_data_reference_count(self) -> int:
        """
        Returns the list of matched data_references known by this DataConnector from its _data_references_cache

        Returns:
            number of matched data_references known by this DataConnector.
        """  # noqa: E501
        return len(self.get_matched_data_references())

    # Interface Method
    @override
    def get_unmatched_data_references(self) -> List[str]:
        """
        Returns the list of data_references unmatched by configuration by looping through items in
        _data_references_cache and returning data_references that do not have an associated data_asset.

        Returns:
            list of data_references that are not matched by configuration.
        """  # noqa: E501
        return self._get_data_references(matched=False)

    # Interface Method
    @override
    def get_unmatched_data_reference_count(self) -> int:
        """
        Returns the list of unmatched data_references known by this DataConnector from its _data_references_cache

        Returns:
            number of unmached data_references known by this DataConnector.
        """  # noqa: E501
        return len(self.get_unmatched_data_references())

    def _get_unfiltered_batch_definition_list(
        self, batch_request: BatchRequest
    ) -> list[LegacyBatchDefinition]:
        """Get all batch definitions for all files from a data connector
         using the supplied batch request.

        Args:
            batch_request: Specifies which batch definitions to get from data connector.

        Returns:
            A list of batch definitions from the data connector based on the batch request.
        """
        if self._whole_directory_path_override:
            # if the override is present, we don't build BatchDefinitions based on a regex,
            # we just make a single BatchDefinition to capture the entire directory
            return self._get_whole_directory_batch_definition_list(
                batch_request=batch_request, data_directory=self._whole_directory_path_override
            )

        if not batch_request.batching_regex:
            raise MissingBatchingRegex(parent_name="BatchRequest")
        # Use a combination of a list and set to preserve iteration order
        batch_definition_list: list[LegacyBatchDefinition] = list()
        batch_definition_set = set()
        for batch_definition in self._get_batch_definitions(
            batching_regex=batch_request.batching_regex
        ):
            if (
                self._batch_definition_matches_batch_request(
                    batch_definition=batch_definition, batch_request=batch_request
                )
                and batch_definition not in batch_definition_set
            ):
                batch_definition_list.append(batch_definition)
                batch_definition_set.add(batch_definition)

        return batch_definition_list

    def _get_whole_directory_batch_definition_list(
        self, batch_request: BatchRequest, data_directory: PathStr
    ) -> list[LegacyBatchDefinition]:
        batch_definition = LegacyBatchDefinition(
            datasource_name=self._datasource_name,
            data_connector_name=_DATA_CONNECTOR_NAME,
            data_asset_name=self._data_asset_name,
            batch_identifiers=IDDict({"path": data_directory}),
        )
        return [batch_definition]

    def _get_data_references(self, matched: bool) -> List[str]:
        """
        Returns the list of data_references unmatched by configuration by looping through items in
        _data_references_cache and returning data_references that do not have an associated data_asset.

        Returns:
            list of data_references that are not matched by configuration.
        """  # noqa: E501

        def _matching_criterion(
            batch_definition_list: Union[List[LegacyBatchDefinition], None],
        ) -> bool:
            return (
                (batch_definition_list is not None) if matched else (batch_definition_list is None)
            )

        data_reference_mapped_element: Tuple[str, Union[List[LegacyBatchDefinition], None]]
        data_references = self._get_data_references_cache(
            batching_regex=self._default_batching_regex
        )
        # noinspection PyTypeChecker
        unmatched_data_references: List[str] = list(
            dict(
                filter(
                    lambda data_reference_mapped_element: _matching_criterion(
                        batch_definition_list=data_reference_mapped_element[1]
                    ),
                    data_references.items(),
                )
            ).keys()
        )
        return unmatched_data_references

    # Interface Method
    @override
    def _generate_batch_spec_parameters_from_batch_definition(
        self, batch_definition: LegacyBatchDefinition
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
        """  # noqa: E501
        if not batch_definition.batching_regex:
            raise MissingBatchingRegex(parent_name="LegacyBatchDefinition")
        regex_parser = RegExParser(
            regex_pattern=batch_definition.batching_regex,
            unnamed_regex_group_prefix=self._unnamed_regex_group_prefix,
        )
        group_names: List[str] = regex_parser.get_all_group_names()
        path: str = map_batch_definition_to_data_reference_string_using_regex(
            batch_definition=batch_definition,
            regex_pattern=batch_definition.batching_regex,
            group_names=group_names,
        )
        if not path:
            raise ValueError(  # noqa: TRY003
                f"""No data reference for data asset name "{batch_definition.data_asset_name}" matches the given
batch identifiers {batch_definition.batch_identifiers} from batch definition {batch_definition}.
"""  # noqa: E501
            )

        path = self._get_full_file_path(path=path)

        return {FilePathDataConnector.FILE_PATH_BATCH_SPEC_KEY: path}

    def _build_batching_regex(self, regex: re.Pattern) -> re.Pattern:
        """Construct the fully qualified regex used to identify Batches."""
        # This implementation adds the FILE_PATH_BATCH_SPEC_KEY to regex, if not already present
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

    def _get_data_references_cache(
        self, batching_regex: re.Pattern
    ) -> Dict[str, List[LegacyBatchDefinition] | None]:
        """Access a map where keys are data references and values are LegacyBatchDefinitions."""
        batching_regex = self._build_batching_regex(regex=batching_regex)
        batch_definitions = self._data_references_cache[batching_regex]
        if batch_definitions:
            return batch_definitions

        # Cache was empty so we need to calculate BatchDefinitions
        for data_reference in self.get_data_references():
            batch_definition = self._build_batch_definition(
                data_reference=data_reference, batching_regex=batching_regex
            )
            if batch_definition:
                # storing these as a list seems unnecessary; in this implementation
                # there can only be one or zero BatchDefinitions per data reference
                batch_definitions[data_reference] = [batch_definition]
            else:
                batch_definitions[data_reference] = None

        return batch_definitions

    def _get_batch_definitions(self, batching_regex: re.Pattern) -> List[LegacyBatchDefinition]:
        batch_definition_map = self._get_data_references_cache(batching_regex=batching_regex)
        batch_definitions = [
            batch_definitions[0]
            for batch_definitions in batch_definition_map.values()
            if batch_definitions is not None
        ]
        return batch_definitions

    def _build_batch_definition(
        self, data_reference: str, batching_regex: re.Pattern
    ) -> LegacyBatchDefinition | None:
        batch_identifiers = self._build_batch_identifiers(
            data_reference=data_reference, batching_regex=batching_regex
        )
        if batch_identifiers is None:
            return None

        from great_expectations.core.batch import LegacyBatchDefinition

        return LegacyBatchDefinition(
            datasource_name=self._datasource_name,
            data_connector_name=_DATA_CONNECTOR_NAME,
            data_asset_name=self._data_asset_name,
            batch_identifiers=batch_identifiers,
            batching_regex=batching_regex,
        )

    def _build_batch_identifiers(
        self, data_reference: str, batching_regex: re.Pattern
    ) -> Optional[IDDict]:
        regex_parser = RegExParser(
            regex_pattern=batching_regex,
            unnamed_regex_group_prefix=self._unnamed_regex_group_prefix,
        )
        matches: Optional[re.Match] = regex_parser.get_matches(target=data_reference)
        if matches is None:
            return None

        num_all_matched_group_values: int = regex_parser.get_num_all_matched_group_values()

        # Check for `(?P<name>)` named group syntax
        defined_group_name_to_group_index_mapping: Dict[str, int] = (
            regex_parser.get_named_group_name_to_group_index_mapping()
        )
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
