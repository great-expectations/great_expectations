from __future__ import annotations

import copy
import logging
import re
import sre_constants
import sre_parse
from abc import abstractmethod
from collections import defaultdict
from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Set, Tuple, Union

from great_expectations.compatibility.typing_extensions import override
from great_expectations.core import IDDict
from great_expectations.core.batch import LegacyBatchDefinition
from great_expectations.core.batch_spec import BatchSpec, PathBatchSpec
from great_expectations.datasource.fluent.batch_identifier_util import make_batch_identifier
from great_expectations.datasource.fluent.constants import _DATA_CONNECTOR_NAME, MATCH_ALL_PATTERN
from great_expectations.datasource.fluent.data_connector import (
    DataConnector,
)
from great_expectations.datasource.fluent.data_connector.batch_filter import (
    BatchFilter,
    build_batch_filter,
)
from great_expectations.datasource.fluent.data_connector.regex_parser import (
    RegExParser,
)

if TYPE_CHECKING:
    from typing import DefaultDict

    from great_expectations.alias_types import PathStr
    from great_expectations.core.partitioners import FileNamePartitioner
    from great_expectations.datasource.fluent import BatchRequest

logger = logging.getLogger(__name__)


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

    def __init__(
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
        legacy_batch_definition_list: List[LegacyBatchDefinition] = (
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
        legacy_batch_definition_list = batch_filter_obj.select_from_data_connector_query(
            batch_definition_list=legacy_batch_definition_list
        )

        return legacy_batch_definition_list

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
        # todo: in the world of BatchDefinition, this method must accept a BatchRequest.
        #       In the meantime, we fall back to a regex that matches everything.
        regex = self._preprocess_batching_regex(MATCH_ALL_PATTERN)
        data_references = self._get_data_references_cache(batching_regex=regex)
        return len(data_references)

    # Interface Method
    @override
    def get_matched_data_references(self, regex: re.Pattern | None = None) -> List[str]:
        """
        Returns the list of data_references matched by configuration by looping through items in
        _data_references_cache and returning data_references that have an associated data_asset.

        Returns:
            list of data_references that are matched by configuration.
        """
        if regex:
            regex = self._preprocess_batching_regex(regex)
        return self._get_data_references(matched=True, regex=regex)

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
        self, batch_request: BatchRequest[FileNamePartitioner]
    ) -> list[LegacyBatchDefinition]:
        """Get all batch definitions for all files from a data connector
         using the supplied batch request.

        Args:
            batch_request: Specifies which batch definitions to get from data connector.

        Returns:
            A list of batch definitions from the data connector based on the batch request.
        """
        # this class is overloaded with two separate implementations:
        if self._whole_directory_path_override:
            return self._get_directory_batch_definition_list(batch_request=batch_request)
        else:
            return self._get_file_batch_definition_list(batch_request=batch_request)

    def _get_file_batch_definition_list(
        self, batch_request: BatchRequest
    ) -> list[LegacyBatchDefinition]:
        # Use a combination of a list and set to preserve iteration order
        batch_definition_list: list[LegacyBatchDefinition] = list()
        batch_definition_set = set()
        if batch_request.partitioner:
            batching_regex = self._preprocess_batching_regex(batch_request.partitioner.regex)
        else:
            # all batch requests coming from the V1 API should have a regex; to support legacy code
            # we fall back to the MATCH_ALL_PATTERN if it's missing.
            batching_regex = self._preprocess_batching_regex(MATCH_ALL_PATTERN)
        for batch_definition in self._get_batch_definitions(batching_regex=batching_regex):
            if (
                self._batch_definition_matches_batch_request(
                    batch_definition=batch_definition, batch_request=batch_request
                )
                and batch_definition not in batch_definition_set
            ):
                batch_definition_list.append(batch_definition)
                batch_definition_set.add(batch_definition)

        return batch_definition_list

    def _get_directory_batch_definition_list(
        self, batch_request: BatchRequest
    ) -> list[LegacyBatchDefinition]:
        data_directory = self._whole_directory_path_override
        batch_definition = LegacyBatchDefinition(
            datasource_name=self._datasource_name,
            data_connector_name=_DATA_CONNECTOR_NAME,
            data_asset_name=self._data_asset_name,
            batch_identifiers=make_batch_identifier({"path": data_directory}),
        )
        return [batch_definition]

    def _get_data_references(self, matched: bool, regex: re.Pattern | None = None) -> List[str]:
        """
        Returns the list of data_references unmatched by configuration by looping through items in
        _data_references_cache and returning data_references that do not have an associated data_asset.

        Returns:
            list of data_references that are not matched by configuration.
        """  # noqa: E501
        if not regex:
            regex = self._preprocess_batching_regex(MATCH_ALL_PATTERN)

        def _matching_criterion(
            batch_definition_list: Union[List[LegacyBatchDefinition], None],
        ) -> bool:
            return (
                (batch_definition_list is not None) if matched else (batch_definition_list is None)
            )

        data_reference_mapped_element: Tuple[str, Union[List[LegacyBatchDefinition], None]]
        data_references = self._get_data_references_cache(batching_regex=regex)
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
        # this class is overloaded with two separate implementations:
        if self._whole_directory_path_override:
            return self._get_batch_spec_params_directory(batch_definition=batch_definition)
        else:
            return self._get_batch_spec_params_file(batch_definition=batch_definition)

    def _get_batch_spec_params_file(self, batch_definition: LegacyBatchDefinition) -> dict:
        """File specific implementation of batch spec parameters"""
        if not batch_definition.batching_regex:
            raise RuntimeError("BatchDefinition must contain a batching_regex.")  # noqa: TRY003

        batching_regex = batch_definition.batching_regex

        regex_parser = RegExParser(
            regex_pattern=batching_regex,
            unnamed_regex_group_prefix=self._unnamed_regex_group_prefix,
        )
        group_names: List[str] = regex_parser.group_names()
        path: str = map_batch_definition_to_data_reference_string_using_regex(
            batch_definition=batch_definition,
            regex_pattern=batching_regex,
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

    def _get_batch_spec_params_directory(self, batch_definition: LegacyBatchDefinition) -> dict:
        """Directory specific implementation of batch spec parameters"""
        path = self._get_full_file_path(path=str(self._whole_directory_path_override))

        return {FilePathDataConnector.FILE_PATH_BATCH_SPEC_KEY: path}

    def _preprocess_batching_regex(self, regex: re.Pattern) -> re.Pattern:
        """Add the FILE_PATH_BATCH_SPEC_KEY group to regex if not already present."""
        regex_parser = RegExParser(
            regex_pattern=regex,
            unnamed_regex_group_prefix=self._unnamed_regex_group_prefix,
        )
        group_names: List[str] = regex_parser.group_names()
        if FilePathDataConnector.FILE_PATH_BATCH_SPEC_KEY not in group_names:
            pattern: str = regex.pattern
            pattern = f"(?P<{FilePathDataConnector.FILE_PATH_BATCH_SPEC_KEY}>{pattern})"
            regex = re.compile(pattern)

        return regex

    def _get_data_references_cache(
        self, batching_regex: re.Pattern
    ) -> Dict[str, List[LegacyBatchDefinition] | None]:
        """Access a map where keys are data references and values are LegacyBatchDefinitions."""

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

        batch_identifiers = make_batch_identifier(group_name_to_group_value_mapping)

        return batch_identifiers

    @abstractmethod
    def _get_full_file_path(self, path: str) -> str:
        pass


def map_batch_definition_to_data_reference_string_using_regex(
    batch_definition: LegacyBatchDefinition,
    regex_pattern: re.Pattern,
    group_names: List[str],
) -> str:
    if not isinstance(batch_definition, LegacyBatchDefinition):
        raise TypeError("batch_definition is not of an instance of type BatchDefinition")  # noqa: TRY003

    data_asset_name: str = batch_definition.data_asset_name
    batch_identifiers: IDDict = batch_definition.batch_identifiers
    data_reference: str = convert_batch_identifiers_to_data_reference_string_using_regex(
        batch_identifiers=batch_identifiers,
        regex_pattern=regex_pattern,
        group_names=group_names,
        data_asset_name=data_asset_name,
    )
    return data_reference


def convert_batch_identifiers_to_data_reference_string_using_regex(
    batch_identifiers: IDDict,
    regex_pattern: re.Pattern,
    group_names: List[str],
    data_asset_name: Optional[str] = None,
) -> str:
    if not isinstance(batch_identifiers, IDDict):
        raise TypeError("batch_identifiers is not " "an instance of type IDDict")  # noqa: TRY003

    template_arguments: dict = copy.deepcopy(batch_identifiers)
    if data_asset_name is not None:
        template_arguments["data_asset_name"] = data_asset_name

    filepath_template: str = _invert_regex_to_data_reference_template(
        regex_pattern=regex_pattern,
        group_names=group_names,
    )
    converted_string: str = filepath_template.format(**template_arguments)

    return converted_string


def _invert_regex_to_data_reference_template(  # noqa: C901 - too complex
    regex_pattern: re.Pattern | str,
    group_names: List[str],
) -> str:
    r"""Create a string template based on a regex and corresponding list of group names.

    For example:

        filepath_template = _invert_regex_to_data_reference_template(
            regex_pattern=r"^(.+)_(\d+)_(\d+)\.csv$",
            group_names=["name", "timestamp", "price"],
        )
        filepath_template
        >> "{name}_{timestamp}_{price}.csv"

    Such templates are useful because they can be populated using string substitution:

        filepath_template.format(**{
            "name": "user_logs",
            "timestamp": "20200101",
            "price": "250",
        })
        >> "user_logs_20200101_250.csv"


    NOTE Abe 20201017: This method is almost certainly still brittle. I haven't exhaustively mapped the OPCODES in sre_constants
    """  # noqa: E501
    data_reference_template: str = ""
    group_name_index: int = 0

    num_groups = len(group_names)

    if isinstance(regex_pattern, re.Pattern):
        regex_pattern = regex_pattern.pattern

    # print("-"*80)
    parsed_sre = sre_parse.parse(str(regex_pattern))
    for parsed_sre_tuple, char in zip(parsed_sre, list(str(regex_pattern))):  # type: ignore[call-overload]
        token, value = parsed_sre_tuple
        if token == sre_constants.LITERAL:
            # Transcribe the character directly into the template
            data_reference_template += chr(value)
        elif token == sre_constants.SUBPATTERN:
            if not (group_name_index < num_groups):
                break
            # Replace the captured group with "{next_group_name}" in the template
            data_reference_template += f"{{{group_names[group_name_index]}}}"
            group_name_index += 1
        elif token in [
            sre_constants.MAX_REPEAT,
            sre_constants.IN,
            sre_constants.BRANCH,
            sre_constants.ANY,
        ]:
            if group_names:
                # Replace the uncaptured group a wildcard in the template
                data_reference_template += "*"
            else:
                # Don't assume that a `.` in a filename should be a star glob
                data_reference_template += char
        elif token in [
            sre_constants.AT,
            sre_constants.ASSERT_NOT,
            sre_constants.ASSERT,
        ]:
            pass
        else:
            raise ValueError(f"Unrecognized regex token {token} in regex pattern {regex_pattern}.")  # noqa: TRY003

    # Collapse adjacent wildcards into a single wildcard
    data_reference_template: str = re.sub("\\*+", "*", data_reference_template)  # type: ignore[no-redef]

    return data_reference_template


def sanitize_prefix_for_gcs_and_s3(text: str) -> str:
    """
    Takes in a given user-prefix and cleans it to work with file-system traversal methods
    (i.e. add '/' to the end of a string meant to represent a directory)

    Customized for S3 paths, ignoring the path separator used by the host OS
    """
    text = text.strip()
    if not text:
        return text

    path_parts = text.split("/")
    if not path_parts:  # Empty prefix
        return text

    if "." in path_parts[-1]:  # File, not folder
        return text

    # Folder, should have trailing /
    return f"{text.rstrip('/')}/"
