from __future__ import annotations

import logging
import pathlib
import re
from typing import TYPE_CHECKING

from great_expectations.compatibility.typing_extensions import override
from great_expectations.core.batch import LegacyBatchDefinition
from great_expectations.core.id_dict import IDDict
from great_expectations.core.partitioners import (
    PartitionerDaily,
    PartitionerMonthly,
    PartitionerPath,
    PartitionerYearly,
)
from great_expectations.datasource.fluent.constants import (
    _DATA_CONNECTOR_NAME,
)
from great_expectations.datasource.fluent.data_asset.data_connector.regex_parser import RegExParser
from great_expectations.datasource.fluent.file_path_data_asset import _FilePathDataAsset

if TYPE_CHECKING:
    from great_expectations.alias_types import PathStr
    from great_expectations.core.batch_definition import BatchDefinition
    from great_expectations.datasource.fluent.batch_request import (
        BatchRequest,
    )

logger = logging.getLogger(__name__)


class _DirectoryDataAssetMixin(_FilePathDataAsset):
    """Mixin encapsulating logic specific to directory-based FilePathDataAssets"""

    data_directory: pathlib.Path

    @override
    def _get_batch_definition_list(
        self, batch_request: BatchRequest
    ) -> list[LegacyBatchDefinition]:
        """Generate a batch definition list from a given batch request, handling a partitioner config if present.

        Args:
            batch_request: Batch request used to generate batch definitions.

        Returns:
            List of batch definitions, in the case of a _DirectoryDataAssetMixin the list contains a single item.
        """  # noqa: E501
        if batch_request.partitioner:
            # Currently non-sql asset partitioners do not introspect the datasource for available
            # batches and only return a single batch based on specified batch_identifiers.
            batch_identifiers = batch_request.options
            if not batch_identifiers.get("path"):
                batch_identifiers["path"] = self.data_directory

            batch_definition = LegacyBatchDefinition(
                datasource_name=self._data_connector.datasource_name,
                data_connector_name=_DATA_CONNECTOR_NAME,
                data_asset_name=self._data_connector.data_asset_name,
                batch_identifiers=IDDict(batch_identifiers),
            )
            batch_definition_list = [batch_definition]
        else:
            batch_definition_list = self._data_connector.get_batch_definition_list(
                batch_request=batch_request
            )
        return batch_definition_list

    @override
    def get_whole_directory_path_override(
        self,
    ) -> PathStr:
        return self.data_directory

    @override
    def _get_reader_method(self) -> str:
        raise NotImplementedError(
            """One needs to explicitly provide "reader_method" for File-Path style DataAsset extensions as temporary \
work-around, until "type" naming convention and method for obtaining 'reader_method' from it are established."""  # noqa: E501
        )

    @override
    def _get_reader_options_include(self) -> set[str]:
        return {
            "data_directory",
        }


class RegexMissingRequiredGroupsError(ValueError):
    def __init__(self, missing_groups: set[str]):
        message = (
            "The following group(s) are required but are "
            f"missing from the regex: {', '.join(missing_groups)}"
        )
        super().__init__(message)
        self.missing_groups = missing_groups


class RegexUnknownGroupsError(ValueError):
    def __init__(self, unknown_groups: set[str]):
        message = (
            "Regex has the following group(s) which do not match "
            f"batch parameters: {', '.join(unknown_groups)}"
        )
        super().__init__(message)
        self.unknown_groups = unknown_groups


class PathNotFoundError(ValueError):
    def __init__(self, path: PathStr):
        message = f"Provided path was not able to be resolved: {path} "
        super().__init__(message)
        self.path = path


class AmbiguousPathError(ValueError):
    def __init__(self, path: PathStr):
        message = f"Provided path matched multiple targets, and must match exactly one: {path} "
        super().__init__(message)
        self.path = path


class _RegexDataAssetMixin(_FilePathDataAsset):
    """Mixin encapsulating logic specific to regex-based FilePathDataAssets"""

    def add_batch_definition_path(self, name: str, path: PathStr) -> BatchDefinition:
        """Adds a BatchDefinition which matches a single Path.

        Parameters:
            name: BatchDefinition name
            path: File path relative to the Asset

        Raises:
             PathNotFoundError: path cannot be resolved
             AmbiguousPathError: path matches more than one file
        """
        regex = re.compile(str(path))
        matched_data_references = len(self._data_connector.get_matched_data_references(regex=regex))
        # we require path to match exactly 1 file
        if matched_data_references < 1:
            raise PathNotFoundError(path=path)
        elif matched_data_references > 1:
            raise AmbiguousPathError(path=path)
        return self.add_batch_definition(
            name=name,
            partitioner=PartitionerPath(regex=regex),
        )

    def add_batch_definition_yearly(self, name: str, regex: re.Pattern) -> BatchDefinition:
        """Adds a BatchDefinition which defines yearly batches by file name.

        Parameters:
            name: BatchDefinition name
            regex: Regular Expression used to define batches by file name.
                Must contain a single group `year`

        Raises:
            RegexMissingRequiredGroupsError: regex is missing the group `year`
            RegexUnknownGroupsError: regex has groups other than `year`
        """
        REQUIRED_GROUP_NAME = {"year"}
        self._assert_group_names_in_regex(regex=regex, required_group_names=REQUIRED_GROUP_NAME)
        return self.add_batch_definition(
            name=name,
            partitioner=PartitionerYearly(regex=regex),
        )

    def add_batch_definition_monthly(self, name: str, regex: re.Pattern) -> BatchDefinition:
        """Adds a BatchDefinition which defines monthly batches by file name.

        Parameters:
            name: BatchDefinition name
            regex: Regular Expression used to define batches by file name.
                Must contain the groups `year` and `month`.

        Raises:
            RegexMissingRequiredGroupsError: regex is missing the groups `year` and/or `month`.
            RegexUnknownGroupsError: regex has groups other than `year` and/or `month`.
        """
        REQUIRED_GROUP_NAMES = {"year", "month"}
        self._assert_group_names_in_regex(regex=regex, required_group_names=REQUIRED_GROUP_NAMES)
        return self.add_batch_definition(
            name=name,
            partitioner=PartitionerMonthly(regex=regex),
        )

    def add_batch_definition_daily(self, name: str, regex: re.Pattern) -> BatchDefinition:
        """Adds a BatchDefinition which defines daily batches by file name.

        Parameters:
            name: BatchDefinition name
            regex: Regular Expression used to define batches by file name.
                Must contain the groups `year`, `month`, and `day`.

        Raises:
            RegexMissingRequiredGroupsError: regex is missing the
                groups `year`, `month`, and/or `day`.
            RegexUnknownGroupsError: regex has groups other than `year`, `month`, and/or `day`.
        """
        REQUIRED_GROUP_NAMES = {"year", "month", "day"}
        self._assert_group_names_in_regex(regex=regex, required_group_names=REQUIRED_GROUP_NAMES)
        return self.add_batch_definition(
            name=name,
            partitioner=PartitionerDaily(regex=regex),
        )

    @classmethod
    def _assert_group_names_in_regex(
        cls, regex: re.Pattern, required_group_names: set[str]
    ) -> None:
        regex_parser = RegExParser(
            regex_pattern=regex,
        )
        actual_group_names = set(regex_parser.group_names())
        if not required_group_names.issubset(actual_group_names):
            missing_groups = required_group_names - actual_group_names
            raise RegexMissingRequiredGroupsError(missing_groups)
        if not actual_group_names.issubset(required_group_names):
            unknown_groups = actual_group_names - required_group_names
            raise RegexUnknownGroupsError(unknown_groups)
