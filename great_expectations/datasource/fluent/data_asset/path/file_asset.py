from __future__ import annotations

import re
from abc import ABC
from typing import TYPE_CHECKING, Generic, List, Optional, Pattern

from great_expectations import exceptions as gx_exceptions
from great_expectations._docs_decorators import public_api
from great_expectations.compatibility import pydantic
from great_expectations.compatibility.typing_extensions import override
from great_expectations.core.partitioners import (
    PartitionerDaily,
    PartitionerMonthly,
    PartitionerPath,
    PartitionerYearly,
    RegexPartitioner,
)
from great_expectations.datasource.fluent import BatchRequest
from great_expectations.datasource.fluent.constants import MATCH_ALL_PATTERN
from great_expectations.datasource.fluent.data_asset.path.path_data_asset import (
    PathDataAsset,
)
from great_expectations.datasource.fluent.data_connector import FILE_PATH_BATCH_SPEC_KEY
from great_expectations.datasource.fluent.data_connector.regex_parser import RegExParser
from great_expectations.datasource.fluent.interfaces import DatasourceT

if TYPE_CHECKING:
    from great_expectations.alias_types import PathStr
    from great_expectations.core.batch import LegacyBatchDefinition
    from great_expectations.core.batch_definition import BatchDefinition
    from great_expectations.datasource.data_connector.batch_filter import BatchSlice
    from great_expectations.datasource.fluent import BatchParameters


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


class FileDataAsset(PathDataAsset[DatasourceT, RegexPartitioner], Generic[DatasourceT], ABC):
    """Base class for PathDataAssets which batch by file."""

    batching_regex: Pattern = (  # must use typing.Pattern for pydantic < v1.10
        MATCH_ALL_PATTERN
    )
    _unnamed_regex_param_prefix: str = pydantic.PrivateAttr(default="batch_request_param_")
    _regex_parser: RegExParser = pydantic.PrivateAttr()

    _group_names: List[str] = pydantic.PrivateAttr()

    def __init__(self, **data):
        super().__init__(**data)

        self._regex_parser = RegExParser(
            regex_pattern=self.batching_regex,
            unnamed_regex_group_prefix=self._unnamed_regex_param_prefix,
        )

        self._group_names = self._regex_parser.group_names()

    @public_api
    def add_batch_definition_path(self, name: str, path: PathStr) -> BatchDefinition:
        """Add a BatchDefinition which matches a single Path.

        Args:
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

    @public_api
    def add_batch_definition_yearly(
        self, name: str, regex: re.Pattern, sort_ascending: bool = True
    ) -> BatchDefinition:
        """Add a BatchDefinition which defines yearly batches by file name.

        Args:
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
            partitioner=PartitionerYearly(regex=regex, sort_ascending=sort_ascending),
        )

    @public_api
    def add_batch_definition_monthly(
        self, name: str, regex: re.Pattern, sort_ascending: bool = True
    ) -> BatchDefinition:
        """Add a BatchDefinition which defines monthly batches by file name.

        Args:
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
            partitioner=PartitionerMonthly(regex=regex, sort_ascending=sort_ascending),
        )

    @public_api
    def add_batch_definition_daily(
        self, name: str, regex: re.Pattern, sort_ascending: bool = True
    ) -> BatchDefinition:
        """Add a BatchDefinition which defines daily batches by file name.

        Args:
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
            partitioner=PartitionerDaily(regex=regex, sort_ascending=sort_ascending),
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

    @override
    def _get_batch_definition_list(
        self, batch_request: BatchRequest
    ) -> list[LegacyBatchDefinition]:
        batch_definition_list = self._data_connector.get_batch_definition_list(
            batch_request=batch_request
        )
        return batch_definition_list

    @override
    def get_batch_parameters_keys(
        self,
        partitioner: Optional[RegexPartitioner] = None,
    ) -> tuple[str, ...]:
        option_keys: tuple[str, ...] = tuple(self._group_names) + (FILE_PATH_BATCH_SPEC_KEY,)
        if partitioner:
            option_keys += tuple(partitioner.param_names)
        return option_keys

    @override
    def get_whole_directory_path_override(
        self,
    ) -> None:
        return None

    @override
    def build_batch_request(
        self,
        options: Optional[BatchParameters] = None,
        batch_slice: Optional[BatchSlice] = None,
        partitioner: Optional[RegexPartitioner] = None,
    ) -> BatchRequest:
        """A batch request that can be used to obtain batches for this DataAsset.

        Args:
            options: A dict that can be used to filter the batch groups returned from the asset.
                The dict structure depends on the asset type. The available keys for dict can be obtained by
                calling get_batch_parameters_keys(...).
            batch_slice: A python slice that can be used to limit the sorted batches by index.
                e.g. `batch_slice = "[-5:]"` will request only the last 5 batches after the options filter is applied.
            partitioner: A Partitioner used to narrow the data returned from the asset.

        Returns:
            A BatchRequest object that can be used to obtain a batch list from a Datasource by calling the
            get_batch_list_from_batch_request method.

        Note:
            Option "batch_slice" is supported for all "DataAsset" extensions of this class identically.  This mechanism
            applies to every "Datasource" type and any "ExecutionEngine" that is capable of loading data from files on
            local and/or cloud/networked filesystems (currently, Pandas and Spark backends work with files).
        """  # noqa: E501
        if options:
            for option, value in options.items():
                if option in self._group_names and value and not isinstance(value, str):
                    raise gx_exceptions.InvalidBatchRequestError(  # noqa: TRY003
                        f"All batching_regex matching options must be strings. The value of '{option}' is "  # noqa: E501
                        f"not a string: {value}"
                    )

        if options is not None and not self._batch_parameters_are_valid(
            options=options,
            partitioner=partitioner,
        ):
            allowed_keys = set(self.get_batch_parameters_keys(partitioner=partitioner))
            actual_keys = set(options.keys())
            raise gx_exceptions.InvalidBatchRequestError(  # noqa: TRY003
                "Batch parameters should only contain keys from the following set:\n"
                f"{allowed_keys}\nbut your specified keys contain\n"
                f"{actual_keys.difference(allowed_keys)}\nwhich is not valid.\n"
            )

        return BatchRequest(
            datasource_name=self.datasource.name,
            data_asset_name=self.name,
            options=options or {},
            batch_slice=batch_slice,
            partitioner=partitioner,
        )
