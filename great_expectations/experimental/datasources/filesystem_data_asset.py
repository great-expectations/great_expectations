from __future__ import annotations

import copy
import logging
import pathlib
from typing import TYPE_CHECKING, List, Optional, Set, Tuple

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.batch_spec import PathBatchSpec
from great_expectations.experimental.datasources.file_path_data_asset import (
    _FilePathDataAsset,
)
from great_expectations.experimental.datasources.interfaces import (
    Batch,
    BatchRequest,
    BatchRequestOptions,
    TestConnectionError,
)

if TYPE_CHECKING:
    from great_expectations.execution_engine import (
        PandasExecutionEngine,
        SparkDFExecutionEngine,
    )
    from great_expectations.experimental.datasources import (
        PandasFilesystemDatasource,
        SparkDatasource,
    )

logger = logging.getLogger(__name__)


class _FilesystemDataAsset(_FilePathDataAsset):
    def _get_reader_method(self) -> str:
        raise NotImplementedError(
            """One needs to explicitly provide "reader_method" for Filesystem DataAsset extensions as temporary \
work-around, until "type" naming convention and method for obtaining 'reader_method' from it are established."""
        )

    def _get_reader_options_include(self) -> Set[str] | None:
        raise NotImplementedError(
            """One needs to explicitly provide set(str)-valued reader options for "pydantic.BaseModel.dict()" method \
to use as its "include" directive for Filesystem style DataAsset processing."""
        )

    def test_connection(self) -> None:
        """Test the connection for the FilesystemDataAsset.

        Raises:
            TestConnectionError: If the connection test fails.
        """
        datasource: PandasFilesystemDatasource | SparkDatasource = self.datasource

        success = False
        for filepath in datasource.base_directory.iterdir():
            if self.regex.match(filepath.name):
                # if one file in the path matches the regex, we consider this asset valid
                success = True
                break
        if not success:
            raise TestConnectionError(
                f"No file at path: {datasource.base_directory.resolve()} matched the regex: {self.regex.pattern}"
            )

    def _fully_specified_batch_requests_with_path(
        self, batch_request: BatchRequest
    ) -> List[Tuple[BatchRequest, pathlib.Path]]:
        """Generates a list fully specified batch requests from partial specified batch request

        Args:
            batch_request: A batch request

        Returns:
            A list of pairs (batch_request, path) where 'batch_request' is a fully specified
            batch request and 'path' is the path to the corresponding file on disk.
            This list will be empty if no files exist on disk that correspond to the input
            batch request.
        """
        datasource: PandasFilesystemDatasource | SparkDatasource = self.datasource

        base_directory: pathlib.Path = datasource.base_directory
        all_files: List[pathlib.Path] = list(pathlib.Path(base_directory).iterdir())

        batch_requests_with_path: List[Tuple[BatchRequest, pathlib.Path]] = []

        file_name: pathlib.Path
        for file_name in all_files:
            match = self._regex_parser.get_matches(target=file_name.name)
            if match:
                # Create the batch request that would correlate to this regex match
                match_options = {}
                for group_id in range(
                    1, self._regex_parser.get_num_all_matched_group_values() + 1
                ):
                    match_options[
                        self._all_group_index_to_group_name_mapping[group_id]
                    ] = match.group(group_id)
                # Determine if this file_name matches the batch_request
                allowed_match = True
                for key, value in batch_request.options.items():
                    if match_options[key] != value:
                        allowed_match = False
                        break
                if allowed_match:
                    batch_requests_with_path.append(
                        (
                            BatchRequest(
                                datasource_name=datasource.name,
                                data_asset_name=self.name,
                                options=match_options,
                            ),
                            base_directory / file_name,
                        )
                    )
                    logger.debug(f"Matching path: {base_directory / file_name}")
        if not batch_requests_with_path:
            logger.warning(
                f"Batch request {batch_request} corresponds to no data files."
            )
        return batch_requests_with_path

    def batch_request_options_template(
        self,
    ) -> BatchRequestOptions:
        idx: int
        return {idx: None for idx in self._all_group_names}

    def build_batch_request(
        self, options: Optional[BatchRequestOptions] = None
    ) -> BatchRequest:
        if options:
            for option, value in options.items():
                if (
                    option in self._all_group_name_to_group_index_mapping
                    and not isinstance(value, str)
                ):
                    raise ge_exceptions.InvalidBatchRequestError(
                        f"All regex matching options must be strings. The value of '{option}' is "
                        f"not a string: {value}"
                    )
        return super().build_batch_request(options)

    def get_batch_list_from_batch_request(
        self, batch_request: BatchRequest
    ) -> List[Batch]:
        self._validate_batch_request(batch_request)
        batch_list: List[Batch] = []

        kwargs: dict | None = getattr(self, "kwargs", None)
        if not kwargs:
            kwargs = {}

        for request, path in self._fully_specified_batch_requests_with_path(
            batch_request
        ):
            batch_spec = PathBatchSpec(
                path=str(path),
                reader_method=self._get_reader_method(),
                reader_options=self.dict(
                    include=self._get_reader_options_include(),
                    exclude=self._EXCLUDE_FROM_READER_OPTIONS,
                    exclude_unset=True,
                    by_alias=True,
                    **kwargs,
                ),
            )
            execution_engine: PandasExecutionEngine | SparkDFExecutionEngine = (
                self.datasource.get_execution_engine()
            )
            data, markers = execution_engine.get_batch_data_and_markers(
                batch_spec=batch_spec
            )

            # batch_definition (along with batch_spec and markers) is only here to satisfy a
            # legacy constraint when computing usage statistics in a validator. We hope to remove
            # it in the future.
            # imports are done inline to prevent a circular dependency with core/batch.py
            from great_expectations.core import IDDict
            from great_expectations.core.batch import BatchDefinition

            batch_definition = BatchDefinition(
                datasource_name=self.datasource.name,
                data_connector_name="experimental",
                data_asset_name=self.name,
                batch_identifiers=IDDict(request.options),
                batch_spec_passthrough=None,
            )

            batch_metadata = copy.deepcopy(request.options)
            batch_metadata["base_directory"] = path

            # Some pydantic annotations are postponed due to circular imports.
            # Batch.update_forward_refs() will set the annotations before we
            # instantiate the Batch class since we can import them in this scope.
            Batch.update_forward_refs()
            batch_list.append(
                Batch(
                    datasource=self.datasource,
                    data_asset=self,
                    batch_request=request,
                    data=data,
                    metadata=batch_metadata,
                    legacy_batch_markers=markers,
                    legacy_batch_spec=batch_spec,
                    legacy_batch_definition=batch_definition,
                )
            )
        self.sort_batches(batch_list)
        return batch_list
