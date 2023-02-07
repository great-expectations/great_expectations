from __future__ import annotations

import copy
import logging
import pathlib
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Dict,
    List,
    Optional,
    Pattern,
    Set,
    Tuple,
)

import pydantic

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.batch_spec import BatchMarkers, PathBatchSpec
from great_expectations.experimental.datasources.data_asset.data_connector.filesystem_data_connector import (
    FilesystemDataConnector,
)
from great_expectations.experimental.datasources.data_asset.data_connector.regex_parser import (
    RegExParser,
)
from great_expectations.experimental.datasources.interfaces import (
    Batch,
    BatchRequest,
    BatchRequestOptions,
    DataAsset,
    TestConnectionError,
)

if TYPE_CHECKING:
    from great_expectations.execution_engine import (
        PandasExecutionEngine,
        SparkDFExecutionEngine,
    )

logger = logging.getLogger(__name__)


class _FilesystemDataAsset(DataAsset):
    # Pandas specific class attrs
    _EXCLUDE_FROM_READER_OPTIONS: ClassVar[Set[str]] = {
        "name",
        "base_directory",
        "regex",
        "glob_directive",
        "order_by",
        "type",
    }

    # Filesystem specific attributes
    base_directory: pathlib.Path
    regex: Pattern
    glob_directive: str = "**/*"

    # Internal attributes
    _unnamed_regex_param_prefix: str = pydantic.PrivateAttr(
        default="batch_request_param_"
    )
    _regex_parser: RegExParser = pydantic.PrivateAttr()

    _all_group_name_to_group_index_mapping: Dict[str, int] = pydantic.PrivateAttr()
    _all_group_index_to_group_name_mapping: Dict[int, str] = pydantic.PrivateAttr()
    _all_group_names: List[str] = pydantic.PrivateAttr()

    # TODO: <Alex>ALEX</Alex>
    # _data_connector: FilesystemDataConnector = pydantic.PrivateAttr()
    # TODO: <Alex>ALEX</Alex>

    class Config:
        """
        Need to allow extra fields for the base type because pydantic will first create
        an instance of `_FilesystemDataAsset` before we select and create the more specific
        asset subtype.
        Each specific subtype should `forbid` extra fields.
        """

        extra = pydantic.Extra.allow

    def __init__(self, **data):
        super().__init__(**data)
        self._regex_parser = RegExParser(
            regex_pattern=self.regex,
            unnamed_regex_group_prefix=self._unnamed_regex_param_prefix,
        )

        self._all_group_name_to_group_index_mapping = (
            self._regex_parser.get_all_group_name_to_group_index_mapping()
        )
        self._all_group_index_to_group_name_mapping = (
            self._regex_parser.get_all_group_index_to_group_name_mapping()
        )
        self._all_group_names = self._regex_parser.get_all_group_names()

        # TODO: <Alex>ALEX</Alex>
        # self._data_connector = FilesystemDataConnector(
        #     name="experimental",
        #     datasource_name=self.datasource.name,
        #     data_asset_name=self.name,
        #     base_directory=self.base_directory,
        #     regex=self.regex,
        #     glob_directive=self.glob_directive,
        # )
        # TODO: <Alex>ALEX</Alex>

    def _get_reader_method(self) -> str:
        raise NotImplementedError(
            """One needs to explicitly provide "reader_method" for File-Path style DataAsset extensions as temporary \
work-around, until "type" naming convention and method for obtaining 'reader_method' from it are established."""
        )

    def _get_reader_options_include(self) -> Set[str] | None:
        raise NotImplementedError(
            """One needs to explicitly provide set(str)-valued reader options for "pydantic.BaseModel.dict()" method \
to use as its "include" directive for File-Path style DataAsset processing."""
        )

    def test_connection(self) -> None:
        """Test the connection for the CSVAsset.

        Raises:
            TestConnectionError: If the connection test fails.
        """
        if not self.base_directory.exists():
            raise TestConnectionError(
                f"Path: {self.base_directory.resolve()} does not exist."
            )

        success = False
        for filepath in self.base_directory.iterdir():
            if self.regex.match(filepath.name):
                # if one file in the path matches the regex, we consider this asset valid
                success = True
                break
        if not success:
            raise TestConnectionError(
                f"No file at path: {self.base_directory} matched the regex: {self.regex.pattern}"
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
        batch_requests_with_path: List[Tuple[BatchRequest, pathlib.Path]] = []

        all_files: List[pathlib.Path] = list(
            pathlib.Path(self.base_directory).iterdir()
        )

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
                                datasource_name=self.datasource.name,
                                data_asset_name=self.name,
                                options=match_options,
                            ),
                            self.base_directory / file_name,
                        )
                    )
                    logger.debug(f"Matching path: {self.base_directory / file_name}")
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

    def get_batch_request(
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
        return super().get_batch_request(options)

    def get_batch_list_from_batch_request(
        self, batch_request: BatchRequest
    ) -> List[Batch]:
        print(
            f"\n[ALEX_TEST] [_FilesystemDataAsset.get_batch_list_from_batch_request()] BATCH_REQUEST.OPTIONS:\n{batch_request.options} ; TYPE: {str(type(batch_request.options))}"
        )
        self._validate_batch_request(batch_request)

        # TODO: <Alex>ALEX</Alex>
        execution_engine: PandasExecutionEngine | SparkDFExecutionEngine = (
            self.datasource.get_execution_engine()
        )

        # Certain imports are done inline in order to prevent a circular dependency with "core/batch.py".
        from great_expectations.core.batch import BatchDefinition

        # TODO: <Alex>ALEX</Alex>
        data_connector = FilesystemDataConnector(
            name="experimental",
            datasource_name=self.datasource.name,
            data_asset_name=self.name,
            base_directory=self.base_directory,
            regex=self.regex,
            glob_directive=self.glob_directive,
        )
        # TODO: <Alex>ALEX</Alex>

        batch_definition_list: List[
            BatchDefinition
        ] = data_connector.get_batch_definition_list_from_batch_request(
            batch_request=batch_request
        )

        batch_list: List[Batch] = []

        batch_spec: PathBatchSpec
        batch_spec_options: dict
        batch_data: Any
        batch_markers: BatchMarkers
        batch_metadata: BatchRequestOptions
        batch: Batch
        for batch_definition in batch_definition_list:
            batch_spec = data_connector.build_batch_spec(
                batch_definition=batch_definition
            )
            batch_spec_options = {
                "reader_method": self._get_reader_method(),
                "reader_options": self.dict(
                    include=self._get_reader_options_include(),
                    exclude=self._EXCLUDE_FROM_READER_OPTIONS,
                    exclude_unset=True,
                    by_alias=True,
                ),
            }
            batch_spec.update(batch_spec_options)
            print(
                f"\n[ALEX_TEST] [_FilesystemDataAsset.get_batch_list_from_batch_request()] BATCH_SPEC:\n{batch_spec} ; TYPE: {str(type(batch_spec))}"
            )

            batch_data, batch_markers = execution_engine.get_batch_data_and_markers(
                batch_spec=batch_spec
            )

            batch_metadata = copy.deepcopy(batch_request.options)
            print(
                f"\n[ALEX_TEST] [_FilesystemDataAsset.get_batch_list_from_batch_request()] BATCH_METADATA:\n{batch_metadata} ; TYPE: {str(type(batch_metadata))}"
            )
            # TODO: <Alex>ALEX-FIX_TO_INSURE_BASE_DIRECTORY_TYPE_IS_PATHLIB.PATH-CONSISTENTLY</Alex>
            # TODO: <Alex>ALEX</Alex>
            # TODO: <Alex>ALEX-FIX_TO_INSURE_PROPERTY_NAME_CORRESPONDDS_TO_ITS_MEANING</Alex>
            batch_metadata["base_directory"] = pathlib.Path(batch_spec["path"])
            # TODO: <Alex>ALEX</Alex>

            # Some pydantic annotations are postponed due to circular imports.
            # Batch.update_forward_refs() will set the annotations before we
            # instantiate the Batch class since we can import them in this scope.
            Batch.update_forward_refs()

            batch = Batch(
                datasource=self.datasource,
                data_asset=self,
                batch_request=batch_request,
                data=batch_data,
                metadata=batch_metadata,
                legacy_batch_markers=batch_markers,
                legacy_batch_spec=batch_spec,
                legacy_batch_definition=batch_definition,
            )
            batch_list.append(batch)

        # TODO: <Alex>ALEX</Alex>
        # return batch_list
        # TODO: <Alex>ALEX</Alex>

        # TODO: <Alex>ALEX</Alex>
        # batch_list: List[Batch] = []
        #
        # for request, path in self._fully_specified_batch_requests_with_path(
        #     batch_request
        # ):
        #     batch_spec = PathBatchSpec(
        #         path=str(path),
        #         reader_method=self._get_reader_method(),
        #         reader_options=self.dict(
        #             include=self._get_reader_options_include(),
        #             exclude=self._EXCLUDE_FROM_READER_OPTIONS,
        #             exclude_unset=True,
        #             by_alias=True,
        #         ),
        #     )
        #     execution_engine: PandasExecutionEngine | SparkDFExecutionEngine = (
        #         self.datasource.get_execution_engine()
        #     )
        #     data, markers = execution_engine.get_batch_data_and_markers(
        #         batch_spec=batch_spec
        #     )
        #
        #     # batch_definition (along with batch_spec and markers) is only here to satisfy a
        #     # legacy constraint when computing usage statistics in a validator. We hope to remove
        #     # it in the future.
        #     # imports are done inline to prevent a circular dependency with core/batch.py
        #     from great_expectations.core import IDDict
        #     from great_expectations.core.batch import BatchDefinition
        #
        #     batch_definition = BatchDefinition(
        #         datasource_name=self.datasource.name,
        #         data_connector_name="experimental",
        #         data_asset_name=self.name,
        #         batch_identifiers=IDDict(request.options),
        #         batch_spec_passthrough=None,
        #     )
        #
        #     batch_metadata = copy.deepcopy(request.options)
        #     batch_metadata["base_directory"] = path
        #
        #     # Some pydantic annotations are postponed due to circular imports.
        #     # Batch.update_forward_refs() will set the annotations before we
        #     # instantiate the Batch class since we can import them in this scope.
        #     Batch.update_forward_refs()
        #     batch_list.append(
        #         Batch(
        #             datasource=self.datasource,
        #             data_asset=self,
        #             batch_request=request,
        #             data=data,
        #             metadata=batch_metadata,
        #             legacy_batch_markers=markers,
        #             legacy_batch_spec=batch_spec,
        #             legacy_batch_definition=batch_definition,
        #         )
        #     )
        # TODO: <Alex>ALEX</Alex>

        self.sort_batches(batch_list)

        return batch_list
