from __future__ import annotations

import copy
import logging
import pathlib
from typing import TYPE_CHECKING, Any, ClassVar, List, Set, cast

from great_expectations.experimental.datasources.data_asset.data_connector.filesystem_data_connector import (
    FilesystemDataConnector,
)
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
    from great_expectations.core.batch import BatchDefinition
    from great_expectations.core.batch_spec import BatchMarkers, PathBatchSpec
    from great_expectations.execution_engine import (
        PandasExecutionEngine,
        SparkDFExecutionEngine,
    )
    from great_expectations.experimental.datasources.data_asset.data_connector.data_connector import (
        DataConnector,
    )

logger = logging.getLogger(__name__)


class _FilesystemDataAsset(_FilePathDataAsset):
    _EXCLUDE_FROM_READER_OPTIONS: ClassVar[
        Set[str]
    ] = _FilePathDataAsset._EXCLUDE_FROM_READER_OPTIONS | {
        "base_directory",
        "glob_directive",
    }

    # Filesystem specific attributes
    base_directory: pathlib.Path
    glob_directive: str = "**/*"

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

    def get_batch_list_from_batch_request(
        self, batch_request: BatchRequest
    ) -> List[Batch]:
        self._validate_batch_request(batch_request)

        execution_engine: PandasExecutionEngine | SparkDFExecutionEngine = (
            self.datasource.get_execution_engine()
        )

        # Certain imports are done inline in order to prevent a circular dependency with "core/batch.py".
        data_connector: DataConnector = FilesystemDataConnector(
            name="experimental",
            datasource_name=self.datasource.name,
            data_asset_name=self.name,
            base_directory=self.base_directory,
            regex=self.regex,
            glob_directive=self.glob_directive,
        )

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
            batch_spec = cast(
                PathBatchSpec,
                data_connector.build_batch_spec(batch_definition=batch_definition),
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

            batch_data, batch_markers = execution_engine.get_batch_data_and_markers(
                batch_spec=batch_spec
            )

            batch_metadata = copy.deepcopy(batch_request.options)
            # TODO: <Alex>ALEX-FIX_TO_INSURE_BASE_DIRECTORY_TYPE_IS_PATHLIB.PATH-CONSISTENTLY</Alex>
            # TODO: <Alex>ALEX</Alex>
            # TODO: <Alex>ALEX-FIX_TO_INSURE_PROPERTY_NAME_CORRESPONDDS_TO_ITS_MEANING</Alex>
            batch_metadata["base_directory"] = pathlib.Path(batch_spec["path"])
            # TODO: <Alex>ALEX</Alex>
            # TODO: <Alex>ALEX</Alex>
            batch_metadata.update(batch_definition.batch_identifiers)
            batch_request.options.update(batch_definition.batch_identifiers)
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

        self.sort_batches(batch_list)

        return batch_list
