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
)

import pydantic

import great_expectations.exceptions as ge_exceptions
from great_expectations.experimental.datasources.data_asset.data_connector.regex_parser import (
    RegExParser,
)
from great_expectations.experimental.datasources.interfaces import (
    Batch,
    BatchRequest,
    BatchRequestOptions,
    DataAsset,
)

if TYPE_CHECKING:
    from great_expectations.core.batch import BatchDefinition, BatchMarkers
    from great_expectations.core.id_dict import BatchSpec
    from great_expectations.execution_engine import (
        PandasExecutionEngine,
        SparkDFExecutionEngine,
    )
    from great_expectations.experimental.datasources.data_asset.data_connector.data_connector import (
        DataConnector,
    )

logger = logging.getLogger(__name__)


class _FilePathDataAsset(DataAsset):
    _EXCLUDE_FROM_READER_OPTIONS: ClassVar[Set[str]] = {
        "name",
        "regex",
        "order_by",
        "type",
    }

    # General file-path DataAsset pertaining attributes.
    regex: Pattern

    # Internal attributes
    _unnamed_regex_param_prefix: str = pydantic.PrivateAttr(
        default="batch_request_param_"
    )
    _regex_parser: RegExParser = pydantic.PrivateAttr()

    _all_group_name_to_group_index_mapping: Dict[str, int] = pydantic.PrivateAttr()
    _all_group_index_to_group_name_mapping: Dict[int, str] = pydantic.PrivateAttr()
    _all_group_names: List[str] = pydantic.PrivateAttr()

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
        self._validate_batch_request(batch_request)

        execution_engine: PandasExecutionEngine | SparkDFExecutionEngine = (
            self.datasource.get_execution_engine()
        )

        data_connector: DataConnector = self._get_data_connector()

        batch_definition_list: List[
            BatchDefinition
        ] = data_connector.get_batch_definition_list_from_batch_request(
            batch_request=batch_request
        )

        batch_list: List[Batch] = []

        batch_spec: BatchSpec
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

        # TODO: <Alex>ALEX_INCLUDE_SORTERS_FUNCTIONALITY_UNDER_PYDANTIC-MAKE_SURE_SORTER_CONFIGURATIONS_ARE_VALIDATED</Alex>
        # TODO: <Alex>ALEX-MOVE_SORTING_INTO_FILE_PATH_DATA_CONNECTOR_ON_BATCH_DEFINITION_OBJECTS</Alex>
        self.sort_batches(batch_list)

        return batch_list

    def test_connection(self) -> None:
        """Test the connection for the DataAsset.

        Raises:
            TestConnectionError: If the connection test fails.
        """
        raise NotImplementedError(
            """One needs to implement "test_connection" on a _FilePathDataAsset subclass."""
        )

    def _get_data_connector(self) -> DataConnector:
        raise NotImplementedError

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
