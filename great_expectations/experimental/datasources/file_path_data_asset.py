from __future__ import annotations

import copy
import dataclasses
import logging
import pathlib
import re
from pprint import pformat as pf
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Dict,
    List,
    Optional,
    Pattern,
    Set,
    Union,
)

import pydantic

import great_expectations.exceptions as gx_exceptions
from great_expectations.experimental.datasources.data_asset.data_connector.regex_parser import (
    RegExParser,
)
from great_expectations.experimental.datasources.interfaces import (
    Batch,
    BatchRequest,
    BatchRequestOptions,
    DataAsset,
    Datasource,
    TestConnectionError,
)

if TYPE_CHECKING:
    from great_expectations.core.batch import BatchDefinition, BatchMarkers
    from great_expectations.core.id_dict import BatchSpec
    from great_expectations.execution_engine import (
        PandasExecutionEngine,
        SparkDFExecutionEngine,
    )
    from great_expectations.experimental.datasources.data_asset.data_connector import (
        DataConnector,
    )

logger = logging.getLogger(__name__)


class _FilePathDataAsset(DataAsset):
    _EXCLUDE_FROM_READER_OPTIONS: ClassVar[Set[str]] = {
        "name",
        "batching_regex",
        "order_by",
        "type",
        "kwargs",  # kwargs need to be unpacked and passed separately
    }

    _ALWAYS_INCLUDE_IN_BATCH_REQUEST_OPTIONS_TEMPLATE: ClassVar[Set[str]] = {
        "path",
    }

    # General file-path DataAsset pertaining attributes.
    batching_regex: Pattern

    _unnamed_regex_param_prefix: str = pydantic.PrivateAttr(
        default="batch_request_param_"
    )
    _regex_parser: RegExParser = pydantic.PrivateAttr()

    _all_group_name_to_group_index_mapping: Dict[str, int] = pydantic.PrivateAttr()
    _all_group_index_to_group_name_mapping: Dict[int, str] = pydantic.PrivateAttr()
    _all_group_names: List[str] = pydantic.PrivateAttr()

    _data_connector: DataConnector | None = pydantic.PrivateAttr()

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
            regex_pattern=self.batching_regex,
            unnamed_regex_group_prefix=self._unnamed_regex_param_prefix,
        )

        self._all_group_name_to_group_index_mapping = (
            self._regex_parser.get_all_group_name_to_group_index_mapping()
        )
        self._all_group_index_to_group_name_mapping = (
            self._regex_parser.get_all_group_index_to_group_name_mapping()
        )
        self._all_group_names = self._regex_parser.get_all_group_names()
        self._data_connector = None

    @pydantic.validator("batching_regex", pre=True)
    def _parse_batching_regex_string(
        cls, batching_regex: Optional[Union[re.Pattern, str]] = None
    ) -> re.Pattern:
        return Datasource.parse_batching_regex_string(batching_regex=batching_regex)

    def batch_request_options_template(
        self,
    ) -> BatchRequestOptions:
        options: set[str] = copy.deepcopy(
            self._ALWAYS_INCLUDE_IN_BATCH_REQUEST_OPTIONS_TEMPLATE
        )
        options.update(set(self._all_group_names))
        return {option: None for option in options}

    def build_batch_request(
        self, options: Optional[BatchRequestOptions] = None
    ) -> BatchRequest:
        if options:
            for option, value in options.items():
                if (
                    option in self._all_group_name_to_group_index_mapping
                    and not isinstance(value, str)
                ):
                    raise gx_exceptions.InvalidBatchRequestError(
                        f"All batching_regex matching options must be strings. The value of '{option}' is "
                        f"not a string: {value}"
                    )

        if options is not None and not self._valid_batch_request_options(options):
            allowed_keys = set(self.batch_request_options_template().keys())
            actual_keys = set(options.keys())
            raise gx_exceptions.InvalidBatchRequestError(
                "Batch request options should only contain keys from the following set:\n"
                f"{allowed_keys}\nbut your specified keys contain\n"
                f"{actual_keys.difference(allowed_keys)}\nwhich is not valid.\n"
            )
        return BatchRequest(
            datasource_name=self.datasource.name,
            data_asset_name=self.name,
            options=options or {},
        )

    def _validate_batch_request(self, batch_request: BatchRequest) -> None:
        """Validates the batch_request has the correct form.

        Args:
            batch_request: A batch request object to be validated.
        """
        if not (
            batch_request.datasource_name == self.datasource.name
            and batch_request.data_asset_name == self.name
            and self._valid_batch_request_options(batch_request.options)
        ):
            expect_batch_request_form = BatchRequest(
                datasource_name=self.datasource.name,
                data_asset_name=self.name,
                options=self.batch_request_options_template(),
            )
            raise gx_exceptions.InvalidBatchRequestError(
                "BatchRequest should have form:\n"
                f"{pf(dataclasses.asdict(expect_batch_request_form))}\n"
                f"but actually has form:\n{pf(dataclasses.asdict(batch_request))}\n"
            )

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
        ] = data_connector.get_batch_definition_list(batch_request=batch_request)

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

    def _get_data_connector(self) -> DataConnector:
        """This private method ensures that exactly one instance of "DataConnector" class is available."""
        data_connector: DataConnector = (
            self._data_connector or self._build_data_connector()
        )
        return data_connector

    def test_connection(self) -> None:
        """Test the connection for the DataAsset.

        Raises:
            TestConnectionError: If the connection test fails.
        """
        data_connector: DataConnector = self._get_data_connector()
        if not data_connector.test_connection():
            raise TestConnectionError(self._build_test_connection_error_message())

    def _build_data_connector(self) -> DataConnector:
        """DataAsset implementations must instantiate appropriate DataConnector class."""
        raise NotImplementedError

    def _build_test_connection_error_message(self) -> str:
        raise NotImplementedError(
            """One needs to explicitly provide "TestConnectionError" instance, containing parametrized error message."""
        )

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
