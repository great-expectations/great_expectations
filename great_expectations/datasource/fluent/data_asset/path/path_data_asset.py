from __future__ import annotations

import copy
import logging
from pprint import pformat as pf
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Generic,
    List,
    Mapping,
    Optional,
    Set,
)

import great_expectations.exceptions as gx_exceptions
from great_expectations.compatibility import pydantic
from great_expectations.compatibility.typing_extensions import override
from great_expectations.core.batch_definition import PartitionerT
from great_expectations.datasource.fluent.batch_request import (
    BatchRequest,
)
from great_expectations.datasource.fluent.data_connector import (
    FilePathDataConnector,  # noqa: TCH001  # pydantic uses type at runtime
)
from great_expectations.datasource.fluent.interfaces import (
    Batch,
    DataAsset,
    DatasourceT,
    TestConnectionError,
)

if TYPE_CHECKING:
    from great_expectations.alias_types import PathStr
    from great_expectations.core.batch import BatchMarkers, LegacyBatchDefinition
    from great_expectations.core.id_dict import BatchSpec
    from great_expectations.datasource.fluent.interfaces import (
        BatchMetadata,
    )
    from great_expectations.execution_engine import (
        PandasExecutionEngine,
        SparkDFExecutionEngine,
    )

logger = logging.getLogger(__name__)


class PathDataAsset(DataAsset, Generic[DatasourceT, PartitionerT]):
    _EXCLUDE_FROM_READER_OPTIONS: ClassVar[Set[str]] = {
        "batch_definitions",
        "type",
        "name",
        "order_by",
        "batch_metadata",
        "batching_regex",  # file_path argument
        "kwargs",  # kwargs need to be unpacked and passed separately
        "batch_metadata",  # noqa: PLW0130
        "connect_options",
        "id",
    }

    # General file-path DataAsset pertaining attributes.

    connect_options: Mapping = pydantic.Field(
        default_factory=dict,
        description="Optional filesystem specific advanced parameters for connecting to data assets",  # noqa: E501
    )

    # `_data_connector`` should be set inside `_build_data_connector()`
    _data_connector: FilePathDataConnector = pydantic.PrivateAttr()
    # more specific `_test_connection_error_message` can be set inside `_build_data_connector()`
    _test_connection_error_message: str = pydantic.PrivateAttr("Could not connect to your asset")

    class Config:
        """
        Need to allow extra fields for the base type because pydantic will first create
        an instance of `PathDataAsset` before we select and create the more specific
        asset subtype.
        Each specific subtype should `forbid` extra fields.
        """

        extra = pydantic.Extra.allow

    @override
    def get_batch_parameters_keys(
        self,
        partitioner: Optional[PartitionerT] = None,
    ) -> tuple[str, ...]:
        raise NotImplementedError

    @override
    def _validate_batch_request(self, batch_request: BatchRequest) -> None:
        """Validates the batch_request has the correct form.

        Args:
            batch_request: A batch request object to be validated.
        """
        # need implementation for both dir & file
        if not (
            batch_request.datasource_name == self.datasource.name
            and batch_request.data_asset_name == self.name
            and self._batch_parameters_are_valid(
                options=batch_request.options, partitioner=batch_request.partitioner
            )
        ):
            valid_options = self.get_batch_parameters_keys(partitioner=batch_request.partitioner)
            options = {option: None for option in valid_options}
            expect_batch_request_form = BatchRequest(
                datasource_name=self.datasource.name,
                data_asset_name=self.name,
                options=options,
                batch_slice=batch_request._batch_slice_input,  # type: ignore[attr-defined]
                partitioner=batch_request.partitioner,
            )
            raise gx_exceptions.InvalidBatchRequestError(  # noqa: TRY003
                "BatchRequest should have form:\n"
                f"{pf(expect_batch_request_form.dict())}\n"
                f"but actually has form:\n{pf(batch_request.dict())}\n"
            )

    @override
    def get_batch_list_from_batch_request(self, batch_request: BatchRequest) -> List[Batch]:
        self._validate_batch_request(batch_request)

        execution_engine: PandasExecutionEngine | SparkDFExecutionEngine = (
            self.datasource.get_execution_engine()
        )

        batch_definition_list = self._get_batch_definition_list(batch_request)

        batch_list: List[Batch] = []

        batch_spec: BatchSpec
        batch_data: Any
        batch_markers: BatchMarkers
        batch_metadata: BatchMetadata
        batch: Batch
        for batch_definition in batch_definition_list:
            batch_spec = self._data_connector.build_batch_spec(batch_definition=batch_definition)
            batch_spec_options = self._batch_spec_options_from_batch_request(batch_request)
            batch_spec.update(batch_spec_options)

            batch_data, batch_markers = execution_engine.get_batch_data_and_markers(
                batch_spec=batch_spec
            )

            fully_specified_batch_request = copy.deepcopy(batch_request)
            fully_specified_batch_request.options.update(batch_definition.batch_identifiers)
            batch_metadata = self._get_batch_metadata_from_batch_request(
                batch_request=fully_specified_batch_request
            )

            batch = Batch(
                datasource=self.datasource,
                data_asset=self,
                batch_request=fully_specified_batch_request,
                data=batch_data,
                metadata=batch_metadata,
                batch_markers=batch_markers,
                batch_spec=batch_spec,
                batch_definition=batch_definition,
            )
            batch_list.append(batch)

        if batch_request.partitioner:
            self.sort_batches(batch_list, batch_request.partitioner)

        return batch_list

    def _get_batch_definition_list(
        self, batch_request: BatchRequest
    ) -> list[LegacyBatchDefinition]:
        """Generate a batch definition list from a given batch request.

        Args:
            batch_request: Batch request used to generate batch definitions.

        Returns:
            List of batch definitions.
        """
        raise NotImplementedError

    def _batch_spec_options_from_batch_request(self, batch_request: BatchRequest) -> dict:
        """Build a set of options for use in a batch spec from a batch request.

        Args:
            batch_request: Batch request to use to generate options.

        Returns:
            Dictionary containing batch spec options.
        """
        get_reader_options_include: set[str] | None = self._get_reader_options_include()
        if not get_reader_options_include:
            # Set to None if empty set to include any additional `extra_kwargs` passed to `add_*_asset`  # noqa: E501
            get_reader_options_include = None
        batch_spec_options = {
            "reader_method": self._get_reader_method(),
            "reader_options": self.dict(
                include=get_reader_options_include,
                exclude=self._EXCLUDE_FROM_READER_OPTIONS,
                exclude_unset=True,
                by_alias=True,
                config_provider=self._datasource._config_provider,
            ),
        }

        return batch_spec_options

    @override
    def test_connection(self) -> None:
        """Test the connection for the DataAsset.

        Raises:
            TestConnectionError: If the connection test fails.
        """
        try:
            if self._data_connector.test_connection():
                return None
        except Exception as e:
            raise TestConnectionError(  # noqa: TRY003
                f"Could not connect to asset using {type(self._data_connector).__name__}: Got {type(e).__name__}"  # noqa: E501
            ) from e
        raise TestConnectionError(self._test_connection_error_message)

    def get_whole_directory_path_override(
        self,
    ) -> PathStr | None:
        """If present, override DataConnector behavior in order to
        treat an entire directory as a single Asset.
        """
        # todo: refactor data connector instantiation so this isn't necessary
        raise NotImplementedError

    def _get_reader_method(self) -> str:
        # subtypes must define a reader method
        raise NotImplementedError

    def _get_reader_options_include(self) -> set[str]:
        # subtypes control how reader options get serialized
        raise NotImplementedError
