from __future__ import annotations

import copy
import logging
from collections import Counter
from pprint import pformat as pf
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Dict,
    List,
    Mapping,
    Optional,
    Pattern,
    Set,
    cast,
)

import pydantic

import great_expectations.exceptions as gx_exceptions
from great_expectations.core._docs_decorators import public_api
from great_expectations.core.id_dict import BatchSpec
from great_expectations.datasource.fluent.batch_request import (
    BatchRequest,
    BatchRequestOptions,
)
from great_expectations.datasource.fluent.constants import MATCH_ALL_PATTERN
from great_expectations.datasource.fluent.data_asset.data_connector import (
    FILE_PATH_BATCH_SPEC_KEY,
)
from great_expectations.datasource.fluent.data_asset.data_connector.file_path_data_connector import (
    get_batch_definition_for_splitter,
)
from great_expectations.datasource.fluent.data_asset.data_connector.regex_parser import (
    RegExParser,
)
from great_expectations.datasource.fluent.interfaces import (
    Batch,
    DataAsset,
    TestConnectionError,
)
from great_expectations.datasource.fluent.spark_generic_splitters import (
    Splitter,
    SplitterColumnValue,
    SplitterDatetimePart,
    SplitterYear,
    SplitterYearAndMonth,
)

if TYPE_CHECKING:
    from typing_extensions import Self

    from great_expectations.core.batch import BatchDefinition, BatchMarkers
    from great_expectations.datasource.fluent.data_asset.data_connector import (
        DataConnector,
    )
    from great_expectations.datasource.fluent.interfaces import (
        BatchMetadata,
        BatchSlice,
    )
    from great_expectations.execution_engine import (
        PandasExecutionEngine,
        SparkDFExecutionEngine,
    )

logger = logging.getLogger(__name__)


class _FilePathDataAsset(DataAsset):
    _EXCLUDE_FROM_READER_OPTIONS: ClassVar[Set[str]] = {
        "type",
        "name",
        "order_by",
        "batch_metadata",
        "batching_regex",  # file_path argument
        "kwargs",  # kwargs need to be unpacked and passed separately
        "batch_metadata",
        "connect_options",
    }

    # General file-path DataAsset pertaining attributes.
    batching_regex: Pattern = (  # must use typing.Pattern for pydantic < v1.10
        MATCH_ALL_PATTERN
    )
    connect_options: Mapping = pydantic.Field(
        default_factory=dict,
        description="Optional filesystem specific advanced parameters for connecting to data assets",
    )
    splitter: Optional[Splitter] = None

    _unnamed_regex_param_prefix: str = pydantic.PrivateAttr(
        default="batch_request_param_"
    )
    _regex_parser: RegExParser = pydantic.PrivateAttr()

    _all_group_name_to_group_index_mapping: Dict[str, int] = pydantic.PrivateAttr()
    _all_group_index_to_group_name_mapping: Dict[int, str] = pydantic.PrivateAttr()
    _all_group_names: List[str] = pydantic.PrivateAttr()

    # `_data_connector`` should be set inside `_build_data_connector()`
    _data_connector: DataConnector = pydantic.PrivateAttr()
    # more specific `_test_connection_error_message` can be set inside `_build_data_connector()`
    _test_connection_error_message: str = pydantic.PrivateAttr(
        "Could not connect to your asset"
    )

    class Config:
        """
        Need to allow extra fields for the base type because pydantic will first create
        an instance of `_FilePathDataAsset` before we select and create the more specific
        asset subtype.
        Each specific subtype should `forbid` extra fields.
        """

        extra = pydantic.Extra.allow

    # example = """
    #     logs/
    #     logs / 2023 - 05 - 03 /
    #     logs/2023-05-04/
    #     logs/2023-05-04/parquet.1
    #     logs/2023-05-04/parquet.2
    #     logs/2023-05-04/parquet.3
    # """
    #
    # regex = "logs/{year}-{month}-{day}/"

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

    @property
    def batch_request_options(
        self,
    ) -> tuple[str, ...]:
        """The potential keys for BatchRequestOptions.

        Example:
        ```python
        >>> print(asset.batch_request_options)
        ("day", "month", "year", "path")
        >>> options = {"year": "2023"}
        >>> batch_request = asset.build_batch_request(options=options)
        ```

        Returns:
            A tuple of keys that can be used in a BatchRequestOptions dictionary.
        """
        splitter_options: tuple[str, ...] = tuple()
        if self.splitter:
            splitter_options = tuple(self.splitter.param_names)
        return (
            tuple(self._all_group_names)
            + (FILE_PATH_BATCH_SPEC_KEY,)
            + splitter_options
        )

    @public_api
    def build_batch_request(
        self,
        options: Optional[BatchRequestOptions] = None,
        batch_slice: Optional[BatchSlice] = None,
    ) -> BatchRequest:
        """A batch request that can be used to obtain batches for this DataAsset.

        Args:
            options: A dict that can be used to filter the batch groups returned from the asset.
                The dict structure depends on the asset type. The available keys for dict can be obtained by
                calling batch_request_options.
            batch_slice: A python slice that can be used to limit the sorted batches by index.
                e.g. `batch_slice = "[-5:]"` will request only the last 5 batches after the options filter is applied.

        Returns:
            A BatchRequest object that can be used to obtain a batch list from a Datasource by calling the
            get_batch_list_from_batch_request method.
        """
        if options:
            for option, value in options.items():
                if (
                    option in self._all_group_name_to_group_index_mapping
                    and value
                    and not isinstance(value, str)
                ):
                    raise gx_exceptions.InvalidBatchRequestError(
                        f"All batching_regex matching options must be strings. The value of '{option}' is "
                        f"not a string: {value}"
                    )

        if options is not None and not self._valid_batch_request_options(options):
            allowed_keys = set(self.batch_request_options)
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
            batch_slice=batch_slice,
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
            options = {option: None for option in self.batch_request_options}
            expect_batch_request_form = BatchRequest(
                datasource_name=self.datasource.name,
                data_asset_name=self.name,
                options=options,
                batch_slice=batch_request._batch_slice_input,  # type: ignore[attr-defined]
            )
            raise gx_exceptions.InvalidBatchRequestError(
                "BatchRequest should have form:\n"
                f"{pf(expect_batch_request_form.dict())}\n"
                f"but actually has form:\n{pf(batch_request.dict())}\n"
            )

    # TODO: This method should be different between file and directory data asset types
    #  use inheritance instead
    def _get_path_for_batch_definition(self):
        if hasattr(self, "data_directory"):
            path = self.data_directory
        else:
            raise Exception("Not sure what to pass here yet")
        return path

    def get_batch_list_from_batch_request(
        self, batch_request: BatchRequest
    ) -> List[Batch]:
        self._validate_batch_request(batch_request)

        execution_engine: PandasExecutionEngine | SparkDFExecutionEngine = (
            self.datasource.get_execution_engine()
        )

        if self.splitter:
            if hasattr(self, "data_directory"):
                batch_definition_list: List[
                    BatchDefinition
                ] = get_batch_definition_for_splitter(
                    data_connector=self._data_connector,
                    batch_request=batch_request,
                    path=self._get_path_for_batch_definition(),
                )
            else:
                # breakpoint()
                # no path in batch_request
                # batch_request = BatchRequest(datasource_name='spark_filesystem_datasource', data_asset_name='file_csv_asset', options={'year': '2020', 'month': '10', 'passenger_count': 2})
                # TODO: ****************************************************************************************
                # TODO: Remove the splitter kwargs from the batch_request and add them back later
                # self.splitter.batch_request_options_to_batch_spec_kwarg_identifiers(batch_request.options)
                # splitter_kwargs = {}
                # for param_name in self.splitter.param_names:
                #     splitter_kwargs[param_name] = batch_request.options.pop(param_name)
                batch_request_options_counts = Counter(self.batch_request_options)
                batch_request_copy_without_splitter_kwargs = copy.deepcopy(
                    batch_request
                )
                for param_name in self.splitter.param_names:
                    # TODO: Maybe don't pop if the splitter kwargs match the asset.batch_request_options (e.g. if they are in there twice)?
                    if batch_request_options_counts[param_name] == 1:
                        batch_request_copy_without_splitter_kwargs.options.pop(
                            param_name
                        )
                    else:
                        # TODO: Better warning here, or can we pass splitter info differently
                        print(
                            "Warning, you are using the same splitter kwarg name as a regex name"
                        )
                # batch_request_copy_without_splitter_kwargs = BatchRequest(datasource_name='spark_filesystem_datasource', data_asset_name='file_csv_asset', options={'year': '2020', 'month': '10'})

                # splitter_kwargs = {param_name: batch_request.options.pop(param_name) for param_name in self.splitter.param_names}

                # TODO: ****************************************************************************************
                batch_definition_list: List[
                    BatchDefinition
                ] = self._data_connector.get_batch_definition_list(
                    batch_request=batch_request_copy_without_splitter_kwargs
                )
                # breakpoint()
            # TODO: Is the difference here that we need to stick the path into the batch_definition?
            # breakpoint()
            # Without path:
            # batch_definition_list = [{'datasource_name': 'spark_filesystem_datasource', 'data_connector_name': 'fluent', 'data_asset_name': 'directory_csv_asset', 'batch_identifiers': {'passenger_count': 2}}]
            # With path:
            # batch_definition_list = [{'datasource_name': 'spark_filesystem_datasource', 'data_connector_name': 'fluent', 'data_asset_name': 'directory_csv_asset', 'batch_identifiers': {'passenger_count': 2, 'path': PosixPath('samples_2020')}}]

            # TODO: BatchRequest here is:
            # BatchRequest(datasource_name='spark_filesystem_datasource', data_asset_name='directory_csv_asset', options={'passenger_count': 2})
            # breakpoint()
            # batch_definition_list: List[
            #     BatchDefinition
            # ] = self._data_connector.get_batch_definition_list(
            #     batch_request=batch_request
            # )
            # Here: batch_definition_list = []
            # breakpoint()
        else:
            # breakpoint()
            # TODO: BatchRequest here is:
            # BatchRequest(datasource_name='spark_filesystem_datasource', data_asset_name='directory_csv_asset', options={})
            # with file based it is:
            # batch_request = BatchRequest(datasource_name='spark_filesystem_datasource', data_asset_name='file_csv_asset', options={'year': '2020', 'month': '10'})
            batch_definition_list: List[
                BatchDefinition
            ] = self._data_connector.get_batch_definition_list(
                batch_request=batch_request
            )
            # breakpoint()
            # Leads to path in batch_identifiers
            # batch_definition_list=[{'datasource_name': 'spark_filesystem_datasource', 'data_connector_name': 'fluent', 'data_asset_name': 'directory_csv_asset', 'batch_identifiers': {'path': PosixPath('samples_2020')}}]
        batch_list: List[Batch] = []

        batch_spec: BatchSpec
        batch_data: Any
        batch_markers: BatchMarkers
        batch_metadata: BatchMetadata
        batch: Batch
        if self.splitter:
            # breakpoint()
            pass
        for batch_definition in batch_definition_list:
            batch_spec = self._data_connector.build_batch_spec(
                batch_definition=batch_definition
            )
            batch_spec_options = self._batch_spec_options_from_batch_request(batch_request)
            batch_spec.update(batch_spec_options)

            batch_data, batch_markers = execution_engine.get_batch_data_and_markers(
                batch_spec=batch_spec
            )

            fully_specified_batch_request = copy.deepcopy(batch_request)
            fully_specified_batch_request.options.update(
                batch_definition.batch_identifiers
            )
            batch_metadata = self._get_batch_metadata_from_batch_request(
                batch_request=fully_specified_batch_request
            )

            # Some pydantic annotations are postponed due to circular imports.
            # Batch.update_forward_refs() will set the annotations before we
            # instantiate the Batch class since we can import them in this scope.
            Batch.update_forward_refs()

            batch = Batch(
                datasource=self.datasource,
                data_asset=self,
                batch_request=fully_specified_batch_request,
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

    def _batch_spec_options_from_batch_request(self, batch_request: BatchRequest) -> dict:
        batch_spec_options = {
            "reader_method": self._get_reader_method(),
            "reader_options": self.dict(
                include=self._get_reader_options_include(),
                exclude=self._EXCLUDE_FROM_READER_OPTIONS,
                exclude_unset=True,
                by_alias=True,
                config_provider=self._datasource._config_provider,
            ),
        }
        if self.splitter:
            batch_spec_options["splitter_method"] = self.splitter.method_name
            batch_spec_options[
                "splitter_kwargs"
            ] = self.splitter.splitter_method_kwargs()
            batch_spec_options["splitter_kwargs"]["batch_identifiers"] = {}
            # mypy infers that batch_spec_kwargs["batch_identifiers"] is a collection, but
            # it is hardcoded to a dict above, so we cast it here.
            cast(
                Dict, batch_spec_options["splitter_kwargs"]["batch_identifiers"]
            ).update(
                self.splitter.batch_request_options_to_batch_spec_kwarg_identifiers(
                    batch_request.options
                )
            )
        return batch_spec_options

    def test_connection(self) -> None:
        """Test the connection for the DataAsset.

        Raises:
            TestConnectionError: If the connection test fails.
        """
        try:
            if self._data_connector.test_connection():
                return None
        except Exception as e:
            raise TestConnectionError(
                f"Could not connect to asset using {type(self._data_connector).__name__}: Got {type(e).__name__}"
            ) from e
        raise TestConnectionError(self._test_connection_error_message)

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

    def test_splitter_connection(self) -> None:
        # TODO: Implementation
        pass

    # TODO: Add more add_splitter_* methods here

    def _add_splitter(self: Self, splitter: Splitter) -> Self:
        self.splitter = splitter
        self.test_splitter_connection()
        return self

    def add_splitter_year(
        self: Self,
        column_name: str,
    ) -> Self:
        """Associates a year splitter with this data asset.
        Args:
            column_name: A column name of the date column where year will be parsed out.
        Returns:
            This asset so we can use this method fluently.
        """
        return self._add_splitter(
            SplitterYear(method_name="split_on_year", column_name=column_name)
        )

    def add_splitter_year_and_month(
        self: Self,
        column_name: str,
    ) -> Self:
        """Associates a year, month splitter with this asset.
        Args:
            column_name: A column name of the date column where year and month will be parsed out.
        Returns:
            This asset so we can use this method fluently.
        """
        return self._add_splitter(
            SplitterYearAndMonth(
                method_name="split_on_year_and_month", column_name=column_name
            )
        )

    def add_splitter_datetime_part(
        self: Self, column_name: str, datetime_parts: List[str]
    ) -> Self:
        """Associates a datetime part splitter with this sql asset.
        Args:
            column_name: Name of the date column where parts will be parsed out.
            datetime_parts: A list of datetime parts to split on, specified as DatePart objects or as their string equivalent e.g. "year", "month", "week", "day", "hour", "minute", or "second"
        Returns:
            This sql asset so we can use this method fluently.
        """
        return self._add_splitter(
            SplitterDatetimePart(
                method_name="split_on_date_parts",
                column_name=column_name,
                datetime_parts=datetime_parts,
            )
        )

    # TODO: Add other splitters
    def add_splitter_column_value(self: Self, column_name: str) -> Self:
        """Associates a column value splitter with this sql asset.
        Args:
            column_name: A column name of the column to split on.
        Returns:
            This sql asset so we can use this method fluently.
        """
        return self._add_splitter(
            SplitterColumnValue(
                method_name="split_on_column_value",
                column_name=column_name,
            )
        )
