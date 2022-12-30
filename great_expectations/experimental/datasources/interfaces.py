from __future__ import annotations

import dataclasses
import itertools
import logging
from datetime import datetime
from pprint import pformat as pf
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Generic,
    List,
    Mapping,
    NamedTuple,
    Optional,
    Sequence,
    Set,
    Type,
    TypeVar,
    Union,
    cast,
)

import pydantic
from pydantic import Field
from pydantic import dataclasses as pydantic_dc
from typing_extensions import ClassVar, TypeAlias

from great_expectations.experimental.datasources.experimental_base_model import (
    ExperimentalBaseModel,
)
from great_expectations.experimental.datasources.metadatasource import MetaDatasource
from great_expectations.experimental.datasources.sources import _SourceFactories

LOGGER = logging.getLogger(__name__)

if TYPE_CHECKING:
    from great_expectations.core.batch import (
        BatchDataType,
        BatchDefinition,
        BatchMarkers,
        BatchSpec,
    )
    from great_expectations.execution_engine import ExecutionEngine


# Data Asset Interface
# BatchRequestOptions is a dict that is composed into a BatchRequest that specifies the
# Batches one wants returned. The keys represent dimensions one can slice the data along
# and the values are the realized. If a value is None or unspecified, the batch_request
# will capture all data along this dimension. For example, if we have a year and month
# splitter and we want to query all months in the year 2020, the batch request options
# would look like:
#   options = { "year": 2020 }
class BatchRequestError(Exception):
    pass


BatchRequestOptions: TypeAlias = Dict[str, Any]


@dataclasses.dataclass(frozen=True)
class BatchRequest:
    datasource_name: str
    data_asset_name: str
    options: BatchRequestOptions


DataAssetSelf = TypeVar("DataAssetSelf", bound="DataAsset")


class DataAsset(ExperimentalBaseModel):
    name: str
    type: str
    column_splitter: Optional[ColumnSplitter] = None
    order_by: List[BatchSorter] = Field(default_factory=list)

    # non-field private attrs
    _datasource: Datasource = pydantic.PrivateAttr()

    # Properties
    @property
    def datasource(self) -> Datasource:
        return self._datasource

    # TODO (kilo): remove setter and add custom init for DataAsset to inject datasource in constructor??
    @datasource.setter
    def datasource(self, ds: Datasource):
        assert isinstance(ds, Datasource)
        self._datasource = ds

    def batch_request_options_template(
        self,
    ) -> BatchRequestOptions:
        """A BatchRequestOptions template for get_batch_request.

        Returns:
            A BatchRequestOptions dictionary with the correct shape that get_batch_request
            will understand. All the option values are defaulted to None.
        """
        template: BatchRequestOptions = {}
        if not self.column_splitter:
            return template
        return {p: None for p in self.column_splitter.param_names}

    def get_batch_request(
        self, options: Optional[BatchRequestOptions] = None
    ) -> BatchRequest:
        """A batch request that can be used to obtain batches for this DataAsset.

        Args:
            options: A dict that can be used to limit the number of batches returned from the asset.
                The dict structure depends on the asset type. A template of the dict can be obtained by
                calling batch_request_options_template.

        Returns:
            A BatchRequest object that can be used to obtain a batch list from a Datasource by calling the
            get_batch_list_from_batch_request method.
        """
        if options is not None and not self._valid_batch_request_options(options):
            raise BatchRequestError(
                "Batch request options should have a subset of keys:\n"
                f"{list(self.batch_request_options_template().keys())}\n"
                f"but actually has the form:\n{pf(options)}\n"
            )
        return BatchRequest(
            datasource_name=self.datasource.name,
            data_asset_name=self.name,
            options=options or {},
        )

    def _valid_batch_request_options(self, options: BatchRequestOptions) -> bool:
        return set(options.keys()).issubset(
            set(self.batch_request_options_template().keys())
        )

    def get_batch_list_from_batch_request(
        self, batch_request: BatchRequest
    ) -> List[Batch]:
        """A list of batches that match the BatchRequest.

        Args:
            batch_request: A batch request for this asset. Usually obtained by calling
                get_batch_request on the asset.

        Returns:
            A list of batches that match the options specified in the batch request.
        """
        # We translate the batch_request into a BatchSpec to hook into GX core.
        self._validate_batch_request(batch_request)

        batch_list: List[Batch] = []
        for request in self._fully_specified_batch_requests(batch_request):
            batch_list.append(self._batch_from_fully_specified_batch_request(request))
        self._sort_batches(batch_list)
        return batch_list

    def _batch_from_fully_specified_batch_request(self, request: BatchRequest):
        """Returns a Batch object from a fully specified batch request

        Args:
            request: A fully specified batch request. A fully specified batch request is
              one where all the arguments are explicitly set so corresponds to exactly 1 batch.

        Returns:
            A single Batch object specified by the batch request
        """
        raise NotImplementedError

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
            raise BatchRequestError(
                "BatchRequest should have form:\n"
                f"{pf(dataclasses.asdict(expect_batch_request_form))}\n"
                f"but actually has form:\n{pf(dataclasses.asdict(batch_request))}\n"
            )

    def _fully_specified_batch_requests(self, batch_request) -> List[BatchRequest]:
        """Populates a batch requests unspecified params producing a list of batch requests."""
        if self.column_splitter is None:
            # Currently batch_request.options is complete determined by the presence of a
            # column splitter. If column_splitter is None, then there are no specifiable options
            # so we return early.
            # In the future, if there are options that are not determined by the column splitter
            # this check will have to be generalized.
            return [batch_request]

        # Make a list of the specified and unspecified params in batch_request
        specified_options = []
        unspecified_options = []
        options_template = self.batch_request_options_template()
        for option_name in options_template.keys():
            if (
                option_name in batch_request.options
                and batch_request.options[option_name] is not None
            ):
                specified_options.append(option_name)
            else:
                unspecified_options.append(option_name)

        # Make a list of the all possible batch_request.options by expanding out the unspecified
        # options
        batch_requests: List[BatchRequest] = []

        if not unspecified_options:
            batch_requests.append(batch_request)
        else:
            # All options are defined by the splitter, so we look at its default values to fill
            # in the option values.
            default_option_values = []
            for option in unspecified_options:
                default_option_values.append(
                    self.column_splitter.param_defaults(self)[option]
                )
            for option_values in itertools.product(*default_option_values):
                # Add options from specified options
                options = {
                    name: batch_request.options[name] for name in specified_options
                }
                # Add options from unspecified options
                for i, option_value in enumerate(option_values):
                    options[unspecified_options[i]] = option_value
                batch_requests.append(
                    BatchRequest(
                        datasource_name=batch_request.datasource_name,
                        data_asset_name=batch_request.data_asset_name,
                        options=options,
                    )
                )
        return batch_requests

    def _sort_batches(self, batch_list: List[Batch]) -> None:
        """Sorts batch_list in place.

        Args:
            batch_list: The list of batches to sort in place.
        """
        for sorter in reversed(self.order_by):
            try:
                batch_list.sort(
                    key=lambda b: b.metadata[sorter.metadata_key],
                    reverse=sorter.reverse,
                )
            except KeyError as e:
                raise KeyError(
                    f"Trying to sort {self.name} table asset batches on key {sorter.metadata_key} "
                    "which isn't available on all batches."
                ) from e

    def add_sorters(
        self: DataAssetSelf, sorters: BatchSortersDefinition
    ) -> DataAssetSelf:
        # NOTE: (kilo59) we could use pydantic `validate_assignment` for this
        # https://docs.pydantic.dev/usage/model_config/#options
        self.order_by = _batch_sorter_from_list(sorters)
        return self

    # Private helper methods
    @pydantic.validator("order_by", pre=True, each_item=True)
    @classmethod
    def _parse_order_by_sorter(
        cls, v: Union[str, BatchSorter]
    ) -> Union[BatchSorter, dict]:
        if isinstance(v, str):
            if not v:
                raise ValueError("empty string")
            return _batch_sorter_from_str(v)
        return v


DataAssetType = TypeVar("DataAssetType", bound=DataAsset)


# Datasource Interface
class Datasource(
    ExperimentalBaseModel, Generic[DataAssetType], metaclass=MetaDatasource
):
    # class attrs
    asset_types: ClassVar[List[Type[DataAsset]]] = []
    # Datasource instance attrs but these will be fed into the `execution_engine` constructor
    _excluded_eng_args: ClassVar[Set[str]] = {
        "name",
        "type",
        "execution_engine",
        "assets",
    }
    # Setting this in a Datasource subclass will override the execution engine type.
    # The primary use case is to inject an execution engine for testing.
    execution_engine_override: ClassVar[Optional[Type[ExecutionEngine]]] = None

    # instance attrs
    type: str
    name: str
    assets: Mapping[str, DataAssetType] = {}
    _execution_engine: ExecutionEngine = pydantic.PrivateAttr()

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        engine_kwargs = {
            k: v for (k, v) in kwargs.items() if k not in self._excluded_eng_args
        }
        self._execution_engine = self._execution_engine_type()(**engine_kwargs)

    @property
    def execution_engine(self) -> ExecutionEngine:
        return self._execution_engine

    class Config:
        # TODO: revisit this (1 option - define __get_validator__ on ExecutionEngine)
        # https://pydantic-docs.helpmanual.io/usage/types/#custom-data-types
        arbitrary_types_allowed = True

    @pydantic.validator("assets", pre=True)
    @classmethod
    def _load_asset_subtype(cls, v: Dict[str, dict]):
        LOGGER.info(f"Loading 'assets' ->\n{pf(v, depth=3)}")
        loaded_assets: Dict[str, DataAssetType] = {}

        # TODO (kilo59): catch key errors
        for asset_name, config in v.items():
            asset_type_name: str = config["type"]
            asset_type: Type[DataAssetType] = _SourceFactories.type_lookup[
                asset_type_name
            ]
            LOGGER.debug(f"Instantiating '{asset_type_name}' as {asset_type}")
            loaded_assets[asset_name] = asset_type(**config)

        LOGGER.debug(f"Loaded 'assets' ->\n{repr(loaded_assets)}")
        return loaded_assets

    def _execution_engine_type(self) -> Type[ExecutionEngine]:
        """Returns the execution engine to be used"""
        return self.execution_engine_override or self.execution_engine_type()

    def execution_engine_type(self) -> Type[ExecutionEngine]:
        """Return the ExecutionEngine type use for this Datasource"""
        raise NotImplementedError(
            "One needs to implement 'execution_engine_type' on a Datasource subclass"
        )

    def get_batch_list_from_batch_request(
        self, batch_request: BatchRequest
    ) -> List[Batch]:
        """A list of batches that match the BatchRequest.

        Args:
            batch_request: A batch request for this asset. Usually obtained by calling
                get_batch_request on the asset.

        Returns:
            A list of batches that match the options specified in the batch request.
        """
        # We translate the batch_request into a BatchSpec to hook into GX core.
        data_asset = self.get_asset(batch_request.data_asset_name)
        return data_asset.get_batch_list_from_batch_request(batch_request)

    def get_asset(self, asset_name: str) -> DataAssetType:
        """Returns the DataAsset referred to by name"""
        # This default implementation will be used if protocol is inherited
        try:
            return self.assets[asset_name]
        except KeyError as exc:
            raise LookupError(
                f"'{asset_name}' not found. Available assets are {list(self.assets.keys())}"
            ) from exc


class Batch:
    # Instance variable declarations
    _datasource: Datasource
    _data_asset: DataAsset
    _batch_request: BatchRequest
    _data: BatchDataType
    _id: str
    # metadata is any arbitrary data one wants to associate with a batch. GX will add arbitrary metadata
    # to a batch so developers may want to namespace any custom metadata they add.
    metadata: Dict[str, Any]

    # TODO: These legacy fields are currently required. They are only used in usage stats so we
    #       should figure out a better way to anonymize and delete them.
    _legacy_batch_markers: BatchMarkers
    _legacy_batch_spec: BatchSpec
    _legacy_batch_definition: BatchDefinition

    def __init__(
        self,
        datasource: Datasource,
        data_asset: DataAsset,
        batch_request: BatchRequest,
        # BatchDataType is Union[core.batch.BatchData, pd.DataFrame, SparkDataFrame].  core.batch.Batchdata is the
        # implicit interface that Datasource implementers can use. We can make this explicit if needed.
        data: BatchDataType,
        # Legacy values that should be removed in the future.
        legacy_batch_markers: BatchMarkers,
        legacy_batch_spec: BatchSpec,
        legacy_batch_definition: BatchDefinition,
        # Optional arguments
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """This represents a batch of data.

        This is usually not the data itself but a hook to the data on an external datastore such as
        a spark or a sql database. An exception exists for pandas or any in-memory datastore.
        """
        # These properties are intended to be READ-ONLY
        self._datasource: Datasource = datasource
        self._data_asset: DataAsset = data_asset
        self._batch_request: BatchRequest = batch_request
        self._data: BatchDataType = data
        self.metadata = metadata or {}

        self._legacy_batch_markers = legacy_batch_markers
        self._legacy_batch_spec = legacy_batch_spec
        self._legacy_batch_definition = legacy_batch_definition

        # computed property
        # We need to unique identifier. This will likely change as I get more input
        options_list = []
        for k, v in batch_request.options.items():
            options_list.append(f"{k}_{v}")

        self._id: str = "-".join([datasource.name, data_asset.name, *options_list])

    @property
    def datasource(self) -> Datasource:
        return self._datasource

    @property
    def data_asset(self) -> DataAsset:
        return self._data_asset

    @property
    def batch_request(self) -> BatchRequest:
        return self._batch_request

    @property
    def id(self) -> str:
        return self._id

    @property
    def data(self) -> BatchDataType:
        return self._data

    @property
    def execution_engine(self) -> ExecutionEngine:
        return self.datasource.execution_engine

    @property
    def batch_markers(self) -> BatchMarkers:
        return self._legacy_batch_markers

    @property
    def batch_spec(self) -> BatchSpec:
        return self._legacy_batch_spec

    @property
    def batch_definition(self) -> BatchDefinition:
        return self._legacy_batch_definition


# Splitter Interface
@pydantic_dc.dataclass(frozen=True)
class ColumnSplitter(Generic[DataAssetType]):
    column_name: str
    method_name: str
    param_names: Sequence[str]

    def param_defaults(self, table_asset: DataAssetType) -> Dict[str, List]:
        raise NotImplementedError

    @pydantic.validator("method_name")
    def _splitter_method_exists(cls, v: str):
        """Fail early if the `method_name` does not exist and would fail at runtime."""
        # NOTE (kilo59): this could be achieved by simply annotating the method_name field
        # as a `SplitterMethod` enum but we get cyclic imports.
        # This also adds the enums to the generated json schema.
        # https://docs.pydantic.dev/usage/types/#enums-and-choices
        # We could use `update_forward_refs()` but would have to change this to a BaseModel
        # https://docs.pydantic.dev/usage/postponed_annotations/
        from great_expectations.execution_engine.split_and_sample.data_splitter import (
            SplitterMethod,
        )

        method_members = set(SplitterMethod)
        if v not in method_members:
            permitted_values_str = "', '".join([m.value for m in method_members])
            raise ValueError(f"unexpected value; permitted: '{permitted_values_str}'")
        return v


# Type supporting datetime splitters
class DatetimeRange(NamedTuple):
    min: datetime
    max: datetime


# Sorter Interface
@pydantic_dc.dataclass(frozen=True)
class BatchSorter:
    metadata_key: str
    reverse: bool = False


BatchSortersDefinition: TypeAlias = Union[List[BatchSorter], List[str]]


def _batch_sorter_from_list(sorters: BatchSortersDefinition) -> List[BatchSorter]:
    if len(sorters) == 0 or isinstance(sorters[0], BatchSorter):
        # mypy gets confused here. Since BatchSortersDefinition has all elements of the
        # same type in the list so if the first on is BatchSorter so are the others.
        return cast(List[BatchSorter], sorters)
    # Likewise, sorters must be List[str] here.
    return [_batch_sorter_from_str(sorter) for sorter in cast(List[str], sorters)]


def _batch_sorter_from_str(sort_key: str) -> BatchSorter:
    """Convert a list of strings to BatchSorters

    Args:
        sort_key: A batch metadata key which will be used to sort batches on a data asset.
                  This can be prefixed with a + or - to indicate increasing or decreasing
                  sorting. If not specified, defaults to increasing order.
    """
    if sort_key[0] == "-":
        return BatchSorter(metadata_key=sort_key[1:], reverse=True)
    elif sort_key[0] == "+":
        return BatchSorter(metadata_key=sort_key[1:], reverse=False)
    else:
        return BatchSorter(metadata_key=sort_key, reverse=False)
