from __future__ import annotations

import dataclasses
import logging
from pprint import pformat as pf
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Dict,
    Generic,
    List,
    MutableMapping,
    Optional,
    Set,
    Type,
    TypeVar,
    Union,
)

import pandas as pd
import pydantic
from pydantic import Field, StrictBool, StrictInt, root_validator, validate_arguments
from pydantic import dataclasses as pydantic_dc
from typing_extensions import TypeAlias, TypeGuard

from great_expectations.core.id_dict import BatchSpec  # noqa: TCH001
from great_expectations.experimental.datasources.constants import _FIELDS_ALWAYS_SET
from great_expectations.experimental.datasources.experimental_base_model import (
    ExperimentalBaseModel,
)
from great_expectations.experimental.datasources.metadatasource import MetaDatasource
from great_expectations.experimental.datasources.sources import _SourceFactories
from great_expectations.validator.metrics_calculator import MetricsCalculator

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    # TODO: When the Batch class is moved out of the experimental directory, we should try to import the annotations
    #       from core.batch so we no longer need to call Batch.update_forward_refs() before instantiation.
    from great_expectations.core.batch import (
        BatchData,
        BatchDefinition,
        BatchMarkers,
    )

try:
    import pyspark
    from pyspark.sql import Row as pyspark_sql_Row
except ImportError:
    pyspark = None
    pyspark_sql_Row = None
    logger.debug("No spark sql dataframe module available.")


class TestConnectionError(Exception):
    pass


# BatchRequestOptions is a dict that is composed into a BatchRequest that specifies the
# Batches one wants as returned. The keys represent dimensions one can slice the data along
# and the values are the realized. If a value is None or unspecified, the batch_request
# will capture all data along this dimension. For example, if we have a year and month
# splitter, and we want to query all months in the year 2020, the batch request options
# would look like:
#   options = { "year": 2020 }
BatchRequestOptions: TypeAlias = Dict[str, Any]


@dataclasses.dataclass(frozen=True)
class BatchRequest:
    datasource_name: str
    data_asset_name: str
    options: BatchRequestOptions


@pydantic_dc.dataclass(frozen=True)
class BatchSorter:
    key: str
    reverse: bool = False


BatchSortersDefinition: TypeAlias = List[Union[BatchSorter, str]]


def _is_batch_sorter_list(
    sorters: BatchSortersDefinition,
) -> TypeGuard[list[BatchSorter]]:
    if len(sorters) == 0 or isinstance(sorters[0], BatchSorter):
        return True
    return False


def _is_str_sorter_list(sorters: BatchSortersDefinition) -> TypeGuard[list[str]]:
    if len(sorters) > 0 and isinstance(sorters[0], str):
        return True
    return False


def _batch_sorter_from_list(sorters: BatchSortersDefinition) -> list[BatchSorter]:
    if _is_batch_sorter_list(sorters):
        return sorters

    # mypy doesn't successfully type-narrow sorters to a list[str] here, so we use
    # another TypeGuard. We could cast instead which may be slightly faster.
    sring_valued_sorter: str
    if _is_str_sorter_list(sorters):
        return [
            _batch_sorter_from_str(sring_valued_sorter)
            for sring_valued_sorter in sorters
        ]

    # This should never be reached because of static typing but is necessary because
    # mypy doesn't know of the if conditions must evaluate to True.
    raise ValueError(
        f"sorters is a not a BatchSortersDefinition but is a {type(sorters)}"
    )


def _batch_sorter_from_str(sort_key: str) -> BatchSorter:
    """Convert a list of strings to BatchSorters

    Args:
        sort_key: A batch metadata key which will be used to sort batches on a data asset.
                  This can be prefixed with a + or - to indicate increasing or decreasing
                  sorting.  If not specified, defaults to increasing order.
    """
    if sort_key[0] == "-":
        return BatchSorter(key=sort_key[1:], reverse=True)
    elif sort_key[0] == "+":
        return BatchSorter(key=sort_key[1:], reverse=False)
    else:
        return BatchSorter(key=sort_key, reverse=False)


# It would be best to bind this to Datasource, but we can't now due to circular dependencies
_DatasourceT = TypeVar("_DatasourceT")


class DataAsset(ExperimentalBaseModel, Generic[_DatasourceT]):
    # To subclass a DataAsset one must define `type` as a Class literal explicitly on the sublass
    # as well as implementing the methods in the `Abstract Methods` section below.
    # Some examples:
    # * type: Literal["MyAssetTypeID"] = "MyAssetTypeID",
    # * type: Literal["table"] = "table"
    # * type: Literal["csv"] = "csv"
    name: str
    type: str

    order_by: List[BatchSorter] = Field(default_factory=list)

    # non-field private attributes
    _datasource: _DatasourceT = pydantic.PrivateAttr()

    @property
    def datasource(self) -> _DatasourceT:
        return self._datasource

    def test_connection(self) -> None:
        """Test the connection for the DataAsset.

        Raises:
            TestConnectionError: If the connection test fails.
        """
        raise NotImplementedError(
            """One needs to implement "test_connection" on a DataAsset subclass."""
        )

    # Abstract Methods
    def batch_request_options_template(
        self,
    ) -> BatchRequestOptions:
        """A BatchRequestOptions template for build_batch_request.

        Returns:
            A BatchRequestOptions dictionary with the correct shape that build_batch_request
            will understand. All the option values are defaulted to None.
        """
        raise NotImplementedError

    def get_batch_list_from_batch_request(
        self, batch_request: BatchRequest
    ) -> List[Batch]:
        raise NotImplementedError

    # End Abstract Methods

    def build_batch_request(
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
        raise NotImplementedError(
            """One must implement "build_batch_request" on a DataAsset subclass."""
        )

    def _valid_batch_request_options(self, options: BatchRequestOptions) -> bool:
        return set(options.keys()).issubset(
            set(self.batch_request_options_template().keys())
        )

    def _validate_batch_request(self, batch_request: BatchRequest) -> None:
        """Validates the batch_request has the correct form.

        Args:
            batch_request: A batch request object to be validated.
        """
        raise NotImplementedError(
            """One must implement "_validate_batch_request" on a DataAsset subclass."""
        )

    # Sorter methods
    @pydantic.validator("order_by", pre=True, each_item=True)
    def _parse_order_by_sorter(
        cls, v: Union[str, BatchSorter]
    ) -> Union[BatchSorter, dict]:
        if isinstance(v, str):
            if not v:
                raise ValueError("empty string")
            return _batch_sorter_from_str(v)
        return v

    def add_sorters(self: _DataAssetT, sorters: BatchSortersDefinition) -> _DataAssetT:
        """Associates a sorter to this DataAsset

        The passed in sorters will replace any previously associated sorters.
        Batches returned from this DataAsset will be sorted on the batch's
        metadata in the order specified by `sorters`. Sorters work left to right.
        That is, batches will be sorted first by sorters[0].key, then
        sorters[1].key, and so on. If sorter[i].reverse is True, that key will
        sort the batches in descending, as opposed to ascending, order.

        Args:
            sorters: A list of either BatchSorter objects or strings. The strings
              are a shorthand for BatchSorter objects and are parsed as follows:
              r'[+-]?.*'
              An optional prefix of '+' or '-' sets BatchSorter.reverse to
              'False' or 'True' respectively. It is 'False' if no prefix is present.
              The rest of the string gets assigned to the BatchSorter.key.
              For example:
              ["key1", "-key2", "key3"]
              is equivalent to:
              [
                  BatchSorter(key="key1", reverse=False),
                  BatchSorter(key="key2", reverse=True),
                  BatchSorter(key="key3", reverse=False),
              ]

        Returns:
            This DataAsset with the passed in sorters accessible via self.order_by
        """
        # NOTE: (kilo59) we could use pydantic `validate_assignment` for this
        # https://docs.pydantic.dev/usage/model_config/#options
        self.order_by = _batch_sorter_from_list(sorters)
        return self

    def sort_batches(self, batch_list: List[Batch]) -> None:
        """Sorts batch_list in place in the order configured in this DataAsset.

        Args:
            batch_list: The list of batches to sort in place.
        """
        for sorter in reversed(self.order_by):
            try:
                batch_list.sort(
                    key=lambda b: b.metadata[sorter.key],
                    reverse=sorter.reverse,
                )
            except KeyError as e:
                raise KeyError(
                    f"Trying to sort {self.name} table asset batches on key {sorter.key} "
                    "which isn't available on all batches."
                ) from e


# If a Datasource can have more than 1 _DataAssetT, this will need to change.
_DataAssetT = TypeVar("_DataAssetT", bound=DataAsset)


# It would be best to bind this to ExecutionEngine, but we can't now due to circular imports
_ExecutionEngineT = TypeVar("_ExecutionEngineT")


class Datasource(
    ExperimentalBaseModel,
    Generic[_DataAssetT, _ExecutionEngineT],
    metaclass=MetaDatasource,
):
    # To subclass Datasource one needs to define:
    # asset_types
    # type
    # assets
    #
    # The important part of defining `assets` is setting the Dict type correctly.
    # In addition, one must define the methods in the `Abstract Methods` section below.
    # If one writes a class level docstring, this will become the documenation for the
    # data context method `data_context.sources.add_my_datasource` method.

    # class attrs
    asset_types: ClassVar[List[Type[DataAsset]]] = []
    # Datasource instance attrs but these will be fed into the `execution_engine` constructor
    _EXCLUDED_EXEC_ENG_ARGS: ClassVar[Set[str]] = {
        "name",
        "type",
        "execution_engine",
        "assets",
        "base_directory",
    }
    # Setting this in a Datasource subclass will override the execution engine type.
    # The primary use case is to inject an execution engine for testing.
    execution_engine_override: ClassVar[Optional[Type[_ExecutionEngineT]]] = None  # type: ignore[misc]  # ClassVar cannot contain type variables

    # instance attrs
    type: str
    name: str
    assets: MutableMapping[str, _DataAssetT] = {}

    # private attrs
    _cached_execution_engine_kwargs: Dict[str, Any] = pydantic.PrivateAttr({})
    _execution_engine: Union[_ExecutionEngineT, None] = pydantic.PrivateAttr(None)

    @pydantic.validator("assets", each_item=True)
    @classmethod
    def _load_asset_subtype(
        cls: Type[Datasource[_DataAssetT, _ExecutionEngineT]], data_asset: DataAsset
    ) -> _DataAssetT:
        """
        Some `data_asset` may be loaded as a less specific asset subtype different than
        what was intended.
        If a more specific subtype is needed the `data_asset` will be converted to a
        more specific `DataAsset`.
        """
        logger.info(f"Loading '{data_asset.name}' asset ->\n{pf(data_asset, depth=4)}")
        asset_type_name: str = data_asset.type
        asset_type: Type[_DataAssetT] = _SourceFactories.type_lookup[asset_type_name]

        if asset_type is type(data_asset):
            # asset is already the intended type
            return data_asset

        # strip out asset default kwargs
        kwargs = data_asset.dict(exclude_unset=True)
        logger.debug(f"{asset_type_name} - kwargs\n{pf(kwargs)}")

        asset_of_intended_type = asset_type(**kwargs)
        logger.debug(f"{asset_type_name} - {repr(asset_of_intended_type)}")
        return asset_of_intended_type

    def _execution_engine_type(self) -> Type[_ExecutionEngineT]:
        """Returns the execution engine to be used"""
        return self.execution_engine_override or self.execution_engine_type

    def get_execution_engine(self) -> _ExecutionEngineT:
        current_execution_engine_kwargs = self.dict(
            exclude=self._EXCLUDED_EXEC_ENG_ARGS
        )
        if (
            current_execution_engine_kwargs != self._cached_execution_engine_kwargs
            or not self._execution_engine
        ):
            self._execution_engine = self._execution_engine_type()(
                **current_execution_engine_kwargs
            )
            self._cached_execution_engine_kwargs = current_execution_engine_kwargs
        return self._execution_engine

    def get_batch_list_from_batch_request(
        self, batch_request: BatchRequest
    ) -> List[Batch]:
        """A list of batches that correspond to the BatchRequest.

        Args:
            batch_request: A batch request for this asset. Usually obtained by calling
                build_batch_request on the asset.

        Returns:
            A list of batches that match the options specified in the batch request.
        """
        data_asset = self.get_asset(batch_request.data_asset_name)
        return data_asset.get_batch_list_from_batch_request(batch_request)

    def get_asset(self, asset_name: str) -> _DataAssetT:
        """Returns the DataAsset referred to by name"""
        # This default implementation will be used if protocol is inherited
        try:
            return self.assets[asset_name]
        except KeyError as exc:
            raise LookupError(
                f"'{asset_name}' not found. Available assets are {list(self.assets.keys())}"
            ) from exc

    def add_asset(self, asset: _DataAssetT) -> _DataAssetT:
        """Adds an asset to a datasource

        Args:
            asset: The DataAsset to be added to this datasource.
        """
        # The setter for datasource is non-functional, so we access _datasource directly.
        # See the comment in DataAsset for more information.
        asset._datasource = self
        asset.test_connection()
        self.assets[asset.name] = asset
        # pydantic needs to know that an asset has been set so that it doesn't get excluded
        # when dumping to dict, json, yaml etc.
        self.__fields_set__.update(_FIELDS_ALWAYS_SET)
        return asset

    # Abstract Methods
    @property
    def execution_engine_type(self) -> Type[_ExecutionEngineT]:
        """Return the ExecutionEngine type use for this Datasource"""
        raise NotImplementedError(
            """One needs to implement "execution_engine_type" on a Datasource subclass."""
        )

    def test_connection(self, test_assets: bool = True) -> None:
        """Test the connection for the Datasource.

        Args:
            test_assets: If assets have been passed to the Datasource, an attempt can be made to test them as well.

        Raises:
            TestConnectionError: If the connection test fails.
        """
        raise NotImplementedError(
            """One needs to implement "test_connection" on a Datasource subclass."""
        )

    # End Abstract Methods


@dataclasses.dataclass(frozen=True)
class HeadData:
    """
    An immutable wrapper around pd.DataFrame for .head() methods which
        are intended to be used for visual inspection of BatchData.
    """

    data: pd.DataFrame

    def __repr__(self) -> str:
        return self.data.__repr__()


class Batch(ExperimentalBaseModel):
    """This represents a batch of data.

    This is usually not the data itself but a hook to the data on an external datastore such as
    a spark or a sql database. An exception exists for pandas or any in-memory datastore.
    """

    datasource: Datasource
    data_asset: DataAsset
    batch_request: BatchRequest
    data: BatchData
    id: str = ""
    # metadata is any arbitrary data one wants to associate with a batch. GX will add arbitrary metadata
    # to a batch so developers may want to namespace any custom metadata they add.
    metadata: Dict[str, Any] = {}

    # TODO: These legacy fields are currently required. They are only used in usage stats so we
    #       should figure out a better way to anonymize and delete them.
    batch_markers: BatchMarkers = Field(..., alias="legacy_batch_markers")
    batch_spec: BatchSpec = Field(..., alias="legacy_batch_spec")
    batch_definition: BatchDefinition = Field(..., alias="legacy_batch_definition")

    class Config:
        allow_mutation = False
        arbitrary_types_allowed = True

    @root_validator(pre=True)
    def _set_id(cls, values: dict) -> dict:
        # We need a unique identifier. This will likely change as we get more input.
        options_list = []
        for k, v in values["batch_request"].options.items():
            options_list.append(f"{k}_{v}")

        values["id"] = "-".join(
            [values["datasource"].name, values["data_asset"].name, *options_list]
        )

        return values

    @classmethod
    def update_forward_refs(cls):
        from great_expectations.core.batch import (
            BatchData,
            BatchDefinition,
            BatchMarkers,
        )

        super().update_forward_refs(
            BatchData=BatchData,
            BatchDefinition=BatchDefinition,
            BatchMarkers=BatchMarkers,
        )

    @validate_arguments
    def head(
        self,
        n_rows: StrictInt = 5,
        fetch_all: StrictBool = False,
    ) -> HeadData:
        """Return the first n rows of this Batch.

        This method returns the first n rows for the Batch based on position.

        For negative values of n_rows, this method returns all rows except the last n rows.

        If n_rows is larger than the number of rows, this method returns all rows.

        Parameters
            n_rows: The number of rows to return from the Batch.
            fetch_all: If True, ignore n_rows and return the entire Batch.

        Returns
            HeadData
        """
        self.data.execution_engine.batch_manager.load_batch_list(batch_list=[self])
        metrics_calculator = MetricsCalculator(
            execution_engine=self.data.execution_engine,
            show_progress_bars=True,
        )
        table_head_df: pd.DataFrame = metrics_calculator.head(
            n_rows=n_rows,
            domain_kwargs={"batch_id": self.id},
            fetch_all=fetch_all,
        )
        return HeadData(data=table_head_df.reset_index(drop=True, inplace=False))
