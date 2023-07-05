from __future__ import annotations

import copy
import dataclasses
import functools
import logging
import uuid
import warnings
from pprint import pformat as pf
from typing import (
    TYPE_CHECKING,
    AbstractSet,
    Any,
    Callable,
    ClassVar,
    Dict,
    Final,
    Generic,
    List,
    Mapping,
    MutableMapping,
    MutableSequence,
    Optional,
    Sequence,
    Set,
    Type,
    TypeVar,
    Union,
)

import pydantic
from pydantic import (
    Field,
    StrictBool,
    StrictInt,
    root_validator,
    validate_arguments,
)
from pydantic import dataclasses as pydantic_dc

from great_expectations.core._docs_decorators import public_api
from great_expectations.core.config_substitutor import _ConfigurationSubstitutor
from great_expectations.core.id_dict import BatchSpec
from great_expectations.datasource.fluent.fluent_base_model import (
    FluentBaseModel,
)
from great_expectations.datasource.fluent.metadatasource import MetaDatasource
from great_expectations.validator.metrics_calculator import MetricsCalculator

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    import pandas as pd
    from typing_extensions import TypeAlias, TypeGuard

    MappingIntStrAny = Mapping[Union[int, str], Any]
    AbstractSetIntStr = AbstractSet[Union[int, str]]
    # TODO: We should try to import the annotations from core.batch so we no longer need to call
    #  Batch.update_forward_refs() before instantiation.
    from great_expectations.core.batch import (
        BatchData,
        BatchDefinition,
        BatchMarkers,
    )
    from great_expectations.core.config_provider import _ConfigurationProvider
    from great_expectations.data_context import (
        AbstractDataContext as GXDataContext,
    )
    from great_expectations.datasource.data_connector.batch_filter import BatchSlice
    from great_expectations.datasource.fluent import (
        BatchRequest,
        BatchRequestOptions,
    )
    from great_expectations.datasource.fluent.data_asset.data_connector import (
        DataConnector,
    )
    from great_expectations.datasource.fluent.type_lookup import (
        TypeLookup,
    )


class TestConnectionError(Exception):
    pass


class GxSerializationWarning(UserWarning):
    pass


BatchMetadata: TypeAlias = Dict[str, Any]


@pydantic_dc.dataclass(frozen=True)
class Sorter:
    key: str
    reverse: bool = False


SortersDefinition: TypeAlias = List[Union[Sorter, str, dict]]


def _is_sorter_list(
    sorters: SortersDefinition,
) -> TypeGuard[list[Sorter]]:
    if len(sorters) == 0 or isinstance(sorters[0], Sorter):
        return True
    return False


def _is_str_sorter_list(sorters: SortersDefinition) -> TypeGuard[list[str]]:
    if len(sorters) > 0 and isinstance(sorters[0], str):
        return True
    return False


def _sorter_from_list(sorters: SortersDefinition) -> list[Sorter]:
    if _is_sorter_list(sorters):
        return sorters

    # mypy doesn't successfully type-narrow sorters to a list[str] here, so we use
    # another TypeGuard. We could cast instead which may be slightly faster.
    sring_valued_sorter: str
    if _is_str_sorter_list(sorters):
        return [
            _sorter_from_str(sring_valued_sorter) for sring_valued_sorter in sorters
        ]

    # This should never be reached because of static typing but is necessary because
    # mypy doesn't know of the if conditions must evaluate to True.
    raise ValueError(f"sorters is a not a SortersDefinition but is a {type(sorters)}")


def _sorter_from_str(sort_key: str) -> Sorter:
    """Convert a list of strings to Sorter objects

    Args:
        sort_key: A batch metadata key which will be used to sort batches on a data asset.
                  This can be prefixed with a + or - to indicate increasing or decreasing
                  sorting.  If not specified, defaults to increasing order.
    """
    if sort_key[0] == "-":
        return Sorter(key=sort_key[1:], reverse=True)

    if sort_key[0] == "+":
        return Sorter(key=sort_key[1:], reverse=False)

    return Sorter(key=sort_key, reverse=False)


# It would be best to bind this to Datasource, but we can't now due to circular dependencies
_DatasourceT = TypeVar("_DatasourceT", bound=MetaDatasource)


class DataAsset(FluentBaseModel, Generic[_DatasourceT]):
    # To subclass a DataAsset one must define `type` as a Class literal explicitly on the sublass
    # as well as implementing the methods in the `Abstract Methods` section below.
    # Some examples:
    # * type: Literal["MyAssetTypeID"] = "MyAssetTypeID",
    # * type: Literal["table"] = "table"
    # * type: Literal["csv"] = "csv"
    name: str
    type: str
    id: Optional[uuid.UUID] = Field(default=None, description="DataAsset id")

    order_by: List[Sorter] = Field(default_factory=list)
    batch_metadata: BatchMetadata = pydantic.Field(default_factory=dict)

    # non-field private attributes
    _datasource: _DatasourceT = pydantic.PrivateAttr()
    _data_connector: Optional[DataConnector] = pydantic.PrivateAttr(default=None)
    _test_connection_error_message: Optional[str] = pydantic.PrivateAttr(default=None)

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
    @property
    def batch_request_options(self) -> tuple[str, ...]:
        """The potential keys for BatchRequestOptions.

        Example:
        ```python
        >>> print(asset.batch_request_options)
        ("day", "month", "year")
        >>> options = {"year": "2023"}
        >>> batch_request = asset.build_batch_request(options=options)
        ```

        Returns:
            A tuple of keys that can be used in a BatchRequestOptions dictionary.
        """
        raise NotImplementedError(
            """One needs to implement "batch_request_options" on a DataAsset subclass."""
        )

    def get_batch_list_from_batch_request(
        self, batch_request: BatchRequest
    ) -> List[Batch]:
        raise NotImplementedError

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
        raise NotImplementedError(
            """One must implement "build_batch_request" on a DataAsset subclass."""
        )

    def _validate_batch_request(self, batch_request: BatchRequest) -> None:
        """Validates the batch_request has the correct form.

        Args:
            batch_request: A batch request object to be validated.
        """
        raise NotImplementedError(
            """One must implement "_validate_batch_request" on a DataAsset subclass."""
        )

    # End Abstract Methods

    def _valid_batch_request_options(self, options: BatchRequestOptions) -> bool:
        return set(options.keys()).issubset(set(self.batch_request_options))

    def _get_batch_metadata_from_batch_request(
        self, batch_request: BatchRequest
    ) -> BatchMetadata:
        """Performs config variable substitution and populates batch request options for
        Batch.metadata at runtime.
        """
        batch_metadata = copy.deepcopy(self.batch_metadata)
        config_variables = self._datasource._data_context.config_variables  # type: ignore[attr-defined]
        batch_metadata = _ConfigurationSubstitutor().substitute_all_config_variables(
            data=batch_metadata, replace_variables_dict=config_variables
        )
        batch_metadata.update(copy.deepcopy(batch_request.options))
        return batch_metadata

    # Sorter methods
    @pydantic.validator("order_by", pre=True)
    def _parse_order_by_sorters(
        cls, order_by: Optional[List[Union[Sorter, str, dict]]] = None
    ) -> List[Sorter]:
        return Datasource.parse_order_by_sorters(order_by=order_by)

    def add_sorters(self: _DataAssetT, sorters: SortersDefinition) -> _DataAssetT:
        """Associates a sorter to this DataAsset

        The passed in sorters will replace any previously associated sorters.
        Batches returned from this DataAsset will be sorted on the batch's
        metadata in the order specified by `sorters`. Sorters work left to right.
        That is, batches will be sorted first by sorters[0].key, then
        sorters[1].key, and so on. If sorter[i].reverse is True, that key will
        sort the batches in descending, as opposed to ascending, order.

        Args:
            sorters: A list of either Sorter objects or strings. The strings
              are a shorthand for Sorter objects and are parsed as follows:
              r'[+-]?.*'
              An optional prefix of '+' or '-' sets Sorter.reverse to
              'False' or 'True' respectively. It is 'False' if no prefix is present.
              The rest of the string gets assigned to the Sorter.key.
              For example:
              ["key1", "-key2", "key3"]
              is equivalent to:
              [
                  Sorter(key="key1", reverse=False),
                  Sorter(key="key2", reverse=True),
                  Sorter(key="key3", reverse=False),
              ]

        Returns:
            This DataAsset with the passed in sorters accessible via self.order_by
        """
        # NOTE: (kilo59) we could use pydantic `validate_assignment` for this
        # https://docs.pydantic.dev/usage/model_config/#options
        self.order_by = _sorter_from_list(sorters)
        return self

    def sort_batches(self, batch_list: List[Batch]) -> None:
        """Sorts batch_list in place in the order configured in this DataAsset.

        Args:
            batch_list: The list of batches to sort in place.
        """
        for sorter in reversed(self.order_by):
            try:
                batch_list.sort(
                    key=functools.cmp_to_key(
                        _sort_batches_with_none_metadata_values(sorter.key)
                    ),
                    reverse=sorter.reverse,
                )
            except KeyError as e:
                raise KeyError(
                    f"Trying to sort {self.name} table asset batches on key {sorter.key} "
                    "which isn't available on all batches."
                ) from e


def _sort_batches_with_none_metadata_values(
    key: str,
) -> Callable[[Batch, Batch], int]:
    def _compare_function(a: Batch, b: Batch) -> int:
        if a.metadata[key] is not None and b.metadata[key] is not None:
            if a.metadata[key] < b.metadata[key]:
                return -1

            if a.metadata[key] > b.metadata[key]:
                return 1

            return 0

        if a.metadata[key] is None and b.metadata[key] is None:
            return 0

        if a.metadata[key] is None:  # b.metadata[key] is not None
            return -1

        if a.metadata[key] is not None:  # b.metadata[key] is None
            return 1

        # This line should never be reached; hence, "ValueError" with corresponding error message is raised.
        raise ValueError(
            f'Unexpected Batch metadata key combination, "{a.metadata[key]}" and "{b.metadata[key]}", was encountered.'
        )

    return _compare_function


# If a Datasource can have more than 1 _DataAssetT, this will need to change.
_DataAssetT = TypeVar("_DataAssetT", bound=DataAsset)


# It would be best to bind this to ExecutionEngine, but we can't now due to circular imports
_ExecutionEngineT = TypeVar("_ExecutionEngineT")


class Datasource(
    FluentBaseModel,
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
    asset_types: ClassVar[Sequence[Type[DataAsset]]] = []
    # Not all Datasources require a DataConnector
    data_connector_type: ClassVar[Optional[Type[DataConnector]]] = None
    # Datasource sublcasses should update this set if the field should not be passed to the execution engine
    _EXTRA_EXCLUDED_EXEC_ENG_ARGS: ClassVar[Set[str]] = set()
    _type_lookup: ClassVar[  # This attribute is set in `MetaDatasource.__new__`
        TypeLookup
    ]
    # Setting this in a Datasource subclass will override the execution engine type.
    # The primary use case is to inject an execution engine for testing.
    execution_engine_override: ClassVar[Optional[Type[_ExecutionEngineT]]] = None  # type: ignore[misc]  # ClassVar cannot contain type variables

    # instance attrs
    type: str
    name: str
    id: Optional[uuid.UUID] = Field(default=None, description="Datasource id")
    assets: MutableSequence[_DataAssetT] = []

    # private attrs
    _data_context: Union[GXDataContext, None] = pydantic.PrivateAttr(None)
    _cached_execution_engine_kwargs: Dict[str, Any] = pydantic.PrivateAttr({})
    _execution_engine: Union[_ExecutionEngineT, None] = pydantic.PrivateAttr(None)

    @property
    def _config_provider(self) -> Union[_ConfigurationProvider, None]:
        return getattr(self._data_context, "config_provider", None)

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
        logger.debug(f"Loading '{data_asset.name}' asset ->\n{pf(data_asset, depth=4)}")
        asset_type_name: str = data_asset.type
        asset_type: Type[_DataAssetT] = cls._type_lookup[asset_type_name]

        if asset_type is type(data_asset):
            # asset is already the intended type
            return data_asset

        # strip out asset default kwargs
        kwargs = data_asset.dict(exclude_unset=True)
        logger.debug(f"{asset_type_name} - kwargs\n{pf(kwargs)}")

        cls._update_asset_forward_refs(asset_type)

        asset_of_intended_type = asset_type(**kwargs)
        logger.debug(f"{asset_type_name} - {repr(asset_of_intended_type)}")
        return asset_of_intended_type

    def _execution_engine_type(self) -> Type[_ExecutionEngineT]:
        """Returns the execution engine to be used"""
        return self.execution_engine_override or self.execution_engine_type

    def get_execution_engine(self) -> _ExecutionEngineT:
        current_execution_engine_kwargs = self.dict(
            exclude=self._get_exec_engine_excludes(),
            config_provider=self._config_provider,
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

    def get_assets_as_dict(self) -> MutableMapping[str, _DataAssetT]:
        """Returns available DataAsset objects as dictionary, with corresponding name as key.

        Returns:
            Dictionary of "_DataAssetT" objects with "name" attribute serving as key.
        """
        asset: _DataAssetT
        assets_as_dict: MutableMapping[str, _DataAssetT] = {
            asset.name: asset for asset in self.assets
        }

        return assets_as_dict

    def get_asset_names(self) -> Set[str]:
        """Returns the set of available DataAsset names

        Returns:
            Set of available DataAsset names.
        """
        asset: _DataAssetT
        return {asset.name for asset in self.assets}

    def get_asset(self, asset_name: str) -> _DataAssetT:
        """Returns the DataAsset referred to by asset_name

        Args:
            asset_name: name of DataAsset sought.

        Returns:
            _DataAssetT -- if named "DataAsset" object exists; otherwise, exception is raised.
        """
        # This default implementation will be used if protocol is inherited
        try:
            asset: _DataAssetT
            found_asset: _DataAssetT = list(
                filter(lambda asset: asset.name == asset_name, self.assets)
            )[0]
            found_asset._datasource = self
            return found_asset
        except IndexError as exc:
            raise LookupError(
                f'"{asset_name}" not found. Available assets are {", ".join(self.get_asset_names())})'
            ) from exc

    def delete_asset(self, asset_name: str) -> None:
        """Removes the DataAsset referred to by asset_name from internal list of available DataAsset objects.

        Args:
            asset_name: name of DataAsset to be deleted.
        """
        asset: _DataAssetT
        self.assets = list(filter(lambda asset: asset.name != asset_name, self.assets))

        self._save_context_project_config()

    def _add_asset(
        self, asset: _DataAssetT, connect_options: dict | None = None
    ) -> _DataAssetT:
        """Adds an asset to a datasource

        Args:
            asset: The DataAsset to be added to this datasource.
        """
        # The setter for datasource is non-functional, so we access _datasource directly.
        # See the comment in DataAsset for more information.
        asset._datasource = self

        if not connect_options:
            connect_options = {}
        self._build_data_connector(asset, **connect_options)

        asset.test_connection()

        asset_names: Set[str] = self.get_asset_names()
        if asset.name in asset_names:
            raise ValueError(
                f'"{asset.name}" already exists (all existing assets are {", ".join(asset_names)})'
            )

        self.assets.append(asset)

        self._save_context_project_config()
        return asset

    def _save_context_project_config(self):
        """Check if a DataContext is available and save the project config."""
        if self._data_context:
            try:
                self._data_context._save_project_config(self)
            except TypeError as type_err:
                warnings.warn(str(type_err), GxSerializationWarning)

    def _rebuild_asset_data_connectors(self) -> None:
        """If Datasource required a data_connector we need to build the data_connector for each asset"""
        if self.data_connector_type:
            for data_asset in self.assets:
                # check if data_connector exist before rebuilding?
                connect_options = getattr(data_asset, "connect_options", {})
                self._build_data_connector(data_asset, **connect_options)

    @staticmethod
    def parse_order_by_sorters(
        order_by: Optional[List[Union[Sorter, str, dict]]] = None
    ) -> List[Sorter]:
        order_by_sorters: list[Sorter] = []
        if order_by:
            for idx, sorter in enumerate(order_by):
                if isinstance(sorter, str):
                    if not sorter:
                        raise ValueError(
                            '"order_by" list cannot contain an empty string'
                        )
                    order_by_sorters.append(_sorter_from_str(sorter))
                elif isinstance(sorter, dict):
                    key: Optional[Any] = sorter.get("key")
                    reverse: Optional[Any] = sorter.get("reverse")
                    if key and reverse:
                        order_by_sorters.append(Sorter(key=key, reverse=reverse))
                    elif key:
                        order_by_sorters.append(Sorter(key=key))
                    else:
                        raise ValueError(
                            '"order_by" list dict must have a key named "key"'
                        )
                else:
                    order_by_sorters.append(sorter)
        return order_by_sorters

    @staticmethod
    def _update_asset_forward_refs(asset_type: Type[_DataAssetT]) -> None:
        """Update forward refs of an asset_type if necessary.

        Note, this should be overridden in child datasource classes if forward
        refs need to be updated. For example, in Spark datasources we need to
        update forward refs only if the optional spark dependencies are installed
        so this method is overridden. Here it is a no op.

        Args:
            asset_type: Asset type to update forward refs.

        Returns:
            None, asset refs is updated in place.
        """
        pass

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

    def _build_data_connector(self, data_asset: _DataAssetT, **kwargs) -> None:
        """Any Datasource subclass that utilizes DataConnector should overwrite this method.

        Specific implementations instantiate appropriate DataConnector class and set "self._data_connector" to it.

        Args:
            data_asset: DataAsset using this DataConnector instance
            kwargs: Extra keyword arguments allow specification of arguments used by particular DataConnector subclasses
        """
        pass

    @classmethod
    def _get_exec_engine_excludes(cls) -> Set[str]:
        """
        Return a set of field names to exclude from the execution engine.

        All datasource fields are passed to the execution engine by default unless they are in this set.

        Default implementation is to return the combined set of field names from `_EXTRA_EXCLUDED_EXEC_ENG_ARGS`
        and `_BASE_DATASOURCE_FIELD_NAMES`.
        """
        return cls._EXTRA_EXCLUDED_EXEC_ENG_ARGS.union(_BASE_DATASOURCE_FIELD_NAMES)

    # End Abstract Methods


# This is used to prevent passing things like `type`, `assets` etc. to the execution engine
_BASE_DATASOURCE_FIELD_NAMES: Final[Set[str]] = {
    name for name in Datasource.__fields__.keys()
}


@dataclasses.dataclass(frozen=True)
class HeadData:
    """
    An immutable wrapper around pd.DataFrame for .head() methods which
        are intended to be used for visual inspection of BatchData.
    """

    data: pd.DataFrame

    def __repr__(self) -> str:
        return self.data.__repr__()


class Batch(FluentBaseModel):
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
    metadata: Dict[str, Any] = Field(default_factory=dict, allow_mutation=True)

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
        for key, value in values["batch_request"].options.items():
            if key != "path":
                options_list.append(f"{key}_{value}")

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
        from great_expectations.datasource.fluent import BatchRequest

        super().update_forward_refs(
            BatchData=BatchData,
            BatchDefinition=BatchDefinition,
            BatchMarkers=BatchMarkers,
            BatchRequest=BatchRequest,
        )

    @public_api
    @validate_arguments
    def columns(self) -> List[str]:
        """Return column names of this Batch.

        Returns
            List[str]
        """
        self.data.execution_engine.batch_manager.load_batch_list(batch_list=[self])
        metrics_calculator = MetricsCalculator(
            execution_engine=self.data.execution_engine,
            show_progress_bars=True,
        )
        return metrics_calculator.columns()

    @public_api
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
