from __future__ import annotations

import copy
import dataclasses
import functools
import logging
import uuid
import warnings
from abc import ABC, abstractmethod
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
    Protocol,
    Sequence,
    Set,
    Type,
    TypeVar,
    Union,
    overload,
)

from great_expectations._docs_decorators import public_api
from great_expectations.compatibility import pydantic
from great_expectations.compatibility.pydantic import (
    Field,
    StrictBool,
    StrictInt,
    validate_arguments,
)
from great_expectations.compatibility.pydantic import dataclasses as pydantic_dc
from great_expectations.compatibility.typing_extensions import override
from great_expectations.core.batch_definition import BatchDefinition, PartitionerT
from great_expectations.core.config_substitutor import _ConfigurationSubstitutor
from great_expectations.core.result_format import DEFAULT_RESULT_FORMAT
from great_expectations.datasource.fluent.constants import (
    _ASSETS_KEY,
)
from great_expectations.datasource.fluent.fluent_base_model import (
    FluentBaseModel,
    GenericBaseModel,
)
from great_expectations.datasource.fluent.metadatasource import MetaDatasource
from great_expectations.exceptions.exceptions import (
    DataAssetInitializationError,
    DataContextError,
    MissingDataContextError,
)
from great_expectations.validator.metrics_calculator import MetricsCalculator

logger = logging.getLogger(__name__)
from great_expectations.datasource.fluent.data_connector import (
    DataConnector,
)

if TYPE_CHECKING:
    import pandas as pd
    from typing_extensions import TypeAlias, TypeGuard

    from great_expectations.core.result_format import ResultFormatUnion
    from great_expectations.core.suite_parameters import SuiteParameterDict

    MappingIntStrAny = Mapping[Union[int, str], Any]
    AbstractSetIntStr = AbstractSet[Union[int, str]]
    from great_expectations.core import (
        ExpectationSuite,
        ExpectationSuiteValidationResult,
        ExpectationValidationResult,
    )
    from great_expectations.core.batch import (
        BatchData,
        BatchMarkers,
        LegacyBatchDefinition,
    )
    from great_expectations.core.config_provider import _ConfigurationProvider
    from great_expectations.core.id_dict import BatchSpec
    from great_expectations.data_context import (
        AbstractDataContext as GXDataContext,
    )
    from great_expectations.datasource.fluent import (
        BatchParameters,
        BatchRequest,
    )
    from great_expectations.datasource.fluent.data_connector.batch_filter import BatchSlice
    from great_expectations.datasource.fluent.type_lookup import (
        TypeLookup,
    )
    from great_expectations.expectations.expectation import Expectation
    from great_expectations.validator.v1_validator import (
        Validator as V1Validator,
    )

_T = TypeVar("_T")


class PartitionerSortingProtocol(Protocol):
    """Interface defining the fields a Partitioner must contain for sorting."""

    sort_ascending: bool

    @property
    def param_names(self) -> Union[list[str], tuple[str, ...]]:
        """The parameter names that specify a batch derived from this partitioner

        For example, for PartitionerYearMonth this returns ["year", "month"]. For more
        examples, please see concrete Partitioner* classes.
        """
        ...


class PartitionerProtocol(PartitionerSortingProtocol, Protocol):
    @property
    def columns(self) -> list[str]:
        """The names of the column used to partition the data"""
        ...

    @property
    def method_name(self) -> str:
        """Returns a partitioner method name.

        The possible values of partitioner method names are defined in the enum,
        great_expectations.execution_engine.partition_and_sample.data_partitioner.PartitionerMethod
        """
        ...

    def partitioner_method_kwargs(self) -> Dict[str, Any]:
        """A shim to our execution engine partitioner methods

        We translate any internal Partitioner state and what is passed in from
        a batch_request to the partitioner_kwargs required by our execution engine.

        Look at Partitioner* classes for concrete examples.
        """
        ...

    def batch_parameters_to_batch_spec_kwarg_identifiers(
        self, options: BatchParameters
    ) -> Dict[str, Any]:
        """Translates `options` to the execution engine batch spec kwarg identifiers

        Arguments:
            options: A BatchRequest.options dictionary that specifies ALL the fields necessary
                     to specify a batch with respect to this partitioner.

        Returns:
            A dictionary that can be added to batch_spec_kwargs["batch_identifiers"].
            This has one of 2 forms:
              1. This category has many parameters are derived from 1 column.
                 These only are datetime partitioners and the batch_spec_kwargs["batch_identifiers"]
                 look like:
                   {column_name: {datepart_1: value, datepart_2: value, ...}
                 where datepart_* are strings like "year", "month", "day". The exact
                 fields depend on the partitioner.

              2. This category has only 1 parameter for each column.
                 This is used for all other partitioners and the
                 batch_spec_kwargs["batch_identifiers"]
                 look like:
                   {column_name_1: value, column_name_2: value, ...}
                 where value is the value of the column after being processed by the partitioner.
                 For example, for the PartitionerModInteger where mod = 3,
                 {"passenger_count": 2}, means the raw passenger count value is in the set:
                 {2, 5, 8, ...} = {2*n + 1 | n is a nonnegative integer }
                 This category was only 1 parameter per column.
        """
        ...


class TestConnectionError(ConnectionError):
    """
    Raised if `.test_connection()` fails to connect to the datasource.
    """

    def __init__(
        self,
        message: str = "Attempt to connect to datasource failed",
        *,
        cause: Exception | None = None,
        addendum: str | None = None,
    ):
        """
        Args:
            `message` base of the error message to be provided to the user.
            `cause` is the original exception that caused the error, the repr of which will be added
                to the error message.
            `addendum` is optional additional information that can be added to the error message.
        """
        self.cause = cause  # not guaranteed to be the same as `self.__cause__`
        self.addendum = addendum
        if cause:
            message += f": due to {cause!r}"
        if addendum:
            message += f": {addendum}"
        super().__init__(message)


class GxDatasourceWarning(UserWarning):
    """
    Warning related to usage or configuration of a Datasource that could lead to
    unexpected behavior.
    """


class GxContextWarning(GxDatasourceWarning):
    """
    Warning related to a Datasource with a missing context.
    Usually because the Datasource was created directly rather than using a
    `context.sources` factory method.
    """


class GxSerializationWarning(GxDatasourceWarning):
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
    return len(sorters) == 0 or isinstance(sorters[0], Sorter)


def _is_str_sorter_list(sorters: SortersDefinition) -> TypeGuard[list[str]]:
    return len(sorters) > 0 and isinstance(sorters[0], str)


def _sorter_from_list(sorters: SortersDefinition) -> list[Sorter]:
    if _is_sorter_list(sorters):
        return sorters

    # mypy doesn't successfully type-narrow sorters to a list[str] here, so we use
    # another TypeGuard. We could cast instead which may be slightly faster.
    sring_valued_sorter: str
    if _is_str_sorter_list(sorters):
        return [_sorter_from_str(sring_valued_sorter) for sring_valued_sorter in sorters]

    # This should never be reached because of static typing but is necessary because
    # mypy doesn't know of the if conditions must evaluate to True.
    raise ValueError(  # noqa: TRY003
        f"sorters is a not a SortersDefinition but is a {type(sorters)}"
    )


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


# It would be best to bind this to ExecutionEngine, but we can't now due to circular imports
_ExecutionEngineT = TypeVar("_ExecutionEngineT")


DatasourceT = TypeVar("DatasourceT", bound="Datasource")


@public_api
class DataAsset(GenericBaseModel, Generic[DatasourceT, PartitionerT], ABC):
    # To subclass a DataAsset one must define `type` as a Class literal explicitly on the sublass
    # as well as implementing the methods in the `Abstract Methods` section below.
    # Some examples:
    # * type: Literal["MyAssetTypeID"] = "MyAssetTypeID",
    # * type: Literal["table"] = "table"
    # * type: Literal["csv"] = "csv"
    name: str
    type: str
    id: Optional[uuid.UUID] = Field(default=None, description="DataAsset id")

    # TODO: order_by should no longer be used and should be removed
    order_by: List[Sorter] = Field(default_factory=list)
    batch_metadata: BatchMetadata = pydantic.Field(default_factory=dict)
    batch_definitions: List[BatchDefinition] = Field(default_factory=list)

    # non-field private attributes
    _datasource: DatasourceT = pydantic.PrivateAttr()
    _data_connector: Optional[DataConnector] = pydantic.PrivateAttr(default=None)
    _test_connection_error_message: Optional[str] = pydantic.PrivateAttr(default=None)

    @property
    def datasource(self) -> DatasourceT:
        return self._datasource

    def test_connection(self) -> None:
        """Test the connection for the DataAsset.

        Raises:
            TestConnectionError: If the connection test fails.
        """
        raise NotImplementedError(
            """One needs to implement "test_connection" on a DataAsset subclass."""
        )

    def get_batch_parameters_keys(
        self, partitioner: Optional[PartitionerT] = None
    ) -> tuple[str, ...]:
        raise NotImplementedError(
            """One needs to implement "get_batch_parameters_keys" on a DataAsset subclass."""
        )

    def build_batch_request(
        self,
        options: Optional[BatchParameters] = None,
        batch_slice: Optional[BatchSlice] = None,
        partitioner: Optional[PartitionerT] = None,
    ) -> BatchRequest[PartitionerT]:
        """A batch request that can be used to obtain batches for this DataAsset.

        Args:
            options: A dict that can be used to filter the batch groups returned from the asset.
                The dict structure depends on the asset type. The available keys for dict can be obtained by
                calling get_batch_parameters_keys(...).
            batch_slice: A python slice that can be used to limit the sorted batches by index.
                e.g. `batch_slice = "[-5:]"` will request only the last 5 batches after the options filter is applied.
            partitioner: A Partitioner used to narrow the data returned from the asset.

        Returns:
            A BatchRequest object that can be used to obtain a batch from an asset by calling the
            get_batch method.
        """  # noqa: E501
        raise NotImplementedError(
            """One must implement "build_batch_request" on a DataAsset subclass."""
        )

    @abstractmethod
    def get_batch_identifiers_list(self, batch_request: BatchRequest) -> List[dict]: ...

    @abstractmethod
    def get_batch(self, batch_request: BatchRequest) -> Batch: ...

    def _validate_batch_request(self, batch_request: BatchRequest) -> None:
        """Validates the batch_request has the correct form.

        Args:
            batch_request: A batch request object to be validated.
        """
        raise NotImplementedError(
            """One must implement "_validate_batch_request" on a DataAsset subclass."""
        )

    # End Abstract Methods

    def add_batch_definition(
        self,
        name: str,
        partitioner: Optional[PartitionerT] = None,
    ) -> BatchDefinition[PartitionerT]:
        """Add a BatchDefinition to this DataAsset.
        BatchDefinition names must be unique within a DataAsset.

        If the DataAsset is tied to a DataContext, the BatchDefinition will be persisted.

        Args:
            name (str): Name of the new batch definition.
            partitioner: Optional Partitioner to partition this BatchDefinition

        Returns:
            BatchDefinition: The new batch definition.
        """
        batch_definition_names = {bc.name for bc in self.batch_definitions}
        if name in batch_definition_names:
            raise ValueError(  # noqa: TRY003
                f'"{name}" already exists (all existing batch_definition names are {", ".join(batch_definition_names)})'  # noqa: E501
            )

        # Let mypy know that self.datasource is a Datasource (it is currently bound to MetaDatasource)  # noqa: E501
        assert isinstance(self.datasource, Datasource)

        batch_definition = BatchDefinition[PartitionerT](name=name, partitioner=partitioner)
        batch_definition.set_data_asset(self)
        self.batch_definitions.append(batch_definition)
        self.update_batch_definition_field_set()
        if self.datasource.data_context:
            try:
                batch_definition = self.datasource.add_batch_definition(batch_definition)
            except Exception:
                self.batch_definitions.remove(batch_definition)
                self.update_batch_definition_field_set()
                raise
        self.update_batch_definition_field_set()
        return batch_definition

    @public_api
    def delete_batch_definition(self, name: str) -> None:
        """Delete a batch definition.

        Args:
            name (str): Name of the BatchDefinition to delete.
        """
        try:
            batch_def = self.get_batch_definition(name)
        except KeyError as err:
            # We collect the names as a list because while we shouldn't have more than 1
            # batch definition with the same name, we want to represent it if it does occur.
            batch_definition_names = [bc.name for bc in self.batch_definitions]
            raise ValueError(  # noqa: TRY003
                f'"{name}" does not exist. Existing batch_definition names are {batch_definition_names})'  # noqa: E501
            ) from err
        self._delete_batch_definition(batch_def)

    def _delete_batch_definition(self, batch_definition: BatchDefinition[PartitionerT]) -> None:
        # Let mypy know that self.datasource is a Datasource (it is currently bound to MetaDatasource)  # noqa: E501
        assert isinstance(self.datasource, Datasource)

        self.batch_definitions.remove(batch_definition)
        if self.datasource.data_context:
            try:
                self.datasource.delete_batch_definition(batch_definition)
            except Exception:
                self.batch_definitions.append(batch_definition)
                raise

        self.update_batch_definition_field_set()

    def update_batch_definition_field_set(self) -> None:
        """Ensure that we have __fields_set__ set correctly for batch_definitions to ensure we serialize IFF needed."""  # noqa: E501

        has_batch_definitions = len(self.batch_definitions) > 0
        if "batch_definitions" in self.__fields_set__ and not has_batch_definitions:
            self.__fields_set__.remove("batch_definitions")
        elif "batch_definitions" not in self.__fields_set__ and has_batch_definitions:
            self.__fields_set__.add("batch_definitions")

    @public_api
    def get_batch_definition(self, name: str) -> BatchDefinition[PartitionerT]:
        """Get a batch definition.

        Args:
            name (str): Name of the BatchDefinition to get.
        Raises:
            KeyError: If the BatchDefinition does not exist.
        """
        batch_definitions = [
            batch_definition
            for batch_definition in self.batch_definitions
            if batch_definition.name == name
        ]
        if len(batch_definitions) == 0:
            raise KeyError(  # noqa: TRY003
                f"BatchDefinition {name} not found"
            )
        elif len(batch_definitions) > 1:
            # Our add_batch_definition() method should enforce that different
            # batch definitions do not share a name.
            raise KeyError(  # noqa: TRY003
                f"Multiple keys for {name} found"
            )
        return batch_definitions[0]

    def _batch_parameters_are_valid(
        self, options: BatchParameters, partitioner: Optional[PartitionerT]
    ) -> bool:
        valid_options = self.get_batch_parameters_keys(partitioner=partitioner)
        return set(options.keys()).issubset(set(valid_options))

    @pydantic.validator("batch_metadata", pre=True)
    def ensure_batch_metadata_is_not_none(cls, value: Any) -> Union[dict, Any]:
        """If batch metadata is None, replace it with an empty dict."""
        if value is None:
            return {}
        return value

    def _get_batch_metadata_from_batch_request(
        self, batch_request: BatchRequest, ignore_options: Sequence = ()
    ) -> BatchMetadata:
        """Performs config variable substitution and populates batch parameters for
        Batch.metadata at runtime.
        """
        batch_metadata = copy.deepcopy(self.batch_metadata)
        if not self._datasource.data_context:
            raise MissingDataContextError()
        config_variables = self._datasource.data_context.config_variables
        batch_metadata = _ConfigurationSubstitutor().substitute_all_config_variables(
            data=batch_metadata, replace_variables_dict=config_variables
        )
        batch_metadata.update(
            copy.deepcopy(
                {k: v for k, v in batch_request.options.items() if k not in ignore_options}
            )
        )
        return batch_metadata

    # Sorter methods
    @pydantic.validator("order_by", pre=True)
    def _order_by_validator(
        cls, order_by: Optional[List[Union[Sorter, str, dict]]] = None
    ) -> List[Sorter]:
        if order_by:
            raise DataAssetInitializationError(
                message="'order_by' is no longer a valid argument. "
                "Sorting should be configured in a batch definition."
            )
        return []

    def sort_batches(
        self, batch_list: List[Batch], partitioner: PartitionerSortingProtocol
    ) -> List[Batch]:
        """Sorts batch_list in place in the order configured in this DataAsset.
        Args:
            batch_list: The list of batches to sort in place.
            partitioner: Configuration used to determine sort.
        """

        def get_value(key: str) -> Callable[[Batch], Any]:
            return lambda bd: bd.metadata[key]

        return self._sort_batch_data_list(batch_list, partitioner, get_value)

    def sort_legacy_batch_definitions(
        self,
        legacy_batch_definition_list: List[LegacyBatchDefinition],
        partitioner: PartitionerSortingProtocol,
    ) -> List[LegacyBatchDefinition]:
        """Sorts batch_definition_list in the order configured by the partitioner."""

        def get_value(key: str) -> Callable[[LegacyBatchDefinition], Any]:
            return lambda bd: bd.batch_identifiers[key]

        return self._sort_batch_data_list(legacy_batch_definition_list, partitioner, get_value)

    def sort_batch_identifiers_list(
        self, batch_identfiers_list: List[dict], partitioner: PartitionerSortingProtocol
    ) -> List[dict]:
        """Sorts batch_identfiers_list in the order configured by the partitioner."""

        def get_value(key: str) -> Callable[[dict], Any]:
            return lambda d: d[key]

        return self._sort_batch_data_list(batch_identfiers_list, partitioner, get_value)

    def _sort_batch_data_list(
        self,
        batch_data_list: List[_T],
        partitioner: PartitionerSortingProtocol,
        get_value: Callable[[str], Any],
    ) -> List[_T]:
        """Sorts batch_data_list in the order configured by the partitioner."""
        reverse = not partitioner.sort_ascending
        for key in reversed(partitioner.param_names):
            try:
                batch_data_list = sorted(
                    batch_data_list,
                    key=functools.cmp_to_key(
                        _sort_batch_identifiers_with_none_metadata_values(get_value(key))
                    ),
                    reverse=reverse,
                )
            except KeyError as e:
                raise KeyError(  # noqa: TRY003
                    f"Trying to sort {self.name}'s batches on key {key}, "
                    "which isn't available on all batches."
                ) from e
        return batch_data_list


def _sort_batch_identifiers_with_none_metadata_values(
    get_val: Callable[[_T], Any],
) -> Callable[[_T, _T], int]:
    def _compare_function(a: _T, b: _T) -> int:
        a_val = get_val(a)
        b_val = get_val(b)

        if a_val is not None and b_val is not None:
            if a_val < b_val:
                return -1
            elif a_val > b_val:
                return 1
            else:
                return 0
        elif a_val is None and b_val is None:
            return 0
        elif a_val is None:  # b.metadata_val is not None
            return -1
        else:  # b[key] is None
            return 1

    return _compare_function


# If a Datasource can have more than 1 _DataAssetT, this will need to change.
_DataAssetT = TypeVar("_DataAssetT", bound=DataAsset)


@public_api
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
    # data context method `data_context.data_sources.add_my_datasource` method.

    # class attrs
    asset_types: ClassVar[Sequence[Type[DataAsset]]] = []
    # Not all Datasources require a DataConnector
    data_connector_type: ClassVar[Optional[Type[DataConnector]]] = None
    # Datasource sublcasses should update this set if the field should not be passed to the execution engine  # noqa: E501
    _EXTRA_EXCLUDED_EXEC_ENG_ARGS: ClassVar[Set[str]] = set()
    _type_lookup: ClassVar[TypeLookup]  # This attribute is set in `MetaDatasource.__new__`
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

    @property
    def data_context(self) -> GXDataContext | None:
        """The data context that this datasource belongs to.

        This method should only be used by library implementers.
        """
        return self._data_context

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
        logger.debug(f"{asset_type_name} - {asset_of_intended_type!r}")
        return asset_of_intended_type

    @pydantic.validator(_ASSETS_KEY, each_item=True)
    def _update_batch_definitions(cls, data_asset: DataAsset) -> DataAsset:
        for batch_definition in data_asset.batch_definitions:
            batch_definition.set_data_asset(data_asset)
        return data_asset

    def _execution_engine_type(self) -> Type[_ExecutionEngineT]:
        """Returns the execution engine to be used"""
        return self.execution_engine_override or self.execution_engine_type

    def add_batch_definition(
        self, batch_definition: BatchDefinition[PartitionerT]
    ) -> BatchDefinition[PartitionerT]:
        asset_name = batch_definition.data_asset.name
        if not self.data_context:
            raise DataContextError(  # noqa: TRY003
                "Cannot save datasource without a data context."
            )

        loaded_datasource = self.data_context.data_sources.get(self.name)
        if loaded_datasource is not self:
            # CachedDatasourceDict will return self; only add batch definition if this is a remote
            # copy
            assert isinstance(loaded_datasource, Datasource)
            loaded_asset = loaded_datasource.get_asset(asset_name)
            loaded_asset.batch_definitions.append(batch_definition)
            loaded_asset.update_batch_definition_field_set()
        updated_datasource = self.data_context.update_datasource(loaded_datasource)
        assert isinstance(updated_datasource, Datasource)

        output = updated_datasource.get_asset(asset_name).get_batch_definition(
            batch_definition.name
        )
        output.set_data_asset(batch_definition.data_asset)
        return output

    def delete_batch_definition(self, batch_definition: BatchDefinition[PartitionerT]) -> None:
        asset_name = batch_definition.data_asset.name
        if not self.data_context:
            raise DataContextError(  # noqa: TRY003
                "Cannot save datasource without a data context."
            )

        loaded_datasource = self.data_context.data_sources.get(self.name)
        if loaded_datasource is not self:
            # CachedDatasourceDict will return self; only add batch definition if this is a remote
            # copy
            assert isinstance(loaded_datasource, Datasource)
            loaded_asset = loaded_datasource.get_asset(asset_name)
            loaded_asset.batch_definitions.remove(batch_definition)
            loaded_asset.update_batch_definition_field_set()
        updated_datasource = self.data_context.update_datasource(loaded_datasource)
        assert isinstance(updated_datasource, Datasource)

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

    def get_batch(self, batch_request: BatchRequest) -> Batch:
        """A Batch that corresponds to the BatchRequest.

        Args:
            batch_request: A batch request for this asset. Usually obtained by calling
                build_batch_request on the asset.

        Returns:
            A Batch that matches the options specified in the batch request.
        """
        data_asset = self.get_asset(batch_request.data_asset_name)
        return data_asset.get_batch(batch_request)

    def get_batch_identifiers_list(self, batch_request: BatchRequest) -> List[dict]:
        data_asset = self.get_asset(batch_request.data_asset_name)
        return data_asset.get_batch_identifiers_list(batch_request)

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

    @public_api
    def get_asset(self, name: str) -> _DataAssetT:
        """Returns the DataAsset referred to by asset_name

        Args:
            name: name of DataAsset sought.

        Returns:
            _DataAssetT -- if named "DataAsset" object exists; otherwise, exception is raised.
        """
        # This default implementation will be used if protocol is inherited
        try:
            asset: _DataAssetT
            found_asset: _DataAssetT = list(filter(lambda asset: asset.name == name, self.assets))[
                0
            ]
            found_asset._datasource = self
            return found_asset
        except IndexError as exc:
            raise LookupError(  # noqa: TRY003
                f'"{name}" not found. Available assets are ({", ".join(self.get_asset_names())})'
            ) from exc

    @public_api
    def delete_asset(self, name: str) -> None:
        """Removes the DataAsset referred to by asset_name from internal list of available DataAsset objects.

        Args:
            name: name of DataAsset to be deleted.
        """  # noqa: E501
        from great_expectations.data_context import CloudDataContext

        asset: _DataAssetT
        asset = self.get_asset(name=name)

        if self._data_context and isinstance(self._data_context, CloudDataContext):
            self._data_context._delete_asset(id=str(asset.id))

        self.assets = list(filter(lambda asset: asset.name != name, self.assets))
        self._save_context_project_config()

    def _add_asset(self, asset: _DataAssetT, connect_options: dict | None = None) -> _DataAssetT:
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
            raise ValueError(  # noqa: TRY003
                f'"{asset.name}" already exists (all existing assets are {", ".join(asset_names)})'
            )

        self.assets.append(asset)

        # if asset was added to a cloud FDS, _update_fluent_datasource will return FDS fetched from cloud,  # noqa: E501
        # which will contain the new asset populated with an id
        if self._data_context:
            updated_datasource = self._data_context._update_fluent_datasource(datasource=self)
            assert isinstance(updated_datasource, Datasource)
            if asset_id := updated_datasource.get_asset(name=asset.name).id:
                asset.id = asset_id

        return asset

    def _save_context_project_config(self) -> None:
        """Check if a DataContext is available and save the project config."""
        if self._data_context:
            try:
                self._data_context._save_project_config()
            except TypeError as type_err:
                warnings.warn(str(type_err), GxSerializationWarning)

    def _rebuild_asset_data_connectors(self) -> None:
        """
        If Datasource required a data_connector we need to build the data_connector for each asset.

        A warning is raised if a data_connector cannot be built for an asset.
        Not all users will have access to the needed dependencies (packages or credentials) for every asset.
        Missing dependencies will stop them from using the asset but should not stop them from loading it from config.
        """  # noqa: E501
        asset_build_failure_direct_cause: dict[str, Exception | BaseException] = {}

        if self.data_connector_type:
            for data_asset in self.assets:
                try:
                    # check if data_connector exist before rebuilding?
                    connect_options = getattr(data_asset, "connect_options", {})
                    self._build_data_connector(data_asset, **connect_options)
                except Exception as dc_build_err:
                    logger.info(
                        f"Unable to build data_connector for {self.type} {data_asset.type} {data_asset.name}",  # noqa: E501
                        exc_info=True,
                    )
                    # reveal direct cause instead of generic, unhelpful MyDatasourceError
                    asset_build_failure_direct_cause[data_asset.name] = (
                        dc_build_err.__cause__ or dc_build_err
                    )
        if asset_build_failure_direct_cause:
            # TODO: allow users to opt out of these warnings
            names_and_error: List[str] = [
                f"{name}:{type(exc).__name__}"
                for (name, exc) in asset_build_failure_direct_cause.items()
            ]
            warnings.warn(
                f"data_connector build failure for {self.name} assets - {', '.join(names_and_error)}",  # noqa: E501
                category=RuntimeWarning,
            )

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
        """  # noqa: E501
        raise NotImplementedError(
            """One needs to implement "test_connection" on a Datasource subclass."""
        )

    def _build_data_connector(self, data_asset: _DataAssetT, **kwargs) -> None:
        """Any Datasource subclass that utilizes DataConnector should overwrite this method.

        Specific implementations instantiate appropriate DataConnector class and set "self._data_connector" to it.

        Args:
            data_asset: DataAsset using this DataConnector instance
            kwargs: Extra keyword arguments allow specification of arguments used by particular DataConnector subclasses
        """  # noqa: E501
        pass

    @classmethod
    def _get_exec_engine_excludes(cls) -> Set[str]:
        """
        Return a set of field names to exclude from the execution engine.

        All datasource fields are passed to the execution engine by default unless they are in this set.

        Default implementation is to return the combined set of field names from `_EXTRA_EXCLUDED_EXEC_ENG_ARGS`
        and `_BASE_DATASOURCE_FIELD_NAMES`.
        """  # noqa: E501
        return cls._EXTRA_EXCLUDED_EXEC_ENG_ARGS.union(_BASE_DATASOURCE_FIELD_NAMES)

    # End Abstract Methods


# This is used to prevent passing things like `type`, `assets` etc. to the execution engine
_BASE_DATASOURCE_FIELD_NAMES: Final[Set[str]] = {name for name in Datasource.__fields__}


@dataclasses.dataclass(frozen=True)
class HeadData:
    """
    An immutable wrapper around pd.DataFrame for .head() methods which
        are intended to be used for visual inspection of BatchData.
    """

    data: pd.DataFrame

    @override
    def __repr__(self) -> str:
        return self.data.__repr__()


@public_api
class Batch:
    """This represents a batch of data.

    This is usually not the data itself but a hook to the data on an external datastore such as
    a spark or a sql database. An exception exists for pandas or any in-memory datastore.
    """

    def __init__(  # noqa: PLR0913
        self,
        datasource: Datasource,
        data_asset: DataAsset,
        batch_request: BatchRequest,
        data: BatchData,
        batch_markers: BatchMarkers,
        batch_spec: BatchSpec,
        batch_definition: LegacyBatchDefinition,
        metadata: Dict[str, Any] | None = None,
    ):
        # Immutable attributes
        self._datasource = datasource
        self._data_asset = data_asset
        self._batch_request = batch_request
        self._data = data

        # Immutable legacy attributes
        # TODO: These legacy fields are required but we should figure out how to delete them
        self._batch_markers = batch_markers
        self._batch_spec = batch_spec
        self._batch_definition = batch_definition

        # Mutable Attribute
        # metadata is any arbitrary data one wants to associate with a batch. GX will add arbitrary metadata  # noqa: E501
        # to a batch so developers may want to namespace any custom metadata they add.
        self.metadata = metadata or {}

        # Immutable generated attribute
        self._id = self._create_id()

    def _create_id(self) -> str:
        options_list = []
        for key, value in self.batch_request.options.items():
            if key not in ("path", "dataframe"):
                options_list.append(f"{key}_{value}")
        return "-".join([self.datasource.name, self.data_asset.name, *options_list])

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
    def data(self) -> BatchData:
        return self._data

    @property
    def batch_markers(self) -> BatchMarkers:
        return self._batch_markers

    @property
    def batch_spec(self) -> BatchSpec:
        return self._batch_spec

    @property
    def batch_definition(self) -> LegacyBatchDefinition:
        return self._batch_definition

    @property
    def id(self) -> str:
        return self._id

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

    @overload
    def validate(
        self,
        expect: Expectation,
        *,
        result_format: ResultFormatUnion = DEFAULT_RESULT_FORMAT,
        expectation_parameters: Optional[SuiteParameterDict] = None,
    ) -> ExpectationValidationResult: ...

    @overload
    def validate(
        self,
        expect: ExpectationSuite,
        *,
        result_format: ResultFormatUnion = DEFAULT_RESULT_FORMAT,
        expectation_parameters: Optional[SuiteParameterDict] = None,
    ) -> ExpectationSuiteValidationResult: ...

    @public_api
    def validate(
        self,
        expect: Expectation | ExpectationSuite,
        *,
        result_format: ResultFormatUnion = DEFAULT_RESULT_FORMAT,
        expectation_parameters: Optional[SuiteParameterDict] = None,
    ) -> ExpectationValidationResult | ExpectationSuiteValidationResult:
        from great_expectations.core import ExpectationSuite
        from great_expectations.expectations.expectation import Expectation

        if isinstance(expect, Expectation):
            return self._validate_expectation(
                expect, result_format=result_format, expectation_parameters=expectation_parameters
            )
        elif isinstance(expect, ExpectationSuite):
            return self._validate_expectation_suite(
                expect, result_format=result_format, expectation_parameters=expectation_parameters
            )
        else:
            # If we are type checking, we should never fall through to this case. However, exploratory  # noqa: E501
            # workflows are not being type checked.
            raise ValueError(  # noqa: TRY003, TRY004
                f"Trying to validate something that isn't an Expectation or an ExpectationSuite: {expect}"  # noqa: E501
            )

    def _validate_expectation(
        self,
        expect: Expectation,
        result_format: ResultFormatUnion,
        expectation_parameters: Optional[SuiteParameterDict] = None,
    ) -> ExpectationValidationResult:
        return self._create_validator(
            result_format=result_format,
        ).validate_expectation(expectation=expect, expectation_parameters=expectation_parameters)

    def _validate_expectation_suite(
        self,
        expect: ExpectationSuite,
        result_format: ResultFormatUnion,
        expectation_parameters: Optional[SuiteParameterDict] = None,
    ) -> ExpectationSuiteValidationResult:
        return self._create_validator(
            result_format=result_format,
        ).validate_expectation_suite(
            expectation_suite=expect, expectation_parameters=expectation_parameters
        )

    def _create_validator(self, *, result_format: ResultFormatUnion) -> V1Validator:
        from great_expectations.validator.v1_validator import Validator as V1Validator

        context = self.datasource.data_context
        if context is None:
            raise ValueError(  # noqa: TRY003
                "We can't validate batches that are attached to datasources without a data context"
            )

        # note: batch definition is created but NOT added to the asset, as it should not persist
        batch_definition = BatchDefinition(
            name="-".join([self.datasource.name, self.data_asset.name, str(uuid.uuid4())]),
            partitioner=self.batch_request.partitioner,
        )
        batch_definition.set_data_asset(self.data_asset)

        return V1Validator(
            batch_definition=batch_definition,
            batch_parameters=self.batch_request.options,
            result_format=result_format,
        )
