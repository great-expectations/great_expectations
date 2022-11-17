from __future__ import annotations

import dataclasses
import itertools
import logging
from datetime import datetime
from pprint import pformat as pf
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    MutableMapping,
    Optional,
    Set,
    Type,
    Union,
    cast,
)

import dateutil.tz
import pydantic
from pydantic import Field
from pydantic import dataclasses as pydantic_dc
from typing_extensions import ClassVar, Literal, Protocol, TypeAlias, runtime_checkable

from great_expectations.core.batch import BatchDataType
from great_expectations.core.batch_spec import SqlAlchemyDatasourceBatchSpec
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.zep.type_lookup import TypeLookup

if TYPE_CHECKING:
    from great_expectations.data_context import DataContext as GXDataContext
    from great_expectations.experimental.context import DataContext


logging.basicConfig(level=logging.DEBUG)  # TODO: remove me
LOGGER = logging.getLogger(__name__)


SourceFactoryFn = Callable[..., "Datasource"]


class TypeRegistrationError(TypeError):
    pass


class _SourceFactories:
    """
    Contains a collection of datasource factory methods in the format `.add_<TYPE_NAME>()`

    Contains a `.type_lookup` dict-like two way mapping between previously registered `Datasource`
    or `DataAsset` types and a simplified name for those types.
    """

    # TODO (kilo59): split DataAsset & Datasource lookups
    type_lookup: ClassVar = TypeLookup()
    __source_factories: ClassVar[Dict[str, SourceFactoryFn]] = {}

    _data_context: Union[DataContext, GXDataContext]

    def __init__(self, data_context: Union[DataContext, GXDataContext]):
        self._data_context = data_context

    @classmethod
    def register_types_and_ds_factory(
        cls,
        ds_type: Type[Datasource],
        factory_fn: SourceFactoryFn,
    ) -> None:
        """
        Add/Register a datasource factory function and all related `Datasource`,
        `DataAsset` and `ExecutionEngine` types.

        Creates mapping table between the `DataSource`/`DataAsset` classes and their
        declared `type` string.


        Example
        -------

        An `.add_pandas()` pandas factory method will be added to `context.sources`.

        >>> class PandasDatasource(Datasource):
        >>>     type: str = 'pandas'`
        >>>     asset_types = [FileAsset]
        >>>     execution_engine: PandasExecutionEngine
        """

        # TODO: check that the name is a valid python identifier (and maybe that it is snake_case?)
        ds_type_name = ds_type.__fields__["type"].default
        if not ds_type_name:
            raise TypeRegistrationError(
                f"`{ds_type.__name__}` is missing a `type` attribute with an assigned string value"
            )

        # rollback type registrations if exception occurs
        with cls.type_lookup.transaction() as type_lookup:

            # TODO: We should namespace the asset type to the datasource so different datasources can reuse asset types.
            cls._register_assets(ds_type, asset_type_lookup=type_lookup)

            cls._register_datasource_and_factory_method(
                ds_type,
                factory_fn=factory_fn,
                ds_type_name=ds_type_name,
                datasource_type_lookup=type_lookup,
            )

    @classmethod
    def _register_datasource_and_factory_method(
        cls,
        ds_type: Type[Datasource],
        factory_fn: SourceFactoryFn,
        ds_type_name: str,
        datasource_type_lookup: TypeLookup,
    ) -> str:
        """
        Register the `Datasource` class and add a factory method for the class on `sources`.
        The method name is pulled from the `Datasource.type` attribute.
        """
        method_name = f"add_{ds_type_name}"
        LOGGER.info(
            f"2a. Registering {ds_type.__name__} as {ds_type_name} with {method_name}() factory"
        )

        pre_existing = cls.__source_factories.get(method_name)
        if pre_existing:
            raise TypeRegistrationError(
                f"'{ds_type_name}' - `sources.{method_name}()` factory already exists",
            )

        datasource_type_lookup[ds_type] = ds_type_name
        LOGGER.info(f"'{ds_type_name}' added to `type_lookup`")
        cls.__source_factories[method_name] = factory_fn
        return ds_type_name

    @classmethod
    def _register_assets(cls, ds_type: Type[Datasource], asset_type_lookup: TypeLookup):

        asset_types: List[Type[DataAsset]] = ds_type.asset_types

        if not asset_types:
            LOGGER.warning(
                f"No `{ds_type.__name__}.asset_types` have be declared for the `Datasource`"
            )

        for t in asset_types:
            try:
                asset_type_name = t.__fields__["type"].default
                if asset_type_name is None:
                    raise TypeError(
                        f"{t.__name__} `type` field must be assigned and cannot be `None`"
                    )
                LOGGER.info(
                    f"2b. Registering `DataAsset` `{t.__name__}` as {asset_type_name}"
                )
                asset_type_lookup[t] = asset_type_name
            except (AttributeError, KeyError, TypeError) as bad_field_exc:
                raise TypeRegistrationError(
                    f"No `type` field found for `{ds_type.__name__}.asset_types` -> `{t.__name__}` unable to register asset type",
                ) from bad_field_exc

    @property
    def factories(self) -> List[str]:
        return list(self.__source_factories.keys())

    def __getattr__(self, attr_name: str):
        try:
            ds_constructor = self.__source_factories[attr_name]

            def wrapped(name: str, **kwargs):
                datasource = ds_constructor(name=name, **kwargs)
                # TODO (bdirks): _attach_datasource_to_context to the AbstractDataContext class
                self._data_context._attach_datasource_to_context(datasource)
                return datasource

            return wrapped
        except KeyError:
            raise AttributeError(f"No factory {attr_name} in {self.factories}")

    def __dir__(self) -> List[str]:
        """Preserves autocompletion for dynamic attributes."""
        return [*self.factories, *super().__dir__()]


class MetaDatasource(pydantic.main.ModelMetaclass):

    __cls_set: Set[Type] = set()

    def __new__(
        meta_cls: Type[MetaDatasource], cls_name: str, bases: tuple[type], cls_dict
    ) -> MetaDatasource:
        """
        MetaDatasource hook that runs when a new `Datasource` is defined.
        This methods binds a factory method for the defined `Datasource` to `_SourceFactories` class which becomes
        available as part of the `DataContext`.

        Also binds asset adding methods according to the declared `asset_types`.
        """
        LOGGER.debug(f"1a. {meta_cls.__name__}.__new__() for `{cls_name}`")

        cls = super().__new__(meta_cls, cls_name, bases, cls_dict)

        if cls_name == "Datasource":
            # NOTE: the above check is brittle and must be kept in-line with the Datasource.__name__
            LOGGER.debug("1c. Skip factory registration of base `Datasource`")
            return cls

        LOGGER.debug(f"  {cls_name} __dict__ ->\n{pf(cls.__dict__, depth=3)}")

        meta_cls.__cls_set.add(cls)
        LOGGER.info(f"Datasources: {len(meta_cls.__cls_set)}")

        def _datasource_factory(name: str, **kwargs) -> Datasource:
            # TODO: update signature to match Datasource __init__ (ex update __signature__)
            LOGGER.info(f"5. Adding '{name}' {cls_name}")
            return cls(name=name, **kwargs)

        # TODO: generate schemas from `cls` if needed

        if cls.__module__ == "__main__":
            LOGGER.warning(
                f"Datasource `{cls_name}` should not be defined as part of __main__ this may cause typing lookup collisions"
            )
        _SourceFactories.register_types_and_ds_factory(cls, _datasource_factory)

        return cls


# BatchRequestOptions is a dict that is composed into a BatchRequest that specifies the
# Batches one wants returned. The keys represent dimensions one can slice the data along
# and the values are the realized. If a value is None or unspecified, the batch_request
# will capture all data along this dimension. For example, if we have a year and month
# splitter and we want to query all months in the year 2020, the batch request options
# would look like:
#   options = { "year": 2020 }
BatchRequestOptions: TypeAlias = Dict[str, Any]


class ZepBaseModel(pydantic.BaseModel):
    class Config:
        extra = pydantic.Extra.forbid


@dataclasses.dataclass(frozen=True)
class BatchRequest:
    datasource_name: str
    data_asset_name: str
    options: BatchRequestOptions


class DataAsset(ZepBaseModel):
    name: str
    type: str

    # non-field private attrs
    _datasource: Datasource = pydantic.PrivateAttr()

    @property
    def datasource(self) -> Datasource:
        return self._datasource

    # TODO (kilo): remove setter and add custom init for DataAsset to inject datasource in constructor??
    @datasource.setter
    def datasource(self, ds: Datasource):
        assert isinstance(ds, Datasource)
        self._datasource = ds

    def get_batch_request(self, options: Optional[BatchRequestOptions]) -> BatchRequest:
        raise NotImplementedError


class Datasource(ZepBaseModel, metaclass=MetaDatasource):

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
    assets: Mapping[str, DataAsset] = {}
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
        loaded_assets: Dict[str, DataAsset] = {}

        # TODO (kilo59): catch key errors
        for asset_name, config in v.items():
            asset_type_name: str = config["type"]
            asset_type: Type[DataAsset] = _SourceFactories.type_lookup[asset_type_name]
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
        """Processes a batch request and returns a list of batches.
        Args:
            batch_request: contains parameters necessary to retrieve batches.
        Returns:
            A list of batches. The list may be empty.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement `.get_batch_list_from_batch_request()`"
        )

    def get_asset(self, asset_name: str) -> DataAsset:
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

    def __init__(
        self,
        datasource: Datasource,
        data_asset: DataAsset,
        batch_request: BatchRequest,
        # BatchDataType is Union[core.batch.BatchData, pd.DataFrame, SparkDataFrame].  core.batch.Batchdata is the
        # implicit interface that Datasource implementers can use. We can make this explicit if needed.
        data: BatchDataType,
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

        # computed property
        # We need to unique identifier. This will likely change as I get more input
        self._id: str = "-".join([datasource.name, data_asset.name, str(batch_request)])

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


# ######################################################################################
# Postgres
# ######################################################################################


class PostgresDatasourceError(Exception):
    pass


class BatchRequestError(Exception):
    pass


# For our year splitter we default the range to the last 2 year.
_CURRENT_YEAR = datetime.now(dateutil.tz.tzutc()).year
_DEFAULT_YEAR_RANGE = range(_CURRENT_YEAR - 1, _CURRENT_YEAR + 1)
_DEFAULT_MONTH_RANGE = range(1, 13)


@pydantic_dc.dataclass(frozen=True)
class ColumnSplitter:
    method_name: str
    column_name: str
    # param_defaults is a Dict where the keys are the parameters of the splitter and the values are the default
    # values are the default values if a batch request using the splitter leaves the parameter unspecified.
    # template_params: List[str]
    param_defaults: Dict[str, Iterable] = Field(default_factory=dict)

    @property
    def param_names(self) -> List[str]:
        return list(self.param_defaults.keys())


class TableAsset(DataAsset):
    # Instance fields
    type: Literal["table"] = "table"
    table_name: str
    column_splitter: Optional[ColumnSplitter] = None
    name: str

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
            datasource_name=self._datasource.name,
            data_asset_name=self.name,
            options=options or {},
        )

    def _valid_batch_request_options(self, options: BatchRequestOptions) -> bool:
        return set(options.keys()).issubset(
            set(self.batch_request_options_template().keys())
        )

    def validate_batch_request(self, batch_request: BatchRequest) -> None:
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

    # This asset type will support a variety of splitters
    def add_year_and_month_splitter(
        self,
        column_name: str,
        default_year_range: Iterable[int] = _DEFAULT_YEAR_RANGE,
        default_month_range: Iterable[int] = _DEFAULT_MONTH_RANGE,
    ) -> TableAsset:
        """Associates a year month splitter with this DataAsset

        Args:
            column_name: A column name of the date column where year and month will be parsed out.
            default_year_range: When this splitter is used, say in a BatchRequest, if no value for
                year is specified, we query over all years in this range.
                will query over all the years in this default range.
            default_month_range: When this splitter is used, say in a BatchRequest, if no value for
                month is specified, we query over all months in this range.

        Returns:
            This TableAsset so we can use this method fluently.
        """
        self.column_splitter = ColumnSplitter(
            method_name="split_on_year_and_month",
            column_name=column_name,
            param_defaults={"year": default_year_range, "month": default_month_range},
        )
        return self

    def fully_specified_batch_requests(self, batch_request) -> List[BatchRequest]:
        """Populates a batch requests unspecified params producing a list of batch requests

        This method does NOT validate the batch_request. If necessary call
        TableAsset.validate_batch_request before calling this method.
        """
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
                    self.column_splitter.param_defaults[option]
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


class PostgresDatasource(Datasource):
    # class var definitions
    asset_types: ClassVar[List[Type[DataAsset]]] = [TableAsset]

    # right side of the operator determines the type name
    # left side enforces the names on instance creation
    type: Literal["postgres"] = "postgres"
    connection_string: str
    assets: Dict[str, TableAsset] = {}

    def execution_engine_type(self) -> Type[ExecutionEngine]:
        """Returns the default execution engine type."""
        from great_expectations.execution_engine import SqlAlchemyExecutionEngine

        return SqlAlchemyExecutionEngine

    def add_table_asset(self, name: str, table_name: str) -> TableAsset:
        """Adds a table asset to this datasource.

        Args:
            name: The name of this table asset.
            table_name: The table where the data resides.

        Returns:
            The TableAsset that is added to the datasource.
        """
        asset = TableAsset(name=name, table_name=table_name)
        # TODO (kilo59): custom init for `DataAsset` to accept datasource in constructor?
        # Will most DataAssets require a `Datasource` attribute?
        asset._datasource = self
        self.assets[name] = asset
        return asset

    def get_asset(self, asset_name: str) -> TableAsset:
        """Returns the TableAsset referred to by name"""
        return super().get_asset(asset_name)  # type: ignore[return-value] # value is subclass

    # When we have multiple types of DataAssets on a datasource, the batch_request argument will be a Union type.
    # To differentiate we could use single dispatch or use an if/else (note pattern matching doesn't appear until
    # python 3.10)
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
        data_asset.validate_batch_request(batch_request)

        batch_list: List[Batch] = []
        column_splitter = data_asset.column_splitter
        for request in data_asset.fully_specified_batch_requests(batch_request):
            batch_spec_kwargs = {
                "type": "table",
                "data_asset_name": data_asset.name,
                "table_name": data_asset.table_name,
                "batch_identifiers": {},
            }
            if column_splitter:
                batch_spec_kwargs["splitter_method"] = column_splitter.method_name
                batch_spec_kwargs["splitter_kwargs"] = {
                    "column_name": column_splitter.column_name
                }
                # mypy infers that batch_spec_kwargs["batch_identifiers"] is a collection, but
                # it is hardcoded to a dict above, so we cast it here.
                cast(Dict, batch_spec_kwargs["batch_identifiers"]).update(
                    {column_splitter.column_name: request.options}
                )
            data, _ = self.execution_engine.get_batch_data_and_markers(
                batch_spec=SqlAlchemyDatasourceBatchSpec(**batch_spec_kwargs)
            )
            batch_list.append(
                Batch(
                    datasource=self,
                    data_asset=data_asset,
                    batch_request=request,
                    data=data,
                )
            )
        return batch_list
