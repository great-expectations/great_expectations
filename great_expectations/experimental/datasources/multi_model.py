from __future__ import annotations

import dataclasses
import functools
import itertools
import logging
from pprint import pformat as pf
from typing import (
    TYPE_CHECKING,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Set,
    Type,
    cast,
    overload,
)

import pydantic
from typing_extensions import ClassVar, Literal, Protocol, runtime_checkable

from great_expectations.core.batch_spec import SqlAlchemyDatasourceBatchSpec
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.experimental.datasources.experimental_base_model import (
    ExperimentalBaseModel,
)
from great_expectations.experimental.datasources.sources import _SourceFactories

if TYPE_CHECKING:
    from great_expectations.execution_engine import SqlAlchemyExecutionEngine
    from great_expectations.experimental.datasources.interfaces import (
        BatchRequest,
        BatchRequestOptions,
    )
    from great_expectations.experimental.datasources.postgres_datasource import (
        _DEFAULT_MONTH_RANGE,
        _DEFAULT_YEAR_RANGE,
        Batch,
        BatchRequestError,
        ColumnSplitter,
    )

# logging.basicConfig(level=logging.DEBUG)  # TODO: remove me
LOGGER = logging.getLogger(__name__)


class DataAssetConfig(ExperimentalBaseModel):
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


class DataAsset(Protocol):
    name: str
    datasource: Datasource
    config: DataAssetConfig

    def __init__(self, config: DataAssetConfig, datasource: Datasource) -> None:
        if isinstance(config, DataAssetConfig):
            self.config = config
        else:
            raise TypeError(
                f"Expected {DataAssetConfig.__name__} got {type(config).__name__}"
            )
        self.datasource = datasource

    def get_batch_request(self, options: Optional[BatchRequestOptions]) -> BatchRequest:
        raise NotImplementedError


class DatasourceConfig(
    ExperimentalBaseModel,
    # metaclass=SomeDifferentMetaclass,
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
    assets: Mapping[str, DataAssetConfig] = {}
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


@runtime_checkable
class Datasource(Protocol):
    # instance attrs
    name: str
    assets: Dict[str, DataAsset]
    execution_engine: ExecutionEngine
    config: DatasourceConfig

    @overload
    def __init__(
        self,
        config: DatasourceConfig,
    ) -> None:
        ...

    @overload
    def __init__(
        self,
        config: None = ...,
        type: str = ...,
        name: str = ...,
        execution_engine: ExecutionEngine = ...,
        assets: Optional[Dict[str, DataAssetConfig]] = ...,
        **kwargs,
    ) -> None:
        ...

    def __init__(
        self,
        config: Optional[DatasourceConfig] = None,
        type: Optional[str] = None,
        name: Optional[str] = None,
        execution_engine: Optional[ExecutionEngine] = None,
        assets: Optional[Dict[str, DataAssetConfig]] = None,
        **kwargs,
    ) -> None:
        """
        Config validation errors are surfaced when trying to initialize the `DatasourceConfig`.
        This logic can be simplified if we only accept either a `DatasourceConfig` or keyword arguments but not both.
        """
        if isinstance(config, DatasourceConfig):
            self.config = config
        else:
            self.config = DatasourceConfig(
                type=type,
                name=name,
                assets=assets,
                execution_engine=execution_engine,
                **kwargs,
            )

        self.name = self.config.name
        self.assets: Dict[str, DataAsset] = {}

        # convert the DataAsset configs into runtime objects
        for asset_name, asset_config in self.config.assets.items():
            self.assets[asset_name] = self.asset_factory(asset_config, self)  # type: ignore[arg-type] # mypy confused about singledispatch instance method?

    @functools.singledispatch
    def asset_factory(self, asset_config: DataAssetConfig) -> DataAsset:
        """Creates runtime asset objects from asset config models."""
        raise NotImplementedError("No single dispatch functions have been registered")

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


# ######################################################################################
# Postgres
# ######################################################################################


class TableAssetConfig(DataAssetConfig):
    type: Literal["table"] = "table"
    name: str
    table_name: str
    column_splitter: Optional[ColumnSplitter] = None


class TableAsset(DataAsset):
    name: str
    table_name: str
    column_splitter: Optional[ColumnSplitter]

    datasource: PostgresDatasource
    config: TableAssetConfig

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


class PostgresDatasourceConfig(DatasourceConfig):
    type: Literal["postgres"] = "postgres"
    assets: Dict[str, TableAssetConfig]


# Assets must register an asset factory
# Maybe this is handled by a new/updated Metaclass
@Datasource.asset_factory.register(TableAssetConfig)
def _table_asset_factory(  # type: ignore[misc] # mypy confused about self & singledispatch?
    self: PostgresDatasource,
    asset_config: TableAssetConfig,
) -> TableAsset:
    return TableAsset(config=asset_config, datasource=self)


class PostgresDatasource(Datasource):
    # instance attrs
    name: str
    assets: Dict[str, TableAsset]  # type: ignore[assignment]
    execution_engine: SqlAlchemyExecutionEngine
    config: DatasourceConfig

    def add_table_asset(self, config: TableAssetConfig) -> TableAsset:
        """Adds a table asset to this datasource.

        Args:
            name: The name of this table asset.
            table_name: The table where the data resides.

        Returns:
            The TableAsset that is added to the datasource.
        """
        asset = TableAsset(config=config, datasource=self)
        self.assets[config.name] = asset
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
                    datasource=self,  # type: ignore[arg-type]
                    data_asset=data_asset,  # type: ignore[arg-type]
                    batch_request=request,
                    data=data,
                )
            )
        return batch_list
