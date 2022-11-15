from __future__ import annotations

import dataclasses
import logging
from pprint import pformat as pf
from typing import Any, Dict, List, Mapping, Optional, Set, Type

import pydantic
from typing_extensions import ClassVar, TypeAlias

from great_expectations.core.batch import BatchDataType
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.zep.metadatasource import MetaDatasource
from great_expectations.zep.sources import _SourceFactories

LOGGER = logging.getLogger(__name__)

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

    # instance attrs
    type: str
    name: str
    execution_engine: ExecutionEngine
    assets: Mapping[str, DataAsset] = {}

    class Config:
        # TODO: revisit this (1 option - define __get_validator__ on ExecutionEngine)
        # https://pydantic-docs.helpmanual.io/usage/types/#custom-data-types
        arbitrary_types_allowed = True

    @pydantic.root_validator(pre=True)
    @classmethod
    def _load_execution_engine(cls, values: Dict[str, Any]):
        """
        Lookup and instantiate an ExecutionEngine based on the 'type' string of the datasource.
        Assign this ExecutionEngine instance to the `execution_engine` field.
        """
        # NOTE (kilo59): this method is only ever called by the Pydantic framework.
        # Should we use name mangling? `__load_execution_engine`?
        LOGGER.info(
            f"Selecting & instantiating `Datasource.execution_engine' ->\n {pf(values, depth=1)}"
        )
        # TODO (kilo59): catch key errors
        ds_type_default = cls.__fields__["type"].default
        registered_ds_type_name: str = values.get("type", ds_type_default)

        engine_type: Type[ExecutionEngine] = _SourceFactories.engine_lookup[
            registered_ds_type_name
        ]

        engine_kwargs = {
            k: v for (k, v) in values.items() if k not in cls._excluded_eng_args
        }
        LOGGER.debug(f"{engine_type} - kwargs: {list(engine_kwargs.keys())}")
        engine = engine_type(**engine_kwargs)
        values["execution_engine"] = engine
        LOGGER.info(f"{registered_ds_type_name} - {engine_type.__name__} - {engine}")
        return values

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
