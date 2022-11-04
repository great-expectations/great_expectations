# import abc
import logging
from pprint import pformat as pf
from typing import Dict, List, Type, Union

from pydantic import BaseModel, confloat, constr, root_validator, validator
from typing_extensions import ClassVar, TypeAlias

from great_expectations.core.batch import Batch, BatchRequest, RuntimeBatchRequest
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.zep.metadatasource import MetaDatasource
from great_expectations.zep.sources import _SourceFactories

LOGGER = logging.getLogger(__name__)

LowerStr: TypeAlias = constr(to_lower=True, strict=True)  # type: ignore[misc]


class DataAsset(BaseModel):
    name: str
    type: str


class DatasourceCfg(BaseModel):
    name: str
    engine: str
    version: confloat(ge=2.0, lt=3.0) = 2.0
    execution_engine: ExecutionEngine
    assets: Dict[str, DataAsset]

    # TODO (kil59): remove name duplication
    #   1. user a root_validator to pull the name for the `assets` keys
    #   2. Update the structure to be a list of `DataAssets`
    # TODO: Instantiate a real Datasource. Execution engine is created in the Datasource init method

    @root_validator(pre=True)
    @classmethod
    def _load_execution_engine(cls, values: dict):
        """
        Lookup and instantiate an ExecutionEngine based on the 'engine' string.
        Assign this ExecutionEngine instance to the `execution_engine` field.
        """
        # NOTE (kilo59): this method is only ever called by the Pydantic framework.
        # Should we use name mangling? `__load_execution_engine`?
        LOGGER.info(
            f"Loading & validating `Datasource.execution_engine'\n {pf(values, depth=1)}"
        )
        # TODO (kilo59): catch key errors
        engine_name: str = values["engine"]
        engine_type: Type[ExecutionEngine] = _SourceFactories.engine_lookup[engine_name]
        # datasource type
        values["execution_engine"] = engine_type()
        return values

    @validator("assets", pre=True)
    @classmethod
    def _load_asset_subtype(cls, v: Dict[str, dict]):
        LOGGER.info(f"Loading 'assets'\n{pf(v, depth=3)} ->")
        loaded_assets: Dict[str, DataAsset] = {}

        # TODO (kilo59): catch key errors
        for asset_name, config in v.items():
            asset_type_name: str = config["type"]
            asset_type: Type[DataAsset] = _SourceFactories.type_lookup[asset_type_name]
            LOGGER.debug(f"Instantiating '{asset_type_name}' as {asset_type}")
            loaded_assets[asset_name] = asset_type(**config)

        LOGGER.info(f"Loaded 'assets' ->\n{pf(loaded_assets)}")
        return loaded_assets

    class Config:
        # TODO: revisit this (1 option - define __get_validator__ on ExecutionEngine)
        # https://pydantic-docs.helpmanual.io/usage/types/#custom-data-types
        arbitrary_types_allowed = True

    def get_asset(self, asset_name: str) -> DataAsset:
        """Returns the DataAsset referred to by name"""
        # This default implementation will be used if protocol is inherited
        return self.assets[asset_name]


# TODO: resolve metaclass conflict with pydantic BaseModel
class Datasource(metaclass=MetaDatasource):
    # class attrs
    asset_types: ClassVar[List[Type[DataAsset]]] = []

    # instance attrs
    name: str
    execution_engine: ExecutionEngine
    assets: Dict[str, DataAsset]

    def get_batch_list_from_batch_request(
        self, batch_request: Union[BatchRequest, RuntimeBatchRequest]
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
        return self.assets[asset_name]
