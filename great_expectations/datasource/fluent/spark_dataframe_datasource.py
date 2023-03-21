from __future__ import annotations

import copy
import logging
from typing import (
    TYPE_CHECKING,
    ClassVar,
    Dict,
    Generic,
    List,
    Type,
    TypeVar,
)

import pydantic
from typing_extensions import Literal

from great_expectations.core.batch_spec import RuntimeDataBatchSpec
from great_expectations.datasource.fluent import _SparkDatasource
from great_expectations.datasource.fluent.constants import (
    _DATA_CONNECTOR_NAME,
)
from great_expectations.datasource.fluent.interfaces import (
    Batch,
    BatchRequest,
    DataAsset,
)

if TYPE_CHECKING:
    from great_expectations.execution_engine.sparkdf_execution_engine import (
        SparkDFExecutionEngine,
    )


logger = logging.getLogger(__name__)


try:
    import pyspark
except ImportError:
    pyspark = None  # type: ignore[assignment]

    logger.debug(
        "Unable to load pyspark; install optional spark dependency for support."
    )

# this enables us to include dataframe in the json schema
_SparkDataFrameT = TypeVar("_SparkDataFrameT")


class DataFrameAsset(DataAsset, Generic[_SparkDataFrameT]):
    # instance attributes
    type: Literal["dataframe"] = "dataframe"
    dataframe: _SparkDataFrameT = pydantic.Field(..., exclude=True, repr=False)

    class Config:
        extra = pydantic.Extra.forbid

    @pydantic.validator("dataframe")
    def _validate_dataframe(
        cls, dataframe: pyspark.sql.DataFrame
    ) -> pyspark.sql.DataFrame:
        if not isinstance(dataframe, pyspark.sql.DataFrame):
            raise ValueError("dataframe must be of type pyspark.sql.DataFrame")

        return dataframe

    def _get_reader_method(self) -> str:
        raise NotImplementedError(
            """Spark DataFrameAsset does not implement "_get_reader_method()" method, because DataFrame is already available."""
        )

    def _get_reader_options_include(self) -> set[str] | None:
        raise NotImplementedError(
            """Spark DataFrameAsset does not implement "_get_reader_options_include()" method, because DataFrame is already available."""
        )

    def get_batch_list_from_batch_request(
        self, batch_request: BatchRequest
    ) -> list[Batch]:
        self._validate_batch_request(batch_request)

        batch_spec = RuntimeDataBatchSpec(batch_data=self.dataframe)
        execution_engine: SparkDFExecutionEngine = (
            self.datasource.get_execution_engine()
        )
        data, markers = execution_engine.get_batch_data_and_markers(
            batch_spec=batch_spec
        )

        # batch_definition (along with batch_spec and markers) is only here to satisfy a
        # legacy constraint when computing usage statistics in a validator. We hope to remove
        # it in the future.
        # imports are done inline to prevent a circular dependency with core/batch.py
        from great_expectations.core import IDDict
        from great_expectations.core.batch import BatchDefinition

        batch_definition = BatchDefinition(
            datasource_name=self.datasource.name,
            data_connector_name=_DATA_CONNECTOR_NAME,
            data_asset_name=self.name,
            batch_identifiers=IDDict(batch_request.options),
            batch_spec_passthrough=None,
        )

        batch_metadata = copy.deepcopy(batch_request.options)

        # Some pydantic annotations are postponed due to circular imports.
        # Batch.update_forward_refs() will set the annotations before we
        # instantiate the Batch class since we can import them in this scope.
        Batch.update_forward_refs()

        return [
            Batch(
                datasource=self.datasource,
                data_asset=self,
                batch_request=batch_request,
                data=data,
                metadata=batch_metadata,
                legacy_batch_markers=markers,
                legacy_batch_spec=batch_spec,
                legacy_batch_definition=batch_definition,
            )
        ]


class SparkDataframeDatasource(_SparkDatasource):
    # class attributes
    asset_types: ClassVar[List[Type[DataAsset]]] = [DataFrameAsset]

    # instance attributes
    type: Literal["spark"] = "spark"

    assets: Dict[str, DataFrameAsset] = {}  # type: ignore[assignment]

    def add_dataframe_asset(
        self, name: str, dataframe: pyspark.sql.DataFrame
    ) -> DataFrameAsset:
        asset = DataFrameAsset(
            name=name,
            dataframe=dataframe,
        )
        return self.add_asset(asset=asset)
