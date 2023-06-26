from __future__ import annotations

import logging
from pprint import pformat as pf
from typing import (
    TYPE_CHECKING,
    ClassVar,
    Dict,
    Generic,
    List,
    Literal,
    Optional,
    Type,
    TypeVar,
    Union,
)

import pydantic
from pydantic import StrictBool, StrictFloat, StrictInt, StrictStr

import great_expectations.exceptions as gx_exceptions
from great_expectations.compatibility.pyspark import DataFrame, pyspark
from great_expectations.core._docs_decorators import (
    deprecated_argument,
    new_argument,
    public_api,
)
from great_expectations.core.batch_spec import RuntimeDataBatchSpec
from great_expectations.datasource.fluent import BatchRequest
from great_expectations.datasource.fluent.constants import (
    _DATA_CONNECTOR_NAME,
)
from great_expectations.datasource.fluent.interfaces import (
    Batch,
    DataAsset,
    Datasource,
    _DataAssetT,
)

if TYPE_CHECKING:
    from typing_extensions import TypeAlias

    from great_expectations.datasource.fluent.interfaces import BatchMetadata
    from great_expectations.execution_engine import SparkDFExecutionEngine


logger = logging.getLogger(__name__)


# this enables us to include dataframe in the json schema
_SparkDataFrameT = TypeVar("_SparkDataFrameT")

SparkConfig: TypeAlias = Dict[
    StrictStr, Union[StrictStr, StrictInt, StrictFloat, StrictBool]
]


class SparkDatasourceError(Exception):
    pass


class _SparkDatasource(Datasource):
    # instance attributes
    spark_config: Union[SparkConfig, None] = None
    force_reuse_spark_context: bool = True

    @staticmethod
    def _update_asset_forward_refs(asset_type: Type[_DataAssetT]) -> None:
        # Only update forward refs if pyspark types are available.
        if pyspark:
            asset_type.update_forward_refs()

    # Abstract Methods
    @property
    def execution_engine_type(self) -> Type[SparkDFExecutionEngine]:
        """Return the SparkDFExecutionEngine unless the override is set"""
        from great_expectations.execution_engine.sparkdf_execution_engine import (
            SparkDFExecutionEngine,
        )

        return SparkDFExecutionEngine

    def test_connection(self, test_assets: bool = True) -> None:
        """Test the connection for the _SparkDatasource.

        Args:
            test_assets: If assets have been passed to the _SparkDatasource,
                         an attempt can be made to test them as well.

        Raises:
            TestConnectionError: If the connection test fails.
        """
        raise NotImplementedError(
            """One needs to implement "test_connection" on a _SparkDatasource subclass."""
        )

    # End Abstract Methods


class DataFrameAsset(DataAsset, Generic[_SparkDataFrameT]):
    # instance attributes
    type: Literal["dataframe"] = "dataframe"
    # TODO: <Alex>05/31/2023: Upon removal of deprecated "dataframe" argument to "PandasDatasource.add_dataframe_asset()", default can be deleted.</Alex>
    dataframe: Optional[_SparkDataFrameT] = pydantic.Field(
        default=None, exclude=True, repr=False
    )

    class Config:
        extra = pydantic.Extra.forbid

    @pydantic.validator("dataframe")
    def _validate_dataframe(cls, dataframe: DataFrame) -> DataFrame:
        if not (DataFrame and isinstance(dataframe, DataFrame)):  # type: ignore[truthy-function]
            raise ValueError("dataframe must be of type pyspark.sql.DataFrame")

        return dataframe

    def test_connection(self) -> None:
        ...

    @property
    def batch_request_options(self) -> tuple[str, ...]:
        return tuple()

    def _get_reader_method(self) -> str:
        raise NotImplementedError(
            """Spark DataFrameAsset does not implement "_get_reader_method()" method, because DataFrame is already available."""
        )

    def _get_reader_options_include(self) -> set[str]:
        raise NotImplementedError(
            """Spark DataFrameAsset does not implement "_get_reader_options_include()" method, because DataFrame is already available."""
        )

    @public_api
    # TODO: <Alex>05/31/2023: Upon removal of deprecated "dataframe" argument to "PandasDatasource.add_dataframe_asset()", its validation code must be deleted.</Alex>
    @new_argument(
        argument_name="dataframe",
        message='The "dataframe" argument is no longer part of "PandasDatasource.add_dataframe_asset()" method call; instead, "dataframe" is the required argument to "DataFrameAsset.build_batch_request()" method.',
        version="0.16.15",
    )
    def build_batch_request(
        self, dataframe: Optional[_SparkDataFrameT] = None
    ) -> BatchRequest:
        """A batch request that can be used to obtain batches for this DataAsset.

        Args:
            dataframe: The Spark Dataframe containing the data for this DataFrame data asset.

        Returns:
            A BatchRequest object that can be used to obtain a batch list from a Datasource by calling the
            get_batch_list_from_batch_request method.
        """
        if dataframe is None:
            df = self.dataframe
        else:
            df = dataframe

        if df is None:
            raise ValueError(
                "Cannot build batch request for dataframe asset without a dataframe"
            )

        self.dataframe = df

        return BatchRequest(
            datasource_name=self.datasource.name,
            data_asset_name=self.name,
            options={},
        )

    def _validate_batch_request(self, batch_request: BatchRequest) -> None:
        """Validates the batch_request has the correct form.

        Args:
            batch_request: A batch request object to be validated.
        """
        if not (
            batch_request.datasource_name == self.datasource.name
            and batch_request.data_asset_name == self.name
            and not batch_request.options
        ):
            expect_batch_request_form = BatchRequest(
                datasource_name=self.datasource.name,
                data_asset_name=self.name,
                options={},
                batch_slice=batch_request._batch_slice_input,  # type: ignore[attr-defined]
            )
            raise gx_exceptions.InvalidBatchRequestError(
                "BatchRequest should have form:\n"
                f"{pf(expect_batch_request_form.dict())}\n"
                f"but actually has form:\n{pf(batch_request.dict())}\n"
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

        batch_metadata: BatchMetadata = self._get_batch_metadata_from_batch_request(
            batch_request=batch_request
        )

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


@public_api
class SparkDatasource(_SparkDatasource):
    # class attributes
    asset_types: ClassVar[List[Type[DataAsset]]] = [DataFrameAsset]

    # instance attributes
    type: Literal["spark"] = "spark"

    assets: List[DataFrameAsset] = []  # type: ignore[assignment]

    def test_connection(self, test_assets: bool = True) -> None:
        ...

    @public_api
    @deprecated_argument(
        argument_name="dataframe",
        message='The "dataframe" argument is no longer part of "PandasDatasource.add_dataframe_asset()" method call; instead, "dataframe" is the required argument to "DataFrameAsset.build_batch_request()" method.',
        version="0.16.15",
    )
    def add_dataframe_asset(
        self,
        name: str,
        dataframe: Optional[_SparkDataFrameT] = None,
        batch_metadata: Optional[BatchMetadata] = None,
    ) -> DataFrameAsset:
        """Adds a Dataframe DataAsset to this SparkDatasource object.

        Args:
            name: The name of the DataFrame asset. This can be any arbitrary string.
            dataframe: The Spark Dataframe containing the data for this DataFrame data asset.
            batch_metadata: An arbitrary user defined dictionary with string keys which will get inherited by any
                            batches created from the asset.

        Returns:
            The DataFameAsset that has been added to this datasource.
        """
        asset: DataFrameAsset = DataFrameAsset(
            name=name,
            batch_metadata=batch_metadata or {},
        )
        asset.dataframe = dataframe
        return self._add_asset(asset=asset)
