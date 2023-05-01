from __future__ import annotations

import logging
from pprint import pformat as pf
from typing import (
    TYPE_CHECKING,
    ClassVar,
    Generic,
    List,
    Optional,
    Type,
    TypeVar,
)

import pydantic
from typing_extensions import Literal

import great_expectations.exceptions as gx_exceptions
from great_expectations.compatibility import pyspark
from great_expectations.core._docs_decorators import public_api
from great_expectations.core.batch_spec import RuntimeDataBatchSpec
from great_expectations.datasource.fluent.constants import (
    _DATA_CONNECTOR_NAME,
)
from great_expectations.datasource.fluent.interfaces import (
    Batch,
    BatchRequest,
    DataAsset,
    Datasource,
)

if TYPE_CHECKING:
    from great_expectations.datasource.fluent.interfaces import BatchMetadata
    from great_expectations.execution_engine import SparkDFExecutionEngine


logger = logging.getLogger(__name__)


# this enables us to include dataframe in the json schema
_SparkDataFrameT = TypeVar("_SparkDataFrameT")


class SparkDatasourceError(Exception):
    pass


class _SparkDatasource(Datasource):
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
    dataframe: _SparkDataFrameT = pydantic.Field(..., exclude=True, repr=False)

    class Config:
        extra = pydantic.Extra.forbid

    @pydantic.validator("dataframe")
    def _validate_dataframe(cls, dataframe: pyspark.DataFrame) -> pyspark.DataFrame:
        if not (pyspark.DataFrame and isinstance(dataframe, pyspark.DataFrame)):  # type: ignore[truthy-function]
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

    def _get_reader_options_include(self) -> set[str] | None:
        raise NotImplementedError(
            """Spark DataFrameAsset does not implement "_get_reader_options_include()" method, because DataFrame is already available."""
        )

    @public_api
    def build_batch_request(self) -> BatchRequest:  # type: ignore[override]
        """A batch request that can be used to obtain batches for this DataAsset.

        Returns:
            A BatchRequest object that can be used to obtain a batch list from a Datasource by calling the
            get_batch_list_from_batch_request method.
        """
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
                batch_slice=batch_request.batch_slice,
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

    def add_dataframe_asset(
        self,
        name: str,
        dataframe: pyspark.DataFrame,
        batch_metadata: Optional[BatchMetadata] = None,
    ) -> DataFrameAsset:
        """Adds a Dataframe DataAsset to this SparkDatasource object.

        Args:
            name: The name of the DataFrame asset. This can be any arbitrary string.
            dataframe: The DataFrame containing the data for this data asset.
            batch_metadata: An arbitrary user defined dictionary with string keys which will get inherited by any
                            batches created from the asset.

        Returns:
            The DataFameAsset that has been added to this datasource.
        """
        asset = DataFrameAsset(
            name=name,
            dataframe=dataframe,
            batch_metadata=batch_metadata or {},
        )
        return self._add_asset(asset=asset)
