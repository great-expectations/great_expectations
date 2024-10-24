from __future__ import annotations

import logging
import warnings
from pprint import pformat as pf
from typing import (
    TYPE_CHECKING,
    Any,
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

import great_expectations.exceptions as gx_exceptions
from great_expectations._docs_decorators import (
    public_api,
)
from great_expectations.compatibility import pydantic
from great_expectations.compatibility.pydantic import (
    StrictBool,
    StrictFloat,
    StrictInt,
    StrictStr,
)
from great_expectations.compatibility.pyspark import ConnectDataFrame, DataFrame, pyspark
from great_expectations.compatibility.typing_extensions import override
from great_expectations.core import IDDict
from great_expectations.core.batch import LegacyBatchDefinition
from great_expectations.core.batch_spec import RuntimeDataBatchSpec
from great_expectations.datasource.fluent import BatchParameters, BatchRequest
from great_expectations.datasource.fluent.batch_identifier_util import make_batch_identifier
from great_expectations.datasource.fluent.constants import (
    _DATA_CONNECTOR_NAME,
)
from great_expectations.datasource.fluent.interfaces import (
    Batch,
    DataAsset,
    Datasource,
    TestConnectionError,
    _DataAssetT,
)
from great_expectations.exceptions.exceptions import BuildBatchRequestError

if TYPE_CHECKING:
    from typing_extensions import TypeAlias, TypeGuard

    from great_expectations.compatibility.pyspark import SparkSession
    from great_expectations.core.batch_definition import BatchDefinition
    from great_expectations.core.partitioners import ColumnPartitioner
    from great_expectations.datasource.fluent.data_connector.batch_filter import BatchSlice
    from great_expectations.datasource.fluent.interfaces import BatchMetadata
    from great_expectations.execution_engine import SparkDFExecutionEngine

logger = logging.getLogger(__name__)


# this enables us to include dataframe in the json schema
_SparkDataFrameT = TypeVar("_SparkDataFrameT")

SparkConfig: TypeAlias = Dict[StrictStr, Union[StrictStr, StrictInt, StrictFloat, StrictBool]]


class SparkDatasourceError(Exception):
    pass


class _SparkDatasource(Datasource):
    # instance attributes
    spark_config: Union[SparkConfig, None] = None
    force_reuse_spark_context: bool = True
    persist: bool = True

    # private attrs
    _spark: Union[SparkSession, None] = pydantic.PrivateAttr(None)

    @pydantic.validator("force_reuse_spark_context")
    @classmethod
    def _force_reuse_spark_context_deprecation_warning(cls, v: bool) -> bool:
        if v is not None:
            # deprecated-v1.0.0
            warnings.warn(
                "force_reuse_spark_context is deprecated and will be removed in version 1.0. "
                "In environments that allow it, the existing Spark context will be reused, adding the "  # noqa: E501
                "spark_config options that have been passed. If the Spark context cannot be updated with "  # noqa: E501
                "the spark_config, the context will be stopped and restarted with the new spark_config.",  # noqa: E501
                category=DeprecationWarning,
            )
        return v

    @classmethod
    @override
    def update_forward_refs(cls) -> None:  # type: ignore[override]
        from great_expectations.compatibility.pyspark import SparkSession

        super().update_forward_refs(SparkSession=SparkSession)

    @staticmethod
    @override
    def _update_asset_forward_refs(asset_type: Type[_DataAssetT]) -> None:
        # Only update forward refs if pyspark types are available
        if pyspark:
            asset_type.update_forward_refs()

    # Abstract Methods
    @property
    @override
    def execution_engine_type(self) -> Type[SparkDFExecutionEngine]:
        """Return the SparkDFExecutionEngine unless the override is set"""
        from great_expectations.execution_engine.sparkdf_execution_engine import (
            SparkDFExecutionEngine,
        )

        return SparkDFExecutionEngine

    def get_spark(self) -> SparkSession:
        # circular imports require us to import SparkSession and update_forward_refs
        # only when assigning to self._spark for SparkSession isinstance check
        self.update_forward_refs()
        self._spark: SparkSession = self.execution_engine_type.get_or_create_spark_session(
            spark_config=self.spark_config,
        )
        return self._spark

    @override
    def get_execution_engine(self) -> SparkDFExecutionEngine:
        # Method override is required because PrivateAttr _spark won't be passed into Execution Engine  # noqa: E501
        # unless it is passed explicitly.
        current_execution_engine_kwargs = self.dict(
            exclude=self._get_exec_engine_excludes(),
            config_provider=self._config_provider,
        )
        if (
            current_execution_engine_kwargs != self._cached_execution_engine_kwargs
            or not self._execution_engine
        ):
            if self._spark:
                self._execution_engine = self._execution_engine_type()(
                    spark=self._spark, **current_execution_engine_kwargs
                )
            else:
                self._execution_engine = self._execution_engine_type()(
                    **current_execution_engine_kwargs
                )

            self._cached_execution_engine_kwargs = current_execution_engine_kwargs
        return self._execution_engine

    @override
    def test_connection(self, test_assets: bool = True) -> None:
        """Test the connection for the _SparkDatasource.

        Args:
            test_assets: If assets have been passed to the _SparkDatasource,
                         an attempt can be made to test them as well.

        Raises:
            TestConnectionError: If the connection test fails.
        """
        try:
            self.get_spark()
        except Exception as e:
            raise TestConnectionError(cause=e) from e

    # End Abstract Methods


@public_api
class DataFrameAsset(DataAsset, Generic[_SparkDataFrameT]):
    # instance attributes
    type: Literal["dataframe"] = "dataframe"

    class Config:
        extra = pydantic.Extra.forbid

    @override
    def test_connection(self) -> None: ...

    @override
    def get_batch_parameters_keys(
        self, partitioner: Optional[ColumnPartitioner] = None
    ) -> tuple[str, ...]:
        return tuple(
            "dataframe",
        )

    def _get_reader_method(self) -> str:
        raise NotImplementedError(
            """Spark DataFrameAsset does not implement "_get_reader_method()" method, because DataFrame is already available."""  # noqa: E501
        )

    def _get_reader_options_include(self) -> set[str]:
        raise NotImplementedError(
            """Spark DataFrameAsset does not implement "_get_reader_options_include()" method, because DataFrame is already available."""  # noqa: E501
        )

    @override
    def build_batch_request(
        self,
        options: Optional[BatchParameters] = None,
        batch_slice: Optional[BatchSlice] = None,
        partitioner: Optional[ColumnPartitioner] = None,
    ) -> BatchRequest:
        """A batch request that can be used to obtain batches for this DataAsset.

        Args:
            options: This should have 1 key, 'dataframe', whose value is the datafame to validate.
            batch_slice: This is not currently supported and must be None for this data asset.
            partitioner: This is not currently supported and must be None for this data asset.


        Returns:
            A BatchRequest object that can be used to obtain a batch from an Asset by calling the
            get_batch method.
        """
        if batch_slice is not None:
            raise BuildBatchRequestError(
                message="batch_slice is not currently supported for this DataAsset "
                "and must be None."
            )

        if partitioner is not None:
            raise BuildBatchRequestError(
                message="partitioner is not currently supported for this DataAsset "
                "and must be None."
            )

        if not (options is not None and "dataframe" in options and len(options) == 1):
            raise BuildBatchRequestError(message="options must contain exactly 1 key, 'dataframe'.")

        if not self.is_spark_data_frame(options["dataframe"]):
            raise BuildBatchRequestError(
                message="Cannot build batch request without a Spark DataFrame."
            )

        return BatchRequest(
            datasource_name=self.datasource.name,
            data_asset_name=self.name,
            options=options,
        )

    @override
    def _validate_batch_request(self, batch_request: BatchRequest) -> None:
        """Validates the batch_request has the correct form.

        Args:
            batch_request: A batch request object to be validated.
        """
        if not (
            batch_request.datasource_name == self.datasource.name
            and batch_request.data_asset_name == self.name
            and batch_request.options
            and len(batch_request.options) == 1
            and "dataframe" in batch_request.options
            and self.is_spark_data_frame(batch_request.options["dataframe"])
        ):
            expect_batch_request_form = BatchRequest[None](
                datasource_name=self.datasource.name,
                data_asset_name=self.name,
                options={},
                batch_slice=batch_request._batch_slice_input,  # type: ignore[attr-defined]
            )
            raise gx_exceptions.InvalidBatchRequestError(  # noqa: TRY003
                "BatchRequest should have form:\n"
                f"{pf(expect_batch_request_form.dict())}\n"
                f"but actually has form:\n{pf(batch_request.dict())}\n"
            )

    @override
    def get_batch_identifiers_list(self, batch_request: BatchRequest) -> List[dict]:
        return [IDDict(batch_request.options)]

    @override
    def get_batch(self, batch_request: BatchRequest) -> Batch:
        self._validate_batch_request(batch_request)

        batch_spec = RuntimeDataBatchSpec(batch_data=batch_request.options["dataframe"])
        execution_engine: SparkDFExecutionEngine = self.datasource.get_execution_engine()
        data, markers = execution_engine.get_batch_data_and_markers(batch_spec=batch_spec)

        # batch_definition (along with batch_spec and markers) is only here to satisfy a
        # legacy constraint when computing usage statistics in a validator. We hope to remove
        # it in the future.
        batch_definition = LegacyBatchDefinition(
            datasource_name=self.datasource.name,
            data_connector_name=_DATA_CONNECTOR_NAME,
            data_asset_name=self.name,
            batch_identifiers=make_batch_identifier(batch_request.options),
            batch_spec_passthrough=None,
        )

        batch_metadata: BatchMetadata = self._get_batch_metadata_from_batch_request(
            batch_request=batch_request, ignore_options=("dataframe",)
        )

        return Batch(
            datasource=self.datasource,
            data_asset=self,
            batch_request=batch_request,
            data=data,
            metadata=batch_metadata,
            batch_markers=markers,
            batch_spec=batch_spec,
            batch_definition=batch_definition,
        )

    @public_api
    def add_batch_definition_whole_dataframe(self, name: str) -> BatchDefinition:
        return self.add_batch_definition(
            name=name,
            partitioner=None,
        )

    @staticmethod
    def is_spark_data_frame(df: Any) -> TypeGuard[Union[DataFrame, ConnectDataFrame]]:
        """Check that a given object is a Spark DataFrame.
        This could either be a regular Spark DataFrame or a Spark Connect DataFrame.
        """
        data_frame_types = [DataFrame, ConnectDataFrame]
        return any((cls and isinstance(df, cls)) for cls in data_frame_types)


@public_api
class SparkDatasource(_SparkDatasource):
    # class attributes
    asset_types: ClassVar[List[Type[DataAsset]]] = [DataFrameAsset]

    # instance attributes
    type: Literal["spark"] = "spark"

    assets: List[DataFrameAsset] = []

    @public_api
    def add_dataframe_asset(
        self,
        name: str,
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
        """  # noqa: E501
        asset: DataFrameAsset = DataFrameAsset(
            name=name,
            batch_metadata=batch_metadata or {},
        )
        return self._add_asset(asset=asset)
