"""
https://learn.microsoft.com/en-us/python/api/semantic-link-sempy/sempy.fabric?view=semantic-link-python
"""

from __future__ import annotations

import logging
import os
import uuid
from pprint import pformat as pf
from typing import (
    TYPE_CHECKING,
    ClassVar,
    Dict,
    Final,
    List,
    Literal,
    Optional,
    Set,
    Type,
    Union,
)

from typing_extensions import Annotated, TypeAlias

import great_expectations.exceptions as gx_exceptions
from great_expectations._docs_decorators import public_api
from great_expectations.compatibility import pydantic
from great_expectations.compatibility.typing_extensions import override
from great_expectations.core import IDDict
from great_expectations.core.batch import LegacyBatchDefinition
from great_expectations.core.batch_spec import FabricBatchSpec
from great_expectations.datasource.fluent import BatchRequest
from great_expectations.datasource.fluent.batch_identifier_util import make_batch_identifier
from great_expectations.datasource.fluent.constants import _DATA_CONNECTOR_NAME
from great_expectations.datasource.fluent.interfaces import (
    Batch,
    DataAsset,
    Datasource,
    Sorter,
    TestConnectionError,
)
from great_expectations.exceptions.exceptions import BuildBatchRequestError

if TYPE_CHECKING:
    from great_expectations.core.batch_spec import FabricReaderMethods
    from great_expectations.core.partitioners import ColumnPartitioner
    from great_expectations.datasource.fluent import BatchParameters
    from great_expectations.datasource.fluent.data_connector.batch_filter import BatchSlice
    from great_expectations.datasource.fluent.interfaces import (
        BatchMetadata,
    )
    from great_expectations.execution_engine import PandasExecutionEngine

LOGGER = logging.getLogger(__name__)

SortersDefinition: TypeAlias = List[Union[Sorter, str, dict]]

_REQUIRED_FABRIC_SERVICE: Final[str] = "Microsoft.ProjectArcadia"
Mode: TypeAlias = Literal["xmla", "rest", "onelake"]


class _PowerBIAsset(DataAsset):
    """Microsoft PowerBI Asset base class."""

    _reader_method: ClassVar[FabricReaderMethods]
    _EXCLUDE_FROM_READER_OPTIONS: ClassVar[Set[str]] = {
        "batch_definitions",
        "batch_metadata",
        "name",
        "order_by",
        "type",
        "id",
    }

    @override
    def test_connection(self) -> None:
        """
        Whatever is needed to test the connection to and/or validity of the asset.
        This could be a noop.
        """
        LOGGER.debug(f"Testing connection to {self.__class__.__name__} has not been implemented")

    @override
    def get_batch_identifiers_list(self, batch_request: BatchRequest) -> List[dict]:
        return [IDDict(batch_request.options)]

    @override
    def get_batch(self, batch_request: BatchRequest) -> Batch:
        self._validate_batch_request(batch_request)

        reader_options = {
            "workspace": self._datasource.workspace,
            "dataset": self._datasource.dataset,
            **self.dict(
                exclude=self._EXCLUDE_FROM_READER_OPTIONS,
                exclude_none=True,
                exclude_unset=True,
                by_alias=True,
                config_provider=self._datasource._config_provider,
            ),
        }

        batch_spec = FabricBatchSpec(
            reader_method=self._reader_method, reader_options=reader_options
        )
        # TODO: update get_batch_data_and_markers types
        execution_engine: PandasExecutionEngine = self.datasource.get_execution_engine()
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
            batch_spec=batch_spec.to_json_dict(),  # type: ignore[arg-type] # will be coerced to BatchSpec
            batch_definition=batch_definition,
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
            options: This is not currently supported and must be {} or None for this data asset.
            batch_slice: This is not currently supported and must be None for this data asset.
            partitioner: This is not currently supported and must be None for this data asset.

        Returns:
            A BatchRequest object that can be used to obtain a batch from an Asset by calling the
            get_batch method.
        """
        asset_type_name: str = self.__class__.__name__
        if options:
            raise BuildBatchRequestError(
                message=f"options is not currently supported for {asset_type_name} "
                "and must be None or {}."
            )

        if batch_slice is not None:
            raise BuildBatchRequestError(
                message=f"batch_slice is not currently supported for {asset_type_name} "
                "and must be None."
            )

        if partitioner is not None:
            raise BuildBatchRequestError(
                message=f"partitioner is not currently supported for {asset_type_name} "
                "and must be None."
            )

        return BatchRequest(
            datasource_name=self.datasource.name,
            data_asset_name=self.name,
            options={},
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
            and not batch_request.options
        ):
            expect_batch_request_form = BatchRequest[None](
                datasource_name=self.datasource.name,
                data_asset_name=self.name,
                options={},
                batch_slice=batch_request._batch_slice_input,  # type: ignore[attr-defined] # private attr does exist
            )
            raise gx_exceptions.InvalidBatchRequestError(  # noqa: TRY003
                "BatchRequest should have form:\n"
                f"{pf(expect_batch_request_form.dict())}\n"
                f"but actually has form:\n{pf(batch_request.dict())}\n"
            )


@public_api
class PowerBIDax(_PowerBIAsset):
    """Microsoft PowerBI DAX."""

    _reader_method: ClassVar[FabricReaderMethods] = "evaluate_dax"

    type: Literal["powerbi_dax"] = "powerbi_dax"
    dax_string: str


@public_api
class PowerBIMeasure(_PowerBIAsset):
    """Microsoft PowerBI Measure."""

    _reader_method: ClassVar[FabricReaderMethods] = "evaluate_measure"

    type: Literal["powerbi_measure"] = "powerbi_measure"
    measure: Union[str, List[str]]
    groupby_columns: Optional[List[str]] = None
    filters: Optional[Dict[str, List[str]]] = None
    fully_qualified_columns: Optional[bool] = None
    num_rows: Optional[int] = None
    use_xmla: bool = False


@public_api
class PowerBITable(_PowerBIAsset):
    """Microsoft PowerBI Table."""

    _reader_method: ClassVar[FabricReaderMethods] = "read_table"

    type: Literal["powerbi_table"] = "powerbi_table"
    table: str
    fully_qualified_columns: bool = False
    num_rows: Optional[int] = None
    multiindex_hierarchies: bool = False
    mode: Mode = "xmla"


# This improves our error messages by providing a more specific type for pydantic to validate against  # noqa: E501
# It also ensure the generated jsonschema has a oneOf instead of anyOf field for assets
# https://docs.pydantic.dev/1.10/usage/types/#discriminated-unions-aka-tagged-unions
AssetTypes = Annotated[
    Union[PowerBITable, PowerBIMeasure, PowerBIDax],
    pydantic.Field(discriminator="type"),
]


@public_api
class FabricPowerBIDatasource(Datasource):
    """
    Microsoft Fabric Datasource.

    https://pypi.org/project/semantic-link/
    """

    # class var definitions
    asset_types: ClassVar[List[Type[DataAsset]]] = [
        PowerBIDax,
        PowerBIMeasure,
        PowerBITable,
    ]
    # any fabric datsource specific fields should be added to this set
    # example a connection_string field or a data directory field
    _EXTRA_EXCLUDED_EXEC_ENG_ARGS: ClassVar[set] = {"workspace", "dataset"}

    # right side of the operator determines the type name
    # left side enforces the names on instance creation
    type: Literal["fabric_powerbi"] = "fabric_powerbi"
    assets: List[AssetTypes] = []

    # fabric datasource specific fields
    workspace: Optional[Union[uuid.UUID, str]] = None
    dataset: Union[uuid.UUID, str]

    @property
    @override
    def execution_engine_type(self) -> Type[PandasExecutionEngine]:
        """Return the PandasExecutionEngine unless the override is set"""
        from great_expectations.execution_engine.pandas_execution_engine import (
            PandasExecutionEngine,
        )

        return PandasExecutionEngine

    @override
    def test_connection(self, test_assets: bool = True) -> None:
        """Test the connection for the FabricPowerBIDatasource.

        Args:
            test_assets: If assets have been passed to the Datasource, whether to test them as well.

        Raises:
            TestConnectionError: If the connection test fails.
        """
        if not self._running_on_fabric():
            raise TestConnectionError("Must be running Microsoft Fabric to use this datasource")  # noqa: TRY003

        try:
            from sempy import fabric  # noqa: F401 # test if fabric is installed
        except Exception as import_err:
            raise TestConnectionError(  # noqa: TRY003
                "Could not import `sempy.fabric`\npip install semantic-link-sempy"
            ) from import_err

        if self.assets and test_assets:
            for asset in self.assets:
                asset._datasource = self
                asset.test_connection()

    @public_api
    def add_powerbi_dax_asset(
        self,
        name: str,
        dax_string: str,
        batch_metadata: Optional[BatchMetadata] = None,
    ) -> PowerBIDax:
        """Adds a PowerBIDax asset to this datasource.

        Args:
            name: The name of this asset.
            TODO: other args
            batch_metadata: BatchMetadata we want to associate with this DataAsset and all batches derived from it.

        Returns:
            The asset that is added to the datasource.
        """  # noqa: E501
        asset = PowerBIDax(
            name=name,
            batch_metadata=batch_metadata or {},
            dax_string=dax_string,
        )
        return self._add_asset(asset)

    @public_api
    def add_powerbi_measure_asset(  # noqa: PLR0913
        self,
        name: str,
        measure: Union[str, List[str]],
        batch_metadata: Optional[BatchMetadata] = None,
        groupby_columns: Optional[List[str]] = None,
        filters: Optional[Dict[str, List[str]]] = None,
        fully_qualified_columns: Optional[bool] = None,
        num_rows: Optional[int] = None,
        use_xmla: bool = False,
    ) -> PowerBIMeasure:
        """Adds a PowerBIMeasure asset to this datasource.

        Args:
            name: The name of this asset.
            batch_metadata: BatchMetadata we want to associate with this DataAsset and all batches derived from it.

        Returns:
            The asset that is added to the datasource.
        """  # noqa: E501
        asset = PowerBIMeasure(
            name=name,
            batch_metadata=batch_metadata or {},
            groupby_columns=groupby_columns,
            measure=measure,
            # TODO: require custom serde for keys that are tuples
            filters=filters,
            fully_qualified_columns=fully_qualified_columns,
            num_rows=num_rows,
            use_xmla=use_xmla,
        )
        return self._add_asset(asset)

    @public_api
    def add_powerbi_table_asset(  # noqa: PLR0913
        self,
        name: str,
        table: str,
        batch_metadata: Optional[BatchMetadata] = None,
        fully_qualified_columns: bool = False,
        num_rows: Optional[int] = None,
        multiindex_hierarchies: bool = False,
        mode: Mode = "xmla",
    ) -> PowerBITable:
        """Adds a PowerBITable asset to this datasource.

        Args:
            name: The name of this table asset.
            table_name: The table where the data resides.
            schema: The schema that holds the table.
            batch_metadata: BatchMetadata we want to associate with this DataAsset and all batches derived from it.

        Returns:
            The asset that is added to the datasource.
        """  # noqa: E501
        asset = PowerBITable(
            name=name,
            batch_metadata=batch_metadata or {},
            table=table,
            fully_qualified_columns=fully_qualified_columns,
            num_rows=num_rows,
            multiindex_hierarchies=multiindex_hierarchies,
            mode=mode,
        )
        return self._add_asset(asset)

    @staticmethod
    def _running_on_fabric() -> bool:
        if (
            os.environ.get("AZURE_SERVICE")  # noqa: TID251 # needed for fabric
            != _REQUIRED_FABRIC_SERVICE
        ):
            return False
        from pyspark.sql import SparkSession  # noqa: TID251 # needed for fabric

        sc = SparkSession.builder.getOrCreate().sparkContext
        return sc.getConf().get("spark.cluster.type") != "synapse"
