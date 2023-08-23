from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, List, Literal, Optional, Type, Union

import pydantic
from typing_extensions import Annotated, TypeAlias

from great_expectations.compatibility.typing_extensions import override
from great_expectations.core.batch_spec import FabricBatchSpec
from great_expectations.datasource.fluent import BatchRequest
from great_expectations.datasource.fluent.interfaces import (
    Batch,
    DataAsset,
    Datasource,
    Sorter,
    TestConnectionError,
)

if TYPE_CHECKING:
    from great_expectations.core.batch_spec import FabricReaderMethods
    from great_expectations.datasource.fluent.interfaces import (
        BatchMetadata,
    )
    from great_expectations.execution_engine import PandasExecutionEngine

SortersDefinition: TypeAlias = List[Union[Sorter, str, dict]]


class _PowerBIAsset(DataAsset):
    """Microsoft PowerBI Asset base class."""

    _reader_method: ClassVar[FabricReaderMethods]

    @override
    def test_connection(self) -> None:
        """
        Whatever is needed to test the connection to and/or validatitly of the asset.
        This could be a noop.
        """
        pass

    @override
    def get_batch_list_from_batch_request(
        self, batch_request: BatchRequest
    ) -> list[Batch]:
        self._validate_batch_request(batch_request)
        batch_list: List[Batch] = []

        batch_spec = FabricBatchSpec(
            reader_method=self._reader_method,
            reader_options=self.dict(
                # exclude=self._EXCLUDE_FROM_READER_OPTIONS,
                exclude_unset=True,
                by_alias=True,
                config_provider=self._datasource._config_provider,
            ),
        )
        # TODO: update get_batch_data_and_markers types
        execution_engine: PandasExecutionEngine = self.datasource.get_execution_engine()
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
            # TODO: what to do about data_connector_name?
            data_connector_name="FABRIC_DATA_CONNECTOR_DOES_NOT_EXIST",
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
        # TODO: update Batch legacy_batch_spec types
        Batch.update_forward_refs()
        batch_list.append(
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
        )
        return batch_list

    @override
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


class PowerBIDax(_PowerBIAsset):
    """Microsoft PowerBI DAX."""

    type: Literal["powerbi_dax"] = "powerbi_dax"
    query: str
    dataset: str
    workspace: str


class PowerBIMeasure(_PowerBIAsset):
    """Microsoft PowerBI Measure."""

    type: Literal["powerbi_measure"] = "powerbi_measure"


class PowerBITable(_PowerBIAsset):
    """Microsoft PowerBI Table."""

    type: Literal["powerbi_table"] = "powerbi_table"
    schema_: Optional[str] = pydantic.Field(None, alias="schema")
    table_name: str


# This improves our error messages by providing a more specific type for pydantic to validate against
# It also ensure the generated jsonschema has a oneOf instead of anyOf field for assets
# https://docs.pydantic.dev/1.10/usage/types/#discriminated-unions-aka-tagged-unions
AssetTypes = Annotated[
    Union[PowerBITable, PowerBIMeasure, PowerBIDax],
    pydantic.Field(discriminator="type"),
]


class FabricDatasource(Datasource):
    """Microsoft Fabric Datasource."""

    # class var definitions
    asset_types: ClassVar[List[Type[DataAsset]]] = [
        PowerBIDax,
        PowerBIMeasure,
        PowerBITable,
    ]
    # any fabric datsource specific fields should be added to this set
    # example a connection_string field or a data directory field
    _EXTRA_EXCLUDED_EXEC_ENG_ARGS: ClassVar[set] = {"extra_field_1"}

    # right side of the operator determines the type name
    # left side enforces the names on instance creation
    type: Literal["fabric"] = "fabric"
    assets: List[AssetTypes] = []

    # fabric datasource specific fields
    extra_field_1: Optional[str] = None

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
        """Test the connection for the FabricDatasource.

        Args:
            test_assets: If assets have been passed to the Datasource, whether to test them as well.

        Raises:
            TestConnectionError: If the connection test fails.
        """
        try:
            from sempy import fabric  # noqa: F401 # test if fabric is installed
        except Exception as import_err:
            raise TestConnectionError(
                "Could not import `sempy.fabric`\npip install semantic-link"
            ) from import_err

        # TODO: check if we are running from within fabric?

        if self.assets and test_assets:
            for asset in self.assets:
                asset._datasource = self
                asset.test_connection()

    def add_powerbi_dax_asset(  # noqa: PLR0913
        self,
        name: str,
        query: str,
        dataset: str,
        workspace: str,
        order_by: Optional[SortersDefinition] = None,
        batch_metadata: Optional[BatchMetadata] = None,
    ) -> PowerBIDax:
        """Adds a PowerBIDax asset to this datasource.

        Args:
            name: The name of this asset.
            TODO: other args
            order_by: A list of Sorters or Sorter strings.
            batch_metadata: BatchMetadata we want to associate with this DataAsset and all batches derived from it.

        Returns:
            The asset that is added to the datasource.
        """
        order_by_sorters: list[Sorter] = self.parse_order_by_sorters(order_by=order_by)
        asset = PowerBIDax(
            name=name,
            query=query,
            dataset=dataset,
            workspace=workspace,
            order_by=order_by_sorters,
            batch_metadata=batch_metadata or {},
        )
        return self._add_asset(asset)

    def add_powerbi_measure_asset(
        self,
        name: str,
        order_by: Optional[SortersDefinition] = None,
        batch_metadata: Optional[BatchMetadata] = None,
    ) -> PowerBIMeasure:
        """Adds a PowerBIMeasure asset to this datasource.

        Args:
            name: The name of this asset.
            order_by: A list of Sorters or Sorter strings.
            batch_metadata: BatchMetadata we want to associate with this DataAsset and all batches derived from it.

        Returns:
            The asset that is added to the datasource.
        """
        order_by_sorters: list[Sorter] = self.parse_order_by_sorters(order_by=order_by)
        asset = PowerBIMeasure(
            name=name,
            order_by=order_by_sorters,
            batch_metadata=batch_metadata or {},
        )
        return self._add_asset(asset)

    def add_powerbi_table_asset(  # noqa: PLR0913
        self,
        name: str,
        table_name: str = "",
        schema: Optional[str] = None,
        order_by: Optional[SortersDefinition] = None,
        batch_metadata: Optional[BatchMetadata] = None,
    ) -> PowerBITable:
        """Adds a PowerBITable asset to this datasource.

        Args:
            name: The name of this table asset.
            table_name: The table where the data resides.
            schema: The schema that holds the table.
            order_by: A list of Sorters or Sorter strings.
            batch_metadata: BatchMetadata we want to associate with this DataAsset and all batches derived from it.

        Returns:
            The asset that is added to the datasource.
        """
        order_by_sorters: list[Sorter] = self.parse_order_by_sorters(order_by=order_by)
        asset = PowerBITable(
            name=name,
            table_name=table_name,
            schema=schema,
            order_by=order_by_sorters,
            batch_metadata=batch_metadata or {},
        )
        return self._add_asset(asset)
