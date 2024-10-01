from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Generic, List, Optional, TypeVar

from great_expectations._docs_decorators import public_api
from great_expectations.compatibility import pydantic

# if we move this import into the TYPE_CHECKING block, we need to provide the
# Partitioner class when we update forward refs, so we just import here.
from great_expectations.core.freshness_diagnostics import (
    BatchDefinitionFreshnessDiagnostics,
)
from great_expectations.core.partitioners import ColumnPartitioner, FileNamePartitioner
from great_expectations.core.serdes import _EncodedValidationData, _IdentifierBundle
from great_expectations.data_context.data_context.context_factory import project_manager
from great_expectations.exceptions import (
    BatchDefinitionNotAddedError,
    BatchDefinitionNotFoundError,
    BatchDefinitionNotFreshError,
    DataAssetNotFoundError,
    DatasourceNotFoundError,
)

if TYPE_CHECKING:
    from great_expectations.datasource.fluent.batch_request import (
        BatchParameters,
        BatchRequest,
    )
    from great_expectations.datasource.fluent.interfaces import Batch, DataAsset, Datasource

# Depending on the Asset
PartitionerT = TypeVar("PartitionerT", ColumnPartitioner, FileNamePartitioner, None)


@public_api
class BatchDefinition(pydantic.GenericModel, Generic[PartitionerT]):
    """Configuration for a batch of data.

    References the DataAsset to be used, and any additional parameters needed to fetch the data.
    """

    id: Optional[str] = None
    name: str
    partitioner: Optional[PartitionerT] = None

    # private attributes that must be set immediately after instantiation
    # Note that we're using type Any, but the getter setter ensure the right types.
    # If we actually specify DataAsset, pydantic errors out.
    _data_asset: Any = pydantic.PrivateAttr()

    @property
    @public_api
    def data_asset(self) -> DataAsset[Any, PartitionerT]:
        return self._data_asset

    def set_data_asset(self, data_asset: DataAsset[Any, PartitionerT]) -> None:
        # pydantic prevents us from using @data_asset.setter
        self._data_asset = data_asset

    def build_batch_request(
        self, batch_parameters: Optional[BatchParameters] = None
    ) -> BatchRequest[PartitionerT]:
        """Build a BatchRequest from the asset and batch parameters."""
        return self.data_asset.build_batch_request(
            options=batch_parameters,
            partitioner=self.partitioner,
        )

    @public_api
    def get_batch(self, batch_parameters: Optional[BatchParameters] = None) -> Batch:
        """
        Retrieves a batch from the underlying asset. Defaults to the last batch
        from the asset's batch list.

        Args:
            batch_parameters: Additional parameters to be used in fetching the batch.

        Returns:
            A Batch of data.
        """
        batch_request = self.build_batch_request(batch_parameters=batch_parameters)
        return self.data_asset.get_batch(batch_request)

    @public_api
    def get_batch_identifiers_list(
        self, batch_parameters: Optional[BatchParameters] = None
    ) -> List[Dict]:
        """
        Retrieves a list of available batch identifiers.
        These identifiers can be used to fetch specific batches via batch_options.

        Args:
            batch_parameters: Additional parameters to be used in fetching the batch identifiers
            list.

        Returns:
            A list of batch identifiers.
        """
        batch_request = self.build_batch_request(batch_parameters=batch_parameters)
        return self.data_asset.get_batch_identifiers_list(batch_request)

    def is_fresh(self) -> BatchDefinitionFreshnessDiagnostics:
        diagnostics = self._is_added()
        if not diagnostics.success:
            return diagnostics
        return self._is_fresh()

    def _is_added(self) -> BatchDefinitionFreshnessDiagnostics:
        return BatchDefinitionFreshnessDiagnostics(
            errors=[] if self.id else [BatchDefinitionNotAddedError(name=self.name)]
        )

    def _is_fresh(self) -> BatchDefinitionFreshnessDiagnostics:
        datasource_dict = project_manager.get_datasources()

        datasource: Datasource | None
        try:
            datasource = datasource_dict[self.data_asset.datasource.name]
        except KeyError:
            datasource = None
        if not datasource:
            return BatchDefinitionFreshnessDiagnostics(
                errors=[
                    DatasourceNotFoundError(
                        f"Could not find datasource '{self.data_asset.datasource.name}'"
                    )
                ]
            )

        try:
            asset = datasource.get_asset(self.data_asset.name)
        except LookupError:
            asset = None
        if not asset:
            return BatchDefinitionFreshnessDiagnostics(
                errors=[DataAssetNotFoundError(f"Could not find asset '{self.data_asset.name}'")]
            )

        batch_def: BatchDefinition | None
        try:
            batch_def = asset.get_batch_definition(self.name)
        except KeyError:
            batch_def = None
        if not batch_def:
            return BatchDefinitionFreshnessDiagnostics(
                errors=[
                    BatchDefinitionNotFoundError(f"Could not find batch definition '{self.name}'")
                ]
            )

        return BatchDefinitionFreshnessDiagnostics(
            errors=[] if self == batch_def else [BatchDefinitionNotFreshError(name=self.name)]
        )

    def identifier_bundle(self) -> _EncodedValidationData:
        # Utilized as a custom json_encoder
        diagnostics = self.is_fresh()
        diagnostics.raise_for_error()

        asset = self.data_asset
        data_source = asset.datasource

        data_source_bundle = _IdentifierBundle(
            name=data_source.name,
            id=str(data_source.id) if data_source.id else None,
        )
        asset_bundle = _IdentifierBundle(
            name=asset.name,
            id=str(asset.id) if asset.id else None,
        )
        batch_definition_bundle = _IdentifierBundle(
            name=self.name,
            id=str(self.id) if self.id else None,
        )

        return _EncodedValidationData(
            datasource=data_source_bundle,
            asset=asset_bundle,
            batch_definition=batch_definition_bundle,
        )
