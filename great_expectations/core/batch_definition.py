from __future__ import annotations

from typing import TYPE_CHECKING, Any, Generic, Optional, TypeVar

from great_expectations.compatibility import pydantic

# if we move this import into the TYPE_CHECKING block, we need to provide the
# Partitioner class when we update forward refs, so we just import here.
from great_expectations.core.partitioners import ColumnPartitioner, RegexPartitioner
from great_expectations.core.serdes import _EncodedValidationData, _IdentifierBundle

if TYPE_CHECKING:
    from great_expectations.datasource.fluent.batch_request import (
        BatchParameters,
        BatchRequest,
    )
    from great_expectations.datasource.fluent.interfaces import Batch, DataAsset

# Depending on the Asset
PartitionerT = TypeVar("PartitionerT", ColumnPartitioner, RegexPartitioner, None)


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

    def get_batch(self, batch_parameters: Optional[BatchParameters] = None) -> Batch:
        """
        Retrieves a batch from the underlying asset. Defaults to the last batch
        from the asset's batch list.

        Args:
            batch_parameters: Additional parameters to be used in fetching the batch.

        Returns:
            A Batch of data.
        """
        batch_list = self.data_asset.get_batch_list_from_batch_request(
            self.build_batch_request(batch_parameters=batch_parameters)
        )

        if len(batch_list) == 0:
            raise ValueError("No batch found")  # noqa: TRY003

        return batch_list[-1]

    def save(self) -> None:
        self.data_asset._save_batch_definition(self)

    def identifier_bundle(self) -> _EncodedValidationData:
        # Utilized as a custom json_encoder
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
            id=self.id,
        )

        return _EncodedValidationData(
            datasource=data_source_bundle,
            asset=asset_bundle,
            batch_definition=batch_definition_bundle,
        )
