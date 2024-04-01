from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from great_expectations.compatibility import pydantic
from great_expectations.core.batch_definition import BatchDefinition

# if we move this import into the TYPE_CHECKING block, we need to provide the
# Partitioner class when we update forward refs, so we just import here.
from great_expectations.core.partitioners import Partitioner  # noqa: TCH001
from great_expectations.core.serdes import _EncodedValidationData, _IdentifierBundle

if TYPE_CHECKING:
    from great_expectations.datasource.fluent.batch_request import (
        BatchRequest,
        BatchRequestOptions,
    )
    from great_expectations.datasource.fluent.interfaces import DataAsset


class BatchDefinitionBase(pydantic.BaseModel):
    """Configuration for a batch of data.

    References the DataAsset to be used, and any additional parameters needed to fetch the data.
    """

    id: Optional[str] = None
    name: str

    # private attributes that must be set immediately after instantiation
    _data_asset: DataAsset = pydantic.PrivateAttr()
    _partitioner: Partitioner = pydantic.PrivateAttr()

    @property
    def data_asset(self) -> DataAsset:
        return self._data_asset

    def set_data_asset(self, data_asset: DataAsset) -> None:
        # pydantic prevents us from using @data_asset.setter
        self._data_asset = data_asset

    def build_batch_request(
        self, batch_request_options: Optional[BatchRequestOptions] = None
    ) -> BatchRequest:
        """Build a BatchRequest from the asset and batch request options."""
        return self.data_asset.build_batch_request(
            options=batch_request_options, partitioner=self._partitioner
        )

    def save(self: BatchDefinition) -> None:
        if not isinstance(self, BatchDefinition):
            raise NotImplementedError
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
