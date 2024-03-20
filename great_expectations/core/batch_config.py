from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from great_expectations.compatibility import pydantic

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


class BatchDefinition(pydantic.BaseModel):
    """Configuration for a batch of data.

    References the DataAsset to be used, and any additional parameters needed to fetch the data.
    """

    id: Optional[str] = None
    name: str
    partitioner: Optional[Partitioner] = None

    # private attributes that must be set immediately after instantiation
    _data_asset: DataAsset = pydantic.PrivateAttr()

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
            options=batch_request_options, partitioner=self.partitioner
        )

    def save(self) -> None:
        self.data_asset._save_batch_config(self)

    def identifier_bundle(self) -> _EncodedValidationData:
        # Utilized as a custom json_encoder
        asset = self.data_asset
        ds = asset.datasource
        return _EncodedValidationData(
            datasource=_IdentifierBundle(
                name=ds.name,
                id=ds.id,
            ),
            asset=_IdentifierBundle(
                name=asset.name,
                id=str(asset.id) if asset.id else None,
            ),
            batch_definition=_IdentifierBundle(
                name=self.name,
                id=self.id,
            ),
        )
