from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from great_expectations.compatibility import pydantic

if TYPE_CHECKING:
    from great_expectations.datasource.fluent.batch_request import (
        BatchRequest,
        BatchRequestOptions,
    )
    from great_expectations.datasource.fluent.interfaces import DataAsset


class BatchConfig(pydantic.BaseModel):
    """Configuration for a batch of data.

    References the DataAsset to be used, and any additional parameters needed to fetch the data.
    """

    id: Optional[str] = None
    name: str

    # private attributes that must be set immediately after instantiation
    _data_asset: DataAsset = pydantic.PrivateAttr()

    @property
    def data_asset(self) -> DataAsset:
        return self._data_asset

    def build_batch_request(
        self, batch_request_options: Optional[BatchRequestOptions] = None
    ) -> BatchRequest:
        """Build a BatchRequest from the asset and batch request options."""
        return self.data_asset.build_batch_request(options=batch_request_options)

    def save(self) -> None:
        self.data_asset._save_batch_config(self)
