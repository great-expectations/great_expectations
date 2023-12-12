from __future__ import annotations

from typing import TYPE_CHECKING, Callable, Optional

from great_expectations.compatibility import pydantic
from great_expectations.datasource.fluent.interfaces import DataAsset  # noqa: TCH001

if TYPE_CHECKING:
    from great_expectations.datasource.fluent.batch_request import (
        BatchRequest,
        BatchRequestOptions,
    )


class BatchConfig(pydantic.BaseModel):
    """Configuration for a batch of data.

    References the DataAsset to be used, and any additional parameters needed to fetch the data.
    TODO: Add splitters and sorters?
    """

    name: str
    data_asset: DataAsset
    _persist: Callable[[], None] = pydantic.PrivateAttr()

    def build_batch_request(
        self, batch_request_options: Optional[BatchRequestOptions] = None
    ) -> BatchRequest:
        """Build a BatchRequest from the asset and batch request options."""
        return self.data_asset.build_batch_request(options=batch_request_options)

    def save(self) -> None:
        self._persist()
