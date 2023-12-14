from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from great_expectations.compatibility import pydantic
from great_expectations.datasource.fluent.interfaces import DataAsset, Datasource

if TYPE_CHECKING:
    from great_expectations.data_context.data_context.abstract_data_context import (
        AbstractDataContext,
    )
    from great_expectations.datasource.fluent.batch_request import (
        BatchRequest,
        BatchRequestOptions,
    )


class BatchConfig(pydantic.BaseModel):
    """Configuration for a batch of data.

    References the DataAsset to be used, and any additional parameters needed to fetch the data.
    TODO: Add splitters and sorters?
    """

    class Config:
        arbitrary_types_allowed = True

    context: AbstractDataContext
    name: str
    datasource_name: str
    data_asset_name: str

    @property
    def data_asset(self) -> DataAsset:
        """Get the DataAsset referenced by this BatchConfig."""
        datasource = self.context.get_datasource(self.datasource_name)
        assert isinstance(datasource, Datasource)

        return datasource.get_asset(self.data_asset_name)

    def build_batch_request(
        self, batch_request_options: Optional[BatchRequestOptions] = None
    ) -> BatchRequest:
        """Build a BatchRequest from the asset and batch request options."""
        return self.data_asset.build_batch_request(options=batch_request_options)
