from typing import Optional

from great_expectations.compatibility import pydantic
from great_expectations.datasource.fluent.batch_request import (
    BatchRequest,
    BatchRequestOptions,
)
from great_expectations.datasource.fluent.interfaces import DataAsset


class BatchConfig(pydantic.BaseModel):
    """Configuration for a batch of data.

    References the DataAsset to be used, and any additional parameters needed to fetch the data.
    TODO: Add splitters and sorters?
    """

    data_asset: DataAsset

    def build_batch_request(
        self, batch_request_options: Optional[BatchRequestOptions] = None
    ) -> BatchRequest:
        """Build a BatchRequest from the asset and batch request options."""
        return self.data_asset.build_batch_request(options=batch_request_options)
