from great_expectations.compatibility import pydantic
from great_expectations.datasource.fluent.interfaces import DataAsset


class BatchConfig(pydantic.BaseModel):
    """Configuration for a batch of data.

    References the DataAsset to be used, and any additional parameters needed to fetch the data.
    TODO: Add splitters and sorters?
    """

    data_asset: DataAsset
