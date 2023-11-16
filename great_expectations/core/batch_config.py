from dataclasses import dataclass

from great_expectations.datasource.fluent.interfaces import DataAsset


@dataclass(frozen=True)
class BatchConfig:
    """Configuration for a batch of data.

    References the DataAsset to be used, and any additional parameters needed to fetch the data.
    TODO: Add splitters and sorters?
    """

    data_asset: DataAsset
