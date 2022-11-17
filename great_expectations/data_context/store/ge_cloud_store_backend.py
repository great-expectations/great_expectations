from typing_extensions import TypeAlias

from great_expectations.data_context.store.gx_cloud_store_backend import (
    GXCloudStoreBackend,
)

# Type alias until we can completely remove GeCloudStoreBackend
GeCloudStoreBackend: TypeAlias = GXCloudStoreBackend
